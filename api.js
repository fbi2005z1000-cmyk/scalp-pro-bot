const express = require('express');
const {
  validateModeBody,
  validateTradeBody,
  validateTelegramTestBody,
  validateConfigBody,
} = require('./requestValidator');

function createRateLimiter(config, logger) {
  const buckets = new Map();
  const windowMs = config.security.rateLimitWindowMs;
  const max = config.security.rateLimitMax;

  return (req, res, next) => {
    const ip = req.ip || req.socket?.remoteAddress || 'unknown';
    const now = Date.now();

    const item = buckets.get(ip) || { count: 0, resetAt: now + windowMs };
    if (now > item.resetAt) {
      item.count = 0;
      item.resetAt = now + windowMs;
    }
    item.count += 1;
    buckets.set(ip, item);

    if (item.count > max) {
      logger.warn('api', 'Rate limit b? ch?n', { ip, path: req.path, method: req.method });
      return res.status(429).json({ ok: false, error: 'Quá nhi?u request, vui lòng th? l?i sau' });
    }

    return next();
  };
}

function createAdminGuard(config) {
  const token = config.security.adminToken;
  const shouldRequire = config.security.requireAdminForWrite || Boolean(token);

  return (req, res, next) => {
    if (!shouldRequire) return next();
    if (!['POST', 'PUT', 'PATCH', 'DELETE'].includes(req.method)) return next();

    const bearer = String(req.headers.authorization || '');
    const headerToken = String(req.headers['x-admin-token'] || '');
    const normalized = bearer.startsWith('Bearer ') ? bearer.slice(7).trim() : '';

    if (token && (headerToken === token || normalized === token)) {
      return next();
    }

    return res.status(401).json({ ok: false, error: 'Thi?u ho?c sai admin token' });
  };
}

function validateOrReject(res, result) {
  if (result.ok) return false;
  return res.status(400).json({
    ok: false,
    error: 'Payload không h?p l?',
    details: result.errors,
  });
}

function createApi({ botEngine, logger, statsService, glossary, stateStore, config }) {
  const router = express.Router();
  const rateLimiter = createRateLimiter(config, logger);
  const adminGuard = createAdminGuard(config);

  router.use(rateLimiter);
  router.use(adminGuard);

  router.get('/signal', (req, res) => {
    res.json({ ok: true, data: botEngine.getCurrentSignal() });
  });

  router.get('/status', (req, res) => {
    res.json({ ok: true, data: botEngine.getStatus() });
  });

  router.get('/fleet', (req, res) => {
    const status = botEngine.getStatus();
    res.json({
      ok: true,
      data: {
        scanner: status.scanner || {},
        fleet: status.fleet || {},
      },
    });
  });

  router.get('/logs', (req, res) => {
    const limit = Number(req.query.limit) || 200;
    res.json({ ok: true, data: logger.getLogs(limit) });
  });

  router.get('/lifecycle', (req, res) => {
    const limit = Number(req.query.limit) || 200;
    const data =
      typeof botEngine.getLifecycleEvents === 'function'
        ? botEngine.getLifecycleEvents(limit)
        : [];
    res.json({ ok: true, data });
  });

  router.get('/stats', (req, res) => {
    res.json({ ok: true, data: statsService.getStats() });
  });

  router.get('/candles', async (req, res) => {
    const symbol = (req.query.symbol || stateStore.state.symbol).toUpperCase();
    const timeframe = req.query.timeframe || '1m';
    const fresh = String(req.query.fresh || '0') === '1';
    const strict = String(req.query.strict || '0') === '1';
    const limit = Math.max(50, Math.min(1200, Number(req.query.limit || config.timeframe.limit || 600)));
    const analysisTf = config.timeframe.analysis || '3m';
    const allow = Array.from(new Set(['1m', '3m', analysisTf, '5m', '15m']));
    if (!allow.includes(timeframe)) {
      return res.status(400).json({ ok: false, error: 'timeframe không h?p l?' });
    }
    try {
      if (fresh) {
        const candles = await botEngine.refreshCandles(symbol, timeframe, limit, {
          strict,
          priority: true,
        });
        return res.json({ ok: true, data: candles, fresh: true });
      }
      const candles = stateStore.getCandles(symbol, timeframe);
      return res.json({ ok: true, data: candles, fresh: false });
    } catch (error) {
      if (fresh && strict) {
        logger.warn('api', 'Lỗi strict fresh candles', {
          symbol,
          timeframe,
          error: error.message,
        });
        return res.status(502).json({
          ok: false,
          error: 'Không lấy được nến fresh strict từ Binance',
          details: error.message,
        });
      }
      const transient =
        typeof botEngine.isTransientRestError === 'function'
          ? botEngine.isTransientRestError(error)
          : false;
      if (!transient) {
        logger.warn('api', 'Lỗi lấy nến fresh, fallback state cache', {
          symbol,
          timeframe,
          error: error.message,
        });
      }
      const candles = stateStore.getCandles(symbol, timeframe);
      return res.json({ ok: true, data: candles, fresh: false, fallback: true });
    }
  });

  router.post('/candles/hydrate', (req, res) => {
    const symbol = String(req.body?.symbol || '').toUpperCase();
    const timeframe = String(req.body?.timeframe || '');
    const candles = Array.isArray(req.body?.candles) ? req.body.candles : [];
    const analysisTf = config.timeframe.analysis || '3m';
    const allow = Array.from(new Set(['1m', '3m', analysisTf, '5m', '15m']));

    if (!symbol) {
      return res.status(400).json({ ok: false, error: 'Thiếu symbol' });
    }
    if (!allow.includes(timeframe)) {
      return res.status(400).json({ ok: false, error: 'timeframe không hợp lệ' });
    }
    if (!candles.length) {
      return res.status(400).json({ ok: false, error: 'Thiếu dữ liệu candles' });
    }

    const result = botEngine.hydrateCandlesFromClient(symbol, timeframe, candles);
    if (!result.ok) {
      return res.status(400).json({ ok: false, error: result.reason || 'Hydrate thất bại' });
    }

    return res.json({ ok: true, data: { symbol, timeframe, count: result.count } });
  });

  router.get('/stream', (req, res) => {
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.flushHeaders?.();

    const send = (event, payload) => {
      res.write(`event: ${event}\n`);
      res.write(`data: ${JSON.stringify(payload)}\n\n`);
    };

    send('status', botEngine.getStatus());

    const unsubscribers = [];
    unsubscribers.push(botEngine.on('tick', (payload) => send('tick', payload)));
    unsubscribers.push(botEngine.on('signal', (payload) => send('signal', payload)));
    unsubscribers.push(botEngine.on('status', (payload) => send('status', payload)));
    unsubscribers.push(botEngine.on('lifecycle', (payload) => send('lifecycle', payload)));
    unsubscribers.push(botEngine.on('error', (payload) => send('error', payload)));

    const heartbeat = setInterval(() => {
      send('heartbeat', { ts: Date.now() });
    }, 10000);

    req.on('close', () => {
      clearInterval(heartbeat);
      for (const off of unsubscribers) off();
      res.end();
    });
  });

  router.get('/market', (req, res) => {
    const symbol = (req.query.symbol || stateStore.state.symbol).toUpperCase();
    res.json({ ok: true, data: stateStore.state.market[symbol] || {} });
  });

  router.get('/glossary', (req, res) => {
    res.json({ ok: true, data: glossary });
  });

  router.post('/start', async (req, res) => {
    try {
      const data = await botEngine.start();
      res.json({ ok: true, data });
    } catch (error) {
      res.status(400).json({ ok: false, error: error.message });
    }
  });

  router.post('/stop', async (req, res) => {
    try {
      const reason = req.body && typeof req.body.reason === 'string' ? req.body.reason : 'MANUAL_STOP';
      const data = await botEngine.stop(reason);
      res.json({ ok: true, data });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message });
    }
  });

  router.post('/trade', async (req, res) => {
    const invalid = validateOrReject(res, validateTradeBody(req.body));
    if (invalid) return invalid;

    try {
      const result = await botEngine.manualConfirmTrade();
      if (!result.ok) return res.status(400).json({ ok: false, error: result.reason });
      return res.json({ ok: true, data: result });
    } catch (error) {
      return res.status(500).json({ ok: false, error: error.message });
    }
  });

  router.post('/telegram/test', async (req, res) => {
    const invalid = validateOrReject(res, validateTelegramTestBody(req.body));
    if (invalid) return invalid;

    try {
      const data = await botEngine.sendTelegramTest(req.body || {});
      return res.json({ ok: true, data });
    } catch (error) {
      return res.status(400).json({ ok: false, error: error.message });
    }
  });

  router.post('/mode', async (req, res) => {
    const invalid = validateOrReject(res, validateModeBody(req.body));
    if (invalid) return invalid;

    try {
      const mode = req.body.mode;
      const data = await botEngine.changeMode(mode);
      res.json({ ok: true, data });
    } catch (error) {
      res.status(400).json({ ok: false, error: error.message });
    }
  });

  router.post('/config', async (req, res) => {
    const validation = validateConfigBody(req.body, config);
    const invalid = validateOrReject(res, validation);
    if (invalid) return invalid;

    try {
      const data = await botEngine.updateConfig(req.body || {});
      res.json({ ok: true, data });
    } catch (error) {
      res.status(400).json({ ok: false, error: error.message });
    }
  });

  router.post('/emergency', async (req, res) => {
    const enabled = Boolean(req.body?.enabled);
    try {
      const data = await botEngine.updateConfig({
        risk: {
          emergencyStop: enabled,
        },
      });
      if (enabled) {
        await botEngine.stop(req.body?.reason || 'EMERGENCY_STOP_MANUAL');
      }
      res.json({ ok: true, data });
    } catch (error) {
      res.status(400).json({ ok: false, error: error.message });
    }
  });

  router.get('/health', (req, res) => {
    res.json({ ok: true, now: Date.now() });
  });

  return router;
}

module.exports = createApi;

