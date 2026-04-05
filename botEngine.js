const axios = require('axios');
const EventEmitter = require('events');
const WebSocket = require('ws');
const SignalEngine = require('./signalEngine');
const { BOT_STATES } = require('./stateStore');
const { formatTs, formatDateKey } = require('./utils');

class BotEngine {
  constructor({
    config,
    logger,
    stateStore,
    websocketManager,
    riskManager,
    orderManager,
    telegramService,
    chartSnapshot,
  }) {
    this.config = config;
    this.logger = logger;
    this.stateStore = stateStore;
    this.websocketManager = websocketManager;
    this.riskManager = riskManager;
    this.orderManager = orderManager;
    this.telegramService = telegramService;
    this.chartSnapshot = chartSnapshot;
    this.events = new EventEmitter();

    this.signalEngine = new SignalEngine(config, logger);
    this.rest = axios.create({
      baseURL: config.binance.restBaseUrl,
      timeout: 12000,
    });

    this.lastSignalFingerprint = null;
    this.lastSignalAt = 0;
    this.preSignalDispatchMap = new Map();
    this.preSignalCountdownMap = new Map();
    this.lastRejectNotifyAt = 0;
    this.lastRejectCode = null;
    this.lastAcceptedSignal = null;
    this.lastEmergencyHandledAt = 0;
    this.lastAnalyzeAt = 0;
    this.lastTradeEmitAt = 0;
    this.lastBookEmitAt = 0;
    this.lastMarkEmitAt = 0;
    this.lastClosedSyncByTf = new Map();
    this.zoneSignalHistory = new Map();
    this.multiScanTimer = null;
    this.multiScanInFlight = false;
    this.multiScanLastCloseByKey = new Map();
    this.multiScanLastSignalByKey = new Map();
    this.multiScanCursor = 0;
    this.directBots = new Map();
    this.directBotWatchdogTimer = null;
    this.directBotRecoveryCount = 0;
    this.directBotRestartCount = 0;
    this.directBotLastWatchdogAt = 0;
    this.outcomeBots = new Map();
    this.outcomeBotWatchdogTimer = null;
    this.outcomeBotRecoveryCount = 0;
    this.outcomeBotLastWatchdogAt = 0;
    this.positionPulseTimer = null;
    this.positionPulseEndAt = 0;
    this.positionPulseLastPrice = null;
    this.positionPulseEndedPositionId = null;
    this.positionStopRiskNotifyMap = new Map();
    this.restBlockedUntil = 0;
    this.restBlockedReason = null;
    this.lastRestBlockLogAt = 0;
    this.lastRestBudgetLogAt = 0;
    this.restCallWindow = [];
    this.restGeoBlockedUntil = 0;
    this.restGeoNotifiedAt = 0;
    this.lastRestGeoLogAt = 0;
    this.lastWsNotifyAt = 0;
    this.wsHardErrorCount = 0;
    this.lastWsHardErrorAt = 0;
    this.wsCircuitTripCount = 0;
    this.scannerWs = null;
    this.scannerWsConnected = false;
    this.scannerWsReconnectAttempt = 0;
    this.scannerWsManualClose = false;
    this.scannerWsLastMessageAt = 0;
    this.scannerWsHealthTimer = null;
    this.scannerWsReconnectTimer = null;
    this.signalAgeMap = new Map();
    this.tpProbNotifyMap = new Map();
    this.lifecycleEvents = [];
    this.lifecycleMax = 2500;
    this.lifecycleDedupeMap = new Map();
    this.globalSignalQueue = new Map();
    this.globalSignalDispatchDedupe = new Map();
    this.globalSignalLastFlushAt = 0;
    this.globalSignalFlushCount = 0;
    this.globalSignalLastDispatchAt = 0;
    this.globalSignalLastDispatched = null;
    this.signalBroadcastMemory = new Map();
    this.signalFollowTracks = new Map();
    this.priceQuoteCache = new Map();
    this.signalOutcomeLastNotifyAt = 0;
    this.bound = false;

    if (this.orderManager && typeof this.orderManager.setLifecycleHook === 'function') {
      this.orderManager.setLifecycleHook((type, payload = {}) => {
        this.emitLifecycle(type, {
          source: 'ORDER_MANAGER',
          ...(payload || {}),
        });
      });
    }
  }

  on(eventName, listener) {
    this.events.on(eventName, listener);
    return () => this.events.off(eventName, listener);
  }

  emitEvent(eventName, payload) {
    this.events.emit(eventName, payload);
  }

  buildTradeId({ signal, timeframe, candleTime, triggerPrice }) {
    const symbol = String(signal?.symbol || '').toUpperCase();
    const side = String(signal?.side || '').toUpperCase();
    const tf = String(timeframe || signal?.signalTimeframe || this.config.timeframe.analysis || '3m');
    const price = Number(
      triggerPrice || signal?.entryPrice || signal?.entryMin || signal?.preSignal?.selected?.triggerPrice || 0,
    );
    const band = Number.isFinite(price) && price > 0
      ? Math.round(price / Math.max(price * 0.0005, 0.5))
      : 0;
    const ts = Number(candleTime || signal?.createdAt || Date.now());
    return `${symbol}:${tf}:${side}:${band}:${Math.floor(ts / 60)}`;
  }

  emitLifecycle(type, payload = {}, options = {}) {
    const eventType = String(type || '').toUpperCase();
    if (!eventType) return null;
    const now = Date.now();
    const dedupeMs = Math.max(300, Number(options.dedupeMs || 1200));
    const tradeId = String(payload.tradeId || this.buildTradeId({ signal: payload, timeframe: payload.timeframe, candleTime: payload.candleTime }) || '');
    const dedupeKey = `${eventType}:${tradeId}:${payload.symbol || ''}:${payload.timeframe || ''}`;
    const lastAt = Number(this.lifecycleDedupeMap.get(dedupeKey) || 0);
    if (lastAt && now - lastAt < dedupeMs) return null;
    this.lifecycleDedupeMap.set(dedupeKey, now);

    const event = {
      ts: now,
      type: eventType,
      tradeId: tradeId || null,
      symbol: payload.symbol || null,
      side: payload.side || null,
      timeframe: payload.timeframe || null,
      source: payload.source || 'BOT_ENGINE',
      data: payload,
    };

    this.lifecycleEvents.push(event);
    if (this.lifecycleEvents.length > this.lifecycleMax) {
      this.lifecycleEvents.splice(0, this.lifecycleEvents.length - this.lifecycleMax);
    }
    if (this.lifecycleDedupeMap.size > 6000) {
      const threshold = now - 30 * 60 * 1000;
      for (const [k, ts] of this.lifecycleDedupeMap.entries()) {
        if (ts < threshold) this.lifecycleDedupeMap.delete(k);
      }
    }

    this.emitEvent('lifecycle', event);
    return event;
  }

  getLifecycleEvents(limit = 200) {
    const cap = Math.max(1, Math.min(5000, Number(limit || 200)));
    return this.lifecycleEvents.slice(-cap);
  }

  getHttpStatus(error) {
    return Number(error?.response?.status || 0);
  }

  isRateLimitError(error) {
    const status = this.getHttpStatus(error);
    return status === 418 || status === 429;
  }

  isGeoBlockedError(error) {
    const status = this.getHttpStatus(error);
    return status === 451 || status === 403;
  }

  isTransientRestError(error) {
    if (this.isRateLimitError(error)) return true;
    if (this.isGeoBlockedError(error)) return true;
    const msg = String(error?.message || '').toUpperCase();
    return (
      msg.includes('STATUS CODE 418') ||
      msg.includes('STATUS CODE 429') ||
      msg.includes('STATUS CODE 451') ||
      msg.includes('STATUS CODE 403') ||
      msg.includes('ECONNRESET') ||
      msg.includes('ETIMEDOUT') ||
      msg.includes('EAI_AGAIN') ||
      msg.includes('REST_BLOCKED') ||
      msg.includes('REST_GEO_BLOCKED')
    );
  }

  isTransientWsErrorMessage(message = '') {
    const msg = String(message || '').toUpperCase();
    return (
      msg.includes('ECONNRESET') ||
      msg.includes('ETIMEDOUT') ||
      msg.includes('EAI_AGAIN') ||
      msg.includes('1006') ||
      msg.includes('CLOSED') ||
      msg.includes('CONNECTION RESET') ||
      msg.includes('PING')
    );
  }

  isTransientRuntimeError(message = '') {
    const msg = String(message || '').toUpperCase();
    return (
      this.isTransientWsErrorMessage(msg) ||
      msg.includes('TELEGRAM') ||
      msg.includes('REST_BLOCKED') ||
      msg.includes('REST_GEO_BLOCKED') ||
      msg.includes('STATUS CODE 418') ||
      msg.includes('STATUS CODE 429') ||
      msg.includes('STATUS CODE 451') ||
      msg.includes('ECONNABORTED') ||
      msg.includes('SOCKET HANG UP') ||
      msg.includes('TIMEOUT')
    );
  }

  normalizeClientCandles(candles = []) {
    if (!Array.isArray(candles)) return [];
    const byTime = new Map();
    for (const c of candles) {
      const t = Number(c?.time);
      if (!Number.isFinite(t)) continue;
      byTime.set(t, {
        time: t,
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close),
        volume: Number(c.volume || 0),
        isClosed: Boolean(c.isClosed),
      });
    }
    return Array.from(byTime.values()).sort((a, b) => a.time - b.time);
  }

  getRetryAfterMs(error) {
    const rawHeader = error?.response?.headers?.['retry-after'];
    const sec = Number(rawHeader);
    if (!Number.isFinite(sec) || sec <= 0) return 0;
    return sec * 1000;
  }

  consumeRestBudget() {
    const cap = Math.max(20, Number(this.config.binance.restSoftLimitPerMin || 180));
    const now = Date.now();
    this.restCallWindow = this.restCallWindow.filter((ts) => now - ts < 60000);

    if (this.restCallWindow.length >= cap) {
      const blockMs = Math.max(3000, Math.floor(60000 / Math.max(1, cap)));
      this.restBlockedUntil = Math.max(this.restBlockedUntil, now + blockMs);
      this.restBlockedReason = 'SOFT_CAP';
      if (now - this.lastRestBudgetLogAt > 20000) {
        this.lastRestBudgetLogAt = now;
        this.logger.info('bot', 'REST chạm ngưỡng nội bộ, tạm hạ nhịp gọi API', {
          capPerMin: cap,
          blockMs,
          blockedUntil: this.restBlockedUntil,
        });
      }
      return false;
    }

    this.restCallWindow.push(now);
    return true;
  }

  blockRestByRateLimit(error) {
    const status = this.getHttpStatus(error);
    const now = Date.now();
    const headerRetryMs = this.getRetryAfterMs(error);
    const configured429 = Math.max(1000, Number(this.config.binance.restBlock429Ms || 45000));
    const configured418 = Math.max(configured429, Number(this.config.binance.restBlock418Ms || 600000));
    const blockMs = Math.max(status === 418 ? configured418 : configured429, headerRetryMs);
    this.restBlockedUntil = Math.max(this.restBlockedUntil, now + blockMs);
    this.restBlockedReason = `HTTP_${status}`;

    if (now - this.lastRestBlockLogAt > 10000) {
      this.lastRestBlockLogAt = now;
      this.logger.warn('bot', 'REST Binance bị giới hạn, tạm giảm tải gọi API', {
        status,
        blockMs,
        blockedUntil: this.restBlockedUntil,
      });
    }
  }

  trySetState(nextState, meta = {}) {
    const current = this.stateStore.state.stateMachine;
    if (current === nextState) return true;
    if (!this.stateStore.canTransition(nextState)) {
      return false;
    }
    return this.stateStore.setStateMachine(nextState, meta);
  }

  async initialize() {
    if (this.stateStore.state.mode === 'SIGNAL_ONLY') {
      this.stateStore.setBotRunning(true);
    }
    this.initializeDirectBots();
    this.initializeOutcomeBots();
    await this.bootstrapCandles(this.stateStore.state.symbol);
    await this.bootstrapScannerSymbols();
    this.bindWebsocket();
    this.websocketManager.connect(this.stateStore.state.symbol);
    this.connectScannerMarketStream();
    this.startMultiSymbolScanner();
    this.startDirectBotWatchdog();
    this.startOutcomeBotWatchdog();
  }

  bindWebsocket() {
    if (this.bound) return;
    this.bound = true;

    this.websocketManager.on('kline', async (candle) => {
      const activeSymbol = this.stateStore.state.symbol;
      if (candle?.symbol && candle.symbol !== activeSymbol) return;
      await this.onKline(candle, candle.timeframe || '1m');
    });

    this.websocketManager.on('book', async (book) => {
      const activeSymbol = this.stateStore.state.symbol;
      if (book?.symbol && book.symbol !== activeSymbol) return;
      this.stateStore.setOrderBook(book);
      const now = Date.now();
      if (now - this.lastBookEmitAt > 120) {
        this.lastBookEmitAt = now;
        this.emitEvent('tick', {
          symbol: book.symbol || this.stateStore.state.symbol,
          ts: now,
          type: 'book',
          payload: { ...book, source: 'LAST' },
        });
      }

      if (this.stateStore.state.activePosition && !this.riskManager.isEmergencyStop()) {
        this.trySetState(BOT_STATES.MANAGING_POSITION, { source: 'book_tick' });
        await this.orderManager.onPriceTick(book.lastPrice);
      }
      this.syncPositionPulse();
    });

    this.websocketManager.on('trade', async (trade) => {
      const symbol = this.stateStore.state.symbol;
      if (trade?.symbol && trade.symbol !== symbol) return;
      const nextOrderBook = {
        lastTradePrice: trade.price,
        lastPrice:
          this.config.trading.priceSource === 'MARK'
            ? this.stateStore.state.orderBook.markPrice || trade.price
            : trade.price,
      };
      this.stateStore.setOrderBook(nextOrderBook);

      const now = Date.now();
      if (now - this.lastTradeEmitAt > 120) {
        this.lastTradeEmitAt = now;
        this.emitEvent('tick', {
          symbol,
          ts: now,
          type: 'trade',
          payload: { ...trade, source: 'LAST' },
        });
      }

      if (this.stateStore.state.activePosition && this.config.trading.priceSource === 'LAST') {
        this.trySetState(BOT_STATES.MANAGING_POSITION, { source: 'trade_tick' });
        await this.orderManager.onPriceTick(trade.price);
      }
      this.syncPositionPulse();
    });

    this.websocketManager.on('mark', async (mark) => {
      const activeSymbol = this.stateStore.state.symbol;
      if (mark?.symbol && mark.symbol !== activeSymbol) return;
      this.stateStore.setOrderBook(mark);
      const now = Date.now();
      if (now - this.lastMarkEmitAt > 120) {
        this.lastMarkEmitAt = now;
        this.emitEvent('tick', {
          symbol: mark.symbol || this.stateStore.state.symbol,
          ts: now,
          type: 'mark',
          payload: { ...mark, source: 'MARK' },
        });
      }

      if (
        this.stateStore.state.activePosition &&
        this.config.trading.priceSource === 'MARK' &&
        !this.riskManager.isEmergencyStop()
      ) {
        this.trySetState(BOT_STATES.MANAGING_POSITION, { source: 'mark_tick' });
        await this.orderManager.onPriceTick(mark.lastPrice || mark.markPrice);
      }
      this.syncPositionPulse();
    });

    this.websocketManager.on('reconnect', async ({ attempt, delay }) => {
      if (attempt >= 2) {
        this.logger.info('websocket', 'Đang reconnect WS', { attempt, delay });
      }
      this.emitEvent('status', this.getStatus());
      try {
        await this.orderManager.syncPosition();
      } catch (error) {
        this.logger.warn('websocket', 'Sync position sau reconnect thất bại', {
          error: error.message,
        });
      }
    });

    this.websocketManager.on('stale', async ({ staleForMs, staleDataMs, strike, shouldReconnect }) => {
      if (shouldReconnect || Number(strike || 0) >= 4) {
        this.logger.info('websocket', 'Phát hiện stale data', {
          staleForMs,
          staleDataMs,
          strike: strike || 1,
          shouldReconnect: Boolean(shouldReconnect),
        });
      }
      this.emitEvent('status', this.getStatus());
    });

    this.websocketManager.on('gap', async ({ from, to, missingMinutes }) => {
      this.logger.warn('websocket', 'Gap dữ liệu nến, đang backfill lại lịch sử', {
        from,
        to,
        missingMinutes,
      });
      try {
        await this.bootstrapCandles(this.stateStore.state.symbol);
      } catch (error) {
        if (!this.isTransientRestError(error)) {
          this.logger.warn('websocket', 'Không backfill được sau gap', { error: error.message });
        }
      }
    });

    this.websocketManager.on('circuit', async ({ attempt, cooldownMs }) => {
      this.logger.error('websocket', 'WS circuit breaker kích hoạt', { attempt, cooldownMs });
      this.wsCircuitTripCount += 1;
      const now = Date.now();
      if (now - this.lastWsNotifyAt > 120000) {
        this.lastWsNotifyAt = now;
        await this.telegramService.sendError({
          title: 'WebSocket circuit breaker',
          detail: `Reconnect thất bại nhiều lần, cooldown ${cooldownMs}ms (auto-recover, trip=${this.wsCircuitTripCount})`,
        });
      }
    });

    this.websocketManager.on('error', async ({ error }) => {
      const message = String(error?.message || 'UNKNOWN_WS_ERROR');
      this.stateStore.setError(message);

      if (!this.isTransientWsErrorMessage(message)) {
        const now = Date.now();
        if (now - this.lastWsHardErrorAt > 180000) {
          this.wsHardErrorCount = 0;
        }
        this.lastWsHardErrorAt = now;
        this.wsHardErrorCount += 1;

        if (this.wsHardErrorCount >= 8) {
          this.riskManager.registerError('WEBSOCKET_HARD', message);
          this.wsHardErrorCount = 0;
        }
      } else {
        this.wsHardErrorCount = 0;
      }
      this.emitEvent('error', { ts: Date.now(), error: message });
      const now = Date.now();
      if (!this.isTransientWsErrorMessage(message) && now - this.lastWsNotifyAt > 120000) {
        this.lastWsNotifyAt = now;
        await this.telegramService.sendError({
          title: 'WebSocket lỗi',
          detail: message,
        });
      }
    });
  }

  async fetchKlines(symbol, interval, limit = 600, options = {}) {
    const priority = Boolean(options.priority);
    if (Date.now() < this.restGeoBlockedUntil) {
      const cached = this.stateStore.getCandles(symbol, interval);
      if (Array.isArray(cached) && cached.length) {
        const picked = cached.slice(-Math.max(20, Number(limit || 600)));
        if (picked.length) return picked;
      }
      const remainMs = this.restGeoBlockedUntil - Date.now();
      const err = new Error(`REST_GEO_BLOCKED ${remainMs}ms`);
      err.code = 'REST_GEO_BLOCKED';
      throw err;
    }
    if (!priority && Date.now() < this.restBlockedUntil) {
      const remainMs = this.restBlockedUntil - Date.now();
      const err = new Error(`REST_BLOCKED ${remainMs}ms`);
      err.code = 'REST_BLOCKED';
      throw err;
    }
    if (!priority && !this.consumeRestBudget()) {
      const remainMs = Math.max(0, this.restBlockedUntil - Date.now());
      const err = new Error(`REST_BLOCKED ${remainMs}ms`);
      err.code = 'REST_BLOCKED';
      throw err;
    }

    const retries = 3;
    let lastError = null;

    for (let attempt = 1; attempt <= retries; attempt += 1) {
      try {
        const response = await this.rest.get('/fapi/v1/klines', {
          params: { symbol, interval, limit },
        });

        const now = Date.now();
        return response.data.map((k) => ({
          time: Math.floor(k[0] / 1000),
          open: Number(k[1]),
          high: Number(k[2]),
          low: Number(k[3]),
          close: Number(k[4]),
          volume: Number(k[5]),
          quoteVolume: Number(k[7] || 0),
          tradeCount: Number(k[8] || 0),
          takerBuyBaseVolume: Number(k[9] || 0),
          takerBuyQuoteVolume: Number(k[10] || 0),
          closeTime: Number(k[6]),
          isClosed: Number(k[6]) <= now,
        }));
      } catch (error) {
        lastError = error;
        if (this.isGeoBlockedError(error)) {
          const now = Date.now();
          const blockMs = 15 * 60 * 1000;
          this.restGeoBlockedUntil = Math.max(this.restGeoBlockedUntil, now + blockMs);
          if (now - this.restGeoNotifiedAt > 60 * 60 * 1000) {
            this.restGeoNotifiedAt = now;
            this.logger.info('bot', 'REST Binance bị chặn theo region/chính sách, tự chuyển WS + cache', {
              status: this.getHttpStatus(error),
              blockMs,
              blockedUntil: this.restGeoBlockedUntil,
            });
          }
          break;
        }
        if (this.isRateLimitError(error)) {
          this.blockRestByRateLimit(error);
          break;
        }
        if (attempt >= retries) break;
        const delayMs = 350 * attempt;
        await new Promise((resolve) => setTimeout(resolve, delayMs));
      }
    }

    throw lastError;
  }

  async fetchLatestPrice(symbol, options = {}) {
    const priority = options.priority !== false;
    const maxAgeMs = Math.max(300, Number(options.maxAgeMs || 1500));
    const key = String(symbol || '').toUpperCase();
    if (!key) return null;

    const cached = this.priceQuoteCache.get(key);
    if (cached && Date.now() - Number(cached.ts || 0) <= maxAgeMs) {
      return cached;
    }

    if (Date.now() < this.restGeoBlockedUntil) return cached || null;
    if (!priority && Date.now() < this.restBlockedUntil) return cached || null;
    if (!priority && !this.consumeRestBudget()) return cached || null;

    try {
      const response = await this.rest.get('/fapi/v1/ticker/price', {
        params: { symbol: key },
        timeout: 6000,
      });
      const p = Number(response?.data?.price || 0);
      if (!Number.isFinite(p) || p <= 0) return cached || null;
      const payload = { symbol: key, price: p, ts: Date.now(), source: 'REST_TICKER_PRICE' };
      this.priceQuoteCache.set(key, payload);
      if (this.priceQuoteCache.size > 500) {
        const cutoff = Date.now() - 5 * 60 * 1000;
        for (const [k, v] of this.priceQuoteCache.entries()) {
          if (Number(v?.ts || 0) < cutoff) this.priceQuoteCache.delete(k);
        }
      }
      return payload;
    } catch (error) {
      if (this.isGeoBlockedError(error)) {
        const now = Date.now();
        this.restGeoBlockedUntil = Math.max(this.restGeoBlockedUntil, now + 10 * 60 * 1000);
      } else if (this.isRateLimitError(error)) {
        this.blockRestByRateLimit(error);
      }
      return cached || null;
    }
  }

  async refreshCandles(symbol, interval, limit = this.config.timeframe.limit, options = {}) {
    const strict = Boolean(options.strict);
    const priority = Boolean(options.priority);
    try {
      const candles = await this.fetchKlines(symbol, interval, limit, { priority });
      this.stateStore.setCandles(symbol, interval, candles, limit);
      return candles;
    } catch (error) {
      if (strict) {
        throw error;
      }
      if (this.isTransientRestError(error)) {
        return this.stateStore.getCandles(symbol, interval);
      }
      throw error;
    }
  }

  hydrateCandlesFromClient(symbol, timeframe, candles = []) {
    const normalized = this.normalizeClientCandles(candles);
    if (!normalized.length) {
      return { ok: false, reason: 'NO_VALID_CANDLES' };
    }
    const limit = Math.max(120, Number(this.config.timeframe.limit || 600));
    this.stateStore.setCandles(String(symbol || '').toUpperCase(), timeframe, normalized, limit);
    return { ok: true, count: normalized.length };
  }

  async bootstrapCandles(symbol) {
    const analysisTf = this.config.timeframe.analysis || '3m';
    const intervals = Array.from(new Set(['1m', '3m', '5m', '15m', analysisTf]));
    const settled = await Promise.allSettled(
      intervals.map(async (tf) => ({
        tf,
        candles: await this.fetchKlines(symbol, tf, this.config.timeframe.limit, { priority: true }),
      })),
    );

    const ok = [];
    const failed = [];
    settled.forEach((item, idx) => {
      if (item.status === 'fulfilled') {
        ok.push(item.value);
      } else {
        failed.push({
          tf: intervals[idx],
          error: item.reason?.message || 'UNKNOWN',
        });
      }
    });

    for (const item of ok) {
      this.stateStore.setCandles(symbol, item.tf, item.candles, this.config.timeframe.limit);
    }

    const countByTf = Object.fromEntries(ok.map((d) => [d.tf, d.candles.length]));
    if (failed.length) {
      const transientOnly = failed.every((f) => this.isTransientRestError({ message: f.error }));
      const logMethod = transientOnly ? this.logger.info.bind(this.logger) : this.logger.warn.bind(this.logger);
      logMethod('bot', 'Bootstrap nến có khung lỗi, dùng dữ liệu còn lại', {
        symbol,
        failed,
        transientOnly,
      });
      if (!transientOnly) {
        this.riskManager.registerError('BOOTSTRAP_PARTIAL', JSON.stringify(failed));
      }
    }

    if (!ok.length) {
      const transientOnly = failed.every((f) => this.isTransientRestError({ message: f.error }));
      if (transientOnly) {
        this.logger.info('bot', 'Lỗi bootstrap candles toàn bộ khung (transient), dùng WS + cache', {
          symbol,
          failed,
        });
      } else {
        this.logger.error('bot', 'Lỗi bootstrap candles toàn bộ khung', { symbol, failed });
        this.riskManager.registerError('BOOTSTRAP', JSON.stringify(failed));
      }
      return;
    }

    this.logger.info('bot', 'Đã nạp dữ liệu lịch sử', {
      symbol,
      analysisTf,
      countByTf,
    });
  }

  async syncClosedCandleFromRest(symbol, timeframe, expectedClosedTime) {
    const key = `${symbol}:${timeframe}`;
    const now = Date.now();
    const lastSyncAt = this.lastClosedSyncByTf.get(key) || 0;
    if (now - lastSyncAt < 1200) return;
    this.lastClosedSyncByTf.set(key, now);

    const latest = await this.fetchKlines(symbol, timeframe, 4, { priority: true });
    const closed = latest.filter((c) => c.isClosed);
    if (!closed.length) return;

    const matched =
      closed.find((c) => c.time === expectedClosedTime) || closed[closed.length - 1];
    this.stateStore.addCandle(symbol, timeframe, matched, this.config.timeframe.limit);

    const prevClosed = closed[closed.length - 2];
    if (prevClosed) {
      this.stateStore.addCandle(symbol, timeframe, prevClosed, this.config.timeframe.limit);
    }
  }

  getSignalZoneKey(signal) {
    if (!signal || signal.side === 'NO_TRADE') return null;
    const sr = signal.market?.supportResistance || {};
    if (signal.side === 'LONG') {
      const zoneId =
        sr?.potential?.longZone?.id ||
        sr?.keyLevels?.nearestSupportZoneId ||
        sr?.supports?.[0]?.id;
      if (zoneId) return `${signal.symbol}:LONG:${zoneId}`;
    }
    if (signal.side === 'SHORT') {
      const zoneId =
        sr?.potential?.shortZone?.id ||
        sr?.keyLevels?.nearestResistanceZoneId ||
        sr?.resistances?.[0]?.id;
      if (zoneId) return `${signal.symbol}:SHORT:${zoneId}`;
    }
    const entryBand = signal.entryPrice
      ? Math.round(signal.entryPrice / Math.max(signal.entryPrice * 0.0006, 0.5))
      : 0;
    return `${signal.symbol}:${signal.side}:band:${entryBand}`;
  }

  registerAcceptedZoneSignal(signal) {
    const zoneKey = this.getSignalZoneKey(signal);
    if (!zoneKey) return;
    const now = Date.now();
    const windowMs = this.config.trading.zoneSignalWindowMs;
    const history = (this.zoneSignalHistory.get(zoneKey) || []).filter((t) => now - t <= windowMs);
    history.push(now);
    this.zoneSignalHistory.set(zoneKey, history);
  }

  pruneSignalBroadcastMemory() {
    if (!this.signalBroadcastMemory || !this.signalBroadcastMemory.size) return;
    const now = Date.now();
    const keepMs = 2 * 60 * 60 * 1000;
    for (const [key, value] of this.signalBroadcastMemory.entries()) {
      const ts = Number(value?.sentAt || 0);
      if (!ts || now - ts > keepMs) this.signalBroadcastMemory.delete(key);
    }
  }

  buildSignalBroadcastKey(signal, timeframe) {
    const symbol = String(signal?.symbol || '').toUpperCase();
    const side = String(signal?.side || '').toUpperCase();
    const tf = String(timeframe || signal?.signalTimeframe || this.config.timeframe.analysis || '3m');
    const entry = Number(signal?.entryPrice || signal?.entryMin || 0);
    const band = entry > 0 ? Math.round(entry / Math.max(entry * 0.0006, 0.5)) : 0;
    return `${symbol}:${tf}:${side}:${band}`;
  }

  buildSignalSnapshot(signal, timeframe, candleTime) {
    return {
      symbol: String(signal?.symbol || '').toUpperCase(),
      side: String(signal?.side || '').toUpperCase(),
      timeframe: String(timeframe || signal?.signalTimeframe || this.config.timeframe.analysis || '3m'),
      candleTime: Number(candleTime || signal?.createdAt || Date.now()),
      sentAt: Date.now(),
      entryPrice: Number(signal?.entryPrice || 0),
      stopLoss: Number(signal?.stopLoss || 0),
      tp1: Number(signal?.tp1 || 0),
      tp2: Number(signal?.tp2 || 0),
      tp3: Number(signal?.tp3 || 0),
      rr: Number(signal?.rr || 0),
      confidence: Number(signal?.confidence || 0),
      level: String(signal?.level || ''),
      trend5m: String(signal?.trend5m || ''),
      volatilityRegime: String(signal?.volatilityRegime || ''),
      globalRank: Number(signal?.globalRanking?.rank || 0),
    };
  }

  getSignalVariation(nextSignal, prevSignal) {
    if (!nextSignal || !prevSignal) return { meaningful: false, changes: [] };
    const changes = [];
    const ratio = (a, b) => {
      if (!Number.isFinite(a) || !Number.isFinite(b) || !b) return 0;
      return Math.abs((a - b) / b);
    };

    const nextEntry = Number(nextSignal.entryPrice || 0);
    const prevEntry = Number(prevSignal.entryPrice || 0);
    const nextSl = Number(nextSignal.stopLoss || 0);
    const prevSl = Number(prevSignal.stopLoss || 0);
    const nextTp1 = Number(nextSignal.tp1 || 0);
    const prevTp1 = Number(prevSignal.tp1 || 0);
    const nextConfidence = Number(nextSignal.confidence || 0);
    const prevConfidence = Number(prevSignal.confidence || 0);
    const nextRr = Number(nextSignal.rr || 0);
    const prevRr = Number(prevSignal.rr || 0);
    const nextRank = Number(nextSignal.globalRanking?.rank || nextSignal.globalRank || 0);
    const prevRank = Number(prevSignal.globalRanking?.rank || prevSignal.globalRank || 0);

    if (ratio(nextEntry, prevEntry) >= 0.0004) changes.push('Vùng entry thay đổi');
    if (ratio(nextSl, prevSl) >= 0.0005) changes.push('Mức SL cập nhật');
    if (ratio(nextTp1, prevTp1) >= 0.0007) changes.push('Mục tiêu TP1 cập nhật');
    if (Math.abs(nextConfidence - prevConfidence) >= 2) changes.push('Confidence thay đổi');
    if (Math.abs(nextRr - prevRr) >= 0.08) changes.push('Tỷ lệ RR thay đổi');
    if (Math.abs(nextRank - prevRank) >= 2.5) changes.push('Xếp hạng kèo thay đổi');
    if (String(nextSignal.level || '') !== String(prevSignal.level || '')) changes.push('Cấp độ tín hiệu đổi trạng thái');
    if (String(nextSignal.volatilityRegime || '') !== String(prevSignal.volatilityRegime || '')) {
      changes.push('Regime thị trường thay đổi');
    }
    if (String(nextSignal.trend5m || '') !== String(prevSignal.trend5m || '')) {
      changes.push('Xu hướng 5m thay đổi');
    }

    return { meaningful: changes.length > 0, changes };
  }

  hasMeaningfulSignalVariation(signal, referenceSignal = null) {
    const prev = referenceSignal || this.lastAcceptedSignal;
    if (!signal || !prev) return false;
    if (String(signal.symbol || '').toUpperCase() !== String(prev.symbol || '').toUpperCase()) return false;
    if (String(signal.side || '').toUpperCase() !== String(prev.side || '').toUpperCase()) return false;
    return this.getSignalVariation(signal, prev).meaningful;
  }

  isSpecialWatchSymbol(symbol) {
    const list = Array.isArray(this.config.trading.specialWatchSymbols)
      ? this.config.trading.specialWatchSymbols
      : [];
    const normalized = String(symbol || '').toUpperCase();
    return list.includes(normalized);
  }

  getSignalLevelRank(level) {
    const normalized = String(level || '').toUpperCase();
    if (normalized === 'STRONG') return 3;
    if (normalized === 'MEDIUM') return 2;
    if (normalized === 'WEAK') return 1;
    return 0;
  }

  buildSignalFollowKey(symbol, timeframe) {
    return `${String(symbol || '').toUpperCase()}:${String(timeframe || '').toLowerCase()}`;
  }

  upsertSignalFollowTrack(signal, timeframe, candleTime) {
    if (!this.config.trading.signalFollowEnabled) return;
    if (!signal || signal.side === 'NO_TRADE') return;
    const symbol = String(signal.symbol || '').toUpperCase();
    if (!symbol) return;
    const tf = String(timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m');
    const key = this.buildSignalFollowKey(symbol, tf);
    const prev = this.signalFollowTracks.get(key);
    const sideChanged = prev && String(prev.side || '').toUpperCase() !== String(signal.side || '').toUpperCase();
    const now = Date.now();
    const entryPrice = Number(signal.entryPrice || signal.entryMin || 0);
    const stopLoss = Number(signal.stopLoss || 0);
    const tp1 = Number(signal.tp1 || 0);
    const tp2 = Number(signal.tp2 || 0);
    const tp3 = Number(signal.tp3 || 0);

    const next = {
      key,
      tradeId: signal.tradeId || prev?.tradeId || null,
      symbol,
      timeframe: tf,
      side: String(signal.side || '').toUpperCase(),
      entryPrice,
      stopLoss,
      tp1,
      tp2,
      tp3,
      confidence: Number(signal.confidence || 0),
      level: String(signal.level || ''),
      rr: Number(signal.rr || 0),
      specialWatch: this.isSpecialWatchSymbol(symbol),
      createdAt: sideChanged || !prev ? now : Number(prev.createdAt || now),
      updatedAt: now,
      lastCandleTime: Number(candleTime || prev?.lastCandleTime || 0),
      lastNotifyAt: sideChanged || !prev ? 0 : Number(prev.lastNotifyAt || 0),
      candleCount: sideChanged || !prev ? 0 : Number(prev.candleCount || 0),
      reached: sideChanged || !prev
        ? { tp1: false, tp2: false, tp3: false, sl: false }
        : {
            tp1: Boolean(prev.reached?.tp1),
            tp2: Boolean(prev.reached?.tp2),
            tp3: Boolean(prev.reached?.tp3),
            sl: Boolean(prev.reached?.sl),
          },
      closed: false,
      lastSignal: {
        ...signal,
        symbol,
        signalTimeframe: tf,
      },
    };
    this.signalFollowTracks.set(key, next);
    this.pruneSignalFollowTracks();
  }

  pruneSignalFollowTracks() {
    if (!this.signalFollowTracks.size) return;
    const now = Date.now();
    const keepMs = 6 * 60 * 60 * 1000;
    for (const [key, track] of this.signalFollowTracks.entries()) {
      const ts = Number(track?.updatedAt || track?.createdAt || 0);
      if (!ts || now - ts > keepMs) this.signalFollowTracks.delete(key);
    }
  }

  getSignalOutcomeWindowMs() {
    return Math.max(5 * 60 * 1000, Number(this.config.trading?.signalOutcomeWindowMs || 60 * 60 * 1000));
  }

  getSignalOutcomeStakeUSDT() {
    return Math.max(1, Number(this.config.trading?.signalOutcomeStakeUsdt || 10));
  }

  toDisplayPrice(value) {
    const n = Number(value);
    if (!Number.isFinite(n)) return null;
    const abs = Math.abs(n);
    if (abs >= 1000) return Number(n.toFixed(2));
    if (abs >= 100) return Number(n.toFixed(3));
    if (abs >= 1) return Number(n.toFixed(4));
    if (abs >= 0.1) return Number(n.toFixed(5));
    if (abs >= 0.01) return Number(n.toFixed(6));
    if (abs >= 0.001) return Number(n.toFixed(7));
    if (abs >= 0.0001) return Number(n.toFixed(8));
    return Number(n.toFixed(10));
  }

  registerSignalOutcomeTrackFromSignal(signal, timeframe, candleTime, options = {}) {
    if (!signal || signal.side === 'NO_TRADE') return null;
    const tradeId = String(signal.tradeId || '').trim();
    if (!tradeId) return null;

    const tf = String(timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m');
    const side = String(signal.side || '').toUpperCase();
    const symbol = String(signal.symbol || '').toUpperCase();
    const entryPrice = Number(signal.entryPrice || signal.entryMin || signal.entryMax || 0);
    const stopLoss = Number(signal.stopLoss || 0);
    const tp1 = Number(signal.tp1 || 0);
    const tp2 = Number(signal.tp2 || 0);
    const tp3 = Number(signal.tp3 || 0);
    if (!(entryPrice > 0 && stopLoss > 0 && tp1 > 0 && tp2 > 0 && tp3 > 0)) return null;

    const sentAt = Date.now();
    const windowMs = this.getSignalOutcomeWindowMs();
    const stakeUSDT = this.getSignalOutcomeStakeUSDT();
    const leverage = Math.max(1, Number(signal.leverage || this.config.binance.leverage || 10));

    const current = this.stateStore.state.signalOutcomes?.active?.[tradeId] || null;
    this.stateStore.upsertSignalOutcomeTrack({
      tradeId,
      symbol,
      side,
      timeframe: tf,
      source: String(options.source || signal.source || 'TOTAL_BRAIN'),
      status: 'OPEN',
      sentAt: Number(current?.sentAt || sentAt),
      expiresAt: Number((current?.sentAt || sentAt) + windowMs),
      updatedAt: sentAt,
      candleTime: Number(candleTime || signal.createdAt || 0),
      entryPrice: this.toDisplayPrice(entryPrice),
      entryMin: this.toDisplayPrice(Number(signal.entryMin || entryPrice)),
      entryMax: this.toDisplayPrice(Number(signal.entryMax || entryPrice)),
      stopLoss: this.toDisplayPrice(stopLoss),
      tp1: this.toDisplayPrice(tp1),
      tp2: this.toDisplayPrice(tp2),
      tp3: this.toDisplayPrice(tp3),
      confidence: Number(signal.confidence || 0),
      level: String(signal.level || ''),
      rr: Number(signal.rr || 0),
      regime: String(signal.volatilityRegime || signal.market?.volatilityRegime || 'N/A'),
      stakeUSDT,
      leverage,
      feePct: Number(signal.takerFeePct || this.config.trading.takerFeePct || 0.0004) * 2,
      telegramRoutedAt: sentAt,
      lastPrice: null,
      result: null,
      note: options.isUpdate ? 'UPDATED' : 'SENT',
    });
    this.stateStore.pruneSignalOutcomes(Date.now());
    this.syncOutcomeBotOpenCount(symbol);
    return tradeId;
  }

  computeSignalOutcomeResult(track, candle) {
    if (!track || !candle) return null;
    const side = String(track.side || '').toUpperCase();
    if (!['LONG', 'SHORT'].includes(side)) return null;
    const high = Number(candle.high);
    const low = Number(candle.low);
    const close = Number(candle.close);
    const entryPrice = Number(track.entryPrice || 0);
    const stopLoss = Number(track.stopLoss || 0);
    const tp1 = Number(track.tp1 || 0);
    const tp2 = Number(track.tp2 || 0);
    const tp3 = Number(track.tp3 || 0);
    if (!(entryPrice > 0 && stopLoss > 0 && tp1 > 0 && tp2 > 0 && tp3 > 0)) return null;

    const isLong = side === 'LONG';
    const tp1Hit = isLong ? high >= tp1 : low <= tp1;
    const tp2Hit = isLong ? high >= tp2 : low <= tp2;
    const tp3Hit = isLong ? high >= tp3 : low <= tp3;
    const slHit = isLong ? low <= stopLoss : high >= stopLoss;

    // Quy tắc bảo thủ: nếu cùng nến vừa chạm TP vừa chạm SL thì tính SL.
    if (slHit) return { status: 'LOSS_SL', hitPrice: stopLoss };
    if (tp3Hit) return { status: 'WIN_TP3', hitPrice: tp3 };
    if (tp2Hit) return { status: 'WIN_TP2', hitPrice: tp2 };
    if (tp1Hit) return { status: 'WIN_TP1', hitPrice: tp1 };

    return {
      status: 'OPEN',
      hitPrice: close,
    };
  }

  buildSignalOutcomePnl(track, status, hitPrice) {
    const entry = Number(track.entryPrice || 0);
    const price = Number(hitPrice || 0);
    const lev = Math.max(1, Number(track.leverage || this.config.binance.leverage || 10));
    const stake = Math.max(1, Number(track.stakeUSDT || this.getSignalOutcomeStakeUSDT()));
    const feePct = Math.max(0, Number(track.feePct || 0));
    if (!(entry > 0 && price > 0)) {
      return { movePct: 0, roiPct: 0, pnlUSDT: 0 };
    }

    const side = String(track.side || '').toUpperCase();
    const direction = side === 'SHORT' ? -1 : 1;
    const movePctRaw = ((price - entry) / entry) * 100 * direction;
    const roiPctRaw = movePctRaw * lev;
    const roiNetPct = roiPctRaw - feePct * 100;
    const pnlUSDT = (stake * roiNetPct) / 100;

    return {
      movePct: Number(movePctRaw.toFixed(4)),
      roiPct: Number(roiNetPct.toFixed(4)),
      pnlUSDT: Number(pnlUSDT.toFixed(4)),
      stakeUSDT: stake,
    };
  }

  resolveSignalOutcome(track, status, candle, reason = '') {
    const hitPrice = Number(
      status === 'LOSS_SL'
        ? track.stopLoss
        : status === 'WIN_TP3'
        ? track.tp3
        : status === 'WIN_TP2'
        ? track.tp2
        : status === 'WIN_TP1'
        ? track.tp1
        : candle?.close || track.entryPrice || 0,
    );
    const pnl = this.buildSignalOutcomePnl(track, status, hitPrice);
    const resolved = this.stateStore.resolveSignalOutcomeTrack(track.tradeId, {
      status,
      result: status,
      reason: reason || status,
      hitPrice: this.toDisplayPrice(hitPrice),
      resolvedAt: Date.now(),
      candleTime: Number(candle?.time || 0),
      movePct: pnl.movePct,
      roiPct: pnl.roiPct,
      pnlUSDT: pnl.pnlUSDT,
      stakeUSDT: pnl.stakeUSDT,
    });
    if (!resolved) return null;

    this.emitLifecycle('SIGNAL_OUTCOME', {
      tradeId: resolved.tradeId,
      symbol: resolved.symbol,
      side: resolved.side,
      timeframe: resolved.timeframe,
      source: 'WINRATE_BOT',
      outcome: resolved.status,
      pnlUSDT: resolved.pnlUSDT,
      roiPct: resolved.roiPct,
      movePct: resolved.movePct,
    });
    return resolved;
  }

  evaluateSignalOutcomesByCandle(symbol, timeframe, candle) {
    if (!candle || !candle.isClosed) return;
    const snapshot = this.stateStore.getSignalOutcomeSnapshot();
    const active = snapshot.active || {};
    const symbolNorm = String(symbol || '').toUpperCase();
    if (!symbolNorm) return;
    const tfNorm = String(timeframe || '').toLowerCase();
    if (!tfNorm) return;
    const now = Date.now();
    const existingBot = this.outcomeBots.get(symbolNorm);
    const candleKey = `${tfNorm}:${Number(candle.time || 0)}`;
    if (existingBot?.lastCandleKey === candleKey) return;

    let resolvedWins = 0;
    let resolvedLosses = 0;
    let resolvedExpired = 0;
    let processed = 0;
    let latestOutcome = null;

    for (const track of Object.values(active)) {
      if (!track || String(track.symbol || '').toUpperCase() !== symbolNorm) continue;
      if (track.status && String(track.status).toUpperCase() !== 'OPEN') continue;
      if (tfNorm && String(track.timeframe || '').toLowerCase() !== tfNorm) {
        // Theo dõi kèo theo chính timeframe phát lệnh; nếu muốn soi dày hơn có thể bỏ chặn này.
        continue;
      }

      if (Number(track.expiresAt || 0) > 0 && now > Number(track.expiresAt)) {
        const resolved = this.resolveSignalOutcome(track, 'EXPIRED', candle, 'WINDOW_1H_EXPIRED');
        if (resolved) {
          resolvedExpired += 1;
          latestOutcome = resolved;
        }
        continue;
      }

      const result = this.computeSignalOutcomeResult(track, candle);
      if (!result) continue;
      processed += 1;
      if (result.status === 'OPEN') {
        this.stateStore.upsertSignalOutcomeTrack({
          ...track,
          tradeId: track.tradeId,
          updatedAt: now,
          lastPrice: this.toDisplayPrice(result.hitPrice),
        });
        continue;
      }
      const resolved = this.resolveSignalOutcome(track, result.status, candle, 'PRICE_HIT_TARGET');
      if (!resolved) continue;
      latestOutcome = resolved;
      if (String(result.status || '').startsWith('WIN')) resolvedWins += 1;
      else if (String(result.status || '').startsWith('LOSS')) resolvedLosses += 1;
    }
    this.stateStore.pruneSignalOutcomes(now);
    this.syncOutcomeBotOpenCount(symbolNorm, {
      timeframe: tfNorm,
      processed,
      resolvedWins,
      resolvedLosses,
      resolvedExpired,
      latestOutcome,
      lastCandleKey: candleKey,
      now,
    });
  }

  getSignalOutcomeReportRange(startTs, endTs = Date.now()) {
    const snapshot = this.stateStore.getSignalOutcomeSnapshot();
    const active = Object.values(snapshot.active || {}).filter((x) => Number(x.sentAt || 0) >= startTs);
    const history = (snapshot.history || []).filter(
      (x) => Number(x.sentAt || 0) >= startTs && Number(x.sentAt || 0) <= endTs,
    );
    const all = [...active, ...history].sort((a, b) => Number(b.sentAt || 0) - Number(a.sentAt || 0));
    const resolved = history.filter((x) => String(x.status || '').toUpperCase() !== 'OPEN');
    const wins = resolved.filter((x) => String(x.status || '').toUpperCase().startsWith('WIN'));
    const losses = resolved.filter((x) => String(x.status || '').toUpperCase().startsWith('LOSS'));
    const expired = resolved.filter((x) => String(x.status || '').toUpperCase() === 'EXPIRED');
    const winRate = wins.length + losses.length > 0 ? (wins.length / (wins.length + losses.length)) * 100 : 0;
    const pnlUSDT = resolved.reduce((sum, x) => sum + Number(x.pnlUSDT || 0), 0);
    return {
      startTs,
      endTs,
      totalSignals: all.length,
      activeSignals: active.length,
      resolvedSignals: resolved.length,
      wins: wins.length,
      losses: losses.length,
      expired: expired.length,
      winRate,
      pnlUSDT: Number(pnlUSDT.toFixed(4)),
      items: all,
    };
  }

  async evaluateSignalFollowTrack(symbol, timeframe, candle) {
    if (!this.config.trading.signalFollowEnabled) return;
    if (!candle || !candle.isClosed) return;
    const normalizedSymbol = String(symbol || '').toUpperCase();
    const normalizedTf = timeframe ? String(timeframe || '').toLowerCase() : '';
    if (!normalizedSymbol) return;

    // Khi không truyền timeframe (hoặc truyền rỗng), dùng cùng một cây nến đóng để
    // cập nhật toàn bộ kèo đang theo dõi của cùng symbol => tránh tình trạng "báo xong im".
    if (!normalizedTf) {
      const trackList = Array.from(this.signalFollowTracks.values()).filter(
        (track) =>
          !track?.closed &&
          String(track?.symbol || '').toUpperCase() === normalizedSymbol &&
          String(track?.timeframe || '').length > 0,
      );
      for (const track of trackList) {
        await this.evaluateSignalFollowTrack(normalizedSymbol, String(track.timeframe), candle);
      }
      return;
    }

    const key = this.buildSignalFollowKey(normalizedSymbol, normalizedTf);
    const track = this.signalFollowTracks.get(key);
    if (!track || track.closed) return;
    if (String(track.symbol || '').toUpperCase() !== normalizedSymbol) return;

    const candleTime = Number(candle.time || 0);
    if (candleTime <= Number(track.lastCandleTime || 0)) return;
    const price = Number(candle.close || candle.lastPrice || 0);
    if (!Number.isFinite(price) || price <= 0) return;
    const entry = Number(track.entryPrice || 0);
    const sl = Number(track.stopLoss || 0);
    const tp1 = Number(track.tp1 || 0);
    const tp2 = Number(track.tp2 || 0);
    const tp3 = Number(track.tp3 || 0);
    if (!(entry > 0 && sl > 0 && tp1 > 0 && tp2 > 0 && tp3 > 0)) return;

    const now = Date.now();
    const maxCandles = Math.max(2, Number(this.config.trading.signalFollowMaxCandles || 12));
    const perCandle = Math.max(1, Number(this.config.trading.signalFollowUpdateEveryCandles || 1));
    const cooldownMs = Math.max(2000, Number(this.config.trading.signalFollowCooldownMs || 10000));
    const nearTpPct = Math.max(0.0001, Number(this.config.trading.signalFollowNearTpPct || 0.0009));
    const nearSlPct = Math.max(0.0001, Number(this.config.trading.signalFollowNearSlPct || 0.0009));
    const side = String(track.side || '').toUpperCase();

    const distanceTo = (target) => {
      if (!(entry > 0 && target > 0)) return Number.POSITIVE_INFINITY;
      return Math.abs((target - price) / entry);
    };

    const isLong = side === 'LONG';
    const hit = {
      tp1: isLong ? price >= tp1 : price <= tp1,
      tp2: isLong ? price >= tp2 : price <= tp2,
      tp3: isLong ? price >= tp3 : price <= tp3,
      sl: isLong ? price <= sl : price >= sl,
    };
    const near = {
      tp1: distanceTo(tp1) <= nearTpPct,
      tp2: distanceTo(tp2) <= nearTpPct,
      tp3: distanceTo(tp3) <= nearTpPct,
      sl: distanceTo(sl) <= nearSlPct,
    };

    track.lastCandleTime = candleTime;
    track.updatedAt = now;
    track.candleCount = Number(track.candleCount || 0) + 1;

    const changes = [];
    const reached = track.reached || { tp1: false, tp2: false, tp3: false, sl: false };
    if (hit.tp1 && !reached.tp1) {
      reached.tp1 = true;
      changes.push('✅ Đã chạm TP1');
    }
    if (hit.tp2 && !reached.tp2) {
      reached.tp2 = true;
      changes.push('✅ Đã chạm TP2');
    }
    if (hit.tp3 && !reached.tp3) {
      reached.tp3 = true;
      changes.push('🏁 Đã chạm TP3 (kèo hoàn tất)');
      track.closed = true;
    }
    if (hit.sl && !reached.sl) {
      reached.sl = true;
      changes.push('🛑 Chạm SL / kèo bị dừng');
      track.closed = true;
    }
    track.reached = reached;

    if (!changes.length) {
      if (near.tp3) changes.push('⚡ Khả năng cao chạm TP3 nếu giữ đà');
      else if (near.tp2) changes.push('⚡ Khả năng chạm TP2 đang tăng');
      else if (near.tp1) changes.push('⚡ Đang áp sát TP1');
      if (near.sl) changes.push('❗ Áp lực về SL tăng, cần quản trị rủi ro');
      if (!changes.length && track.candleCount % perCandle === 0) {
        changes.push('🔄 Kèo vẫn đang được theo dõi liên tục theo nến đóng');
      }
    }

    if (track.candleCount >= maxCandles && !track.closed) {
      changes.push(`⌛ Hết thời hạn theo dõi ${maxCandles} nến, đóng kèo theo dõi`);
      track.closed = true;
    }

    const shouldNotify = changes.length > 0 && now - Number(track.lastNotifyAt || 0) >= cooldownMs;
    if (!shouldNotify) {
      this.signalFollowTracks.set(key, track);
      return;
    }

    track.lastNotifyAt = now;
    this.signalFollowTracks.set(key, track);

    const baseSignal = {
      ...(track.lastSignal || {}),
      symbol: track.symbol,
      side: track.side,
      signalTimeframe: track.timeframe,
      entryPrice: track.entryPrice,
      entryMin: track.lastSignal?.entryMin || track.entryPrice,
      entryMax: track.lastSignal?.entryMax || track.entryPrice,
      stopLoss: track.stopLoss,
      tp1: track.tp1,
      tp2: track.tp2,
      tp3: track.tp3,
      confidence: track.confidence,
      livePriceAtSend: price,
      livePriceAtSendTs: now,
      livePriceSource: 'CLOSE_CANDLE',
      liveDeviationPct: entry > 0 ? Math.abs((price - entry) / entry) : 0,
      tradeId: track.tradeId,
      specialWatch: track.specialWatch,
    };
    await this.telegramService.sendSignalUpdate(baseSignal, changes, null);

    if (track.closed) {
      this.signalFollowTracks.delete(key);
    }
  }

  getSignalBroadcastDecision(signal, timeframe, candleTime) {
    const key = this.buildSignalBroadcastKey(signal, timeframe);
    const prev = this.signalBroadcastMemory.get(key);
    if (!prev) return { suppress: false, isUpdate: false, key, changes: [] };

    const now = Date.now();
    const sinceLastMs = now - Number(prev.sentAt || 0);
    if (sinceLastMs < 3500) {
      return {
        suppress: true,
        isUpdate: false,
        key,
        reason: 'UPDATE_COOLDOWN',
        changes: [],
      };
    }

    const current = this.buildSignalSnapshot(signal, timeframe, candleTime);
    const variation = this.getSignalVariation(current, prev.snapshot || prev);
    if (!variation.meaningful) {
      return {
        suppress: true,
        isUpdate: false,
        key,
        reason: 'NO_MATERIAL_CHANGE',
        changes: [],
      };
    }

    return {
      suppress: false,
      isUpdate: true,
      key,
      changes: variation.changes.slice(0, 4),
    };
  }

  commitSignalBroadcast(signal, timeframe, candleTime) {
    const key = this.buildSignalBroadcastKey(signal, timeframe);
    const snapshot = this.buildSignalSnapshot(signal, timeframe, candleTime);
    this.signalBroadcastMemory.set(key, {
      key,
      sentAt: Date.now(),
      snapshot,
    });
    this.pruneSignalBroadcastMemory();
  }

  getSignalDuplicateDecision(signal) {
    const st = this.stateStore.state;
    const cooldownMs = this.config.trading.signalDuplicateCooldownMs;
    const maxPriceRangePct = this.config.trading.signalDuplicatePriceRangePct;

    if (!signal || signal.side === 'NO_TRADE') {
      return { skip: false };
    }

    if (st.activePosition && !this.config.trading.allowMultiplePosition) {
      return { skip: true, reason: 'Đang có vị thế mở, chặn tín hiệu mới để tránh chồng lệnh' };
    }

    if (
      [BOT_STATES.OPEN_POSITION, BOT_STATES.MANAGING_POSITION, BOT_STATES.PARTIAL_TP].includes(
        st.stateMachine,
      )
    ) {
      return { skip: true, reason: `State ${st.stateMachine}: không nhận tín hiệu mới` };
    }

    if (Date.now() - signal.createdAt > this.config.trading.maxSignalAgeMs) {
      return { skip: true, reason: 'Signal đã quá cũ theo maxSignalAgeMs' };
    }

    if (st.stateMachine === BOT_STATES.WAIT_CONFIRM && st.manualPendingSignal?.side === signal.side) {
      const pending = st.manualPendingSignal;
      const dev = Math.abs((pending.entryPrice - signal.entryPrice) / signal.entryPrice);
      if (dev <= maxPriceRangePct) {
        return { skip: true, reason: 'Đang chờ xác nhận tín hiệu gần giống trước đó' };
      }
    }

    const zoneKey = this.getSignalZoneKey(signal);
    if (zoneKey) {
      const now = Date.now();
      const zoneHistory = (this.zoneSignalHistory.get(zoneKey) || []).filter(
        (t) => now - t <= this.config.trading.zoneSignalWindowMs,
      );
      this.zoneSignalHistory.set(zoneKey, zoneHistory);

      if (
        zoneHistory.length >= this.config.trading.maxSignalsPerZoneWindow &&
        now - zoneHistory[zoneHistory.length - 1] < this.config.trading.zoneSignalCooldownMs
      ) {
        return {
          skip: true,
          reason: `Zone cooldown: ${signal.side} đã chạm giới hạn ${this.config.trading.maxSignalsPerZoneWindow} tín hiệu trong cùng vùng`,
        };
      }

      if (
        zoneHistory.length &&
        now - zoneHistory[zoneHistory.length - 1] < this.config.trading.zoneSignalCooldownMs
      ) {
        const lastAccepted = this.lastAcceptedSignal;
        if (lastAccepted && lastAccepted.side === signal.side) {
          const priceDev = Math.abs((lastAccepted.entryPrice - signal.entryPrice) / signal.entryPrice);
          if (priceDev <= maxPriceRangePct) {
            if (this.hasMeaningfulSignalVariation(signal, lastAccepted)) {
              return { skip: false, reason: 'MATERIAL_UPDATE_ALLOWED', updateHint: true };
            }
            return {
              skip: true,
              reason: `Duplicate signal ${signal.side} cùng vùng giá, đang cooldown`,
            };
          }
        }
      }
    }

    if (this.lastAcceptedSignal) {
      const sameSide = this.lastAcceptedSignal.side === signal.side;
      const withinCooldown = Date.now() - this.lastSignalAt < cooldownMs;
      const priceDev = Math.abs((this.lastAcceptedSignal.entryPrice - signal.entryPrice) / signal.entryPrice);
      const sameRegime =
        (this.lastAcceptedSignal.volatilityRegime || 'UNKNOWN') ===
        (signal.volatilityRegime || 'UNKNOWN');
      const sameTrend = (this.lastAcceptedSignal.trend5m || 'UNKNOWN') === (signal.trend5m || 'UNKNOWN');

      if (sameSide && sameRegime && sameTrend && withinCooldown && priceDev <= maxPriceRangePct) {
        if (this.hasMeaningfulSignalVariation(signal, this.lastAcceptedSignal)) {
          return { skip: false, reason: 'MATERIAL_UPDATE_ALLOWED', updateHint: true };
        }
        return {
          skip: true,
          reason: `Duplicate signal guard: cùng hướng/regime/trend, lệch giá ${(priceDev * 100).toFixed(
            3,
          )}% trong cửa sổ ${cooldownMs}ms`,
        };
      }
    }

    return { skip: false };
  }

  timeframeToSec(tf) {
    const parsed = String(tf || '').trim().toLowerCase().match(/^(\d+)m$/);
    if (!parsed) return 180;
    const minutes = Number(parsed[1]);
    if (!Number.isFinite(minutes) || minutes <= 0) return 180;
    return minutes * 60;
  }

  getPreSignalTimeframes() {
    return ['1m', '3m', '5m'];
  }

  getPreSignalLeadCandles(timeframe) {
    const tf = String(timeframe || '').toLowerCase();
    if (tf === '1m') return Math.max(1, Number(this.config.trading.preSignalLeadCandles1m || 2));
    if (tf === '3m') return Math.max(1, Number(this.config.trading.preSignalLeadCandles3m || 1));
    if (tf === '5m') return Math.max(1, Number(this.config.trading.preSignalLeadCandles5m || 1));
    return Math.max(1, Number(this.config.telegram.preSignalMinCandles || 1));
  }

  prunePreSignalDispatchMap() {
    const now = Date.now();
    const keepMs = 3 * 60 * 60 * 1000;
    for (const [key, value] of this.preSignalDispatchMap.entries()) {
      const ts = Number(value?.lastSentAt || value || 0);
      if (!ts || now - ts > keepMs) this.preSignalDispatchMap.delete(key);
    }
    for (const [key, value] of this.preSignalCountdownMap.entries()) {
      const ts = Number(value?.lastSentAt || value || 0);
      if (!ts || now - ts > keepMs) this.preSignalCountdownMap.delete(key);
    }
  }

  buildPreSignalCountdownStep(remainingCandles) {
    const n = Number(remainingCandles);
    if (!Number.isFinite(n)) return null;
    const clamped = Math.max(0, Math.min(5, Math.ceil(n)));
    return clamped;
  }

  buildPreSignalCampaignKey(signal, timeframe, pre) {
    const triggerPrice = Number(pre?.triggerPrice || 0);
    const band = triggerPrice > 0 ? Math.round(triggerPrice / Math.max(triggerPrice * 0.0005, 0.5)) : 0;
    return `${signal.symbol}:${timeframe}:${pre.side}:${band}`;
  }

  findRecentPreCampaign(symbol, timeframe, side) {
    const prefix = `${symbol}:${timeframe}:${side}:`;
    const now = Date.now();
    let best = null;
    for (const [key, campaign] of this.preSignalCountdownMap.entries()) {
      if (!key.startsWith(prefix)) continue;
      if (!campaign || campaign.cancelledAt || campaign.confirmedAt) continue;
      const ts = Number(campaign.lastSentAt || 0);
      if (!ts) continue;
      if (now - ts > 3 * 60 * 1000) continue;
      if (!best || ts > Number(best.campaign.lastSentAt || 0)) {
        best = { key, campaign };
      }
    }
    return best;
  }

  shouldSendEntryConfirm(signal, timeframe) {
    if (!signal || signal.side === 'NO_TRADE') {
      return { ok: false, reason: 'NO_ACTIONABLE_SIGNAL' };
    }
    const isSpecialWatch = this.isSpecialWatchSymbol(signal.symbol);
    if (isSpecialWatch && this.config.trading.specialWatchDisablePreSignal !== false) {
      const minLevel = String(this.config.trading.specialWatchMinLevel || 'MEDIUM').toUpperCase();
      const requiredRank = this.getSignalLevelRank(minLevel);
      const levelRank = this.getSignalLevelRank(signal.level);
      const candleConfirm = signal?.priceAction?.candleConfirm === true;
      if (!candleConfirm) {
        return { ok: false, reason: 'SPECIAL_WATCH_WAIT_CANDLE_CONFIRM', remainingCandles: null };
      }
      if (levelRank < requiredRank) {
        return { ok: false, reason: `SPECIAL_WATCH_LEVEL_TOO_LOW_${signal.level || 'UNKNOWN'}`, remainingCandles: null };
      }
      return { ok: true, reason: 'SPECIAL_WATCH_CANDLE_CONFIRM', remainingCandles: 0 };
    }

    const tf = timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m';
    const side = String(signal.side || '').toUpperCase();
    const pre = signal.preSignal?.selected || null;
    const preConsensus = signal.preSignal?.consensus || null;
    const prePack2 = signal.preSignal?.packs?.pack2 || null;
    const tfSec = this.timeframeToSec(tf);

    if (this.config.trading.preSignalRequireConsensus !== false) {
      if (!preConsensus?.ok) {
        return {
          ok: false,
          reason: preConsensus?.reason || 'WAIT_PACK_CONSENSUS',
          remainingCandles: null,
        };
      }
      if (preConsensus?.side && String(preConsensus.side).toUpperCase() !== side) {
        return {
          ok: false,
          reason: 'PACK_CONSENSUS_SIDE_MISMATCH',
          remainingCandles: null,
        };
      }
    }

    if (this.config.trading.entryConfirmRequirePack2 !== false) {
      if (!prePack2?.ok) {
        return {
          ok: false,
          reason: prePack2?.reason || 'WAIT_PACK2_CONFIRM',
          remainingCandles: null,
        };
      }
      if (prePack2?.side && String(prePack2.side).toUpperCase() !== side) {
        return {
          ok: false,
          reason: 'PACK2_SIDE_MISMATCH',
          remainingCandles: null,
        };
      }
    }

    if (pre && String(pre.side || '').toUpperCase() === side) {
      const remain = Math.max(0, Math.ceil(Number(pre.etaSec || 0) / tfSec));
      if (remain <= 1) {
        return { ok: true, reason: 'PRE_COUNTDOWN_READY', remainingCandles: remain };
      }
      return { ok: false, reason: 'WAIT_PRE_COUNTDOWN', remainingCandles: remain };
    }

    const recent = this.findRecentPreCampaign(signal.symbol, tf, side);
    if (recent && Number(recent.campaign.lastStep || 9) <= 1) {
      return { ok: true, reason: 'PRE_COUNTDOWN_RECENT_READY', remainingCandles: recent.campaign.lastStep };
    }

    if (
      String(signal.level || '').toUpperCase() === 'STRONG' &&
      Number(signal.confidence || 0) >= Number(this.config.trading.autoThreshold || 75) + 2
    ) {
      return { ok: true, reason: 'STRONG_FALLBACK_CONFIRM', remainingCandles: null };
    }

    return { ok: false, reason: 'NO_PRE_CONFIRM_WINDOW' };
  }

  markPreCampaignConfirmed(signal, timeframe, candleTime) {
    if (!signal || signal.side === 'NO_TRADE') return;
    const tf = timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m';
    const side = String(signal.side || '').toUpperCase();
    const now = Date.now();
    const recent = this.findRecentPreCampaign(signal.symbol, tf, side);
    if (!recent) return;
    this.preSignalCountdownMap.set(recent.key, {
      ...recent.campaign,
      confirmedAt: now,
      confirmCandleTime: Number(candleTime || 0),
    });
  }

  async notifyPreSignalCancelIfNeeded(signal, timeframe, candleTime) {
    if (!this.config.telegram.preSignalEnabled) return;
    const tf = timeframe || signal?.signalTimeframe || this.config.timeframe.analysis || '3m';
    const symbol = String(signal?.symbol || '').toUpperCase();
    if (!symbol) return;
    const now = Date.now();
    const prefix = `${symbol}:${tf}:`;

    for (const [key, campaign] of this.preSignalCountdownMap.entries()) {
      if (!key.startsWith(prefix)) continue;
      if (!campaign || campaign.cancelledAt || campaign.confirmedAt) continue;
      const lastSentAt = Number(campaign.lastSentAt || 0);
      if (!lastSentAt || now - lastSentAt > 3 * 60 * 1000) continue;

      let cancelReason = null;
      if (!signal || signal.side === 'NO_TRADE') {
        const rejectCode = String(signal?.rejectCode || signal?.rejectCodes?.[0] || 'NO_TRADE').toUpperCase();
        if (['SIDEWAY', 'LOW_CONFIDENCE', 'BAD_RR', 'HIGH_SPREAD', 'HIGH_SLIPPAGE', 'NO_TRADE'].includes(rejectCode)) {
          cancelReason = `Hủy kèo: điều kiện thị trường đổi (${rejectCode})`;
        }
      } else if (String(signal.side || '').toUpperCase() !== String(campaign.side || '').toUpperCase()) {
        cancelReason = `Hủy kèo: tín hiệu đảo chiều sang ${signal.side}`;
      }

      if (!cancelReason) continue;
      await this.telegramService.sendPreSignalCancel({
        symbol,
        timeframe: tf,
        side: campaign.side,
        reason: cancelReason,
        countdownStep: Number(campaign.lastStep || 0),
        candleTime,
      });
      this.preSignalCountdownMap.set(key, {
        ...campaign,
        cancelledAt: now,
        cancelReason,
      });
    }
  }

  calculatePreSignalLiveStats(signal, pre) {
    const trigger = Number(pre.triggerPrice || 0);
    const sl = Number(pre.stopLoss || 0);
    const tp1 = Number(pre.tp1 || 0);
    const tp2 = Number(pre.tp2 || 0);
    const tp3 = Number(pre.tp3 || 0);
    const leverage = Number(pre.leverage || signal.leverage || this.config.binance.leverage || 1);

    const calcMovePct = (from, to) => {
      if (!Number.isFinite(from) || !Number.isFinite(to) || from <= 0) return 0;
      return Math.abs((to - from) / from);
    };

    const tp1Pct = calcMovePct(trigger, tp1);
    const tp2Pct = calcMovePct(trigger, tp2);
    const tp3Pct = calcMovePct(trigger, tp3);
    const slPct = calcMovePct(trigger, sl);
    const takerFeePct = Number(pre.takerFeePct || signal.takerFeePct || this.config.trading.takerFeePct || 0);
    const feePenaltyPct = takerFeePct * 2 * leverage * 100;

    const market = signal.market || {};
    const pressure = market.pressure || {};
    const candleStrength = Number(pressure.candleStrength || 0);
    const buyPressure = Number(pressure.buyPressure || 0);
    const sellPressure = Number(pressure.sellPressure || 0);
    const sidePressure = pre.side === 'LONG' ? buyPressure - sellPressure : sellPressure - buyPressure;
    const momentumScore = Math.max(0, Math.min(100, 50 + sidePressure * 0.6 + candleStrength * 0.35));
    const winRateNow = Math.max(
      35,
      Math.min(97, Math.round(Number(pre.probability || 0) * 0.62 + momentumScore * 0.38)),
    );

    return {
      leverage,
      takerFeePct,
      targetPnLPct: {
        tp1: tp1Pct * leverage * 100,
        tp2: tp2Pct * leverage * 100,
        tp3: tp3Pct * leverage * 100,
      },
      targetRoiNetPct: {
        tp1: tp1Pct * leverage * 100 - feePenaltyPct,
        tp2: tp2Pct * leverage * 100 - feePenaltyPct,
        tp3: tp3Pct * leverage * 100 - feePenaltyPct,
      },
      feePenaltyPct,
      stopLossRiskPct: slPct * leverage * 100,
      momentumScore,
      winRateNow,
      candleStrength,
    };
  }

  shouldNotifyPreSignal(signal, timeframe, candleTime) {
    if (!this.config.telegram.preSignalEnabled) return false;
    if (!signal) return false;
    if (this.stateStore.state.activePosition) return false;
    if (this.isSpecialWatchSymbol(signal.symbol) && this.config.trading.specialWatchDisablePreSignal !== false) {
      return false;
    }

    const preConsensus = signal.preSignal?.consensus || null;
    if (this.config.trading.preSignalRequireConsensus !== false && !preConsensus?.ok) return false;

    const pre = signal.preSignal?.selected;
    if (!pre) return false;
    if (!['LONG', 'SHORT'].includes(pre.side)) return false;
    if (!['EARLY_WATCH', 'READY_SOON', 'ARMED'].includes(pre.mode)) return false;

    const probability = Number(pre.probability || 0);
    const baseMinProb = Number(this.config.telegram.preSignalMinProbability || 78);
    const modeMinProb = pre.mode === 'EARLY_WATCH' ? baseMinProb + 8 : baseMinProb;
    if (probability < modeMinProb) return false;

    const etaSec = Number(pre.etaSec || 0);
    if (!Number.isFinite(etaSec) || etaSec <= 0) return false;

    const tf = timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m';
    const candleSec = this.timeframeToSec(tf);
    const remainingCandles = Math.max(0, Math.ceil(etaSec / candleSec));
    const leadCandles = this.getPreSignalLeadCandles(tf);
    if (remainingCandles <= 0) return false;
    if (remainingCandles > leadCandles) return false;
    return true;
  }

  async notifyPreSignal(signal, timeframe, candleTime) {
    if (!this.shouldNotifyPreSignal(signal, timeframe, candleTime)) return;
    const pre = signal.preSignal.selected;
    const tf = timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m';
    const tfSec = this.timeframeToSec(tf);
    const leadCandles = this.getPreSignalLeadCandles(tf);
    const remainingCandlesRaw = Math.max(0, Math.ceil(Number(pre.etaSec || 0) / tfSec));
    const step = this.buildPreSignalCountdownStep(remainingCandlesRaw);
    if (step === null) return;
    if (remainingCandlesRaw <= 0) return;
    if (remainingCandlesRaw > leadCandles) return;

    const campaignKey = this.buildPreSignalCampaignKey(signal, tf, pre);
    this.prunePreSignalDispatchMap();
    const campaign = this.preSignalCountdownMap.get(campaignKey) || {
      lastStep: null,
      lastSentAt: 0,
      lastCandleTime: null,
    };
    const now = Date.now();
    const stepCooldown = Math.max(4000, Number(this.config.trading.preSignalEmitIntervalMs || 3500));
    const sameStep = campaign.lastStep === step;
    const sameCandle = Number(campaign.lastCandleTime || 0) === Number(candleTime || 0);

    // Gửi theo nhịp nến: chỉ khi step giảm, hoặc đổi nến.
    if (sameStep && (sameCandle || now - Number(campaign.lastSentAt || 0) < stepCooldown)) {
      return;
    }
    if (Number.isFinite(Number(campaign.lastStep)) && step > Number(campaign.lastStep)) {
      // ETA dao động tăng tạm thời => bỏ qua để tránh nhảy số ngược.
      return;
    }

    const feeRates = await this.orderManager.getFeeRates(signal.symbol);
    const signalWithFee = {
      ...signal,
      takerFeePct: Number(signal.takerFeePct || feeRates.takerFeePct),
    };
    const preWithFee = {
      ...pre,
      leverage: Number(pre.leverage || signalWithFee.leverage || this.config.binance.leverage || 10),
      takerFeePct: Number(pre.takerFeePct || signalWithFee.takerFeePct),
    };
    const liveStats = this.calculatePreSignalLiveStats(signalWithFee, preWithFee);
    if (Number(liveStats.targetRoiNetPct?.tp1 || 0) <= 0) return;
    const remainingCandles = Math.max(0, remainingCandlesRaw);
    await this.telegramService.sendPreSignal({
      symbol: signal.symbol,
      timeframe: tf,
      side: pre.side,
      mode: pre.mode,
      probability: pre.probability,
      triggerPrice: pre.triggerPrice,
      etaSec: pre.etaSec,
      stopLoss: preWithFee.stopLoss,
      tp1: preWithFee.tp1,
      tp2: preWithFee.tp2,
      tp3: preWithFee.tp3,
      rr: preWithFee.rr,
      leverage: preWithFee.leverage,
      takerFeePct: preWithFee.takerFeePct,
      reasons: pre.reasons || [],
      liveStats,
      remainingCandles,
      leadCandles,
      countdownStep: step,
      candleTime,
      packConsensus: signal.preSignal?.consensus || null,
      pack1: signal.preSignal?.packs?.pack1 || null,
      pack2: signal.preSignal?.packs?.pack2 || null,
    });

    this.emitLifecycle(
      'PREPARE',
      {
        symbol: signal.symbol,
        side: pre.side,
        timeframe: tf,
        candleTime,
        source: 'DIRECT_BOT_PRE_SIGNAL',
        tradeId: this.buildTradeId({
          signal,
          timeframe: tf,
          candleTime,
          triggerPrice: pre.triggerPrice,
        }),
        probability: pre.probability,
        mode: pre.mode,
        triggerPrice: pre.triggerPrice,
        remainingCandles,
        countdownStep: step,
        stopLoss: preWithFee.stopLoss,
        tp1: preWithFee.tp1,
        tp2: preWithFee.tp2,
        tp3: preWithFee.tp3,
      },
      { dedupeMs: Math.max(1000, Number(this.config.trading.preSignalEmitIntervalMs || 3500)) },
    );

    this.preSignalCountdownMap.set(campaignKey, {
      symbol: signal.symbol,
      timeframe: tf,
      side: pre.side,
      triggerPrice: Number(pre.triggerPrice || 0),
      lastStep: step,
      lastSentAt: now,
      lastCandleTime: Number(candleTime || 0),
      cancelledAt: null,
      confirmedAt: null,
    });
  }

  async analyzeAndNotifyPreSignalByTimeframe(symbol, timeframe, candleTime) {
    if (!this.config.telegram.preSignalEnabled) return;
    if (!this.getPreSignalTimeframes().includes(timeframe)) return;

    const candles1m = this.stateStore.getCandles(symbol, '1m');
    const candlesTf = this.stateStore.getCandles(symbol, timeframe);
    const candles5m = this.stateStore.getCandles(symbol, '5m');
    const candles15m = this.stateStore.getCandles(symbol, '15m');

    const minAnalysis = Math.max(40, Number(this.config.advanced?.minCandlesAnalysis || 80));
    const minTrend5 = Math.max(40, Number(this.config.advanced?.minCandlesTrend5m || 80));
    const minTrend15 = Math.max(30, Number(this.config.advanced?.minCandlesTrend15m || 40));

    if (!candlesTf || candlesTf.length < minAnalysis) return;
    if (!candles5m || candles5m.length < minTrend5) return;
    if (!candles15m || candles15m.length < minTrend15) return;

    const tfSignal = this.signalEngine.analyze({
      symbol,
      candles1m,
      candles2m: candlesTf,
      candles5m,
      candles15m,
      orderBook: this.stateStore.state.orderBook,
      analysisTimeframe: timeframe,
    });

    await this.notifyPreSignal(tfSignal, timeframe, candleTime);
    await this.notifyPreSignalCancelIfNeeded(tfSignal, timeframe, candleTime);
  }

  getScannerSymbols() {
    const list = Array.isArray(this.config.binance.symbols) ? this.config.binance.symbols : [];
    const normalized = list
      .map((s) => String(s || '').trim().toUpperCase())
      .filter(Boolean);
    if (!normalized.length) return [this.stateStore.state.symbol];
    return normalized;
  }

  initializeDirectBots() {
    const symbols = this.getScannerSymbols();
    const now = Date.now();
    const symbolSet = new Set(symbols);
    for (const existing of this.directBots.keys()) {
      if (!symbolSet.has(existing)) {
        this.directBots.delete(existing);
      }
    }
    for (const symbol of symbols) {
      const current = this.directBots.get(symbol);
      if (current) continue;
      this.directBots.set(symbol, {
        symbol,
        state: 'WARMING',
        timeframe: null,
        inFlight: false,
        analysisCount: 0,
        signalCount: 0,
        rejectCount: 0,
        lastRunAt: now,
        lastSignalAt: 0,
        lastSignal: null,
        lastRejectCode: null,
        lastError: null,
      });
    }
  }

  initializeOutcomeBots() {
    const symbols = this.getScannerSymbols();
    const now = Date.now();
    const symbolSet = new Set(symbols);
    for (const existing of this.outcomeBots.keys()) {
      if (!symbolSet.has(existing)) {
        this.outcomeBots.delete(existing);
      }
    }
    for (const symbol of symbols) {
      const current = this.outcomeBots.get(symbol);
      if (current) continue;
      this.outcomeBots.set(symbol, {
        symbol,
        state: 'WARMING',
        timeframe: null,
        checkCount: 0,
        openTracks: 0,
        resolvedWins: 0,
        resolvedLosses: 0,
        resolvedExpired: 0,
        lastRunAt: now,
        lastResolveAt: 0,
        lastOutcome: null,
        lastError: null,
        lastCandleKey: null,
      });
    }
  }

  setOutcomeBotState(symbol, patch = {}) {
    const base =
      this.outcomeBots.get(symbol) ||
      {
        symbol,
        state: 'IDLE',
        timeframe: null,
        checkCount: 0,
        openTracks: 0,
        resolvedWins: 0,
        resolvedLosses: 0,
        resolvedExpired: 0,
        lastRunAt: 0,
        lastResolveAt: 0,
        lastOutcome: null,
        lastError: null,
        lastCandleKey: null,
      };
    this.outcomeBots.set(symbol, { ...base, ...patch });
  }

  syncOutcomeBotOpenCount(symbol, extra = {}) {
    const symbolNorm = String(symbol || '').toUpperCase();
    if (!symbolNorm) return;

    const snapshot = this.stateStore.getSignalOutcomeSnapshot();
    const openTracks = Object.values(snapshot.active || {}).filter(
      (track) => String(track?.symbol || '').toUpperCase() === symbolNorm,
    ).length;

    const prev = this.outcomeBots.get(symbolNorm) || {};
    const now = Number(extra.now || Date.now());
    const resolvedWins = Number(prev.resolvedWins || 0) + Number(extra.resolvedWins || 0);
    const resolvedLosses = Number(prev.resolvedLosses || 0) + Number(extra.resolvedLosses || 0);
    const resolvedExpired = Number(prev.resolvedExpired || 0) + Number(extra.resolvedExpired || 0);

    this.setOutcomeBotState(symbolNorm, {
      symbol: symbolNorm,
      state: openTracks > 0 ? 'TRACKING' : 'IDLE',
      timeframe: extra.timeframe || prev.timeframe || null,
      checkCount: Number(prev.checkCount || 0) + 1,
      openTracks,
      resolvedWins,
      resolvedLosses,
      resolvedExpired,
      lastRunAt: now,
      lastResolveAt: extra.latestOutcome ? now : Number(prev.lastResolveAt || 0),
      lastOutcome: extra.latestOutcome || prev.lastOutcome || null,
      lastError: null,
      lastCandleKey: extra.lastCandleKey || prev.lastCandleKey || null,
      processed: Number(extra.processed || 0),
    });
  }

  classifyOutcomeBotHealth(bot, now = Date.now()) {
    if (!bot) return 'INACTIVE';
    if (String(bot.state || '').toUpperCase() === 'ERROR') return 'INACTIVE';
    const lastRunAt = Number(bot.lastRunAt || 0);
    if (!lastRunAt) return 'INACTIVE';
    const staleMs = this.getDirectBotStaleMs();
    if (now - lastRunAt > staleMs) return 'STALLED';
    return 'ACTIVE';
  }

  getOutcomeBotHealthSnapshot() {
    this.initializeOutcomeBots();
    const symbols = this.getScannerSymbols();
    const now = Date.now();
    const staleMs = this.getDirectBotStaleMs();
    const bots = symbols
      .map((symbol) => this.outcomeBots.get(symbol))
      .filter(Boolean)
      .map((bot) => {
        const health = this.classifyOutcomeBotHealth(bot, now);
        return {
          ...bot,
          health,
          staleForMs: bot.lastRunAt ? Math.max(0, now - bot.lastRunAt) : null,
        };
      });
    const activeBots = bots.filter((b) => b.health === 'ACTIVE');
    const stalledBots = bots.filter((b) => b.health === 'STALLED');
    const inactiveBots = bots.filter((b) => b.health === 'INACTIVE');
    return {
      staleMs,
      bots,
      activeBots,
      stalledBots,
      inactiveBots,
      activeCount: activeBots.length,
      stalledCount: stalledBots.length,
      inactiveCount: inactiveBots.length,
    };
  }

  setDirectBotState(symbol, patch = {}) {
    const base =
      this.directBots.get(symbol) ||
      {
        symbol,
        state: 'IDLE',
        timeframe: null,
        inFlight: false,
        analysisCount: 0,
        signalCount: 0,
        rejectCount: 0,
        lastRunAt: 0,
        lastSignalAt: 0,
        lastSignal: null,
        lastRejectCode: null,
        lastError: null,
      };
    this.directBots.set(symbol, { ...base, ...patch });
  }

  updateDirectBotFromSignal(symbol, timeframe, signal, candleTime) {
    const now = Date.now();
    const isActionable = signal && signal.side && signal.side !== 'NO_TRADE';
    this.setDirectBotState(symbol, {
      timeframe,
      state: isActionable ? 'SIGNAL' : 'NO_TRADE',
      inFlight: false,
      analysisCount: (this.directBots.get(symbol)?.analysisCount || 0) + 1,
      signalCount: (this.directBots.get(symbol)?.signalCount || 0) + (isActionable ? 1 : 0),
      rejectCount:
        (this.directBots.get(symbol)?.rejectCount || 0) + (isActionable ? 0 : 1),
      lastRunAt: now,
      lastSignalAt: now,
      lastSignal: signal
        ? {
            side: signal.side,
            confidence: signal.confidence,
            timeframe,
            candleTime,
            entryPrice: signal.entryPrice,
            stopLoss: signal.stopLoss,
            tp1: signal.tp1,
            tp2: signal.tp2,
            tp3: signal.tp3,
            rejectCode: signal.rejectCode || null,
          }
        : null,
      lastRejectCode: signal?.rejectCode || null,
      lastError: null,
    });
  }

  getScannerTimeframes() {
    return ['1m', '3m', '5m', '15m'];
  }

  async bootstrapScannerSymbols() {
    if (!this.config.binance.scannerEnabled) return;
    if (Date.now() < this.restGeoBlockedUntil) return;
    const symbols = this.getScannerSymbols();
    const intervals = ['1m', '3m', '5m', '15m'];
    const batchSize = Math.max(1, Number(this.config.binance.scannerBootstrapBatchSize || 2));
    const pauseMs = Math.max(0, Number(this.config.binance.scannerBootstrapDelayMs || 220));
    const candleLimit = Math.max(220, Number(this.config.binance.scannerCandleLimit || 260));

    for (let i = 0; i < symbols.length; i += batchSize) {
      if (Date.now() < this.restGeoBlockedUntil) break;
      const batch = symbols.slice(i, i + batchSize);
      await Promise.allSettled(
        batch.map(async (symbol) => {
          const datasets = await Promise.allSettled(
            intervals.map(async (tf) => ({
              tf,
              candles: await this.fetchKlines(symbol, tf, candleLimit),
            })),
          );

          for (const row of datasets) {
            if (row.status !== 'fulfilled') continue;
            const item = row.value;
            this.stateStore.setCandles(symbol, item.tf, item.candles, this.config.timeframe.limit);
          }
        }),
      );
      if (pauseMs > 0 && i + batchSize < symbols.length) {
        await new Promise((resolve) => setTimeout(resolve, pauseMs));
      }
    }
  }

  startMultiSymbolScanner() {
    if (this.multiScanTimer) return;
    if (!this.config.binance.scannerEnabled) return;
    const loopMs = Math.max(3000, Number(this.config.binance.scannerLoopMs || 12000));
    this.multiScanTimer = setInterval(() => {
      this.runMultiSymbolScan().catch((error) => {
        if (!this.isTransientRestError(error)) {
          this.logger.warn('bot', 'Lỗi scanner đa coin', { error: error.message });
        }
      });
    }, loopMs);
  }

  stopMultiSymbolScanner() {
    if (!this.multiScanTimer) return;
    clearInterval(this.multiScanTimer);
    this.multiScanTimer = null;
  }

  getDirectBotStaleMs() {
    const configured = Number(this.config.binance.directBotStaleMs || 60000);
    const symbols = this.getScannerSymbols();
    const batch = Math.max(1, Number(this.config.binance.scannerBatchSize || 1));
    const loopMs = Math.max(3000, Number(this.config.binance.scannerLoopMs || 8000));
    const cycleMs = loopMs * Math.max(1, Math.ceil(symbols.length / batch));
    const floor = Math.max(20000, cycleMs * 2);
    return Math.max(floor, configured);
  }

  classifyDirectBotHealth(bot, now = Date.now()) {
    if (!bot) return 'INACTIVE';
    if (bot.inFlight) return 'ACTIVE';
    if (bot.state === 'ERROR') return 'INACTIVE';
    const lastRunAt = Number(bot.lastRunAt || 0);
    if (!lastRunAt) return 'INACTIVE';
    const staleMs = this.getDirectBotStaleMs();
    if (now - lastRunAt > staleMs) return 'STALLED';
    return 'ACTIVE';
  }

  getDirectBotHealthSnapshot() {
    this.initializeDirectBots();
    const symbols = this.getScannerSymbols();
    const now = Date.now();
    const staleMs = this.getDirectBotStaleMs();
    const bots = symbols
      .map((symbol) => this.directBots.get(symbol))
      .filter(Boolean)
      .map((bot) => {
        const health = this.classifyDirectBotHealth(bot, now);
        return {
          ...bot,
          health,
          staleForMs: bot.lastRunAt ? Math.max(0, now - bot.lastRunAt) : null,
        };
      });
    const activeBots = bots.filter((b) => b.health === 'ACTIVE');
    const stalledBots = bots.filter((b) => b.health === 'STALLED');
    const inactiveBots = bots.filter((b) => b.health === 'INACTIVE');
    return {
      staleMs,
      bots,
      activeBots,
      stalledBots,
      inactiveBots,
      activeCount: activeBots.length,
      stalledCount: stalledBots.length,
      inactiveCount: inactiveBots.length,
    };
  }

  startDirectBotWatchdog() {
    if (this.directBotWatchdogTimer) return;
    if (!this.config.binance.scannerEnabled) return;
    if (!this.config.binance.directBotWatchdogEnabled) return;

    const watchdogMs = Math.max(5000, Number(this.config.binance.directBotWatchdogMs || 15000));
    setTimeout(() => {
      this.recoverInactiveDirectBots().catch((error) => {
        this.logger.warn('bot', 'Watchdog warmup lỗi', { error: error.message });
      });
    }, 1200);
    this.directBotWatchdogTimer = setInterval(() => {
      this.recoverInactiveDirectBots().catch((error) => {
        this.logger.warn('bot', 'Watchdog direct bots lỗi', { error: error.message });
      });
    }, watchdogMs);
  }

  stopDirectBotWatchdog() {
    if (!this.directBotWatchdogTimer) return;
    clearInterval(this.directBotWatchdogTimer);
    this.directBotWatchdogTimer = null;
  }

  startOutcomeBotWatchdog() {
    if (this.outcomeBotWatchdogTimer) return;
    if (!this.config.binance.scannerEnabled) return;
    if (!this.config.binance.directBotWatchdogEnabled) return;

    const watchdogMs = Math.max(5000, Number(this.config.binance.directBotWatchdogMs || 15000));
    this.outcomeBotWatchdogTimer = setInterval(() => {
      this.recoverInactiveOutcomeBots().catch((error) => {
        this.logger.warn('bot', 'Watchdog outcome bots lỗi', { error: error.message });
      });
    }, watchdogMs);
  }

  stopOutcomeBotWatchdog() {
    if (!this.outcomeBotWatchdogTimer) return;
    clearInterval(this.outcomeBotWatchdogTimer);
    this.outcomeBotWatchdogTimer = null;
  }

  async recoverInactiveOutcomeBots() {
    if (!this.config.binance.scannerEnabled) return;
    this.outcomeBotLastWatchdogAt = Date.now();
    this.initializeOutcomeBots();
    const health = this.getOutcomeBotHealthSnapshot();
    const recoverPerTick = Math.max(1, Number(this.config.binance.directBotRecoverPerTick || 2));
    const targets = health.stalledBots.slice(0, recoverPerTick);
    if (!targets.length) return;

    for (const bot of targets) {
      const symbol = String(bot.symbol || '').toUpperCase();
      if (!symbol) continue;
      for (const tf of this.getScannerTimeframes()) {
        const candles = this.stateStore.getCandles(symbol, tf).filter((x) => x?.isClosed);
        const lastClosed = candles[candles.length - 1];
        if (!lastClosed) continue;
        this.evaluateSignalOutcomesByCandle(symbol, tf, lastClosed);
      }
      this.outcomeBotRecoveryCount += 1;
    }
  }

  startScannerWsHealthMonitor() {
    this.stopScannerWsHealthMonitor();
    const tickMs = 5000;
    this.scannerWsHealthTimer = setInterval(() => {
      if (!this.scannerWsConnected || !this.scannerWs || this.scannerWsManualClose) return;
      const staleFor = Date.now() - Number(this.scannerWsLastMessageAt || 0);
      const staleLimit = Math.max(20000, Number(this.config.websocket.staleDataMs || 12000) * 2);
      if (staleFor > staleLimit) {
        this.logger.warn('scanner-ws', 'Scanner WS stale, terminate để reconnect', {
          staleForMs: staleFor,
          staleLimitMs: staleLimit,
        });
        try {
          this.scannerWs.terminate();
        } catch (_) {
          // ignore
        }
      }
    }, tickMs);
  }

  stopScannerWsHealthMonitor() {
    if (!this.scannerWsHealthTimer) return;
    clearInterval(this.scannerWsHealthTimer);
    this.scannerWsHealthTimer = null;
  }

  buildScannerWsUrl() {
    const symbols = this.getScannerSymbols();
    const tfs = this.getScannerTimeframes();
    const streams = [];
    for (const symbol of symbols) {
      const s = String(symbol || '').toLowerCase();
      if (!s) continue;
      for (const tf of tfs) {
        streams.push(`${s}@kline_${tf}`);
      }
    }
    const base = String(this.config.binance.wsBaseUrl || '');
    const sep = base.includes('?') ? '&' : '?';
    return `${base}${sep}streams=${streams.join('/')}`;
  }

  connectScannerMarketStream() {
    if (!this.config.binance.scannerEnabled) return;
    const symbols = this.getScannerSymbols();
    if (!symbols.length) return;

    if (this.scannerWsReconnectTimer) {
      clearTimeout(this.scannerWsReconnectTimer);
      this.scannerWsReconnectTimer = null;
    }
    this.stopScannerWsHealthMonitor();
    if (this.scannerWs) {
      this.scannerWsManualClose = true;
      try {
        this.scannerWs.terminate();
      } catch (_) {
        // ignore
      }
      this.scannerWs = null;
    }
    this.scannerWsManualClose = false;

    const url = this.buildScannerWsUrl();
    this.scannerWs = new WebSocket(url);

    this.scannerWs.on('open', () => {
      this.scannerWsConnected = true;
      this.scannerWsReconnectAttempt = 0;
      this.scannerWsLastMessageAt = Date.now();
      this.logger.info('scanner-ws', 'Đã kết nối scanner WS đa coin', {
        symbols: symbols.length,
        timeframes: this.getScannerTimeframes(),
      });
      this.startScannerWsHealthMonitor();
      this.emitEvent('status', this.getStatus());
    });

    this.scannerWs.on('message', (raw) => {
      this.scannerWsLastMessageAt = Date.now();
      this.handleScannerWsMessage(String(raw || ''));
    });

    this.scannerWs.on('close', () => {
      this.scannerWsConnected = false;
      this.stopScannerWsHealthMonitor();
      this.emitEvent('status', this.getStatus());
      if (!this.scannerWsManualClose) {
        this.scheduleScannerWsReconnect();
      }
    });

    this.scannerWs.on('error', (error) => {
      this.logger.warn('scanner-ws', 'Scanner WS lỗi', { error: error.message });
    });
  }

  scheduleScannerWsReconnect() {
    if (this.scannerWsReconnectTimer) return;
    this.scannerWsReconnectAttempt += 1;
    const delay = Math.min(2000 * this.scannerWsReconnectAttempt, 20000);
    this.scannerWsReconnectTimer = setTimeout(() => {
      this.scannerWsReconnectTimer = null;
      if (!this.scannerWsManualClose) {
        this.connectScannerMarketStream();
      }
    }, delay);
  }

  handleScannerWsMessage(payload) {
    let parsed = null;
    try {
      parsed = JSON.parse(payload);
    } catch (_) {
      return;
    }
    const stream = String(parsed?.stream || '');
    const body = parsed?.data;
    if (!stream || !body) return;

    const match = stream.match(/@kline_(\d+m)$/);
    if (!match) return;
    const timeframe = match[1];
    const k = body.k;
    if (!k) return;

    const symbol = String(k.s || body.s || stream.split('@')[0] || '').toUpperCase();
    if (!symbol) return;

    const candle = {
      symbol,
      time: Math.floor(Number(k.t) / 1000),
      open: Number(k.o),
      high: Number(k.h),
      low: Number(k.l),
      close: Number(k.c),
      volume: Number(k.v),
      quoteVolume: Number(k.q || 0),
      tradeCount: Number(k.n || 0),
      takerBuyBaseVolume: Number(k.V || 0),
      takerBuyQuoteVolume: Number(k.Q || 0),
      closeTime: Number(k.T),
      isClosed: Boolean(k.x),
      eventTime: Number(body.E || 0),
      latencyMs: body.E ? Date.now() - Number(body.E) : 0,
      timeframe,
    };
    if (!Number.isFinite(candle.time)) return;

    this.stateStore.addCandle(symbol, timeframe, candle, this.config.timeframe.limit);
    this.setDirectBotState(symbol, {
      state: 'STREAMING',
      lastRunAt: Date.now(),
      lastError: null,
    });

    if (candle.isClosed) {
      this.scanDirectSymbolFromState(symbol, {
        source: 'SCANNER_WS',
        timeframes: [timeframe],
        dispatchSignals: true,
      }).catch((error) => {
        this.logger.warn('scanner-ws', 'Lỗi scan từ scanner WS', {
          symbol,
          timeframe,
          error: error.message,
        });
      });
    }
  }

  async scanDirectSymbolFromState(symbol, options = {}) {
    const tfs = Array.isArray(options.timeframes) && options.timeframes.length
      ? options.timeframes
      : this.getScannerTimeframes();
    const dispatchSignals = options.dispatchSignals !== false;
    const forceAnalyze = options.forceAnalyze === true;

    const byTf = {
      '1m': this.stateStore.getCandles(symbol, '1m'),
      '3m': this.stateStore.getCandles(symbol, '3m'),
      '5m': this.stateStore.getCandles(symbol, '5m'),
      '15m': this.stateStore.getCandles(symbol, '15m'),
    };

    let analyzed = false;
    for (const tf of tfs) {
      const tfCandles = byTf[tf] || [];
      const closed = tfCandles.filter((c) => c.isClosed);
      if (!closed.length) continue;
      const lastClosed = closed[closed.length - 1];
      const closeKey = `${symbol}:${tf}`;
      const prevClose = this.multiScanLastCloseByKey.get(closeKey) || 0;
      if (!forceAnalyze && lastClosed.time <= prevClose) continue;
      this.multiScanLastCloseByKey.set(closeKey, lastClosed.time);
      this.evaluateSignalOutcomesByCandle(symbol, tf, lastClosed);
      if (String(tf || '').toLowerCase() === '1m') {
        await this.evaluateSignalFollowTrack(symbol, null, lastClosed);
      } else {
        await this.evaluateSignalFollowTrack(symbol, tf, lastClosed);
      }

      const signal = this.signalEngine.analyze({
        symbol,
        candles1m: byTf['1m'] || [],
        candles2m: byTf[tf] || [],
        candles5m: byTf['5m'] || [],
        candles15m: byTf['15m'] || [],
        orderBook: this.stateStore.state.orderBook,
        analysisTimeframe: tf,
      });

      this.stateStore.setMarket(symbol, signal.market || {});
      this.updateDirectBotFromSignal(symbol, tf, signal, lastClosed.time);
      await this.notifyPreSignal(signal, tf, lastClosed.time);
      await this.notifyPreSignalCancelIfNeeded(signal, tf, lastClosed.time);
      if (dispatchSignals) {
        await this.dispatchActionableScanSignal(signal, tf, lastClosed.time);
      }
      analyzed = true;
    }

    if (!analyzed) {
      this.setDirectBotState(symbol, {
        inFlight: false,
        state: 'IDLE',
        lastRunAt: Date.now(),
      });
    }
  }

  detectMarketRegime(signal) {
    const market = signal?.market || {};
    if (market.sideway || market.trend === 'SIDEWAY') return 'SIDEWAY';
    const adx = Number(market.adx2m || 0);
    const spreadPct = Number(market.spreadPct || 0);
    const bbBandwidth = Number(market.bbBandwidth2m || 0);
    const pressure = market.pressure || {};
    const priceSpeed = Math.abs(Number(pressure.priceSpeed || 0));
    const trendAdxMin = Number(this.config.trading.regimeTrendAdxMin || 22);
    const noisySpreadMul = Number(this.config.trading.regimeNoisySpreadMul || 0.9);
    const squeezeMax = Number(this.config.trading.bbSqueezeMax || 0.0045);

    if (
      adx >= trendAdxMin &&
      spreadPct <= Number(this.config.trading.maxSpreadPct || 0.001) * 0.7 &&
      bbBandwidth > squeezeMax * 1.1
    ) {
      return 'TREND_STRONG';
    }

    if (
      spreadPct >= Number(this.config.trading.maxSpreadPct || 0.001) * noisySpreadMul ||
      bbBandwidth <= squeezeMax ||
      priceSpeed < 0.00008
    ) {
      return 'NOISY';
    }

    return 'NORMAL';
  }

  applySignalQualityDecay(signal, timeframe, candleTime) {
    if (!signal || signal.side === 'NO_TRADE') return { signal, dropped: false };
    if (this.config.trading.qualityDecayEnabled === false) return { signal, dropped: false };

    const tf = String(timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m');
    const tfSec = Math.max(60, this.timeframeToSec(tf));
    const candleTsSec = Number(candleTime || Math.floor(Date.now() / 1000));
    const entryBand = signal.entryPrice
      ? Math.round(signal.entryPrice / Math.max(signal.entryPrice * 0.0005, 0.5))
      : 0;
    const setupKey = `${signal.symbol}:${tf}:${signal.side}:${entryBand}`;

    const prev = this.signalAgeMap.get(setupKey) || {
      firstSeenCandleSec: candleTsSec,
      lastSeenCandleSec: candleTsSec,
      hits: 0,
    };
    const next = {
      firstSeenCandleSec: prev.firstSeenCandleSec || candleTsSec,
      lastSeenCandleSec: candleTsSec,
      hits: Number(prev.hits || 0) + 1,
    };
    this.signalAgeMap.set(setupKey, next);

    if (this.signalAgeMap.size > 8000) {
      const cutoff = candleTsSec - 6 * 60 * 60;
      for (const [k, v] of this.signalAgeMap.entries()) {
        if (Number(v?.lastSeenCandleSec || 0) < cutoff) this.signalAgeMap.delete(k);
      }
    }

    const ageCandles = Math.max(
      0,
      Math.floor((candleTsSec - Number(next.firstSeenCandleSec || candleTsSec)) / tfSec),
    );
    const expireCandles = Math.max(2, Number(this.config.trading.qualityDecayExpireCandles || 6));
    if (ageCandles >= expireCandles) {
      return { signal: null, dropped: true, reason: `QUALITY_DECAY_EXPIRED_${ageCandles}` };
    }

    const perCandle = Math.max(0, Number(this.config.trading.qualityDecayPerCandle || 1.5));
    const maxPenalty = Math.max(0, Number(this.config.trading.qualityDecayMaxPenalty || 18));
    const penalty = Math.min(maxPenalty, ageCandles * perCandle);
    if (penalty <= 0) return { signal, dropped: false };

    const nextSignal = { ...signal };
    nextSignal.confidence = Math.max(0, Number(signal.confidence || 0) - penalty);
    nextSignal.qualityDecay = {
      enabled: true,
      ageCandles,
      penalty,
      expireCandles,
    };
    nextSignal.confidenceBreakdown = {
      ...(signal.confidenceBreakdown || {}),
      qualityDecayPenalty: -penalty,
      finalScore: nextSignal.confidence,
    };
    return { signal: nextSignal, dropped: false };
  }

  simulateExecutionGate(signal) {
    if (!signal || signal.side === 'NO_TRADE') return { ok: false, reason: 'NO_ACTIONABLE_SIGNAL' };
    if (this.config.trading.executionSimEnabled === false) return { ok: true, score: 100 };

    const ob = this.stateStore.state.orderBook || {};
    const spreadPct = Number(ob.spreadPct || signal.market?.spreadPct || 0);
    const estimatedSlippagePct = Number(this.estimateSlippage(signal) || 0);
    const entry = Number(signal.entryPrice || 0);
    const tp1 = Number(signal.tp1 || 0);
    const lev = Math.max(1, Number(signal.leverage || this.config.binance.leverage || 10));
    const takerFeePct = Math.max(0, Number(signal.takerFeePct || this.config.trading.takerFeePct || 0.0004));
    const liquidityScore = Number(signal.market?.pressure?.liquidityScore || 0);

    if (!(entry > 0 && tp1 > 0)) {
      return { ok: false, reason: 'EXEC_SIM_INVALID_PLAN' };
    }

    const grossRoiPct = (Math.abs(tp1 - entry) / entry) * lev * 100;
    const feeRoiPct = takerFeePct * 2 * lev * 100;
    const frictionPct = (spreadPct + estimatedSlippagePct) * lev * 100;
    const depthPenalty = Math.max(0, (Number(this.config.trading.executionSimMinLiquidity || 35) - liquidityScore) * 0.05);
    const passScore = grossRoiPct - feeRoiPct - frictionPct - depthPenalty;
    const minPassScore = Number(this.config.trading.executionSimMinPassScore || 0.3);

    if (liquidityScore < Number(this.config.trading.executionSimMinLiquidity || 35)) {
      return {
        ok: false,
        reason: 'EXEC_SIM_LOW_LIQUIDITY',
        score: passScore,
        details: { spreadPct, estimatedSlippagePct, liquidityScore, grossRoiPct, feeRoiPct, frictionPct },
      };
    }
    if (passScore < minPassScore) {
      return {
        ok: false,
        reason: 'EXEC_SIM_NEGATIVE_EDGE',
        score: passScore,
        details: { spreadPct, estimatedSlippagePct, liquidityScore, grossRoiPct, feeRoiPct, frictionPct },
      };
    }

    return {
      ok: true,
      score: passScore,
      details: { spreadPct, estimatedSlippagePct, liquidityScore, grossRoiPct, feeRoiPct, frictionPct },
    };
  }

  async routeSignalLifecycle(signal, timeframe, candleTime, options = {}) {
    if (!signal || signal.side === 'NO_TRADE') return { ok: false, reason: 'NO_TRADE' };

    const tf = String(timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m');
    const tradeId = this.buildTradeId({
      signal,
      timeframe: tf,
      candleTime,
      triggerPrice: signal.entryPrice,
    });
    const basePayload = {
      ...signal,
      tradeId,
      timeframe: tf,
      candleTime,
      source: options.source || signal.source || 'DIRECT_BOT',
    };

    this.emitLifecycle('PREPARE', basePayload, { dedupeMs: 800 });

    const decayResult = this.applySignalQualityDecay(basePayload, tf, candleTime);
    if (decayResult.dropped || !decayResult.signal) {
      this.emitLifecycle(
        'RISK_ALERT',
        {
          ...basePayload,
          reason: decayResult.reason || 'QUALITY_DECAY_DROP',
          riskTag: 'QUALITY_DECAY',
        },
        { dedupeMs: 1200 },
      );
      return { ok: false, reason: decayResult.reason || 'QUALITY_DECAY_DROP' };
    }
    const signalAfterDecay = decayResult.signal;

    const entryGate = options.entryGate || this.shouldSendEntryConfirm(signalAfterDecay, tf);
    if (!entryGate.ok) {
      return { ok: false, reason: entryGate.reason || 'WAIT_ENTRY_CONFIRM' };
    }
    this.emitLifecycle(
      'RECONFIRM_ENTRY',
      {
        ...signalAfterDecay,
        tradeId,
        timeframe: tf,
        candleTime,
        reconfirmReason: entryGate.reason,
      },
      { dedupeMs: 800 },
    );

    const totalGate = options.totalGate || this.runTotalBrainGate(signalAfterDecay, tf);
    if (!totalGate.ok) {
      this.emitLifecycle(
        'RISK_ALERT',
        {
          ...signalAfterDecay,
          tradeId,
          timeframe: tf,
          candleTime,
          reason: totalGate.reason,
          riskTag: 'TOTAL_BRAIN_REJECT',
        },
        { dedupeMs: 1000 },
      );
      return { ok: false, reason: totalGate.reason };
    }

    const payload = {
      ...signalAfterDecay,
      tradeId,
      signalTimeframe: tf,
      entryConfirm: true,
      entryConfirmReason: entryGate.reason,
      regimeLabel: totalGate.regime,
      totalBrainMinScore: totalGate.minScore,
      source: options.source || signalAfterDecay.source || 'DIRECT_BOT',
    };

    const liveQuote = await this.fetchLatestPrice(payload.symbol, { priority: true, maxAgeMs: 1200 });
    const livePrice =
      Number(liveQuote?.price || payload.market?.currentPrice || payload.entryPrice || 0) || 0;
    const entryPrice = Number(payload.entryPrice || 0);
    const liveDeviationPct =
      livePrice > 0 && entryPrice > 0 ? Math.abs((livePrice - entryPrice) / entryPrice) : 0;
    payload.livePriceAtSend = livePrice > 0 ? livePrice : null;
    payload.livePriceAtSendTs = Number(liveQuote?.ts || Date.now());
    payload.livePriceSource = liveQuote?.source || 'SIGNAL_CANDLE';
    payload.entryReference = 'CLOSED_CANDLE';
    payload.liveDeviationPct = liveDeviationPct;
    const maxDeviation = Math.max(0.0005, Number(this.config.trading.maxEntryDeviationPct || 0.0018));
    if (liveDeviationPct > maxDeviation * 1.8) {
      this.emitLifecycle(
        'RISK_ALERT',
        {
          ...payload,
          timeframe: tf,
          candleTime,
          reason: `ENTRY_MOVED_AWAY_${(liveDeviationPct * 100).toFixed(3)}%`,
          riskTag: 'ENTRY_DEVIATION',
        },
        { dedupeMs: 1200 },
      );
      return { ok: false, reason: 'ENTRY_MOVED_AWAY' };
    }

    const broadcastDecision = this.getSignalBroadcastDecision(payload, tf, candleTime);
    if (broadcastDecision.suppress) {
      return { ok: false, reason: broadcastDecision.reason || 'SIGNAL_UPDATE_SUPPRESSED' };
    }

    this.markPreCampaignConfirmed(payload, tf, candleTime);

    let snapshotPath = null;
    if (
      options.allowSnapshot &&
      payload.level === 'STRONG' &&
      this.config.chartSnapshot.sendOnStrongSignal
    ) {
      snapshotPath = await this.chartSnapshot.capture(payload, 'strong-signal');
    }
    if (broadcastDecision.isUpdate && typeof this.telegramService.sendSignalUpdate === 'function') {
      await this.telegramService.sendSignalUpdate(payload, broadcastDecision.changes, snapshotPath);
      this.emitLifecycle(
        'ENTRY_UPDATE',
        {
          ...payload,
          tradeId,
          event: 'ENTRY_SIGNAL_UPDATED',
          changes: broadcastDecision.changes,
        },
        { dedupeMs: 1000 },
      );
    } else {
      await this.telegramService.sendSignal(payload, snapshotPath);
      this.emitLifecycle(
        'ENTRY',
        {
          ...payload,
          tradeId,
          event: 'ENTRY_SIGNAL_SENT',
        },
        { dedupeMs: 1000 },
      );
    }

    this.commitSignalBroadcast(payload, tf, candleTime);
    this.upsertSignalFollowTrack(payload, tf, candleTime);
    this.registerSignalOutcomeTrackFromSignal(payload, tf, candleTime, {
      source: payload.source || 'TOTAL_BRAIN',
      isUpdate: Boolean(broadcastDecision.isUpdate),
    });
    return { ok: true, tradeId, signal: payload, update: Boolean(broadcastDecision.isUpdate) };
  }

  runTotalBrainGate(signal, timeframe) {
    if (!signal || signal.side === 'NO_TRADE') {
      return { ok: false, reason: 'NO_TRADE' };
    }
    const isSpecialWatch =
      this.isSpecialWatchSymbol(signal.symbol) && this.config.trading.specialWatchDisablePreSignal !== false;
    const preConsensus = signal.preSignal?.consensus || null;
    const prePack2 = signal.preSignal?.packs?.pack2 || null;
    if (!isSpecialWatch && this.config.trading.preSignalRequireConsensus !== false && !preConsensus?.ok) {
      return { ok: false, reason: preConsensus?.reason || 'TOTAL_BRAIN_WAIT_PACK_CONSENSUS' };
    }
    if (!isSpecialWatch && this.config.trading.entryConfirmRequirePack2 !== false && !prePack2?.ok) {
      return { ok: false, reason: prePack2?.reason || 'TOTAL_BRAIN_WAIT_PACK2' };
    }

    const hardRejectSet = new Set([
      'SIDEWAY',
      'BAD_RR',
      'LOW_CONFIDENCE',
      'HIGH_SPREAD',
      'HIGH_SLIPPAGE',
      'HIGH_LATENCY',
      'INSUFFICIENT_DATA',
    ]);
    const rejectCodes = Array.isArray(signal.rejectCodes) ? signal.rejectCodes : [];
    const hardCode = rejectCodes.find((c) => hardRejectSet.has(String(c || '').toUpperCase()));
    if (hardCode) {
      return { ok: false, reason: `TOTAL_BRAIN_REJECT_${hardCode}` };
    }

    const regime = this.detectMarketRegime(signal);
    if (regime === 'SIDEWAY') {
      return { ok: false, reason: 'TOTAL_BRAIN_REJECT_SIDEWAY_REGIME', regime };
    }

    const tfBoost = timeframe === '15m' ? 0 : 2;
    let minScore = Math.max(55, Number(this.config.trading.signalThreshold || 56) + tfBoost);
    if (regime === 'TREND_STRONG') minScore = Math.max(52, minScore - 2);
    if (regime === 'NOISY') minScore = Math.min(95, minScore + 6);
    if (Number(signal.confidence || 0) < minScore) {
      return { ok: false, reason: `TOTAL_BRAIN_LOW_SCORE_${signal.confidence}`, regime, minScore };
    }

    return { ok: true, regime, minScore };
  }

  getAdaptiveSetupWeight(signal, timeframe) {
    if (!this.config.trading.adaptiveSetupWeightEnabled) {
      return { adjustment: 0, count: 0, setupKey: null };
    }
    const historyAll = Array.isArray(this.stateStore.state.risk?.tradeHistory)
      ? this.stateStore.state.risk.tradeHistory
      : [];
    const lookback = Math.max(20, Number(this.config.trading.adaptiveSetupLookbackTrades || 260));
    const history = historyAll.slice(0, lookback);

    const side = String(signal?.side || '').toUpperCase();
    const tf = String(timeframe || signal?.signalTimeframe || this.config.timeframe.analysis || '3m');
    const setupType = String(signal?.level || signal?.setupType || 'UNKNOWN').toUpperCase();
    const regime = String(signal?.volatilityRegime || signal?.market?.volatilityRegime || 'UNKNOWN').toUpperCase();
    const setupKey = `${setupType}:${tf}:${side}:${regime}`;

    const matched = history.filter((t) => {
      const tSide = String(t?.side || '').toUpperCase();
      const tTf = String(t?.timeframe || '').toLowerCase();
      const tSetup = String(t?.setupType || 'UNKNOWN').toUpperCase();
      const tRegime = String(t?.regime || 'UNKNOWN').toUpperCase();
      return tSide === side && tTf === String(tf).toLowerCase() && tSetup === setupType && tRegime === regime;
    });

    const minTrades = Math.max(3, Number(this.config.trading.adaptiveSetupMinTrades || 10));
    if (matched.length < minTrades) {
      return { adjustment: 0, count: matched.length, setupKey };
    }

    const wins = matched.filter((t) => Number(t?.pnl || 0) > 0);
    const winRate = (wins.length / matched.length) * 100;
    const grossWin = wins.reduce((sum, t) => sum + Number(t?.pnl || 0), 0);
    const grossLossAbs = matched
      .filter((t) => Number(t?.pnl || 0) < 0)
      .reduce((sum, t) => sum + Math.abs(Number(t?.pnl || 0)), 0);
    const profitFactor = grossLossAbs > 0 ? grossWin / grossLossAbs : (grossWin > 0 ? 999 : 0);

    const maxBonus = Math.max(0, Number(this.config.trading.adaptiveSetupMaxBonus || 10));
    const maxPenalty = Math.max(0, Number(this.config.trading.adaptiveSetupMaxPenalty || 12));
    const winAdj = (winRate - 50) * 0.18;
    const pfAdj = (Math.min(profitFactor, 3.5) - 1) * 2.2;
    const countAdj = Math.min(2.5, matched.length / 20);
    const rawAdj = winAdj + pfAdj + countAdj;
    const adjustment = Math.max(-maxPenalty, Math.min(maxBonus, rawAdj));

    return {
      adjustment,
      count: matched.length,
      setupKey,
      winRate,
      profitFactor,
    };
  }

  computeGlobalSignalRank(signal, timeframe, candleTime) {
    const confidence = Number(signal?.confidence || 0);
    const rr = Number(signal?.rr || 0);
    const marketQuality = Number(signal?.marketQuality?.score || signal?.market?.qualityScore || 50);
    const pressure = signal?.market?.pressure || {};
    const buy = Number(pressure.buyPressure || 0);
    const sell = Number(pressure.sellPressure || 0);
    const side = String(signal?.side || '').toUpperCase();
    const pressureEdge = side === 'LONG' ? buy - sell : sell - buy;
    const regime = this.detectMarketRegime(signal);
    const regimeAdj = regime === 'TREND_STRONG' ? 4 : regime === 'NOISY' ? -6 : 0;
    const rrAdj = rr > 0 ? Math.max(-8, Math.min(12, (rr - 1) * 8)) : -6;
    const qualityAdj = Math.max(-8, Math.min(10, (marketQuality - 50) * 0.2));
    const pressureAdj = Math.max(-6, Math.min(8, pressureEdge * 0.25));
    const setupPerf = this.getAdaptiveSetupWeight(signal, timeframe);

    const tf = String(timeframe || signal?.signalTimeframe || this.config.timeframe.analysis || '3m');
    const tfAdj = tf === '15m' ? 3 : tf === '5m' ? 2 : tf === '3m' ? 1 : 0;

    const rank = Math.max(
      0,
      Math.min(
        100,
        confidence * 0.55 + rrAdj + qualityAdj + pressureAdj + regimeAdj + tfAdj + Number(setupPerf.adjustment || 0),
      ),
    );

    return {
      rank,
      components: {
        confidenceCore: confidence * 0.55,
        rrAdj,
        qualityAdj,
        pressureAdj,
        regimeAdj,
        timeframeAdj: tfAdj,
        setupHistoryAdj: Number(setupPerf.adjustment || 0),
      },
      setupPerf,
      regime,
      candleTime: Number(candleTime || 0),
    };
  }

  buildGlobalSignalQueueKey(signal, timeframe, candleTime) {
    const tf = String(timeframe || signal?.signalTimeframe || this.config.timeframe.analysis || '3m');
    const entry = Number(signal?.entryPrice || 0);
    const entryBand = entry > 0 ? Math.round(entry / Math.max(entry * 0.0005, 0.5)) : 0;
    const bucket = Math.floor(Number(candleTime || Date.now() / 1000) / Math.max(15, this.timeframeToSec(tf)));
    return `${String(signal?.symbol || '').toUpperCase()}:${tf}:${String(signal?.side || '').toUpperCase()}:${entryBand}:${bucket}`;
  }

  pruneGlobalSignalMaps() {
    const now = Date.now();
    const queueKeepMs = Math.max(10000, Number(this.config.trading.globalRankingWindowMs || 5000) * 6);
    for (const [key, item] of this.globalSignalQueue.entries()) {
      if (!item || now - Number(item.createdAt || 0) > queueKeepMs) this.globalSignalQueue.delete(key);
    }
    const dispatchKeepMs = Math.max(
      30000,
      Number(this.config.trading.globalRankingDispatchCooldownMs || 14000) * 8,
    );
    for (const [key, ts] of this.globalSignalDispatchDedupe.entries()) {
      if (!ts || now - Number(ts) > dispatchKeepMs) this.globalSignalDispatchDedupe.delete(key);
    }
  }

  enqueueGlobalSignalCandidate(signal, timeframe, candleTime) {
    const rankInfo = this.computeGlobalSignalRank(signal, timeframe, candleTime);
    const key = this.buildGlobalSignalQueueKey(signal, timeframe, candleTime);
    const existing = this.globalSignalQueue.get(key);
    const candidate = {
      key,
      signal,
      timeframe,
      candleTime,
      rank: Number(rankInfo.rank || 0),
      rankInfo,
      createdAt: Date.now(),
    };
    if (!existing || candidate.rank >= Number(existing.rank || 0)) {
      this.globalSignalQueue.set(key, candidate);
    }
    this.pruneGlobalSignalMaps();
    return candidate;
  }

  async flushGlobalSignalQueue(options = {}) {
    if (!this.config.trading.globalRankingEnabled) {
      return { dispatched: 0, considered: 0, skipped: 0, reason: 'DISABLED' };
    }
    const now = Date.now();
    const windowMs = Math.max(1000, Number(this.config.trading.globalRankingWindowMs || 5000));
    if (!options.force && now - this.globalSignalLastFlushAt < windowMs) {
      return { dispatched: 0, considered: this.globalSignalQueue.size, skipped: 0, reason: 'WINDOW_WAIT' };
    }

    const minRank = Math.max(0, Math.min(100, Number(this.config.trading.globalRankingMinRank || 45)));
    const topN = Math.max(1, Number(this.config.trading.globalRankingTopN || 8));
    const dispatchCooldown = Math.max(
      1000,
      Number(this.config.trading.globalRankingDispatchCooldownMs || 14000),
    );

    const ranked = Array.from(this.globalSignalQueue.values()).sort(
      (a, b) => Number(b.rank || 0) - Number(a.rank || 0),
    );
    this.globalSignalQueue.clear();

    let dispatched = 0;
    let skipped = 0;
    for (let idx = 0; idx < ranked.length; idx += 1) {
      const item = ranked[idx];
      if (!item) {
        skipped += 1;
        continue;
      }

      const signal = item.signal || {};
      const entry = Number(signal.entryPrice || 0);
      const entryBand = entry > 0 ? Math.round(entry / Math.max(entry * 0.0005, 0.5)) : 0;
      const dedupeKey = `${signal.symbol}:${item.timeframe}:${signal.side}:${entryBand}`;
      const lastAt = Number(this.globalSignalDispatchDedupe.get(dedupeKey) || 0);
      if (lastAt && now - lastAt < dispatchCooldown) {
        skipped += 1;
        continue;
      }

      const payload = {
        ...signal,
        globalRanking: {
          enabled: true,
          rank: Number(item.rank || 0),
          info: item.rankInfo,
          position: idx + 1,
          topN,
          minRank,
          isTop: idx < topN && Number(item.rank || 0) >= minRank,
          tier: idx < topN && Number(item.rank || 0) >= minRank ? 'TOP' : 'NORMAL',
        },
      };
      const routed = await this.routeSignalLifecycle(payload, item.timeframe, item.candleTime, {
        source: 'GLOBAL_RANKING',
        allowSnapshot: false,
      });
      if (!routed?.ok) {
        skipped += 1;
        continue;
      }

      this.lastAcceptedSignal = payload;
      this.lastSignalAt = Date.now();
      this.registerAcceptedZoneSignal(payload);
      this.globalSignalDispatchDedupe.set(dedupeKey, Date.now());
      this.globalSignalLastDispatched = {
        symbol: payload.symbol,
        side: payload.side,
        timeframe: item.timeframe,
        rank: Number(item.rank || 0),
        at: Date.now(),
      };
      dispatched += 1;
    }

    this.globalSignalLastFlushAt = now;
    this.globalSignalFlushCount += 1;
    if (dispatched > 0) this.globalSignalLastDispatchAt = now;
    this.pruneGlobalSignalMaps();
    return {
      dispatched,
      considered: ranked.length,
      skipped,
    };
  }

  async dispatchActionableScanSignal(signal, timeframe, candleTime) {
    if (!signal || signal.side === 'NO_TRADE') return;
    if (signal.confidence < this.config.trading.signalThreshold) return;
    const feeRates = await this.orderManager.getFeeRates(signal.symbol);
    signal.takerFeePct = Number(signal.takerFeePct || feeRates.takerFeePct);
    signal.makerFeePct = Number(signal.makerFeePct || feeRates.makerFeePct);
    signal.feeSource = feeRates.source;
    signal.leverage = Number(signal.leverage || this.config.binance.leverage || 10);
    const entry = Number(signal.entryPrice || 0);
    const tp1 = Number(signal.tp1 || 0);
    if (entry > 0 && tp1 > 0) {
      const lev = Number(signal.leverage || this.config.binance.leverage || 1);
      const gross = (Math.abs(tp1 - entry) / entry) * lev * 100;
      const fees = Number(signal.takerFeePct || this.config.trading.takerFeePct || 0) * 2 * lev * 100;
      if (gross - fees <= 0) return;
    }

    const activeSymbol = this.stateStore.state.symbol;
    const analysisTf = this.config.timeframe.analysis || '3m';
    if (signal.symbol === activeSymbol && timeframe === analysisTf) return;

    const entryBand = signal.entryPrice
      ? Math.round(signal.entryPrice / Math.max(signal.entryPrice * 0.0005, 0.5))
      : 0;
    const key = `${signal.symbol}:${timeframe}:${signal.side}:${entryBand}:${candleTime}`;
    if (this.multiScanLastSignalByKey.has(key)) return;
    this.multiScanLastSignalByKey.set(key, Date.now());

    const maxKeep = 1500;
    if (this.multiScanLastSignalByKey.size > maxKeep) {
      const sorted = Array.from(this.multiScanLastSignalByKey.entries()).sort((a, b) => a[1] - b[1]);
      for (let i = 0; i < sorted.length - maxKeep; i += 1) {
        this.multiScanLastSignalByKey.delete(sorted[i][0]);
      }
    }

    if (!this.config.trading.globalRankingEnabled) {
      await this.routeSignalLifecycle(signal, timeframe, candleTime, {
        source: 'DIRECT_BOT',
        allowSnapshot: false,
      });
      return;
    }

    this.enqueueGlobalSignalCandidate(signal, timeframe, candleTime);
    const topN = Math.max(1, Number(this.config.trading.globalRankingTopN || 8));
    const shouldForceFlush =
      this.globalSignalQueue.size >= topN ||
      Date.now() - this.globalSignalLastFlushAt >=
        Math.max(1000, Number(this.config.trading.globalRankingWindowMs || 5000));
    await this.flushGlobalSignalQueue({ force: shouldForceFlush });
  }

  async scanDirectSymbol(symbol, options = {}) {
    const tfs = Array.isArray(options.timeframes) && options.timeframes.length
      ? options.timeframes
      : this.getScannerTimeframes();
    const candleLimit = Math.max(
      220,
      Number(options.candleLimit || this.config.binance.scannerCandleLimit || 260),
    );
    const source = String(options.source || 'SCANNER_BATCH').toUpperCase();
    const dispatchSignals = options.dispatchSignals !== false;

    this.setDirectBotState(symbol, {
      inFlight: true,
      state: 'ANALYZING',
      lastRunAt: Date.now(),
    });

    const datasets = await Promise.allSettled([
      this.fetchKlines(symbol, '1m', candleLimit),
      this.fetchKlines(symbol, '3m', candleLimit),
      this.fetchKlines(symbol, '5m', candleLimit),
      this.fetchKlines(symbol, '15m', candleLimit),
    ]);

    if (datasets.some((d) => d.status !== 'fulfilled')) {
      const failed = datasets
        .map((d, idx) => ({ d, tf: ['1m', '3m', '5m', '15m'][idx] }))
        .filter((x) => x.d.status !== 'fulfilled')
        .map((x) => `${x.tf}:${x.d.reason?.message || 'ERR'}`)
        .join(' | ');
      const transientOnly = datasets
        .filter((d) => d.status !== 'fulfilled')
        .every((d) => this.isTransientRestError(d.reason));
      this.setDirectBotState(symbol, {
        inFlight: false,
        state: transientOnly ? 'IDLE' : 'ERROR',
        lastRunAt: Date.now(),
        lastError: failed || 'SCAN_DATASET_FAILED',
      });
      return;
    }

    const byTf = {
      '1m': datasets[0].value,
      '3m': datasets[1].value,
      '5m': datasets[2].value,
      '15m': datasets[3].value,
    };

    for (const tf of ['1m', '3m', '5m', '15m']) {
      this.stateStore.setCandles(symbol, tf, byTf[tf], this.config.timeframe.limit);
    }

    let gotClosed = false;
    for (const tf of tfs) {
      const closed = byTf[tf].filter((c) => c.isClosed);
      if (!closed.length) continue;
      const lastClosed = closed[closed.length - 1];
      const closeKey = `${symbol}:${tf}`;
      const prevClose = this.multiScanLastCloseByKey.get(closeKey) || 0;
      if (lastClosed.time <= prevClose) continue;
      this.multiScanLastCloseByKey.set(closeKey, lastClosed.time);
      this.evaluateSignalOutcomesByCandle(symbol, tf, lastClosed);
      if (String(tf || '').toLowerCase() === '1m') {
        await this.evaluateSignalFollowTrack(symbol, null, lastClosed);
      } else {
        await this.evaluateSignalFollowTrack(symbol, tf, lastClosed);
      }
      gotClosed = true;

      const signal = this.signalEngine.analyze({
        symbol,
        candles1m: byTf['1m'],
        candles2m: byTf[tf],
        candles5m: byTf['5m'],
        candles15m: byTf['15m'],
        orderBook: this.stateStore.state.orderBook,
        analysisTimeframe: tf,
      });

      this.stateStore.setMarket(symbol, signal.market || {});
      this.updateDirectBotFromSignal(symbol, tf, signal, lastClosed.time);
      await this.notifyPreSignal(signal, tf, lastClosed.time);
      await this.notifyPreSignalCancelIfNeeded(signal, tf, lastClosed.time);
      if (dispatchSignals) {
        await this.dispatchActionableScanSignal(signal, tf, lastClosed.time);
      }
    }

    const prev = this.directBots.get(symbol) || {};
    this.setDirectBotState(symbol, {
      inFlight: false,
      state: gotClosed ? prev.state || 'IDLE' : 'IDLE',
      lastRunAt: Date.now(),
      lastError: null,
    });

    if (source === 'WATCHDOG') {
      this.directBotRecoveryCount += 1;
    }
  }

  async recoverInactiveDirectBots() {
    if (!this.config.binance.scannerEnabled) return;
    if (!this.config.binance.directBotWatchdogEnabled) return;
    if (this.multiScanInFlight) return;
    if (Date.now() < this.restBlockedUntil && !this.scannerWsConnected) return;
    if (Date.now() < this.restGeoBlockedUntil && !this.scannerWsConnected) return;

    this.directBotLastWatchdogAt = Date.now();
    if (!this.multiScanTimer) {
      this.startMultiSymbolScanner();
      this.directBotRestartCount += 1;
    }

    const health = this.getDirectBotHealthSnapshot();
    const stalled = health.stalledBots.sort((a, b) => (b.staleForMs || 0) - (a.staleForMs || 0));
    if (!stalled.length) return;

    const recoverPerTick = Math.max(1, Number(this.config.binance.directBotRecoverPerTick || 2));
    const targets = stalled.slice(0, recoverPerTick).map((b) => b.symbol);

    await Promise.allSettled(
      targets.map(async (symbol) => {
        if (this.scannerWsConnected) {
          await this.scanDirectSymbolFromState(symbol, {
            source: 'WATCHDOG_STATE',
            dispatchSignals: true,
            forceAnalyze: true,
          });
        } else {
          await this.scanDirectSymbol(symbol, {
            source: 'WATCHDOG',
            dispatchSignals: true,
          });
        }
      }),
    );
    this.emitEvent('status', this.getStatus());
  }

  async runMultiSymbolScan() {
    if (this.multiScanInFlight) return;
    if (!this.config.binance.scannerEnabled) return;
    const scannerWsFresh =
      this.scannerWsConnected &&
      this.scannerWsLastMessageAt > 0 &&
      Date.now() - this.scannerWsLastMessageAt < Math.max(15000, this.getDirectBotStaleMs());
    if (scannerWsFresh) {
      const symbolsAll = this.getScannerSymbols();
      if (!symbolsAll.length) return;
      const configuredBatch = Math.max(1, Number(this.config.binance.scannerBatchSize || 2));
      const batchSize = Math.min(configuredBatch, symbolsAll.length);
      const start = this.multiScanCursor % symbolsAll.length;
      const symbols = [];
      for (let i = 0; i < batchSize; i += 1) {
        symbols.push(symbolsAll[(start + i) % symbolsAll.length]);
      }
      this.multiScanCursor = (start + batchSize) % symbolsAll.length;
      await Promise.allSettled(
        symbols.map(async (symbol) => {
          await this.scanDirectSymbolFromState(symbol, {
            source: 'SCANNER_STATE',
            dispatchSignals: true,
            forceAnalyze: false,
          });
        }),
      );
      this.emitEvent('status', this.getStatus());
      return;
    }
    if (Date.now() < this.restBlockedUntil) return;
    if (Date.now() < this.restGeoBlockedUntil) return;
    this.multiScanInFlight = true;

    try {
      const symbolsAll = this.getScannerSymbols();
      if (!symbolsAll.length) return;
      const configuredBatch = Math.max(1, Number(this.config.binance.scannerBatchSize || 2));
      const batchSize = Math.min(configuredBatch, symbolsAll.length);
      const start = this.multiScanCursor % symbolsAll.length;
      const symbols = [];
      for (let i = 0; i < batchSize; i += 1) {
        symbols.push(symbolsAll[(start + i) % symbolsAll.length]);
      }
      this.multiScanCursor = (start + batchSize) % symbolsAll.length;

      const tfs = this.getScannerTimeframes();
      const candleLimit = Math.max(220, Number(this.config.binance.scannerCandleLimit || 260));
      await Promise.allSettled(
        symbols.map(async (symbol) => {
          await this.scanDirectSymbol(symbol, {
            source: 'SCANNER_BATCH',
            timeframes: tfs,
            candleLimit,
            dispatchSignals: true,
          });
        }),
      );
    } finally {
      if (this.config.trading.globalRankingEnabled && this.globalSignalQueue.size > 0) {
        try {
          await this.flushGlobalSignalQueue({ force: true });
        } catch (error) {
          this.logger.warn('bot', 'Lỗi flush global ranking queue', { error: error.message });
        }
      }
      this.multiScanInFlight = false;
      this.emitEvent('status', this.getStatus());
    }
  }

  calcLivePositionProbability(position, price) {
    const entry = Number(position.entryPrice || 0);
    const stop = Number(position.stopLoss || 0);
    const tp1 = Number(position.tp1 || entry);
    if (!Number.isFinite(entry) || entry <= 0 || !Number.isFinite(price) || price <= 0) return 50;

    const sideSign = position.side === 'LONG' ? 1 : -1;
    const riskDistance = Math.max(Math.abs(entry - stop), entry * 0.0008);
    const rewardDistance = Math.max(Math.abs(tp1 - entry), riskDistance);
    const move = sideSign * (price - entry);
    const riskBuffer = sideSign * (price - stop);

    const moveScore = Math.max(-100, Math.min(100, (move / rewardDistance) * 100));
    const riskScore = Math.max(-100, Math.min(100, (riskBuffer / riskDistance) * 100));

    const market = this.stateStore.state.market[position.symbol] || {};
    const pressure = market.pressure || {};
    const buy = Number(pressure.buyPressure || 0);
    const sell = Number(pressure.sellPressure || 0);
    const pressureEdge = position.side === 'LONG' ? buy - sell : sell - buy;

    const probability = Math.round(52 + moveScore * 0.24 + riskScore * 0.12 + pressureEdge * 0.2);
    return Math.max(8, Math.min(98, probability));
  }

  calcStopRiskSnapshot(position, currentPrice, probability, direction) {
    const entry = Number(position.entryPrice || 0);
    const stop = Number(position.stopLoss || 0);
    if (!Number.isFinite(entry) || entry <= 0 || !Number.isFinite(stop) || !Number.isFinite(currentPrice)) {
      return null;
    }

    const side = String(position.side || '').toUpperCase();
    const sideSign = side === 'LONG' ? 1 : -1;
    const riskDistance = Math.max(Math.abs(entry - stop), entry * 0.0008);
    const distanceToStop = sideSign * (currentPrice - stop);
    const distanceRatio = distanceToStop / riskDistance;
    const riskUsedPct = Math.max(0, Math.min(200, (1 - distanceRatio) * 100));

    const adverseMove = sideSign * (currentPrice - entry) < 0;
    const directionAgainst = (side === 'LONG' && direction === 'DOWN') || (side === 'SHORT' && direction === 'UP');
    const safeProbability = Math.max(0, Math.min(100, Number(probability || 0)));
    const stopRiskPct = Math.max(
      0,
      Math.min(
        99,
        riskUsedPct * 0.72 +
          (100 - safeProbability) * 0.34 +
          (adverseMove ? 8 : 0) +
          (directionAgainst ? 6 : 0),
      ),
    );

    const reasons = [];
    if (riskUsedPct >= 70) reasons.push('Giá đang tiến sâu về vùng SL');
    if (safeProbability <= 45) reasons.push('Xác suất an toàn realtime giảm thấp');
    if (directionAgainst) reasons.push('Nhịp giá hiện tại đang đi ngược hướng lệnh');

    return {
      stopRiskPct,
      riskUsedPct,
      reason: reasons.length ? reasons.join(' | ') : 'Biến động bất lợi tăng gần vùng SL',
    };
  }

  shouldSendStopRiskWarning(position, stopRiskSnapshot) {
    if (!this.config.telegram.stopRiskAlertEnabled) return false;
    if (!stopRiskSnapshot) return false;

    const threshold = Math.max(50, Math.min(99, Number(this.config.telegram.stopRiskAlertThreshold || 76)));
    if (Number(stopRiskSnapshot.stopRiskPct || 0) < threshold) return false;

    const cooldownMs = Math.max(10000, Number(this.config.telegram.stopRiskAlertCooldownMs || 30000));
    const now = Date.now();
    const key = `${position.symbol}:${position.id || position.openedAt || position.entryPrice}:${position.side}`;

    for (const [k, info] of this.positionStopRiskNotifyMap.entries()) {
      if (now - Number(info?.at || 0) > cooldownMs * 8) this.positionStopRiskNotifyMap.delete(k);
    }

    const prev = this.positionStopRiskNotifyMap.get(key);
    if (prev && now - prev.at < cooldownMs) return false;

    this.positionStopRiskNotifyMap.set(key, {
      at: now,
      stopRiskPct: Number(stopRiskSnapshot.stopRiskPct || 0),
    });
    return true;
  }

  async sendPositionPulse() {
    const position = this.stateStore.state.activePosition;
    if (!position) {
      this.stopPositionPulse();
      return;
    }

    const ob = this.stateStore.state.orderBook || {};
    const currentPrice =
      this.config.trading.priceSource === 'MARK'
        ? ob.markPrice || ob.lastPrice || ob.lastTradePrice
        : ob.lastTradePrice || ob.lastPrice || ob.markPrice;

    if (!Number.isFinite(currentPrice) || currentPrice <= 0) return;

    const sideSign = position.side === 'LONG' ? 1 : -1;
    const rawPnlPct = ((currentPrice - position.entryPrice) / position.entryPrice) * 100 * sideSign;
    const leverage = Number(position.leverage || this.config.binance.leverage || 1);
    const leveragedPnlPct = rawPnlPct * leverage;
    const feePenaltyPct = Number(position.takerFeePct || this.config.trading.takerFeePct || 0) * 2 * leverage * 100;
    const netRoiPct = leveragedPnlPct - feePenaltyPct;
    const probability = this.calcLivePositionProbability(position, currentPrice);

    let direction = 'FLAT';
    if (Number.isFinite(this.positionPulseLastPrice)) {
      if (currentPrice > this.positionPulseLastPrice) direction = 'UP';
      if (currentPrice < this.positionPulseLastPrice) direction = 'DOWN';
    }
    this.positionPulseLastPrice = currentPrice;

    await this.telegramService.sendPositionHeartbeat({
      symbol: position.symbol,
      side: position.side,
      currentPrice,
      entryPrice: position.entryPrice,
      stopLoss: position.stopLoss,
      tp1: position.tp1,
      tp2: position.tp2,
      tp3: position.tp3,
      leverage,
      rawPnlPct,
      leveragedPnlPct,
      feePenaltyPct,
      netRoiPct,
      probability,
      direction,
      trailingEnabled: Boolean(position.trailingEnabled),
      partialClosedPct: Number(position.partialClosedPct || 0),
    });

    const tpProbKey = `${position.symbol}:${position.id || position.openedAt || position.entryPrice}:${position.side}`;
    const prevTpProb = this.tpProbNotifyMap.get(tpProbKey);
    const now = Date.now();
    const shouldEmitTpProb =
      !prevTpProb ||
      Math.abs(Number(prevTpProb.probability || 0) - Number(probability || 0)) >= 3 ||
      String(prevTpProb.direction || 'FLAT') !== String(direction || 'FLAT') ||
      now - Number(prevTpProb.at || 0) > 12000;
    if (shouldEmitTpProb) {
      this.tpProbNotifyMap.set(tpProbKey, {
        at: now,
        probability: Number(probability || 0),
        direction,
      });
      this.emitLifecycle(
        'TP_PROB_UPDATE',
        {
          tradeId: position.id,
          symbol: position.symbol,
          side: position.side,
          timeframe: position.signal?.signalTimeframe || this.config.timeframe.analysis || '3m',
          probability,
          direction,
          currentPrice,
          entryPrice: position.entryPrice,
          stopLoss: position.stopLoss,
          tp1: position.tp1,
          tp2: position.tp2,
          tp3: position.tp3,
          netRoiPct,
        },
        { dedupeMs: 2000 },
      );
    }

    const stopRiskSnapshot = this.calcStopRiskSnapshot(position, currentPrice, probability, direction);
    if (this.shouldSendStopRiskWarning(position, stopRiskSnapshot)) {
      this.emitLifecycle(
        'RISK_ALERT',
        {
          tradeId: position.id,
          symbol: position.symbol,
          side: position.side,
          timeframe: position.signal?.signalTimeframe || this.config.timeframe.analysis || '3m',
          currentPrice,
          entryPrice: position.entryPrice,
          stopLoss: position.stopLoss,
          probability,
          stopRiskPct: stopRiskSnapshot.stopRiskPct,
          riskUsedPct: stopRiskSnapshot.riskUsedPct,
          reason: stopRiskSnapshot.reason,
        },
        { dedupeMs: 1500 },
      );
      await this.telegramService.sendStopRiskWarning({
        symbol: position.symbol,
        side: position.side,
        currentPrice,
        entryPrice: position.entryPrice,
        stopLoss: position.stopLoss,
        probability,
        stopRiskPct: stopRiskSnapshot.stopRiskPct,
        riskUsedPct: stopRiskSnapshot.riskUsedPct,
        reason: stopRiskSnapshot.reason,
      });
    }
  }

  startPositionPulse() {
    if (!this.config.telegram.positionHeartbeatEnabled) return;
    const position = this.stateStore.state.activePosition;
    if (!position) return;
    if (position.id && position.id === this.positionPulseEndedPositionId) return;

    const intervalMs = Math.max(5000, Number(this.config.telegram.positionHeartbeatMs || 5000));
    if (this.positionPulseTimer) return;

    // Giữ heartbeat tới khi vị thế đóng hẳn (không dừng ở cuối 1 nến).
    this.positionPulseEndAt = 0;
    this.positionPulseLastPrice = null;
    this.positionPulseEndedPositionId = null;

    this.positionPulseTimer = setInterval(() => {
      this.sendPositionPulse().catch((error) => {
        this.logger.warn('telegram', 'Position pulse lỗi', { error: error.message });
      });
    }, intervalMs);
  }

  stopPositionPulse(markEnded = false) {
    if (markEnded && this.stateStore.state.activePosition?.id) {
      this.positionPulseEndedPositionId = this.stateStore.state.activePosition.id;
    }
    if (!this.positionPulseTimer) return;
    clearInterval(this.positionPulseTimer);
    this.positionPulseTimer = null;
    this.positionPulseEndAt = 0;
    this.positionPulseLastPrice = null;
  }

  syncPositionPulse() {
    if (this.stateStore.state.activePosition) this.startPositionPulse();
    else {
      this.stopPositionPulse();
      this.positionPulseEndedPositionId = null;
      this.positionStopRiskNotifyMap.clear();
      this.tpProbNotifyMap.clear();
    }
  }

  async onKline(candle, timeframe = '1m') {
    try {
      const currentSymbol = this.stateStore.state.symbol;
      const symbol = String(candle?.symbol || currentSymbol).toUpperCase();
      if (symbol !== currentSymbol) {
        return;
      }
      const analysisTf = this.config.timeframe.analysis || '3m';
      const trackedTf = new Set(['1m', '3m', '5m', '15m', analysisTf]);
      if (trackedTf.has(timeframe)) {
        this.stateStore.addCandle(symbol, timeframe, candle, this.config.timeframe.limit);
        if (
          candle.isClosed &&
          this.config.binance.restClosedSyncEnabled &&
          Date.now() >= this.restBlockedUntil
        ) {
          this.syncClosedCandleFromRest(symbol, timeframe, candle.time).catch((error) => {
            if (!this.isTransientRestError(error)) {
              this.logger.warn('bot', 'Không thể đồng bộ nến đóng từ REST', {
                symbol,
                timeframe,
                error: error.message,
              });
            }
          });
        }
        if (candle.isClosed) {
          this.evaluateSignalOutcomesByCandle(symbol, timeframe, candle);
          if (String(timeframe || '').toLowerCase() === '1m') {
            await this.evaluateSignalFollowTrack(symbol, null, candle);
          } else {
            await this.evaluateSignalFollowTrack(symbol, timeframe, candle);
          }
        }
      }

      this.emitEvent('tick', {
        symbol,
        ts: Date.now(),
        type: 'kline',
        payload: {
          timeframe,
          time: candle.time,
          open: candle.open,
          high: candle.high,
          low: candle.low,
          close: candle.close,
          volume: candle.volume,
          quoteVolume: candle.quoteVolume,
          tradeCount: candle.tradeCount,
          takerBuyBaseVolume: candle.takerBuyBaseVolume,
          takerBuyQuoteVolume: candle.takerBuyQuoteVolume,
          closeTime: candle.closeTime,
          isClosed: candle.isClosed,
          eventTime: candle.eventTime,
        },
      });

      if (timeframe !== analysisTf) {
        if (candle.isClosed) {
          await this.analyzeAndNotifyPreSignalByTimeframe(symbol, timeframe, candle.time);
        }
        return;
      }

      if (!candle.isClosed) {
        const now = Date.now();
        if (now - this.lastAnalyzeAt < this.config.trading.liveAnalyzeIntervalMs) {
          return;
        }
      }
      this.lastAnalyzeAt = Date.now();

      if (
        this.stateStore.state.stateMachine === BOT_STATES.PAUSED_BY_RISK &&
        !this.riskManager.isEmergencyStop() &&
        !this.stateStore.state.activePosition
      ) {
        this.trySetState(BOT_STATES.IDLE, { source: 'risk_released' });
      }

      if (this.riskManager.isEmergencyStop()) {
        this.trySetState(BOT_STATES.PAUSED_BY_RISK, { source: 'on_kline' });
      } else if (this.stateStore.state.activePosition) {
        this.trySetState(BOT_STATES.MANAGING_POSITION, { source: 'on_kline' });
      } else {
        this.trySetState(BOT_STATES.ANALYZING, { source: 'on_kline' });
      }

      const candles1m = this.stateStore.getCandles(symbol, '1m');
      const candlesAnalysis = this.stateStore.getCandles(symbol, analysisTf);
      const candles5m = this.stateStore.getCandles(symbol, '5m');
      const candles15m = this.stateStore.getCandles(symbol, '15m');

      const signal = this.signalEngine.analyze({
        symbol,
        candles1m,
        candles2m: candlesAnalysis,
        candles5m,
        candles15m,
        orderBook: this.stateStore.state.orderBook,
      });

      if (signal.side !== 'NO_TRADE') {
        const feeRates = await this.orderManager.getFeeRates(signal.symbol);
        signal.takerFeePct = Number(signal.takerFeePct || feeRates.takerFeePct);
        signal.makerFeePct = Number(signal.makerFeePct || feeRates.makerFeePct);
        signal.feeSource = feeRates.source;
        signal.leverage = Number(signal.leverage || this.config.binance.leverage || 10);
        if (signal.preSignal?.selected) {
          signal.preSignal.selected.leverage = Number(
            signal.preSignal.selected.leverage || signal.leverage || this.config.binance.leverage || 10,
          );
          signal.preSignal.selected.takerFeePct = Number(
            signal.preSignal.selected.takerFeePct || signal.takerFeePct,
          );
        }
      }

      this.stateStore.setMarket(symbol, signal.market || {});
      this.applySignalState(signal);

      const dedupeDecision = this.getSignalDuplicateDecision(signal);
      if (dedupeDecision.skip) {
        this.logger.info('signal', 'Bỏ qua tín hiệu duplicate', {
          side: signal.side,
          confidence: signal.confidence,
          reason: dedupeDecision.reason,
        });
        this.stateStore.setLastReject({
          timestamp: Date.now(),
          code: 'DUPLICATE_ZONE_SIGNAL',
          codes: ['DUPLICATE_ZONE_SIGNAL'],
          signal,
          reasons: [dedupeDecision.reason],
        });
        return;
      }

      const entryBand = signal.entryPrice
        ? Math.round(signal.entryPrice / Math.max(signal.entryPrice * 0.0005, 0.5))
        : 0;
      const pre = signal.preSignal?.selected || null;
      const preBand = pre?.triggerPrice
        ? Math.round(pre.triggerPrice / Math.max(pre.triggerPrice * 0.0005, 0.5))
        : 0;
      const preMode = pre?.mode || 'NA';
      const preProb = Number.isFinite(pre?.probability) ? Math.round(pre.probability / 5) * 5 : 0;
      const dedupeBucketMs =
        signal.side === 'NO_TRADE'
          ? Math.max(1500, this.config.trading.preSignalEmitIntervalMs || 4000)
          : 15000;
      const fingerprint = [
        signal.symbol,
        signal.side,
        signal.level,
        signal.rejectCode || 'NA',
        signal.volatilityRegime || 'NA',
        signal.trend5m || 'NA',
        entryBand,
        pre?.side || 'NA',
        preMode,
        preBand,
        preProb,
        Math.floor(signal.createdAt / dedupeBucketMs),
      ].join(':');
    const duplicated =
        this.lastSignalFingerprint === fingerprint &&
        Date.now() - this.lastSignalAt < this.config.trading.signalDuplicateCooldownMs;
      const allowDuplicateUpdate = duplicated && this.hasMeaningfulSignalVariation(signal);

      if (!duplicated || allowDuplicateUpdate) {
        this.stateStore.setLastSignal(signal);
        this.lastSignalFingerprint = fingerprint;
        this.lastSignalAt = Date.now();
        if (signal.side !== 'NO_TRADE') {
          this.lastAcceptedSignal = signal;
          if (!allowDuplicateUpdate) {
            this.registerAcceptedZoneSignal(signal);
          }
        }

        this.logger.signal('Tín hiệu mới', {
          symbol,
          side: signal.side,
          confidence: signal.confidence,
          level: signal.level,
          preSignal: signal.preSignal?.selected || null,
          reasons: signal.reasons,
          rejectCodes: signal.rejectCodes,
          breakdown: signal.confidenceBreakdown,
        });

        await this.notifyPreSignal(signal, analysisTf, candle.time);
        await this.notifyPreSignalCancelIfNeeded(signal, analysisTf, candle.time);

        const hasNetPositiveTp1 = (() => {
          const entry = Number(signal.entryPrice || 0);
          const tp1 = Number(signal.tp1 || 0);
          if (!(entry > 0 && tp1 > 0)) return true;
          const leverage = Number(signal.leverage || this.config.binance.leverage || 1);
          const gross = (Math.abs(tp1 - entry) / entry) * leverage * 100;
          const fees = Number(signal.takerFeePct || this.config.trading.takerFeePct || 0) * 2 * leverage * 100;
          return gross - fees > 0;
        })();

        if (
          signal.side !== 'NO_TRADE' &&
          signal.confidence >= this.config.trading.signalThreshold &&
          hasNetPositiveTp1
        ) {
          const entryGate = this.shouldSendEntryConfirm(signal, analysisTf);
          if (!entryGate.ok) {
            this.logger.info('signal', 'Chờ ENTRY CONFIRM, chưa gửi lệnh', {
              symbol: signal.symbol,
              side: signal.side,
              confidence: signal.confidence,
              reason: entryGate.reason,
            });
          } else {
            await this.routeSignalLifecycle(signal, analysisTf, candle.time, {
              source: 'TOTAL_BRAIN',
              entryGate,
              allowSnapshot: true,
            });
          }
        }

        this.emitEvent('signal', signal);
      }

      await this.handleModeAction(signal, candle.latencyMs || 0);
      this.syncPositionPulse();
      this.emitEvent('status', this.getStatus());
      this.riskManager.registerHealthyTick();
    } catch (error) {
      const message = String(error?.message || 'UNKNOWN_ON_KLINE_ERROR');
      this.stateStore.setError(message);
      if (this.isTransientRuntimeError(message)) {
        this.logger.warn('bot', 'Lỗi tạm thời onKline, bot tiếp tục chạy', { error: message });
        this.emitEvent('error', { ts: Date.now(), error: message, transient: true });
        return;
      }

      this.trySetState(BOT_STATES.ERROR, { source: 'on_kline_error' });
      this.logger.error('bot', 'Lỗi onKline', { error: message });
      this.riskManager.registerError('ON_KLINE', message);
      this.emitEvent('error', { ts: Date.now(), error: message, transient: false });
      await this.telegramService.sendError({
        title: 'Lỗi phân tích kline',
        detail: message,
      });
    }
  }

  applySignalState(signal) {
    if (this.riskManager.isEmergencyStop()) {
      this.trySetState(BOT_STATES.PAUSED_BY_RISK, { source: 'signal_state' });
      return;
    }

    if (this.stateStore.state.activePosition) {
      this.trySetState(BOT_STATES.MANAGING_POSITION, { source: 'signal_state' });
      return;
    }

    if (signal.side === 'NO_TRADE') {
      if (signal.rejectCode === 'SIDEWAY') {
        this.stateStore.markSidewayDetected();
        this.trySetState(BOT_STATES.PAUSED_BY_SIDEWAY, { source: 'signal_state' });
      } else {
        this.trySetState(BOT_STATES.IDLE, { source: 'signal_state' });
      }
      return;
    }

    if (signal.level === 'WEAK') this.trySetState(BOT_STATES.SIGNAL_WEAK, { source: 'signal_state' });
    else if (signal.level === 'MEDIUM') this.trySetState(BOT_STATES.SIGNAL_MEDIUM, { source: 'signal_state' });
    else this.trySetState(BOT_STATES.SIGNAL_STRONG, { source: 'signal_state' });
  }

  async handleModeAction(signal, latencyMs = 0) {
    if (!this.stateStore.state.botRunning) return;

    if (this.riskManager.isEmergencyStop()) {
      this.trySetState(BOT_STATES.PAUSED_BY_RISK, { source: 'mode_action' });
      return;
    }

    if (this.stateStore.state.activePosition) {
      this.trySetState(BOT_STATES.MANAGING_POSITION, { source: 'mode_action' });
      return;
    }

    const mode = this.stateStore.state.mode;

    if (signal.side === 'NO_TRADE') {
      if (mode === 'AUTO_BOT') {
        this.trySetState(BOT_STATES.PAUSED_BY_SIDEWAY, { source: 'mode_action' });
      }
      return;
    }

    if (mode === 'SIGNAL_ONLY') {
      this.trySetState(BOT_STATES.IDLE, { source: 'mode_action' });
      return;
    }

    if (mode === 'SEMI_AUTO') {
      if (signal.confidence >= this.config.trading.semiThreshold) {
        this.stateStore.setManualPendingSignal(signal);
        this.trySetState(BOT_STATES.WAIT_CONFIRM, { source: 'mode_action' });
      }
      return;
    }

    if (mode === 'AUTO_BOT') {
      if (signal.confidence < this.config.trading.autoThreshold) return;

      const execSim = this.simulateExecutionGate(signal);
      if (!execSim.ok) {
        this.emitLifecycle(
          'RISK_ALERT',
          {
            ...signal,
            timeframe: signal.signalTimeframe || this.config.timeframe.analysis || '3m',
            reason: execSim.reason,
            executionSim: execSim,
            source: 'AUTO_PRE_ORDER',
          },
          { dedupeMs: 1200 },
        );
        return;
      }

      const orderResult = await this.orderManager.placeMarketOrder(signal, {
        latencyMs,
        estimatedSlippagePct: this.estimateSlippage(signal),
      });

      if (orderResult.ok) {
        this.trySetState(BOT_STATES.OPEN_POSITION, { source: 'mode_action' });
        this.syncPositionPulse();
        await this.sendEntrySnapshot(signal);
      } else {
        this.trySetState(BOT_STATES.PAUSED_BY_RISK, { source: 'mode_action' });
      }
    }
  }

  estimateSlippage(signal) {
    const ob = this.stateStore.state.orderBook;
    const mid =
      this.config.trading.priceSource === 'MARK'
        ? ob.markPrice || ob.lastPrice
        : ob.lastTradePrice || (ob.bestAsk && ob.bestBid ? (ob.bestAsk + ob.bestBid) / 2 : 0);
    if (!mid) return 0;
    return Math.abs(signal.entryPrice - mid) / mid;
  }

  async notifyNoTradeBlock(signal) {
    const rejectCode = signal.rejectCode || 'NO_TRADE';
    const now = Date.now();
    const cooldown = Math.max(this.config.telegram.cooldownMs, 60000);
    if (this.lastRejectCode === rejectCode && now - this.lastRejectNotifyAt < cooldown) {
      return;
    }

    this.lastRejectCode = rejectCode;
    this.lastRejectNotifyAt = now;

    await this.telegramService.sendReject({
      symbol: signal.symbol,
      side: signal.side,
      confidence: signal.confidence,
      codes: signal.rejectCodes || [rejectCode],
      reasons: signal.reasons || ['Điều kiện chưa đạt để vào lệnh'],
    });
  }

  async sendEntrySnapshot(signal) {
    if (!this.config.chartSnapshot.sendOnEntry) return;
    const snapshotPath = await this.chartSnapshot.capture(signal, 'entry');
    if (!snapshotPath) return;

    await this.telegramService.sendPhoto(
      snapshotPath,
      [
        `📸 <b>ENTRY SNAPSHOT</b>`,
        `Coin: ${signal.symbol}`,
        `Loại: ${signal.side}`,
        `Entry: ${signal.entryMin.toFixed(2)} - ${signal.entryMax.toFixed(2)}`,
        `SL: ${signal.stopLoss.toFixed(2)}`,
        `TP: ${signal.tp1.toFixed(2)} / ${signal.tp2.toFixed(2)} / ${signal.tp3.toFixed(2)}`,
        `Confidence: ${signal.confidence}/100`,
      ].join('\n'),
    );
  }

  async manualConfirmTrade() {
    const signal = this.stateStore.state.manualPendingSignal;
    if (!signal) return { ok: false, reason: 'Không có tín hiệu chờ xác nhận' };

    if (this.riskManager.isEmergencyStop()) {
      this.trySetState(BOT_STATES.PAUSED_BY_RISK, { source: 'manual_confirm' });
      return { ok: false, reason: 'Emergency stop đang bật' };
    }

    const result = await this.orderManager.placeMarketOrder(signal, {
      latencyMs: 0,
      estimatedSlippagePct: this.estimateSlippage(signal),
    });

    if (result.ok) {
      this.stateStore.clearManualPendingSignal();
      this.trySetState(BOT_STATES.OPEN_POSITION, { source: 'manual_confirm' });
      this.syncPositionPulse();
      await this.sendEntrySnapshot(signal);
      this.emitEvent('status', this.getStatus());
    }

    return result;
  }

  async start() {
    if (this.stateStore.state.mode === 'SIGNAL_ONLY') {
      this.stateStore.setBotRunning(true);
      this.stateStore.setPause(null);
      this.trySetState(BOT_STATES.IDLE, { source: 'start_signal_only' });
      this.emitEvent('status', this.getStatus());
      return this.stateStore.getStatus();
    }

    if (this.riskManager.isEmergencyStop()) {
      if (!this.stateStore.state.activePosition) {
        this.riskManager.releaseEmergencyStop();
        this.logger.warn('bot', 'Đã gỡ emergency stop khi bật bot (không có vị thế mở)');
      }
      if (this.riskManager.isEmergencyStop()) {
        this.stateStore.setBotRunning(true);
        this.trySetState(BOT_STATES.PAUSED_BY_RISK, { source: 'start' });
        this.stateStore.setPause('RUNNING_WITH_RISK_LOCK');
        this.emitEvent('status', this.getStatus());
        return this.stateStore.getStatus();
      }
    }

    this.stateStore.setBotRunning(true);
    this.trySetState(BOT_STATES.IDLE, { source: 'start' });
    this.stateStore.setPause(null);
    this.stateStore.clearCooldown();
    this.syncPositionPulse();
    this.logger.info('bot', 'Bot đã bật');
    this.emitEvent('status', this.getStatus());
    return this.stateStore.getStatus();
  }

  async stop(reason = 'MANUAL_STOP') {
    if (this.stateStore.state.mode === 'SIGNAL_ONLY') {
      this.stateStore.setBotRunning(true);
      this.stateStore.setPause('SIGNAL_ONLY_ALWAYS_ON');
      this.trySetState(BOT_STATES.IDLE, { source: 'stop_signal_only_ignored' });
      this.emitEvent('status', this.getStatus());
      return this.stateStore.getStatus();
    }

    this.stateStore.setBotRunning(false);
    const emergencyReason = String(reason || '').toUpperCase().includes('EMERGENCY');
    if (emergencyReason || this.riskManager.isEmergencyStop()) {
      this.trySetState(BOT_STATES.PAUSED_BY_RISK, { source: 'stop' });
    } else {
      this.trySetState(BOT_STATES.IDLE, { source: 'stop' });
    }
    this.stateStore.setPause(reason);
    this.stopPositionPulse();
    this.logger.warn('bot', 'Bot đã dừng', { reason });
    this.emitEvent('status', this.getStatus());
    return this.stateStore.getStatus();
  }

  async changeMode(mode) {
    const allow = ['SIGNAL_ONLY', 'SEMI_AUTO', 'AUTO_BOT'];
    if (!allow.includes(mode)) {
      throw new Error('Mode không hợp lệ');
    }

    const prevMode = this.stateStore.state.mode;
    this.stateStore.setMode(mode);
    if (mode === 'SIGNAL_ONLY') {
      this.stateStore.setBotRunning(true);
      this.stateStore.setPause(null);
      this.trySetState(BOT_STATES.IDLE, { source: 'change_mode_signal_only' });
    } else if (prevMode === 'SIGNAL_ONLY' && !this.stateStore.state.activePosition) {
      this.stateStore.setBotRunning(false);
      this.stateStore.setPause('AWAITING_USER_START');
      this.trySetState(BOT_STATES.IDLE, { source: 'change_mode_need_start' });
    }
    this.logger.info('bot', 'Đã đổi mode', { mode });
    this.emitEvent('status', this.getStatus());
    return this.stateStore.getStatus();
  }

  async changeSymbol(symbol) {
    const next = symbol.toUpperCase();
    if (this.stateStore.state.activePosition) {
      throw new Error('Không thể đổi symbol khi còn vị thế mở');
    }

    this.stateStore.clearManualPendingSignal();
    this.trySetState(BOT_STATES.IDLE, { source: 'change_symbol' });
    this.stateStore.setSymbol(next);
    for (const key of this.zoneSignalHistory.keys()) {
      if (key.startsWith(`${next}:`)) continue;
      this.zoneSignalHistory.delete(key);
    }
    await this.bootstrapCandles(next);
    this.websocketManager.switchSymbol(next);
    this.logger.info('bot', 'Đã đổi symbol', { symbol: next });
    this.emitEvent('status', this.getStatus());
    return this.stateStore.getStatus();
  }

  async updateConfig(payload) {
    const updates = payload || {};

    if (updates.mode) {
      await this.changeMode(updates.mode);
    }

    if (updates.symbol) {
      await this.changeSymbol(updates.symbol);
    }

    if (updates.risk) {
      this.riskManager.updateRiskConfig(updates.risk);
    }

    if (updates.trading) {
      Object.assign(this.config.trading, updates.trading);
      if (updates.trading.priceSource) {
        this.config.trading.priceSource =
          String(updates.trading.priceSource).toUpperCase() === 'MARK' ? 'MARK' : 'LAST';
        const ob = this.stateStore.state.orderBook;
        this.stateStore.setOrderBook({
          lastPrice:
            this.config.trading.priceSource === 'MARK'
              ? ob.markPrice || ob.lastTradePrice || ob.lastPrice
              : ob.lastTradePrice || ob.lastPrice,
        });
      }
    }

    if (updates.advanced) {
      Object.assign(this.config.advanced, updates.advanced);
    }

    if (updates.positionSizing) {
      this.config.positionSizing = {
        ...(this.config.positionSizing || {}),
        ...updates.positionSizing,
      };
    }

    this.logger.info('bot', 'Đã cập nhật config runtime', { updates });
    this.emitEvent('status', this.getStatus());

    return this.stateStore.getStatus();
  }

  async sendTelegramTest(payload = {}) {
    if (!this.telegramService.isEnabled || !this.telegramService.isEnabled()) {
      throw new Error(
        'Telegram chưa bật hoặc chưa có route hợp lệ. Kiểm tra TELEGRAM_ENABLED, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, TELEGRAM_SECONDARY_*',
      );
    }

    const route = String(payload.route || 'ALL').toUpperCase();
    const forceRoute = route === 'PRIMARY' || route === 'SECONDARY' ? route : '';
    const symbol = payload.symbol ? String(payload.symbol).toUpperCase() : '';
    const options = {
      symbol,
      broadcast: route === 'ALL',
      forceRoute,
      cooldownMs: 0,
      idempotencyKey: `manual-test:${Date.now()}:${Math.floor(Math.random() * 100000)}`,
      idempotencyTtlMs: 1000,
    };

    const rawMessage = String(payload.message || '').trim();
    const message =
      rawMessage ||
      `🧪 <b>TEST TELEGRAM</b>\nBot: <b>${this.config.app.name}</b>\nRoute: <b>${route}</b>\nTime: <b>${formatTs(
        Date.now(),
        this.config.app.timezone,
      )}</b>`;
    const sticker = String(payload.sticker || '').trim();
    const animation = String(payload.animation || '').trim();
    const caption = String(payload.caption || '🎬 Animation test từ TUẤN ANH BOT');
    const diceEmoji = String(payload.diceEmoji || '').trim();

    let sentText = false;
    let sentSticker = false;
    let sentAnimation = false;
    let sentDice = false;
    let textReport = null;

    if (message) {
      textReport = await this.telegramService.sendText(message, options);
      sentText = Boolean(textReport?.ok);
    }

    if (sticker) {
      sentSticker = await this.telegramService.sendSticker(sticker, {
        ...options,
        idempotencyKey: `${options.idempotencyKey}:sticker`,
      });
    }

    if (animation) {
      sentAnimation = await this.telegramService.sendAnimation(animation, caption, {
        ...options,
        idempotencyKey: `${options.idempotencyKey}:animation`,
      });
    }

    if (diceEmoji) {
      sentDice = await this.telegramService.sendDice(diceEmoji, {
        ...options,
        idempotencyKey: `${options.idempotencyKey}:dice`,
      });
    }

    this.logger.info('telegram', 'Admin gửi test Telegram từ app', {
      route,
      symbol: symbol || null,
      sentText,
      sentSticker,
      sentAnimation,
      sentDice,
    });

    if (!sentText && !sentSticker && !sentAnimation && !sentDice) {
      const firstError = textReport?.errors?.[0]?.error || '';
      throw new Error(
        `Không gửi được Telegram. ${firstError || 'Kiểm tra bot token, chat id, và quyền bot trong group.'}`,
      );
    }

    return {
      route,
      symbol: symbol || null,
      sentText,
      sentSticker,
      sentAnimation,
      sentDice,
      textReport,
    };
  }

  getCurrentSignal() {
    return this.stateStore.state.lastSignal;
  }

  getStatus() {
    const scannerSymbols = this.getScannerSymbols();
    const health = this.getDirectBotHealthSnapshot();
    const directBotList = health.bots;
    const outcomeHealth = this.getOutcomeBotHealthSnapshot();
    const outcomeBotList = outcomeHealth.bots;
    return {
      ...this.stateStore.getStatus(),
      profile: this.config.profile || 'BALANCED',
      priceSource: this.config.trading.priceSource,
      advanced: this.config.advanced,
      positionSizing: this.config.positionSizing,
      scanner: {
        enabled: this.config.binance.scannerEnabled,
        loopMs: this.config.binance.scannerLoopMs,
        batchSize: this.config.binance.scannerBatchSize,
        watchdogEnabled: this.config.binance.directBotWatchdogEnabled,
        watchdogMs: this.config.binance.directBotWatchdogMs,
        staleMs: health.staleMs,
        wsConnected: this.scannerWsConnected,
        wsLastMessageAt: this.scannerWsLastMessageAt,
        timeframes: this.getScannerTimeframes(),
        symbols: scannerSymbols,
        symbolCount: scannerSymbols.length,
        totalBrainEnabled: true,
      },
      ranking: {
        enabled: this.config.trading.globalRankingEnabled,
        windowMs: this.config.trading.globalRankingWindowMs,
        topN: this.config.trading.globalRankingTopN,
        minRank: this.config.trading.globalRankingMinRank,
        dispatchCooldownMs: this.config.trading.globalRankingDispatchCooldownMs,
        queueSize: this.globalSignalQueue.size,
        flushCount: this.globalSignalFlushCount,
        lastFlushAt: this.globalSignalLastFlushAt || 0,
        lastDispatchAt: this.globalSignalLastDispatchAt || 0,
        lastDispatched: this.globalSignalLastDispatched || null,
        adaptiveSetupWeightEnabled: this.config.trading.adaptiveSetupWeightEnabled,
      },
      fleet: {
        centralBot: {
          name: this.config.app.name,
          role: 'TOTAL_BRAIN',
        },
        directBots: {
          requestedCount: scannerSymbols.length,
          runningCount: health.activeCount,
          inactiveCount: health.inactiveCount,
          stalledCount: health.stalledCount,
          staleMs: health.staleMs,
          watchdog: {
            enabled: this.config.binance.directBotWatchdogEnabled,
            watchdogMs: this.config.binance.directBotWatchdogMs,
            recoverPerTick: this.config.binance.directBotRecoverPerTick,
            lastRunAt: this.directBotLastWatchdogAt || 0,
            recoveryCount: this.directBotRecoveryCount,
            scannerRestartCount: this.directBotRestartCount,
          },
          bots: directBotList,
        },
        outcomeBots: {
          requestedCount: scannerSymbols.length,
          runningCount: outcomeHealth.activeCount,
          inactiveCount: outcomeHealth.inactiveCount,
          stalledCount: outcomeHealth.stalledCount,
          staleMs: outcomeHealth.staleMs,
          watchdog: {
            enabled: this.config.binance.directBotWatchdogEnabled,
            watchdogMs: this.config.binance.directBotWatchdogMs,
            recoverPerTick: this.config.binance.directBotRecoverPerTick,
            lastRunAt: this.outcomeBotLastWatchdogAt || 0,
            recoveryCount: this.outcomeBotRecoveryCount,
          },
          bots: outcomeBotList,
        },
        telegramBots: {
          primaryEnabled: this.config.telegram.enabled,
          secondaryEnabled: this.config.telegram.secondaryEnabled,
          count:
            (this.config.telegram.enabled ? 1 : 0) +
            (this.config.telegram.secondaryEnabled ? 1 : 0),
        },
      },
      leveragePolicy: {
        dynamicEnabled: this.config.trading.dynamicLeverageEnabled,
        min: this.config.trading.dynamicLeverageMin,
        max: this.config.trading.dynamicLeverageMax,
        step: this.config.trading.leverageConfidenceStep,
      },
      binance: {
        useTestnet: this.config.binance.useTestnet,
        leverage: this.config.binance.leverage,
        restBaseUrl: this.config.binance.restBaseUrl,
        wsBaseUrl: this.config.binance.wsBaseUrl,
      },
      telegram: this.telegramService.getMetrics ? this.telegramService.getMetrics() : undefined,
      lifecycle: {
        count: this.lifecycleEvents.length,
        last: this.lifecycleEvents.length ? this.lifecycleEvents[this.lifecycleEvents.length - 1] : null,
      },
    };
  }
}

module.exports = BotEngine;


