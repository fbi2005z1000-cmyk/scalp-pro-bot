const axios = require('axios');
const EventEmitter = require('events');
const SignalEngine = require('./signalEngine');
const { BOT_STATES } = require('./stateStore');

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
    this.positionPulseTimer = null;
    this.positionPulseEndAt = 0;
    this.positionPulseLastPrice = null;
    this.positionPulseEndedPositionId = null;
    this.restBlockedUntil = 0;
    this.restBlockedReason = null;
    this.lastRestBlockLogAt = 0;
    this.lastRestBudgetLogAt = 0;
    this.restCallWindow = [];
    this.bound = false;
  }

  on(eventName, listener) {
    this.events.on(eventName, listener);
    return () => this.events.off(eventName, listener);
  }

  emitEvent(eventName, payload) {
    this.events.emit(eventName, payload);
  }

  getHttpStatus(error) {
    return Number(error?.response?.status || 0);
  }

  isRateLimitError(error) {
    const status = this.getHttpStatus(error);
    return status === 418 || status === 429;
  }

  isTransientRestError(error) {
    if (this.isRateLimitError(error)) return true;
    const msg = String(error?.message || '').toUpperCase();
    return (
      msg.includes('STATUS CODE 418') ||
      msg.includes('STATUS CODE 429') ||
      msg.includes('ECONNRESET') ||
      msg.includes('ETIMEDOUT') ||
      msg.includes('EAI_AGAIN') ||
      msg.includes('REST_BLOCKED')
    );
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
    await this.bootstrapCandles(this.stateStore.state.symbol);
    await this.bootstrapScannerSymbols();
    this.bindWebsocket();
    this.websocketManager.connect(this.stateStore.state.symbol);
    this.startMultiSymbolScanner();
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
      this.logger.warn('websocket', 'Đang reconnect WS', { attempt, delay });
      this.emitEvent('status', this.getStatus());
      await this.telegramService.sendError({
        title: 'WebSocket Binance bị ngắt',
        detail: `Reconnect lần ${attempt}, delay ${delay}ms`,
      });
      await this.orderManager.syncPosition();
    });

    this.websocketManager.on('stale', async ({ staleForMs, staleDataMs }) => {
      this.logger.warn('websocket', 'Phát hiện stale data', { staleForMs, staleDataMs });
      this.riskManager.registerError('WS_STALE_DATA', `staleForMs=${staleForMs}`);
      this.emitEvent('status', this.getStatus());
      await this.telegramService.sendError({
        title: 'WebSocket stale data',
        detail: `Không có data mới trong ${staleForMs}ms (ngưỡng ${staleDataMs}ms)`,
      });
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
        this.riskManager.registerError('WS_GAP_BACKFILL', error.message);
      }
    });

    this.websocketManager.on('circuit', async ({ attempt, cooldownMs }) => {
      this.logger.error('websocket', 'WS circuit breaker kích hoạt', { attempt, cooldownMs });
      this.riskManager.activateEmergencyStop('WS_CIRCUIT_BREAKER', `attempt=${attempt}`);

      if (this.stateStore.state.activePosition && this.config.risk.closePositionOnEmergency) {
        const now = Date.now();
        if (now - this.lastEmergencyHandledAt > 10000) {
          this.lastEmergencyHandledAt = now;
          await this.orderManager.forceClosePosition('EMERGENCY_STOP_WS_CIRCUIT');
        }
      }

      await this.telegramService.sendError({
        title: 'WebSocket circuit breaker',
        detail: `Reconnect thất bại nhiều lần, cooldown ${cooldownMs}ms`,
      });
    });

    this.websocketManager.on('error', async ({ error }) => {
      this.stateStore.setError(error.message);
      this.riskManager.registerError('WEBSOCKET', error.message);
      this.emitEvent('error', { ts: Date.now(), error: error.message });
      await this.telegramService.sendError({
        title: 'WebSocket lỗi',
        detail: error.message,
      });
    });
  }

  async fetchKlines(symbol, interval, limit = 600, options = {}) {
    const priority = Boolean(options.priority);
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
          closeTime: Number(k[6]),
          isClosed: Number(k[6]) <= now,
        }));
      } catch (error) {
        lastError = error;
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
      this.logger.warn('bot', 'Bootstrap nến có khung lỗi, dùng dữ liệu còn lại', {
        symbol,
        failed,
      });
      const transientOnly = failed.every((f) => this.isTransientRestError({ message: f.error }));
      if (!transientOnly) {
        this.riskManager.registerError('BOOTSTRAP_PARTIAL', JSON.stringify(failed));
      }
    }

    if (!ok.length) {
      this.logger.error('bot', 'Lỗi bootstrap candles toàn bộ khung', { symbol, failed });
      const transientOnly = failed.every((f) => this.isTransientRestError({ message: f.error }));
      if (!transientOnly) {
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

  prunePreSignalDispatchMap() {
    const now = Date.now();
    const keepMs = 3 * 60 * 60 * 1000;
    for (const [key, ts] of this.preSignalDispatchMap.entries()) {
      if (now - ts > keepMs) this.preSignalDispatchMap.delete(key);
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
    if (!signal || signal.side !== 'NO_TRADE') return false;
    if (this.stateStore.state.activePosition) return false;

    const pre = signal.preSignal?.selected;
    if (!pre) return false;
    if (!['LONG', 'SHORT'].includes(pre.side)) return false;
    if (!['READY_SOON', 'ARMED'].includes(pre.mode)) return false;

    const probability = Number(pre.probability || 0);
    if (probability < Number(this.config.telegram.preSignalMinProbability || 78)) return false;

    const etaSec = Number(pre.etaSec || 0);
    if (!Number.isFinite(etaSec) || etaSec <= 0) return false;

    const candleSec = this.timeframeToSec(timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m');
    const minEta = Math.max(candleSec * Number(this.config.telegram.preSignalMinCandles || 4), 30);
    const maxEta = Math.max(candleSec * Number(this.config.telegram.preSignalMaxCandles || 4), minEta);
    if (etaSec < minEta || etaSec > maxEta) return false;

    const triggerPrice = Number(pre.triggerPrice || 0);
    if (!Number.isFinite(triggerPrice) || triggerPrice <= 0) return false;

    const band = Math.round(triggerPrice / Math.max(triggerPrice * 0.0005, 0.5));
    this.prunePreSignalDispatchMap();
    const closeKey = Number.isFinite(Number(candleTime)) ? Number(candleTime) : Math.floor(Date.now() / 1000);
    const fingerprint = `${signal.symbol}:${timeframe}:${closeKey}:${pre.side}:${pre.mode}:${band}`;
    if (this.preSignalDispatchMap.has(fingerprint)) return false;
    this.preSignalDispatchMap.set(fingerprint, Date.now());
    return true;
  }

  async notifyPreSignal(signal, timeframe, candleTime) {
    if (!this.shouldNotifyPreSignal(signal, timeframe, candleTime)) return;
    const pre = signal.preSignal.selected;
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
    const tfSec = this.timeframeToSec(timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m');
    const remainingCandles = Math.max(1, Math.ceil(Number(pre.etaSec || 0) / tfSec));
    await this.telegramService.sendPreSignal({
      symbol: signal.symbol,
      timeframe: timeframe || signal.signalTimeframe || this.config.timeframe.analysis || '3m',
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
      candleTime,
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
  }

  getScannerSymbols() {
    const list = Array.isArray(this.config.binance.symbols) ? this.config.binance.symbols : [];
    const normalized = list
      .map((s) => String(s || '').trim().toUpperCase())
      .filter(Boolean);
    if (!normalized.length) return [this.stateStore.state.symbol];
    return normalized;
  }

  getScannerTimeframes() {
    return ['1m', '3m', '5m', '15m'];
  }

  async bootstrapScannerSymbols() {
    if (!this.config.binance.scannerEnabled) return;
    const symbols = this.getScannerSymbols();
    const intervals = ['1m', '3m', '5m', '15m'];
    const batchSize = Math.max(1, Number(this.config.binance.scannerBootstrapBatchSize || 2));
    const pauseMs = Math.max(0, Number(this.config.binance.scannerBootstrapDelayMs || 220));
    const candleLimit = Math.max(220, Number(this.config.binance.scannerCandleLimit || 260));

    for (let i = 0; i < symbols.length; i += batchSize) {
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

  runTotalBrainGate(signal, timeframe) {
    if (!signal || signal.side === 'NO_TRADE') {
      return { ok: false, reason: 'NO_TRADE' };
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

    const tfBoost = timeframe === '15m' ? 0 : 2;
    const minScore = Math.max(55, Number(this.config.trading.signalThreshold || 56) + tfBoost);
    if (Number(signal.confidence || 0) < minScore) {
      return { ok: false, reason: `TOTAL_BRAIN_LOW_SCORE_${signal.confidence}` };
    }

    return { ok: true };
  }

  async dispatchActionableScanSignal(signal, timeframe, candleTime) {
    if (!signal || signal.side === 'NO_TRADE') return;
    if (signal.confidence < this.config.trading.signalThreshold) return;
    const totalGate = this.runTotalBrainGate(signal, timeframe);
    if (!totalGate.ok) return;
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

    await this.telegramService.sendSignal({
      ...signal,
      signalTimeframe: timeframe,
      source: 'MULTI_SCANNER',
    });
  }

  async runMultiSymbolScan() {
    if (this.multiScanInFlight) return;
    if (!this.config.binance.scannerEnabled) return;
    if (Date.now() < this.restBlockedUntil) return;
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
          const datasets = await Promise.allSettled([
            this.fetchKlines(symbol, '1m', candleLimit),
            this.fetchKlines(symbol, '3m', candleLimit),
            this.fetchKlines(symbol, '5m', candleLimit),
            this.fetchKlines(symbol, '15m', candleLimit),
          ]);

          if (datasets.some((d) => d.status !== 'fulfilled')) return;

          const byTf = {
            '1m': datasets[0].value,
            '3m': datasets[1].value,
            '5m': datasets[2].value,
            '15m': datasets[3].value,
          };

          for (const tf of ['1m', '3m', '5m', '15m']) {
            this.stateStore.setCandles(symbol, tf, byTf[tf], this.config.timeframe.limit);
          }

          for (const tf of tfs) {
            const closed = byTf[tf].filter((c) => c.isClosed);
            if (!closed.length) continue;
            const lastClosed = closed[closed.length - 1];
            const closeKey = `${symbol}:${tf}`;
            const prevClose = this.multiScanLastCloseByKey.get(closeKey) || 0;
            if (lastClosed.time <= prevClose) continue;
            this.multiScanLastCloseByKey.set(closeKey, lastClosed.time);

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
            await this.notifyPreSignal(signal, tf, lastClosed.time);
            await this.dispatchActionableScanSignal(signal, tf, lastClosed.time);
          }
        }),
      );
    } finally {
      this.multiScanInFlight = false;
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

      if (!duplicated) {
        this.stateStore.setLastSignal(signal);
        this.lastSignalFingerprint = fingerprint;
        this.lastSignalAt = Date.now();
        if (signal.side !== 'NO_TRADE') {
          this.lastAcceptedSignal = signal;
          this.registerAcceptedZoneSignal(signal);
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

        if (candle.isClosed) {
          await this.notifyPreSignal(signal, analysisTf, candle.time);
        }

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
          let snapshotPath = null;
          if (signal.level === 'STRONG' && this.config.chartSnapshot.sendOnStrongSignal) {
            snapshotPath = await this.chartSnapshot.capture(signal, 'strong-signal');
          }
          await this.telegramService.sendSignal(signal, snapshotPath);
        }

        this.emitEvent('signal', signal);
      }

      await this.handleModeAction(signal, candle.latencyMs || 0);
      this.syncPositionPulse();
      this.emitEvent('status', this.getStatus());
      this.riskManager.registerHealthyTick();
    } catch (error) {
      this.stateStore.setError(error.message);
      this.trySetState(BOT_STATES.ERROR, { source: 'on_kline_error' });
      this.logger.error('bot', 'Lỗi onKline', { error: error.message });
      this.riskManager.registerError('ON_KLINE', error.message);
      this.emitEvent('error', { ts: Date.now(), error: error.message });
      await this.telegramService.sendError({
        title: 'Lỗi phân tích kline',
        detail: error.message,
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
      `🧪 <b>TEST TELEGRAM</b>\nBot: <b>${this.config.app.name}</b>\nRoute: <b>${route}</b>\nTime: <b>${new Date().toLocaleString('vi-VN')}</b>`;
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
    return {
      ...this.stateStore.getStatus(),
      priceSource: this.config.trading.priceSource,
      advanced: this.config.advanced,
      positionSizing: this.config.positionSizing,
      scanner: {
        enabled: this.config.binance.scannerEnabled,
        loopMs: this.config.binance.scannerLoopMs,
        batchSize: this.config.binance.scannerBatchSize,
        timeframes: this.getScannerTimeframes(),
        symbols: scannerSymbols,
        symbolCount: scannerSymbols.length,
        totalBrainEnabled: true,
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
    };
  }
}

module.exports = BotEngine;
