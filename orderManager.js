const axios = require('axios');
const crypto = require('crypto');
const { uniqueId } = require('./utils');
const { BOT_STATES } = require('./stateStore');

class OrderManager {
  constructor(config, stateStore, logger, riskManager, telegramService) {
    this.config = config;
    this.stateStore = stateStore;
    this.logger = logger;
    this.riskManager = riskManager;
    this.telegramService = telegramService;

    this.http = axios.create({
      baseURL: config.binance.restBaseUrl,
      timeout: 10000,
    });

    this.lastOrderFingerprint = null;
    this.lastOrderAt = 0;
    this.liveOrderEnabled = process.env.LIVE_ORDER_ENABLED === 'true';
    this.symbolFilters = new Map();
    this.lastLeverageBySymbol = new Map();
    this.feeRateCache = new Map();
    this.lifecycleHook = null;
  }

  setLifecycleHook(fn) {
    this.lifecycleHook = typeof fn === 'function' ? fn : null;
  }

  emitLifecycle(type, payload = {}) {
    if (!this.lifecycleHook) return;
    try {
      this.lifecycleHook(type, payload);
    } catch (error) {
      this.logger.warn('trade', 'Lifecycle hook lỗi', {
        type,
        error: error.message,
      });
    }
  }

  getSignalLeverage(signal) {
    const fallback = Math.max(1, Number(this.config.binance.leverage || 10));
    const dynEnabled = this.config.trading.dynamicLeverageEnabled !== false;
    const minLev = Math.max(1, Number(this.config.trading.dynamicLeverageMin || fallback));
    const maxLev = Math.max(minLev, Number(this.config.trading.dynamicLeverageMax || fallback));
    const raw = Number(signal?.leverage || fallback);
    const safe = Number.isFinite(raw) ? Math.round(raw) : fallback;
    if (!dynEnabled) {
      return Math.min(maxLev, Math.max(minLev, Math.round(fallback)));
    }
    return Math.min(maxLev, Math.max(minLev, safe));
  }

  getNotionalProfile(symbol) {
    const s = String(symbol || '').toUpperCase();
    const cfg = this.config.positionSizing || {};
    const toSet = (arr) =>
      new Set((Array.isArray(arr) ? arr : []).map((x) => String(x || '').toUpperCase()));
    const highLiq = toSet(cfg.highLiqSymbols);
    const scp = toSet(cfg.scpSymbols);

    if (highLiq.has(s)) {
      return {
        group: 'HIGH_LIQ',
        minNotional: Number(cfg.highLiqMinNotionalUSDT || 5),
        maxNotional: Number(cfg.highLiqMaxNotionalUSDT || 20),
      };
    }

    if (scp.has(s)) {
      return {
        group: 'SCP',
        minNotional: Number(cfg.scpMinNotionalUSDT || 5),
        maxNotional: Number(cfg.scpMaxNotionalUSDT || 10),
      };
    }

    return {
      group: 'DEFAULT',
      minNotional: Number(this.config.risk.minNotionalUSDT || 5),
      maxNotional: Infinity,
    };
  }

  async ensureLeverage(symbol, leverage) {
    const lev = Math.max(1, Math.round(Number(leverage || this.config.binance.leverage || 10)));
    if (!this.liveOrderEnabled) return lev;

    const key = `${symbol}:${lev}`;
    if (this.lastLeverageBySymbol.get(symbol) === lev) return lev;

    try {
      await this.sendSignedRequest('POST', '/fapi/v1/leverage', {
        symbol,
        leverage: lev,
      });
      this.lastLeverageBySymbol.set(symbol, lev);
      this.logger.info('trade', 'Đã set leverage theo kèo', { symbol, leverage: lev, key });
      return lev;
    } catch (error) {
      this.logger.warn('trade', 'Không set được leverage, fallback leverage mặc định', {
        symbol,
        leverage: lev,
        error: error.message,
      });
      return Math.max(1, Number(this.config.binance.leverage || lev));
    }
  }

  async getFeeRates(symbol) {
    const fallbackTaker = Math.max(0, Number(this.config.trading.takerFeePct || 0.0004));
    const fallbackMaker = Math.max(0, Number(this.config.trading.makerFeePct || 0.0002));
    const cacheKey = String(symbol || '').toUpperCase();
    const now = Date.now();
    const cached = this.feeRateCache.get(cacheKey);
    if (cached && cached.expiresAt > now) {
      return {
        takerFeePct: cached.takerFeePct,
        makerFeePct: cached.makerFeePct,
        source: cached.source,
      };
    }

    if (!this.config.binance.apiKey || !this.config.binance.apiSecret) {
      return { takerFeePct: fallbackTaker, makerFeePct: fallbackMaker, source: 'CONFIG_FALLBACK' };
    }

    try {
      const feeData = await this.sendSignedRequest('GET', '/fapi/v1/commissionRate', {
        symbol: cacheKey,
      });
      const takerFeePct = Math.max(0, Number(feeData?.takerCommissionRate || fallbackTaker));
      const makerFeePct = Math.max(0, Number(feeData?.makerCommissionRate || fallbackMaker));
      this.feeRateCache.set(cacheKey, {
        takerFeePct,
        makerFeePct,
        source: 'BINANCE_COMMISSION_RATE',
        expiresAt: now + 30 * 60 * 1000,
      });
      return { takerFeePct, makerFeePct, source: 'BINANCE_COMMISSION_RATE' };
    } catch (error) {
      this.logger.warn('trade', 'Không lấy được commissionRate Binance, dùng fee cấu hình', {
        symbol: cacheKey,
        error: error.message,
      });
      this.feeRateCache.set(cacheKey, {
        takerFeePct: fallbackTaker,
        makerFeePct: fallbackMaker,
        source: 'CONFIG_FALLBACK',
        expiresAt: now + 5 * 60 * 1000,
      });
      return { takerFeePct: fallbackTaker, makerFeePct: fallbackMaker, source: 'CONFIG_FALLBACK' };
    }
  }

  applyNotionalRange(symbol, qty, entryPrice, symbolFilter) {
    const cfg = this.config.positionSizing || {};
    const enforce = cfg.enforceNotionalRange !== false;
    const profile = this.getNotionalProfile(symbol);
    if (!enforce) {
      return {
        qty,
        notional: qty * entryPrice,
        profile,
      };
    }

    const minNotional = Math.max(
      Number(profile.minNotional || 0),
      Number(this.config.risk.minNotionalUSDT || 0),
      Number(symbolFilter?.minNotional || 0),
    );
    const maxNotional = Number.isFinite(Number(profile.maxNotional))
      ? Math.max(Number(profile.maxNotional), minNotional)
      : Infinity;

    let nextQty = qty;
    let nextNotional = nextQty * entryPrice;

    if (nextNotional < minNotional) {
      nextQty = this.roundQty(symbol, minNotional / entryPrice);
      nextNotional = nextQty * entryPrice;
    }

    if (nextNotional > maxNotional) {
      nextQty = this.roundQty(symbol, maxNotional / entryPrice);
      nextNotional = nextQty * entryPrice;
    }

    return {
      qty: nextQty,
      notional: nextNotional,
      minNotional,
      maxNotional,
      profile,
    };
  }

  canOpenNewOrder(signal) {
    if (!signal || signal.side === 'NO_TRADE') {
      return { ok: false, reason: 'Không có tín hiệu để mở lệnh' };
    }
    const blockingCodes = new Set([
      'NEAR_STRONG_RESISTANCE',
      'NEAR_STRONG_SUPPORT',
      'EXTENDED_FROM_MA25',
      'OVERBOUGHT_NEAR_RESISTANCE',
      'OVERSOLD_NEAR_SUPPORT',
      'LATE_ENTRY',
      'LOW_REWARD_DISTANCE',
      'BAD_PULLBACK',
      'WEAK_PULLBACK_STRUCTURE',
      'FOMO_BREAKOUT',
      'BOS_AGAINST_TREND',
    ]);
    const rejectCodes = Array.isArray(signal.rejectCodes) ? signal.rejectCodes : [];
    const blocked = rejectCodes.find((c) => blockingCodes.has(c));
    if (blocked) {
      return { ok: false, reason: `Signal bị chặn bởi zone filter: ${blocked}` };
    }

    const status = this.stateStore.getStatus();
    if (status.pendingOrderId) {
      return { ok: false, reason: 'Đang có lệnh chờ xử lý' };
    }

    if (status.activePosition && !this.config.trading.allowMultiplePosition) {
      return { ok: false, reason: 'Đang có vị thế mở' };
    }

    const fingerprint = [
      signal.symbol,
      signal.side,
      signal.level,
      Math.round(signal.entryPrice / Math.max(signal.entryPrice * 0.0005, 0.5)),
      signal.volatilityRegime || 'NA',
      Math.round(signal.createdAt / 12000),
    ].join(':');

    const now = Date.now();
    if (
      this.lastOrderFingerprint === fingerprint &&
      now - this.lastOrderAt < this.config.trading.signalDuplicateCooldownMs
    ) {
      return { ok: false, reason: 'Duplicate order guard: tín hiệu vừa vào gần đây' };
    }

    return { ok: true, fingerprint };
  }

  signQuery(params) {
    const query = new URLSearchParams(params).toString();
    const signature = crypto
      .createHmac('sha256', this.config.binance.apiSecret)
      .update(query)
      .digest('hex');

    return `${query}&signature=${signature}`;
  }

  async sendSignedRequest(method, url, params) {
    if (!this.config.binance.apiKey || !this.config.binance.apiSecret) {
      throw new Error('Thiếu BINANCE_API_KEY hoặc BINANCE_API_SECRET');
    }

    const timestamp = Date.now();
    const query = this.signQuery({ ...params, recvWindow: this.config.binance.recvWindow, timestamp });

    const response = await this.http.request({
      method,
      url: `${url}?${query}`,
      headers: {
        'X-MBX-APIKEY': this.config.binance.apiKey,
      },
    });

    return response.data;
  }

  async ensureSymbolFilter(symbol) {
    if (this.symbolFilters.has(symbol)) return this.symbolFilters.get(symbol);

    const response = await this.http.get('/fapi/v1/exchangeInfo');
    const item = (response.data.symbols || []).find((s) => s.symbol === symbol);
    if (!item) {
      throw new Error(`Không tìm thấy exchange filter cho ${symbol}`);
    }

    const lot = (item.filters || []).find((f) => f.filterType === 'LOT_SIZE') || {};
    const priceFilter = (item.filters || []).find((f) => f.filterType === 'PRICE_FILTER') || {};
    const minNotional = (item.filters || []).find((f) => f.filterType === 'MIN_NOTIONAL') || {};

    const filter = {
      minQty: Number(lot.minQty || 0),
      maxQty: Number(lot.maxQty || 0),
      stepSize: Number(lot.stepSize || 0.001),
      tickSize: Number(priceFilter.tickSize || 0.01),
      minNotional: Number(minNotional.notional || minNotional.minNotional || 0),
    };

    this.symbolFilters.set(symbol, filter);
    return filter;
  }

  floorToStep(value, step) {
    if (!Number.isFinite(value) || !Number.isFinite(step) || step <= 0) return 0;
    const precision = Math.min(10, (step.toString().split('.')[1] || '').length);
    const floored = Math.floor(value / step) * step;
    return Number(floored.toFixed(precision));
  }

  roundQty(symbol, qty) {
    const filter = this.symbolFilters.get(symbol);
    if (!filter) {
      if (qty >= 1) return Number(qty.toFixed(3));
      if (qty >= 0.1) return Number(qty.toFixed(4));
      return Number(qty.toFixed(5));
    }

    let rounded = this.floorToStep(qty, filter.stepSize);
    if (filter.minQty && rounded < filter.minQty) rounded = filter.minQty;
    if (filter.maxQty && rounded > filter.maxQty) rounded = filter.maxQty;
    return rounded;
  }

  roundPrice(symbol, price) {
    const filter = this.symbolFilters.get(symbol);
    if (!filter?.tickSize) return Number(price.toFixed(2));
    return this.floorToStep(price, filter.tickSize);
  }

  async checkAvailableMargin(symbol, entryPrice, qty, leverage = this.config.binance.leverage) {
    if (!this.liveOrderEnabled) return { ok: true, available: Infinity, required: 0 };

    const balances = await this.sendSignedRequest('GET', '/fapi/v2/balance', {});
    const usdt = (balances || []).find((b) => b.asset === 'USDT');
    const available = Number(usdt?.availableBalance || 0);

    const notional = entryPrice * qty;
    const required = (notional / Math.max(Number(leverage || 1), 1)) * 1.1;

    return {
      ok: available >= required,
      available,
      required,
    };
  }

  async reconcileOrder(symbol, order) {
    if (!this.liveOrderEnabled || !order) return null;

    const normalize = (o) => {
      const executedQty = Number(o.executedQty || o.origQty || 0);
      const cumQuote = Number(o.cumQuote || o.cummulativeQuoteQty || 0);
      const avgPrice = Number(o.avgPrice || (executedQty > 0 ? cumQuote / executedQty : 0));
      return {
        orderId: o.orderId,
        clientOrderId: o.clientOrderId,
        status: o.status,
        executedQty,
        cumQuote,
        avgPrice,
      };
    };

    try {
      const first = normalize(order);
      if (first.status === 'FILLED' && first.executedQty > 0) return first;

      const synced = await this.sendSignedRequest('GET', '/fapi/v1/order', {
        symbol,
        orderId: order.orderId,
      });
      return normalize(synced);
    } catch (error) {
      this.logger.warn('trade', 'Không reconcile được order live', {
        symbol,
        orderId: order?.orderId,
        error: error.message,
      });
      return null;
    }
  }

  estimateFees(notional, takerFeePct = this.config.trading.takerFeePct) {
    return notional * Math.max(Number(takerFeePct || 0), 0);
  }

  calcPnlOnQty(position, price, qty) {
    if (position.side === 'LONG') return (price - position.entryPrice) * qty;
    return (position.entryPrice - price) * qty;
  }

  async placeMarketOrder(signal, options = {}) {
    if (this.riskManager.isEmergencyStop()) {
      return { ok: false, reason: 'Emergency stop đang bật' };
    }

    const openCheck = this.canOpenNewOrder(signal);
    if (!openCheck.ok) {
      return { ok: false, reason: openCheck.reason };
    }

    const signalLeverage = this.getSignalLeverage(signal);
    const feeRates = await this.getFeeRates(signal.symbol);
    const signalForExecution = {
      ...signal,
      leverage: signalLeverage,
      takerFeePct: feeRates.takerFeePct,
      makerFeePct: feeRates.makerFeePct,
      feeSource: feeRates.source,
    };

    const ob = this.stateStore.state.orderBook;
    const midPrice =
      this.config.trading.priceSource === 'MARK'
        ? ob.markPrice || ob.lastPrice || signalForExecution.entryPrice
        : ob.lastTradePrice ||
          (ob.bestAsk && ob.bestBid ? (ob.bestAsk + ob.bestBid) / 2 : signalForExecution.entryPrice);
    const entryDeviationPct = Math.abs((signalForExecution.entryPrice - midPrice) / midPrice);

    const riskCheck = this.riskManager.evaluate(signalForExecution, {
      spreadPct: this.stateStore.state.orderBook.spreadPct,
      estimatedSlippagePct: options.estimatedSlippagePct || 0,
      entryDeviationPct,
      latencyMs: options.latencyMs || 0,
    });

    if (!riskCheck.allowed) {
      await this.telegramService.sendReject({
        symbol: signalForExecution.symbol,
        reasons: riskCheck.reasons,
        codes: riskCheck.codes,
        side: signalForExecution.side,
        confidence: signalForExecution.confidence,
      });
      return { ok: false, reason: riskCheck.reasons.join('; ') };
    }

    const pendingId = uniqueId();
    this.stateStore.setPendingOrderId(pendingId);

    try {
      const equity = this.stateStore.state.risk.equity;
      let qty = this.riskManager.calcPositionSize({
        entryPrice: signalForExecution.entryPrice,
        stopLoss: signalForExecution.stopLoss,
        equity,
        riskPerTradePct: this.config.risk.defaultRiskPerTradePct,
        signal: signalForExecution,
      });

      if (!qty || qty <= 0) throw new Error('Không tính được khối lượng hợp lệ');

      await this.ensureSymbolFilter(signalForExecution.symbol);
      const symbolFilter = this.symbolFilters.get(signalForExecution.symbol);
      qty = this.roundQty(signalForExecution.symbol, qty);

      if (!qty || qty <= 0) throw new Error('Khối lượng sau khi round không hợp lệ');

      let notional = qty * signalForExecution.entryPrice;
      if (symbolFilter?.minNotional && notional < symbolFilter.minNotional) {
        qty = this.roundQty(signalForExecution.symbol, symbolFilter.minNotional / signalForExecution.entryPrice);
        notional = qty * signalForExecution.entryPrice;
      }

      const ranged = this.applyNotionalRange(signalForExecution.symbol, qty, signalForExecution.entryPrice, symbolFilter);
      qty = ranged.qty;
      notional = ranged.notional;

      if (!qty || qty <= 0) throw new Error('Khối lượng không đạt ngưỡng notional sau khi chuẩn hóa');
      if (Number.isFinite(ranged.minNotional) && notional < ranged.minNotional * 0.995) {
        throw new Error(
          `Notional ${notional.toFixed(2)} thấp hơn ngưỡng tối thiểu ${ranged.minNotional.toFixed(2)} (${ranged.profile.group})`,
        );
      }
      const marginCheck = await this.checkAvailableMargin(
        signalForExecution.symbol,
        signalForExecution.entryPrice,
        qty,
        signalLeverage,
      );
      if (!marginCheck.ok) {
        throw new Error(
          `Margin không đủ: required ${marginCheck.required.toFixed(2)} > available ${marginCheck.available.toFixed(2)}`,
        );
      }

      const orderSide = signalForExecution.side === 'LONG' ? 'BUY' : 'SELL';

      let exchangeOrder = null;
      let reconcile = null;
      let fillPrice = signalForExecution.entryPrice;
      let fillQty = qty;

      if (this.liveOrderEnabled) {
        const appliedLeverage = await this.ensureLeverage(signalForExecution.symbol, signalLeverage);
        signalForExecution.leverage = appliedLeverage;
        exchangeOrder = await this.sendSignedRequest('POST', '/fapi/v1/order', {
          symbol: signalForExecution.symbol,
          side: orderSide,
          type: 'MARKET',
          quantity: qty,
          newClientOrderId: `SPB-${Date.now()}`,
        });

        reconcile = await this.reconcileOrder(signalForExecution.symbol, exchangeOrder);
        if (reconcile?.executedQty > 0) fillQty = reconcile.executedQty;
        if (reconcile?.avgPrice > 0) fillPrice = reconcile.avgPrice;
      }

      const openFee = this.estimateFees(fillPrice * fillQty, feeRates.takerFeePct);

      const position = {
        symbol: signalForExecution.symbol,
        side: signalForExecution.side,
        entryPrice: fillPrice,
        size: fillQty,
        remainingSize: fillQty,
        leverage: signalForExecution.leverage,
        takerFeePct: feeRates.takerFeePct,
        makerFeePct: feeRates.makerFeePct,
        feeSource: feeRates.source,
        notionalProfile: ranged.profile.group,
        stopLoss: this.roundPrice(signalForExecution.symbol, signalForExecution.stopLoss),
        tp1: this.roundPrice(signalForExecution.symbol, signalForExecution.tp1),
        tp2: this.roundPrice(signalForExecution.symbol, signalForExecution.tp2),
        tp3: this.roundPrice(signalForExecution.symbol, signalForExecution.tp3),
        rr: signalForExecution.rr,
        partialClosedPct: 0,
        trailingEnabled: false,
        trailingActivatedAt: null,
        initialStopLoss: signalForExecution.stopLoss,
        initialRiskDistance: Math.abs(fillPrice - signalForExecution.stopLoss),
        trailingActivationRR: this.config.trading.trailingActivationRR,
        trailingLockRR: this.config.trading.trailingLockRR,
        state: 'OPEN',
        mode: this.stateStore.state.mode,
        exchangeOrder,
        exchangeReconcile: reconcile,
        signal: signalForExecution,
        realizedPnl: -openFee,
        feesPaid: openFee,
      };

      this.stateStore.setActivePosition(position);
      this.stateStore.setPendingOrderId(null);
      this.stateStore.setStateMachine(BOT_STATES.OPEN_POSITION);
      this.lastOrderFingerprint = openCheck.fingerprint;
      this.lastOrderAt = Date.now();

      this.emitLifecycle('ENTRY', {
        symbol: signalForExecution.symbol,
        side: signalForExecution.side,
        timeframe: signalForExecution.signalTimeframe || this.config.timeframe.analysis || '3m',
        tradeId: position.id,
        entryPrice: fillPrice,
        stopLoss: position.stopLoss,
        tp1: position.tp1,
        tp2: position.tp2,
        tp3: position.tp3,
        leverage: signalForExecution.leverage,
        confidence: signalForExecution.confidence,
        mode: this.stateStore.state.mode,
      });

      this.logger.trade('Đã mở vị thế mới', {
        symbol: signalForExecution.symbol,
        side: signalForExecution.side,
        entryPrice: fillPrice,
        size: fillQty,
        leverage: signalForExecution.leverage,
        profile: ranged.profile.group,
        liveOrderEnabled: this.liveOrderEnabled,
      });

      await this.telegramService.sendTradeOpened({
        symbol: signalForExecution.symbol,
        side: signalForExecution.side,
        entryPrice: fillPrice,
        size: fillQty,
        leverage: signalForExecution.leverage,
        reason: signalForExecution.reasons.join(' | '),
      });

      return { ok: true, position };
    } catch (error) {
      this.stateStore.setPendingOrderId(null);
      this.logger.error('trade', 'Lỗi đặt lệnh', {
        error: error.message,
        symbol: signalForExecution.symbol,
      });
      await this.telegramService.sendError({
        title: 'Lỗi đặt lệnh',
        detail: error.message,
      });

      if (this.config.risk.lockBotOnError) {
        this.stateStore.setRiskLock(true, 'ORDER_ERROR_LOCK');
      }

      return {
        ok: false,
        reason: error.message,
      };
    }
  }

  async placeReduceOnly(position, qty) {
    const roundedQty = this.roundQty(position.symbol, qty);
    if (!roundedQty || roundedQty <= 0) {
      return { ok: false, reason: 'Khối lượng reduce-only không hợp lệ', executedQty: 0, avgPrice: 0 };
    }

    if (!this.liveOrderEnabled) {
      return { ok: true, executedQty: roundedQty, avgPrice: 0 };
    }

    try {
      const closeSide = position.side === 'LONG' ? 'SELL' : 'BUY';
      const order = await this.sendSignedRequest('POST', '/fapi/v1/order', {
        symbol: position.symbol,
        side: closeSide,
        type: 'MARKET',
        quantity: roundedQty,
        reduceOnly: 'true',
        newClientOrderId: `SPB-REDUCE-${Date.now()}`,
      });
      const reconcile = await this.reconcileOrder(position.symbol, order);

      return {
        ok: true,
        executedQty: reconcile?.executedQty || roundedQty,
        avgPrice: reconcile?.avgPrice || 0,
        order,
        reconcile,
      };
    } catch (error) {
      this.logger.error('trade', 'Reduce-only thất bại', {
        symbol: position.symbol,
        error: error.message,
      });
      return { ok: false, reason: error.message, executedQty: 0, avgPrice: 0 };
    }
  }

  async fetchExchangePositionSize(symbol) {
    if (!this.liveOrderEnabled) return null;
    try {
      const data = await this.sendSignedRequest('GET', '/fapi/v2/positionRisk', { symbol });
      if (!Array.isArray(data) || !data.length) return 0;
      const amt = Number(data[0]?.positionAmt || 0);
      return Math.abs(amt);
    } catch (error) {
      this.logger.warn('trade', 'Không đọc được size vị thế từ exchange', {
        symbol,
        error: error.message,
      });
      return null;
    }
  }

  async forceClosePosition(reason = 'MANUAL_CLOSE', marketPrice = null) {
    const position = this.stateStore.state.activePosition;
    if (!position) return { ok: false, reason: 'Không có vị thế mở' };

    const closeQty = Math.max(position.remainingSize || position.size || 0, 0);
    if (!closeQty) {
      this.stateStore.clearActivePosition();
      this.stateStore.setStateMachine(BOT_STATES.CLOSED);
      this.emitLifecycle('CLOSE', {
        symbol: position.symbol,
        side: position.side,
        timeframe: position.signal?.signalTimeframe || this.config.timeframe.analysis || '3m',
        tradeId: position.id,
        reason,
        pnl: position.realizedPnl || 0,
        pnlPct: 0,
      });
      return { ok: true, pnl: position.realizedPnl || 0, pnlPct: 0 };
    }

    let closePrice = marketPrice || this.stateStore.state.orderBook.lastPrice || position.entryPrice;
    let executedQty = closeQty;

    if (this.liveOrderEnabled) {
      const closeResult = await this.placeReduceOnly(position, closeQty);
      if (!closeResult.ok) {
        return { ok: false, reason: closeResult.reason };
      }
      executedQty = closeResult.executedQty || closeQty;
      if (closeResult.avgPrice > 0) closePrice = closeResult.avgPrice;
    }

    const grossRemaining = this.calcPnlOnQty(position, closePrice, executedQty);
    const closeFee = this.estimateFees(closePrice * executedQty, position.takerFeePct);
    const totalPnl = (position.realizedPnl || 0) + grossRemaining - closeFee;
    const totalFees = (position.feesPaid || 0) + closeFee;
    const basisNotional = Math.max(position.entryPrice * position.size, 0.0000001);
    const pnlPct = (totalPnl / basisNotional) * 100;

    this.stateStore.clearActivePosition();
    this.stateStore.setStateMachine(BOT_STATES.CLOSED);

    const reasonUpper = String(reason || '').toUpperCase();
    let closeType = 'CLOSE';
    if (reasonUpper.includes('STOP')) closeType = 'SL';
    else if (reasonUpper === 'TP1' || reasonUpper === 'TP2' || reasonUpper === 'TP3') closeType = reasonUpper;
    this.emitLifecycle(closeType, {
      symbol: position.symbol,
      side: position.side,
      timeframe: position.signal?.signalTimeframe || this.config.timeframe.analysis || '3m',
      tradeId: position.id,
      reason,
      closePrice,
      pnl: totalPnl,
      pnlPct,
      fees: totalFees,
    });

    this.riskManager.onTradeClosed({
      symbol: position.symbol,
      side: position.side,
      timeframe: position.signal?.signalTimeframe || this.config.timeframe.analysis || '3m',
      setupType: position.signal?.level || 'UNKNOWN',
      regime: position.signal?.volatilityRegime || 'UNKNOWN',
      entryPrice: position.entryPrice,
      closePrice,
      pnl: totalPnl,
      pnlPct,
      reason,
      fees: totalFees,
    });

    if (this.stateStore.isCooldownActive()) {
      this.stateStore.setStateMachine(BOT_STATES.COOLDOWN);
    }

    this.logger.trade('Đã đóng vị thế', {
      symbol: position.symbol,
      side: position.side,
      pnl: totalPnl,
      pnlPct,
      reason,
      fees: totalFees,
    });

    if (totalPnl >= 0) {
      await this.telegramService.sendTakeProfit({
        symbol: position.symbol,
        side: position.side,
        tp: reason,
        pnl: totalPnl,
        pnlPct,
        remain: 'Đã đóng toàn bộ',
        trailing: position.trailingEnabled,
      });
    } else {
      await this.telegramService.sendStopLoss({
        symbol: position.symbol,
        side: position.side,
        pnl: totalPnl,
        pnlPct,
        reason,
        cooldownSec: this.config.risk.cooldownAfterLossSec,
      });
    }

    return { ok: true, pnl: totalPnl, pnlPct };
  }

  async processPartialTakeProfit(position, tpName, requestedPct, price) {
    const livePos = this.stateStore.state.activePosition;
    if (!livePos || livePos.id !== position.id) return;

    const closeQtyRaw = position.size * requestedPct;
    const closeQty = Math.min(closeQtyRaw, livePos.remainingSize || 0);
    if (!closeQty || closeQty <= 0) return;

    let exchangeBeforeSize = null;
    if (this.liveOrderEnabled) {
      exchangeBeforeSize = await this.fetchExchangePositionSize(livePos.symbol);
    }

    const reduce = await this.placeReduceOnly(livePos, closeQty);
    if (!reduce.ok) {
      await this.telegramService.sendError({
        title: `Lỗi chốt ${tpName}`,
        detail: reduce.reason,
      });
      return;
    }

    let exchangeDelta = null;
    if (this.liveOrderEnabled && Number.isFinite(exchangeBeforeSize)) {
      await new Promise((resolve) => setTimeout(resolve, 220));
      const exchangeAfterSize = await this.fetchExchangePositionSize(livePos.symbol);
      if (Number.isFinite(exchangeAfterSize)) {
        exchangeDelta = Math.max(exchangeBeforeSize - exchangeAfterSize, 0);
      }
    }

    let executedQtyRaw = reduce.executedQty || closeQty;
    if (this.liveOrderEnabled && Number.isFinite(exchangeDelta)) {
      // Ưu tiên delta thực tế từ exchange để tránh state nội bộ lệch với sàn.
      executedQtyRaw = Math.max(executedQtyRaw, exchangeDelta);
    }

    const executedQty = Math.min(executedQtyRaw, livePos.remainingSize || closeQty);
    if (this.liveOrderEnabled && (!Number.isFinite(executedQty) || executedQty <= 0)) {
      await this.telegramService.sendError({
        title: `Lỗi xác minh ${tpName}`,
        detail: 'Reduce-only không giảm được vị thế trên exchange, bỏ qua cập nhật state nội bộ',
      });
      this.logger.error('trade', 'Partial TP không khớp exchange', {
        symbol: livePos.symbol,
        tpName,
        closeQty,
        reduceExecutedQty: reduce.executedQty,
        exchangeDelta,
      });
      return;
    }

    const execPrice = reduce.avgPrice > 0 ? reduce.avgPrice : price;
    const gross = this.calcPnlOnQty(livePos, execPrice, executedQty);
    const fee = this.estimateFees(execPrice * executedQty, livePos.takerFeePct);
    const partPnl = gross - fee;

    const nextRemaining = Math.max((livePos.remainingSize || 0) - executedQty, 0);
    const closedPct = ((livePos.size - nextRemaining) / livePos.size) * 100;

    const update = {
      remainingSize: nextRemaining,
      partialClosedPct: closedPct,
      realizedPnl: (livePos.realizedPnl || 0) + partPnl,
      feesPaid: (livePos.feesPaid || 0) + fee,
    };

    if (tpName === 'TP1') {
      update.stopLoss = livePos.entryPrice;
    }

    if (tpName === 'TP2') {
      update.trailingEnabled = true;
      update.trailingActivatedAt = Date.now();
    }

    this.stateStore.updateActivePosition(update);
    this.stateStore.setStateMachine(BOT_STATES.PARTIAL_TP);

    this.emitLifecycle(tpName, {
      symbol: livePos.symbol,
      side: livePos.side,
      timeframe: livePos.signal?.signalTimeframe || this.config.timeframe.analysis || '3m',
      tradeId: livePos.id,
      tp: tpName,
      entryPrice: livePos.entryPrice,
      closePrice: execPrice,
      pnl: partPnl,
      pnlPct: (partPnl / Math.max(livePos.entryPrice * livePos.size, 0.000001)) * 100,
      closedPct,
      remainingSize: nextRemaining,
      stopLoss: update.stopLoss || livePos.stopLoss,
    });

    const remainPct = Math.max(0, 100 - closedPct);
    await this.telegramService.sendTakeProfit({
      symbol: livePos.symbol,
      side: livePos.side,
      tp: tpName,
      pnl: partPnl,
      pnlPct: (partPnl / Math.max(livePos.entryPrice * livePos.size, 0.000001)) * 100,
      remain: `Còn ${remainPct.toFixed(1)}% vị thế`,
      trailing: tpName === 'TP2' || livePos.trailingEnabled,
    });
  }

  async onPriceTick(price) {
    const position = this.stateStore.state.activePosition;
    if (!position || position.state !== 'OPEN') return;
    if (position.requiresManualPlan) {
      this.stateStore.setStateMachine(BOT_STATES.PAUSED_BY_RISK);
      if (!position.syncNoPlanWarned) {
        this.stateStore.updateActivePosition({ syncNoPlanWarned: true });
        this.logger.warn('trade', 'Vị thế sync chưa có trade plan, tạm ngưng quản lý auto', {
          symbol: position.symbol,
          side: position.side,
        });
      }
      return;
    }
    if (
      !Number.isFinite(position.stopLoss) ||
      !Number.isFinite(position.tp1) ||
      !Number.isFinite(position.tp2) ||
      !Number.isFinite(position.tp3)
    ) {
      this.stateStore.setStateMachine(BOT_STATES.PAUSED_BY_RISK);
      this.logger.warn('trade', 'Thiếu SL/TP hợp lệ, dừng quản lý vị thế để tránh xử lý sai', {
        symbol: position.symbol,
        side: position.side,
      });
      return;
    }
    this.stateStore.setStateMachine(BOT_STATES.MANAGING_POSITION);

    const side = position.side;
    const hitStop = side === 'LONG' ? price <= position.stopLoss : price >= position.stopLoss;
    const hitTp1 = side === 'LONG' ? price >= position.tp1 : price <= position.tp1;
    const hitTp2 = side === 'LONG' ? price >= position.tp2 : price <= position.tp2;
    const hitTp3 = side === 'LONG' ? price >= position.tp3 : price <= position.tp3;

    if (hitStop) {
      await this.forceClosePosition('STOP_LOSS', price);
      return;
    }

    if (hitTp1 && position.partialClosedPct < 30) {
      await this.processPartialTakeProfit(position, 'TP1', 0.3, price);
    }

    const pAfterTp1 = this.stateStore.state.activePosition;
    if (!pAfterTp1) return;

    if (hitTp2 && pAfterTp1.partialClosedPct < 60) {
      await this.processPartialTakeProfit(pAfterTp1, 'TP2', 0.3, price);
    }

    const livePosition = this.stateStore.state.activePosition;
    if (!livePosition) return;

    const profitDistance = side === 'LONG' ? price - livePosition.entryPrice : livePosition.entryPrice - price;
    const activationDistance =
      livePosition.initialRiskDistance * Math.max(livePosition.trailingActivationRR || 1.5, 0.1);

    if (!livePosition.trailingEnabled && profitDistance >= activationDistance) {
      this.stateStore.updateActivePosition({
        trailingEnabled: true,
        trailingActivatedAt: Date.now(),
      });
    }

    const trailingPosition = this.stateStore.state.activePosition;
    if (trailingPosition?.trailingEnabled) {
      const trailGap = trailingPosition.initialRiskDistance * Math.max(trailingPosition.trailingLockRR || 0.7, 0.2);
      if (side === 'LONG') {
        const newStop = Math.max(trailingPosition.stopLoss, price - trailGap, trailingPosition.entryPrice);
        this.stateStore.updateActivePosition({ stopLoss: newStop });
      } else {
        const newStop = Math.min(trailingPosition.stopLoss, price + trailGap, trailingPosition.entryPrice);
        this.stateStore.updateActivePosition({ stopLoss: newStop });
      }
    }

    if (hitTp3) {
      await this.forceClosePosition('TP3', price);
      return;
    }

    const checkPos = this.stateStore.state.activePosition;
    if (checkPos && (checkPos.remainingSize || 0) <= 0) {
      await this.forceClosePosition('FLAT_AFTER_PARTIAL', price);
    }
  }

  async syncPosition() {
    if (!this.liveOrderEnabled) return;

    try {
      const data = await this.sendSignedRequest('GET', '/fapi/v2/positionRisk', {
        symbol: this.stateStore.state.symbol,
      });

      if (!Array.isArray(data) || !data.length) return;
      const p = data[0];
      const amt = Number(p.positionAmt || 0);
      const feeRates = await this.getFeeRates(p.symbol || this.stateStore.state.symbol);
      if (amt === 0) {
        if (this.stateStore.state.activePosition?.symbol === this.stateStore.state.symbol) {
          this.logger.warn('trade', 'Exchange không còn vị thế, clear trạng thái local', {
            symbol: this.stateStore.state.symbol,
          });
          this.stateStore.clearActivePosition();
          this.stateStore.setStateMachine(BOT_STATES.CLOSED);
          this.stateStore.setStateMachine(BOT_STATES.IDLE);
        }
        return;
      }

      const side = amt > 0 ? 'LONG' : 'SHORT';
      const size = Math.abs(amt);
      const entry = Number(p.entryPrice);
      const existing = this.stateStore.state.activePosition;

      if (existing && existing.symbol === p.symbol && existing.side === side) {
        this.stateStore.setActivePosition({
          ...existing,
          symbol: p.symbol,
          side,
          entryPrice: entry,
          size,
          remainingSize: Math.min(existing.remainingSize || size, size),
          leverage: Number(p.leverage || existing.leverage || this.config.binance.leverage),
          takerFeePct: Number(existing.takerFeePct || feeRates.takerFeePct),
          makerFeePct: Number(existing.makerFeePct || feeRates.makerFeePct),
          feeSource: existing.feeSource || feeRates.source,
          synced: true,
        });
        this.logger.trade('Đồng bộ position với state hiện có', { side, symbol: p.symbol, size });
        return;
      }

      const syncAction = String(this.config.risk.syncUnknownPositionAction || 'PAUSE').toUpperCase();
      const syncedPosition = {
        symbol: p.symbol,
        side,
        entryPrice: entry,
        size,
        remainingSize: size,
        leverage: Number(p.leverage || this.config.binance.leverage),
        takerFeePct: feeRates.takerFeePct,
        makerFeePct: feeRates.makerFeePct,
        feeSource: feeRates.source,
        stopLoss: null,
        tp1: null,
        tp2: null,
        tp3: null,
        partialClosedPct: 0,
        trailingEnabled: false,
        trailingActivatedAt: null,
        initialStopLoss: null,
        initialRiskDistance: null,
        trailingActivationRR: this.config.trading.trailingActivationRR,
        trailingLockRR: this.config.trading.trailingLockRR,
        state: 'OPEN',
        synced: true,
        syncedWithoutPlan: true,
        requiresManualPlan: true,
        syncNoPlanWarned: false,
        realizedPnl: 0,
        feesPaid: 0,
      };

      this.stateStore.setActivePosition(syncedPosition);
      this.stateStore.setRiskLock(true, 'SYNC_POSITION_NO_PLAN');
      this.stateStore.setStateMachine(BOT_STATES.PAUSED_BY_RISK);
      this.logger.warn('trade', 'Sync position không có trade plan local, chuyển sang chế độ an toàn', {
        side,
        symbol: p.symbol,
        size,
        action: syncAction,
      });
      await this.telegramService.sendError({
        title: 'Sync position cần xác nhận thủ công',
        detail: `Phát hiện vị thế ${side} ${p.symbol} nhưng không có SL/TP local. Action=${syncAction}`,
      });

      if (syncAction === 'CLOSE') {
        const markPrice = Number(p.markPrice || this.stateStore.state.orderBook.markPrice || entry);
        await this.forceClosePosition('SYNC_NO_PLAN_CLOSE', markPrice);
      }

      if (syncAction === 'KEEP') {
        this.stateStore.setRiskLock(false, 'SYNC_POSITION_KEEP');
        this.stateStore.setStateMachine(BOT_STATES.MANAGING_POSITION);
      }

      if (syncAction === 'PAUSE') {
        return;
      }

      if (syncAction !== 'KEEP' && syncAction !== 'CLOSE') {
        return;
      }

      if (syncAction === 'CLOSE') {
        return;
      }

      this.logger.trade('Đồng bộ position fallback sau reconnect', {
        side,
        symbol: p.symbol,
        size,
        action: syncAction,
      });
      return;
    } catch (error) {
      this.logger.error('trade', 'Lỗi sync position', { error: error.message });
    }
  }
}

module.exports = OrderManager;
