const { uniqueId, nowTs } = require('./utils');

const BOT_STATES = {
  IDLE: 'IDLE',
  ANALYZING: 'ANALYZING',
  SIGNAL_WEAK: 'SIGNAL_WEAK',
  SIGNAL_MEDIUM: 'SIGNAL_MEDIUM',
  SIGNAL_STRONG: 'SIGNAL_STRONG',
  WAIT_CONFIRM: 'WAIT_CONFIRM',
  OPEN_POSITION: 'OPEN_POSITION',
  MANAGING_POSITION: 'MANAGING_POSITION',
  PARTIAL_TP: 'PARTIAL_TP',
  CLOSED: 'CLOSED',
  COOLDOWN: 'COOLDOWN',
  PAUSED_BY_RISK: 'PAUSED_BY_RISK',
  PAUSED_BY_SIDEWAY: 'PAUSED_BY_SIDEWAY',
  ERROR: 'ERROR',
};

const STATE_TRANSITIONS = {
  [BOT_STATES.IDLE]: [
    BOT_STATES.ANALYZING,
    BOT_STATES.SIGNAL_WEAK,
    BOT_STATES.SIGNAL_MEDIUM,
    BOT_STATES.SIGNAL_STRONG,
    BOT_STATES.WAIT_CONFIRM,
    BOT_STATES.OPEN_POSITION,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.PAUSED_BY_SIDEWAY,
    BOT_STATES.COOLDOWN,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.ANALYZING]: [
    BOT_STATES.IDLE,
    BOT_STATES.SIGNAL_WEAK,
    BOT_STATES.SIGNAL_MEDIUM,
    BOT_STATES.SIGNAL_STRONG,
    BOT_STATES.PAUSED_BY_SIDEWAY,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.SIGNAL_WEAK]: [
    BOT_STATES.IDLE,
    BOT_STATES.ANALYZING,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.PAUSED_BY_SIDEWAY,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.SIGNAL_MEDIUM]: [
    BOT_STATES.WAIT_CONFIRM,
    BOT_STATES.IDLE,
    BOT_STATES.ANALYZING,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.PAUSED_BY_SIDEWAY,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.SIGNAL_STRONG]: [
    BOT_STATES.WAIT_CONFIRM,
    BOT_STATES.OPEN_POSITION,
    BOT_STATES.IDLE,
    BOT_STATES.ANALYZING,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.PAUSED_BY_SIDEWAY,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.WAIT_CONFIRM]: [
    BOT_STATES.OPEN_POSITION,
    BOT_STATES.IDLE,
    BOT_STATES.COOLDOWN,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.OPEN_POSITION]: [
    BOT_STATES.MANAGING_POSITION,
    BOT_STATES.PARTIAL_TP,
    BOT_STATES.CLOSED,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.MANAGING_POSITION]: [
    BOT_STATES.PARTIAL_TP,
    BOT_STATES.CLOSED,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.PARTIAL_TP]: [
    BOT_STATES.MANAGING_POSITION,
    BOT_STATES.CLOSED,
    BOT_STATES.PAUSED_BY_RISK,
    BOT_STATES.ERROR,
  ],
  [BOT_STATES.CLOSED]: [BOT_STATES.COOLDOWN, BOT_STATES.IDLE, BOT_STATES.ANALYZING, BOT_STATES.ERROR],
  [BOT_STATES.COOLDOWN]: [BOT_STATES.IDLE, BOT_STATES.ANALYZING, BOT_STATES.PAUSED_BY_RISK, BOT_STATES.ERROR],
  [BOT_STATES.PAUSED_BY_RISK]: [BOT_STATES.IDLE, BOT_STATES.COOLDOWN, BOT_STATES.ERROR],
  [BOT_STATES.PAUSED_BY_SIDEWAY]: [BOT_STATES.IDLE, BOT_STATES.ANALYZING, BOT_STATES.PAUSED_BY_RISK, BOT_STATES.ERROR],
  [BOT_STATES.ERROR]: [BOT_STATES.IDLE, BOT_STATES.PAUSED_BY_RISK],
};

class StateStore {
  constructor(config, logger = null) {
    this.config = config;
    this.logger = logger;

    this.state = {
      botRunning: config.mode === 'SIGNAL_ONLY',
      mode: config.mode,
      stateMachine: BOT_STATES.IDLE,
      symbol: config.binance.symbol,
      selectedTimeframe: config.timeframe.analysis,
      lastSignal: null,
      lastReject: null,
      reconnectCount: 0,
      invalidTransitionCount: 0,
      lastStateTransition: null,
      pauseReason: null,
      startedAt: null,
      lastError: null,
      cooldownUntil: 0,
      manualPendingSignal: null,
      activePosition: null,
      pendingOrderId: null,
      signals: [],
      market: {},
      risk: {
        dailyPnl: 0,
        weeklyPnl: 0,
        totalPnl: 0,
        dayStartEquity: config.risk.initialCapital,
        weekStartEquity: config.risk.initialCapital,
        equity: config.risk.initialCapital,
        peakEquity: config.risk.initialCapital,
        consecutiveLosses: 0,
        consecutiveWins: 0,
        consecutiveErrors: 0,
        lastErrorAt: 0,
        lockedByRisk: false,
        tradeHistory: [],
      },
      stats: {
        totalSignals: 0,
        totalTrades: 0,
        wins: 0,
        losses: 0,
        largestWin: 0,
        largestLoss: 0,
        averagePnl: 0,
        longestWinStreak: 0,
        longestLossStreak: 0,
        rejectedTrades: 0,
        rejectedByCode: {},
        sidewayDetected: 0,
        pauseCount: 0,
        reconnectCount: 0,
      },
      candles: {},
      orderBook: {
        spreadPct: 0,
        bestBid: 0,
        bestAsk: 0,
        lastPrice: 0,
        lastTradePrice: 0,
        markPrice: 0,
        indexPrice: 0,
        fundingRate: 0,
      },
      signalOutcomes: {
        active: {},
        history: [],
      },
    };

    this.initializeSymbolBuckets(config.binance.symbols);
  }

  initializeSymbolBuckets(symbols) {
    const analysisTf = this.config.timeframe.analysis || '3m';
    for (const symbol of symbols) {
      this.state.candles[symbol] = { '1m': [], [analysisTf]: [], '5m': [], '15m': [] };
      this.state.market[symbol] = {
        trend: 'UNKNOWN',
        sideway: false,
        indicators: {},
        pressure: {},
      };
    }
  }

  hydrate(snapshot) {
    if (!snapshot || typeof snapshot !== 'object') return false;

    const next = { ...snapshot };
    const analysisTf = this.config.timeframe.analysis || '3m';

    next.botRunning = (next.mode || this.config.mode) === 'SIGNAL_ONLY';
    next.pendingOrderId = null;
    if (!next.selectedTimeframe || String(next.selectedTimeframe).toLowerCase() === '2m') {
      next.selectedTimeframe = analysisTf;
    }
    next.stateMachine = Object.values(BOT_STATES).includes(next.stateMachine)
      ? next.stateMachine
      : BOT_STATES.IDLE;

    if (!next.candles || typeof next.candles !== 'object') next.candles = {};
    if (!next.market || typeof next.market !== 'object') next.market = {};

    for (const symbol of Object.keys(next.candles)) {
      const bucket = next.candles[symbol];
      if (!bucket || typeof bucket !== 'object') continue;
      if (bucket['2m'] && !bucket[analysisTf]) {
        bucket[analysisTf] = bucket['2m'];
      }
      delete bucket['2m'];
    }

    this.initializeSymbolBuckets(this.config.binance.symbols);

    this.state = {
      ...this.state,
      ...next,
      risk: {
        ...this.state.risk,
        ...(next.risk || {}),
        tradeHistory: Array.isArray(next.risk?.tradeHistory) ? next.risk.tradeHistory.slice(0, 1200) : [],
      },
      stats: {
        ...this.state.stats,
        ...(next.stats || {}),
        rejectedByCode: { ...(next.stats?.rejectedByCode || {}) },
      },
      candles: {
        ...this.state.candles,
        ...(next.candles || {}),
      },
      market: {
        ...this.state.market,
        ...(next.market || {}),
      },
      orderBook: {
        ...this.state.orderBook,
        ...(next.orderBook || {}),
      },
      signalOutcomes: {
        active: { ...(next.signalOutcomes?.active || {}) },
        history: Array.isArray(next.signalOutcomes?.history) ? next.signalOutcomes.history.slice(0, 4000) : [],
      },
    };

    for (const symbol of Object.keys(this.state.candles || {})) {
      const tfBuckets = this.state.candles[symbol] || {};
      for (const tf of Object.keys(tfBuckets)) {
        tfBuckets[tf] = this.normalizeCandles(tfBuckets[tf], this.config.timeframe.limit || 600);
      }
      this.state.candles[symbol] = tfBuckets;
    }

    if (!this.state.risk.dayStartEquity) this.state.risk.dayStartEquity = this.state.risk.equity;
    if (!this.state.risk.weekStartEquity) this.state.risk.weekStartEquity = this.state.risk.equity;

    return true;
  }

  canTransition(nextState) {
    const current = this.state.stateMachine;
    if (!Object.values(BOT_STATES).includes(nextState)) return false;
    if (current === nextState) return true;
    if (nextState === BOT_STATES.PAUSED_BY_RISK || nextState === BOT_STATES.ERROR) return true;
    return (STATE_TRANSITIONS[current] || []).includes(nextState);
  }

  setBotRunning(value) {
    this.state.botRunning = value;
    if (value && !this.state.startedAt) this.state.startedAt = nowTs();
  }

  setMode(mode) {
    this.state.mode = mode;
  }

  setStateMachine(nextState, meta = {}) {
    const fromState = this.state.stateMachine;
    if (!this.canTransition(nextState)) {
      this.state.invalidTransitionCount += 1;
      this.state.lastStateTransition = {
        from: fromState,
        to: nextState,
        ok: false,
        at: nowTs(),
        meta,
      };
      this.logger?.warn('state', 'Chan chuyen state khong hop le', {
        from: fromState,
        to: nextState,
        ...meta,
      });
      return false;
    }
    this.state.stateMachine = nextState;
    this.state.lastStateTransition = {
      from: fromState,
      to: nextState,
      ok: true,
      at: nowTs(),
      meta,
    };
    return true;
  }

  setSymbol(symbol) {
    this.state.symbol = symbol;
  }

  setTimeframe(tf) {
    this.state.selectedTimeframe = tf;
  }

  normalizeCandles(candles, maxLen = 600) {
    if (!Array.isArray(candles) || candles.length === 0) return [];
    const byTime = new Map();
    for (const c of candles) {
      const t = Number(c?.time);
      if (!Number.isFinite(t)) continue;
      byTime.set(t, { ...c, time: t });
    }
    const normalized = Array.from(byTime.values()).sort((a, b) => a.time - b.time);
    if (normalized.length > maxLen) {
      return normalized.slice(normalized.length - maxLen);
    }
    return normalized;
  }

  addCandle(symbol, timeframe, candle, maxLen = 600) {
    if (!this.state.candles[symbol]) {
      const analysisTf = this.config.timeframe.analysis || '3m';
      this.state.candles[symbol] = { '1m': [], [analysisTf]: [], '5m': [], '15m': [] };
    }
    const bucket = this.state.candles[symbol][timeframe] || [];
    const cTime = Number(candle?.time);
    if (!Number.isFinite(cTime)) return;

    const last = bucket[bucket.length - 1];
    if (!last || cTime > last.time) {
      bucket.push({ ...candle, time: cTime });
    } else if (cTime === last.time) {
      bucket[bucket.length - 1] = { ...candle, time: cTime };
    } else {
      const idx = bucket.findIndex((c) => c.time === cTime);
      if (idx >= 0) bucket[idx] = { ...candle, time: cTime };
      else bucket.push({ ...candle, time: cTime });
    }

    this.state.candles[symbol][timeframe] = this.normalizeCandles(bucket, maxLen);
  }

  setCandles(symbol, timeframe, candles, maxLen = 600) {
    if (!this.state.candles[symbol]) {
      const analysisTf = this.config.timeframe.analysis || '3m';
      this.state.candles[symbol] = { '1m': [], [analysisTf]: [], '5m': [], '15m': [] };
    }
    this.state.candles[symbol][timeframe] = this.normalizeCandles(candles, maxLen);
  }

  getCandles(symbol, timeframe) {
    return (this.state.candles[symbol] && this.state.candles[symbol][timeframe]) || [];
  }

  setLastSignal(signal) {
    this.state.lastSignal = signal;
    this.state.signals.unshift(signal);
    if (this.state.signals.length > 300) {
      this.state.signals = this.state.signals.slice(0, 300);
    }
    this.state.stats.totalSignals += 1;
  }

  setLastReject(reject) {
    this.state.lastReject = reject;
    this.state.stats.rejectedTrades += 1;
    const codeList = Array.isArray(reject?.codes) && reject.codes.length
      ? reject.codes
      : [reject?.code || reject?.signal?.rejectCode || 'UNKNOWN'];
    for (const code of codeList) {
      this.state.stats.rejectedByCode[code] = (this.state.stats.rejectedByCode[code] || 0) + 1;
    }
  }

  setMarket(symbol, payload) {
    this.state.market[symbol] = {
      ...this.state.market[symbol],
      ...payload,
    };
  }

  setOrderBook(orderBook) {
    this.state.orderBook = { ...this.state.orderBook, ...orderBook };
  }

  upsertSignalOutcomeTrack(track) {
    if (!track || !track.tradeId) return;
    if (!this.state.signalOutcomes || typeof this.state.signalOutcomes !== 'object') {
      this.state.signalOutcomes = { active: {}, history: [] };
    }
    if (!this.state.signalOutcomes.active || typeof this.state.signalOutcomes.active !== 'object') {
      this.state.signalOutcomes.active = {};
    }
    if (!Array.isArray(this.state.signalOutcomes.history)) {
      this.state.signalOutcomes.history = [];
    }

    const prev = this.state.signalOutcomes.active[track.tradeId];
    this.state.signalOutcomes.active[track.tradeId] = {
      ...(prev || {}),
      ...track,
      tradeId: String(track.tradeId),
      sentAt: Number(track.sentAt || prev?.sentAt || nowTs()),
      expiresAt: Number(track.expiresAt || prev?.expiresAt || nowTs() + 60 * 60 * 1000),
      status: track.status || prev?.status || 'OPEN',
      updatedAt: Number(track.updatedAt || nowTs()),
      open: true,
    };
  }

  resolveSignalOutcomeTrack(tradeId, resolution = {}) {
    const id = String(tradeId || '').trim();
    if (!id) return null;
    if (!this.state.signalOutcomes?.active?.[id]) return null;

    const active = this.state.signalOutcomes.active[id];
    const resolvedAt = Number(resolution.resolvedAt || nowTs());
    const row = {
      ...active,
      ...resolution,
      tradeId: id,
      open: false,
      status: resolution.status || active.status || 'UNKNOWN',
      resolvedAt,
      updatedAt: resolvedAt,
    };

    delete this.state.signalOutcomes.active[id];
    this.state.signalOutcomes.history.unshift(row);
    if (this.state.signalOutcomes.history.length > 4000) {
      this.state.signalOutcomes.history = this.state.signalOutcomes.history.slice(0, 4000);
    }
    return row;
  }

  pruneSignalOutcomes(now = nowTs()) {
    if (!this.state.signalOutcomes?.active) return;
    for (const [tradeId, track] of Object.entries(this.state.signalOutcomes.active)) {
      const ts = Number(track?.updatedAt || track?.sentAt || 0);
      if (!ts || now - ts > 3 * 24 * 60 * 60 * 1000) {
        delete this.state.signalOutcomes.active[tradeId];
      }
    }

    if (!Array.isArray(this.state.signalOutcomes.history)) return;
    const cutoff = now - 10 * 24 * 60 * 60 * 1000;
    this.state.signalOutcomes.history = this.state.signalOutcomes.history
      .filter((item) => Number(item?.sentAt || item?.resolvedAt || 0) >= cutoff)
      .slice(0, 4000);
  }

  getSignalOutcomeSnapshot() {
    const active = this.state.signalOutcomes?.active || {};
    const history = Array.isArray(this.state.signalOutcomes?.history) ? this.state.signalOutcomes.history : [];
    return {
      active: { ...active },
      history: history.slice(),
    };
  }

  getSignalOutcomeSummary() {
    const active = this.state.signalOutcomes?.active || {};
    const history = Array.isArray(this.state.signalOutcomes?.history) ? this.state.signalOutcomes.history : [];
    const totalResolved = history.length;
    const wins = history.filter((x) => String(x?.status || '').toUpperCase().startsWith('WIN')).length;
    const losses = history.filter((x) => String(x?.status || '').toUpperCase().startsWith('LOSS')).length;
    const expired = history.filter((x) => String(x?.status || '').toUpperCase() === 'EXPIRED').length;
    return {
      active: Object.keys(active).length,
      totalResolved,
      wins,
      losses,
      expired,
      winRate: wins + losses > 0 ? (wins / (wins + losses)) * 100 : 0,
    };
  }

  registerReconnect() {
    this.state.reconnectCount += 1;
    this.state.stats.reconnectCount += 1;
  }

  setPause(reason) {
    this.state.pauseReason = reason;
    if (reason) this.state.stats.pauseCount += 1;
  }

  setCooldown(seconds) {
    this.state.cooldownUntil = nowTs() + seconds * 1000;
  }

  clearCooldown() {
    this.state.cooldownUntil = 0;
  }

  isCooldownActive() {
    return this.state.cooldownUntil > nowTs();
  }

  setActivePosition(position) {
    if (!position) {
      this.state.activePosition = null;
      return;
    }

    const prev = this.state.activePosition;
    const keepIdentity =
      prev &&
      prev.symbol === position.symbol &&
      prev.side === position.side &&
      prev.state !== 'CLOSED';

    const id = position.id || (keepIdentity ? prev.id : uniqueId());
    const openedAt = position.openedAt || (keepIdentity ? prev.openedAt : nowTs());

    this.state.activePosition = {
      id,
      openedAt,
      ...position,
      id,
      openedAt,
      remainingSize:
        typeof position.remainingSize === 'number'
          ? position.remainingSize
          : typeof position.size === 'number'
          ? position.size
          : keepIdentity
          ? prev.remainingSize
          : 0,
    };
  }

  updateActivePosition(partial) {
    if (!this.state.activePosition) return;
    this.state.activePosition = { ...this.state.activePosition, ...partial };
  }

  clearActivePosition() {
    this.state.activePosition = null;
  }

  setPendingOrderId(id) {
    this.state.pendingOrderId = id;
  }

  recordTradeResult(result) {
    const pnl = Number(result.pnl || 0);
    const risk = this.state.risk;

    risk.totalPnl += pnl;
    risk.dailyPnl += pnl;
    risk.weeklyPnl += pnl;
    risk.equity += pnl;
    risk.peakEquity = Math.max(risk.peakEquity, risk.equity);
    risk.tradeHistory.unshift({ ...result, timestamp: nowTs() });
    if (risk.tradeHistory.length > 1200) {
      risk.tradeHistory = risk.tradeHistory.slice(0, 1200);
    }

    this.state.stats.totalTrades += 1;
    if (pnl >= 0) {
      this.state.stats.wins += 1;
      risk.consecutiveWins += 1;
      risk.consecutiveLosses = 0;
      this.state.stats.longestWinStreak = Math.max(this.state.stats.longestWinStreak, risk.consecutiveWins);
      this.state.stats.largestWin = Math.max(this.state.stats.largestWin, pnl);
    } else {
      this.state.stats.losses += 1;
      risk.consecutiveLosses += 1;
      risk.consecutiveWins = 0;
      this.state.stats.longestLossStreak = Math.max(this.state.stats.longestLossStreak, risk.consecutiveLosses);
      this.state.stats.largestLoss = Math.min(this.state.stats.largestLoss, pnl);
    }

    const trades = this.state.stats.totalTrades;
    this.state.stats.averagePnl = trades > 0 ? risk.totalPnl / trades : 0;
  }

  markSidewayDetected() {
    this.state.stats.sidewayDetected += 1;
  }

  setManualPendingSignal(signal) {
    this.state.manualPendingSignal = signal;
  }

  clearManualPendingSignal() {
    this.state.manualPendingSignal = null;
  }

  setRiskLock(locked, reason = null) {
    this.state.risk.lockedByRisk = locked;
    if (locked) this.setPause(reason || 'LOCKED_BY_RISK');
    if (!locked) {
      this.state.pauseReason = null;
    }
  }

  setError(errorMessage) {
    this.state.lastError = errorMessage;
  }

  clearDailyStats() {
    this.state.risk.dailyPnl = 0;
    this.state.risk.dayStartEquity = this.state.risk.equity;
  }

  clearWeeklyStats() {
    this.state.risk.weeklyPnl = 0;
    this.state.risk.weekStartEquity = this.state.risk.equity;
  }

  getStatus() {
    return {
      botRunning: this.state.botRunning,
      mode: this.state.mode,
      stateMachine: this.state.stateMachine,
      symbol: this.state.symbol,
      selectedTimeframe: this.state.selectedTimeframe,
      pauseReason: this.state.pauseReason,
      cooldownUntil: this.state.cooldownUntil,
      cooldownActive: this.isCooldownActive(),
      reconnectCount: this.state.reconnectCount,
      invalidTransitionCount: this.state.invalidTransitionCount,
      lastStateTransition: this.state.lastStateTransition,
      activePosition: this.state.activePosition,
      pendingOrderId: this.state.pendingOrderId,
      risk: this.state.risk,
      stats: this.state.stats,
      orderBook: this.state.orderBook,
      lastSignal: this.state.lastSignal,
      lastReject: this.state.lastReject,
      lastError: this.state.lastError,
      signalOutcomes: this.getSignalOutcomeSummary(),
    };
  }
}

module.exports = {
  StateStore,
  BOT_STATES,
};
