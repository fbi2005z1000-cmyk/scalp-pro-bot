const dayjs = require('dayjs');
const { BOT_STATES } = require('./stateStore');

class RiskManager {
  constructor(config, stateStore, logger) {
    this.config = config;
    this.stateStore = stateStore;
    this.logger = logger;
  }

  countTradesInWindow(trades, minutes) {
    const since = Date.now() - minutes * 60 * 1000;
    return trades.filter((t) => t.timestamp >= since).length;
  }

  countTradesToday(trades) {
    const start = dayjs().startOf('day').valueOf();
    return trades.filter((t) => t.timestamp >= start).length;
  }

  calcDrawdownPct() {
    const { equity, peakEquity } = this.stateStore.state.risk;
    if (!peakEquity) return 0;
    return (peakEquity - equity) / peakEquity;
  }

  calcDailyLossPct() {
    const { dailyPnl, dayStartEquity, equity } = this.stateStore.state.risk;
    const base = Math.max(dayStartEquity || equity || this.config.risk.initialCapital || 1, 1);
    return Math.abs(Math.min(dailyPnl, 0)) / base;
  }

  isEmergencyStop() {
    return Boolean(this.config.risk.emergencyStop || this.stateStore.state.risk.lockedByRisk);
  }

  activateEmergencyStop(reason, detail = '') {
    if (this.config.risk.emergencyStop && this.stateStore.state.risk.lockedByRisk) return;
    this.config.risk.emergencyStop = true;
    this.stateStore.setRiskLock(true, reason || 'EMERGENCY_STOP');
    if (!this.config.risk.keepRunningOnRiskLock) {
      this.stateStore.setBotRunning(false);
    }
    this.stateStore.setStateMachine(BOT_STATES.PAUSED_BY_RISK);
    this.logger.risk('Kích hoạt emergency stop', { reason, detail });
  }

  releaseEmergencyStop() {
    this.config.risk.emergencyStop = false;
    this.stateStore.setRiskLock(false, 'EMERGENCY_STOP_RELEASED');
    this.stateStore.state.pauseReason = null;
    this.stateStore.state.risk.consecutiveErrors = 0;
  }

  registerError(context = 'UNKNOWN', errorMessage = '') {
    this.stateStore.state.risk.consecutiveErrors += 1;
    this.stateStore.state.risk.lastErrorAt = Date.now();

    const streak = this.stateStore.state.risk.consecutiveErrors;
    this.logger.error('risk', 'Lỗi vận hành bot', {
      context,
      errorMessage,
      consecutiveErrors: streak,
    });

    if (
      this.config.risk.emergencyStopOnConsecutiveErrors &&
      streak >= this.config.risk.maxConsecutiveErrors
    ) {
      this.activateEmergencyStop('CONSECUTIVE_ERRORS', `streak=${streak}`);
    }
  }

  registerHealthyTick() {
    if (this.stateStore.state.risk.consecutiveErrors > 0) {
      this.stateStore.state.risk.consecutiveErrors -= 1;
    }
  }

  getRiskClusterId(symbol) {
    const s = String(symbol || '').toUpperCase();
    const high = new Set(this.config.positionSizing?.highLiqSymbols || []);
    const scp = new Set(this.config.positionSizing?.scpSymbols || []);
    if (high.has(s)) return 'HIGH_LIQ';
    if (scp.has(s)) return 'SCP';
    return 'OTHER';
  }

  evaluateClusterRisk(signal) {
    if (!this.config.risk.clusterGuardEnabled) {
      return { blocked: false, reasons: [] };
    }
    const symbol = String(signal?.symbol || '').toUpperCase();
    const side = String(signal?.side || '').toUpperCase();
    if (!symbol || !side || side === 'NO_TRADE') {
      return { blocked: false, reasons: [] };
    }

    const cluster = this.getRiskClusterId(symbol);
    const windowMin = Math.max(10, Number(this.config.risk.clusterWindowMin || 120));
    const maxSameSideTrades = Math.max(1, Number(this.config.risk.clusterMaxSameSideTrades || 3));
    const maxConsecutiveLosses = Math.max(
      1,
      Number(this.config.risk.clusterMaxConsecutiveLosses || 2),
    );
    const pauseAfterLossMin = Math.max(0, Number(this.config.risk.clusterPauseAfterLossMin || 45));
    const since = Date.now() - windowMin * 60 * 1000;
    const history = Array.isArray(this.stateStore.state.risk?.tradeHistory)
      ? this.stateStore.state.risk.tradeHistory
      : [];

    const clusterTrades = history.filter((t) => {
      if (Number(t?.timestamp || 0) < since) return false;
      return this.getRiskClusterId(t?.symbol) === cluster;
    });
    const sameSideTrades = clusterTrades.filter((t) => String(t?.side || '').toUpperCase() === side);

    const reasons = [];
    if (sameSideTrades.length >= maxSameSideTrades) {
      reasons.push({
        code: 'RISK_CLUSTER_OVEREXPOSED',
        reason: `Cluster ${cluster} đã có ${sameSideTrades.length} lệnh ${side}/${windowMin}m`,
      });
    }

    const sameSideSorted = sameSideTrades.sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));
    let consecutiveLosses = 0;
    for (const t of sameSideSorted) {
      const pnl = Number(t?.pnl || 0);
      if (pnl < 0) consecutiveLosses += 1;
      else break;
    }
    if (consecutiveLosses >= maxConsecutiveLosses) {
      reasons.push({
        code: 'RISK_CLUSTER_LOSS_STREAK',
        reason: `Cluster ${cluster} đang có chuỗi thua ${consecutiveLosses} lệnh ${side}`,
      });
    }

    const latestLoss = sameSideSorted.find((t) => Number(t?.pnl || 0) < 0);
    if (latestLoss && pauseAfterLossMin > 0) {
      const ageMs = Date.now() - Number(latestLoss.timestamp || 0);
      const pauseMs = pauseAfterLossMin * 60 * 1000;
      if (ageMs < pauseMs) {
        reasons.push({
          code: 'RISK_CLUSTER_COOLDOWN',
          reason: `Cluster ${cluster} đang cooldown sau lệnh thua (${Math.ceil((pauseMs - ageMs) / 60000)}m)`,
        });
      }
    }

    return {
      blocked: reasons.length > 0,
      reasons,
      meta: {
        cluster,
        windowMin,
        sameSideTrades: sameSideTrades.length,
        consecutiveLosses,
      },
    };
  }

  evaluate(signal, marketMeta = {}) {
    const reasons = [];
    const codes = [];
    const addReject = (code, reason) => {
      reasons.push(reason);
      codes.push(code);
    };
    const st = this.stateStore.state;
    const riskCfg = this.config.risk;
    const tradingCfg = this.config.trading;

    if (this.isEmergencyStop()) addReject('EMERGENCY_STOP', 'Emergency stop đang bật');
    if (!st.botRunning) addReject('BOT_NOT_RUNNING', 'Bot chưa bật');
    if (st.risk.lockedByRisk) addReject('RISK_LOCK', 'Bot đang khóa bởi risk rule');
    if (this.stateStore.isCooldownActive()) addReject('COOLDOWN', 'Đang trong thời gian cooldown');

    if (st.activePosition && !tradingCfg.allowMultiplePosition) {
      addReject('POSITION_OPEN', 'Đang có vị thế mở, không cho phép chồng lệnh');
    }

    if (!signal || signal.side === 'NO_TRADE') addReject('NO_SIGNAL', 'Không có tín hiệu hợp lệ');

    if (signal && signal.rr && signal.rr < tradingCfg.minRR) addReject('BAD_RR', 'RR thấp hơn ngưỡng');
    if (signal && signal.confidence < tradingCfg.signalThreshold) addReject('LOW_CONFIDENCE', 'Confidence thấp');

    const blockingSignalCodes = new Set([
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
    const signalRejectCodes = Array.isArray(signal?.rejectCodes) ? signal.rejectCodes : [];
    const blockedBySignal = signalRejectCodes.find((code) => blockingSignalCodes.has(code));
    if (blockedBySignal) {
      addReject('SIGNAL_FILTER_BLOCK', `Signal bị chặn bởi bộ lọc: ${blockedBySignal}`);
    }

    if (signal?.market?.volumeStatus?.status === 'THẤP') addReject('LOW_VOLUME', 'Volume yếu');
    if (signal?.market?.sideway) addReject('SIDEWAY', 'Thị trường sideway');
    if (signal?.market?.nearStrongResistance && signal?.side === 'LONG') {
      addReject('NEAR_STRONG_RESISTANCE', 'Gần kháng cự mạnh - không vào LONG');
    }
    if (signal?.market?.nearStrongSupport && signal?.side === 'SHORT') {
      addReject('NEAR_STRONG_SUPPORT', 'Gần hỗ trợ mạnh - không vào SHORT');
    }
    if ((signal?.market?.distanceToMA25 || 0) > tradingCfg.maxDistanceFromMA25Pct) {
      addReject('EXTENDED_FROM_MA25', 'Giá đã chạy quá xa MA25');
    }
    if (signal?.side === 'LONG' && Number.isFinite(signal?.rewardToResistancePct)) {
      if (signal.rewardToResistancePct < tradingCfg.minRewardPct) {
        addReject('LOW_REWARD_DISTANCE', 'Kháng cự quá gần, reward không đủ cho LONG');
      }
    }
    if (signal?.side === 'SHORT' && Number.isFinite(signal?.rewardToSupportPct)) {
      if (signal.rewardToSupportPct < tradingCfg.minRewardPct) {
        addReject('LOW_REWARD_DISTANCE', 'Hỗ trợ quá gần, reward không đủ cho SHORT');
      }
    }

    if ((marketMeta.spreadPct || 0) > tradingCfg.maxSpreadPct) addReject('HIGH_SPREAD', 'Spread vượt ngưỡng');
    if ((marketMeta.estimatedSlippagePct || 0) > tradingCfg.maxSlippagePct) {
      addReject('HIGH_SLIPPAGE', 'Slippage vượt ngưỡng');
    }
    if ((marketMeta.entryDeviationPct || 0) > tradingCfg.maxEntryDeviationPct) {
      addReject('HIGH_DEVIATION', 'Giá lệch vùng entry quá xa');
    }
    if ((marketMeta.latencyMs || 0) > tradingCfg.maxLatencyMs) addReject('HIGH_LATENCY', 'Latency quá cao');

    if (signal?.createdAt && Date.now() - signal.createdAt > tradingCfg.maxSignalAgeMs) {
      addReject('STALE_SIGNAL', 'Tín hiệu đã quá cũ');
    }

    if (signal?.entryPrice && signal?.stopLoss) {
      const stopRiskPct = Math.abs(signal.entryPrice - signal.stopLoss) / signal.entryPrice;
      const signalLeverage = Math.max(Number(signal?.leverage || this.config.binance.leverage || 1), 1);
      const approxLiqDistancePct = 1 / signalLeverage;
      if (stopRiskPct >= approxLiqDistancePct * 0.8) {
        addReject(
          'LIQUIDATION_RISK',
          `Stop risk ${(stopRiskPct * 100).toFixed(2)}% quá gần ngưỡng liquidation x${signalLeverage}`,
        );
      }
    }

    const trades = st.risk.tradeHistory;
    const tradesHour = this.countTradesInWindow(trades, 60);
    const tradesDay = this.countTradesToday(trades);

    const clusterRisk = this.evaluateClusterRisk(signal);
    if (clusterRisk.blocked) {
      for (const item of clusterRisk.reasons) {
        addReject(item.code, item.reason);
      }
    }

    if (tradesHour >= riskCfg.maxTradesPerHour) addReject('MAX_TRADES_HOUR', 'Đã đạt max số lệnh mỗi giờ');
    if (tradesDay >= riskCfg.maxTradesPerDay) addReject('MAX_TRADES_DAY', 'Đã đạt max số lệnh mỗi ngày');

    if (st.risk.consecutiveLosses >= riskCfg.maxConsecutiveLosses) {
      addReject('CONSECUTIVE_LOSS', 'Đã thua liên tiếp quá ngưỡng cho phép');
    }

    const dailyLossPct = this.calcDailyLossPct();
    if (dailyLossPct >= riskCfg.dailyLossLimitPct) {
      addReject('DAILY_LOSS_LIMIT', 'Đã chạm giới hạn lỗ ngày');
      this.activateEmergencyStop('DAILY_LOSS_LIMIT', `${dailyLossPct.toFixed(4)}`);
    }

    const drawdownPct = this.calcDrawdownPct();
    const emergencyDrawdownPct = Math.min(riskCfg.autoEmergencyDrawdownPct, riskCfg.maxDrawdownPct);
    if (drawdownPct >= riskCfg.maxDrawdownPct) {
      addReject('MAX_DRAWDOWN', 'Drawdown vượt ngưỡng tối đa');
      this.activateEmergencyStop('MAX_DRAWDOWN', `${drawdownPct.toFixed(4)}`);
    } else if (drawdownPct >= emergencyDrawdownPct) {
      addReject('SEVERE_DRAWDOWN', 'Drawdown lớn bất thường');
      this.activateEmergencyStop('SEVERE_DRAWDOWN', `${drawdownPct.toFixed(4)}`);
    }

    const allowed = reasons.length === 0;
    if (!allowed) {
      this.stateStore.setLastReject({
        timestamp: Date.now(),
        code: codes[0] || 'REJECTED',
        codes: [...new Set(codes)],
        signal,
        reasons,
      });
      this.logger.risk('Từ chối lệnh bởi anti-cháy', {
        side: signal?.side,
        confidence: signal?.confidence,
        reasons,
      });
    }

    return {
      allowed,
      reasons,
      codes: [...new Set(codes)],
      meta: {
        tradesHour,
        tradesDay,
        dailyLossPct,
        drawdownPct,
        clusterRisk: clusterRisk?.meta || null,
      },
    };
  }

  calcPositionSize({ entryPrice, stopLoss, equity, riskPerTradePct, signal }) {
    const cappedRiskPct = Math.min(
      Math.max(riskPerTradePct, this.config.risk.minRiskPerTradePct),
      this.config.risk.maxRiskPerTradePct,
    );

    let volatilityFactor = 1;
    const regime = signal?.volatilityRegime || signal?.market?.volatilityRegime;
    if (this.config.advanced?.enabled) {
      if (regime === 'HIGH_VOL') volatilityFactor = 0.65;
      else if (regime === 'LOW_VOL') volatilityFactor = 0.75;
    }

    const riskAmount = equity * cappedRiskPct * volatilityFactor;
    const stopDistance = Math.abs(entryPrice - stopLoss);
    if (stopDistance <= 0) return 0;

    const rawQty = riskAmount / stopDistance;
    const notional = rawQty * entryPrice;

    if (notional < this.config.risk.minNotionalUSDT) {
      return this.config.risk.minNotionalUSDT / entryPrice;
    }

    return rawQty;
  }

  onTradeClosed(result) {
    this.stateStore.recordTradeResult(result);

    const pnl = Number(result.pnl || 0);
    if (pnl < 0) {
      this.stateStore.setCooldown(this.config.risk.cooldownAfterLossSec);
    } else {
      this.stateStore.setCooldown(this.config.risk.cooldownAfterWinSec);
    }

    this.registerHealthyTick();

    const dailyLossPct = this.calcDailyLossPct();
    const drawdownPct = this.calcDrawdownPct();

    if (dailyLossPct >= this.config.risk.dailyLossLimitPct || drawdownPct >= this.config.risk.maxDrawdownPct) {
      this.activateEmergencyStop('RISK_GUARD_TRIGGERED', `daily=${dailyLossPct};dd=${drawdownPct}`);
    }
  }

  updateRiskConfig(partial) {
    const risk = this.config.risk;
    const next = partial || {};
    const emergencyStopToggle =
      typeof next.emergencyStop === 'boolean' ? next.emergencyStop : undefined;
    if (Object.prototype.hasOwnProperty.call(next, 'emergencyStop')) {
      delete next.emergencyStop;
    }

    Object.assign(risk, next);

    risk.maxRiskPerTradePct = Math.min(Math.max(risk.maxRiskPerTradePct, 0), 0.05);
    risk.minRiskPerTradePct = Math.max(Math.min(risk.minRiskPerTradePct, risk.maxRiskPerTradePct), 0);

    risk.defaultRiskPerTradePct = Math.min(
      Math.max(risk.defaultRiskPerTradePct, risk.minRiskPerTradePct),
      risk.maxRiskPerTradePct,
    );
    risk.syncUnknownPositionAction = ['PAUSE', 'KEEP', 'CLOSE'].includes(
      String(risk.syncUnknownPositionAction || '').toUpperCase(),
    )
      ? String(risk.syncUnknownPositionAction).toUpperCase()
      : 'PAUSE';

    if (typeof emergencyStopToggle === 'boolean') {
      if (emergencyStopToggle) this.activateEmergencyStop('MANUAL_EMERGENCY_STOP');
      else this.releaseEmergencyStop();
    }
  }
}

module.exports = RiskManager;
