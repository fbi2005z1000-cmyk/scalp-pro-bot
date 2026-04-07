const { resolveAutoTradeProfile } = require('./autoTradeProfile');
const { evaluateSafetyRules } = require('./safetyRules');
const { evaluateIntent } = require('./intentModel');

function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

class AutoTradeEngine {
  constructor({ config, logger, stateStore }) {
    this.config = config;
    this.logger = logger;
    this.stateStore = stateStore;
    this.lastDecision = null;
    this.lastDecisionBySymbol = new Map();
  }

  getHardRejectSet() {
    const fallback = [
      'NEAR_STRONG_RESISTANCE',
      'NEAR_STRONG_SUPPORT',
      'LOW_REWARD_DISTANCE',
      'BAD_PULLBACK',
      'WEAK_PULLBACK_STRUCTURE',
      'BOS_AGAINST_TREND',
      'PRICE_ACTION_BLOCK',
      'CANDLE_CONFIRM_MISSING',
      'HIGH_SPREAD',
      'HIGH_LATENCY',
      'HIGH_SLIPPAGE',
      'LOW_MARKET_QUALITY',
      'STRICT_QUALITY_GATE',
      'FOMO_BREAKOUT',
    ];
    const configured = Array.isArray(this.config?.trading?.autoHardRejectCodes)
      ? this.config.trading.autoHardRejectCodes
      : fallback;
    return new Set(
      configured
        .map((x) => String(x || '').trim().toUpperCase())
        .filter(Boolean),
    );
  }

  normalizeDecision(decision) {
    const payload = {
      decidedAt: Date.now(),
      ...(decision || {}),
    };
    this.lastDecision = payload;
    const symbol = String(payload?.symbol || '').toUpperCase();
    if (symbol) this.lastDecisionBySymbol.set(symbol, payload);
    if (this.lastDecisionBySymbol.size > 600) {
      const now = Date.now();
      for (const [k, v] of this.lastDecisionBySymbol.entries()) {
        const ts = Number(v?.decidedAt || 0);
        if (!ts || now - ts > 3 * 60 * 60 * 1000) this.lastDecisionBySymbol.delete(k);
      }
    }
    return payload;
  }

  checkDuplicateDecision(signal) {
    const side = String(signal?.side || '').toUpperCase();
    const symbol = String(signal?.symbol || '').toUpperCase();
    const entry = toNum(signal?.entryPrice);
    if (!symbol || !side || !entry) return { ok: true };

    const cooldownMs = Math.max(1000, toNum(this.config?.trading?.autoDecisionCooldownMs, 12000));
    const priceRangePct = Math.max(0.0001, toNum(this.config?.trading?.signalDuplicatePriceRangePct, 0.0015));
    const prev = this.lastDecisionBySymbol.get(symbol);
    if (!prev) return { ok: true };

    if (!prev.ok) return { ok: true };
    if (String(prev.side || '').toUpperCase() !== side) return { ok: true };

    const age = Date.now() - toNum(prev.decidedAt);
    if (age > cooldownMs) return { ok: true };

    const prevEntry = toNum(prev.entryPrice);
    const priceDiffPct = prevEntry > 0 ? Math.abs(entry - prevEntry) / prevEntry : 0;
    if (priceDiffPct <= priceRangePct) {
      return {
        ok: false,
        code: 'AUTO_DUPLICATE_DECISION',
        reason: `Lệnh trùng vùng giá trong ${cooldownMs}ms`,
      };
    }
    return { ok: true };
  }

  evaluateSignal(signal, context = {}) {
    const side = String(signal?.side || '').toUpperCase();
    const symbol = String(signal?.symbol || '').toUpperCase();
    const st = this.stateStore?.state || {};
    const trading = this.config?.trading || {};
    const profile = resolveAutoTradeProfile(this.config);

    const rejectCodes = [];
    const rejectReasons = [];
    const addReject = (code, reason) => {
      rejectCodes.push(String(code || 'AUTO_REJECT'));
      rejectReasons.push(String(reason || 'AUTO gate reject'));
    };

    if (!symbol) addReject('AUTO_INVALID_SYMBOL', 'Thiếu symbol');
    if (!side || side === 'NO_TRADE') addReject('AUTO_NO_ACTIONABLE_SIGNAL', 'Không có tín hiệu vào lệnh');
    if (st.activePosition) addReject('AUTO_MAX_ONE_POSITION', 'Đang có vị thế mở, AUTO chỉ cho 1 lệnh');
    if (!st.botRunning) addReject('AUTO_BOT_STOPPED', 'Bot đang dừng');
    if (st.risk?.lockedByRisk) addReject('AUTO_RISK_LOCK', 'Bot đang bị khóa risk');
    if (this.stateStore?.isCooldownActive?.()) addReject('AUTO_COOLDOWN', 'Đang trong cooldown');

    const hardRejectSet = this.getHardRejectSet();
    const signalRejectCodes = Array.isArray(signal?.rejectCodes)
      ? signal.rejectCodes.map((x) => String(x || '').toUpperCase())
      : [];
    const hardSignalReject = signalRejectCodes.find((x) => hardRejectSet.has(x));
    if (hardSignalReject) {
      addReject('AUTO_HARD_SIGNAL_REJECT', `Signal bị chặn cứng: ${hardSignalReject}`);
    }

    const spreadPct = toNum(context?.spreadPct, toNum(st?.orderBook?.spreadPct));
    const estimatedSlippagePct = toNum(context?.estimatedSlippagePct);
    const latencyMs = toNum(context?.latencyMs);
    const minConfidence = clamp(
      Math.max(
        toNum(profile.signalThreshold, 0),
        toNum(trading.autoThreshold, 0),
        toNum(trading.autoMinConfidence, 0),
      ),
      0,
      100,
    );
    const minIntentScore = clamp(toNum(trading.autoMinIntentScore, 62), 0, 100);
    const minWinProbability = clamp(toNum(trading.autoMinWinProbability, 65), 0, 100);

    const intent = evaluateIntent(signal, {
      spreadPct,
      slippagePct: estimatedSlippagePct,
      minLiquidityScore: toNum(trading.autoLiquidityMinScore, 30),
    });

    const safety = evaluateSafetyRules({
      side,
      confidence: signal?.confidence,
      minConfidence,
      rr: signal?.rr,
      minRR: profile.minRR,
      spreadPct,
      maxSpreadPct: profile.maxSpreadPct,
      slippagePct: estimatedSlippagePct,
      maxSlippagePct: profile.maxSlippagePct,
      entry: signal?.entryPrice,
      ma25: signal?.indicatorSnapshot?.ma25 || signal?.market?.indicatorSnapshot?.ma25,
      maxDistanceFromMA25Pct: toNum(trading.maxDistanceFromMA25Pct, 0.0072),
      nearStrongResistance: signal?.market?.nearStrongResistance,
      nearStrongSupport: signal?.market?.nearStrongSupport,
      rewardToResistancePct: signal?.rewardToResistancePct,
      rewardToSupportPct: signal?.rewardToSupportPct,
      minRewardPct: toNum(trading.minRewardPct, 0.001),
    });

    if (!safety.ok) {
      for (const code of safety.rejects) addReject(code, `Safety reject: ${code}`);
    }

    const confidence = toNum(signal?.confidence, 0);
    if (confidence < minConfidence) {
      addReject('AUTO_LOW_CONFIDENCE', `Confidence ${confidence.toFixed(1)} < ${minConfidence}`);
    }

    if (intent.intentScore < minIntentScore) {
      addReject('AUTO_LOW_INTENT_SCORE', `Intent score ${intent.intentScore} < ${minIntentScore}`);
    }
    if (intent.winProbability < minWinProbability) {
      addReject('AUTO_LOW_WIN_PROB', `Win prob ${intent.winProbability}% < ${minWinProbability}%`);
    }

    if (latencyMs > toNum(trading.maxLatencyMs, 2200)) {
      addReject('AUTO_HIGH_LATENCY', `Latency ${latencyMs}ms quá cao`);
    }

    if (String(signal?.level || '').toUpperCase() === 'WEAK' && trading.autoRequireStrongLevel !== false) {
      if (!(trading.autoAllowWeakWhenConsensus === true && signal?.preSignal?.consensus)) {
        addReject('AUTO_WEAK_LEVEL', 'Tín hiệu WEAK bị chặn ở AUTO');
      }
    }

    if (Boolean(signal?.market?.sideway)) {
      addReject('AUTO_SIDEWAY', 'Thị trường sideway, chặn AUTO');
    }

    const duplicateDecision = this.checkDuplicateDecision(signal);
    if (!duplicateDecision.ok) {
      addReject(duplicateDecision.code, duplicateDecision.reason);
    }

    const ok = rejectCodes.length === 0;
    const decision = this.normalizeDecision({
      ok,
      code: ok ? 'AUTO_OK' : rejectCodes[0],
      symbol,
      side,
      profile: profile.profile,
      confidence,
      entryPrice: toNum(signal?.entryPrice),
      intent,
      safety,
      thresholds: {
        minConfidence,
        minIntentScore,
        minWinProbability,
      },
      rejectCodes: [...new Set(rejectCodes)],
      rejectReasons,
      context: {
        spreadPct,
        estimatedSlippagePct,
        latencyMs,
      },
    });

    if (!ok) {
      this.logger.info('auto-trade', 'AutoTradeEngine chặn lệnh', {
        symbol,
        side,
        code: decision.code,
        rejectCodes: decision.rejectCodes,
        minConfidence,
        intentScore: intent.intentScore,
        winProbability: intent.winProbability,
      });
    }

    return decision;
  }

  getRuntimeStatus() {
    return {
      enabled: true,
      profile: String(this.config?.profile || 'BALANCED').toUpperCase(),
      last: this.lastDecision,
      trackedSymbols: this.lastDecisionBySymbol.size,
    };
  }
}

module.exports = {
  AutoTradeEngine,
};

