const SAFE = 'SAFE';
const BALANCED = 'BALANCED';
const AGGRESSIVE = 'AGGRESSIVE';

const PROFILE_SET = new Set([SAFE, BALANCED, AGGRESSIVE]);

function normalizeProfile(profile) {
  const value = String(profile || BALANCED).trim().toUpperCase();
  return PROFILE_SET.has(value) ? value : BALANCED;
}

function resolveAutoTradeProfile(config = {}) {
  const profile = normalizeProfile(config.profile);
  const defaults = {
    signalThreshold: Number(config?.trading?.signalThreshold || 56),
    minRR: Number(config?.trading?.minRR || 1.2),
    riskPerTradePct: Number(config?.risk?.defaultRiskPerTradePct || 0.008),
    maxSpreadPct: Number(config?.trading?.maxSpreadPct || 0.0012),
    maxSlippagePct: Number(config?.trading?.maxSlippagePct || 0.0018),
  };

  if (profile === SAFE) {
    return {
      profile,
      ...defaults,
      signalThreshold: Math.max(defaults.signalThreshold, 58),
      minRR: Math.max(defaults.minRR, 1.25),
      riskPerTradePct: Math.min(defaults.riskPerTradePct, 0.0075),
      maxSpreadPct: Math.min(defaults.maxSpreadPct, 0.0012),
      maxSlippagePct: Math.min(defaults.maxSlippagePct, 0.0016),
    };
  }

  if (profile === AGGRESSIVE) {
    return {
      profile,
      ...defaults,
      signalThreshold: Math.min(defaults.signalThreshold, 48),
      minRR: Math.min(defaults.minRR, 1.1),
      riskPerTradePct: Math.min(Math.max(defaults.riskPerTradePct, 0.009), 0.012),
      maxSpreadPct: Math.max(defaults.maxSpreadPct, 0.0018),
      maxSlippagePct: Math.max(defaults.maxSlippagePct, 0.0022),
    };
  }

  return {
    profile,
    ...defaults,
    signalThreshold: defaults.signalThreshold,
    minRR: defaults.minRR,
    riskPerTradePct: defaults.riskPerTradePct,
    maxSpreadPct: defaults.maxSpreadPct,
    maxSlippagePct: defaults.maxSlippagePct,
  };
}

module.exports = {
  resolveAutoTradeProfile,
};

