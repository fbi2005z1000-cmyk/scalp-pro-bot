function pctDistance(a, b) {
  const x = Number(a);
  const y = Number(b);
  if (!(Number.isFinite(x) && Number.isFinite(y) && y !== 0)) return null;
  return Math.abs(x - y) / Math.abs(y);
}

function evaluateSafetyRules({
  side,
  confidence,
  minConfidence,
  rr,
  minRR,
  spreadPct,
  maxSpreadPct,
  slippagePct,
  maxSlippagePct,
  entry,
  ma25,
  maxDistanceFromMA25Pct,
  nearStrongResistance,
  nearStrongSupport,
  rewardToResistancePct,
  rewardToSupportPct,
  minRewardPct,
} = {}) {
  const rejects = [];

  if (Number(confidence || 0) < Number(minConfidence || 0)) rejects.push('LOW_CONFIDENCE');
  if (Number(rr || 0) < Number(minRR || 0)) rejects.push('BAD_RR');
  if (Number(spreadPct || 0) > Number(maxSpreadPct || 0)) rejects.push('HIGH_SPREAD');
  if (Number(slippagePct || 0) > Number(maxSlippagePct || 0)) rejects.push('HIGH_SLIPPAGE');

  const ma25Distance = pctDistance(entry, ma25);
  if (
    Number.isFinite(ma25Distance) &&
    Number(maxDistanceFromMA25Pct || 0) > 0 &&
    ma25Distance > Number(maxDistanceFromMA25Pct)
  ) {
    rejects.push('TOO_FAR_FROM_MA25');
  }

  if (String(side || '').toUpperCase() === 'LONG') {
    if (nearStrongResistance) rejects.push('NEAR_STRONG_RESISTANCE');
    if (
      Number(minRewardPct || 0) > 0 &&
      Number(rewardToResistancePct || 0) > 0 &&
      Number(rewardToResistancePct) < Number(minRewardPct)
    ) {
      rejects.push('LOW_REWARD_TO_RESISTANCE');
    }
  }

  if (String(side || '').toUpperCase() === 'SHORT') {
    if (nearStrongSupport) rejects.push('NEAR_STRONG_SUPPORT');
    if (
      Number(minRewardPct || 0) > 0 &&
      Number(rewardToSupportPct || 0) > 0 &&
      Number(rewardToSupportPct) < Number(minRewardPct)
    ) {
      rejects.push('LOW_REWARD_TO_SUPPORT');
    }
  }

  return {
    ok: rejects.length === 0,
    rejects,
  };
}

module.exports = {
  evaluateSafetyRules,
};

