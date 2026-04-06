function clamp(value, min, max) {
  if (!Number.isFinite(value)) return min;
  return Math.max(min, Math.min(max, value));
}

function buildCapitalPlan({
  equityUSDT,
  riskPerTradePct,
  leverage,
  stopDistancePct,
  minNotionalUSDT = 5,
  maxNotionalUSDT = 20,
} = {}) {
  const equity = Number(equityUSDT || 0);
  const riskPct = Number(riskPerTradePct || 0);
  const lev = Number(leverage || 1);
  const stopPct = Number(stopDistancePct || 0);

  if (!(equity > 0 && riskPct > 0 && lev > 0 && stopPct > 0)) {
    return {
      ok: false,
      reason: 'INVALID_CAPITAL_INPUT',
      positionNotionalUSDT: 0,
      riskUSDT: 0,
    };
  }

  const riskUSDT = equity * riskPct;
  const theoreticalNotional = riskUSDT / stopPct;
  const leverageAdjusted = theoreticalNotional / Math.max(1, lev);
  const boundedNotional = clamp(leverageAdjusted, minNotionalUSDT, maxNotionalUSDT);

  return {
    ok: true,
    reason: 'OK',
    riskUSDT,
    positionNotionalUSDT: boundedNotional,
    theoreticalNotionalUSDT: theoreticalNotional,
    leverage,
  };
}

module.exports = {
  buildCapitalPlan,
};

