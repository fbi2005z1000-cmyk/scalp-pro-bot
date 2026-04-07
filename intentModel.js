function toNum(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function clamp(value, min, max) {
  return Math.max(min, Math.min(max, value));
}

function boolText(value) {
  return value ? 'YES' : 'NO';
}

function scoreTrend(signal) {
  const side = String(signal?.side || '').toUpperCase();
  const trend5m = String(signal?.trend5m || '').toUpperCase();
  const trend15m = String(signal?.trend15m || '').toUpperCase();
  const structure2m = String(signal?.market?.structure2m || '').toUpperCase();
  const structure5m = String(signal?.market?.structure5m || '').toUpperCase();

  let score = 0;
  const notes = [];

  const trendOk5m = (side === 'LONG' && trend5m === 'UP') || (side === 'SHORT' && trend5m === 'DOWN');
  const trendOk15m = (side === 'LONG' && trend15m === 'UP') || (side === 'SHORT' && trend15m === 'DOWN');
  const structureOk2m =
    (side === 'LONG' && structure2m.includes('UP')) ||
    (side === 'SHORT' && structure2m.includes('DOWN'));
  const structureOk5m =
    (side === 'LONG' && structure5m.includes('UP')) ||
    (side === 'SHORT' && structure5m.includes('DOWN'));

  if (trendOk5m) {
    score += 18;
    notes.push('Trend 5m cùng hướng');
  } else {
    score -= 20;
    notes.push('Trend 5m lệch hướng');
  }

  if (trend15m) {
    if (trendOk15m) {
      score += 8;
      notes.push('Trend 15m đồng thuận');
    } else {
      score -= 10;
      notes.push('Trend 15m nghịch');
    }
  }

  if (structureOk2m) {
    score += 7;
    notes.push('Cấu trúc 2m ủng hộ');
  } else if (structure2m) {
    score -= 7;
    notes.push('Cấu trúc 2m yếu');
  }

  if (structureOk5m) {
    score += 5;
    notes.push('Cấu trúc 5m giữ trend');
  } else if (structure5m) {
    score -= 6;
    notes.push('Cấu trúc 5m suy yếu');
  }

  return { score, notes, trendOk5m, trendOk15m };
}

function scorePressure(signal) {
  const side = String(signal?.side || '').toUpperCase();
  const market = signal?.market || {};
  const pressure = market.pressure || {};
  const buyPressure = toNum(signal?.buyPressure, toNum(pressure.buyPressure));
  const sellPressure = toNum(signal?.sellPressure, toNum(pressure.sellPressure));
  const candleStrength = toNum(pressure.candleStrength);
  const deltaVolumeTrend = toNum(pressure.deltaVolumeTrend);
  const takerBuyRatio = toNum(pressure.takerBuyRatio, 0.5);

  const align = side === 'LONG' ? buyPressure - sellPressure : sellPressure - buyPressure;
  const deltaAlign = side === 'LONG' ? deltaVolumeTrend : -deltaVolumeTrend;
  const takerAlign = side === 'LONG' ? takerBuyRatio - 0.5 : 0.5 - takerBuyRatio;

  let score = clamp(Math.round(align * 0.34), -22, 22);
  score += clamp(Math.round(deltaAlign * 0.16), -10, 10);
  score += clamp(Math.round((takerAlign * 100) * 0.1), -7, 7);
  score += clamp(Math.round((candleStrength - 50) * 0.14), -8, 9);

  const notes = [
    `Buy/Sell: ${buyPressure.toFixed(1)}/${sellPressure.toFixed(1)}`,
    `Delta: ${deltaVolumeTrend.toFixed(2)}%`,
    `Candle strength: ${candleStrength.toFixed(1)}`,
    `Taker buy ratio: ${(takerBuyRatio * 100).toFixed(1)}%`,
  ];

  return {
    score,
    notes,
    buyPressure,
    sellPressure,
    deltaVolumeTrend,
    candleStrength,
  };
}

function scoreLiquidity(signal, minLiquidityScore = 35) {
  const market = signal?.market || {};
  const pressure = market.pressure || {};
  const volumeStatus = String(signal?.volumeStatus || market?.volumeStatus?.status || '').toUpperCase();
  const liquidityScore = toNum(pressure.liquidityScore);
  const tradeIntensity = toNum(pressure.tradeIntensity);
  const quoteVolumeStatus = String(pressure.quoteVolumeStatus || '').toUpperCase();

  let score = clamp(Math.round((liquidityScore - minLiquidityScore) * 0.42), -18, 16);
  score += clamp(Math.round((tradeIntensity - 1) * 5), -6, 8);

  if (volumeStatus.includes('THẤP') || volumeStatus.includes('THAP')) score -= 6;
  if (quoteVolumeStatus.includes('THẤP') || quoteVolumeStatus.includes('THAP')) score -= 4;
  if (quoteVolumeStatus.includes('CAO')) score += 4;

  const notes = [
    `Liquidity score: ${liquidityScore.toFixed(1)}`,
    `Trade intensity: ${tradeIntensity.toFixed(2)}`,
    `Volume status: ${volumeStatus || 'N/A'}`,
    `Quote volume status: ${quoteVolumeStatus || 'N/A'}`,
  ];

  return {
    score,
    notes,
    liquidityScore,
    tradeIntensity,
  };
}

function scoreEntryTiming(signal) {
  const side = String(signal?.side || '').toUpperCase();
  const market = signal?.market || {};
  const timing = market.entryTiming || {};
  const rangePos = toNum(timing.rangePos, NaN);
  const distanceToMA25 = toNum(market.distanceToMA25, 0);
  const nearStrongResistance = Boolean(market.nearStrongResistance);
  const nearStrongSupport = Boolean(market.nearStrongSupport);
  const rewardToResistancePct = toNum(signal?.rewardToResistancePct);
  const rewardToSupportPct = toNum(signal?.rewardToSupportPct);
  const rr = toNum(signal?.rr);

  let score = 0;
  const notes = [];

  if (Number.isFinite(rangePos)) {
    if (side === 'LONG') {
      if (rangePos <= 0.82) score += 8;
      else score -= 12;
    } else if (side === 'SHORT') {
      if (rangePos >= 0.18) score += 8;
      else score -= 12;
    }
    notes.push(`RangePos: ${rangePos.toFixed(3)}`);
  }

  if (distanceToMA25 > 0) {
    const maScore = clamp(Math.round((0.006 - distanceToMA25) * 2200), -12, 9);
    score += maScore;
    notes.push(`Distance MA25: ${(distanceToMA25 * 100).toFixed(3)}%`);
  }

  if (side === 'LONG' && nearStrongResistance) {
    score -= 18;
    notes.push('Gần kháng cự mạnh');
  }
  if (side === 'SHORT' && nearStrongSupport) {
    score -= 18;
    notes.push('Gần hỗ trợ mạnh');
  }

  if (side === 'LONG' && rewardToResistancePct > 0) {
    score += clamp(Math.round((rewardToResistancePct - 0.0012) * 3500), -12, 10);
    notes.push(`Reward->R: ${(rewardToResistancePct * 100).toFixed(3)}%`);
  }
  if (side === 'SHORT' && rewardToSupportPct > 0) {
    score += clamp(Math.round((rewardToSupportPct - 0.0012) * 3500), -12, 10);
    notes.push(`Reward->S: ${(rewardToSupportPct * 100).toFixed(3)}%`);
  }

  if (rr > 0) {
    score += clamp(Math.round((rr - 1) * 6), -5, 9);
    notes.push(`RR: ${rr.toFixed(2)}`);
  }

  return {
    score,
    notes,
    rangePos,
    distanceToMA25,
    nearStrongResistance,
    nearStrongSupport,
  };
}

function buildWinProbability({
  confidence,
  intentScore,
  rr,
  spreadPct,
  slippagePct,
  trendAligned,
  liquidityScore,
}) {
  const rrBonus = clamp((toNum(rr) - 1) * 6, -6, 8);
  const spreadPenalty = clamp(toNum(spreadPct) * 10000 * 0.5, 0, 12);
  const slippagePenalty = clamp(toNum(slippagePct) * 10000 * 0.45, 0, 12);
  const trendBonus = trendAligned ? 4 : -7;
  const liqBonus = clamp((toNum(liquidityScore) - 45) * 0.15, -6, 8);

  const base = toNum(confidence) * 0.56 + toNum(intentScore) * 0.44;
  const winProbability = clamp(Math.round(base + rrBonus + trendBonus + liqBonus - spreadPenalty - slippagePenalty), 1, 99);

  return {
    winProbability,
    breakdown: {
      base: Math.round(base),
      rrBonus: Math.round(rrBonus),
      trendBonus: Math.round(trendBonus),
      liquidityBonus: Math.round(liqBonus),
      spreadPenalty: -Math.round(spreadPenalty),
      slippagePenalty: -Math.round(slippagePenalty),
    },
  };
}

function evaluateIntent(signal, options = {}) {
  const confidence = toNum(signal?.confidence);
  const rr = toNum(signal?.rr);
  const spreadPct = toNum(options?.spreadPct, toNum(signal?.market?.spreadPct));
  const slippagePct = toNum(options?.slippagePct);
  const minLiquidityScore = toNum(options?.minLiquidityScore, 35);

  const trend = scoreTrend(signal);
  const pressure = scorePressure(signal);
  const liquidity = scoreLiquidity(signal, minLiquidityScore);
  const timing = scoreEntryTiming(signal);

  const rawIntent = 50 + trend.score + pressure.score + liquidity.score + timing.score;
  const intentScore = clamp(Math.round(rawIntent), 0, 100);

  const winModel = buildWinProbability({
    confidence,
    intentScore,
    rr,
    spreadPct,
    slippagePct,
    trendAligned: trend.trendOk5m,
    liquidityScore: liquidity.liquidityScore,
  });

  return {
    intentScore,
    winProbability: winModel.winProbability,
    summary: `trend:${trend.score} pressure:${pressure.score} liquidity:${liquidity.score} timing:${timing.score}`,
    strengths: [
      ...trend.notes.slice(0, 2),
      ...pressure.notes.slice(0, 2),
      ...liquidity.notes.slice(0, 1),
    ],
    warnings: [
      ...timing.notes.slice(0, 3),
      ...(trend.trendOk5m ? [] : ['Trend 5m đang lệch']),
      ...(liquidity.liquidityScore < minLiquidityScore ? ['Thanh khoản nến thấp'] : []),
    ],
    details: {
      confidence,
      rr,
      spreadPct,
      slippagePct,
      trend,
      pressure,
      liquidity,
      timing,
      winModel: winModel.breakdown,
      gateSnapshot: {
        trendAligned5m: boolText(trend.trendOk5m),
        trendAligned15m: boolText(trend.trendOk15m),
        liquidityOk: boolText(liquidity.liquidityScore >= minLiquidityScore),
      },
    },
  };
}

module.exports = {
  evaluateIntent,
};

