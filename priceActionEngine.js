const CandleAnalyzer = require('./candleAnalyzer');
const { average, clamp } = require('./utils');

class PriceActionEngine {
  static findSwings(candles, left = 2, right = 2) {
    const highs = [];
    const lows = [];

    if (!Array.isArray(candles) || candles.length < left + right + 3) {
      return { highs, lows };
    }

    for (let i = left; i < candles.length - right; i += 1) {
      const c = candles[i];
      let isHigh = true;
      let isLow = true;

      for (let j = i - left; j <= i + right; j += 1) {
        if (j === i) continue;
        if (candles[j].high >= c.high) isHigh = false;
        if (candles[j].low <= c.low) isLow = false;
      }

      if (isHigh) highs.push({ index: i, price: c.high, time: c.time });
      if (isLow) lows.push({ index: i, price: c.low, time: c.time });
    }

    return { highs, lows };
  }

  static detectStructure(candles) {
    const swings = this.findSwings(candles, 2, 2);
    const highs = swings.highs.slice(-2);
    const lows = swings.lows.slice(-2);

    if (highs.length < 2 || lows.length < 2) {
      return {
        structure: 'SIDEWAY',
        swings,
        hh: false,
        hl: false,
        lh: false,
        ll: false,
      };
    }

    const hh = highs[1].price > highs[0].price;
    const lh = highs[1].price < highs[0].price;
    const hl = lows[1].price > lows[0].price;
    const ll = lows[1].price < lows[0].price;

    let structure = 'SIDEWAY';
    if (hh && hl) structure = 'UP';
    if (lh && ll) structure = 'DOWN';

    return {
      structure,
      swings,
      hh,
      hl,
      lh,
      ll,
      prevHigh: highs[0].price,
      lastHigh: highs[1].price,
      prevLow: lows[0].price,
      lastLow: lows[1].price,
    };
  }

  static detectBos(candles, structureData) {
    const output = {
      bullish: false,
      bearish: false,
      direction: 'NONE',
      againstTrend: false,
      level: null,
    };

    if (!Array.isArray(candles) || candles.length < 5) return output;
    const latest = candles[candles.length - 1];
    const swings = structureData?.swings || { highs: [], lows: [] };
    const lastHigh = swings.highs[swings.highs.length - 1]?.price;
    const lastLow = swings.lows[swings.lows.length - 1]?.price;

    if (Number.isFinite(lastHigh) && latest.close > lastHigh * 1.00025) {
      output.bullish = true;
      output.direction = 'BULLISH';
      output.level = lastHigh;
    }
    if (Number.isFinite(lastLow) && latest.close < lastLow * 0.99975) {
      output.bearish = true;
      output.direction = output.bullish ? 'MIXED' : 'BEARISH';
      output.level = Number.isFinite(output.level) ? output.level : lastLow;
    }

    if (structureData?.structure === 'UP' && output.bearish) output.againstTrend = true;
    if (structureData?.structure === 'DOWN' && output.bullish) output.againstTrend = true;

    return output;
  }

  static streakInfo(candles) {
    const recent = candles.slice(-4);
    if (recent.length < 3) return { count: 0, color: 'NONE', strong: false };

    const color = (c) => (c.close > c.open ? 'GREEN' : c.close < c.open ? 'RED' : 'DOJI');
    const colors = recent.map(color);
    const latestColor = colors[colors.length - 1];
    if (latestColor === 'DOJI') return { count: 0, color: 'DOJI', strong: false };

    let count = 0;
    for (let i = colors.length - 1; i >= 0; i -= 1) {
      if (colors[i] !== latestColor) break;
      count += 1;
    }

    const bodyRatios = recent.slice(recent.length - count).map((c) => {
      const range = Math.max(c.high - c.low, 1e-9);
      return Math.abs(c.close - c.open) / range;
    });
    const strong = count >= 3 && average(bodyRatios) >= 0.58;
    return { count, color: latestColor, strong };
  }

  static classifyPullback({ trend, candles, ma25, nearSupport, nearResistance, swings }) {
    const latest = candles[candles.length - 1];
    const distanceToMA25 = Number.isFinite(ma25) && ma25 > 0 ? Math.abs(latest.close - ma25) / latest.close : 0;
    const nearMA = distanceToMA25 <= 0.0038;

    const lastSwingLow = swings?.lows?.[swings.lows.length - 1]?.price;
    const lastSwingHigh = swings?.highs?.[swings.highs.length - 1]?.price;

    if (trend === 'UP') {
      const brokeStructure = Number.isFinite(lastSwingLow) && latest.low < lastSwingLow * 0.9993;
      if (brokeStructure) {
        return { quality: 'FAIL', depthPct: distanceToMA25, brokeStructure: true };
      }
      const deep = Number.isFinite(ma25) && latest.close < ma25 * 0.996;
      if ((nearMA || nearSupport) && !deep) {
        return { quality: 'GOOD', depthPct: distanceToMA25, brokeStructure: false };
      }
      return { quality: 'DEEP', depthPct: distanceToMA25, brokeStructure: false };
    }

    if (trend === 'DOWN') {
      const brokeStructure = Number.isFinite(lastSwingHigh) && latest.high > lastSwingHigh * 1.0007;
      if (brokeStructure) {
        return { quality: 'FAIL', depthPct: distanceToMA25, brokeStructure: true };
      }
      const deep = Number.isFinite(ma25) && latest.close > ma25 * 1.004;
      if ((nearMA || nearResistance) && !deep) {
        return { quality: 'GOOD', depthPct: distanceToMA25, brokeStructure: false };
      }
      return { quality: 'DEEP', depthPct: distanceToMA25, brokeStructure: false };
    }

    return { quality: 'FAIL', depthPct: distanceToMA25, brokeStructure: false };
  }

  static detectCandleConfirm(trend, latestCandle, insight) {
    const patterns = insight?.patterns || {};
    const range = Math.max(latestCandle.high - latestCandle.low, 1e-9);
    const closeNearHigh = (latestCandle.high - latestCandle.close) / range <= 0.2;
    const closeNearLow = (latestCandle.close - latestCandle.low) / range <= 0.2;

    const bullish =
      Boolean(patterns.bullishEngulfing) ||
      Boolean(patterns.lowerPinbar) ||
      Boolean(patterns.bullishConfirm) ||
      closeNearHigh;
    const bearish =
      Boolean(patterns.bearishEngulfing) ||
      Boolean(patterns.upperPinbar) ||
      Boolean(patterns.bearishConfirm) ||
      closeNearLow;

    if (trend === 'UP') {
      return { ok: bullish, bullish, bearish, closeNearHigh, closeNearLow };
    }
    if (trend === 'DOWN') {
      return { ok: bearish, bullish, bearish, closeNearHigh, closeNearLow };
    }
    return { ok: false, bullish, bearish, closeNearHigh, closeNearLow };
  }

  static analyze({
    candles,
    trendHint = 'SIDEWAY',
    ma25 = null,
    rsi = 50,
    supportResistance = null,
    candleInsight = null,
    volumeStatus = null,
    config = null,
  }) {
    const output = {
      side: 'NONE',
      confidence: 0,
      reason: [],
      structure: 'SIDEWAY',
      structureDetails: null,
      bos: {
        bullish: false,
        bearish: false,
        direction: 'NONE',
        againstTrend: false,
        level: null,
      },
      pullbackQuality: 'FAIL',
      pullbackDepthPct: null,
      candleConfirm: false,
      nearSupport: false,
      nearResistance: false,
      antiFomo: {
        farFromMA25: false,
        streakFomo: false,
        rsiExtreme: false,
        breakoutNoPullback: false,
        nearOppositeZone: false,
      },
      breakdown: {
        trend: 0,
        pullback: 0,
        zone: 0,
        candle: 0,
        volume: 0,
        penalty: 0,
        total: 0,
      },
    };

    if (!Array.isArray(candles) || candles.length < 45) {
      output.reason.push('Thieu du lieu nen cho Price Action');
      return output;
    }

    const latest = candles[candles.length - 1];
    const structureData = this.detectStructure(candles.slice(-90));
    output.structure = structureData.structure;
    output.structureDetails = structureData;

    const trend = ['UP', 'DOWN'].includes(String(trendHint || '').toUpperCase())
      ? String(trendHint).toUpperCase()
      : structureData.structure;

    const keyLevels = supportResistance?.keyLevels || supportResistance || {};
    const distToSupport = Number.isFinite(keyLevels?.distToSupportPct)
      ? keyLevels.distToSupportPct
      : Number.isFinite(keyLevels?.rewardToSupportPct)
      ? keyLevels.rewardToSupportPct
      : null;
    const distToResistance = Number.isFinite(keyLevels?.distToResistancePct)
      ? keyLevels.distToResistancePct
      : Number.isFinite(keyLevels?.rewardToResistancePct)
      ? keyLevels.rewardToResistancePct
      : null;
    const nearLevelPct =
      Number(config?.trading?.nearStrongLevelPct) > 0 ? Number(config.trading.nearStrongLevelPct) : 0.0024;

    output.nearSupport = distToSupport !== null && distToSupport <= nearLevelPct * 1.6;
    output.nearResistance = distToResistance !== null && distToResistance <= nearLevelPct * 1.6;

    const maxDistanceMA25 =
      Number(config?.trading?.maxDistanceFromMA25Pct) > 0
        ? Number(config.trading.maxDistanceFromMA25Pct)
        : 0.0045;
    const distanceToMA25 = Number.isFinite(ma25) && ma25 > 0 ? Math.abs(latest.close - ma25) / latest.close : 0;

    const bos = this.detectBos(candles.slice(-90), structureData);
    output.bos = bos;

    const pullback = this.classifyPullback({
      trend,
      candles,
      ma25,
      nearSupport: output.nearSupport,
      nearResistance: output.nearResistance,
      swings: structureData.swings,
    });
    output.pullbackQuality = pullback.quality;
    output.pullbackDepthPct = pullback.depthPct;

    const streak = this.streakInfo(candles);
    const insight = candleInsight || CandleAnalyzer.analyze(candles);
    const candleConfirm = this.detectCandleConfirm(trend, latest, insight);
    output.candleConfirm = candleConfirm.ok;

    const overbought = Number(config?.trading?.overboughtLongRsi) || 66;
    const oversold = Number(config?.trading?.oversoldShortRsi) || 34;
    const volRatio = Number(volumeStatus?.ratio || 0);

    output.antiFomo.farFromMA25 = distanceToMA25 > maxDistanceMA25;
    output.antiFomo.streakFomo = streak.strong;
    output.antiFomo.breakoutNoPullback =
      (trend === 'UP' && bos.bullish && pullback.quality !== 'GOOD') ||
      (trend === 'DOWN' && bos.bearish && pullback.quality !== 'GOOD');
    output.antiFomo.rsiExtreme =
      (trend === 'UP' && rsi >= overbought) || (trend === 'DOWN' && rsi <= oversold) || rsi >= 75 || rsi <= 25;
    output.antiFomo.nearOppositeZone =
      (trend === 'UP' && output.nearResistance) || (trend === 'DOWN' && output.nearSupport);

    let score = 0;
    const breakdown = {
      trend: 0,
      pullback: 0,
      zone: 0,
      candle: 0,
      volume: 0,
      penalty: 0,
      total: 0,
    };

    if (trend === 'UP' || trend === 'DOWN') {
      breakdown.trend = 20;
      score += 20;
    } else {
      breakdown.trend = -25;
      score -= 25;
      output.reason.push('Cau truc sideway, khong nen vao lenh');
    }

    if (pullback.quality === 'GOOD') {
      breakdown.pullback = 20;
      score += 20;
    } else if (pullback.quality === 'DEEP') {
      breakdown.pullback = -8;
      score -= 8;
      output.reason.push('Pullback qua sau, can cho xac nhan them');
    } else {
      breakdown.pullback = -24;
      score -= 24;
      output.reason.push('Pullback pha cau truc (FAIL)');
    }

    const zoneGood =
      (trend === 'UP' && output.nearSupport && !output.nearResistance) ||
      (trend === 'DOWN' && output.nearResistance && !output.nearSupport);
    if (zoneGood) {
      breakdown.zone = 15;
      score += 15;
    }

    if (candleConfirm.ok) {
      breakdown.candle = 20;
      score += 20;
    } else {
      breakdown.candle = -14;
      score -= 14;
      output.reason.push('Thieu nen xac nhan entry');
    }

    if (volRatio > 1.05) {
      breakdown.volume = 10;
      score += 10;
    } else {
      breakdown.volume = -5;
      score -= 5;
    }

    if (trend === 'UP' && output.nearResistance) {
      breakdown.penalty -= 20;
      score -= 20;
      output.reason.push('Gan khang cu manh, cam LONG');
    }
    if (trend === 'DOWN' && output.nearSupport) {
      breakdown.penalty -= 20;
      score -= 20;
      output.reason.push('Gan ho tro manh, cam SHORT');
    }
    if (output.antiFomo.streakFomo) {
      breakdown.penalty -= 15;
      score -= 15;
      output.reason.push('Chuoi nen manh lien tiep, rui ro FOMO');
    }
    if (output.antiFomo.farFromMA25) {
      breakdown.penalty -= 15;
      score -= 15;
      output.reason.push('Gia dang xa MA25, khong duoi lenh');
    }
    if (output.antiFomo.breakoutNoPullback) {
      breakdown.penalty -= 10;
      score -= 10;
      output.reason.push('Vua breakout/breakdown nhung chua co pullback dep');
    }
    if (output.antiFomo.rsiExtreme) {
      breakdown.penalty -= 12;
      score -= 12;
      output.reason.push('RSI dang o vung cuc doan');
    }
    if (bos.againstTrend) {
      breakdown.penalty -= 18;
      score -= 18;
      output.reason.push('BOS nguoc trend, block entry');
    }

    const confidence = clamp(Math.round(score), 0, 100);
    breakdown.total = confidence;
    output.breakdown = breakdown;
    output.confidence = confidence;

    const hardBlock =
      trend === 'SIDEWAY' ||
      pullback.quality === 'FAIL' ||
      output.antiFomo.streakFomo ||
      output.antiFomo.farFromMA25 ||
      output.antiFomo.nearOppositeZone ||
      bos.againstTrend;

    if (trend === 'UP' && pullback.quality === 'GOOD' && candleConfirm.ok && !hardBlock && confidence >= 55) {
      output.side = 'LONG';
      output.reason.push('Trend tang + pullback dep + nen xac nhan LONG');
    } else if (
      trend === 'DOWN' &&
      pullback.quality === 'GOOD' &&
      candleConfirm.ok &&
      !hardBlock &&
      confidence >= 55
    ) {
      output.side = 'SHORT';
      output.reason.push('Trend giam + pullback dep + nen xac nhan SHORT');
    } else {
      output.side = 'NONE';
    }

    if (output.side === 'NONE' && !output.reason.length) {
      output.reason.push('Price Action chua du dep de vao lenh');
    }

    return output;
  }
}

module.exports = PriceActionEngine;
