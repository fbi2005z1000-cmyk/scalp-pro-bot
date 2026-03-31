const { average, calcSlope, clamp, normalizeStrength } = require('./utils');

class IndicatorService {
  static sma(values, period) {
    if (!Array.isArray(values) || !values.length) return [];
    const result = new Array(values.length).fill(null);
    let sum = 0;

    for (let i = 0; i < values.length; i += 1) {
      sum += values[i];
      if (i >= period) sum -= values[i - period];
      if (i >= period - 1) {
        result[i] = sum / period;
      }
    }

    return result;
  }

  static rsi(values, period = 14) {
    if (!Array.isArray(values) || values.length < period + 1) {
      return new Array(values.length).fill(null);
    }

    const result = new Array(values.length).fill(null);
    let gain = 0;
    let loss = 0;

    for (let i = 1; i <= period; i += 1) {
      const diff = values[i] - values[i - 1];
      if (diff >= 0) gain += diff;
      else loss += Math.abs(diff);
    }

    let avgGain = gain / period;
    let avgLoss = loss / period;
    result[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);

    for (let i = period + 1; i < values.length; i += 1) {
      const diff = values[i] - values[i - 1];
      const currentGain = diff > 0 ? diff : 0;
      const currentLoss = diff < 0 ? Math.abs(diff) : 0;

      avgGain = (avgGain * (period - 1) + currentGain) / period;
      avgLoss = (avgLoss * (period - 1) + currentLoss) / period;

      if (avgLoss === 0) result[i] = 100;
      else {
        const rs = avgGain / avgLoss;
        result[i] = 100 - 100 / (1 + rs);
      }
    }

    return result;
  }

  static ema(values, period) {
    if (!Array.isArray(values) || values.length < period) {
      return new Array(values?.length || 0).fill(null);
    }

    const result = new Array(values.length).fill(null);
    const multiplier = 2 / (period + 1);
    let seed = 0;
    for (let i = 0; i < period; i += 1) {
      seed += values[i];
    }
    result[period - 1] = seed / period;

    for (let i = period; i < values.length; i += 1) {
      const prev = result[i - 1] ?? values[i - 1];
      result[i] = (values[i] - prev) * multiplier + prev;
    }

    return result;
  }

  static atr(candles, period = 14) {
    if (!Array.isArray(candles) || candles.length < period + 1) {
      return new Array(candles?.length || 0).fill(null);
    }

    const trs = new Array(candles.length).fill(null);
    for (let i = 1; i < candles.length; i += 1) {
      const high = candles[i].high;
      const low = candles[i].low;
      const prevClose = candles[i - 1].close;
      const tr = Math.max(high - low, Math.abs(high - prevClose), Math.abs(low - prevClose));
      trs[i] = tr;
    }

    const atr = new Array(candles.length).fill(null);
    let seed = 0;
    for (let i = 1; i <= period; i += 1) {
      seed += trs[i] || 0;
    }
    atr[period] = seed / period;

    for (let i = period + 1; i < candles.length; i += 1) {
      atr[i] = ((atr[i - 1] || 0) * (period - 1) + (trs[i] || 0)) / period;
    }

    return atr;
  }

  static calcVolumeStatus(candles, lookback = 6) {
    const volumes = candles.map((c) => c.volume || 0);
    const current = volumes[volumes.length - 1] || 0;
    const prev = volumes.slice(-1 - lookback, -1);
    const avgPrev = average(prev) || 1;
    const ratio = current / avgPrev;

    let status = 'THẤP';
    if (ratio > 1.35) status = 'CAO';
    else if (ratio > 1.05) status = 'TRUNG_BÌNH';

    return {
      current,
      average: avgPrev,
      ratio,
      status,
    };
  }

  static calcPressure(candles, lookback = 8) {
    if (!candles || candles.length < 4) {
      return {
        buyPressure: 0,
        sellPressure: 0,
        candleStrength: 0,
        buyLevel: 'YẾU',
        sellLevel: 'YẾU',
        deltaVolume: 0,
        deltaVolumeTrend: 0,
        priceSpeed: 0,
        absorptionScore: 0,
        imbalance: 0,
      };
    }

    const slice = candles.slice(-lookback);
    const bodyRatios = [];
    const closesNearHigh = [];
    const closesNearLow = [];
    const ranges = [];
    const volumes = [];
    const signedVolumes = [];
    const closes = [];
    const absorptionScores = [];

    for (const c of slice) {
      const range = Math.max(c.high - c.low, 0.0000001);
      const body = Math.abs(c.close - c.open);
      const bodyRatio = body / range;
      const closeNearHigh = (c.close - c.low) / range;
      const closeNearLow = (c.high - c.close) / range;
      const signedVolume = ((c.close - c.open) / range) * (c.volume || 0);
      const absorption = (1 - bodyRatio) * (Math.max(closeNearHigh, closeNearLow) || 0.5);

      bodyRatios.push(bodyRatio);
      closesNearHigh.push(closeNearHigh);
      closesNearLow.push(closeNearLow);
      ranges.push(range);
      volumes.push(c.volume || 0);
      signedVolumes.push(signedVolume);
      closes.push(c.close);
      absorptionScores.push(absorption);
    }

    const avgRange = average(ranges) || 1;
    const avgVolume = average(volumes) || 1;
    const totalVolume = volumes.reduce((sum, v) => sum + v, 0) || 1;
    const deltaVolume = signedVolumes.reduce((sum, v) => sum + v, 0);
    const deltaVolumeTrend = (deltaVolume / totalVolume) * 100;

    const last = slice[slice.length - 1];
    const lastRange = Math.max(last.high - last.low, 0.0000001);
    const lastBody = Math.abs(last.close - last.open);
    const lastBodyRatio = lastBody / lastRange;
    const first = slice[0];
    const elapsedSec = Math.max((last.time || 0) - (first.time || 0), 60);
    const priceSpeed = ((last.close - first.close) / first.close) / (elapsedSec / 60);

    const lastBuyComponent = ((last.close - last.low) / lastRange) * lastBodyRatio;
    const lastSellComponent = ((last.high - last.close) / lastRange) * lastBodyRatio;
    const speedBoostBuy = clamp(priceSpeed * 1800, -25, 25);
    const speedBoostSell = clamp(-priceSpeed * 1800, -25, 25);

    const volumeBoost = clamp((last.volume || 0) / avgVolume, 0.4, 2.5);
    const rangeBoost = clamp(lastRange / avgRange, 0.5, 2.5);
    const deltaBoostBuy = clamp(deltaVolumeTrend, -30, 30);
    const deltaBoostSell = clamp(-deltaVolumeTrend, -30, 30);
    const absorptionScore = normalizeStrength(average(absorptionScores) * 100, 5, 80);

    const buyRaw =
      average(closesNearHigh) * 40 +
      average(bodyRatios) * 30 +
      lastBuyComponent * 20 +
      volumeBoost * 6 +
      rangeBoost * 4 +
      deltaBoostBuy * 0.5 +
      speedBoostBuy * 0.45;

    const sellRaw =
      average(closesNearLow) * 40 +
      average(bodyRatios) * 30 +
      lastSellComponent * 20 +
      volumeBoost * 6 +
      rangeBoost * 4 +
      deltaBoostSell * 0.5 +
      speedBoostSell * 0.45;

    const buyPressure = normalizeStrength(buyRaw, 10, 100);
    const sellPressure = normalizeStrength(sellRaw, 10, 100);
    const candleStrength = normalizeStrength(lastBodyRatio * 100 * volumeBoost, 10, 220);
    const imbalance = buyPressure - sellPressure;

    const levelOf = (value) => {
      if (value >= 72) return 'MẠNH';
      if (value >= 52) return 'TRUNG_BÌNH';
      return 'YẾU';
    };

    return {
      buyPressure,
      sellPressure,
      candleStrength,
      buyLevel: levelOf(buyPressure),
      sellLevel: levelOf(sellPressure),
      deltaVolume,
      deltaVolumeTrend,
      priceSpeed,
      absorptionScore,
      imbalance,
      absorption:
        lastBodyRatio < 0.35 &&
        volumeBoost > 1.25 &&
        Math.abs(last.close - last.open) / (last.high - last.low || 1) < 0.3,
      impulse: lastBodyRatio > 0.68 && volumeBoost > 1.15,
    };
  }

  static calculate(candles) {
    if (!candles || candles.length < 10) {
      return {
        ma7: [],
        ma25: [],
        ma99: [],
        ma200: [],
        rsi14: [],
        latest: null,
      };
    }

    const closes = candles.map((c) => c.close);
    const ma7 = this.sma(closes, 7);
    const ma25 = this.sma(closes, 25);
    const ma99 = this.sma(closes, 99);
    const ma200 = this.sma(closes, 200);
    const ema20 = this.ema(closes, 20);
    const rsi14 = this.rsi(closes, 14);
    const atr14 = this.atr(candles, 14);

    const latestIndex = candles.length - 1;

    return {
      ma7,
      ma25,
      ma99,
      ma200,
      ema20,
      rsi14,
      atr14,
      latest: {
        ma7: ma7[latestIndex],
        ma25: ma25[latestIndex],
        ma99: ma99[latestIndex],
        ma200: ma200[latestIndex],
        ema20: ema20[latestIndex],
        rsi14: rsi14[latestIndex],
        atr14: atr14[latestIndex],
        ma7Slope: calcSlope(ma7.filter((v) => v !== null), 5),
        ma25Slope: calcSlope(ma25.filter((v) => v !== null), 5),
        ma99Slope: calcSlope(ma99.filter((v) => v !== null), 6),
        ma200Slope: calcSlope(ma200.filter((v) => v !== null), 6),
      },
      volumeStatus: this.calcVolumeStatus(candles),
      pressure: this.calcPressure(candles),
    };
  }
}

module.exports = IndicatorService;
