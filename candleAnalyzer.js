const { average } = require('./utils');

class CandleAnalyzer {
  static getCandleMetrics(c) {
    const range = Math.max(c.high - c.low, 0.0000001);
    const body = Math.abs(c.close - c.open);
    const upperWick = c.high - Math.max(c.open, c.close);
    const lowerWick = Math.min(c.open, c.close) - c.low;
    return {
      range,
      body,
      bodyRatio: body / range,
      upperWick,
      lowerWick,
      upperWickRatio: upperWick / range,
      lowerWickRatio: lowerWick / range,
      bullish: c.close > c.open,
      bearish: c.close < c.open,
    };
  }

  static analyze(candles) {
    if (!candles || candles.length < 5) {
      return {
        latest: {},
        patterns: {},
        score: { bull: 0, bear: 0 },
        summary: 'Dữ liệu nến chưa đủ',
      };
    }

    const last = candles[candles.length - 1];
    const prev = candles[candles.length - 2];
    const prev2 = candles[candles.length - 3];
    const slice = candles.slice(-10);

    const mLast = this.getCandleMetrics(last);
    const mPrev = this.getCandleMetrics(prev);
    const mPrev2 = this.getCandleMetrics(prev2);

    const ranges = slice.map((c) => Math.max(c.high - c.low, 0.0000001));
    const avgRange = average(ranges);

    const longBody = mLast.bodyRatio > 0.62;
    const smallBody = mLast.bodyRatio < 0.3;
    const lowerPinbar = mLast.lowerWickRatio > 0.45 && mLast.bodyRatio < 0.45;
    const upperPinbar = mLast.upperWickRatio > 0.45 && mLast.bodyRatio < 0.45;

    const bullishEngulfing =
      mLast.bullish &&
      mPrev.bearish &&
      last.open <= prev.close &&
      last.close >= prev.open;

    const bearishEngulfing =
      mLast.bearish &&
      mPrev.bullish &&
      last.open >= prev.close &&
      last.close <= prev.open;

    const strongBreakout =
      mLast.bullish &&
      longBody &&
      last.close > Math.max(prev.high, prev2.high) &&
      mLast.range > avgRange * 1.25;

    const strongBreakdown =
      mLast.bearish &&
      longBody &&
      last.close < Math.min(prev.low, prev2.low) &&
      mLast.range > avgRange * 1.25;

    const fakeBreakout =
      prev.close > Math.max(...slice.slice(0, -2).map((c) => c.high)) && last.close < prev.close;

    const fakeBreakdown =
      prev.close < Math.min(...slice.slice(0, -2).map((c) => c.low)) && last.close > prev.close;

    const bullishConfirm =
      bullishEngulfing ||
      strongBreakout ||
      (mLast.bullish && longBody && last.close > prev.high) ||
      lowerPinbar;

    const bearishConfirm =
      bearishEngulfing ||
      strongBreakdown ||
      (mLast.bearish && longBody && last.close < prev.low) ||
      upperPinbar;

    const absorptionCandle = smallBody && (mLast.upperWickRatio > 0.35 || mLast.lowerWickRatio > 0.35);
    const exhaustion = mLast.range > avgRange * 1.6 && mLast.bodyRatio < 0.38;

    let bull = 0;
    let bear = 0;

    if (bullishEngulfing) bull += 25;
    if (strongBreakout) bull += 20;
    if (lowerPinbar) bull += 16;
    if (mLast.bullish && longBody) bull += 14;
    if (last.close > prev.high) bull += 8;

    if (bearishEngulfing) bear += 25;
    if (strongBreakdown) bear += 20;
    if (upperPinbar) bear += 16;
    if (mLast.bearish && longBody) bear += 14;
    if (last.close < prev.low) bear += 8;

    if (exhaustion) {
      bull -= 5;
      bear -= 5;
    }

    const summary = bullishConfirm
      ? 'Nến xác nhận tăng rõ'
      : bearishConfirm
      ? 'Nến xác nhận giảm rõ'
      : 'Nến xác nhận yếu, cần chờ thêm';

    return {
      latest: {
        ...mLast,
        open: last.open,
        high: last.high,
        low: last.low,
        close: last.close,
      },
      patterns: {
        longBody,
        smallBody,
        lowerPinbar,
        upperPinbar,
        bullishEngulfing,
        bearishEngulfing,
        strongBreakout,
        strongBreakdown,
        fakeBreakout,
        fakeBreakdown,
        bullishConfirm,
        bearishConfirm,
        absorptionCandle,
        exhaustion,
      },
      score: {
        bull,
        bear,
      },
      summary,
    };
  }
}

module.exports = CandleAnalyzer;
