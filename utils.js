const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

const safeNumber = (value, fallback = 0) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const nowTs = () => Date.now();

const DEFAULT_TIMEZONE = process.env.APP_TIMEZONE || process.env.TZ || 'Asia/Ho_Chi_Minh';

function getDateParts(ts, timezone = DEFAULT_TIMEZONE) {
  const date = new Date(Number.isFinite(Number(ts)) ? Number(ts) : Date.now());
  const safeTz = timezone || DEFAULT_TIMEZONE;

  try {
    const parts = new Intl.DateTimeFormat('en-GB', {
      timeZone: safeTz,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
      hour12: false,
    }).formatToParts(date);
    const map = Object.fromEntries(parts.map((p) => [p.type, p.value]));
    return {
      year: map.year,
      month: map.month,
      day: map.day,
      hour: map.hour,
      minute: map.minute,
      second: map.second,
    };
  } catch (_) {
    // fallback UTC n?u timezone l?i
    return {
      year: String(date.getUTCFullYear()),
      month: String(date.getUTCMonth() + 1).padStart(2, '0'),
      day: String(date.getUTCDate()).padStart(2, '0'),
      hour: String(date.getUTCHours()).padStart(2, '0'),
      minute: String(date.getUTCMinutes()).padStart(2, '0'),
      second: String(date.getUTCSeconds()).padStart(2, '0'),
    };
  }
}

const formatTs = (ts, timezone = DEFAULT_TIMEZONE) => {
  const p = getDateParts(ts, timezone);
  return `${p.year}-${p.month}-${p.day} ${p.hour}:${p.minute}:${p.second}`;
};

const formatDateKey = (ts, timezone = DEFAULT_TIMEZONE) => {
  const p = getDateParts(ts, timezone);
  return `${p.year}-${p.month}-${p.day}`;
};

const percentile = (arr, p) => {
  if (!arr.length) return 0;
  const sorted = [...arr].sort((a, b) => a - b);
  const idx = clamp(Math.floor((sorted.length - 1) * p), 0, sorted.length - 1);
  return sorted[idx];
};

const average = (arr) => {
  if (!arr.length) return 0;
  return arr.reduce((sum, val) => sum + val, 0) / arr.length;
};

const stdDev = (arr) => {
  if (arr.length < 2) return 0;
  const avg = average(arr);
  const variance = average(arr.map((n) => (n - avg) ** 2));
  return Math.sqrt(variance);
};

const calcSlope = (arr, lookback = 5) => {
  if (!arr || arr.length < lookback + 1) return 0;
  const slice = arr.slice(-lookback);
  if (!slice.length) return 0;
  return (slice[slice.length - 1] - slice[0]) / slice.length;
};

const aggregateCandles = (candles, factor, options = {}) => {
  const includePartial = Boolean(options.includePartial);
  if (!Array.isArray(candles) || !candles.length || factor < 2) return [];
  if (!includePartial && candles.length < factor) return [];
  const resultMap = new Map();
  const sorted = [...candles].sort((a, b) => a.time - b.time);
  const bucketSec = factor * 60;

  for (const c of sorted) {
    const key = Math.floor(c.time / bucketSec) * bucketSec;
    const existing = resultMap.get(key);
    if (!existing) {
      resultMap.set(key, {
        time: key,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
        volume: c.volume || 0,
        quoteVolume: c.quoteVolume || 0,
        tradeCount: c.tradeCount || 0,
        takerBuyBaseVolume: c.takerBuyBaseVolume || 0,
        takerBuyQuoteVolume: c.takerBuyQuoteVolume || 0,
        closeTime: c.closeTime,
        _count: 1,
        _isAllClosed: Boolean(c.isClosed),
      });
    } else {
      existing.high = Math.max(existing.high, c.high);
      existing.low = Math.min(existing.low, c.low);
      existing.close = c.close;
      existing.volume += c.volume || 0;
      existing.quoteVolume += c.quoteVolume || 0;
      existing.tradeCount += c.tradeCount || 0;
      existing.takerBuyBaseVolume += c.takerBuyBaseVolume || 0;
      existing.takerBuyQuoteVolume += c.takerBuyQuoteVolume || 0;
      existing.closeTime = c.closeTime;
      existing._count += 1;
      existing._isAllClosed = existing._isAllClosed && Boolean(c.isClosed);
    }
  }

  const result = Array.from(resultMap.values())
    .sort((a, b) => a.time - b.time)
    .filter((c) => includePartial || c._count >= factor)
    .map((c) => ({
      time: c.time,
      open: c.open,
      high: c.high,
      low: c.low,
      close: c.close,
      volume: c.volume,
      quoteVolume: c.quoteVolume,
      tradeCount: c.tradeCount,
      takerBuyBaseVolume: c.takerBuyBaseVolume,
      takerBuyQuoteVolume: c.takerBuyQuoteVolume,
      closeTime: c.closeTime,
      isClosed: c._count >= factor && c._isAllClosed,
    }));

  return result;
};

const detectPriceStructure = (candles, lookback = 8) => {
  if (!candles || candles.length < lookback + 2) {
    return { structure: 'UNKNOWN', higherHighs: false, lowerLows: false };
  }

  const slice = candles.slice(-(lookback + 2));
  const highs = slice.map((c) => c.high);
  const lows = slice.map((c) => c.low);

  let hh = 0;
  let hl = 0;
  let lh = 0;
  let ll = 0;

  for (let i = 1; i < highs.length; i += 1) {
    if (highs[i] > highs[i - 1]) hh += 1;
    if (highs[i] < highs[i - 1]) lh += 1;
    if (lows[i] > lows[i - 1]) hl += 1;
    if (lows[i] < lows[i - 1]) ll += 1;
  }

  if (hh >= highs.length * 0.55 && hl >= lows.length * 0.55) {
    return { structure: 'BULLISH', higherHighs: true, lowerLows: false, hh, hl, lh, ll };
  }
  if (lh >= highs.length * 0.55 && ll >= lows.length * 0.55) {
    return { structure: 'BEARISH', higherHighs: false, lowerLows: true, hh, hl, lh, ll };
  }

  return { structure: 'RANGE', higherHighs: false, lowerLows: false, hh, hl, lh, ll };
};

const normalizeStrength = (value, min, max) => {
  if (max - min === 0) return 50;
  return clamp(((value - min) / (max - min)) * 100, 0, 100);
};

const uniqueId = () => `${Date.now()}-${Math.random().toString(16).slice(2, 10)}`;

module.exports = {
  clamp,
  safeNumber,
  nowTs,
  formatTs,
  formatDateKey,
  DEFAULT_TIMEZONE,
  percentile,
  average,
  stdDev,
  calcSlope,
  aggregateCandles,
  detectPriceStructure,
  normalizeStrength,
  uniqueId,
};
