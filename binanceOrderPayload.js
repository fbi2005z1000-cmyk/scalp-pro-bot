function floorToStep(value, step) {
  const v = Number(value);
  const s = Number(step);
  if (!(Number.isFinite(v) && Number.isFinite(s) && s > 0)) return 0;
  const times = Math.floor(v / s);
  return times * s;
}

function roundToTick(value, tickSize) {
  const v = Number(value);
  const t = Number(tickSize);
  if (!(Number.isFinite(v) && Number.isFinite(t) && t > 0)) return 0;
  return Math.round(v / t) * t;
}

function buildFuturesOrderPayload({
  symbol,
  side,
  quantity,
  orderType = 'MARKET',
  reduceOnly = false,
  filters = {},
} = {}) {
  const stepSize = Number(filters.stepSize || 0);
  const minQty = Number(filters.minQty || 0);
  const tickSize = Number(filters.tickSize || 0);

  let qty = Number(quantity || 0);
  if (stepSize > 0) qty = floorToStep(qty, stepSize);
  if (minQty > 0 && qty < minQty) {
    return {
      ok: false,
      reason: 'QTY_BELOW_MIN',
      payload: null,
    };
  }

  const normalizedSymbol = String(symbol || '').toUpperCase();
  const normalizedSide = String(side || '').toUpperCase();
  if (!normalizedSymbol || (normalizedSide !== 'BUY' && normalizedSide !== 'SELL')) {
    return {
      ok: false,
      reason: 'INVALID_ORDER_INPUT',
      payload: null,
    };
  }

  const payload = {
    symbol: normalizedSymbol,
    side: normalizedSide,
    type: String(orderType || 'MARKET').toUpperCase(),
    quantity: qty,
  };

  if (reduceOnly) payload.reduceOnly = 'true';
  if (tickSize > 0) payload.price = roundToTick(payload.price, tickSize);

  return {
    ok: true,
    reason: 'OK',
    payload,
  };
}

module.exports = {
  buildFuturesOrderPayload,
};

