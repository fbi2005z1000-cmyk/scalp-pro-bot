# Auto Trade Folder

This folder isolates Binance auto-trade logic so it can evolve without touching core signal rendering code.

## Files

- `index.js`: export hub for auto-trade modules.
- `autoTradeProfile.js`: profile tuning (`SAFE`, `BALANCED`, `AGGRESSIVE`).
- `capitalPlan.js`: position sizing from equity, risk, leverage, and stop distance.
- `safetyRules.js`: hard reject gate for anti-burn protection.
- `binanceOrderPayload.js`: Binance Futures payload builder with precision normalization.

## Integration target

Use these modules from:

- `backend/botEngine.js` (final execution decision)
- `backend/riskManager.js` (hard reject + emergency guard)
- `backend/orderManager.js` (payload building before signed REST order)

