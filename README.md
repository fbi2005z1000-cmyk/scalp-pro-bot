# Auto Trade Folder

This folder isolates Binance auto-trade logic so it can evolve without touching core signal rendering code.

## Files

- `index.js`: export hub for auto-trade modules.
- `autoTradeProfile.js`: profile tuning (`SAFE`, `BALANCED`, `AGGRESSIVE`).
- `capitalPlan.js`: position sizing from equity, risk, leverage, and stop distance.
- `safetyRules.js`: hard reject gate for anti-burn protection.
- `binanceOrderPayload.js`: Binance Futures payload builder with precision normalization.
- `intentModel.js`: điểm hóa ý đồ thị trường (trend/pressure/liquidity/timing) để chọn kèo AUTO chất lượng cao.
- `autoTradeEngine.js`: engine tổng hợp cho `AUTO_BOT` (max 1 lệnh, anti-duplicate, confidence/win-prob gate, reject reason rõ ràng).

## Integration target

Use these modules from:

- `backend/botEngine.js` (final execution decision)
- `backend/riskManager.js` (hard reject + emergency guard)
- `backend/orderManager.js` (payload building before signed REST order)

## Core flow mới cho AUTO

1. `signalEngine` sinh tín hiệu.
2. `AutoTradeEngine` trong folder này chạy gate nhiều lớp:
   - Safety rules
   - Intent score
   - Win probability
   - Duplicate/cooldown/max-one-position
3. Nếu pass mới cho phép `orderManager` đặt lệnh.
