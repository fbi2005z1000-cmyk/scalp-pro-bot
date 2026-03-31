const WebSocket = require('ws');

class WebsocketManager {
  constructor(config, logger, stateStore) {
    this.config = config;
    this.logger = logger;
    this.stateStore = stateStore;

    this.ws = null;
    this.symbol = config.binance.symbol;
    this.reconnectAttempt = 0;
    this.manualClose = false;
    this.lastMessageAt = 0;
    this.lastPongAt = 0;
    this.lastKlineTimeByTf = {};
    this.heartbeatTimer = null;
    this.circuitOpenUntil = 0;
    this.handlers = {
      kline: [],
      trade: [],
      book: [],
      mark: [],
      reconnect: [],
      error: [],
      stale: [],
      gap: [],
      circuit: [],
    };
  }

  on(eventName, cb) {
    if (this.handlers[eventName]) {
      this.handlers[eventName].push(cb);
    }
  }

  emit(eventName, payload) {
    const list = this.handlers[eventName] || [];
    for (const cb of list) {
      try {
        cb(payload);
      } catch (error) {
        this.logger.error('websocket', 'Lỗi callback WS', { error: error.message, eventName });
      }
    }
  }

  buildUrl(symbol) {
    const s = symbol.toLowerCase();
    const tfSet = new Set([
      '1m',
      '3m',
      '5m',
      '15m',
      String(this.config.timeframe.analysis || '3m').toLowerCase(),
    ]);
    const tfStreams = Array.from(tfSet).map((tf) => `${s}@kline_${tf}`);
    const streams = [...tfStreams, `${s}@bookTicker`, `${s}@markPrice@1s`, `${s}@aggTrade`];
    return `${this.config.binance.wsBaseUrl}?streams=${streams.join('/')}`;
  }

  connect(symbol = this.symbol) {
    this.symbol = symbol;
    this.manualClose = false;
    this.lastKlineTimeByTf = {};

    const now = Date.now();
    if (this.circuitOpenUntil > now) {
      const wait = this.circuitOpenUntil - now;
      this.logger.warn('websocket', 'Circuit breaker đang mở, trì hoãn connect', {
        waitMs: wait,
        symbol,
      });
      setTimeout(() => {
        if (!this.manualClose) this.connect(this.symbol);
      }, wait);
      return;
    }

    const url = this.buildUrl(symbol);

    this.ws = new WebSocket(url);

    this.ws.on('open', () => {
      this.reconnectAttempt = 0;
      this.lastMessageAt = Date.now();
      this.lastPongAt = Date.now();
      this.logger.info('websocket', 'Đã kết nối Binance WS', { symbol });
      this.startHeartbeat();
    });

    this.ws.on('message', (raw) => {
      this.lastMessageAt = Date.now();
      this.handleMessage(raw.toString());
    });

    this.ws.on('pong', () => {
      this.lastPongAt = Date.now();
    });

    this.ws.on('close', () => {
      this.clearHeartbeat();
      this.logger.warn('websocket', 'WS đã đóng', { symbol });
      if (!this.manualClose) {
        this.scheduleReconnect();
      }
    });

    this.ws.on('error', (error) => {
      this.logger.error('websocket', 'WS lỗi', { error: error.message, symbol });
      this.emit('error', { error });
    });
  }

  disconnect() {
    this.manualClose = true;
    this.clearHeartbeat();
    if (this.ws) this.ws.close();
  }

  switchSymbol(symbol) {
    this.disconnect();
    setTimeout(() => this.connect(symbol), 400);
  }

  scheduleReconnect() {
    this.reconnectAttempt += 1;
    const maxAttempts = this.config.websocket.maxReconnectAttempts;
    if (this.reconnectAttempt > maxAttempts) {
      const cooldown = this.config.websocket.circuitCooldownMs;
      this.circuitOpenUntil = Date.now() + cooldown;
      this.emit('circuit', {
        symbol: this.symbol,
        attempt: this.reconnectAttempt,
        cooldownMs: cooldown,
      });
      this.logger.error('websocket', 'Mở circuit breaker WS do reconnect lỗi liên tiếp', {
        symbol: this.symbol,
        reconnectAttempt: this.reconnectAttempt,
        cooldownMs: cooldown,
      });
      this.reconnectAttempt = 0;
      setTimeout(() => {
        if (!this.manualClose) this.connect(this.symbol);
      }, cooldown);
      return;
    }

    const delay = Math.min(1800 * this.reconnectAttempt, 16000);

    this.stateStore.registerReconnect();
    this.emit('reconnect', { attempt: this.reconnectAttempt, delay });

    setTimeout(() => {
      if (!this.manualClose) {
        this.connect(this.symbol);
      }
    }, delay);
  }

  startHeartbeat() {
    this.clearHeartbeat();
    const tick = Math.max(800, this.config.websocket.heartbeatIntervalMs);
    const staleMs = Math.max(1200, this.config.websocket.staleDataMs);

    this.heartbeatTimer = setInterval(() => {
      const now = Date.now();
      if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;

      const staleFor = now - this.lastMessageAt;
      if (staleFor > staleMs) {
        this.emit('stale', {
          symbol: this.symbol,
          staleForMs: staleFor,
          staleDataMs: staleMs,
        });
        this.logger.warn('websocket', 'Phát hiện stale data, chủ động reconnect', {
          symbol: this.symbol,
          staleForMs: staleFor,
        });
        this.ws.terminate();
        return;
      }

      const pongStale = now - this.lastPongAt;
      if (pongStale > staleMs * 1.8) {
        this.logger.warn('websocket', 'Không nhận pong đúng hạn, reconnect', {
          symbol: this.symbol,
          pongStaleMs: pongStale,
        });
        this.ws.terminate();
        return;
      }

      try {
        this.ws.ping();
      } catch (error) {
        this.emit('error', { error });
      }
    }, tick);
  }

  clearHeartbeat() {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = null;
  }

  handleMessage(payload) {
    try {
      const data = JSON.parse(payload);
      const stream = data.stream;
      const body = data.data;

      if (!stream || !body) return;
      const streamSymbol = String(stream.split('@')[0] || '').toUpperCase();

      const klineMatch = stream.match(/@kline_(\d+m)$/);
      if (klineMatch) {
        const timeframe = klineMatch[1];
        const k = body.k;
        const candle = {
          symbol: String(k?.s || body?.s || streamSymbol || this.symbol).toUpperCase(),
          time: Math.floor(k.t / 1000),
          open: Number(k.o),
          high: Number(k.h),
          low: Number(k.l),
          close: Number(k.c),
          volume: Number(k.v),
          closeTime: k.T,
          isClosed: k.x,
          eventTime: body.E,
          latencyMs: Date.now() - body.E,
          timeframe,
        };

        const tfMinutes = Number(String(timeframe).replace('m', ''));
        const tfSec = Number.isFinite(tfMinutes) && tfMinutes > 0 ? tfMinutes * 60 : 60;
        const prevKlineTime = this.lastKlineTimeByTf[timeframe] || 0;
        if (prevKlineTime && candle.time > prevKlineTime + tfSec) {
          const missingCandles = Math.floor((candle.time - prevKlineTime) / tfSec) - 1;
          const missingMinutes = missingCandles * (tfSec / 60);
          if (missingMinutes > 0) {
            this.emit('gap', {
              symbol: this.symbol,
              timeframe,
              from: prevKlineTime,
              to: candle.time,
              missingMinutes,
            });
            this.logger.warn('websocket', 'Phát hiện gap dữ liệu nến', {
              symbol: this.symbol,
              timeframe,
              missingMinutes,
              from: prevKlineTime,
              to: candle.time,
            });
          }
        }
        this.lastKlineTimeByTf[timeframe] = Math.max(prevKlineTime, candle.time);

        this.emit('kline', candle);
      }

      if (stream.endsWith('@bookTicker')) {
        const bestBid = Number(body.b);
        const bestAsk = Number(body.a);
        const spreadPct = bestAsk > 0 ? (bestAsk - bestBid) / bestAsk : 0;
        const lastTradePrice = (bestBid + bestAsk) / 2;
        const payloadBook = {
          symbol: String(body.s || streamSymbol || this.symbol).toUpperCase(),
          bestBid,
          bestAsk,
          spreadPct,
          lastTradePrice,
          lastPrice:
            this.config.trading.priceSource === 'MARK'
              ? this.stateStore.state.orderBook.markPrice || lastTradePrice
              : lastTradePrice,
          eventTime: body.E,
          latencyMs: Date.now() - body.E,
        };

        this.stateStore.setOrderBook(payloadBook);
        this.emit('book', payloadBook);
      }

      if (stream.endsWith('@aggTrade')) {
        const tradePayload = {
          symbol: String(body.s || streamSymbol || this.symbol).toUpperCase(),
          price: Number(body.p || 0),
          quantity: Number(body.q || 0),
          eventTime: body.E,
          tradeTime: body.T,
          latencyMs: Date.now() - body.E,
        };
        this.emit('trade', tradePayload);
      }

      if (stream.includes('@markPrice')) {
        const markPrice = Number(body.p || 0);
        const markPayload = {
          symbol: String(body.s || streamSymbol || this.symbol).toUpperCase(),
          markPrice,
          indexPrice: Number(body.i || 0),
          fundingRate: Number(body.r || 0),
          eventTime: body.E,
          latencyMs: Date.now() - body.E,
        };
        if (this.config.trading.priceSource === 'MARK' && markPrice > 0) {
          markPayload.lastPrice = markPrice;
        }
        this.stateStore.setOrderBook(markPayload);
        this.emit('mark', markPayload);
      }
    } catch (error) {
      this.logger.error('websocket', 'Lỗi parse message WS', { error: error.message });
    }
  }
}

module.exports = WebsocketManager;
