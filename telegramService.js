const axios = require('axios');
const fs = require('fs');
const FormData = require('form-data');

function esc(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;');
}

class TelegramService {
  constructor(config, logger) {
    this.config = config.telegram;
    this.logger = logger;
    this.lastSentAt = 0;
    this.queue = Promise.resolve();
    this.idempotency = new Map();
    this.routes = this.buildRoutes();
    this.metrics = {
      sent: 0,
      failed: 0,
      retried: 0,
      droppedDuplicate: 0,
    };
  }

  buildRoutes() {
    const routes = [];
    const parseIds = (raw) =>
      String(raw || '')
        .split(',')
        .map((x) => x.trim())
        .filter(Boolean);
    const toSet = (arr) => new Set((Array.isArray(arr) ? arr : []).map((s) => String(s).toUpperCase()));

    const primaryIds = parseIds(this.config.chatId);
    if (this.config.token && primaryIds.length) {
      routes.push({
        id: 'PRIMARY',
        name: this.config.primaryName || 'BOT THANH KHOẢN CAO',
        token: this.config.token,
        chatIds: primaryIds,
        symbols: toSet(this.config.highLiqSymbols),
      });
    }

    const secondaryIds = parseIds(this.config.secondaryChatId);
    if (this.config.secondaryEnabled && this.config.secondaryToken && secondaryIds.length) {
      routes.push({
        id: 'SECONDARY',
        name: this.config.secondaryName || 'BOT SCP',
        token: this.config.secondaryToken,
        chatIds: secondaryIds,
        symbols: toSet(this.config.scpSymbols),
      });
    }

    return routes;
  }

  isEnabled() {
    return this.config.enabled && this.routes.length > 0;
  }

  getTargetRoutes(options = {}) {
    if (!this.routes.length) return [];

    const forcedRoute = String(options.forceRoute || '').toUpperCase();
    if (forcedRoute) {
      const forced = this.routes.filter((r) => r.id === forcedRoute);
      if (forced.length) return forced;
      return [];
    }

    if (options.broadcast) return this.routes;

    const symbol = String(options.symbol || '').toUpperCase();
    if (!symbol) {
      if (this.config.sendSystemToAllRoutes) return this.routes;
      return [this.routes[0]];
    }

    const matched = this.routes.find((r) => r.symbols && r.symbols.has(symbol));
    if (matched) return [matched];

    if (this.config.routeUnknownToPrimary) {
      const primary = this.routes.find((r) => r.id === 'PRIMARY') || this.routes[0];
      return primary ? [primary] : [];
    }
    return [this.routes[0]];
  }

  getMetrics() {
    return {
      ...this.metrics,
      routes: this.routes.map((r) => ({
        id: r.id,
        name: r.name,
        chats: r.chatIds.length,
        symbols: r.symbols ? r.symbols.size : 0,
      })),
    };
  }

  getSymbolIcon(symbol) {
    const s = String(symbol || '').toUpperCase();
    if (s.includes('BTC')) return '₿🟠';
    if (s.includes('ETH')) return 'Ξ🟣';
    if (s.includes('SOL')) return '◎🟢';
    return '🔹';
  }

  enqueue(task) {
    this.queue = this.queue.then(() => task()).catch(() => {});
    return this.queue;
  }

  shouldDropByIdempotency(key, ttlMs = 30000) {
    if (!key) return false;
    const now = Date.now();

    for (const [k, exp] of this.idempotency.entries()) {
      if (exp <= now) this.idempotency.delete(k);
    }

    const prev = this.idempotency.get(key);
    if (prev && prev > now) {
      this.metrics.droppedDuplicate += 1;
      return true;
    }

    this.idempotency.set(key, now + ttlMs);
    return false;
  }

  async cooldown(customMs = null) {
    const cooldownMs = Number.isFinite(Number(customMs))
      ? Math.max(0, Number(customMs))
      : this.config.cooldownMs;
    const now = Date.now();
    const diff = now - this.lastSentAt;
    if (diff < cooldownMs) {
      const wait = cooldownMs - diff;
      await new Promise((resolve) => setTimeout(resolve, wait));
    }
  }

  async postWithRetry(route, endpoint, payload, isMultipart = false) {
    let attempt = 0;
    let lastErr;

    while (attempt <= this.config.maxRetry) {
      try {
        const url = `https://api.telegram.org/bot${route.token}/${endpoint}`;
        if (isMultipart) {
          const response = await axios.post(url, payload, {
            timeout: 12000,
            headers: payload.getHeaders(),
          });
          return response.data;
        }

        const response = await axios.post(url, payload, { timeout: 12000 });
        return response.data;
      } catch (error) {
        lastErr = error;
        attempt += 1;
        if (attempt <= this.config.maxRetry) {
          this.metrics.retried += 1;
          await new Promise((resolve) => setTimeout(resolve, 650 * attempt));
        }
      }
    }

    throw lastErr;
  }

  async sendText(message, options = {}) {
    if (!this.isEnabled()) return;
    const routes = this.getTargetRoutes(options);
    if (!routes.length) return;

    return this.enqueue(async () => {
      await this.cooldown(options.cooldownMs);
      const idempotencyKey = options.idempotencyKey || null;
      let successCount = 0;
      let failCount = 0;

      for (const route of routes) {
        const routeIdem = idempotencyKey ? `${idempotencyKey}:${route.id}` : null;
        if (this.shouldDropByIdempotency(routeIdem, options.idempotencyTtlMs || 25000)) continue;

        for (const chatId of route.chatIds) {
          try {
            await this.postWithRetry(route, 'sendMessage', {
              chat_id: chatId,
              text: String(message).slice(0, 4096),
              parse_mode: this.config.parseMode,
              disable_web_page_preview: true,
            });
            successCount += 1;
          } catch (error) {
            failCount += 1;
            this.logger.error('telegram', 'Gửi Telegram text lỗi theo chat', {
              route: route.name,
              chatId,
              error: error.message,
            });
          }
        }
      }

      if (successCount > 0) {
        this.lastSentAt = Date.now();
        this.metrics.sent += 1;
        if (failCount > 0) {
          this.logger.warn('telegram', 'Một số chat Telegram lỗi khi gửi text', {
            idempotencyKey,
            successCount,
            failCount,
          });
        } else {
          this.logger.telegram('Đã gửi Telegram text', { idempotencyKey });
        }
      } else {
        this.metrics.failed += 1;
        this.logger.error('telegram', 'Gửi Telegram text lỗi toàn bộ chat', { idempotencyKey });
      }
    });
  }

  async sendPhoto(imagePath, caption = '', options = {}) {
    if (!this.isEnabled()) return false;
    const routes = this.getTargetRoutes(options);
    if (!routes.length) return false;

    return this.enqueue(async () => {
      await this.cooldown(options.cooldownMs);
      const idempotencyKey = options.idempotencyKey || null;

      if (!fs.existsSync(imagePath)) {
        this.metrics.failed += 1;
        this.logger.error('telegram', 'Gửi Telegram ảnh lỗi', { error: `Không tìm thấy ảnh: ${imagePath}` });
        return false;
      }

      let successCount = 0;
      let failCount = 0;

      for (const route of routes) {
        const routeIdem = idempotencyKey ? `${idempotencyKey}:${route.id}` : null;
        if (this.shouldDropByIdempotency(routeIdem, options.idempotencyTtlMs || 25000)) continue;

        for (const chatId of route.chatIds) {
          try {
            const form = new FormData();
            form.append('chat_id', chatId);
            form.append('caption', String(caption).slice(0, 1024));
            form.append('parse_mode', this.config.parseMode);
            form.append('photo', fs.createReadStream(imagePath));
            await this.postWithRetry(route, 'sendPhoto', form, true);
            successCount += 1;
          } catch (error) {
            failCount += 1;
            this.logger.error('telegram', 'Gửi Telegram ảnh lỗi theo chat', {
              route: route.name,
              chatId,
              error: error.message,
            });
          }
        }
      }

      if (successCount > 0) {
        this.lastSentAt = Date.now();
        this.metrics.sent += 1;
        if (failCount > 0) {
          this.logger.warn('telegram', 'Một số chat Telegram lỗi khi gửi ảnh', {
            imagePath,
            idempotencyKey,
            successCount,
            failCount,
          });
        } else {
          this.logger.telegram('Đã gửi Telegram ảnh', { imagePath, idempotencyKey });
        }
        return true;
      }
      this.metrics.failed += 1;
      this.logger.error('telegram', 'Gửi Telegram ảnh lỗi toàn bộ chat', { imagePath, idempotencyKey });
      return false;
    });
  }

  async sendSticker(sticker, options = {}) {
    if (!this.isEnabled()) return false;
    const routes = this.getTargetRoutes(options);
    if (!routes.length) return false;
    const stickerValue = String(sticker || '').trim();
    if (!stickerValue) return false;

    return this.enqueue(async () => {
      await this.cooldown(options.cooldownMs);
      const idempotencyKey = options.idempotencyKey || null;
      let successCount = 0;
      let failCount = 0;

      for (const route of routes) {
        const routeIdem = idempotencyKey ? `${idempotencyKey}:${route.id}` : null;
        if (this.shouldDropByIdempotency(routeIdem, options.idempotencyTtlMs || 25000)) continue;

        for (const chatId of route.chatIds) {
          try {
            await this.postWithRetry(route, 'sendSticker', {
              chat_id: chatId,
              sticker: stickerValue,
            });
            successCount += 1;
          } catch (error) {
            failCount += 1;
            this.logger.error('telegram', 'Gửi Telegram sticker lỗi theo chat', {
              route: route.name,
              chatId,
              error: error.message,
            });
          }
        }
      }

      if (successCount > 0) {
        this.lastSentAt = Date.now();
        this.metrics.sent += 1;
        if (failCount > 0) {
          this.logger.warn('telegram', 'Một số chat Telegram lỗi khi gửi sticker', {
            idempotencyKey,
            successCount,
            failCount,
          });
        } else {
          this.logger.telegram('Đã gửi Telegram sticker', { idempotencyKey });
        }
        return true;
      }
      this.metrics.failed += 1;
      this.logger.error('telegram', 'Gửi Telegram sticker lỗi toàn bộ chat', { idempotencyKey });
      return false;
    });
  }

  async sendAnimation(animation, caption = '', options = {}) {
    if (!this.isEnabled()) return false;
    const routes = this.getTargetRoutes(options);
    if (!routes.length) return false;
    const animationValue = String(animation || '').trim();
    if (!animationValue) return false;

    return this.enqueue(async () => {
      await this.cooldown(options.cooldownMs);
      const idempotencyKey = options.idempotencyKey || null;
      let successCount = 0;
      let failCount = 0;

      for (const route of routes) {
        const routeIdem = idempotencyKey ? `${idempotencyKey}:${route.id}` : null;
        if (this.shouldDropByIdempotency(routeIdem, options.idempotencyTtlMs || 25000)) continue;

        for (const chatId of route.chatIds) {
          try {
            await this.postWithRetry(route, 'sendAnimation', {
              chat_id: chatId,
              animation: animationValue,
              caption: String(caption || '').slice(0, 1024),
              parse_mode: this.config.parseMode,
            });
            successCount += 1;
          } catch (error) {
            failCount += 1;
            this.logger.error('telegram', 'Gửi Telegram animation lỗi theo chat', {
              route: route.name,
              chatId,
              error: error.message,
            });
          }
        }
      }

      if (successCount > 0) {
        this.lastSentAt = Date.now();
        this.metrics.sent += 1;
        if (failCount > 0) {
          this.logger.warn('telegram', 'Một số chat Telegram lỗi khi gửi animation', {
            idempotencyKey,
            successCount,
            failCount,
          });
        } else {
          this.logger.telegram('Đã gửi Telegram animation', { idempotencyKey });
        }
        return true;
      }
      this.metrics.failed += 1;
      this.logger.error('telegram', 'Gửi Telegram animation lỗi toàn bộ chat', { idempotencyKey });
      return false;
    });
  }

  async sendDice(emoji = '🎯', options = {}) {
    if (!this.isEnabled()) return false;
    const routes = this.getTargetRoutes(options);
    if (!routes.length) return false;
    const allowed = new Set(['🎲', '🎯', '🏀', '⚽', '🎳', '🎰']);
    const diceEmoji = allowed.has(emoji) ? emoji : '🎯';

    return this.enqueue(async () => {
      await this.cooldown(options.cooldownMs);
      const idempotencyKey = options.idempotencyKey || null;
      let successCount = 0;
      let failCount = 0;

      for (const route of routes) {
        const routeIdem = idempotencyKey ? `${idempotencyKey}:${route.id}` : null;
        if (this.shouldDropByIdempotency(routeIdem, options.idempotencyTtlMs || 10000)) continue;

        for (const chatId of route.chatIds) {
          try {
            await this.postWithRetry(route, 'sendDice', {
              chat_id: chatId,
              emoji: diceEmoji,
            });
            successCount += 1;
          } catch (error) {
            failCount += 1;
            this.logger.error('telegram', 'Gửi Telegram dice lỗi theo chat', {
              route: route.name,
              chatId,
              error: error.message,
            });
          }
        }
      }

      if (successCount > 0) {
        this.lastSentAt = Date.now();
        this.metrics.sent += 1;
        if (failCount > 0) {
          this.logger.warn('telegram', 'Một số chat Telegram lỗi khi gửi dice', {
            idempotencyKey,
            successCount,
            failCount,
          });
        } else {
          this.logger.telegram('Đã gửi Telegram dice', { idempotencyKey, emoji: diceEmoji });
        }
        return true;
      }
      this.metrics.failed += 1;
      this.logger.error('telegram', 'Gửi Telegram dice lỗi toàn bộ chat', { idempotencyKey, emoji: diceEmoji });
      return false;
    });
  }

  buildEntryZoneText(signalData) {
    const market = signalData.market || {};
    const sr = market.supportResistance || {};
    const supports = Array.isArray(sr.supports) ? sr.supports : [];
    const resistances = Array.isArray(sr.resistances) ? sr.resistances : [];
    const key = sr.keyLevels || {};

    if (signalData.side === 'LONG') {
      const s1 = supports.find((z) => z.label === 'S1') || supports[0];
      const ma25 = market.ma2m?.ma25;
      if (s1?.price) {
        return `Gần ${s1.label || 'S1'} Support (Hỗ trợ) ${Number(s1.price).toFixed(2)}${ma25 ? ` + MA25 ${Number(ma25).toFixed(2)}` : ''}`;
      }
      if (key.support) return `Gần Support (Hỗ trợ) ${Number(key.support).toFixed(2)}`;
      return 'Vùng pullback theo MA25/Support';
    }

    if (signalData.side === 'SHORT') {
      const r1 = resistances.find((z) => z.label === 'R1') || resistances[0];
      const ma25 = market.ma2m?.ma25;
      if (r1?.price) {
        return `Gần ${r1.label || 'R1'} Resistance (Kháng cự) ${Number(r1.price).toFixed(2)}${ma25 ? ` + MA25 ${Number(ma25).toFixed(2)}` : ''}`;
      }
      if (key.resistance) return `Gần Resistance (Kháng cự) ${Number(key.resistance).toFixed(2)}`;
      return 'Vùng pullback theo MA25/Resistance';
    }

    return 'Chưa có vùng vào lệnh hợp lệ';
  }

  buildCandleWaitText(signalData) {
    const side = signalData.side;
    const entryMin = Number(signalData.entryMin || 0);
    const entryMax = Number(signalData.entryMax || 0);

    if (side === 'LONG') {
      return [
        '- Chờ nến xác nhận tăng (bullish engulfing hoặc pinbar rút chân dưới)',
        `- Nến đóng trên vùng entry (${entryMin.toFixed(2)} - ${entryMax.toFixed(2)})`,
      ].join('\n');
    }

    if (side === 'SHORT') {
      return [
        '- Chờ nến xác nhận giảm (bearish engulfing hoặc pinbar rút râu trên)',
        `- Nến đóng dưới vùng entry (${entryMin.toFixed(2)} - ${entryMax.toFixed(2)})`,
      ].join('\n');
    }

    return '- Không có setup đạt chuẩn, tiếp tục quan sát';
  }

  normalizeReasonText(text) {
    return String(text || '')
      .replace(/\uFFFD/g, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  hasEncodingIssue(text) {
    const s = String(text || '');
    if (!s) return true;
    // Dấu hiệu thường gặp khi chuỗi bị lỗi mã hóa khi đọc từ nguồn upstream.
    return /[�]/.test(s) || /\?[a-zA-ZÀ-ỹ]/.test(s) || /\s\?\s/.test(s);
  }

  buildFallbackReasons(signalData, market = {}) {
    const side = String(signalData.side || '').toUpperCase();
    const trendRaw = String(signalData.trend5m || market.trend || 'N/A').toUpperCase();
    const trendVi =
      trendRaw === 'UP' ? 'tăng' : trendRaw === 'DOWN' ? 'giảm' : trendRaw === 'SIDEWAY' ? 'đi ngang' : trendRaw.toLowerCase();

    const ma25 = Number(market?.ma2m?.ma25 || 0);
    const r1 = Number(market?.supportResistance?.keyLevels?.nearestResistance || 0);
    const s1 = Number(market?.supportResistance?.keyLevels?.nearestSupport || 0);

    if (side === 'SHORT') {
      return [
        r1 > 0 ? `Giá hồi lên gần kháng cự (${r1.toFixed(r1 < 1 ? 4 : 2)})` : 'Giá hồi lên gần vùng kháng cự',
        ma25 > 0 ? `Giá bám quanh MA25 (${ma25.toFixed(ma25 < 1 ? 4 : 2)})` : 'Giá bám quanh MA25',
        `Xu hướng 5m đang ${trendVi}`,
      ];
    }

    if (side === 'LONG') {
      return [
        s1 > 0 ? `Giá hồi về gần hỗ trợ (${s1.toFixed(s1 < 1 ? 4 : 2)})` : 'Giá hồi về gần vùng hỗ trợ',
        ma25 > 0 ? `Giá bám quanh MA25 (${ma25.toFixed(ma25 < 1 ? 4 : 2)})` : 'Giá bám quanh MA25',
        `Xu hướng 5m đang ${trendVi}`,
      ];
    }

    return ['Setup đang chờ xác nhận thêm'];
  }

  async sendPreSignal(preData) {
    if (!this.config.sendSignal) return;
    if (!this.config.preSignalEnabled) return;

    const side = String(preData.side || '').toUpperCase();
    if (!['LONG', 'SHORT'].includes(side)) return;

    const icon = side === 'LONG' ? '🟢' : '🔴';
    const coinIcon = this.getSymbolIcon(preData.symbol);
    const trigger = Number(preData.triggerPrice || 0);
    const sl = Number(preData.stopLoss || 0);
    const tp1 = Number(preData.tp1 || 0);
    const tp2 = Number(preData.tp2 || 0);
    const tp3 = Number(preData.tp3 || 0);
    const probability = Number(preData.probability || 0);
    const etaSec = Number(preData.etaSec || 0);
    const etaMin = etaSec > 0 ? (etaSec / 60).toFixed(1) : 'N/A';
    const reasonsRaw = Array.isArray(preData.reasons) ? preData.reasons : [];
    const reasons = reasonsRaw
      .map((r) => this.normalizeReasonText(r))
      .filter((r) => r && !this.hasEncodingIssue(r))
      .slice(0, 3);
    const live = preData.liveStats || {};
    const lev = Number(live.leverage || 0);
    const keoDep =
      Number(preData.probability || 0) >= 85
        ? '🔥 CỰC ĐẸP'
        : Number(preData.probability || 0) >= 78
        ? '✅ ĐẸP'
        : '⚠️ THEO DÕI';
    const remainingCandles = Number(preData.remainingCandles || 0);

    const fmtPrice = (val) => {
      const n = Number(val);
      if (!Number.isFinite(n)) return 'N/A';
      return Math.abs(n) < 1 ? n.toFixed(4) : n.toFixed(2);
    };

    const text = [
      `🟡 <b>KÈO CHUẨN BỊ</b> | ${icon} <b>${side}</b> | ${coinIcon} <b>${esc(preData.symbol || 'N/A')}</b> | <b>${esc(preData.timeframe || 'N/A')}</b>`,
      ``,
      `⏳ Còn khoảng: <b>${remainingCandles || '?'} nến</b> (${etaMin} phút)`,
      `📍 Vùng trigger: <b>${fmtPrice(trigger)}</b>`,
      `🛑 SL dự kiến: <b>${fmtPrice(sl)}</b>`,
      `🎯 TP1/TP2/TP3: <b>${fmtPrice(tp1)} / ${fmtPrice(tp2)} / ${fmtPrice(tp3)}</b>`,
      Number.isFinite(lev) && lev > 0 ? `⚙️ Đòn bẩy dự kiến: <b>x${lev.toFixed(0)}</b>` : '',
      `📊 Xác suất hiện tại: <b>${probability}/100</b> (${keoDep})`,
      Number.isFinite(live.targetRoiNetPct?.tp1) ? `💰 TP1 ước tính: <b>${Number(live.targetRoiNetPct.tp1).toFixed(2)}%</b> sau phí` : '',
      Number.isFinite(live.stopLossRiskPct) ? `💸 SL ước tính: <b>-${Number(live.stopLossRiskPct).toFixed(2)}%</b>` : '',
      reasons.length ? `✅ Lý do: ${esc(reasons.join(' | '))}` : '',
    ].filter(Boolean).join('\n');

    const idem = `pre:${preData.symbol}:${preData.timeframe}:${side}:${Math.round(trigger * 10)}:${preData.candleTime || 0}`;
    await this.sendText(text, {
      idempotencyKey: idem,
      idempotencyTtlMs: Math.max(15000, Number(this.config.preSignalCooldownMs || 90000)),
      symbol: preData.symbol,
    });
  }

  async sendSignal(signalData, imagePath = null) {
    if (!this.config.sendSignal) return;
    const market = signalData.market || {};
    const pa = signalData.priceAction || market.priceAction || {};

    const fmtPrice = (val) => {
      const n = Number(val);
      if (!Number.isFinite(n)) return 'N/A';
      const abs = Math.abs(n);
      if (abs < 1) return n.toFixed(4);
      if (abs < 100) return n.toFixed(3);
      return n.toFixed(2);
    };

    const trendRaw = String(signalData.trend5m || market.trend || 'N/A').toUpperCase();
    const trendVi =
      trendRaw === 'UP' ? 'TĂNG' : trendRaw === 'DOWN' ? 'GIẢM' : trendRaw === 'SIDEWAY' ? 'ĐI NGANG' : trendRaw;

    const side = String(signalData.side || '').toUpperCase();
    const sideMeta =
      side === 'LONG'
        ? { titleIcon: '🟢', trendIcon: '📈', label: 'KÈO LONG' }
        : side === 'SHORT'
        ? { titleIcon: '🔴', trendIcon: '📉', label: 'KÈO SHORT' }
        : { titleIcon: '⚪', trendIcon: '📊', label: 'NO TRADE' };

    const entryMin = Number(signalData.entryMin || signalData.entryPrice || 0);
    const entryMax = Number(signalData.entryMax || signalData.entryPrice || 0);
    const leverage = Number(signalData.leverage || signalData.market?.leverage || 10);
    const entryPrice = Number(signalData.entryPrice || 0);
    const tp1Price = Number(signalData.tp1 || 0);
    const slPrice = Number(signalData.stopLoss || 0);
    const feePct = Number(signalData.takerFeePct || 0.0004) * 2 * leverage * 100;
    const grossTp1Pct =
      entryPrice > 0 && tp1Price > 0 ? (Math.abs(tp1Price - entryPrice) / entryPrice) * leverage * 100 : 0;
    const grossSlPct =
      entryPrice > 0 && slPrice > 0 ? (Math.abs(slPrice - entryPrice) / entryPrice) * leverage * 100 : 0;
    const netTp1Roi = grossTp1Pct - feePct;

    const reasonsRaw = Array.isArray(signalData.reasons) ? signalData.reasons : [];
    const reasonsClean = reasonsRaw
      .map((r) => this.normalizeReasonText(r))
      .filter((r) => r && !this.hasEncodingIssue(r));
    const reasons = (reasonsClean.length ? reasonsClean : this.buildFallbackReasons(signalData, market)).slice(0, 3);
    const reasonIcons = ['🎯', '📐', '🧭', '🕯️', '📦'];
    const reasonLines = reasons.length
      ? reasons.map((r, idx) => `- ${reasonIcons[idx % reasonIcons.length]} ${esc(r)}`)
      : ['- 🎯 Setup đạt chuẩn theo điều kiện hệ thống'];

    const rejectCodes = Array.isArray(signalData.rejectCodes) ? signalData.rejectCodes : [];
    const weaknesses = [];
    const volumeText = String(signalData.volumeStatus || '').toUpperCase();
    if (volumeText.includes('THẤP') || volumeText.includes('LOW')) weaknesses.push('Volume thấp');
    if (rejectCodes.includes('LOW_VOLUME')) weaknesses.push('Thanh khoản yếu');
    if (rejectCodes.includes('WEAK_ORDERFLOW')) weaknesses.push('Lực thị trường chưa thật mạnh');
    if (rejectCodes.includes('HIGH_SPREAD')) weaknesses.push('Spread hơi cao');
    if (side === 'LONG' && Number(signalData.buyPressure || 0) < Number(signalData.sellPressure || 0)) {
      weaknesses.push('Lực mua chưa áp đảo');
    }
    if (side === 'SHORT' && Number(signalData.sellPressure || 0) < Number(signalData.buyPressure || 0)) {
      weaknesses.push('Lực bán chưa áp đảo');
    }
    const weakIcons = ['⚠️', '🌊', '🧪', '🛡️'];
    const weaknessLines = weaknesses.length
      ? weaknesses.slice(0, 3).map((w, idx) => `- ${weakIcons[idx % weakIcons.length]} ${esc(w)}`)
      : ['- ✅ Chưa thấy điểm yếu lớn'];

    const coinIcon = this.getSymbolIcon(signalData.symbol);
    const timeframe = esc(signalData.signalTimeframe || '3m');
    const text = [
      `${sideMeta.titleIcon} <b>${sideMeta.label}</b> | ${coinIcon} <b>${esc(signalData.symbol)}</b> | <b>${timeframe}</b>`,
      ``,
      `📍 <b>Vùng vào:</b> ${fmtPrice(entryMin)} - ${fmtPrice(entryMax)}`,
      `🛑 <b>Cắt lỗ:</b> ${fmtPrice(signalData.stopLoss)}`,
      `🎯 <b>TP1:</b> ${fmtPrice(signalData.tp1)}`,
      `🎯 <b>TP2:</b> ${fmtPrice(signalData.tp2)}`,
      `🎯 <b>TP3:</b> ${fmtPrice(signalData.tp3)}`,
      ``,
      `⚙️ <b>Đòn bẩy:</b> x${Number.isFinite(leverage) && leverage > 0 ? leverage.toFixed(0) : '10'}`,
      `📊 <b>Confidence:</b> ${Number(signalData.confidence || 0)}/100`,
      `${sideMeta.trendIcon} <b>Trend 5m:</b> ${esc(trendVi)}`,
      ``,
      `✅ <b>Lý do vào:</b>`,
      ...reasonLines,
      ``,
      `⚠️ <b>Điểm yếu:</b>`,
      ...weaknessLines,
      ``,
      Number.isFinite(netTp1Roi) ? `💰 <b>TP1 ước tính:</b> +${netTp1Roi.toFixed(2)}% sau phí` : '',
      Number.isFinite(grossSlPct) ? `💸 <b>SL ước tính:</b> -${grossSlPct.toFixed(2)}%` : '',
    ]
      .filter(Boolean)
      .join('\n');

    const idem = `signal:${signalData.symbol}:${signalData.side}:${Math.round((signalData.entryPrice || 0) * 10)}`;

    if (imagePath && this.config.sendChartImage) {
      const ok = await this.sendPhoto(imagePath, text, {
        idempotencyKey: `${idem}:photo`,
        symbol: signalData.symbol,
      });
      if (!ok) await this.sendText(text, { idempotencyKey: `${idem}:text`, symbol: signalData.symbol });
      return;
    }

    await this.sendText(text, { idempotencyKey: `${idem}:text`, symbol: signalData.symbol });
  }

  async sendTradeOpened(orderData) {
    if (!this.config.sendOrderOpen) return;
    const coinIcon = this.getSymbolIcon(orderData.symbol);

    await this.sendText(
      [
        `🚀 <b>ĐÃ VÀO LỆNH</b>`,
        `Coin: ${coinIcon} <b>${esc(orderData.symbol)}</b>`,
        `Loại: <b>${esc(orderData.side)}</b>`,
        `Giá vào: <b>${Number(orderData.entryPrice || 0).toFixed(2)}</b>`,
        `Khối lượng: <b>${Number(orderData.size || 0).toFixed(4)}</b>`,
        `Đòn bẩy: x${esc(orderData.leverage)}`,
        `Lý do: ${esc(orderData.reason || 'N/A')}`,
      ].join('\n'),
      {
        idempotencyKey: `open:${orderData.symbol}:${orderData.side}:${Math.round((orderData.entryPrice || 0) * 10)}`,
        symbol: orderData.symbol,
      },
    );
  }

  async sendTakeProfit(tpData) {
    if (!this.config.sendOrderClose) return;
    const coinIcon = this.getSymbolIcon(tpData.symbol);

    await this.sendText(
      [
        `💰 <b>CHỐT LỜI ${esc(tpData.tp)}</b>`,
        `Coin: ${coinIcon} <b>${esc(tpData.symbol)}</b>`,
        `Loại: ${esc(tpData.side)}`,
        `PnL: <b>${Number(tpData.pnl || 0).toFixed(2)} USDT</b> (${Number(tpData.pnlPct || 0).toFixed(2)}%)`,
        `Trạng thái: ${esc(tpData.remain || '')}`,
        `Trailing: ${tpData.trailing ? 'ĐANG BẬT' : 'CHƯA BẬT'}`,
      ].join('\n'),
      {
        idempotencyKey: `tp:${tpData.symbol}:${tpData.tp}:${Math.round(Date.now() / 4000)}`,
        idempotencyTtlMs: 12000,
        symbol: tpData.symbol,
      },
    );
  }

  async sendStopLoss(slData) {
    if (!this.config.sendOrderClose) return;
    const coinIcon = this.getSymbolIcon(slData.symbol);

    await this.sendText(
      [
        `❌ <b>STOP LOSS</b>`,
        `Coin: ${coinIcon} <b>${esc(slData.symbol)}</b>`,
        `Loại: ${esc(slData.side)}`,
        `PnL: <b>${Number(slData.pnl || 0).toFixed(2)} USDT</b> (${Number(slData.pnlPct || 0).toFixed(2)}%)`,
        `Lý do: ${esc(slData.reason || '')}`,
        `Cooldown: ${esc(slData.cooldownSec || 0)}s`,
      ].join('\n'),
      {
        idempotencyKey: `sl:${slData.symbol}:${Math.round(Date.now() / 4000)}`,
        idempotencyTtlMs: 12000,
        symbol: slData.symbol,
      },
    );
  }

  async sendPositionHeartbeat(heartbeat) {
    if (!this.config.sendOrderOpen) return;
    if (!this.config.positionHeartbeatEnabled) return;

    const side = String(heartbeat.side || '').toUpperCase();
    const icon = side === 'LONG' ? '🟢' : '🔴';
    const coinIcon = this.getSymbolIcon(heartbeat.symbol);
    const prob = Number(heartbeat.probability || 0);
    const rawPnl = Number(heartbeat.rawPnlPct || 0);
    const levPnl = Number(heartbeat.leveragedPnlPct || 0);
    const feePenalty = Number(heartbeat.feePenaltyPct || 0);
    const netRoi = Number(heartbeat.netRoiPct || 0);
    const direction = String(heartbeat.direction || 'FLAT').toUpperCase();
    const dirIcon = direction === 'UP' ? '⬆️' : direction === 'DOWN' ? '⬇️' : '⏸️';
    const pnlIcon = levPnl >= 0 ? '📈' : '📉';
    const cooldownMs = Math.max(5000, Number(this.config.positionHeartbeatMs || 5000));

    const text = [
      `🛡️ <b>GIÁM SÁT LỆNH 5S</b> ${icon} <b>${esc(side)}</b>`,
      `Coin: ${coinIcon} <b>${esc(heartbeat.symbol || 'N/A')}</b>`,
      `Giá hiện tại: <b>${Number(heartbeat.currentPrice || 0).toFixed(2)}</b> ${dirIcon} ${direction}`,
      `Entry: <b>${Number(heartbeat.entryPrice || 0).toFixed(2)}</b>`,
      `SL/TP: <b>${Number(heartbeat.stopLoss || 0).toFixed(2)} | ${Number(heartbeat.tp1 || 0).toFixed(2)} / ${Number(heartbeat.tp2 || 0).toFixed(2)} / ${Number(heartbeat.tp3 || 0).toFixed(2)}</b>`,
      `Đòn bẩy: <b>x${Number(heartbeat.leverage || 0).toFixed(0)}</b>`,
      `${pnlIcon} PnL hiện tại: <b>${rawPnl >= 0 ? '+' : ''}${rawPnl.toFixed(3)}%</b> (x lev: <b>${levPnl >= 0 ? '+' : ''}${levPnl.toFixed(2)}%</b>)`,
      `ROI ròng sau phí: <b>${netRoi >= 0 ? '+' : ''}${netRoi.toFixed(2)}%</b> (phí ~${feePenalty.toFixed(2)}%)`,
      `Xác suất an toàn realtime: <b>${prob.toFixed(1)}%</b>`,
      `Trailing: ${heartbeat.trailingEnabled ? 'ĐANG BẬT' : 'CHƯA BẬT'} | Đã chốt: ${Number(heartbeat.partialClosedPct || 0).toFixed(0)}%`,
    ].join('\n');

    await this.sendText(text, {
      idempotencyKey: `pulse:${heartbeat.symbol}:${side}:${Math.floor(Date.now() / cooldownMs)}`,
      idempotencyTtlMs: Math.max(1000, cooldownMs - 300),
      cooldownMs,
      symbol: heartbeat.symbol,
    });
  }

  async sendReject(reasonData) {
    if (!this.config.sendRejectReason) return;
    const codeExplain = {
      NEAR_STRONG_RESISTANCE: 'Gần R-zone EXTREME, không LONG',
      NEAR_STRONG_SUPPORT: 'Gần S-zone EXTREME, không SHORT',
      EXTENDED_FROM_MA25: 'Giá đã đi quá xa MA25',
      LATE_ENTRY: 'Entry đang ở cuối sóng, rủi ro đuổi giá',
      LOW_REWARD_DISTANCE: 'Khoảng reward tới TP quá ngắn',
      FOMO_BREAKOUT: 'Vừa breakout/breakdown, chưa pullback đẹp',
      BOS_AGAINST_TREND: 'BOS ngược trend, không vào lệnh',
      DEEP_PULLBACK: 'Pullback còn sâu, chưa đẹp để vào ngay',
    };
    const firstCode = reasonData.codes?.[0] || '';
    const quickReason = codeExplain[firstCode] || '';

    await this.sendText(
      [
        `⚠️ <b>TỪ CHỐI LỆNH</b>`,
        `Coin: <b>${esc(reasonData.symbol)}</b>`,
        `Loại: ${esc(reasonData.side)}`,
        `Confidence: ${Number(reasonData.confidence || 0)}`,
        reasonData.codes?.length ? `Codes: ${esc(reasonData.codes.join(', '))}` : '',
        quickReason ? `Tóm tắt: ${esc(quickReason)}` : '',
        `Lý do:`,
        ...(reasonData.reasons || []).map((r) => `- ${esc(r)}`),
      ]
        .filter(Boolean)
        .join('\n'),
      {
        idempotencyKey: `reject:${reasonData.symbol}:${reasonData.codes?.[0] || 'NONE'}:${Math.round(Date.now() / 10000)}`,
        idempotencyTtlMs: 25000,
        symbol: reasonData.symbol,
      },
    );
  }

  async sendError(errorData) {
    if (!this.config.sendErrors) return;

    await this.sendText(
      [
        `🚨 <b>LỖI BOT</b>`,
        `${esc(errorData.title || 'Lỗi hệ thống')}`,
        `${esc(errorData.detail || 'Không có chi tiết')}`,
      ].join('\n'),
      {
        idempotencyKey: `error:${esc(errorData.title || 'ERR')}:${Math.round(Date.now() / 10000)}`,
        idempotencyTtlMs: 18000,
        symbol: errorData.symbol,
        broadcast: !errorData.symbol,
      },
    );
  }

  async sendDailyReport(reportData) {
    if (!this.config.sendDailyReport) return;
    const topReject =
      Object.entries(reportData.rejectedByCode || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || 'N/A';

    await this.sendText(
      [
        `📘 <b>BÁO CÁO NGÀY</b>`,
        `Lệnh hôm nay: ${Number(reportData.totalTrades || 0)}`,
        `Winrate: ${Number(reportData.winRate || 0).toFixed(2)}%`,
        `PnL hôm nay: ${Number(reportData.pnlToday || 0).toFixed(2)} USDT`,
        `Giờ tốt nhất: ${esc(reportData.bestHour || 'N/A')}`,
        `Lý do thua nhiều: ${esc(reportData.topLossReason || 'N/A')}`,
        `Lệnh bị chặn: ${Number(reportData.rejectedTrades || 0)}`,
        `Reject code nhiều nhất: ${esc(topReject)}`,
      ].join('\n'),
      {
        idempotencyKey: `daily:${new Date().toISOString().slice(0, 10)}`,
        idempotencyTtlMs: 3600000,
        broadcast: true,
      },
    );
  }

  async sendWeeklyReport(reportData) {
    if (!this.config.sendWeeklyReport) return;
    const topReject =
      Object.entries(reportData.rejectedByCode || {}).sort((a, b) => b[1] - a[1])[0]?.[0] || 'N/A';

    await this.sendText(
      [
        `📗 <b>BÁO CÁO TUẦN</b>`,
        `Tổng lệnh: ${Number(reportData.totalTrades || 0)}`,
        `Winrate: ${Number(reportData.winRate || 0).toFixed(2)}%`,
        `PnL tuần: ${Number(reportData.pnlWeek || 0).toFixed(2)} USDT`,
        `Coin tốt nhất: ${esc(reportData.bestSymbol || 'N/A')}`,
        `Coin kém nhất: ${esc(reportData.worstSymbol || 'N/A')}`,
        `Setup tốt nhất: ${esc(reportData.bestSetup || 'N/A')}`,
        `Khung giờ tốt: ${esc(reportData.bestHour || 'N/A')}`,
        `Risk block: ${Number(reportData.riskBlocks || 0)}`,
        `Reject code nhiều nhất: ${esc(topReject)}`,
      ].join('\n'),
      {
        idempotencyKey: `weekly:${new Date().toISOString().slice(0, 10)}`,
        idempotencyTtlMs: 3600000,
        broadcast: true,
      },
    );
  }
}

module.exports = TelegramService;
