const axios = require('axios');

class SelfPingService {
  constructor(config, logger) {
    this.logger = logger;
    this.enabled = Boolean(config?.selfPing?.enabled);
    this.intervalMs = Number(config?.selfPing?.intervalMs) || 240000;
    this.retryMs = Number(config?.selfPing?.retryMs) || 5000;
    this.timeoutMs = Number(config?.selfPing?.timeoutMs) || 4000;
    this.url = String(config?.selfPing?.url || '').trim();
    this.timer = null;
    this.inFlight = false;
    this.stopped = false;
    this.failCount = 0;
    this.okCount = 0;
    this.lastOkAt = null;
  }

  start() {
    if (!this.enabled) return;
    if (!this.url) {
      this.logger.warn('self-ping', 'SELF_PING_ENABLED=true nhưng chưa có URL ping, bỏ qua self-ping');
      return;
    }
    this.stopped = false;
    this.logger.info('self-ping', 'Bật self-ping', {
      url: this.url,
      intervalMs: this.intervalMs,
      retryMs: this.retryMs,
      timeoutMs: this.timeoutMs,
    });
    this.#schedule(0);
  }

  stop() {
    this.stopped = true;
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = null;
    }
    this.logger.info('self-ping', 'Đã tắt self-ping', {
      okCount: this.okCount,
      failCount: this.failCount,
      lastOkAt: this.lastOkAt,
    });
  }

  #schedule(delayMs) {
    if (this.stopped) return;
    if (this.timer) clearTimeout(this.timer);
    this.timer = setTimeout(() => {
      this.#run().catch(() => {
        // handled in #run
      });
    }, Math.max(0, delayMs));
  }

  async #run() {
    if (this.stopped || this.inFlight) return;
    this.inFlight = true;
    const startedAt = Date.now();
    try {
      const resp = await axios.get(this.url, {
        timeout: this.timeoutMs,
        validateStatus: () => true,
        headers: {
          'x-self-ping': '1',
        },
      });
      if (resp.status >= 200 && resp.status < 300) {
        this.okCount += 1;
        this.lastOkAt = new Date().toISOString();
        this.logger.info('self-ping', 'Ping thành công', {
          status: resp.status,
          latencyMs: Date.now() - startedAt,
          okCount: this.okCount,
        });
        this.#schedule(this.intervalMs);
      } else {
        this.failCount += 1;
        this.logger.warn('self-ping', 'Ping lỗi HTTP, sẽ retry sau 5s', {
          status: resp.status,
          latencyMs: Date.now() - startedAt,
          failCount: this.failCount,
        });
        this.#schedule(this.retryMs);
      }
    } catch (error) {
      this.failCount += 1;
      this.logger.warn('self-ping', 'Ping thất bại, sẽ retry sau 5s', {
        error: error.message,
        latencyMs: Date.now() - startedAt,
        failCount: this.failCount,
      });
      this.#schedule(this.retryMs);
    } finally {
      this.inFlight = false;
    }
  }
}

module.exports = SelfPingService;
