const axios = require('axios');

class SelfPingService {
  constructor(config, logger) {
    this.logger = logger;
    this.enabled = Boolean(config?.selfPing?.enabled);
    this.intervalMs = Number(config?.selfPing?.intervalMs) || 240000;
    this.retryMs = Number(config?.selfPing?.retryMs) || 5000;
    this.timeoutMs = Number(config?.selfPing?.timeoutMs) || 4000;
    this.preferLocal = config?.selfPing?.preferLocal !== false;
    this.url = String(config?.selfPing?.url || '').trim();
    this.localFallbackUrl = `http://127.0.0.1:${Number(config?.app?.port) || 3000}/api/health`;
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
      this.logger.warn('self-ping', 'SELF_PING_ENABLED=true but ping URL is empty, skip self-ping');
      return;
    }
    this.stopped = false;
    this.logger.info('self-ping', 'Self-ping started', {
      url: this.url,
      localUrl: this.localFallbackUrl,
      preferLocal: this.preferLocal,
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
    this.logger.info('self-ping', 'Self-ping stopped', {
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
      const firstUrl = this.preferLocal ? this.localFallbackUrl : this.url;
      const secondUrl = this.preferLocal ? this.url : this.localFallbackUrl;

      const primary = await this.#request(firstUrl);
      if (primary.ok) {
        this.okCount += 1;
        this.lastOkAt = new Date().toISOString();
        this.logger.info('self-ping', 'Ping success', {
          status: primary.status,
          latencyMs: primary.latencyMs,
          via: this.preferLocal ? 'local' : 'public',
          okCount: this.okCount,
        });
        this.#schedule(this.intervalMs);
      } else {
        const fallback = await this.#request(secondUrl);
        if (fallback.ok) {
          this.okCount += 1;
          this.lastOkAt = new Date().toISOString();
          this.logger.info('self-ping', 'Ping success via local fallback', {
            primaryStatus: primary.status,
            fallbackStatus: fallback.status,
            fallbackLatencyMs: fallback.latencyMs,
            via: this.preferLocal ? 'public-fallback' : 'local-fallback',
            okCount: this.okCount,
          });
          this.#schedule(this.intervalMs);
        } else {
          this.failCount += 1;
          this.logger.warn('self-ping', 'Ping HTTP error, retry in 5s', {
            primaryStatus: primary.status,
            primaryUrl: firstUrl,
            fallbackStatus: fallback.status,
            fallbackUrl: secondUrl,
            failCount: this.failCount,
            totalLatencyMs: Date.now() - startedAt,
          });
          this.#schedule(this.retryMs);
        }
      }
    } catch (error) {
      this.failCount += 1;
      this.logger.warn('self-ping', 'Ping failed, retry in 5s', {
        error: error.message,
        latencyMs: Date.now() - startedAt,
        failCount: this.failCount,
        url: this.url,
      });
      this.#schedule(this.retryMs);
    } finally {
      this.inFlight = false;
    }
  }

  async #request(url) {
    try {
      const startedAt = Date.now();
      const resp = await axios.get(url, {
        timeout: this.timeoutMs,
        validateStatus: () => true,
        maxRedirects: 5,
        headers: { 'x-self-ping': '1' },
      });
      return {
        ok: resp.status >= 200 && resp.status < 400,
        status: resp.status,
        latencyMs: Date.now() - startedAt,
      };
    } catch (error) {
      return {
        ok: false,
        status: 0,
        latencyMs: 0,
        error: error.message,
      };
    }
  }
}

module.exports = SelfPingService;
