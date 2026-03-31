const path = require('path');
const fs = require('fs');

class ChartSnapshot {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
    this.lastCaptureByKey = new Map();
    this.snapshotDir = path.resolve(process.cwd(), 'snapshots');
  }

  async capture(signal, snapshotType = 'signal') {
    if (!this.config.chartSnapshot.enabled) return null;
    const key = `${signal.symbol}:${snapshotType}`;
    const lastAt = this.lastCaptureByKey.get(key) || 0;
    if (Date.now() - lastAt < this.config.chartSnapshot.cooldownMs) {
      return null;
    }

    let chromium;
    try {
      ({ chromium } = require('playwright'));
    } catch (error) {
      this.logger.warn('snapshot', 'Playwright chưa sẵn sàng, bỏ qua chụp ảnh', {
        error: error.message,
      });
      return null;
    }

    if (!fs.existsSync(this.snapshotDir)) fs.mkdirSync(this.snapshotDir, { recursive: true });
    this.cleanupOldSnapshots();

    const filename = `${snapshotType}-${signal.symbol}-${signal.side}-${Date.now()}.png`;
    const outPath = path.resolve(this.snapshotDir, filename);

    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });

      const query = new URLSearchParams({
        snapshot: '1',
        symbol: signal.symbol,
        focusSignal: '1',
        side: signal.side || 'NO_TRADE',
        entry: String(signal.entryPrice || 0),
        sl: String(signal.stopLoss || 0),
        tp1: String(signal.tp1 || 0),
        tp2: String(signal.tp2 || 0),
        tp3: String(signal.tp3 || 0),
      });
      const url = `${this.config.chartSnapshot.baseUrl}/?${query.toString()}`;

      await page.goto(url, { waitUntil: 'domcontentloaded', timeout: this.config.chartSnapshot.timeoutMs });
      await page.waitForFunction(() => window.__chartReady === true, {
        timeout: this.config.chartSnapshot.timeoutMs,
      }).catch(() => null);

      const chart = await page.$('#chart-card');
      if (!chart) {
        throw new Error('Không tìm thấy vùng chart để chụp');
      }

      await chart.screenshot({ path: outPath });
      this.lastCaptureByKey.set(key, Date.now());
      this.logger.info('snapshot', 'Đã chụp chart snapshot', { outPath });
      return fs.existsSync(outPath) ? outPath : null;
    } catch (error) {
      this.logger.error('snapshot', 'Chụp ảnh chart lỗi', { error: error.message });
      return null;
    } finally {
      await browser.close();
    }
  }

  cleanupOldSnapshots() {
    try {
      if (!fs.existsSync(this.snapshotDir)) return;
      const files = fs.readdirSync(this.snapshotDir);
      const maxAgeMs = Math.max(this.config.chartSnapshot.keepHours || 48, 1) * 60 * 60 * 1000;
      const now = Date.now();
      for (const file of files) {
        const filePath = path.join(this.snapshotDir, file);
        const stat = fs.statSync(filePath);
        if (now - stat.mtimeMs > maxAgeMs) {
          fs.unlinkSync(filePath);
        }
      }
    } catch (error) {
      this.logger.warn('snapshot', 'Không cleanup được snapshot cũ', { error: error.message });
    }
  }
}

module.exports = ChartSnapshot;
