const fs = require('fs');
const path = require('path');

class PersistenceService {
  constructor(config, logger) {
    this.config = config.persistence;
    this.logger = logger;
    this.timer = null;
    this.saving = false;
    this.filePath = path.resolve(process.cwd(), this.config.path);

    const dir = path.dirname(this.filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }
  }

  isEnabled() {
    return Boolean(this.config.enabled);
  }

  buildSnapshot(state) {
    const candles = {};
    for (const [symbol, tfMap] of Object.entries(state.candles || {})) {
      candles[symbol] = {};
      for (const [tf, list] of Object.entries(tfMap || {})) {
        candles[symbol][tf] = Array.isArray(list) ? list.slice(-420) : [];
      }
    }

    return {
      version: 1,
      savedAt: Date.now(),
      state: {
        botRunning: false,
        mode: state.mode,
        stateMachine: state.stateMachine,
        symbol: state.symbol,
        selectedTimeframe: state.selectedTimeframe,
        lastSignal: state.lastSignal,
        lastReject: state.lastReject,
        reconnectCount: state.reconnectCount,
        pauseReason: state.pauseReason,
        startedAt: state.startedAt,
        lastError: state.lastError,
        cooldownUntil: state.cooldownUntil,
        manualPendingSignal: state.manualPendingSignal,
        activePosition: state.activePosition,
        pendingOrderId: null,
        signals: Array.isArray(state.signals) ? state.signals.slice(0, 120) : [],
        market: state.market,
        risk: state.risk,
        stats: state.stats,
        candles,
        orderBook: state.orderBook,
      },
    };
  }

  save(state) {
    if (!this.isEnabled()) return;
    if (this.saving) return;

    this.saving = true;
    try {
      const snapshot = this.buildSnapshot(state);
      const tmpPath = `${this.filePath}.tmp`;
      fs.writeFileSync(tmpPath, JSON.stringify(snapshot, null, 2), 'utf8');
      fs.renameSync(tmpPath, this.filePath);
    } catch (error) {
      this.logger.error('persistence', 'L?i luu snapshot', { error: error.message });
    } finally {
      this.saving = false;
    }
  }

  load() {
    if (!this.isEnabled()) return null;
    if (!fs.existsSync(this.filePath)) return null;

    try {
      const raw = fs.readFileSync(this.filePath, 'utf8');
      const parsed = JSON.parse(raw);
      if (!parsed || !parsed.state) return null;
      return parsed;
    } catch (error) {
      this.logger.error('persistence', 'L?i d?c snapshot', { error: error.message });
      return null;
    }
  }

  startAutoSave(getState) {
    if (!this.isEnabled()) return;
    this.stopAutoSave();
    this.timer = setInterval(() => {
      try {
        this.save(getState());
      } catch (error) {
        this.logger.error('persistence', 'L?i autosave state', { error: error.message });
      }
    }, this.config.autosaveMs);
  }

  stopAutoSave() {
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
  }
}

module.exports = PersistenceService;

