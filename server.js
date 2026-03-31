const express = require('express');
const path = require('path');
const cors = require('cors');

const config = require('./config');
const { validateStartupConfig } = require('./configValidator');
const Logger = require('./logger');
const { StateStore } = require('./stateStore');
const PersistenceService = require('./persistenceService');
const RiskManager = require('./riskManager');
const OrderManager = require('./orderManager');
const WebsocketManager = require('./websocketManager');
const TelegramService = require('./telegramService');
const ChartSnapshot = require('./chartSnapshot');
const StatsService = require('./statsService');
const ReportService = require('./reportService');
const SelfPingService = require('./selfPingService');
const BotEngine = require('./botEngine');
const createApi = require('./api');
const glossary = require('./glossary');

async function bootstrap() {
  const startupValidation = validateStartupConfig(config);
  if (!startupValidation.ok) {
    throw new Error(
      `Cấu hình khởi động không hợp lệ:\\n- ${startupValidation.errors.join('\\n- ')}`,
    );
  }

  const logger = new Logger(config);
  const stateStore = new StateStore(config, logger);
  const persistenceService = new PersistenceService(config, logger);

  const saved = persistenceService.load();
  if (saved?.state) {
    const hydrated = stateStore.hydrate(saved.state);
    if (hydrated) {
      logger.info('persistence', 'Đã nạp trạng thái từ snapshot', {
        savedAt: saved.savedAt,
        path: config.persistence.path,
      });
    }
  }

  const telegramService = new TelegramService(config, logger);
  const websocketManager = new WebsocketManager(config, logger, stateStore);
  const riskManager = new RiskManager(config, stateStore, logger);
  const orderManager = new OrderManager(config, stateStore, logger, riskManager, telegramService);
  const chartSnapshot = new ChartSnapshot(config, logger);
  const statsService = new StatsService(stateStore);

  const botEngine = new BotEngine({
    config,
    logger,
    stateStore,
    websocketManager,
    riskManager,
    orderManager,
    telegramService,
    chartSnapshot,
  });

  const reportService = new ReportService(config, logger, statsService, telegramService, stateStore);
  const selfPingService = new SelfPingService(config, logger);

  await botEngine.initialize();
  reportService.start();
  persistenceService.startAutoSave(() => stateStore.state);

  const app = express();
  app.use(cors());
  app.use(express.json({ limit: '1mb' }));

  app.use('/api', createApi({ botEngine, logger, statsService, glossary, stateStore, config }));

  app.use(express.static(path.resolve(process.cwd(), 'frontend')));

  app.get('*', (req, res) => {
    res.sendFile(path.resolve(process.cwd(), 'frontend', 'index.html'));
  });

  const server = app.listen(config.app.port, () => {
    logger.info('server', `${config.app.name} đang chạy tại cổng ${config.app.port}`, {
      mode: config.mode,
      symbol: config.binance.symbol,
      testnet: config.binance.useTestnet,
    });
    selfPingService.start();
  });

  server.on('error', (error) => {
    logger.error('server', 'Lỗi khởi động HTTP server', { error: error.message });
    process.exit(1);
  });

  const shutdown = async () => {
    logger.warn('server', 'Đang tắt bot...');
    reportService.stop();
    selfPingService.stop();
    persistenceService.stopAutoSave();
    persistenceService.save(stateStore.state);
    websocketManager.disconnect();
    process.exit(0);
  };

  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

bootstrap().catch((error) => {
  // eslint-disable-next-line no-console
  console.error('Fatal startup error:', error);
  process.exit(1);
});
