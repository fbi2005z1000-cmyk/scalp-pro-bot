require('dotenv').config();

const toNumber = (value, fallback) => {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
};

const parseSymbolList = (symbols, fallback = []) => {
  const src = symbols || fallback.join(',');
  return Array.from(
    new Set(
      String(src)
        .split(',')
        .map((s) => s.trim().toUpperCase())
        .filter(Boolean),
    ),
  );
};

const parseSymbols = (symbols) => {
  if (!symbols) {
    return [
      'BTCUSDT',
      'ETHUSDT',
      'SOLUSDT',
      'BNBUSDT',
      'XRPUSDT',
      'DOGEUSDT',
      'BCHUSDT',
      '1000PEPEUSDT',
      'UNIUSDT',
      'ADAUSDT',
      'TRXUSDT',
      'LINKUSDT',
      'POLUSDT',
      'AVAXUSDT',
      'LTCUSDT',
      'DOTUSDT',
      'ATOMUSDT',
      'APTUSDT',
      'NEARUSDT',
      'FILUSDT',
      'ETCUSDT',
      'SUIUSDT',
      'SEIUSDT',
      'ARBUSDT',
      'OPUSDT',
    ];
  }
  return parseSymbolList(symbols);
};

const configuredSymbols = parseSymbols(process.env.SYMBOLS);
const resolvedPort = toNumber(process.env.PORT, 3000);

const normalizeUrl = (value) => {
  const raw = String(value || '').trim();
  if (!raw) return '';
  if (raw.startsWith('http://') || raw.startsWith('https://')) return raw;
  return `https://${raw}`;
};

const deriveSelfPingBaseUrl = () => {
  const explicit = normalizeUrl(process.env.PUBLIC_BASE_URL || '');
  if (explicit) return explicit;

  const renderExternal = normalizeUrl(process.env.RENDER_EXTERNAL_URL || '');
  if (renderExternal) return renderExternal;

  const railwayStatic = normalizeUrl(process.env.RAILWAY_STATIC_URL || process.env.RAILWAY_PUBLIC_DOMAIN || '');
  if (railwayStatic) return railwayStatic;

  return `http://127.0.0.1:${resolvedPort}`;
};

const explicitSelfPingUrl = normalizeUrl(process.env.SELF_PING_URL || '');
const selfPingBaseUrl = deriveSelfPingBaseUrl();
const selfPingPath = String(process.env.SELF_PING_PATH || '/api/health').trim() || '/api/health';
const selfPingUrl =
  explicitSelfPingUrl ||
  `${selfPingBaseUrl.replace(/\/+$/, '')}${selfPingPath.startsWith('/') ? '' : '/'}${selfPingPath}`;

const config = {
  app: {
    name: process.env.APP_NAME || 'Scalp Pro Bot',
    port: resolvedPort,
    env: process.env.NODE_ENV || 'development',
  },
  selfPing: {
    enabled: process.env.SELF_PING_ENABLED === 'true',
    intervalMs: toNumber(process.env.SELF_PING_INTERVAL_MS, 240000),
    retryMs: toNumber(process.env.SELF_PING_RETRY_MS, 5000),
    timeoutMs: toNumber(process.env.SELF_PING_TIMEOUT_MS, 4000),
    url: selfPingUrl,
  },
  security: {
    adminToken: process.env.ADMIN_TOKEN || '',
    requireAdminForWrite: process.env.REQUIRE_ADMIN_FOR_WRITE === 'true',
    rateLimitWindowMs: toNumber(process.env.RATE_LIMIT_WINDOW_MS, 60000),
    rateLimitMax: toNumber(process.env.RATE_LIMIT_MAX, 180),
  },
  binance: {
    useTestnet: process.env.BINANCE_USE_TESTNET === 'true',
    apiKey: process.env.BINANCE_API_KEY || '',
    apiSecret: process.env.BINANCE_API_SECRET || '',
    restBaseUrl:
      process.env.BINANCE_REST_BASE_URL ||
      (process.env.BINANCE_USE_TESTNET === 'true'
        ? 'https://testnet.binancefuture.com'
        : 'https://fapi.binance.com'),
    wsBaseUrl:
      process.env.BINANCE_WS_BASE_URL ||
      (process.env.BINANCE_USE_TESTNET === 'true'
        ? 'wss://stream.binancefuture.com/stream'
        : 'wss://fstream.binance.com/stream'),
    symbol: (process.env.DEFAULT_SYMBOL || 'BTCUSDT').toUpperCase(),
    symbols: configuredSymbols,
    leverage: toNumber(process.env.DEFAULT_LEVERAGE, 10),
    recvWindow: toNumber(process.env.BINANCE_RECV_WINDOW, 5000),
    restSoftLimitPerMin: toNumber(process.env.BINANCE_REST_SOFT_LIMIT_PER_MIN, 180),
    restBlock429Ms: toNumber(process.env.BINANCE_REST_BLOCK_429_MS, 45000),
    restBlock418Ms: toNumber(process.env.BINANCE_REST_BLOCK_418_MS, 600000),
    restClosedSyncEnabled: process.env.REST_CLOSED_SYNC_ENABLED !== 'false',
    restClosedSyncMinGapMs: toNumber(process.env.REST_CLOSED_SYNC_MIN_GAP_MS, 12000),
    scannerEnabled: process.env.MULTI_SCANNER_ENABLED !== 'false',
    scannerLoopMs: toNumber(process.env.MULTI_SCANNER_LOOP_MS, 8000),
    scannerBatchSize: toNumber(process.env.MULTI_SCANNER_BATCH_SIZE, Math.max(1, configuredSymbols.length)),
    scannerCandleLimit: toNumber(process.env.MULTI_SCANNER_CANDLE_LIMIT, 260),
    scannerBootstrapBatchSize: toNumber(process.env.MULTI_SCANNER_BOOTSTRAP_BATCH_SIZE, 3),
    scannerBootstrapDelayMs: toNumber(process.env.MULTI_SCANNER_BOOTSTRAP_DELAY_MS, 180),
    directBotWatchdogEnabled: process.env.DIRECT_BOT_WATCHDOG_ENABLED !== 'false',
    directBotWatchdogMs: toNumber(process.env.DIRECT_BOT_WATCHDOG_MS, 15000),
    directBotStaleMs: toNumber(process.env.DIRECT_BOT_STALE_MS, 60000),
    directBotRecoverPerTick: toNumber(process.env.DIRECT_BOT_RECOVER_PER_TICK, 2),
  },
  timeframe: {
    base: '1m',
    analysis: (process.env.ANALYSIS_TIMEFRAME || '3m').toLowerCase(),
    trend: '5m',
    limit: toNumber(process.env.CANDLE_LIMIT, 600),
  },
  mode: process.env.BOT_MODE || 'SIGNAL_ONLY',
  websocket: {
    staleDataMs: toNumber(process.env.WS_STALE_DATA_MS, 12000),
    heartbeatIntervalMs: toNumber(process.env.WS_HEARTBEAT_INTERVAL_MS, 4000),
    maxReconnectAttempts: toNumber(process.env.WS_MAX_RECONNECT_ATTEMPTS, 10),
    circuitCooldownMs: toNumber(process.env.WS_CIRCUIT_COOLDOWN_MS, 45000),
  },
  persistence: {
    enabled: process.env.PERSISTENCE_ENABLED !== 'false',
    autosaveMs: toNumber(process.env.PERSISTENCE_AUTOSAVE_MS, 5000),
    path: process.env.PERSISTENCE_PATH || 'data/runtime-state.json',
  },
  trading: {
    allowAuto: process.env.ALLOW_AUTO === 'true',
    priceSource: (process.env.PRICE_SOURCE || 'LAST').toUpperCase() === 'MARK' ? 'MARK' : 'LAST',
    liveAnalyzeIntervalMs: toNumber(process.env.LIVE_ANALYZE_INTERVAL_MS, 1200),
    preSignalEnabled: process.env.PRE_SIGNAL_ENABLED !== 'false',
    preSignalMinProbability: toNumber(process.env.PRE_SIGNAL_MIN_PROBABILITY, 60),
    preSignalWatchDistancePct: toNumber(process.env.PRE_SIGNAL_WATCH_DISTANCE_PCT, 0.0052),
    preSignalArmDistancePct: toNumber(process.env.PRE_SIGNAL_ARM_DISTANCE_PCT, 0.0016),
    preSignalEmitIntervalMs: toNumber(process.env.PRE_SIGNAL_EMIT_INTERVAL_MS, 4000),
    preSignalPack1MinProbability: toNumber(process.env.PRE_SIGNAL_PACK1_MIN_PROBABILITY, 60),
    preSignalPack2MinScore: toNumber(process.env.PRE_SIGNAL_PACK2_MIN_SCORE, 68),
    preSignalRequireConsensus: process.env.PRE_SIGNAL_REQUIRE_CONSENSUS !== 'false',
    entryConfirmRequirePack2: process.env.ENTRY_CONFIRM_REQUIRE_PACK2 !== 'false',
    preSignalLeadCandles1m: toNumber(process.env.PRE_SIGNAL_LEAD_1M, 2),
    preSignalLeadCandles3m: toNumber(process.env.PRE_SIGNAL_LEAD_3M, 1),
    preSignalLeadCandles5m: toNumber(process.env.PRE_SIGNAL_LEAD_5M, 1),
    entryZoneBufferPct: toNumber(process.env.ENTRY_ZONE_BUFFER_PCT, 0.0008),
    maxSpreadPct: toNumber(process.env.MAX_SPREAD_PCT, 0.001),
    maxSlippagePct: toNumber(process.env.MAX_SLIPPAGE_PCT, 0.0016),
    maxEntryDeviationPct: toNumber(process.env.MAX_ENTRY_DEVIATION_PCT, 0.0018),
    maxLatencyMs: toNumber(process.env.MAX_LATENCY_MS, 2200),
    maxDistanceFromMA25Pct: toNumber(process.env.MAX_DISTANCE_MA25_PCT, 0.0058),
    maxExtendedCandlePct: toNumber(process.env.MAX_EXTENDED_CANDLE_PCT, 0.009),
    nearStrongLevelPct: toNumber(process.env.NEAR_STRONG_LEVEL_PCT, 0.0024),
    minRewardPct: toNumber(process.env.MIN_REWARD_PCT, 0.0018),
    overboughtLongRsi: toNumber(process.env.OVERBOUGHT_LONG_RSI, 68),
    oversoldShortRsi: toNumber(process.env.OVERSOLD_SHORT_RSI, 32),
    minAdx: toNumber(process.env.MIN_ADX, 19),
    bbSqueezeMax: toNumber(process.env.BB_SQUEEZE_MAX, 0.0045),
    macdConfirmRequired: process.env.MACD_CONFIRM_REQUIRED !== 'false',
    strictNoTrashMode: process.env.STRICT_NO_TRASH_MODE !== 'false',
    zoneSignalWindowMs: toNumber(process.env.ZONE_SIGNAL_WINDOW_MS, 300000),
    zoneSignalCooldownMs: toNumber(process.env.ZONE_SIGNAL_COOLDOWN_MS, 90000),
    maxSignalsPerZoneWindow: toNumber(process.env.MAX_SIGNALS_PER_ZONE_WINDOW, 2),
    minRR: toNumber(process.env.MIN_RR, 1.25),
    signalThreshold: toNumber(process.env.SIGNAL_THRESHOLD, 56),
    autoThreshold: toNumber(process.env.AUTO_THRESHOLD, 75),
    semiThreshold: toNumber(process.env.SEMI_THRESHOLD, 61),
    cooldownMinSec: toNumber(process.env.COOLDOWN_MIN_SEC, 30),
    cooldownMaxSec: toNumber(process.env.COOLDOWN_MAX_SEC, 120),
    signalDuplicateCooldownMs: toNumber(process.env.SIGNAL_DUPLICATE_COOLDOWN_MS, 22000),
    signalDuplicatePriceRangePct: toNumber(process.env.SIGNAL_DUPLICATE_PRICE_RANGE_PCT, 0.0015),
    maxSignalAgeMs: toNumber(process.env.MAX_SIGNAL_AGE_MS, 120000),
    trailingActivationRR: toNumber(process.env.TRAILING_ACTIVATION_RR, 1.5),
    trailingLockRR: toNumber(process.env.TRAILING_LOCK_RR, 0.7),
    takerFeePct: toNumber(process.env.TAKER_FEE_PCT, 0.0004),
    makerFeePct: toNumber(process.env.MAKER_FEE_PCT, 0.0002),
    allowMultiplePosition: process.env.ALLOW_MULTIPLE_POSITION === 'true',
    dynamicLeverageEnabled: process.env.DYNAMIC_LEVERAGE_ENABLED !== 'false',
    dynamicLeverageMin: toNumber(process.env.DYNAMIC_LEVERAGE_MIN, 10),
    dynamicLeverageMax: toNumber(process.env.DYNAMIC_LEVERAGE_MAX, 20),
    leverageConfidenceStep: toNumber(process.env.LEVERAGE_CONFIDENCE_STEP, 3),
  },
  positionSizing: {
    enforceNotionalRange: process.env.ENFORCE_NOTIONAL_RANGE !== 'false',
    highLiqMinNotionalUSDT: toNumber(process.env.HIGH_LIQ_MIN_NOTIONAL_USDT, 5),
    highLiqMaxNotionalUSDT: toNumber(process.env.HIGH_LIQ_MAX_NOTIONAL_USDT, 20),
    scpMinNotionalUSDT: toNumber(process.env.SCP_MIN_NOTIONAL_USDT, 5),
    scpMaxNotionalUSDT: toNumber(process.env.SCP_MAX_NOTIONAL_USDT, 10),
    highLiqSymbols: parseSymbolList(process.env.HIGH_LIQ_SYMBOLS, [
      'BTCUSDT',
      'ETHUSDT',
      'BNBUSDT',
      'DOGEUSDT',
      'SOLUSDT',
      'XRPUSDT',
      'BCHUSDT',
      '1000PEPEUSDT',
      'UNIUSDT',
    ]),
    scpSymbols: parseSymbolList(process.env.SCP_SYMBOLS, [
      'ADAUSDT',
      'TRXUSDT',
      'LINKUSDT',
      'POLUSDT',
      'AVAXUSDT',
      'LTCUSDT',
      'DOTUSDT',
      'ATOMUSDT',
      'APTUSDT',
      'NEARUSDT',
      'FILUSDT',
      'ETCUSDT',
      'SUIUSDT',
      'SEIUSDT',
      'ARBUSDT',
      'OPUSDT',
    ]),
  },
  advanced: {
    enabled: process.env.ADVANCED_ENABLED !== 'false',
    useTrend15mConfluence: process.env.USE_TREND_15M_CONFLUENCE !== 'false',
    minCandlesAnalysis: toNumber(process.env.MIN_CANDLES_ANALYSIS, 80),
    minCandlesTrend5m: toNumber(process.env.MIN_CANDLES_TREND_5M, 80),
    minCandlesTrend15m: toNumber(process.env.MIN_CANDLES_TREND_15M, 40),
    minAtrPct2m: toNumber(process.env.MIN_ATR_PCT_2M, 0.0009),
    maxAtrPct2m: toNumber(process.env.MAX_ATR_PCT_2M, 0.008),
    sessionFilterEnabled: process.env.SESSION_FILTER_ENABLED === 'true',
    allowedSessions: (process.env.ALLOWED_SESSIONS || 'EUROPE,US')
      .split(',')
      .map((s) => s.trim().toUpperCase())
      .filter(Boolean),
    strictSessionReject: process.env.STRICT_SESSION_REJECT === 'true',
    dynamicThresholdByVolatility: process.env.DYNAMIC_THRESHOLD_BY_VOLATILITY !== 'false',
  },
  risk: {
    defaultRiskPerTradePct: toNumber(process.env.RISK_PER_TRADE_PCT, 0.008),
    maxRiskPerTradePct: toNumber(process.env.MAX_RISK_PER_TRADE_PCT, 0.05),
    minRiskPerTradePct: toNumber(process.env.MIN_RISK_PER_TRADE_PCT, 0.005),
    dailyLossLimitPct: toNumber(process.env.DAILY_LOSS_LIMIT_PCT, 0.05),
    maxDrawdownPct: toNumber(process.env.MAX_DRAWDOWN_PCT, 0.1),
    maxConsecutiveLosses: toNumber(process.env.MAX_CONSECUTIVE_LOSSES, 3),
    maxTradesPerHour: toNumber(process.env.MAX_TRADES_PER_HOUR, 5),
    maxTradesPerDay: toNumber(process.env.MAX_TRADES_PER_DAY, 22),
    cooldownAfterLossSec: toNumber(process.env.COOLDOWN_AFTER_LOSS_SEC, 90),
    cooldownAfterWinSec: toNumber(process.env.COOLDOWN_AFTER_WIN_SEC, 35),
    maxConsecutiveErrors: toNumber(process.env.MAX_CONSECUTIVE_ERRORS, 4),
    autoEmergencyDrawdownPct: toNumber(process.env.AUTO_EMERGENCY_DRAWDOWN_PCT, 0.14),
    emergencyStopOnConsecutiveErrors: process.env.EMERGENCY_STOP_ON_CONSECUTIVE_ERRORS !== 'false',
    closePositionOnEmergency: process.env.CLOSE_POSITION_ON_EMERGENCY !== 'false',
    keepRunningOnRiskLock: process.env.KEEP_RUNNING_ON_RISK_LOCK !== 'false',
    lockBotOnError: process.env.LOCK_BOT_ON_ERROR !== 'false',
    syncUnknownPositionAction: (process.env.SYNC_UNKNOWN_POSITION_ACTION || 'PAUSE').toUpperCase(),
    emergencyStop: false,
    initialCapital: toNumber(process.env.INITIAL_CAPITAL, 1000),
    minNotionalUSDT: toNumber(process.env.MIN_NOTIONAL_USDT, 5),
  },
  telegram: {
    enabled: process.env.TELEGRAM_ENABLED === 'true',
    token: process.env.TELEGRAM_BOT_TOKEN || '',
    chatId: process.env.TELEGRAM_CHAT_ID || '',
    primaryName: process.env.TELEGRAM_PRIMARY_NAME || 'BOT THANH KHOẢN CAO',
    highLiqSymbols: parseSymbolList(process.env.TELEGRAM_HIGH_LIQ_SYMBOLS, [
      'BTCUSDT',
      'ETHUSDT',
      'BNBUSDT',
      'DOGEUSDT',
      'SOLUSDT',
      'XRPUSDT',
      'BCHUSDT',
      '1000PEPEUSDT',
      'UNIUSDT',
    ]),
    secondaryEnabled: process.env.TELEGRAM_SECONDARY_ENABLED !== 'false',
    secondaryName: process.env.TELEGRAM_SECONDARY_NAME || 'BOT SCP',
    secondaryToken: process.env.TELEGRAM_SECONDARY_BOT_TOKEN || '',
    secondaryChatId: process.env.TELEGRAM_SECONDARY_CHAT_ID || '',
    scpSymbols: parseSymbolList(process.env.TELEGRAM_SCP_SYMBOLS, [
      'ADAUSDT',
      'TRXUSDT',
      'LINKUSDT',
      'POLUSDT',
      'AVAXUSDT',
      'LTCUSDT',
      'DOTUSDT',
      'ATOMUSDT',
      'APTUSDT',
      'NEARUSDT',
      'FILUSDT',
      'ETCUSDT',
      'SUIUSDT',
      'SEIUSDT',
      'ARBUSDT',
      'OPUSDT',
    ]),
    primaryIcon: process.env.TELEGRAM_PRIMARY_ICON || '🔵',
    secondaryIcon: process.env.TELEGRAM_SECONDARY_ICON || '🟢',
    sentryIcon: process.env.TELEGRAM_SENTRY_ICON || '🛰️',
    routeUnknownToPrimary: process.env.TELEGRAM_ROUTE_UNKNOWN_TO_PRIMARY !== 'false',
    sendSystemToAllRoutes: process.env.TELEGRAM_SEND_SYSTEM_TO_ALL !== 'false',
    parseMode: process.env.TELEGRAM_PARSE_MODE || 'HTML',
    sendSignal: process.env.TELEGRAM_SEND_SIGNAL !== 'false',
    sendOrderOpen: process.env.TELEGRAM_SEND_ORDER_OPEN !== 'false',
    sendOrderClose: process.env.TELEGRAM_SEND_ORDER_CLOSE !== 'false',
    sendRejectReason: process.env.TELEGRAM_SEND_REJECT !== 'false',
    sendErrors: process.env.TELEGRAM_SEND_ERRORS !== 'false',
    sendDailyReport: process.env.TELEGRAM_SEND_DAILY !== 'false',
    sendWeeklyReport: process.env.TELEGRAM_SEND_WEEKLY !== 'false',
    sendChartImage: process.env.TELEGRAM_SEND_CHART_IMAGE !== 'false',
    preSignalEnabled: process.env.TELEGRAM_PRE_SIGNAL_ENABLED !== 'false',
    preSignalMinProbability: toNumber(process.env.TELEGRAM_PRE_SIGNAL_MIN_PROB, 78),
    preSignalMinCandles: toNumber(process.env.TELEGRAM_PRE_SIGNAL_MIN_CANDLES, 1),
    preSignalMaxCandles: toNumber(process.env.TELEGRAM_PRE_SIGNAL_MAX_CANDLES, 5),
    preSignalCooldownMs: toNumber(process.env.TELEGRAM_PRE_SIGNAL_COOLDOWN_MS, 90000),
    positionHeartbeatEnabled: process.env.TELEGRAM_POSITION_HEARTBEAT_ENABLED !== 'false',
    positionHeartbeatMs: toNumber(process.env.TELEGRAM_POSITION_HEARTBEAT_MS, 5000),
    stopRiskAlertEnabled: process.env.TELEGRAM_STOP_RISK_ALERT_ENABLED !== 'false',
    stopRiskAlertThreshold: toNumber(process.env.TELEGRAM_STOP_RISK_ALERT_THRESHOLD, 76),
    stopRiskAlertCooldownMs: toNumber(process.env.TELEGRAM_STOP_RISK_ALERT_COOLDOWN_MS, 30000),
    cooldownMs: toNumber(process.env.TELEGRAM_COOLDOWN_MS, 15000),
    maxRetry: toNumber(process.env.TELEGRAM_MAX_RETRY, 2),
  },
  chartSnapshot: {
    enabled: process.env.CHART_SNAPSHOT_ENABLED !== 'false',
    timeoutMs: toNumber(process.env.CHART_SNAPSHOT_TIMEOUT_MS, 12000),
    cooldownMs: toNumber(process.env.CHART_SNAPSHOT_COOLDOWN_MS, 45000),
    keepHours: toNumber(process.env.CHART_SNAPSHOT_KEEP_HOURS, 48),
    sendOnStrongSignal: process.env.CHART_SNAPSHOT_STRONG_SIGNAL_ONLY !== 'false',
    sendOnEntry: process.env.CHART_SNAPSHOT_ON_ENTRY !== 'false',
    baseUrl: process.env.CHART_BASE_URL || `http://localhost:${toNumber(process.env.PORT, 3000)}`,
  },
  log: {
    maxMemoryLogs: toNumber(process.env.MAX_MEMORY_LOGS, 1500),
    saveToFile: process.env.LOG_SAVE_TO_FILE !== 'false',
  },
};

module.exports = config;
