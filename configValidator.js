function inRange(value, min, max) {
  return Number.isFinite(value) && value >= min && value <= max;
}

function validateCoreConfig(config) {
  const errors = [];

  const modes = ['SIGNAL_ONLY', 'SEMI_AUTO', 'AUTO_BOT'];
  if (!modes.includes(config.mode)) {
    errors.push('BOT_MODE khong hop le');
  }
  const rawTfAllow = ['1m', '3m', '5m', '15m'];
  if (!rawTfAllow.includes(String(config.timeframe.analysis || '').toLowerCase())) {
    errors.push('timeframe.analysis chi duoc dung 1m/3m/5m/15m de lay nen goc Binance');
  }

  const t = config.trading;
  if (!inRange(t.signalThreshold, 0, 100)) errors.push('trading.signalThreshold phai trong [0,100]');
  if (!inRange(t.semiThreshold, 0, 100)) errors.push('trading.semiThreshold phai trong [0,100]');
  if (!inRange(t.autoThreshold, 0, 100)) errors.push('trading.autoThreshold phai trong [0,100]');
  if (t.autoThreshold <= t.signalThreshold) {
    errors.push('AUTO_THRESHOLD phai lon hon SIGNAL_THRESHOLD');
  }
  if (t.semiThreshold < t.signalThreshold) {
    errors.push('SEMI_THRESHOLD phai >= SIGNAL_THRESHOLD');
  }

  const percentFields = [
    ['trading.maxSpreadPct', t.maxSpreadPct],
    ['trading.maxSlippagePct', t.maxSlippagePct],
    ['trading.maxEntryDeviationPct', t.maxEntryDeviationPct],
    ['trading.maxDistanceFromMA25Pct', t.maxDistanceFromMA25Pct],
    ['trading.maxExtendedCandlePct', t.maxExtendedCandlePct],
    ['trading.preSignalWatchDistancePct', t.preSignalWatchDistancePct],
    ['trading.preSignalArmDistancePct', t.preSignalArmDistancePct],
    ['trading.nearStrongLevelPct', t.nearStrongLevelPct],
    ['trading.minRewardPct', t.minRewardPct],
    ['trading.entryZoneBufferPct', t.entryZoneBufferPct],
    ['trading.takerFeePct', t.takerFeePct],
  ];

  for (const [name, value] of percentFields) {
    if (!Number.isFinite(value) || value < 0) {
      errors.push(`${name} phai la so khong am`);
    }
  }

  if (!Number.isFinite(t.cooldownMinSec) || t.cooldownMinSec < 0) {
    errors.push('trading.cooldownMinSec phai >= 0');
  }
  if (!Number.isFinite(t.cooldownMaxSec) || t.cooldownMaxSec < t.cooldownMinSec) {
    errors.push('trading.cooldownMaxSec phai >= cooldownMinSec');
  }
  if (!Number.isFinite(t.liveAnalyzeIntervalMs) || t.liveAnalyzeIntervalMs < 500) {
    errors.push('trading.liveAnalyzeIntervalMs phai >= 500');
  }
  if (!inRange(t.preSignalMinProbability, 0, 100)) {
    errors.push('trading.preSignalMinProbability phai trong [0,100]');
  }
  if (!inRange(t.preSignalPack1MinProbability, 0, 100)) {
    errors.push('trading.preSignalPack1MinProbability phai trong [0,100]');
  }
  if (!inRange(t.preSignalPack2MinScore, 0, 100)) {
    errors.push('trading.preSignalPack2MinScore phai trong [0,100]');
  }
  if (!Number.isFinite(t.preSignalEmitIntervalMs) || t.preSignalEmitIntervalMs < 1000) {
    errors.push('trading.preSignalEmitIntervalMs phai >= 1000');
  }
  if (!Number.isFinite(t.preSignalLeadCandles1m) || t.preSignalLeadCandles1m < 1) {
    errors.push('trading.preSignalLeadCandles1m phai >= 1');
  }
  if (!Number.isFinite(t.preSignalLeadCandles3m) || t.preSignalLeadCandles3m < 1) {
    errors.push('trading.preSignalLeadCandles3m phai >= 1');
  }
  if (!Number.isFinite(t.preSignalLeadCandles5m) || t.preSignalLeadCandles5m < 1) {
    errors.push('trading.preSignalLeadCandles5m phai >= 1');
  }
  if (
    Number.isFinite(t.preSignalWatchDistancePct) &&
    Number.isFinite(t.preSignalArmDistancePct) &&
    t.preSignalWatchDistancePct < t.preSignalArmDistancePct
  ) {
    errors.push('trading.preSignalWatchDistancePct phai >= preSignalArmDistancePct');
  }
  if (!Number.isFinite(t.zoneSignalWindowMs) || t.zoneSignalWindowMs < 10000) {
    errors.push('trading.zoneSignalWindowMs phai >= 10000');
  }
  if (!Number.isFinite(t.zoneSignalCooldownMs) || t.zoneSignalCooldownMs < 1000) {
    errors.push('trading.zoneSignalCooldownMs phai >= 1000');
  }
  if (!Number.isFinite(t.maxSignalsPerZoneWindow) || t.maxSignalsPerZoneWindow < 1) {
    errors.push('trading.maxSignalsPerZoneWindow phai >= 1');
  }
  if (!inRange(t.overboughtLongRsi, 50, 100)) {
    errors.push('trading.overboughtLongRsi phai trong [50,100]');
  }
  if (!inRange(t.oversoldShortRsi, 0, 50)) {
    errors.push('trading.oversoldShortRsi phai trong [0,50]');
  }
  if (!inRange(t.minAdx, 5, 60)) {
    errors.push('trading.minAdx phai trong [5,60]');
  }
  if (!Number.isFinite(t.bbSqueezeMax) || t.bbSqueezeMax < 0) {
    errors.push('trading.bbSqueezeMax phai la so khong am');
  }
  if (Number.isFinite(t.overboughtLongRsi) && Number.isFinite(t.oversoldShortRsi) && t.overboughtLongRsi <= t.oversoldShortRsi) {
    errors.push('trading.overboughtLongRsi phai lon hon oversoldShortRsi');
  }
  if (!Number.isFinite(t.dynamicLeverageMin) || t.dynamicLeverageMin < 1) {
    errors.push('trading.dynamicLeverageMin phai >= 1');
  }
  if (!Number.isFinite(t.dynamicLeverageMax) || t.dynamicLeverageMax < t.dynamicLeverageMin) {
    errors.push('trading.dynamicLeverageMax phai >= dynamicLeverageMin');
  }
  if (!Number.isFinite(t.leverageConfidenceStep) || t.leverageConfidenceStep < 1) {
    errors.push('trading.leverageConfidenceStep phai >= 1');
  }

  const r = config.risk;
  if (!Number.isFinite(r.minRiskPerTradePct) || r.minRiskPerTradePct < 0) {
    errors.push('risk.minRiskPerTradePct phai >= 0');
  }
  if (!Number.isFinite(r.maxRiskPerTradePct) || r.maxRiskPerTradePct <= 0) {
    errors.push('risk.maxRiskPerTradePct phai > 0');
  }
  if (!Number.isFinite(r.defaultRiskPerTradePct) || r.defaultRiskPerTradePct <= 0) {
    errors.push('risk.defaultRiskPerTradePct phai > 0');
  }
  if (r.minRiskPerTradePct > r.defaultRiskPerTradePct || r.defaultRiskPerTradePct > r.maxRiskPerTradePct) {
    errors.push('Can dam bao minRisk <= defaultRisk <= maxRisk');
  }
  if (r.maxRiskPerTradePct > 0.05) {
    errors.push('risk.maxRiskPerTradePct khong duoc vuot 0.05');
  }

  const nonNegativeRisk = [
    ['risk.dailyLossLimitPct', r.dailyLossLimitPct],
    ['risk.maxDrawdownPct', r.maxDrawdownPct],
    ['risk.autoEmergencyDrawdownPct', r.autoEmergencyDrawdownPct],
    ['risk.minNotionalUSDT', r.minNotionalUSDT],
  ];
  for (const [name, value] of nonNegativeRisk) {
    if (!Number.isFinite(value) || value < 0) {
      errors.push(`${name} phai la so khong am`);
    }
  }

  if (!Number.isFinite(r.maxTradesPerHour) || r.maxTradesPerHour < 1) {
    errors.push('risk.maxTradesPerHour phai >= 1');
  }
  if (!Number.isFinite(r.maxTradesPerDay) || r.maxTradesPerDay < 1) {
    errors.push('risk.maxTradesPerDay phai >= 1');
  }
  if (!['PAUSE', 'KEEP', 'CLOSE'].includes(String(r.syncUnknownPositionAction || '').toUpperCase())) {
    errors.push('risk.syncUnknownPositionAction chi nhan PAUSE | KEEP | CLOSE');
  }

  const ps = config.positionSizing || {};
  if (!Number.isFinite(ps.highLiqMinNotionalUSDT) || ps.highLiqMinNotionalUSDT < 0) {
    errors.push('positionSizing.highLiqMinNotionalUSDT phai >= 0');
  }
  if (!Number.isFinite(ps.highLiqMaxNotionalUSDT) || ps.highLiqMaxNotionalUSDT < ps.highLiqMinNotionalUSDT) {
    errors.push('positionSizing.highLiqMaxNotionalUSDT phai >= highLiqMinNotionalUSDT');
  }
  if (!Number.isFinite(ps.scpMinNotionalUSDT) || ps.scpMinNotionalUSDT < 0) {
    errors.push('positionSizing.scpMinNotionalUSDT phai >= 0');
  }
  if (!Number.isFinite(ps.scpMaxNotionalUSDT) || ps.scpMaxNotionalUSDT < ps.scpMinNotionalUSDT) {
    errors.push('positionSizing.scpMaxNotionalUSDT phai >= scpMinNotionalUSDT');
  }

  const ws = config.websocket;
  if (!Number.isFinite(ws.staleDataMs) || ws.staleDataMs < 1000) {
    errors.push('websocket.staleDataMs phai >= 1000');
  }
  if (!Number.isFinite(ws.heartbeatIntervalMs) || ws.heartbeatIntervalMs < 500) {
    errors.push('websocket.heartbeatIntervalMs phai >= 500');
  }
  if (!Number.isFinite(ws.maxReconnectAttempts) || ws.maxReconnectAttempts < 1) {
    errors.push('websocket.maxReconnectAttempts phai >= 1');
  }
  if (!Number.isFinite(ws.maxBookLatencyMs) || ws.maxBookLatencyMs < 1000) {
    errors.push('websocket.maxBookLatencyMs phai >= 1000');
  }

  const sp = config.selfPing || {};
  if (sp.enabled) {
    if (!Number.isFinite(sp.intervalMs) || sp.intervalMs < 60000) {
      errors.push('selfPing.intervalMs phai >= 60000');
    }
    if (!Number.isFinite(sp.retryMs) || sp.retryMs < 1000) {
      errors.push('selfPing.retryMs phai >= 1000');
    }
    if (!Number.isFinite(sp.timeoutMs) || sp.timeoutMs < 1000) {
      errors.push('selfPing.timeoutMs phai >= 1000');
    }
    if (!sp.url || !/^https?:\/\//i.test(String(sp.url))) {
      errors.push('selfPing.url phai la URL hop le bat dau bang http/https');
    }
  }

  const b = config.binance || {};
  if (!Number.isFinite(b.restSoftLimitPerMin) || b.restSoftLimitPerMin < 20) {
    errors.push('binance.restSoftLimitPerMin phai >= 20');
  }
  if (!Number.isFinite(b.restBlock429Ms) || b.restBlock429Ms < 1000) {
    errors.push('binance.restBlock429Ms phai >= 1000');
  }
  if (!Number.isFinite(b.restBlock418Ms) || b.restBlock418Ms < b.restBlock429Ms) {
    errors.push('binance.restBlock418Ms phai >= binance.restBlock429Ms');
  }
  if (!Number.isFinite(b.restClosedSyncMinGapMs) || b.restClosedSyncMinGapMs < 500) {
    errors.push('binance.restClosedSyncMinGapMs phai >= 500');
  }
  if (!Number.isFinite(b.scannerLoopMs) || b.scannerLoopMs < 3000) {
    errors.push('binance.scannerLoopMs phai >= 3000');
  }
  if (!Number.isFinite(b.scannerBatchSize) || b.scannerBatchSize < 1) {
    errors.push('binance.scannerBatchSize phai >= 1');
  }
  if (!Number.isFinite(b.scannerCandleLimit) || b.scannerCandleLimit < 220) {
    errors.push('binance.scannerCandleLimit phai >= 220 de du MA200');
  }
  if (!Number.isFinite(b.scannerBootstrapBatchSize) || b.scannerBootstrapBatchSize < 1) {
    errors.push('binance.scannerBootstrapBatchSize phai >= 1');
  }
  if (!Number.isFinite(b.scannerBootstrapDelayMs) || b.scannerBootstrapDelayMs < 0) {
    errors.push('binance.scannerBootstrapDelayMs phai >= 0');
  }
  if (!Number.isFinite(b.directBotWatchdogMs) || b.directBotWatchdogMs < 5000) {
    errors.push('binance.directBotWatchdogMs phai >= 5000');
  }
  if (!Number.isFinite(b.directBotStaleMs) || b.directBotStaleMs < 10000) {
    errors.push('binance.directBotStaleMs phai >= 10000');
  }
  if (!Number.isFinite(b.directBotRecoverPerTick) || b.directBotRecoverPerTick < 1) {
    errors.push('binance.directBotRecoverPerTick phai >= 1');
  }

  const tg = config.telegram || {};
  if (tg.enabled) {
    const hasPrimary = Boolean(tg.token && tg.chatId);
    const hasSecondary = Boolean(tg.secondaryEnabled && tg.secondaryToken && tg.secondaryChatId);
    if (!hasPrimary && !hasSecondary) {
      errors.push('telegram.enabled=true nhưng chưa cấu hình bot/chat hợp lệ');
    }
  }
  if (!inRange(Number(tg.preSignalMinProbability), 0, 100)) {
    errors.push('telegram.preSignalMinProbability phai trong [0,100]');
  }
  if (!Number.isFinite(tg.preSignalMinCandles) || tg.preSignalMinCandles < 1) {
    errors.push('telegram.preSignalMinCandles phai >= 1');
  }
  if (!Number.isFinite(tg.preSignalMaxCandles) || tg.preSignalMaxCandles < tg.preSignalMinCandles) {
    errors.push('telegram.preSignalMaxCandles phai >= preSignalMinCandles');
  }
  if (!Number.isFinite(tg.preSignalCooldownMs) || tg.preSignalCooldownMs < 5000) {
    errors.push('telegram.preSignalCooldownMs phai >= 5000');
  }
  if (!Number.isFinite(tg.positionHeartbeatMs) || tg.positionHeartbeatMs < 5000) {
    errors.push('telegram.positionHeartbeatMs phai >= 5000');
  }
  if (!inRange(Number(tg.stopRiskAlertThreshold), 1, 100)) {
    errors.push('telegram.stopRiskAlertThreshold phai trong [1,100]');
  }
  if (!Number.isFinite(tg.stopRiskAlertCooldownMs) || tg.stopRiskAlertCooldownMs < 5000) {
    errors.push('telegram.stopRiskAlertCooldownMs phai >= 5000');
  }

  if (!Number.isFinite(config.security.rateLimitWindowMs) || config.security.rateLimitWindowMs < 1000) {
    errors.push('security.rateLimitWindowMs phai >= 1000');
  }
  if (!Number.isFinite(config.security.rateLimitMax) || config.security.rateLimitMax < 1) {
    errors.push('security.rateLimitMax phai >= 1');
  }
  if (config.security.requireAdminForWrite && !config.security.adminToken) {
    errors.push('REQUIRE_ADMIN_FOR_WRITE=true nhung ADMIN_TOKEN dang rong');
  }

  const allowedSources = ['LAST', 'MARK'];
  if (!allowedSources.includes(config.trading.priceSource)) {
    errors.push('trading.priceSource chi nhan LAST hoac MARK');
  }

  const rest = String(config.binance.restBaseUrl || '').toLowerCase();
  const wsUrl = String(config.binance.wsBaseUrl || '').toLowerCase();
  const restFutureOk = rest.includes('fapi.binance.com') || rest.includes('testnet.binancefuture.com');
  const wsFutureOk = wsUrl.includes('fstream.binance.com') || wsUrl.includes('stream.binancefuture.com');
  if (!restFutureOk) {
    errors.push('binance.restBaseUrl phai la Binance Futures endpoint (fapi/testnet.binancefuture)');
  }
  if (!wsFutureOk) {
    errors.push('binance.wsBaseUrl phai la Binance Futures WS endpoint (fstream/stream.binancefuture)');
  }

  return errors;
}

function validateStartupConfig(config) {
  const errors = validateCoreConfig(config);
  return {
    ok: errors.length === 0,
    errors,
  };
}

module.exports = {
  validateStartupConfig,
  validateCoreConfig,
};
