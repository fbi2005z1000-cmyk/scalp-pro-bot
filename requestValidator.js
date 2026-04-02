function isObject(v) {
  return v !== null && typeof v === 'object' && !Array.isArray(v);
}

function toError(message, field) {
  return { message, field };
}

function validateModeBody(body) {
  const modes = ['SIGNAL_ONLY', 'SEMI_AUTO', 'AUTO_BOT'];
  if (!isObject(body)) return { ok: false, errors: [toError('Body phai la object JSON')] };
  if (!modes.includes(body.mode)) {
    return { ok: false, errors: [toError('mode khong hop le', 'mode')] };
  }
  return { ok: true, errors: [] };
}

function validateTradeBody(body) {
  if (body === undefined || body === null) return { ok: true, errors: [] };
  if (!isObject(body)) return { ok: false, errors: [toError('Body phai la object JSON')] };
  if (Object.prototype.hasOwnProperty.call(body, 'confirm') && typeof body.confirm !== 'boolean') {
    return { ok: false, errors: [toError('confirm phai la boolean', 'confirm')] };
  }
  return { ok: true, errors: [] };
}

function validateTelegramTestBody(body) {
  if (!isObject(body)) return { ok: false, errors: [toError('Body phai la object JSON')] };

  const errors = [];
  const allowKeys = [
    'message',
    'route',
    'symbol',
    'sticker',
    'animation',
    'caption',
    'diceEmoji',
  ];

  for (const key of Object.keys(body)) {
    if (!allowKeys.includes(key)) errors.push(toError(`Khong cho phep truong ${key}`, key));
  }

  if (body.message !== undefined && typeof body.message !== 'string') {
    errors.push(toError('message phai la string', 'message'));
  }
  if (typeof body.message === 'string' && body.message.length > 4000) {
    errors.push(toError('message khong duoc vuot qua 4000 ky tu', 'message'));
  }

  if (body.route !== undefined) {
    const route = String(body.route || '').toUpperCase();
    if (!['ALL', 'PRIMARY', 'SECONDARY'].includes(route)) {
      errors.push(toError('route chi nhan ALL | PRIMARY | SECONDARY', 'route'));
    }
  }

  if (body.symbol !== undefined && typeof body.symbol !== 'string') {
    errors.push(toError('symbol phai la string', 'symbol'));
  }
  if (body.symbol && String(body.symbol).trim().length < 6) {
    errors.push(toError('symbol khong hop le', 'symbol'));
  }

  if (body.sticker !== undefined && typeof body.sticker !== 'string') {
    errors.push(toError('sticker phai la string', 'sticker'));
  }
  if (typeof body.sticker === 'string' && body.sticker.length > 1024) {
    errors.push(toError('sticker khong duoc vuot qua 1024 ky tu', 'sticker'));
  }

  if (body.animation !== undefined && typeof body.animation !== 'string') {
    errors.push(toError('animation phai la string', 'animation'));
  }
  if (typeof body.animation === 'string' && body.animation.length > 1024) {
    errors.push(toError('animation khong duoc vuot qua 1024 ky tu', 'animation'));
  }

  if (body.caption !== undefined && typeof body.caption !== 'string') {
    errors.push(toError('caption phai la string', 'caption'));
  }
  if (typeof body.caption === 'string' && body.caption.length > 1024) {
    errors.push(toError('caption khong duoc vuot qua 1024 ky tu', 'caption'));
  }

  if (body.diceEmoji !== undefined) {
    const allowedDice = ['🎲', '🎯', '🏀', '⚽', '🎳', '🎰'];
    const diceEmoji = String(body.diceEmoji || '').trim();
    if (diceEmoji && !allowedDice.includes(diceEmoji)) {
      errors.push(toError('diceEmoji chi nhan 🎲 🎯 🏀 ⚽ 🎳 🎰', 'diceEmoji'));
    }
  }

  return {
    ok: errors.length === 0,
    errors,
  };
}

function validateConfigBody(body, runtimeConfig) {
  if (!isObject(body)) return { ok: false, errors: [toError('Body phai la object JSON')] };

  const errors = [];
  const allowRoot = ['mode', 'symbol', 'risk', 'trading', 'advanced', 'positionSizing'];
  for (const key of Object.keys(body)) {
    if (!allowRoot.includes(key)) errors.push(toError(`Khong cho phep truong ${key}`, key));
  }

  if (body.mode) {
    const modeVal = validateModeBody({ mode: body.mode });
    if (!modeVal.ok) errors.push(...modeVal.errors);
  }

  if (body.symbol && (typeof body.symbol !== 'string' || body.symbol.length < 6)) {
    errors.push(toError('symbol khong hop le', 'symbol'));
  }

  if (body.risk && !isObject(body.risk)) {
    errors.push(toError('risk phai la object', 'risk'));
  }

  if (body.trading && !isObject(body.trading)) {
    errors.push(toError('trading phai la object', 'trading'));
  }

  if (body.advanced && !isObject(body.advanced)) {
    errors.push(toError('advanced phai la object', 'advanced'));
  }
  if (body.positionSizing && !isObject(body.positionSizing)) {
    errors.push(toError('positionSizing phai la object', 'positionSizing'));
  }

  if (body.risk && isObject(body.risk)) {
    const numericRisk = [
      'defaultRiskPerTradePct',
      'maxRiskPerTradePct',
      'minRiskPerTradePct',
      'dailyLossLimitPct',
      'maxDrawdownPct',
      'maxConsecutiveLosses',
      'maxTradesPerHour',
      'maxTradesPerDay',
      'cooldownAfterLossSec',
      'cooldownAfterWinSec',
      'maxConsecutiveErrors',
      'autoEmergencyDrawdownPct',
      'initialCapital',
      'minNotionalUSDT',
      'clusterWindowMin',
      'clusterMaxSameSideTrades',
      'clusterMaxConsecutiveLosses',
      'clusterPauseAfterLossMin',
    ];
    for (const key of numericRisk) {
      if (Object.prototype.hasOwnProperty.call(body.risk, key)) {
        const value = body.risk[key];
        if (typeof value !== 'number' || !Number.isFinite(value)) {
          errors.push(toError(`${key} phai la so`, `risk.${key}`));
        }
      }
    }

    if (
      Object.prototype.hasOwnProperty.call(body.risk, 'emergencyStop') &&
      typeof body.risk.emergencyStop !== 'boolean'
    ) {
      errors.push(toError('emergencyStop phai la boolean', 'risk.emergencyStop'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.risk, 'keepRunningOnRiskLock') &&
      typeof body.risk.keepRunningOnRiskLock !== 'boolean'
    ) {
      errors.push(toError('keepRunningOnRiskLock phai la boolean', 'risk.keepRunningOnRiskLock'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.risk, 'clusterGuardEnabled') &&
      typeof body.risk.clusterGuardEnabled !== 'boolean'
    ) {
      errors.push(toError('clusterGuardEnabled phai la boolean', 'risk.clusterGuardEnabled'));
    }
    if (Object.prototype.hasOwnProperty.call(body.risk, 'syncUnknownPositionAction')) {
      const action = String(body.risk.syncUnknownPositionAction || '').toUpperCase();
      if (!['PAUSE', 'KEEP', 'CLOSE'].includes(action)) {
        errors.push(
          toError('syncUnknownPositionAction chi nhan PAUSE | KEEP | CLOSE', 'risk.syncUnknownPositionAction'),
        );
      }
    }
  }

  if (body.trading && isObject(body.trading)) {
    const numericTrading = [
      'signalThreshold',
      'semiThreshold',
      'autoThreshold',
      'maxSpreadPct',
      'maxSlippagePct',
      'maxEntryDeviationPct',
      'maxLatencyMs',
      'maxDistanceFromMA25Pct',
      'maxExtendedCandlePct',
      'nearStrongLevelPct',
      'minRewardPct',
      'overboughtLongRsi',
      'oversoldShortRsi',
      'zoneSignalWindowMs',
      'zoneSignalCooldownMs',
      'maxSignalsPerZoneWindow',
      'minRR',
      'cooldownMinSec',
      'cooldownMaxSec',
      'signalDuplicateCooldownMs',
      'signalDuplicatePriceRangePct',
      'maxSignalAgeMs',
      'liveAnalyzeIntervalMs',
      'preSignalMinProbability',
      'preSignalPack1MinProbability',
      'preSignalPack2MinScore',
      'preSignalWatchDistancePct',
      'preSignalArmDistancePct',
      'preSignalEmitIntervalMs',
      'globalRankingWindowMs',
      'globalRankingTopN',
      'globalRankingMinRank',
      'globalRankingDispatchCooldownMs',
      'adaptiveSetupLookbackTrades',
      'adaptiveSetupMinTrades',
      'adaptiveSetupMaxBonus',
      'adaptiveSetupMaxPenalty',
      'executionSimMinPassScore',
      'executionSimMinLiquidity',
      'trailingActivationRR',
      'trailingLockRR',
      'takerFeePct',
      'dynamicLeverageMin',
      'dynamicLeverageMax',
      'leverageConfidenceStep',
    ];
    for (const key of numericTrading) {
      if (Object.prototype.hasOwnProperty.call(body.trading, key)) {
        const value = body.trading[key];
        if (typeof value !== 'number' || !Number.isFinite(value)) {
          errors.push(toError(`${key} phai la so`, `trading.${key}`));
        }
      }
    }

    if (
      Object.prototype.hasOwnProperty.call(body.trading, 'allowMultiplePosition') &&
      typeof body.trading.allowMultiplePosition !== 'boolean'
    ) {
      errors.push(toError('allowMultiplePosition phai la boolean', 'trading.allowMultiplePosition'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.trading, 'preSignalEnabled') &&
      typeof body.trading.preSignalEnabled !== 'boolean'
    ) {
      errors.push(toError('preSignalEnabled phai la boolean', 'trading.preSignalEnabled'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.trading, 'preSignalRequireConsensus') &&
      typeof body.trading.preSignalRequireConsensus !== 'boolean'
    ) {
      errors.push(toError('preSignalRequireConsensus phai la boolean', 'trading.preSignalRequireConsensus'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.trading, 'entryConfirmRequirePack2') &&
      typeof body.trading.entryConfirmRequirePack2 !== 'boolean'
    ) {
      errors.push(toError('entryConfirmRequirePack2 phai la boolean', 'trading.entryConfirmRequirePack2'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.trading, 'dynamicLeverageEnabled') &&
      typeof body.trading.dynamicLeverageEnabled !== 'boolean'
    ) {
      errors.push(toError('dynamicLeverageEnabled phai la boolean', 'trading.dynamicLeverageEnabled'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.trading, 'globalRankingEnabled') &&
      typeof body.trading.globalRankingEnabled !== 'boolean'
    ) {
      errors.push(toError('globalRankingEnabled phai la boolean', 'trading.globalRankingEnabled'));
    }
    if (
      Object.prototype.hasOwnProperty.call(body.trading, 'adaptiveSetupWeightEnabled') &&
      typeof body.trading.adaptiveSetupWeightEnabled !== 'boolean'
    ) {
      errors.push(toError('adaptiveSetupWeightEnabled phai la boolean', 'trading.adaptiveSetupWeightEnabled'));
    }

    if (Object.prototype.hasOwnProperty.call(body.trading, 'priceSource')) {
      const v = String(body.trading.priceSource || '').toUpperCase();
      if (!['LAST', 'MARK'].includes(v)) {
        errors.push(toError('priceSource chi nhan LAST hoac MARK', 'trading.priceSource'));
      }
    }
  }

  const mergedTrading = { ...runtimeConfig.trading, ...(body.trading || {}) };
  const mergedRisk = { ...runtimeConfig.risk, ...(body.risk || {}) };
  const mergedPositionSizing = { ...runtimeConfig.positionSizing, ...(body.positionSizing || {}) };

  if (mergedTrading.autoThreshold <= mergedTrading.signalThreshold) {
    errors.push(toError('AUTO_THRESHOLD phai lon hon SIGNAL_THRESHOLD', 'trading.autoThreshold'));
  }
  if (mergedTrading.semiThreshold < mergedTrading.signalThreshold) {
    errors.push(toError('SEMI_THRESHOLD phai >= SIGNAL_THRESHOLD', 'trading.semiThreshold'));
  }
  if (mergedTrading.liveAnalyzeIntervalMs < 500) {
    errors.push(toError('liveAnalyzeIntervalMs phai >= 500', 'trading.liveAnalyzeIntervalMs'));
  }
  if (mergedTrading.preSignalMinProbability < 0 || mergedTrading.preSignalMinProbability > 100) {
    errors.push(toError('preSignalMinProbability phai trong [0,100]', 'trading.preSignalMinProbability'));
  }
  if (
    mergedTrading.preSignalPack1MinProbability < 0 ||
    mergedTrading.preSignalPack1MinProbability > 100
  ) {
    errors.push(
      toError('preSignalPack1MinProbability phai trong [0,100]', 'trading.preSignalPack1MinProbability'),
    );
  }
  if (mergedTrading.preSignalPack2MinScore < 0 || mergedTrading.preSignalPack2MinScore > 100) {
    errors.push(toError('preSignalPack2MinScore phai trong [0,100]', 'trading.preSignalPack2MinScore'));
  }
  if (mergedTrading.preSignalWatchDistancePct < 0) {
    errors.push(toError('preSignalWatchDistancePct phai >= 0', 'trading.preSignalWatchDistancePct'));
  }
  if (mergedTrading.preSignalArmDistancePct < 0) {
    errors.push(toError('preSignalArmDistancePct phai >= 0', 'trading.preSignalArmDistancePct'));
  }
  if (mergedTrading.preSignalWatchDistancePct < mergedTrading.preSignalArmDistancePct) {
    errors.push(
      toError(
        'preSignalWatchDistancePct phai >= preSignalArmDistancePct',
        'trading.preSignalWatchDistancePct',
      ),
    );
  }
  if (mergedTrading.preSignalEmitIntervalMs < 1000) {
    errors.push(toError('preSignalEmitIntervalMs phai >= 1000', 'trading.preSignalEmitIntervalMs'));
  }
  if (mergedTrading.minRewardPct < 0) {
    errors.push(toError('minRewardPct phai >= 0', 'trading.minRewardPct'));
  }
  if (mergedTrading.nearStrongLevelPct < 0) {
    errors.push(toError('nearStrongLevelPct phai >= 0', 'trading.nearStrongLevelPct'));
  }
  if (mergedTrading.zoneSignalWindowMs < 10000) {
    errors.push(toError('zoneSignalWindowMs phai >= 10000', 'trading.zoneSignalWindowMs'));
  }
  if (mergedTrading.zoneSignalCooldownMs < 1000) {
    errors.push(toError('zoneSignalCooldownMs phai >= 1000', 'trading.zoneSignalCooldownMs'));
  }
  if (mergedTrading.maxSignalsPerZoneWindow < 1) {
    errors.push(toError('maxSignalsPerZoneWindow phai >= 1', 'trading.maxSignalsPerZoneWindow'));
  }
  if (mergedTrading.globalRankingWindowMs < 1000) {
    errors.push(toError('globalRankingWindowMs phai >= 1000', 'trading.globalRankingWindowMs'));
  }
  if (mergedTrading.globalRankingTopN < 1) {
    errors.push(toError('globalRankingTopN phai >= 1', 'trading.globalRankingTopN'));
  }
  if (mergedTrading.globalRankingMinRank < 0 || mergedTrading.globalRankingMinRank > 100) {
    errors.push(toError('globalRankingMinRank phai trong [0,100]', 'trading.globalRankingMinRank'));
  }
  if (mergedTrading.globalRankingDispatchCooldownMs < 1000) {
    errors.push(
      toError('globalRankingDispatchCooldownMs phai >= 1000', 'trading.globalRankingDispatchCooldownMs'),
    );
  }
  if (mergedTrading.adaptiveSetupLookbackTrades < 20) {
    errors.push(toError('adaptiveSetupLookbackTrades phai >= 20', 'trading.adaptiveSetupLookbackTrades'));
  }
  if (mergedTrading.adaptiveSetupMinTrades < 3) {
    errors.push(toError('adaptiveSetupMinTrades phai >= 3', 'trading.adaptiveSetupMinTrades'));
  }
  if (mergedTrading.adaptiveSetupMaxBonus < 0) {
    errors.push(toError('adaptiveSetupMaxBonus phai >= 0', 'trading.adaptiveSetupMaxBonus'));
  }
  if (mergedTrading.adaptiveSetupMaxPenalty < 0) {
    errors.push(toError('adaptiveSetupMaxPenalty phai >= 0', 'trading.adaptiveSetupMaxPenalty'));
  }
  if (mergedTrading.executionSimMinLiquidity < 0 || mergedTrading.executionSimMinLiquidity > 100) {
    errors.push(toError('executionSimMinLiquidity phai trong [0,100]', 'trading.executionSimMinLiquidity'));
  }
  if (mergedTrading.overboughtLongRsi <= mergedTrading.oversoldShortRsi) {
    errors.push(toError('overboughtLongRsi phai lon hon oversoldShortRsi', 'trading.overboughtLongRsi'));
  }
  if (mergedTrading.dynamicLeverageMin < 1) {
    errors.push(toError('dynamicLeverageMin phai >= 1', 'trading.dynamicLeverageMin'));
  }
  if (mergedTrading.dynamicLeverageMax < mergedTrading.dynamicLeverageMin) {
    errors.push(
      toError('dynamicLeverageMax phai >= dynamicLeverageMin', 'trading.dynamicLeverageMax'),
    );
  }
  if (mergedTrading.leverageConfidenceStep < 1) {
    errors.push(toError('leverageConfidenceStep phai >= 1', 'trading.leverageConfidenceStep'));
  }

  if (body.positionSizing && isObject(body.positionSizing)) {
    const numericSizing = [
      'highLiqMinNotionalUSDT',
      'highLiqMaxNotionalUSDT',
      'scpMinNotionalUSDT',
      'scpMaxNotionalUSDT',
    ];
    for (const key of numericSizing) {
      if (Object.prototype.hasOwnProperty.call(body.positionSizing, key)) {
        const value = body.positionSizing[key];
        if (typeof value !== 'number' || !Number.isFinite(value) || value < 0) {
          errors.push(toError(`${key} phai la so khong am`, `positionSizing.${key}`));
        }
      }
    }
    if (
      Object.prototype.hasOwnProperty.call(body.positionSizing, 'enforceNotionalRange') &&
      typeof body.positionSizing.enforceNotionalRange !== 'boolean'
    ) {
      errors.push(toError('enforceNotionalRange phai la boolean', 'positionSizing.enforceNotionalRange'));
    }
    const listFields = ['highLiqSymbols', 'scpSymbols'];
    for (const key of listFields) {
      if (
        Object.prototype.hasOwnProperty.call(body.positionSizing, key) &&
        !Array.isArray(body.positionSizing[key])
      ) {
        errors.push(toError(`${key} phai la array`, `positionSizing.${key}`));
      }
    }
  }

  if (mergedPositionSizing.highLiqMaxNotionalUSDT < mergedPositionSizing.highLiqMinNotionalUSDT) {
    errors.push(
      toError(
        'highLiqMaxNotionalUSDT phai >= highLiqMinNotionalUSDT',
        'positionSizing.highLiqMaxNotionalUSDT',
      ),
    );
  }
  if (mergedPositionSizing.scpMaxNotionalUSDT < mergedPositionSizing.scpMinNotionalUSDT) {
    errors.push(
      toError('scpMaxNotionalUSDT phai >= scpMinNotionalUSDT', 'positionSizing.scpMaxNotionalUSDT'),
    );
  }
  if (mergedRisk.minRiskPerTradePct > mergedRisk.defaultRiskPerTradePct) {
    errors.push(toError('minRiskPerTradePct phai <= defaultRiskPerTradePct', 'risk.minRiskPerTradePct'));
  }
  if (mergedRisk.defaultRiskPerTradePct > mergedRisk.maxRiskPerTradePct) {
    errors.push(toError('defaultRiskPerTradePct phai <= maxRiskPerTradePct', 'risk.defaultRiskPerTradePct'));
  }
  if (mergedRisk.maxRiskPerTradePct > 0.05) {
    errors.push(toError('maxRiskPerTradePct khong duoc vuot 0.05', 'risk.maxRiskPerTradePct'));
  }
  if (!['PAUSE', 'KEEP', 'CLOSE'].includes(String(mergedRisk.syncUnknownPositionAction || '').toUpperCase())) {
    errors.push(
      toError('syncUnknownPositionAction chi nhan PAUSE | KEEP | CLOSE', 'risk.syncUnknownPositionAction'),
    );
  }
  if (mergedRisk.clusterWindowMin < 10) {
    errors.push(toError('clusterWindowMin phai >= 10', 'risk.clusterWindowMin'));
  }
  if (mergedRisk.clusterMaxSameSideTrades < 1) {
    errors.push(toError('clusterMaxSameSideTrades phai >= 1', 'risk.clusterMaxSameSideTrades'));
  }
  if (mergedRisk.clusterMaxConsecutiveLosses < 1) {
    errors.push(toError('clusterMaxConsecutiveLosses phai >= 1', 'risk.clusterMaxConsecutiveLosses'));
  }
  if (mergedRisk.clusterPauseAfterLossMin < 0) {
    errors.push(toError('clusterPauseAfterLossMin phai >= 0', 'risk.clusterPauseAfterLossMin'));
  }

  return {
    ok: errors.length === 0,
    errors,
  };
}

module.exports = {
  validateModeBody,
  validateTradeBody,
  validateTelegramTestBody,
  validateConfigBody,
};
