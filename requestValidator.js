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
      'preSignalWatchDistancePct',
      'preSignalArmDistancePct',
      'preSignalEmitIntervalMs',
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
      Object.prototype.hasOwnProperty.call(body.trading, 'dynamicLeverageEnabled') &&
      typeof body.trading.dynamicLeverageEnabled !== 'boolean'
    ) {
      errors.push(toError('dynamicLeverageEnabled phai la boolean', 'trading.dynamicLeverageEnabled'));
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

  return {
    ok: errors.length === 0,
    errors,
  };
}

module.exports = {
  validateModeBody,
  validateTradeBody,
  validateConfigBody,
};
