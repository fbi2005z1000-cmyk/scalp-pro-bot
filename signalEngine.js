const IndicatorService = require('./indicator');
const CandleAnalyzer = require('./candleAnalyzer');
const { average, detectPriceStructure, clamp } = require('./utils');
const SupportResistanceService = require('./supportResistance');
const PriceActionEngine = require('./priceActionEngine');

class SignalEngine {
  constructor(config, logger) {
    this.config = config;
    this.logger = logger;
  }

  addReject(rejects, rejectCodes, code, message) {
    rejects.push(message);
    rejectCodes.push(code);
  }

  isHardRejectCode(code) {
    const hardCodes = new Set([
      'NEAR_STRONG_RESISTANCE',
      'NEAR_STRONG_SUPPORT',
      'LOW_REWARD_DISTANCE',
      'EXTENDED_FROM_MA25',
      'OVERBOUGHT_NEAR_RESISTANCE',
      'OVERSOLD_NEAR_SUPPORT',
      'LATE_ENTRY',
      'BAD_PULLBACK',
      'WEAK_PULLBACK_STRUCTURE',
      'PRICE_ACTION_BLOCK',
      'CANDLE_CONFIRM_MISSING',
      'FOMO_STREAK',
      'FOMO_DISTANCE_MA25',
      'FOMO_BREAKOUT',
      'BOS_AGAINST_TREND',
      'WEAK_TREND_STRENGTH',
      'BOLLINGER_SQUEEZE',
      'MACD_CONFLICT',
      'MICRO_1M_NOT_CONFIRMED',
      'LOW_MARKET_QUALITY',
      'LOW_MARKET_QUALITY_CONTEXT',
      'STRICT_QUALITY_GATE',
    ]);
    return hardCodes.has(code);
  }

  estimateEtaSec(currentPrice, triggerPrice, priceSpeedPerMin) {
    if (!Number.isFinite(currentPrice) || !Number.isFinite(triggerPrice) || currentPrice <= 0) return null;
    const distancePct = Math.abs((triggerPrice - currentPrice) / currentPrice);
    if (!distancePct) return 0;
    const speed = Math.abs(Number(priceSpeedPerMin || 0));
    if (!Number.isFinite(speed) || speed < 0.00005) return null;
    return clamp(Math.round((distancePct / speed) * 60), 8, 2700);
  }

  computePreviewPlan(side, triggerPrice, atr) {
    if (!Number.isFinite(triggerPrice) || triggerPrice <= 0) return null;
    const riskDistance = Math.max(triggerPrice * 0.0012, Number(atr || 0) * 0.75, triggerPrice * 0.0008);
    const stopLoss = side === 'LONG' ? triggerPrice - riskDistance : triggerPrice + riskDistance;
    const tp1 = side === 'LONG' ? triggerPrice + riskDistance : triggerPrice - riskDistance;
    const tp2 = side === 'LONG' ? triggerPrice + riskDistance * 2 : triggerPrice - riskDistance * 2;
    const tp3 = side === 'LONG' ? triggerPrice + riskDistance * 3 : triggerPrice - riskDistance * 3;
    return { stopLoss, tp1, tp2, tp3, rr: 2 };
  }

  buildPreSignalCandidate({
    side,
    evalResult,
    trend,
    currentPrice,
    ma25,
    atr,
    potentialZone,
    pressure,
    rsi,
    reasons,
    keyLevels,
  }) {
    if (!this.config.trading.preSignalEnabled) return null;
    if (!evalResult || !Number.isFinite(currentPrice) || currentPrice <= 0) return null;

    const trendOk = side === 'LONG' ? trend === 'UP' : trend === 'DOWN';
    if (!trendOk) return null;

    const hardBlockCode = (evalResult.hardRejectCodes || []).find((code) =>
      ['NEAR_STRONG_RESISTANCE', 'NEAR_STRONG_SUPPORT', 'LOW_REWARD_DISTANCE'].includes(code),
    );
    if (hardBlockCode) return null;

    const triggerPrice =
      Number(potentialZone?.price) ||
      (Number.isFinite(ma25) ? ma25 : currentPrice);
    if (!Number.isFinite(triggerPrice) || triggerPrice <= 0) return null;

    const distancePct = Math.abs((triggerPrice - currentPrice) / currentPrice);
    if (distancePct > this.config.trading.preSignalWatchDistancePct * 1.9) return null;

    const armPct = this.config.trading.preSignalArmDistancePct;
    const watchPct = this.config.trading.preSignalWatchDistancePct;
    const mode = distancePct <= armPct ? 'ARMED' : distancePct <= watchPct ? 'READY_SOON' : 'EARLY_WATCH';

    const zoneScore = Number(potentialZone?.score || 0);
    const baseScore = clamp(Number(evalResult.score || 0), 0, 100);
    const pressureAlign =
      side === 'LONG'
        ? Number(pressure?.buyPressure || 0) - Number(pressure?.sellPressure || 0)
        : Number(pressure?.sellPressure || 0) - Number(pressure?.buyPressure || 0);
    const pressureScore = clamp(50 + pressureAlign, 0, 100);
    const distanceScore = clamp(100 - distancePct * 15000, 0, 100);

    const rsiFit =
      side === 'LONG'
        ? rsi >= 30 && rsi <= 50
          ? 100
          : rsi >= 26 && rsi <= 58
          ? 74
          : 42
        : rsi >= 50 && rsi <= 70
        ? 100
        : rsi >= 42 && rsi <= 74
        ? 74
        : 42;

    let probability = Math.round(
      baseScore * 0.45 + zoneScore * 0.28 + distanceScore * 0.17 + pressureScore * 0.06 + rsiFit * 0.04,
    );

    if (mode === 'EARLY_WATCH') probability = Math.min(probability, 88);
    if (mode === 'READY_SOON') probability = Math.min(probability, 93);
    if (mode === 'ARMED') probability = Math.min(probability, 96);

    if (side === 'LONG' && keyLevels?.nearStrongResistance) probability = Math.min(probability, 74);
    if (side === 'SHORT' && keyLevels?.nearStrongSupport) probability = Math.min(probability, 74);
    probability = clamp(probability, 35, 96);

    if (probability < this.config.trading.preSignalMinProbability) return null;

    const etaSec = this.estimateEtaSec(currentPrice, triggerPrice, pressure?.priceSpeed);
    const plan = this.computePreviewPlan(side, triggerPrice, atr);
    if (!plan) return null;

    const topReasons = (reasons || []).slice(0, 3);
    if (potentialZone?.id) {
      topReasons.unshift(
        `${side === 'LONG' ? 'Gần hỗ trợ tiềm năng' : 'Gần kháng cự tiềm năng'} (${mode})`,
      );
    }

    const leverage = this.computeDynamicLeverage({
      score: probability,
      rr: plan.rr,
      volatilityRegime: evalResult?.volatilityRegime || null,
      rejectCodes: evalResult?.rejectCodes || [],
      side,
      marketContext: keyLevels || {},
    });

    return {
      side,
      mode,
      probability,
      triggerPrice,
      distancePct,
      etaSec,
      stopLoss: plan.stopLoss,
      tp1: plan.tp1,
      tp2: plan.tp2,
      tp3: plan.tp3,
      rr: plan.rr,
      leverage,
      takerFeePct: this.config.trading.takerFeePct,
      reasons: topReasons.slice(0, 4),
      source: potentialZone ? 'SR_ZONE' : 'MA25',
      zoneId: potentialZone?.id || null,
    };
  }

  selectPack2Eval({ trend, longEval, shortEval }) {
    if (trend === 'UP') return longEval;
    if (trend === 'DOWN') return shortEval;
    return Number(longEval?.score || 0) >= Number(shortEval?.score || 0) ? longEval : shortEval;
  }

  buildDualPreSignalPacks({
    trend,
    longCandidate,
    shortCandidate,
    longEval,
    shortEval,
  }) {
    const modeRank = { ARMED: 3, READY_SOON: 2, EARLY_WATCH: 1 };
    const rankedCandidates = [longCandidate, shortCandidate]
      .filter(Boolean)
      .sort((a, b) => {
        if ((modeRank[b.mode] || 0) !== (modeRank[a.mode] || 0)) {
          return (modeRank[b.mode] || 0) - (modeRank[a.mode] || 0);
        }
        if (b.probability !== a.probability) return b.probability - a.probability;
        return Number(a.distancePct || 0) - Number(b.distancePct || 0);
      });

    const pack1MinProb = Number(
      this.config.trading.preSignalPack1MinProbability || this.config.trading.preSignalMinProbability || 60,
    );
    const pack1Candidate = rankedCandidates[0] || null;
    const pack1 =
      pack1Candidate && Number(pack1Candidate.probability || 0) >= pack1MinProb
        ? {
            ok: true,
            name: 'PACK_1',
            gate: 'EARLY_SCOUT',
            ...pack1Candidate,
          }
        : {
            ok: false,
            name: 'PACK_1',
            gate: 'EARLY_SCOUT',
            side: pack1Candidate?.side || null,
            probability: Number(pack1Candidate?.probability || 0),
            reason: pack1Candidate ? `Prob thấp hơn ngưỡng PACK_1 (${pack1MinProb})` : 'Chưa có setup PACK_1',
          };

    const pack2MinScore = Number(this.config.trading.preSignalPack2MinScore || 68);
    const pack2Eval = this.selectPack2Eval({ trend, longEval, shortEval });
    const pack2Side = String(pack2Eval?.side || '').toUpperCase();
    const pack2Candidate = pack2Side === 'LONG' ? longCandidate : pack2Side === 'SHORT' ? shortCandidate : null;
    const criticalRejectSet = new Set([
      'LOW_VOLUME',
      'WEAK_ORDERFLOW',
      'HIGH_SPREAD',
      'LOW_VOLATILITY',
      'BOLLINGER_SQUEEZE',
      'MACD_CONFLICT',
      'STRICT_QUALITY_GATE',
      'NEAR_STRONG_RESISTANCE',
      'NEAR_STRONG_SUPPORT',
      'LOW_REWARD_DISTANCE',
    ]);
    const blockedByCritical = Array.isArray(pack2Eval?.rejectCodes)
      ? pack2Eval.rejectCodes.filter((code) => criticalRejectSet.has(String(code || '').toUpperCase()))
      : [];
    const pack2Score = Number(pack2Eval?.score || 0);
    const pack2Ok =
      Boolean(pack2Eval) &&
      !pack2Eval.hardBlocked &&
      pack2Score >= pack2MinScore &&
      blockedByCritical.length === 0 &&
      Boolean(pack2Candidate);

    const pack2Mode =
      pack2Score >= Number(this.config.trading.autoThreshold || 75)
        ? 'ARMED'
        : pack2Score >= Number(this.config.trading.semiThreshold || 61)
        ? 'READY_SOON'
        : 'EARLY_WATCH';

    const pack2 = pack2Ok
      ? {
          ok: true,
          name: 'PACK_2',
          gate: 'STRICT_CONFIRM',
          ...pack2Candidate,
          mode: pack2Mode,
          probability: clamp(Math.round(pack2Score), 0, 99),
          reasons: (pack2Eval.reasons || []).slice(0, 4),
        }
      : {
          ok: false,
          name: 'PACK_2',
          gate: 'STRICT_CONFIRM',
          side: pack2Side || null,
          probability: clamp(Math.round(pack2Score), 0, 99),
          reason:
            !pack2Eval
              ? 'Chưa có setup PACK_2'
              : pack2Eval.hardBlocked
              ? `PACK_2 bị hard-block: ${(pack2Eval.hardRejectCodes || []).join(', ') || 'UNKNOWN'}`
              : blockedByCritical.length
              ? `PACK_2 bị chặn bởi reject critical: ${blockedByCritical.join(', ')}`
              : !pack2Candidate
              ? 'PACK_2 chưa có trigger zone/MA25 phù hợp'
              : `Score PACK_2 thấp hơn ngưỡng (${pack2MinScore})`,
        };

    const requireConsensus = this.config.trading.preSignalRequireConsensus !== false;
    const sameSide = Boolean(pack1.ok && pack2.ok && pack1.side === pack2.side);

    let consensusOk = false;
    let selected = null;
    let reason = 'NO_PACK_PASS';

    if (requireConsensus) {
      if (sameSide) {
        consensusOk = true;
        selected = {
          ...pack2,
          mode: (modeRank[pack2.mode] || 0) >= (modeRank[pack1.mode] || 0) ? pack2.mode : pack1.mode,
          probability: clamp(Math.round(Number(pack2.probability || 0) * 0.6 + Number(pack1.probability || 0) * 0.4), 0, 99),
          reasons: Array.from(new Set([...(pack2.reasons || []), ...(pack1.reasons || [])])).slice(0, 4),
          source: 'PACK_1+PACK_2',
        };
        reason = 'PACK_CONSENSUS_OK';
      } else {
        reason = 'PACK_CONSENSUS_MISMATCH';
      }
    } else if (pack2.ok) {
      consensusOk = true;
      selected = { ...pack2, source: 'PACK_2' };
      reason = 'PACK_2_OK';
    } else if (pack1.ok) {
      consensusOk = true;
      selected = { ...pack1, source: 'PACK_1' };
      reason = 'PACK_1_OK';
    }

    return {
      pack1,
      pack2,
      consensus: {
        ok: consensusOk,
        reason,
        sameSide,
        requireConsensus,
        side: selected?.side || null,
      },
      selected,
    };
  }

  buildPreSignal({
    currentPrice,
    trendResult,
    ind2m,
    marketContext,
    longEval,
    shortEval,
    supportResistance,
    keyLevels,
  }) {
    if (!this.config.trading.preSignalEnabled) return null;

    const longCandidate = this.buildPreSignalCandidate({
      side: 'LONG',
      evalResult: longEval,
      trend: trendResult.trend,
      currentPrice,
      ma25: ind2m.latest.ma25,
      atr: ind2m.latest.atr14,
      potentialZone: supportResistance?.potential?.longZone || null,
      pressure: marketContext.pressure,
      rsi: ind2m.latest.rsi14,
      reasons: longEval.reasons,
      keyLevels,
    });

    const shortCandidate = this.buildPreSignalCandidate({
      side: 'SHORT',
      evalResult: shortEval,
      trend: trendResult.trend,
      currentPrice,
      ma25: ind2m.latest.ma25,
      atr: ind2m.latest.atr14,
      potentialZone: supportResistance?.potential?.shortZone || null,
      pressure: marketContext.pressure,
      rsi: ind2m.latest.rsi14,
      reasons: shortEval.reasons,
      keyLevels,
    });

    const dualPacks = this.buildDualPreSignalPacks({
      trend: trendResult.trend,
      longCandidate,
      shortCandidate,
      longEval,
      shortEval,
    });

    const selected = dualPacks.selected;

    return {
      selected,
      long: longCandidate,
      short: shortCandidate,
      packs: {
        pack1: dualPacks.pack1,
        pack2: dualPacks.pack2,
      },
      consensus: dualPacks.consensus,
      generatedAt: Date.now(),
    };
  }

  analyze({ symbol, candles1m, candles2m, candles5m, candles15m, orderBook, analysisTimeframe }) {
    const analysisTf = analysisTimeframe || this.config.timeframe.analysis || '3m';
    const candles15 = candles15m && candles15m.length ? candles15m : candles5m;
    const minAnalysis = Math.max(40, Number(this.config.advanced?.minCandlesAnalysis || 80));
    const minTrend5 = Math.max(40, Number(this.config.advanced?.minCandlesTrend5m || 80));
    const minTrend15 = Math.max(30, Number(this.config.advanced?.minCandlesTrend15m || 40));

    if (candles2m.length < minAnalysis || candles5m.length < minTrend5 || candles15.length < minTrend15) {
      return this.noTrade(symbol, 'Thiếu dữ liệu nến để phân tích an toàn', {
        code: 'INSUFFICIENT_DATA',
        diagnostics: {
          candles2m: candles2m.length,
          candles5m: candles5m.length,
          candles15m: candles15.length,
          minAnalysis,
          minTrend5,
          minTrend15,
        },
      });
    }

    const source1m = candles1m && candles1m.length ? candles1m : candles2m;
    const ind1m = IndicatorService.calculate(source1m);
    const ind2m = IndicatorService.calculate(candles2m);
    const ind5m = IndicatorService.calculate(candles5m);
    const ind15m = IndicatorService.calculate(candles15);
    const candleInsight1m = CandleAnalyzer.analyze(source1m);
    const candleInsight = CandleAnalyzer.analyze(candles2m);
    const structure1m = detectPriceStructure(source1m, 8);
    const structure2m = detectPriceStructure(candles2m, 8);
    const structure5m = detectPriceStructure(candles5m, 8);

    const trendResult = this.detectTrend5m(candles5m, ind5m);
    const trend15mResult = this.detectTrend15m(candles15, ind15m);
    const sidewayResult = this.detectSideway(candles2m, ind2m, candleInsight);

    const latest2 = candles2m[candles2m.length - 1];
    const prev2 = candles2m[candles2m.length - 2];
    const supportResistance = SupportResistanceService.analyze({
      candlesByTimeframe: {
        '1m': candles1m,
        [analysisTf]: candles2m,
        '5m': candles5m,
        '15m': candles15,
      },
      currentPrice: latest2.close,
      trend5m: trendResult.trend,
      rsi: ind2m.latest.rsi14,
      candlePatterns: candleInsight.patterns,
    });
    const keyLevels =
      supportResistance?.keyLevels?.support || supportResistance?.keyLevels?.resistance
        ? supportResistance.keyLevels
        : this.detectKeyLevels(candles2m, latest2.close);
    const nearStrongResistance = Boolean(
      keyLevels?.nearStrongResistance ||
        (keyLevels?.distToResistancePct !== null &&
          keyLevels?.distToResistancePct <= this.config.trading.nearStrongLevelPct),
    );
    const nearStrongSupport = Boolean(
      keyLevels?.nearStrongSupport ||
        (keyLevels?.distToSupportPct !== null &&
          keyLevels?.distToSupportPct <= this.config.trading.nearStrongLevelPct),
    );
    const distanceToMA25 =
      ind2m.latest.ma25 && ind2m.latest.ma25 !== 0
        ? Math.abs(latest2.close - ind2m.latest.ma25) / ind2m.latest.ma25
        : 0;
    const priceAction = PriceActionEngine.analyze({
      candles: candles2m,
      trendHint: trendResult.trend,
      ma25: ind2m.latest.ma25,
      rsi: ind2m.latest.rsi14,
      supportResistance,
      candleInsight,
      volumeStatus: ind2m.volumeStatus,
      config: this.config,
    });

    const spreadPct = orderBook.spreadPct || 0;
    const atrPct2m = ind2m.latest.atr14 ? ind2m.latest.atr14 / latest2.close : 0;
    const volatilityRegime = this.detectVolatilityRegime(atrPct2m);

    const currentSession = this.detectSession();
    const sessionAllowed = this.isSessionAllowed(currentSession);
    const marketQuality = this.assessMarketQuality({
      trendResult,
      trend15mResult,
      sidewayResult,
      ind2m,
      ind5m,
      ind15m,
      spreadPct,
    });

    const marketContext = {
      trend: trendResult.trend,
      trend15m: trend15mResult.trend,
      sideway: sidewayResult.sideway,
      sidewayReasons: sidewayResult.reasons,
      structure2m,
      structure5m,
      pressure: ind2m.pressure,
      volumeStatus: ind2m.volumeStatus,
      rsi2m: ind2m.latest.rsi14,
      rsi5m: ind5m.latest.rsi14,
      atr2m: ind2m.latest.atr14,
      atrPct2m,
      volatilityRegime,
      session: currentSession,
      sessionAllowed,
      ma2m: ind2m.latest,
      ma5m: ind5m.latest,
      ma15m: ind15m.latest,
      ma1m: ind1m.latest,
      adx2m: ind2m.latest.adx,
      plusDI2m: ind2m.latest.plusDI,
      minusDI2m: ind2m.latest.minusDI,
      bbBandwidth2m: ind2m.latest.bbBandwidth,
      macd2m: ind2m.latest.macd,
      macdSignal2m: ind2m.latest.macdSignal,
      macdHist2m: ind2m.latest.macdHist,
      spreadPct,
      distanceToMA25,
      keyLevels,
      nearStrongResistance,
      nearStrongSupport,
      supportResistance,
      priceAction,
      marketQuality,
      lastPrice: latest2.close,
      analysisTimeframe: analysisTf,
    };

    if (trendResult.trend === 'SIDEWAY' || sidewayResult.sideway) {
      return this.noTrade(symbol, 'THỊ TRƯỜNG ĐI NGANG - KHÔNG NÊN VÀO', {
        code: 'SIDEWAY',
        market: marketContext,
        diagnostics: {
          sidewayReasons: sidewayResult.reasons,
        },
      });
    }

    if (this.config.advanced.sessionFilterEnabled && !sessionAllowed && this.config.advanced.strictSessionReject) {
      return this.noTrade(symbol, `Phiên ${currentSession} ngoài khung scalp ưu tiên`, {
        code: 'SESSION_FILTER',
        market: marketContext,
      });
    }

    if ((marketQuality?.score || 0) < Number(this.config.trading.marketQualityMinScore || 52)) {
      return this.noTrade(symbol, 'Chất lượng thị trường thấp, đứng ngoài để tránh lệnh rác', {
        code: 'LOW_MARKET_QUALITY',
        market: marketContext,
        diagnostics: {
          marketQuality,
          threshold: Number(this.config.trading.marketQualityMinScore || 52),
        },
      });
    }

    const sharedInput = {
      latest2,
      prev2,
      candles2m,
      ind1m,
      ind2m,
      ind5m,
      ind15m,
      candleInsight1m,
      trendResult,
      trend15mResult,
      structure1m,
      candleInsight,
      structure2m,
      structure5m,
      spreadPct,
      distanceToMA25,
      atrPct2m,
      volatilityRegime,
      sessionAllowed,
      currentSession,
      keyLevels,
      supportResistance,
      priceAction,
      marketQuality,
    };

    const longEval = this.evaluateLong(sharedInput);
    const shortEval = this.evaluateShort(sharedInput);
    const preSignal = this.buildPreSignal({
      currentPrice: latest2.close,
      trendResult,
      ind2m,
      marketContext,
      longEval,
      shortEval,
      supportResistance,
      keyLevels,
    });

    let selected = longEval.score >= shortEval.score ? longEval : shortEval;
    if (trendResult.trend === 'UP') selected = longEval;
    if (trendResult.trend === 'DOWN') selected = shortEval;

    const requiredScore = this.computeRequiredScore(volatilityRegime, sessionAllowed, marketQuality?.score || 0);

    if (selected.hardBlocked) {
      return this.noTrade(
        symbol,
        `Từ chối vào lệnh: ${selected.hardRejectCodes.join(', ')}`,
        {
        code: selected.hardRejectCodes[0] || 'HARD_FILTER_BLOCK',
        market: marketContext,
        preSignal,
        diagnostics: {
          requiredScore,
          longScore: longEval.score,
          shortScore: shortEval.score,
            longBreakdown: longEval.breakdown,
            shortBreakdown: shortEval.breakdown,
            rejects: selected.rejects,
            rejectCodes: selected.rejectCodes,
            hardRejectCodes: selected.hardRejectCodes,
          },
        },
      );
    }

    if (!selected.allowed || selected.score < requiredScore) {
      return this.noTrade(symbol, 'Điều kiện chưa đủ đẹp để vào lệnh', {
        code: 'LOW_QUALITY',
        market: marketContext,
        preSignal,
        diagnostics: {
          requiredScore,
          longScore: longEval.score,
          shortScore: shortEval.score,
          longBreakdown: longEval.breakdown,
          shortBreakdown: shortEval.breakdown,
          rejects: [...new Set([...longEval.rejects, ...shortEval.rejects])].slice(0, 18),
          rejectCodes: [...new Set([...(longEval.rejectCodes || []), ...(shortEval.rejectCodes || [])])],
        },
      });
    }

    const tradePlan = this.buildTradePlan({
      side: selected.side,
      candles2m,
      ind2m,
      entryPrice: latest2.close,
      score: selected.score,
      distanceToMA25,
      keyLevels,
    });

    if (!tradePlan.valid) {
      return this.noTrade(symbol, tradePlan.reason, {
        code: 'INVALID_TRADE_PLAN',
        market: marketContext,
        preSignal,
        diagnostics: {
          breakdown: selected.breakdown,
          rejects: selected.rejects,
          rejectCodes: selected.rejectCodes || [],
        },
      });
    }

    const finalScore = clamp(selected.score - tradePlan.penalty, 0, 100);
    const breakdown = {
      ...selected.breakdown,
      planPenalty: -Math.abs(tradePlan.penalty || 0),
      finalScore,
    };

    const level = this.getSignalLevel(finalScore);
    const recommendedLeverage = this.computeDynamicLeverage({
      score: finalScore,
      rr: tradePlan.rr,
      volatilityRegime,
      rejectCodes: selected.rejectCodes || [],
      side: selected.side,
      marketContext,
    });

    return {
      symbol,
      side: selected.side,
      action: selected.side,
      level,
      confidence: finalScore,
      leverage: recommendedLeverage,
      takerFeePct: this.config.trading.takerFeePct,
      makerFeePct: this.config.trading.makerFeePct,
      confidenceBreakdown: breakdown,
      requiredScore,
      trend5m: trendResult.trend,
      trend15m: trend15mResult.trend,
      volatilityRegime,
      session: currentSession,
      signalTimeframe: analysisTf,
      entryMin: tradePlan.entryMin,
      entryMax: tradePlan.entryMax,
      entryPrice: tradePlan.entryPrice,
      stopLoss: tradePlan.stopLoss,
      tp1: tradePlan.tp1,
      tp2: tradePlan.tp2,
      tp3: tradePlan.tp3,
      rr: tradePlan.rr,
      rewardToResistancePct: tradePlan.rewardToResistancePct,
      rewardToSupportPct: tradePlan.rewardToSupportPct,
      buyPressure: ind2m.pressure.buyPressure,
      sellPressure: ind2m.pressure.sellPressure,
      pressureLevel: selected.side === 'LONG' ? ind2m.pressure.buyLevel : ind2m.pressure.sellLevel,
      volumeStatus: ind2m.volumeStatus.status,
      candleSummary: candleInsight.summary,
      trendReason: trendResult.reasons,
      reasons: selected.reasons,
      risks: selected.risks,
      rejects: selected.rejects,
      rejectCodes: selected.rejectCodes,
      market: marketContext,
      priceAction,
      diagnostics: {
        requiredScore,
        longScore: longEval.score,
        shortScore: shortEval.score,
        longBreakdown: longEval.breakdown,
        shortBreakdown: shortEval.breakdown,
      },
      preSignal: {
        ...(preSignal || {}),
        selected: {
          side: selected.side,
          mode: 'ENTRY_NOW',
          probability: clamp(Math.max(finalScore, this.config.trading.preSignalMinProbability), 0, 99),
          triggerPrice: tradePlan.entryPrice,
          distancePct: 0,
          etaSec: 0,
          stopLoss: tradePlan.stopLoss,
          tp1: tradePlan.tp1,
          tp2: tradePlan.tp2,
          tp3: tradePlan.tp3,
          rr: tradePlan.rr,
          leverage: recommendedLeverage,
          takerFeePct: this.config.trading.takerFeePct,
          reasons: selected.reasons.slice(0, 4),
          source: 'LIVE_SIGNAL',
          zoneId: null,
        },
      },
      newbieNote: this.buildNewbieNote({
        side: selected.side,
        rsi: ind2m.latest.rsi14,
        trend: trendResult.trend,
        volumeStatus: ind2m.volumeStatus.status,
        score: finalScore,
      }),
      createdAt: Date.now(),
    };
  }

  detectSession() {
    const h = new Date().getUTCHours();
    if (h >= 0 && h < 7) return 'ASIA';
    if (h >= 7 && h < 13) return 'EUROPE_OPEN';
    if (h >= 13 && h < 20) return 'US';
    if (h >= 20 && h <= 23) return 'US_LATE';
    return 'UNKNOWN';
  }

  isSessionAllowed(session) {
    if (!this.config.advanced.sessionFilterEnabled) return true;
    const norm = String(session || '').toUpperCase();
    return this.config.advanced.allowedSessions.some((s) => norm.includes(s));
  }

  detectVolatilityRegime(atrPct2m) {
    if (!atrPct2m || atrPct2m < this.config.advanced.minAtrPct2m) return 'LOW_VOL';
    if (atrPct2m > this.config.advanced.maxAtrPct2m) return 'HIGH_VOL';
    return 'NORMAL_VOL';
  }

  assessMarketQuality({ trendResult, trend15mResult, sidewayResult, ind2m, ind5m, ind15m, spreadPct }) {
    const reasons = [];
    let score = 70;

    if (trendResult?.trend === 'SIDEWAY' || sidewayResult?.sideway) {
      score -= 22;
      reasons.push('Market sideway/range mạnh');
    }

    const adx2m = Number(ind2m?.latest?.adx || 0);
    const adx5m = Number(ind5m?.latest?.adx || 0);
    const adx15m = Number(ind15m?.latest?.adx || 0);

    if (adx2m >= 20) score += 5;
    else score -= 6;
    if (adx5m >= 18) score += 4;
    else score -= 5;
    if (adx15m >= 16) score += 3;
    else score -= 4;

    const bb2m = Number(ind2m?.latest?.bbBandwidth || 0);
    const bb5m = Number(ind5m?.latest?.bbBandwidth || 0);
    if (bb2m <= this.config.trading.bbSqueezeMax) {
      score -= 7;
      reasons.push('Bollinger 2m squeeze');
    }
    if (bb5m <= this.config.trading.bbSqueezeMax * 1.1) {
      score -= 5;
      reasons.push('Bollinger 5m squeeze');
    }

    const volRatio = Number(ind2m?.volumeStatus?.ratio || 0);
    if (volRatio >= 1.05) score += 4;
    else if (volRatio < 0.85) {
      score -= 8;
      reasons.push('Volume thấp');
    }

    if (Number(spreadPct || 0) > this.config.trading.maxSpreadPct * 0.8) {
      score -= 7;
      reasons.push('Spread cao so với ngưỡng');
    }

    if (trendResult?.trend !== 'SIDEWAY' && trend15mResult?.trend !== 'SIDEWAY' && trendResult?.trend === trend15mResult?.trend) {
      score += 4;
    } else if (trend15mResult?.trend !== 'SIDEWAY' && trendResult?.trend !== trend15mResult?.trend) {
      score -= 6;
      reasons.push('Trend 5m/15m lệch nhau');
    }

    return {
      score: clamp(Math.round(score), 0, 100),
      reasons,
    };
  }

  computeRequiredScore(volatilityRegime, sessionAllowed, marketQualityScore = 100) {
    let threshold = this.config.trading.signalThreshold;

    if (this.config.advanced.dynamicThresholdByVolatility) {
      if (volatilityRegime === 'HIGH_VOL') threshold += 6;
      if (volatilityRegime === 'LOW_VOL') threshold += 4;
    }

    if (this.config.advanced.sessionFilterEnabled && !sessionAllowed) {
      threshold += this.config.advanced.strictSessionReject ? 100 : 6;
    }

    if (marketQualityScore < Number(this.config.trading.marketQualityMinScore || 52) + 8) {
      threshold += 6;
    } else if (marketQualityScore >= 75) {
      threshold -= 2;
    }

    return clamp(threshold, 55, 98);
  }

  computeDynamicLeverage({ score, rr, volatilityRegime, rejectCodes, side, marketContext }) {
    const fallback = Math.max(1, Number(this.config.binance.leverage || 10));
    if (this.config.trading.dynamicLeverageEnabled === false) return fallback;

    const minLev = Math.max(1, Number(this.config.trading.dynamicLeverageMin || 10));
    const maxLev = Math.max(minLev, Number(this.config.trading.dynamicLeverageMax || 20));
    const step = Math.max(1, Number(this.config.trading.leverageConfidenceStep || 3));
    const baseThreshold = Math.max(40, Number(this.config.trading.signalThreshold || 60));

    let leverage = minLev;
    const normalizedScore = clamp(Number(score || 0), 0, 100);
    if (normalizedScore > baseThreshold) {
      leverage += Math.floor((normalizedScore - baseThreshold) / step);
    }

    if (Number.isFinite(rr) && rr >= 2) leverage += 1;
    if (volatilityRegime === 'HIGH_VOL') leverage -= 2;
    if (volatilityRegime === 'LOW_VOL') leverage -= 1;

    const rejectSet = new Set(Array.isArray(rejectCodes) ? rejectCodes : []);
    if (rejectSet.has('LOW_VOLUME') || rejectSet.has('WEAK_ORDERFLOW')) leverage -= 1;
    if (rejectSet.has('HIGH_SPREAD') || rejectSet.has('HIGH_VOLATILITY')) leverage -= 1;

    const nearRes = Boolean(marketContext?.nearStrongResistance);
    const nearSup = Boolean(marketContext?.nearStrongSupport);
    if (side === 'LONG' && nearRes) leverage -= 2;
    if (side === 'SHORT' && nearSup) leverage -= 2;

    return clamp(Math.round(leverage), minLev, maxLev);
  }

  noTrade(symbol, reason, extra = {}) {
    const analysisTf = this.config.timeframe.analysis || '3m';
    const rejectCodes = extra.diagnostics?.rejectCodes || (extra.code ? [extra.code] : ['NO_TRADE']);
    return {
      symbol,
      side: 'NO_TRADE',
      action: 'NO_TRADE',
      level: 'WEAK',
      confidence: 0,
      confidenceBreakdown: {
        trend: 0,
        confluence15m: 0,
        pullback: 0,
        maMomentum: 0,
        rsi: 0,
        volume: 0,
        candle: 0,
        pressure: 0,
        structure: 0,
        volatility: 0,
        session: 0,
        riskPenalty: 0,
        noisePenalty: 0,
        finalScore: 0,
      },
      trend5m: (extra.market && extra.market.trend) || 'UNKNOWN',
      trend15m: (extra.market && extra.market.trend15m) || 'UNKNOWN',
      reasons: [reason],
      risks: [],
      rejects: extra.diagnostics?.rejects || [],
      rejectCodes,
      market: extra.market || {},
      diagnostics: extra.diagnostics || {},
      preSignal: extra.preSignal || null,
      rejectCode: extra.code || 'NO_TRADE',
      signalTimeframe: analysisTf,
      createdAt: Date.now(),
      newbieNote: 'Hiện chưa có kèo đủ đẹp, đứng ngoài để bảo toàn vốn.',
    };
  }

  newBreakdown() {
    return {
      trend: 0,
      confluence15m: 0,
      pullback: 0,
      maMomentum: 0,
      rsi: 0,
      volume: 0,
      candle: 0,
      pressure: 0,
      structure: 0,
      volatility: 0,
      session: 0,
      riskPenalty: 0,
      noisePenalty: 0,
    };
  }

  detectTrend5m(candles5m, ind5m) {
    const latest = candles5m[candles5m.length - 1];
    const close = latest.close;
    const { ma99, ma200, ma99Slope, rsi14 } = ind5m.latest;
    const structure = detectPriceStructure(candles5m, 10);

    const reasons = [];
    const maDistance = ma99 && ma200 ? Math.abs(ma99 - ma200) / close : 0;
    const nearEachOther = maDistance < 0.0015;

    const upCond = [
      close > (ma99 || close * 2),
      close > (ma200 || close * 2),
      (ma99 || 0) >= (ma200 || 0) || ma99Slope > 0,
      structure.structure !== 'BEARISH',
      (rsi14 || 50) > 45,
    ];

    const downCond = [
      close < (ma99 || 0),
      close < (ma200 || 0),
      (ma99 || 0) <= (ma200 || 0) || ma99Slope < 0,
      structure.structure !== 'BULLISH',
      (rsi14 || 50) < 55,
    ];

    const upCount = upCond.filter(Boolean).length;
    const downCount = downCond.filter(Boolean).length;

    if (nearEachOther) reasons.push('MA99 và MA200 đang quá gần nhau');
    if ((rsi14 || 50) >= 45 && (rsi14 || 50) <= 55) reasons.push('RSI 5m trung tính');
    if (structure.structure === 'RANGE') reasons.push('Cấu trúc 5m đi ngang');

    if (upCount >= 4 && !nearEachOther) {
      return {
        trend: 'UP',
        reasons: ['Giá 5m nằm trên MA99/MA200, cấu trúc nghiêng tăng'],
      };
    }

    if (downCount >= 4 && !nearEachOther) {
      return {
        trend: 'DOWN',
        reasons: ['Giá 5m nằm dưới MA99/MA200, cấu trúc nghiêng giảm'],
      };
    }

    return {
      trend: 'SIDEWAY',
      reasons: reasons.length ? reasons : ['Xu hướng 5m chưa rõ ràng'],
    };
  }

  detectTrend15m(candles15m, ind15m) {
    const latest = candles15m[candles15m.length - 1];
    const close = latest.close;
    const { ma99, ma200, ma99Slope } = ind15m.latest;

    const up = close > (ma99 || close * 2) && close > (ma200 || close * 2) && ((ma99 || 0) >= (ma200 || 0) || ma99Slope > 0);
    const down = close < (ma99 || 0) && close < (ma200 || 0) && ((ma99 || 0) <= (ma200 || 0) || ma99Slope < 0);

    if (up) return { trend: 'UP' };
    if (down) return { trend: 'DOWN' };
    return { trend: 'SIDEWAY' };
  }

  detectSideway(candles2m, ind2m, candleInsight) {
    const reasons = [];
    const latest = candles2m[candles2m.length - 1];

    const recent = candles2m.slice(-10);
    const ranges = recent.map((c) => c.high - c.low);
    const avgRange = average(ranges);
    const closeToMA25Count = recent.filter((c) => {
      if (!ind2m.latest.ma25) return false;
      return Math.abs(c.close - ind2m.latest.ma25) / c.close < 0.0012;
    }).length;

    const maSpread =
      ind2m.latest.ma99 && ind2m.latest.ma25
        ? Math.abs(ind2m.latest.ma25 - ind2m.latest.ma99) / latest.close
        : 0;

    const rsiSeries = ind2m.rsi14.filter((v) => v !== null).slice(-12);
    const midRsiCount = rsiSeries.filter((v) => v >= 40 && v <= 60).length;

    if (maSpread < 0.0015) reasons.push('MA25 và MA99 quá hẹp');
    if (closeToMA25Count >= 6) reasons.push('Giá cắt MA25 liên tục');
    if (midRsiCount >= Math.max(6, rsiSeries.length * 0.7)) reasons.push('RSI 2m lình xình vùng 40-60');
    if (avgRange / latest.close < 0.0014) reasons.push('Biên độ nến quá nhỏ');
    if (ind2m.volumeStatus.status === 'THẤP') reasons.push('Volume thấp');
    if (Math.abs(ind2m.pressure.imbalance || 0) < 6) reasons.push('Lực mua bán cân bằng, thiếu ưu thế');
    if (candleInsight.patterns.fakeBreakout || candleInsight.patterns.fakeBreakdown) {
      reasons.push('Nhiều dấu hiệu breakout giả');
    }

    return {
      sideway: reasons.length >= 3,
      reasons,
    };
  }

  detectKeyLevels(candles, price, lookback = 80) {
    const slice = candles.slice(-lookback);
    if (!slice.length) {
      return {
        support: null,
        resistance: null,
        distToSupportPct: null,
        distToResistancePct: null,
        nearestSupport: null,
        nearestResistance: null,
        rewardToResistancePct: null,
        rewardToSupportPct: null,
        supportStrength: null,
        resistanceStrength: null,
      };
    }

    const highs = [...new Set(slice.map((c) => Number(c.high.toFixed(4))))].sort((a, b) => a - b);
    const lows = [...new Set(slice.map((c) => Number(c.low.toFixed(4))))].sort((a, b) => a - b);

    const resistance = highs.find((h) => h >= price) ?? null;
    const support = [...lows].reverse().find((l) => l <= price) ?? null;

    return {
      support,
      resistance,
      distToSupportPct: support ? Math.abs((price - support) / price) : null,
      distToResistancePct: resistance ? Math.abs((resistance - price) / price) : null,
      nearStrongSupport: false,
      nearStrongResistance: false,
      nearestSupport: support,
      nearestResistance: resistance,
      rewardToResistancePct: resistance ? Math.abs((resistance - price) / price) : null,
      rewardToSupportPct: support ? Math.abs((price - support) / price) : null,
      supportStrength: null,
      resistanceStrength: null,
      nearestSupportZoneId: null,
      nearestResistanceZoneId: null,
      nearestSupportStrength: null,
      nearestResistanceStrength: null,
    };
  }

  evaluateLong(params) {
    const {
      latest2,
      candles2m,
      ind1m,
      ind2m,
      ind5m,
      trendResult,
      trend15mResult,
      candleInsight1m,
      candleInsight,
      structure1m,
      structure2m,
      structure5m,
      spreadPct,
      distanceToMA25,
      atrPct2m,
      volatilityRegime,
      sessionAllowed,
      currentSession,
      keyLevels,
      supportResistance,
      priceAction,
      marketQuality,
    } = params;

    const reasons = [];
    const rejects = [];
    const rejectCodes = [];
    const risks = [];
    const breakdown = this.newBreakdown();
    const recent = candles2m.slice(-18);
    const recentHigh = recent.length ? Math.max(...recent.map((c) => c.high)) : latest2.high;
    const recentLow = recent.length ? Math.min(...recent.map((c) => c.low)) : latest2.low;
    const recentRange = Math.max(recentHigh - recentLow, latest2.close * 0.0001);
    const positionInRange = (latest2.close - recentLow) / recentRange;
    const nearStrongResistance = Boolean(
      keyLevels?.nearStrongResistance ||
        (keyLevels?.distToResistancePct !== null &&
          keyLevels?.distToResistancePct <= this.config.trading.nearStrongLevelPct),
    );
    const extendedFromMA25 = distanceToMA25 > this.config.trading.maxDistanceFromMA25Pct;
    const hardRejectSet = new Set();
    const rewardToResistancePct = Number.isFinite(keyLevels?.distToResistancePct)
      ? keyLevels.distToResistancePct
      : null;
    const dynamicMinRewardPct = Math.max(this.config.trading.minRewardPct, distanceToMA25 * 0.7);

    if (priceAction?.structure === 'SIDEWAY') {
      breakdown.riskPenalty -= 25;
      this.addReject(rejects, rejectCodes, 'PRICE_ACTION_BLOCK', 'Price Action báo sideway, chặn LONG');
      hardRejectSet.add('PRICE_ACTION_BLOCK');
    }
    if (priceAction?.pullbackQuality === 'DEEP') {
      breakdown.pullback -= 8;
      this.addReject(rejects, rejectCodes, 'DEEP_PULLBACK', 'Pullback còn sâu, chưa tối ưu cho LONG');
    }
    if (priceAction?.pullbackQuality === 'FAIL') {
      breakdown.pullback -= 16;
      this.addReject(rejects, rejectCodes, 'BAD_PULLBACK', 'Pullback FAIL theo Price Action, chặn LONG');
      hardRejectSet.add('BAD_PULLBACK');
    }
    if (priceAction?.candleConfirm === false) {
      breakdown.candle -= 12;
      this.addReject(rejects, rejectCodes, 'CANDLE_CONFIRM_MISSING', 'Thiếu nến xác nhận LONG theo Price Action');
      hardRejectSet.add('CANDLE_CONFIRM_MISSING');
    }

    const microLongConfirm = Boolean(
      (ind1m?.latest?.ma7 || 0) >= (ind1m?.latest?.ma25 || 0) &&
        (ind1m?.latest?.ma7Slope || 0) > 0 &&
        (ind1m?.latest?.rsi14 || 50) >= 36 &&
        (ind1m?.latest?.rsi14 || 50) <= 64 &&
        (candleInsight1m?.patterns?.bullishConfirm || structure1m?.structure === 'BULLISH'),
    );
    if (this.config.trading.requireMicro1mConfirm) {
      if (microLongConfirm) {
        breakdown.candle += 6;
        reasons.push('1m xác nhận tăng cùng chiều (micro confirm)');
      } else {
        breakdown.candle -= 10;
        this.addReject(
          rejects,
          rejectCodes,
          'MICRO_1M_NOT_CONFIRMED',
          '1m chưa xác nhận LONG, tránh vào sớm sai nhịp',
        );
        hardRejectSet.add('MICRO_1M_NOT_CONFIRMED');
      }
    }
    if (priceAction?.antiFomo?.streakFomo) {
      breakdown.noisePenalty -= 15;
      this.addReject(rejects, rejectCodes, 'FOMO_STREAK', 'Chuỗi nến tăng mạnh liên tiếp, chặn FOMO LONG');
      hardRejectSet.add('FOMO_STREAK');
    }
    if (priceAction?.antiFomo?.farFromMA25) {
      breakdown.noisePenalty -= 12;
      this.addReject(rejects, rejectCodes, 'FOMO_DISTANCE_MA25', 'Giá đang xa MA25, không đuổi LONG');
      hardRejectSet.add('FOMO_DISTANCE_MA25');
    }
    if (priceAction?.antiFomo?.breakoutNoPullback) {
      breakdown.noisePenalty -= 10;
      this.addReject(
        rejects,
        rejectCodes,
        'FOMO_BREAKOUT',
        'Giá vừa breakout nhưng chưa pullback đẹp, chặn LONG',
      );
      hardRejectSet.add('FOMO_BREAKOUT');
    }
    if (priceAction?.bos?.againstTrend) {
      breakdown.structure -= 15;
      this.addReject(rejects, rejectCodes, 'BOS_AGAINST_TREND', 'BOS ngược trend, không LONG');
      hardRejectSet.add('BOS_AGAINST_TREND');
    }

    const adx = Number(ind2m.latest.adx || ind5m?.latest?.adx || 0);
    const plusDI = Number(ind2m.latest.plusDI || 0);
    const minusDI = Number(ind2m.latest.minusDI || 0);
    const bbBandwidth = Number(ind2m.latest.bbBandwidth || 0);
    const macdHist = Number(ind2m.latest.macdHist || 0);
    const macd = Number(ind2m.latest.macd || 0);
    const macdSignal = Number(ind2m.latest.macdSignal || 0);

    if (!Number.isFinite(adx) || adx < this.config.trading.minAdx) {
      breakdown.trend -= 12;
      this.addReject(
        rejects,
        rejectCodes,
        'WEAK_TREND_STRENGTH',
        `ADX yếu (${Number.isFinite(adx) ? adx.toFixed(1) : 'N/A'}), chặn LONG`,
      );
      hardRejectSet.add('WEAK_TREND_STRENGTH');
    } else if (plusDI > minusDI) {
      breakdown.trend += 8;
      reasons.push(`ADX mạnh (${adx.toFixed(1)}), DMI ủng hộ LONG`);
    } else {
      breakdown.structure -= 8;
      this.addReject(rejects, rejectCodes, 'DMI_CONFLICT', 'DMI chưa ủng hộ LONG');
    }

    if (Number.isFinite(bbBandwidth) && bbBandwidth <= this.config.trading.bbSqueezeMax) {
      breakdown.noisePenalty -= 12;
      this.addReject(
        rejects,
        rejectCodes,
        'BOLLINGER_SQUEEZE',
        `Bollinger đang siết chặt (${(bbBandwidth * 100).toFixed(2)}%), dễ nhiễu`,
      );
      hardRejectSet.add('BOLLINGER_SQUEEZE');
    }

    if (this.config.trading.macdConfirmRequired) {
      if (macdHist > 0 && macd >= macdSignal) {
        breakdown.maMomentum += 8;
        reasons.push('MACD xác nhận động lượng tăng');
      } else {
        breakdown.maMomentum -= 10;
        this.addReject(rejects, rejectCodes, 'MACD_CONFLICT', 'MACD chưa xác nhận LONG');
        hardRejectSet.add('MACD_CONFLICT');
      }
    }

    if (trendResult.trend === 'UP') {
      breakdown.trend += 20;
      reasons.push('Trend 5m tăng rõ');
    } else {
      this.addReject(rejects, rejectCodes, 'TREND_MISMATCH', 'Trend 5m không ủng hộ LONG');
    }

    if (priceAction?.side === 'SHORT') {
      breakdown.structure -= 18;
      this.addReject(rejects, rejectCodes, 'PRICE_ACTION_BLOCK', 'Price Action đang nghiêng SHORT, chặn LONG');
      hardRejectSet.add('PRICE_ACTION_BLOCK');
    }

    if (this.config.advanced.useTrend15mConfluence) {
      if (trend15mResult.trend === 'UP') {
        breakdown.confluence15m += 10;
        reasons.push('Confluence 15m cùng chiều tăng');
      } else if (trend15mResult.trend === 'DOWN') {
        breakdown.confluence15m -= 12;
        this.addReject(rejects, rejectCodes, 'TREND_15M_CONFLICT', '15m đang nghịch chiều LONG');
      }
    }

    if (Number(marketQuality?.score || 0) >= 75) {
      breakdown.trend += 4;
      reasons.push('Chất lượng thị trường tốt, setup sạch');
    } else if (Number(marketQuality?.score || 0) < Number(this.config.trading.marketQualityMinScore || 52) + 8) {
      breakdown.riskPenalty -= 8;
      this.addReject(
        rejects,
        rejectCodes,
        'LOW_MARKET_QUALITY_CONTEXT',
        'Market quality thấp, ưu tiên bỏ qua LONG',
      );
    }

    const pullback =
      ind2m.latest.ma25
        ? latest2.close >= ind2m.latest.ma25 * 0.996 &&
          latest2.close <= ind2m.latest.ma25 * 1.0032
        : false;
    const structureHold = Boolean(
      (structure2m.hh || 0) >= (structure2m.lh || 0) &&
        (structure2m.hl || 0) >= (structure2m.ll || 0),
    );
    if (pullback && !extendedFromMA25 && structureHold) {
      breakdown.pullback += 15;
      reasons.push('Pullback gần MA25 hợp lý');
    } else {
      this.addReject(
        rejects,
        rejectCodes,
        'BAD_PULLBACK',
        'Giá lệch MA25 quá xa hoặc pullback chưa đẹp',
      );
      if (!structureHold) {
        breakdown.structure -= 8;
        this.addReject(
          rejects,
          rejectCodes,
          'WEAK_PULLBACK_STRUCTURE',
          'Pullback làm yếu cấu trúc HH/HL, không LONG',
        );
      }
      if (extendedFromMA25) {
        breakdown.riskPenalty -= 12;
        this.addReject(
          rejects,
          rejectCodes,
          'EXTENDED_FROM_MA25',
          `Giá đã chạy xa MA25 (${(distanceToMA25 * 100).toFixed(2)}%), không LONG`,
        );
        hardRejectSet.add('EXTENDED_FROM_MA25');
      }
      if (!pullback && positionInRange > 0.72) {
        hardRejectSet.add('BAD_PULLBACK');
      }
    }

    const nearestSupport = supportResistance?.supports?.[0];
    if (nearestSupport && nearestSupport.distancePct <= 0.0042) {
      if (nearestSupport.strengthScore >= 68) {
        breakdown.structure += 8;
        reasons.push(
          `Giá gần ${nearestSupport.label || 'Support (Hỗ trợ) mạnh'} (${nearestSupport.primaryTimeframe})`,
        );
      } else {
        breakdown.structure += 3;
        reasons.push('Giá đang gần Support (Hỗ trợ)');
      }
    }

    if (nearStrongResistance) {
      breakdown.riskPenalty -= 16;
      this.addReject(
        rejects,
        rejectCodes,
        'NEAR_STRONG_RESISTANCE',
        'Gần Resistance mạnh (R-zone), không vào LONG để tránh vào muộn',
      );
      hardRejectSet.add('NEAR_STRONG_RESISTANCE');
    }

    if (supportResistance?.potential?.longZone) {
      breakdown.structure += 6;
      reasons.push('Vùng LONG tiềm năng đa khung đã xác nhận');
    }

    if ((ind2m.latest.ma7Slope || 0) > 0 && (ind2m.latest.ma7 || 0) >= (ind2m.latest.ma25 || 0)) {
      breakdown.maMomentum += 10;
      reasons.push('MA7 cong lên theo hướng LONG');
    } else {
      risks.push('MA7 chưa xác nhận mạnh');
    }

    if ((ind2m.latest.rsi14 || 50) >= 30 && (ind2m.latest.rsi14 || 50) <= 58) {
      breakdown.rsi += 15;
      reasons.push('RSI 2m ở vùng hồi và bật lên');
    } else {
      this.addReject(rejects, rejectCodes, 'RSI_NOT_READY', 'RSI chưa vào vùng thuận lợi cho LONG');
    }

    const rsiNow = ind2m.latest.rsi14 || 50;
    const nearResistanceForRsiCheck =
      keyLevels?.distToResistancePct !== null &&
      keyLevels.distToResistancePct <= this.config.trading.nearStrongLevelPct * 1.8;
    if (rsiNow >= this.config.trading.overboughtLongRsi && nearResistanceForRsiCheck) {
      breakdown.riskPenalty -= 14;
      this.addReject(
        rejects,
        rejectCodes,
        'OVERBOUGHT_NEAR_RESISTANCE',
        `RSI cao (${rsiNow.toFixed(2)}) và gần Resistance, không LONG`,
      );
      hardRejectSet.add('OVERBOUGHT_NEAR_RESISTANCE');
    }

    if (ind2m.volumeStatus.ratio > 1.05) {
      breakdown.volume += 10;
      reasons.push('Volume tăng so với cụm trước');
    } else {
      this.addReject(rejects, rejectCodes, 'LOW_VOLUME', 'Volume yếu cho LONG');
    }

    if (candleInsight.patterns.bullishConfirm) {
      breakdown.candle += 15;
      reasons.push('Nến xác nhận tăng rõ');
    } else {
      risks.push('Nến xác nhận còn yếu');
    }

    const p = ind2m.pressure;
    if (p.buyPressure >= 58 && p.buyPressure > p.sellPressure && p.deltaVolumeTrend > 0 && p.priceSpeed > -0.0002) {
      breakdown.pressure += 10;
      reasons.push('Orderflow mô phỏng ủng hộ LONG');
    } else {
      this.addReject(rejects, rejectCodes, 'WEAK_ORDERFLOW', 'Orderflow chưa ủng hộ LONG');
    }

    if (structure2m.structure === 'BULLISH') {
      breakdown.structure += 5;
      reasons.push('Cấu trúc 2m ủng hộ tăng');
    } else if (structure2m.structure === 'BEARISH') {
      breakdown.structure -= 8;
      this.addReject(rejects, rejectCodes, 'STRUCTURE_CONFLICT', 'Cấu trúc 2m nghịch hướng LONG');
    }

    if (structure5m?.structure === 'BEARISH') {
      breakdown.structure -= 12;
      this.addReject(rejects, rejectCodes, 'STRUCTURE_5M_CONFLICT', 'Cấu trúc 5m suy yếu, không LONG');
    }

    const longHardZone = clamp(Number(this.config.trading.entryTimingLongMaxRangePos || 0.8), 0.5, 0.95);
    const longWarnZone = Math.max(0.68, longHardZone - 0.08);

    if (positionInRange >= longHardZone) {
      breakdown.noisePenalty -= 12;
      this.addReject(
        rejects,
        rejectCodes,
        'LATE_ENTRY',
        `Entry LONG ở cuối sóng (${(positionInRange * 100).toFixed(1)}% biên gần nhất)`,
      );
      hardRejectSet.add('LATE_ENTRY');
    } else if (positionInRange >= longWarnZone) {
      breakdown.noisePenalty -= 6;
      risks.push('Entry LONG hơi muộn, cần cẩn trọng');
    }

    if (volatilityRegime === 'NORMAL_VOL') {
      breakdown.volatility += 8;
      reasons.push('Volatility 2m phù hợp scalp');
    } else if (volatilityRegime === 'HIGH_VOL') {
      breakdown.volatility -= 6;
      this.addReject(rejects, rejectCodes, 'HIGH_VOLATILITY', `Volatility cao (${(atrPct2m * 100).toFixed(2)}%)`);
    } else {
      breakdown.volatility -= 5;
      this.addReject(rejects, rejectCodes, 'LOW_VOLATILITY', `Volatility thấp (${(atrPct2m * 100).toFixed(2)}%)`);
    }

    if (this.config.advanced.sessionFilterEnabled) {
      if (sessionAllowed) {
        breakdown.session += 5;
        reasons.push(`Phiên ${currentSession} phù hợp scalp`);
      } else {
        breakdown.session -= 4;
        this.addReject(
          rejects,
          rejectCodes,
          'SESSION_FILTER',
          `Phiên ${currentSession} ngoài khung ưu tiên`,
        );
      }
    }

    if (spreadPct > this.config.trading.maxSpreadPct) {
      breakdown.riskPenalty -= 12;
      this.addReject(rejects, rejectCodes, 'HIGH_SPREAD', 'Spread cao vượt ngưỡng scalp');
    }

    const latestRange = latest2.high - latest2.low;
    if (latestRange / latest2.close > this.config.trading.maxExtendedCandlePct) {
      breakdown.noisePenalty -= 10;
      this.addReject(rejects, rejectCodes, 'EXTENDED_MOVE', 'Nến đã chạy quá dài, rủi ro đuổi giá');
    }

    if (p.absorptionScore > 60 && p.imbalance < 0) {
      breakdown.noisePenalty -= 6;
      this.addReject(rejects, rejectCodes, 'ABSORPTION_RISK', 'Nến hấp thụ nghiêng bán, LONG rủi ro');
    }

    if (
      keyLevels?.distToResistancePct !== null &&
      keyLevels.distToResistancePct < this.config.trading.nearStrongLevelPct * 1.3
    ) {
      breakdown.riskPenalty -= nearStrongResistance ? 14 : 9;
      this.addReject(
        rejects,
        rejectCodes,
        'NEAR_RESISTANCE',
        `Entry quá sát Resistance (Kháng cự) gần (${(keyLevels.distToResistancePct * 100).toFixed(2)}%)`,
      );
    }

    if (rewardToResistancePct !== null && rewardToResistancePct < dynamicMinRewardPct) {
      breakdown.riskPenalty -= 16;
      this.addReject(
        rejects,
        rejectCodes,
        'LOW_REWARD_DISTANCE',
        `Kháng cự gần (${(rewardToResistancePct * 100).toFixed(2)}%), reward < ${(dynamicMinRewardPct * 100).toFixed(2)}%`,
      );
      hardRejectSet.add('LOW_REWARD_DISTANCE');
    }

    const score = clamp(Object.values(breakdown).reduce((sum, v) => sum + v, 0), 0, 100);
    const uniqueRejectCodes = [...new Set(rejectCodes)];

    if (this.config.trading.strictNoTrashMode) {
      const strictGateSet = new Set([
        'LOW_VOLUME',
        'WEAK_ORDERFLOW',
        'RSI_NOT_READY',
        'DMI_CONFLICT',
        'HIGH_SPREAD',
        'LOW_VOLATILITY',
        'MICRO_1M_NOT_CONFIRMED',
        'LATE_ENTRY',
      ]);
      const blocked = uniqueRejectCodes.filter((code) => strictGateSet.has(code));
      if (blocked.length) {
        blocked.forEach((code) => hardRejectSet.add(code));
        this.addReject(
          rejects,
          rejectCodes,
          'STRICT_QUALITY_GATE',
          `Strict gate chặn LONG do: ${blocked.join(', ')}`,
        );
      }
    }

    const hardRejectCodes = uniqueRejectCodes.filter(
      (code) => hardRejectSet.has(code) || this.isHardRejectCode(code),
    );
    const hardBlocked = hardRejectCodes.length > 0;
    const maxSoftRejects = this.config.trading.strictNoTrashMode ? 1 : 5;

    return {
      side: 'LONG',
      score,
      breakdown,
      reasons,
      rejects,
      rejectCodes: uniqueRejectCodes,
      hardRejectCodes,
      hardBlocked,
      risks,
      allowed: rejects.length <= maxSoftRejects && !hardBlocked,
    };
  }

  evaluateShort(params) {
    const {
      latest2,
      candles2m,
      ind1m,
      ind2m,
      ind5m,
      trendResult,
      trend15mResult,
      candleInsight1m,
      candleInsight,
      structure1m,
      structure2m,
      structure5m,
      spreadPct,
      distanceToMA25,
      atrPct2m,
      volatilityRegime,
      sessionAllowed,
      currentSession,
      keyLevels,
      supportResistance,
      priceAction,
      marketQuality,
    } = params;

    const reasons = [];
    const rejects = [];
    const rejectCodes = [];
    const risks = [];
    const breakdown = this.newBreakdown();
    const recent = candles2m.slice(-18);
    const recentHigh = recent.length ? Math.max(...recent.map((c) => c.high)) : latest2.high;
    const recentLow = recent.length ? Math.min(...recent.map((c) => c.low)) : latest2.low;
    const recentRange = Math.max(recentHigh - recentLow, latest2.close * 0.0001);
    const positionInRange = (latest2.close - recentLow) / recentRange;
    const nearStrongSupport = Boolean(
      keyLevels?.nearStrongSupport ||
        (keyLevels?.distToSupportPct !== null &&
          keyLevels?.distToSupportPct <= this.config.trading.nearStrongLevelPct),
    );
    const extendedFromMA25 = distanceToMA25 > this.config.trading.maxDistanceFromMA25Pct;
    const hardRejectSet = new Set();
    const rewardToSupportPct = Number.isFinite(keyLevels?.distToSupportPct)
      ? keyLevels.distToSupportPct
      : null;
    const dynamicMinRewardPct = Math.max(this.config.trading.minRewardPct, distanceToMA25 * 0.7);

    if (priceAction?.structure === 'SIDEWAY') {
      breakdown.riskPenalty -= 25;
      this.addReject(rejects, rejectCodes, 'PRICE_ACTION_BLOCK', 'Price Action báo sideway, chặn SHORT');
      hardRejectSet.add('PRICE_ACTION_BLOCK');
    }
    if (priceAction?.pullbackQuality === 'DEEP') {
      breakdown.pullback -= 8;
      this.addReject(rejects, rejectCodes, 'DEEP_PULLBACK', 'Pullback còn sâu, chưa tối ưu cho SHORT');
    }
    if (priceAction?.pullbackQuality === 'FAIL') {
      breakdown.pullback -= 16;
      this.addReject(rejects, rejectCodes, 'BAD_PULLBACK', 'Pullback FAIL theo Price Action, chặn SHORT');
      hardRejectSet.add('BAD_PULLBACK');
    }
    if (priceAction?.candleConfirm === false) {
      breakdown.candle -= 12;
      this.addReject(rejects, rejectCodes, 'CANDLE_CONFIRM_MISSING', 'Thiếu nến xác nhận SHORT theo Price Action');
      hardRejectSet.add('CANDLE_CONFIRM_MISSING');
    }

    const microShortConfirm = Boolean(
      (ind1m?.latest?.ma7 || 0) <= (ind1m?.latest?.ma25 || 0) &&
        (ind1m?.latest?.ma7Slope || 0) < 0 &&
        (ind1m?.latest?.rsi14 || 50) >= 36 &&
        (ind1m?.latest?.rsi14 || 50) <= 64 &&
        (candleInsight1m?.patterns?.bearishConfirm || structure1m?.structure === 'BEARISH'),
    );
    if (this.config.trading.requireMicro1mConfirm) {
      if (microShortConfirm) {
        breakdown.candle += 6;
        reasons.push('1m xác nhận giảm cùng chiều (micro confirm)');
      } else {
        breakdown.candle -= 10;
        this.addReject(
          rejects,
          rejectCodes,
          'MICRO_1M_NOT_CONFIRMED',
          '1m chưa xác nhận SHORT, tránh vào sớm sai nhịp',
        );
        hardRejectSet.add('MICRO_1M_NOT_CONFIRMED');
      }
    }
    if (priceAction?.antiFomo?.streakFomo) {
      breakdown.noisePenalty -= 15;
      this.addReject(rejects, rejectCodes, 'FOMO_STREAK', 'Chuỗi nến giảm mạnh liên tiếp, chặn FOMO SHORT');
      hardRejectSet.add('FOMO_STREAK');
    }
    if (priceAction?.antiFomo?.farFromMA25) {
      breakdown.noisePenalty -= 12;
      this.addReject(rejects, rejectCodes, 'FOMO_DISTANCE_MA25', 'Giá đang xa MA25, không đuổi SHORT');
      hardRejectSet.add('FOMO_DISTANCE_MA25');
    }
    if (priceAction?.antiFomo?.breakoutNoPullback) {
      breakdown.noisePenalty -= 10;
      this.addReject(
        rejects,
        rejectCodes,
        'FOMO_BREAKOUT',
        'Giá vừa breakdown nhưng chưa pullback đẹp, chặn SHORT',
      );
      hardRejectSet.add('FOMO_BREAKOUT');
    }
    if (priceAction?.bos?.againstTrend) {
      breakdown.structure -= 15;
      this.addReject(rejects, rejectCodes, 'BOS_AGAINST_TREND', 'BOS ngược trend, không SHORT');
      hardRejectSet.add('BOS_AGAINST_TREND');
    }

    const adx = Number(ind2m.latest.adx || ind5m?.latest?.adx || 0);
    const plusDI = Number(ind2m.latest.plusDI || 0);
    const minusDI = Number(ind2m.latest.minusDI || 0);
    const bbBandwidth = Number(ind2m.latest.bbBandwidth || 0);
    const macdHist = Number(ind2m.latest.macdHist || 0);
    const macd = Number(ind2m.latest.macd || 0);
    const macdSignal = Number(ind2m.latest.macdSignal || 0);

    if (!Number.isFinite(adx) || adx < this.config.trading.minAdx) {
      breakdown.trend -= 12;
      this.addReject(
        rejects,
        rejectCodes,
        'WEAK_TREND_STRENGTH',
        `ADX yếu (${Number.isFinite(adx) ? adx.toFixed(1) : 'N/A'}), chặn SHORT`,
      );
      hardRejectSet.add('WEAK_TREND_STRENGTH');
    } else if (minusDI > plusDI) {
      breakdown.trend += 8;
      reasons.push(`ADX mạnh (${adx.toFixed(1)}), DMI ủng hộ SHORT`);
    } else {
      breakdown.structure -= 8;
      this.addReject(rejects, rejectCodes, 'DMI_CONFLICT', 'DMI chưa ủng hộ SHORT');
    }

    if (Number.isFinite(bbBandwidth) && bbBandwidth <= this.config.trading.bbSqueezeMax) {
      breakdown.noisePenalty -= 12;
      this.addReject(
        rejects,
        rejectCodes,
        'BOLLINGER_SQUEEZE',
        `Bollinger đang siết chặt (${(bbBandwidth * 100).toFixed(2)}%), dễ nhiễu`,
      );
      hardRejectSet.add('BOLLINGER_SQUEEZE');
    }

    if (this.config.trading.macdConfirmRequired) {
      if (macdHist < 0 && macd <= macdSignal) {
        breakdown.maMomentum += 8;
        reasons.push('MACD xác nhận động lượng giảm');
      } else {
        breakdown.maMomentum -= 10;
        this.addReject(rejects, rejectCodes, 'MACD_CONFLICT', 'MACD chưa xác nhận SHORT');
        hardRejectSet.add('MACD_CONFLICT');
      }
    }

    if (trendResult.trend === 'DOWN') {
      breakdown.trend += 20;
      reasons.push('Trend 5m giảm rõ');
    } else {
      this.addReject(rejects, rejectCodes, 'TREND_MISMATCH', 'Trend 5m không ủng hộ SHORT');
    }

    if (priceAction?.side === 'LONG') {
      breakdown.structure -= 18;
      this.addReject(rejects, rejectCodes, 'PRICE_ACTION_BLOCK', 'Price Action đang nghiêng LONG, chặn SHORT');
      hardRejectSet.add('PRICE_ACTION_BLOCK');
    }

    if (this.config.advanced.useTrend15mConfluence) {
      if (trend15mResult.trend === 'DOWN') {
        breakdown.confluence15m += 10;
        reasons.push('Confluence 15m cùng chiều giảm');
      } else if (trend15mResult.trend === 'UP') {
        breakdown.confluence15m -= 12;
        this.addReject(rejects, rejectCodes, 'TREND_15M_CONFLICT', '15m đang nghịch chiều SHORT');
      }
    }

    if (Number(marketQuality?.score || 0) >= 75) {
      breakdown.trend += 4;
      reasons.push('Chất lượng thị trường tốt, setup sạch');
    } else if (Number(marketQuality?.score || 0) < Number(this.config.trading.marketQualityMinScore || 52) + 8) {
      breakdown.riskPenalty -= 8;
      this.addReject(
        rejects,
        rejectCodes,
        'LOW_MARKET_QUALITY_CONTEXT',
        'Market quality thấp, ưu tiên bỏ qua SHORT',
      );
    }

    const pullback =
      ind2m.latest.ma25
        ? latest2.close <= ind2m.latest.ma25 * 1.004 &&
          latest2.close >= ind2m.latest.ma25 * 0.9968
        : false;
    const structureHold = Boolean(
      (structure2m.lh || 0) >= (structure2m.hh || 0) &&
        (structure2m.ll || 0) >= (structure2m.hl || 0),
    );
    if (pullback && !extendedFromMA25 && structureHold) {
      breakdown.pullback += 15;
      reasons.push('Pullback lên MA25 hợp lý cho SHORT');
    } else {
      this.addReject(
        rejects,
        rejectCodes,
        'BAD_PULLBACK',
        'Giá lệch MA25 quá xa hoặc pullback chưa đẹp',
      );
      if (!structureHold) {
        breakdown.structure -= 8;
        this.addReject(
          rejects,
          rejectCodes,
          'WEAK_PULLBACK_STRUCTURE',
          'Pullback làm yếu cấu trúc LH/LL, không SHORT',
        );
      }
      if (extendedFromMA25) {
        breakdown.riskPenalty -= 12;
        this.addReject(
          rejects,
          rejectCodes,
          'EXTENDED_FROM_MA25',
          `Giá đã chạy xa MA25 (${(distanceToMA25 * 100).toFixed(2)}%), không SHORT`,
        );
        hardRejectSet.add('EXTENDED_FROM_MA25');
      }
      if (!pullback && positionInRange < 0.28) {
        hardRejectSet.add('BAD_PULLBACK');
      }
    }

    const nearestResistance = supportResistance?.resistances?.[0];
    if (nearestResistance && nearestResistance.distancePct <= 0.0042) {
      if (nearestResistance.strengthScore >= 68) {
        breakdown.structure += 8;
        reasons.push(
          `Giá gần ${nearestResistance.label || 'Resistance (Kháng cự) mạnh'} (${nearestResistance.primaryTimeframe})`,
        );
      } else {
        breakdown.structure += 3;
        reasons.push('Giá đang gần Resistance (Kháng cự)');
      }
    }

    if (nearStrongSupport) {
      breakdown.riskPenalty -= 16;
      this.addReject(
        rejects,
        rejectCodes,
        'NEAR_STRONG_SUPPORT',
        'Gần Support mạnh (S-zone), không vào SHORT để tránh vào muộn',
      );
      hardRejectSet.add('NEAR_STRONG_SUPPORT');
    }

    if (supportResistance?.potential?.shortZone) {
      breakdown.structure += 6;
      reasons.push('Vùng SHORT tiềm năng đa khung đã xác nhận');
    }

    if ((ind2m.latest.ma7Slope || 0) < 0 && (ind2m.latest.ma7 || 0) <= (ind2m.latest.ma25 || 0)) {
      breakdown.maMomentum += 10;
      reasons.push('MA7 cong xuống theo hướng SHORT');
    } else {
      risks.push('MA7 chưa xác nhận giảm đủ mạnh');
    }

    if ((ind2m.latest.rsi14 || 50) <= 70 && (ind2m.latest.rsi14 || 50) >= 42) {
      breakdown.rsi += 15;
      reasons.push('RSI 2m hồi lên rồi quay đầu');
    } else {
      this.addReject(rejects, rejectCodes, 'RSI_NOT_READY', 'RSI chưa vào vùng thuận lợi cho SHORT');
    }

    const rsiNow = ind2m.latest.rsi14 || 50;
    const nearSupportForRsiCheck =
      keyLevels?.distToSupportPct !== null &&
      keyLevels.distToSupportPct <= this.config.trading.nearStrongLevelPct * 1.8;
    if (rsiNow <= this.config.trading.oversoldShortRsi && nearSupportForRsiCheck) {
      breakdown.riskPenalty -= 14;
      this.addReject(
        rejects,
        rejectCodes,
        'OVERSOLD_NEAR_SUPPORT',
        `RSI thấp (${rsiNow.toFixed(2)}) và gần Support, không SHORT`,
      );
      hardRejectSet.add('OVERSOLD_NEAR_SUPPORT');
    }

    if (ind2m.volumeStatus.ratio > 1.05) {
      breakdown.volume += 10;
      reasons.push('Volume tăng so với cụm trước');
    } else {
      this.addReject(rejects, rejectCodes, 'LOW_VOLUME', 'Volume yếu cho SHORT');
    }

    if (candleInsight.patterns.bearishConfirm) {
      breakdown.candle += 15;
      reasons.push('Nến xác nhận giảm rõ');
    } else {
      risks.push('Nến xác nhận còn yếu');
    }

    const p = ind2m.pressure;
    if (p.sellPressure >= 58 && p.sellPressure > p.buyPressure && p.deltaVolumeTrend < 0 && p.priceSpeed < 0.0002) {
      breakdown.pressure += 10;
      reasons.push('Orderflow mô phỏng ủng hộ SHORT');
    } else {
      this.addReject(rejects, rejectCodes, 'WEAK_ORDERFLOW', 'Orderflow chưa ủng hộ SHORT');
    }

    if (structure2m.structure === 'BEARISH') {
      breakdown.structure += 5;
      reasons.push('Cấu trúc 2m ủng hộ giảm');
    } else if (structure2m.structure === 'BULLISH') {
      breakdown.structure -= 8;
      this.addReject(rejects, rejectCodes, 'STRUCTURE_CONFLICT', 'Cấu trúc 2m nghịch hướng SHORT');
    }

    if (structure5m?.structure === 'BULLISH') {
      breakdown.structure -= 12;
      this.addReject(rejects, rejectCodes, 'STRUCTURE_5M_CONFLICT', 'Cấu trúc 5m suy yếu cho SHORT');
    }

    const shortHardZone = clamp(Number(this.config.trading.entryTimingShortMinRangePos || 0.2), 0.05, 0.5);
    const shortWarnZone = Math.min(0.32, shortHardZone + 0.08);

    if (positionInRange <= shortHardZone) {
      breakdown.noisePenalty -= 12;
      this.addReject(
        rejects,
        rejectCodes,
        'LATE_ENTRY',
        `Entry SHORT ở cuối sóng (${(positionInRange * 100).toFixed(1)}% biên gần nhất)`,
      );
      hardRejectSet.add('LATE_ENTRY');
    } else if (positionInRange <= shortWarnZone) {
      breakdown.noisePenalty -= 6;
      risks.push('Entry SHORT hơi muộn, cần cẩn trọng');
    }

    if (volatilityRegime === 'NORMAL_VOL') {
      breakdown.volatility += 8;
      reasons.push('Volatility 2m phù hợp scalp');
    } else if (volatilityRegime === 'HIGH_VOL') {
      breakdown.volatility -= 6;
      this.addReject(rejects, rejectCodes, 'HIGH_VOLATILITY', `Volatility cao (${(atrPct2m * 100).toFixed(2)}%)`);
    } else {
      breakdown.volatility -= 5;
      this.addReject(rejects, rejectCodes, 'LOW_VOLATILITY', `Volatility thấp (${(atrPct2m * 100).toFixed(2)}%)`);
    }

    if (this.config.advanced.sessionFilterEnabled) {
      if (sessionAllowed) {
        breakdown.session += 5;
        reasons.push(`Phiên ${currentSession} phù hợp scalp`);
      } else {
        breakdown.session -= 4;
        this.addReject(
          rejects,
          rejectCodes,
          'SESSION_FILTER',
          `Phiên ${currentSession} ngoài khung ưu tiên`,
        );
      }
    }

    if (spreadPct > this.config.trading.maxSpreadPct) {
      breakdown.riskPenalty -= 12;
      this.addReject(rejects, rejectCodes, 'HIGH_SPREAD', 'Spread cao vượt ngưỡng scalp');
    }

    const latestRange = latest2.high - latest2.low;
    if (latestRange / latest2.close > this.config.trading.maxExtendedCandlePct) {
      breakdown.noisePenalty -= 10;
      this.addReject(rejects, rejectCodes, 'EXTENDED_MOVE', 'Nến đã chạy quá dài, rủi ro đuổi giá');
    }

    if (p.absorptionScore > 60 && p.imbalance > 0) {
      breakdown.noisePenalty -= 6;
      this.addReject(rejects, rejectCodes, 'ABSORPTION_RISK', 'Nến hấp thụ nghiêng mua, SHORT rủi ro');
    }

    if (
      keyLevels?.distToSupportPct !== null &&
      keyLevels.distToSupportPct < this.config.trading.nearStrongLevelPct * 1.3
    ) {
      breakdown.riskPenalty -= nearStrongSupport ? 14 : 9;
      this.addReject(
        rejects,
        rejectCodes,
        'NEAR_SUPPORT',
        `Entry quá sát Support (Hỗ trợ) gần (${(keyLevels.distToSupportPct * 100).toFixed(2)}%)`,
      );
    }

    if (rewardToSupportPct !== null && rewardToSupportPct < dynamicMinRewardPct) {
      breakdown.riskPenalty -= 16;
      this.addReject(
        rejects,
        rejectCodes,
        'LOW_REWARD_DISTANCE',
        `Hỗ trợ gần (${(rewardToSupportPct * 100).toFixed(2)}%), reward < ${(dynamicMinRewardPct * 100).toFixed(2)}%`,
      );
      hardRejectSet.add('LOW_REWARD_DISTANCE');
    }

    const score = clamp(Object.values(breakdown).reduce((sum, v) => sum + v, 0), 0, 100);
    const uniqueRejectCodes = [...new Set(rejectCodes)];

    if (this.config.trading.strictNoTrashMode) {
      const strictGateSet = new Set([
        'LOW_VOLUME',
        'WEAK_ORDERFLOW',
        'RSI_NOT_READY',
        'DMI_CONFLICT',
        'HIGH_SPREAD',
        'LOW_VOLATILITY',
        'MICRO_1M_NOT_CONFIRMED',
        'LATE_ENTRY',
      ]);
      const blocked = uniqueRejectCodes.filter((code) => strictGateSet.has(code));
      if (blocked.length) {
        blocked.forEach((code) => hardRejectSet.add(code));
        this.addReject(
          rejects,
          rejectCodes,
          'STRICT_QUALITY_GATE',
          `Strict gate chặn SHORT do: ${blocked.join(', ')}`,
        );
      }
    }

    const hardRejectCodes = uniqueRejectCodes.filter(
      (code) => hardRejectSet.has(code) || this.isHardRejectCode(code),
    );
    const hardBlocked = hardRejectCodes.length > 0;
    const maxSoftRejects = this.config.trading.strictNoTrashMode ? 1 : 5;

    return {
      side: 'SHORT',
      score,
      breakdown,
      reasons,
      rejects,
      rejectCodes: uniqueRejectCodes,
      hardRejectCodes,
      hardBlocked,
      risks,
      allowed: rejects.length <= maxSoftRejects && !hardBlocked,
    };
  }

  getSignalLevel(score) {
    if (score >= 84) return 'STRONG';
    if (score >= 70) return 'MEDIUM';
    return 'WEAK';
  }

  findSwing(candles, side, lookback = 18) {
    const slice = candles.slice(-lookback);
    if (!slice.length) return null;

    if (side === 'LONG') return Math.min(...slice.map((c) => c.low));
    return Math.max(...slice.map((c) => c.high));
  }

  buildTradePlan({ side, candles2m, ind2m, entryPrice, distanceToMA25, keyLevels }) {
    const latest = candles2m[candles2m.length - 1];
    const ma25 = ind2m.latest.ma25 || latest.close;
    const entryBuffer = entryPrice * this.config.trading.entryZoneBufferPct;

    const entryMin = Math.min(ma25, entryPrice) - entryBuffer;
    const entryMax = Math.max(ma25, entryPrice) + entryBuffer;

    const swing = this.findSwing(candles2m, side, 16);

    let stopLoss;
    if (side === 'LONG') {
      stopLoss = Math.min(swing, latest.low) * 0.9992;
      if (stopLoss >= entryPrice) return { valid: false, reason: 'SL không hợp lệ cho LONG', penalty: 0 };
    } else {
      stopLoss = Math.max(swing, latest.high) * 1.0008;
      if (stopLoss <= entryPrice) return { valid: false, reason: 'SL không hợp lệ cho SHORT', penalty: 0 };
    }

    const riskDistance = Math.abs(entryPrice - stopLoss);
    const riskPct = riskDistance / entryPrice;

    if (riskPct > 0.012) return { valid: false, reason: 'SL quá xa cho chiến lược scalp', penalty: 20 };
    if (distanceToMA25 > this.config.trading.maxDistanceFromMA25Pct) {
      return { valid: false, reason: 'Giá đã chạy quá xa MA25', penalty: 16 };
    }

    const tp1 = side === 'LONG' ? entryPrice + riskDistance : entryPrice - riskDistance;
    const tp2 = side === 'LONG' ? entryPrice + riskDistance * 2 : entryPrice - riskDistance * 2;
    const tp3 = side === 'LONG' ? entryPrice + riskDistance * 3 : entryPrice - riskDistance * 3;

    const rr = Math.abs(tp2 - entryPrice) / riskDistance;
    if (rr < this.config.trading.minRR) return { valid: false, reason: 'RR thấp hơn ngưỡng cấu hình', penalty: 20 };

    let rewardToResistancePct = null;
    let rewardToSupportPct = null;

    if (side === 'LONG' && keyLevels?.resistance) {
      const dist = Math.abs((keyLevels.resistance - entryPrice) / entryPrice);
      rewardToResistancePct = dist;
      const minRewardPct = Math.max(this.config.trading.minRewardPct, riskPct * 1.1);
      if (dist < minRewardPct) {
        return {
          valid: false,
          reason: 'Kháng cự quá gần - RR xấu cho LONG',
          penalty: 14,
        };
      }
      if (keyLevels.nearStrongResistance && dist < Math.max(this.config.trading.minRewardPct * 1.4, riskPct * 1.3)) {
        return {
          valid: false,
          reason: 'Resistance mạnh quá gần, không LONG',
          penalty: 18,
        };
      }
    }

    if (side === 'SHORT' && keyLevels?.support) {
      const dist = Math.abs((entryPrice - keyLevels.support) / entryPrice);
      rewardToSupportPct = dist;
      const minRewardPct = Math.max(this.config.trading.minRewardPct, riskPct * 1.1);
      if (dist < minRewardPct) {
        return {
          valid: false,
          reason: 'Hỗ trợ quá gần - RR xấu cho SHORT',
          penalty: 14,
        };
      }
      if (keyLevels.nearStrongSupport && dist < Math.max(this.config.trading.minRewardPct * 1.4, riskPct * 1.3)) {
        return {
          valid: false,
          reason: 'Support mạnh quá gần, không SHORT',
          penalty: 18,
        };
      }
    }

    return {
      valid: true,
      entryMin,
      entryMax,
      entryPrice,
      stopLoss,
      tp1,
      tp2,
      tp3,
      rr,
      rewardToResistancePct,
      rewardToSupportPct,
      penalty: 0,
    };
  }

  buildNewbieNote({ side, rsi, trend, volumeStatus, score }) {
    if (side === 'LONG') {
      return `Xu hướng 5m đang ${trend === 'UP' ? 'tăng' : 'không rõ'}, RSI ${rsi?.toFixed(1) || 'N/A'} cho thấy giá có thể bật lên. Volume ${String(volumeStatus || 'N/A').toLowerCase()}, điểm tin cậy ${score}/100.`;
    }

    return `Xu hướng 5m đang ${trend === 'DOWN' ? 'giảm' : 'không rõ'}, RSI ${rsi?.toFixed(1) || 'N/A'} cho thấy giá có thể yếu thêm. Volume ${String(volumeStatus || 'N/A').toLowerCase()}, điểm tin cậy ${score}/100.`;
  }
}

module.exports = SignalEngine;
