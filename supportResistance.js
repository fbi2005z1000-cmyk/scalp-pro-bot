const { average, clamp } = require('./utils');

const TF_META = {
  '1m': { rank: 1, weight: 1.0 },
  '3m': { rank: 2, weight: 1.25 },
  '5m': { rank: 3, weight: 1.6 },
  '15m': { rank: 4, weight: 2.2 },
};

class SupportResistanceService {
  static analyze({ candlesByTimeframe, currentPrice, trend5m, rsi, candlePatterns }) {
    if (!candlesByTimeframe || !Number.isFinite(currentPrice) || currentPrice <= 0) {
      return this.emptyResult(currentPrice);
    }

    const rawZones = [];
    for (const [timeframe, candles] of Object.entries(candlesByTimeframe)) {
      if (!Array.isArray(candles) || candles.length < 40) continue;
      const zones = this.detectForTimeframe(candles, timeframe, currentPrice);
      rawZones.push(...zones.supports, ...zones.resistances);
    }

    const merged = this.mergeAcrossTimeframes(rawZones, currentPrice);
    const supports = merged
      .filter((z) => z.type === 'support')
      .sort((a, b) => a.distancePct - b.distancePct)
      .slice(0, 3);
    const resistances = merged
      .filter((z) => z.type === 'resistance')
      .sort((a, b) => a.distancePct - b.distancePct)
      .slice(0, 3);

    supports.sort((a, b) => b.price - a.price);
    resistances.sort((a, b) => a.price - b.price);
    supports.forEach((z, idx) => {
      z.label = `S${idx + 1}`;
      z.displayLabel = `${z.label} Support (Hỗ trợ) (${z.primaryTimeframe})`;
    });
    resistances.forEach((z, idx) => {
      z.label = `R${idx + 1}`;
      z.displayLabel = `${z.label} Resistance (Kháng cự) (${z.primaryTimeframe})`;
    });

    const potential = this.detectPotentialZones({
      supports,
      resistances,
      trend5m,
      rsi,
      candlePatterns,
      currentPrice,
    });

    if (potential.longZone) {
      const zone = supports.find((z) => z.id === potential.longZone.id);
      if (zone) {
        zone.intent = 'LONG';
        zone.blink = true;
      }
    }

    if (potential.shortZone) {
      const zone = resistances.find((z) => z.id === potential.shortZone.id);
      if (zone) {
        zone.intent = 'SHORT';
        zone.blink = true;
      }
    }

    const keyLevels = this.buildKeyLevels({ supports, resistances, currentPrice });
    return {
      zones: [...supports, ...resistances],
      supports,
      resistances,
      keyLevels,
      nearestSupport: keyLevels.support,
      nearestResistance: keyLevels.resistance,
      nearStrongSupport: keyLevels.nearStrongSupport,
      nearStrongResistance: keyLevels.nearStrongResistance,
      supportStrength: keyLevels.nearestSupportStrength,
      resistanceStrength: keyLevels.nearestResistanceStrength,
      rewardToResistancePct: keyLevels.distToResistancePct,
      rewardToSupportPct: keyLevels.distToSupportPct,
      potential,
      updatedAt: Date.now(),
    };
  }

  static emptyResult(currentPrice = 0) {
    return {
      zones: [],
      supports: [],
      resistances: [],
      keyLevels: {
        support: null,
        resistance: null,
        distToSupportPct: null,
        distToResistancePct: null,
        nearStrongSupport: false,
        nearStrongResistance: false,
        nearestSupportStrength: null,
        nearestResistanceStrength: null,
      },
      nearestSupport: null,
      nearestResistance: null,
      nearStrongSupport: false,
      nearStrongResistance: false,
      supportStrength: null,
      resistanceStrength: null,
      rewardToResistancePct: null,
      rewardToSupportPct: null,
      potential: {
        longZone: null,
        shortZone: null,
      },
      updatedAt: Date.now(),
      currentPrice,
    };
  }

  static detectForTimeframe(candles, timeframe, currentPrice) {
    const swings = this.findSwings(candles, 2, 2);
    const supportCandidates = swings.lows.map((s) => this.toZoneCandidate('support', timeframe, s));
    const resistanceCandidates = swings.highs.map((s) => this.toZoneCandidate('resistance', timeframe, s));

    const supports = this.clusterAndScore(supportCandidates, candles, timeframe, currentPrice, 'support');
    const resistances = this.clusterAndScore(
      resistanceCandidates,
      candles,
      timeframe,
      currentPrice,
      'resistance',
    );

    return { supports, resistances };
  }

  static findSwings(candles, left = 2, right = 2) {
    const highs = [];
    const lows = [];
    for (let i = left; i < candles.length - right; i += 1) {
      const c = candles[i];
      let isHigh = true;
      let isLow = true;

      for (let j = i - left; j <= i + right; j += 1) {
        if (j === i) continue;
        if (candles[j].high >= c.high) isHigh = false;
        if (candles[j].low <= c.low) isLow = false;
      }

      if (isHigh) highs.push({ index: i, candle: c });
      if (isLow) lows.push({ index: i, candle: c });
    }
    return { highs, lows };
  }

  static toZoneCandidate(type, timeframe, swing) {
    const c = swing.candle;
    if (type === 'resistance') {
      const top = c.high;
      const bottom = Math.max(c.open, c.close);
      return {
        type,
        timeframe,
        top: Math.max(top, bottom),
        bottom: Math.min(top, bottom),
        price: (top + bottom) / 2,
        index: swing.index,
        volume: c.volume || 0,
        touchTime: c.time,
      };
    }

    const bottom = c.low;
    const top = Math.min(c.open, c.close);
    return {
      type,
      timeframe,
      top: Math.max(top, bottom),
      bottom: Math.min(top, bottom),
      price: (top + bottom) / 2,
      index: swing.index,
      volume: c.volume || 0,
      touchTime: c.time,
    };
  }

  static clusterAndScore(candidates, candles, timeframe, currentPrice, type) {
    if (!candidates.length) return [];
    const meta = TF_META[timeframe] || TF_META['1m'];
    const proximityPct = timeframe === '15m' ? 0.0025 : timeframe === '5m' ? 0.0021 : 0.0017;

    const clusters = [];
    for (const c of candidates) {
      const hit = clusters.find((z) => {
        const overlap = c.bottom <= z.top && c.top >= z.bottom;
        const closeEnough = Math.abs(c.price - z.price) / currentPrice <= proximityPct;
        return overlap || closeEnough;
      });

      if (!hit) {
        clusters.push({
          id: `${type}:${timeframe}:${c.touchTime}:${Math.round(c.price)}`,
          type,
          timeframe,
          top: c.top,
          bottom: c.bottom,
          price: c.price,
          indices: [c.index],
          volumes: [c.volume],
          times: [c.touchTime],
          timeframeSet: new Set([timeframe]),
        });
      } else {
        hit.top = Math.max(hit.top, c.top);
        hit.bottom = Math.min(hit.bottom, c.bottom);
        hit.price = (hit.top + hit.bottom) / 2;
        hit.indices.push(c.index);
        hit.volumes.push(c.volume);
        hit.times.push(c.touchTime);
      }
    }

    const avgVol = average(candles.map((x) => x.volume || 0)) || 1;
    const zones = [];
    for (const z of clusters) {
      const stats = this.calcZoneStats({ zone: z, candles, type, avgVol });
      if (!stats.valid) continue;

      const tfBoost = meta.weight * 10;
      const touchScore = clamp(stats.touches * 15, 0, 45);
      const reactionScore = clamp(stats.reactions * 10, 0, 28);
      const volumeScore = clamp((stats.volumeRatio - 0.85) * 22, 0, 22);
      const breakPenalty = clamp(stats.breaks * 7, 0, 24);
      const score = clamp(touchScore + reactionScore + volumeScore + tfBoost - breakPenalty, 0, 100);
      const strength = this.scoreToStrength(score);
      const distancePct = Math.abs((currentPrice - z.price) / currentPrice);

      zones.push({
        ...z,
        top: Number(z.top),
        bottom: Number(z.bottom),
        price: Number(z.price),
        widthPct: Math.abs((z.top - z.bottom) / z.price),
        touches: stats.touches,
        reactions: stats.reactions,
        breaks: stats.breaks,
        volumeRatio: stats.volumeRatio,
        strengthScore: Number(score.toFixed(2)),
        strength,
        tfRank: meta.rank,
        tfWeight: meta.weight,
        distancePct,
        lineWidth: strength === 'EXTREME' ? 3 : strength === 'STRONG' ? 2 : 1,
        opacity: strength === 'EXTREME' ? 0.28 : strength === 'STRONG' ? 0.22 : strength === 'NORMAL' ? 0.16 : 0.12,
        primaryTimeframe: timeframe,
        lastTouchTime: Math.max(...z.times),
        blink: false,
      });
    }

    return zones.sort((a, b) => a.distancePct - b.distancePct).slice(0, 4);
  }

  static calcZoneStats({ zone, candles, type, avgVol }) {
    const zoneHeight = Math.max(zone.top - zone.bottom, zone.price * 0.0003);
    let touches = 0;
    let reactions = 0;
    let breaks = 0;
    let lastTouchIndex = -99;
    const touchVolumes = [];

    for (let i = 0; i < candles.length; i += 1) {
      const c = candles[i];
      const touched = c.high >= zone.bottom && c.low <= zone.top;
      if (!touched) continue;

      if (i - lastTouchIndex >= 2) {
        touches += 1;
        lastTouchIndex = i;
        touchVolumes.push(c.volume || 0);

        const future = candles.slice(i + 1, i + 4);
        if (type === 'resistance') {
          const rejected = future.some((f) => f.close <= zone.bottom - zoneHeight * 0.35);
          if (rejected) reactions += 1;
        } else {
          const rejected = future.some((f) => f.close >= zone.top + zoneHeight * 0.35);
          if (rejected) reactions += 1;
        }
      }

      const isBreak = type === 'resistance' ? c.close > zone.top * 1.0007 : c.close < zone.bottom * 0.9993;
      if (isBreak) breaks += 1;
    }

    const recent = candles.slice(-6);
    const brokenRecently = recent.some((c) =>
      type === 'resistance' ? c.close > zone.top * 1.0011 : c.close < zone.bottom * 0.9989,
    );
    if (brokenRecently) return { valid: false, touches, reactions, breaks, volumeRatio: 0 };
    if (touches < 2) return { valid: false, touches, reactions, breaks, volumeRatio: 0 };

    const volumeRatio = (average(touchVolumes) || avgVol) / avgVol;
    return {
      valid: true,
      touches,
      reactions,
      breaks,
      volumeRatio,
    };
  }

  static mergeAcrossTimeframes(zones, currentPrice) {
    if (!zones.length) return [];
    const merged = [];
    const proximityPct = 0.0023;

    for (const z of zones) {
      const hit = merged.find((m) => {
        if (m.type !== z.type) return false;
        const overlap = z.bottom <= m.top && z.top >= m.bottom;
        const closeEnough = Math.abs(z.price - m.price) / currentPrice <= proximityPct;
        return overlap || closeEnough;
      });

      if (!hit) {
        merged.push({
          ...z,
          timeframeSet: new Set([z.timeframe]),
        });
      } else {
        hit.top = Math.max(hit.top, z.top);
        hit.bottom = Math.min(hit.bottom, z.bottom);
        hit.price = (hit.price * hit.strengthScore + z.price * z.strengthScore) / Math.max(hit.strengthScore + z.strengthScore, 1);
        hit.touches += z.touches;
        hit.reactions += z.reactions;
        hit.breaks += z.breaks;
        hit.strengthScore = clamp(hit.strengthScore + z.strengthScore * 0.35, 0, 100);
        hit.tfRank = Math.max(hit.tfRank, z.tfRank);
        hit.tfWeight = Math.max(hit.tfWeight, z.tfWeight);
        hit.primaryTimeframe = hit.tfRank >= z.tfRank ? hit.primaryTimeframe : z.primaryTimeframe;
        hit.opacity = Math.max(hit.opacity, z.opacity);
        hit.lineWidth = Math.max(hit.lineWidth, z.lineWidth);
        hit.lastTouchTime = Math.max(hit.lastTouchTime, z.lastTouchTime);
        hit.timeframeSet.add(z.timeframe);
      }
    }

    return merged
      .map((z) => {
        const distancePct = Math.abs((currentPrice - z.price) / currentPrice);
        const score = clamp(
          z.strengthScore + (z.timeframeSet.size - 1) * 6 + Math.max(0, 20 - distancePct * 2500),
          0,
          100,
        );
        return {
          ...z,
          distancePct,
          strengthScore: Number(score.toFixed(2)),
          strength: this.scoreToStrength(score),
          timeframeSet: Array.from(z.timeframeSet).sort(
            (a, b) => (TF_META[b]?.rank || 0) - (TF_META[a]?.rank || 0),
          ),
        };
      })
      .sort((a, b) => a.distancePct - b.distancePct)
      .filter((z) => z.distancePct <= 0.035)
      .slice(0, 12);
  }

  static buildKeyLevels({ supports, resistances, currentPrice }) {
    const nearestSupportZone =
      supports.find((z) => z.price <= currentPrice + currentPrice * 0.0008) ||
      [...supports].sort((a, b) => a.distancePct - b.distancePct)[0] ||
      null;
    const nearestResistanceZone =
      resistances.find((z) => z.price >= currentPrice - currentPrice * 0.0008) ||
      [...resistances].sort((a, b) => a.distancePct - b.distancePct)[0] ||
      null;
    const support = nearestSupportZone?.price || null;
    const resistance = nearestResistanceZone?.price || null;
    const distToSupportPct = support ? Math.abs((currentPrice - support) / currentPrice) : null;
    const distToResistancePct = resistance ? Math.abs((resistance - currentPrice) / currentPrice) : null;
    const nearStrongSupport = Boolean(
      nearestSupportZone &&
        nearestSupportZone.strengthScore >= 68 &&
        distToSupportPct !== null &&
        distToSupportPct <= 0.0032,
    );
    const nearStrongResistance = Boolean(
      nearestResistanceZone &&
        nearestResistanceZone.strengthScore >= 68 &&
        distToResistancePct !== null &&
        distToResistancePct <= 0.0032,
    );
    return {
      support,
      resistance,
      distToSupportPct,
      distToResistancePct,
      nearStrongSupport,
      nearStrongResistance,
      nearestSupport: support,
      nearestResistance: resistance,
      rewardToResistancePct: distToResistancePct,
      rewardToSupportPct: distToSupportPct,
      supportStrength: nearestSupportZone?.strength || null,
      resistanceStrength: nearestResistanceZone?.strength || null,
      nearestSupportZoneId: nearestSupportZone?.id || null,
      nearestResistanceZoneId: nearestResistanceZone?.id || null,
      nearestSupportStrength: nearestSupportZone?.strength || null,
      nearestResistanceStrength: nearestResistanceZone?.strength || null,
    };
  }

  static detectPotentialZones({ supports, resistances, trend5m, rsi, candlePatterns, currentPrice }) {
    const longCandidate = supports
      .filter((z) => z.distancePct <= 0.0062)
      .sort((a, b) => a.distancePct - b.distancePct)[0];
    const shortCandidate = resistances
      .filter((z) => z.distancePct <= 0.0062)
      .sort((a, b) => a.distancePct - b.distancePct)[0];

    let longZone = null;
    if (longCandidate && trend5m === 'UP' && longCandidate.strengthScore >= 45) {
      const rsiScore = rsi >= 30 && rsi <= 45 ? 18 : rsi >= 25 && rsi <= 52 ? 10 : 0;
      const candleScore = candlePatterns?.bullishConfirm ? 14 : candlePatterns?.lowerPinbar ? 8 : 3;
      const strict = rsi >= 30 && rsi <= 45 && candlePatterns?.bullishConfirm;
      longZone = {
        id: longCandidate.id,
        type: 'LONG_ZONE',
        mode: strict ? 'STRONG' : 'WATCH',
        distancePct: longCandidate.distancePct,
        price: longCandidate.price,
        score: clamp(
          longCandidate.strengthScore +
            rsiScore +
            candleScore +
            (longCandidate.tfRank || 1) * 4 +
            Math.max(0, 14 - longCandidate.distancePct * 2100),
          0,
          100,
        ),
      };
    }
    if (!longZone && longCandidate && longCandidate.strengthScore >= 52 && longCandidate.distancePct <= 0.0048) {
      longZone = {
        id: longCandidate.id,
        type: 'LONG_ZONE',
        mode: 'WATCH',
        distancePct: longCandidate.distancePct,
        price: longCandidate.price,
        score: clamp(longCandidate.strengthScore + Math.max(0, 12 - longCandidate.distancePct * 1800), 0, 100),
      };
    }

    let shortZone = null;
    if (shortCandidate && trend5m === 'DOWN' && shortCandidate.strengthScore >= 45) {
      const rsiScore = rsi >= 55 && rsi <= 70 ? 18 : rsi >= 50 && rsi <= 75 ? 10 : 0;
      const candleScore = candlePatterns?.bearishConfirm ? 14 : candlePatterns?.upperPinbar ? 8 : 3;
      const strict = rsi >= 55 && rsi <= 70 && candlePatterns?.bearishConfirm;
      shortZone = {
        id: shortCandidate.id,
        type: 'SHORT_ZONE',
        mode: strict ? 'STRONG' : 'WATCH',
        distancePct: shortCandidate.distancePct,
        price: shortCandidate.price,
        score: clamp(
          shortCandidate.strengthScore +
            rsiScore +
            candleScore +
            (shortCandidate.tfRank || 1) * 4 +
            Math.max(0, 14 - shortCandidate.distancePct * 2100),
          0,
          100,
        ),
      };
    }
    if (!shortZone && shortCandidate && shortCandidate.strengthScore >= 52 && shortCandidate.distancePct <= 0.0048) {
      shortZone = {
        id: shortCandidate.id,
        type: 'SHORT_ZONE',
        mode: 'WATCH',
        distancePct: shortCandidate.distancePct,
        price: shortCandidate.price,
        score: clamp(shortCandidate.strengthScore + Math.max(0, 12 - shortCandidate.distancePct * 1800), 0, 100),
      };
    }

    return {
      longZone,
      shortZone,
    };
  }

  static scoreToStrength(score) {
    if (score >= 84) return 'EXTREME';
    if (score >= 68) return 'STRONG';
    if (score >= 48) return 'NORMAL';
    return 'WEAK';
  }
}

module.exports = SupportResistanceService;
