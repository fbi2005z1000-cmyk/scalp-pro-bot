const dayjs = require('dayjs');

class StatsService {
  constructor(stateStore) {
    this.stateStore = stateStore;
  }

  getTrades() {
    return this.stateStore.state.risk.tradeHistory || [];
  }

  filterByRange(startTs, endTs = Date.now()) {
    return this.getTrades().filter((t) => t.timestamp >= startTs && t.timestamp <= endTs);
  }

  calcWinRate(trades) {
    if (!trades.length) return 0;
    const wins = trades.filter((t) => Number(t.pnl || 0) >= 0).length;
    return (wins / trades.length) * 100;
  }

  calcHourDistribution(trades) {
    const buckets = {};
    for (const t of trades) {
      const hour = dayjs(t.timestamp).format('HH:00');
      if (!buckets[hour]) buckets[hour] = { count: 0, pnl: 0 };
      buckets[hour].count += 1;
      buckets[hour].pnl += Number(t.pnl || 0);
    }
    return buckets;
  }

  bestHour(trades) {
    const dist = this.calcHourDistribution(trades);
    let best = null;
    for (const [hour, item] of Object.entries(dist)) {
      if (!best || item.pnl > best.pnl) {
        best = { hour, pnl: item.pnl };
      }
    }
    return best ? best.hour : null;
  }

  symbolPerformance(trades) {
    const map = {};
    for (const t of trades) {
      if (!map[t.symbol]) map[t.symbol] = { pnl: 0, count: 0 };
      map[t.symbol].pnl += Number(t.pnl || 0);
      map[t.symbol].count += 1;
    }
    return map;
  }

  groupedWinRate(trades, field, fallback = 'UNKNOWN') {
    const map = {};
    for (const t of trades) {
      const key = t[field] || fallback;
      if (!map[key]) map[key] = { wins: 0, losses: 0, count: 0, pnl: 0 };
      map[key].count += 1;
      map[key].pnl += Number(t.pnl || 0);
      if (Number(t.pnl || 0) >= 0) map[key].wins += 1;
      else map[key].losses += 1;
    }

    const result = {};
    for (const [key, item] of Object.entries(map)) {
      result[key] = {
        count: item.count,
        winRate: item.count ? (item.wins / item.count) * 100 : 0,
        pnl: item.pnl,
      };
    }

    return result;
  }

  hourWinRate(trades) {
    const map = {};
    for (const t of trades) {
      const key = dayjs(t.timestamp).format('HH:00');
      if (!map[key]) map[key] = { wins: 0, losses: 0, count: 0, pnl: 0 };
      map[key].count += 1;
      map[key].pnl += Number(t.pnl || 0);
      if (Number(t.pnl || 0) >= 0) map[key].wins += 1;
      else map[key].losses += 1;
    }

    const result = {};
    for (const [key, item] of Object.entries(map)) {
      result[key] = {
        count: item.count,
        winRate: item.count ? (item.wins / item.count) * 100 : 0,
        pnl: item.pnl,
      };
    }

    return result;
  }

  getStats() {
    const status = this.stateStore.getStatus();
    const allTrades = this.getTrades();
    const todayStart = dayjs().startOf('day').valueOf();
    const weekStart = dayjs().startOf('week').valueOf();

    const tradesToday = this.filterByRange(todayStart);
    const tradesWeek = this.filterByRange(weekStart);
    const grossWin = allTrades.filter((t) => Number(t.pnl || 0) > 0).reduce((sum, t) => sum + Number(t.pnl || 0), 0);
    const grossLoss = allTrades.filter((t) => Number(t.pnl || 0) < 0).reduce((sum, t) => sum + Number(t.pnl || 0), 0);
    const wins = allTrades.filter((t) => Number(t.pnl || 0) > 0);
    const losses = allTrades.filter((t) => Number(t.pnl || 0) < 0);
    const avgWin = wins.length ? grossWin / wins.length : 0;
    const avgLossAbs = losses.length ? Math.abs(grossLoss / losses.length) : 0;
    const winRateDec = allTrades.length ? wins.length / allTrades.length : 0;
    const expectancy = winRateDec * avgWin - (1 - winRateDec) * avgLossAbs;

    return {
      ...status.stats,
      totalPnl: status.risk.totalPnl,
      pnlToday: status.risk.dailyPnl,
      pnlWeek: status.risk.weeklyPnl,
      winRate: this.calcWinRate(allTrades),
      winRateToday: this.calcWinRate(tradesToday),
      winRateWeek: this.calcWinRate(tradesWeek),
      avgPnlPerTrade: status.stats.totalTrades > 0 ? status.risk.totalPnl / status.stats.totalTrades : 0,
      bestHour: this.bestHour(allTrades),
      tradeCountToday: tradesToday.length,
      tradeCountWeek: tradesWeek.length,
      equity: status.risk.equity,
      drawdownPct:
        status.risk.peakEquity > 0
          ? ((status.risk.peakEquity - status.risk.equity) / status.risk.peakEquity) * 100
          : 0,
      profitFactor: grossLoss === 0 ? (grossWin > 0 ? 999 : 0) : grossWin / Math.abs(grossLoss),
      expectancy,
      avgWin,
      avgLossAbs,
      winRateBySymbol: this.groupedWinRate(allTrades, 'symbol'),
      winRateByTimeframe: this.groupedWinRate(allTrades, 'timeframe'),
      winRateBySetupType: this.groupedWinRate(allTrades, 'setupType'),
      winRateByRegime: this.groupedWinRate(allTrades, 'regime'),
      winRateBySide: this.groupedWinRate(allTrades, 'side'),
      winRateByHour: this.hourWinRate(allTrades),
      rejectedByCode: status.stats.rejectedByCode || {},
    };
  }

  buildDailyReport() {
    const start = dayjs().startOf('day').valueOf();
    const trades = this.filterByRange(start);
    const losses = trades.filter((t) => t.pnl < 0);

    const reasonCount = {};
    for (const t of losses) {
      const reason = t.reason || 'UNKNOWN';
      reasonCount[reason] = (reasonCount[reason] || 0) + 1;
    }

    const topLossReason = Object.entries(reasonCount).sort((a, b) => b[1] - a[1])[0]?.[0] || 'N/A';
    const totalPnl = trades.reduce((sum, t) => sum + Number(t.pnl || 0), 0);

    return {
      totalTrades: trades.length,
      winRate: this.calcWinRate(trades),
      pnlToday: totalPnl,
      bestHour: this.bestHour(trades),
      topLossReason,
      rejectedTrades: this.stateStore.state.stats.rejectedTrades,
      rejectedByCode: this.stateStore.state.stats.rejectedByCode || {},
      trades,
    };
  }

  buildWeeklyReport() {
    const start = dayjs().startOf('week').valueOf();
    const trades = this.filterByRange(start);
    const totalPnl = trades.reduce((sum, t) => sum + Number(t.pnl || 0), 0);

    const perf = this.symbolPerformance(trades);
    let bestSymbol = null;
    let worstSymbol = null;

    for (const [symbol, data] of Object.entries(perf)) {
      if (!bestSymbol || data.pnl > bestSymbol.pnl) bestSymbol = { symbol, pnl: data.pnl };
      if (!worstSymbol || data.pnl < worstSymbol.pnl) worstSymbol = { symbol, pnl: data.pnl };
    }

    const bySetup = this.groupedWinRate(trades, 'setupType');
    const bestSetup = Object.entries(bySetup).sort((a, b) => b[1].pnl - a[1].pnl)[0]?.[0] || 'N/A';

    return {
      totalTrades: trades.length,
      winRate: this.calcWinRate(trades),
      pnlWeek: totalPnl,
      bestSymbol: bestSymbol?.symbol || 'N/A',
      worstSymbol: worstSymbol?.symbol || 'N/A',
      bestHour: this.bestHour(trades),
      bestSetup,
      riskBlocks: this.stateStore.state.stats.rejectedTrades,
      rejectedByCode: this.stateStore.state.stats.rejectedByCode || {},
      trades,
    };
  }
}

module.exports = StatsService;
