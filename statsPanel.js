(function () {
  class StatsPanel {
    constructor(container) {
      this.container = container;
    }

    pickBest(mapObj) {
      if (!mapObj) return null;
      const entries = Object.entries(mapObj).filter(([, v]) => (v.count || 0) > 0);
      if (!entries.length) return null;
      entries.sort((a, b) => b[1].winRate - a[1].winRate);
      return { key: entries[0][0], ...entries[0][1] };
    }

    render(data) {
      if (!data) {
        this.container.innerHTML = '';
        return;
      }

      const bestSymbol = this.pickBest(data.winRateBySymbol);
      const bestTf = this.pickBest(data.winRateByTimeframe);
      const bestSetup = this.pickBest(data.winRateBySetupType);
      const bestRegime = this.pickBest(data.winRateByRegime);
      const bestHour = this.pickBest(data.winRateByHour);
      const bestSide = this.pickBest(data.winRateBySide);
      const topReject = Object.entries(data.rejectedByCode || {}).sort((a, b) => b[1] - a[1])[0];

      const list = [
        ['Tổng tín hiệu', data.totalSignals],
        ['Tổng lệnh', data.totalTrades],
        ['Winrate', `${(data.winRate || 0).toFixed(2)}%`],
        ['PnL tổng', `${(data.totalPnl || 0).toFixed(2)} USDT`],
        ['PnL hôm nay', `${(data.pnlToday || 0).toFixed(2)} USDT`],
        ['PnL tuần', `${(data.pnlWeek || 0).toFixed(2)} USDT`],
        ['Thắng/Thua', `${data.wins || 0}/${data.losses || 0}`],
        ['Avg / lệnh', `${(data.avgPnlPerTrade || 0).toFixed(2)} USDT`],
        ['Profit factor', Number.isFinite(data.profitFactor) ? (data.profitFactor || 0).toFixed(2) : 'N/A'],
        ['Expectancy', `${(data.expectancy || 0).toFixed(2)} USDT`],
        ['Avg Win', `${(data.avgWin || 0).toFixed(2)} USDT`],
        ['Avg Loss', `${(data.avgLossAbs || 0).toFixed(2)} USDT`],
        ['Win lớn nhất', `${(data.largestWin || 0).toFixed(2)} USDT`],
        ['Thua lớn nhất', `${(data.largestLoss || 0).toFixed(2)} USDT`],
        ['Chuỗi thắng dài', data.longestWinStreak || 0],
        ['Chuỗi thua dài', data.longestLossStreak || 0],
        ['Lệnh bị chặn', data.rejectedTrades || 0],
        ['Sideway detect', data.sidewayDetected || 0],
        ['Pause count', data.pauseCount || 0],
        ['Reconnect', data.reconnectCount || 0],
        ['Best coin WR', bestSymbol ? `${bestSymbol.key} (${bestSymbol.winRate.toFixed(1)}%)` : 'N/A'],
        ['Best TF WR', bestTf ? `${bestTf.key} (${bestTf.winRate.toFixed(1)}%)` : 'N/A'],
        ['Best setup WR', bestSetup ? `${bestSetup.key} (${bestSetup.winRate.toFixed(1)}%)` : 'N/A'],
        ['Best regime WR', bestRegime ? `${bestRegime.key} (${bestRegime.winRate.toFixed(1)}%)` : 'N/A'],
        ['Best side WR', bestSide ? `${bestSide.key} (${bestSide.winRate.toFixed(1)}%)` : 'N/A'],
        ['Best hour WR', bestHour ? `${bestHour.key} (${bestHour.winRate.toFixed(1)}%)` : 'N/A'],
        ['Reject code top', topReject ? `${topReject[0]} (${topReject[1]})` : 'N/A'],
      ];

      this.container.innerHTML = list
        .map(
          ([k, v]) => `<div class="item"><div class="k">${k}</div><div class="v">${v}</div></div>`,
        )
        .join('');
    }
  }

  window.StatsPanel = StatsPanel;
})();
