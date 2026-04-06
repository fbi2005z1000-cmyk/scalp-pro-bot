(() => {
  const state = {
    symbol: 'BTCUSDT',
    timeframe: '1m',
    signal: null,
    status: null,
    stats: null,
    logs: [],
    candles1m: [],
    candles3m: [],
    candles5m: [],
    candles15m: [],
    candles: [],
    glossary: {},
    toggles: {
      ma: true,
      rsi: true,
      volume: true,
      candleLiquidity: true,
      markers: true,
      zones: true,
      newbie: true,
    },
    priceLines: [],
    signalMarkers: [],
    srPriceLines: [],
    srData: null,
    latestIndicators: {
      ma7: null,
      ma25: null,
      ma99: null,
      ma200: null,
      lastTime: null,
    },
    entryHint: null,
    streamConnected: false,
    lastTickTs: 0,
    lastClosedSyncKey: '',
    pendingChartRefresh: false,
    timezoneMode: localStorage.getItem('APP_TIMEZONE_MODE') || 'VN',
    priceSource: 'LAST',
    livePrice: 0,
    directKlineWs: null,
    directKlineToken: 0,
    directKlineConnected: false,
    directReconcileTimer: null,
    lastLiquidityDrawAt: 0,
    fleetHydrationInProgress: false,
    fleetHydrationDone: false,
    fleetHydrationAt: 0,
    lastFullRenderAt: 0,
    candleCountdownText: '--:--',
    snapshotMode: new URLSearchParams(window.location.search).get('snapshot') === '1',
    adminToken: localStorage.getItem('ADMIN_TOKEN') || '',
  };

  const el = {
    symbolSelect: document.getElementById('symbolSelect'),
    timeframeSelect: document.getElementById('timeframeSelect'),
    priceSourceSelect: document.getElementById('priceSourceSelect'),
    timezoneSelect: document.getElementById('timezoneSelect'),
    connectionBadge: document.getElementById('connectionBadge'),
    botStateBadge: document.getElementById('botStateBadge'),
    signalType: document.getElementById('signalType'),
    signalGrid: document.getElementById('signalGrid'),
    signalReason: document.getElementById('signalReason'),
    newbieHint: document.getElementById('newbieHint'),
    pressureGrid: document.getElementById('pressureGrid'),
    quickWarning: document.getElementById('quickWarning'),
    modeSelect: document.getElementById('modeSelect'),
    riskPerTradeInput: document.getElementById('riskPerTradeInput'),
    dailyLossInput: document.getElementById('dailyLossInput'),
    maxTradesHourInput: document.getElementById('maxTradesHourInput'),
    startBtn: document.getElementById('startBtn'),
    stopBtn: document.getElementById('stopBtn'),
    confirmTradeBtn: document.getElementById('confirmTradeBtn'),
    killSwitchBtn: document.getElementById('killSwitchBtn'),
    applyConfigBtn: document.getElementById('applyConfigBtn'),
    telegramTestInput: document.getElementById('telegramTestInput'),
    telegramRouteSelect: document.getElementById('telegramRouteSelect'),
    telegramStickerInput: document.getElementById('telegramStickerInput'),
    telegramAnimationInput: document.getElementById('telegramAnimationInput'),
    telegramDiceSelect: document.getElementById('telegramDiceSelect'),
    sendTelegramTestBtn: document.getElementById('sendTelegramTestBtn'),
    toggleMA: document.getElementById('toggleMA'),
    toggleRSI: document.getElementById('toggleRSI'),
    toggleVolume: document.getElementById('toggleVolume'),
    toggleCandleLiquidity: document.getElementById('toggleCandleLiquidity'),
    toggleMarkers: document.getElementById('toggleMarkers'),
    toggleZones: document.getElementById('toggleZones'),
    toggleNewbie: document.getElementById('toggleNewbie'),
    chartContainer: document.getElementById('chartContainer'),
    chartFootNote: document.getElementById('chartFootNote'),
    candleCountdownBadge: document.getElementById('candleCountdownBadge'),
    indicatorLegend: document.getElementById('indicatorLegend'),
    logContainer: document.getElementById('logContainer'),
    fleetBadge: document.getElementById('fleetBadge'),
    fleetSummary: document.getElementById('fleetSummary'),
    fleetActiveList: document.getElementById('fleetActiveList'),
    fleetInactiveList: document.getElementById('fleetInactiveList'),
    outcomeFleetBadge: document.getElementById('outcomeFleetBadge'),
    outcomeFleetSummary: document.getElementById('outcomeFleetSummary'),
    outcomeFleetActiveList: document.getElementById('outcomeFleetActiveList'),
    outcomeFleetInactiveList: document.getElementById('outcomeFleetInactiveList'),
    rsGuidePriceNow: document.getElementById('rsGuidePriceNow'),
    rsMeaningGrid: document.getElementById('rsMeaningGrid'),
    rsScenarioBox: document.getElementById('rsScenarioBox'),
    livePlanGrid: document.getElementById('livePlanGrid'),
  };

  let stream = null;

  const chart = LightweightCharts.createChart(document.getElementById('chartContainer'), {
    layout: {
      background: { color: '#101a27' },
      textColor: '#d9e5f2',
    },
    grid: {
      vertLines: { color: 'rgba(151, 173, 195, 0.12)' },
      horzLines: { color: 'rgba(151, 173, 195, 0.12)' },
    },
    rightPriceScale: { borderColor: 'rgba(132, 158, 182, 0.35)' },
    timeScale: { borderColor: 'rgba(132, 158, 182, 0.35)', timeVisible: false },
    crosshair: { mode: LightweightCharts.CrosshairMode.Normal },
    autoSize: true,
  });

  const volumeChart = LightweightCharts.createChart(document.getElementById('volumeContainer'), {
    layout: {
      background: { color: '#0f1723' },
      textColor: '#a8bdd0',
    },
    grid: {
      vertLines: { color: 'rgba(151, 173, 195, 0.08)' },
      horzLines: { color: 'rgba(151, 173, 195, 0.08)' },
    },
    rightPriceScale: { borderColor: 'rgba(132, 158, 182, 0.28)' },
    timeScale: { visible: false },
    autoSize: true,
  });

  const rsiChart = LightweightCharts.createChart(document.getElementById('rsiContainer'), {
    layout: {
      background: { color: '#0f1723' },
      textColor: '#a8bdd0',
    },
    grid: {
      vertLines: { color: 'rgba(151, 173, 195, 0.08)' },
      horzLines: { color: 'rgba(151, 173, 195, 0.08)' },
    },
    rightPriceScale: { borderColor: 'rgba(132, 158, 182, 0.28)' },
    timeScale: { borderColor: 'rgba(132, 158, 182, 0.28)', timeVisible: true },
    autoSize: true,
  });

  const candleSeries = chart.addCandlestickSeries({
    upColor: '#16c47f',
    downColor: '#ea5455',
    borderVisible: false,
    wickUpColor: '#16c47f',
    wickDownColor: '#ea5455',
  });

  const volumeSeries = volumeChart.addHistogramSeries({
    priceFormat: { type: 'volume' },
    scaleMargins: { top: 0.12, bottom: 0 },
  });

  const ma7Series = chart.addLineSeries({ color: '#ffd166', lineWidth: 1.6 });
  const ma25Series = chart.addLineSeries({ color: '#4ea5d9', lineWidth: 1.6 });
  const ma99Series = chart.addLineSeries({ color: '#b28dff', lineWidth: 1.3 });
  const ma200Series = chart.addLineSeries({ color: '#8f9db1', lineWidth: 1.3 });

  const rsiSeries = rsiChart.addLineSeries({ color: '#4ecdc4', lineWidth: 1.5 });
  const rsiOverSold = rsiChart.addLineSeries({ color: 'rgba(22,196,127,0.35)', lineWidth: 1 });
  const rsiOverBought = rsiChart.addLineSeries({ color: 'rgba(234,84,85,0.35)', lineWidth: 1 });
  const rsiMid = rsiChart.addLineSeries({ color: 'rgba(130,146,166,0.35)', lineWidth: 1 });
  const srZoneOverlay = document.createElement('div');
  srZoneOverlay.className = 'sr-zone-overlay';
  el.chartContainer.appendChild(srZoneOverlay);
  const maTagOverlay = document.createElement('div');
  maTagOverlay.className = 'ma-tag-overlay';
  el.chartContainer.appendChild(maTagOverlay);
  const candleLiqOverlay = document.createElement('div');
  candleLiqOverlay.className = 'candle-liq-overlay';
  el.chartContainer.appendChild(candleLiqOverlay);

  chart.timeScale().subscribeVisibleLogicalRangeChange((range) => {
    if (!range) return;
    volumeChart.timeScale().setVisibleLogicalRange(range);
    rsiChart.timeScale().setVisibleLogicalRange(range);
    drawSupportResistance(state.srData);
    drawMaMiniTags();
    drawCandleLiquidityTags(true);
  });

  const glossaryPanel = new GlossaryPanel(document.getElementById('glossaryContainer'));
  const statsPanel = new StatsPanel(document.getElementById('statsGrid'));
  const tooltip = new HelpTooltip();

  tooltip.bind('[data-tip]');
  window.__chartReady = false;

  const api = {
    get: async (path) => {
      const res = await fetch(`/api${path}`);
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || `API lỗi ${path}`);
      return data.data;
    },
    post: async (path, body = {}) => {
      const headers = { 'Content-Type': 'application/json' };
      if (state.adminToken) headers['x-admin-token'] = state.adminToken;
      const res = await fetch(`/api${path}`, {
        method: 'POST',
        headers,
        body: JSON.stringify(body),
      });
      const data = await res.json();
      if (!res.ok || !data.ok) throw new Error(data.error || `API lỗi ${path}`);
      return data.data;
    },
  };

  function chartTimeToDate(time) {
    if (typeof time === 'number') return new Date(time * 1000);
    if (time && typeof time === 'object') {
      if (typeof time.timestamp === 'number') return new Date(time.timestamp * 1000);
      if (typeof time.year === 'number') {
        return new Date(Date.UTC(time.year, (time.month || 1) - 1, time.day || 1));
      }
    }
    return new Date();
  }

  const TZ = {
    VN: 'Asia/Ho_Chi_Minh',
    UTC: 'UTC',
  };

  function formatByTimezone(date, withDate = false) {
    if (!(date instanceof Date)) return '';
    if (state.timezoneMode === 'UTC' || state.timezoneMode === 'VN') {
      const zone = state.timezoneMode === 'VN' ? TZ.VN : TZ.UTC;
      return withDate
        ? new Intl.DateTimeFormat('en-GB', {
            timeZone: zone,
            day: '2-digit',
            month: '2-digit',
            hour: '2-digit',
            minute: '2-digit',
          }).format(date)
        : new Intl.DateTimeFormat('en-GB', {
            timeZone: zone,
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
          }).format(date);
    }
    return withDate ? date.toLocaleString() : date.toLocaleTimeString();
  }

  function applyTimezoneToChart() {
    const common = {
      localization: {
        timeFormatter: (time) => formatByTimezone(chartTimeToDate(time), false),
      },
      timeScale: {
        tickMarkFormatter: (time) => formatByTimezone(chartTimeToDate(time), true),
      },
    };
    chart.applyOptions(common);
    volumeChart.applyOptions(common);
    rsiChart.applyOptions(common);
  }

  function formatNum(n, digits = 2) {
    if (n === null || n === undefined || Number.isNaN(Number(n))) return 'N/A';
    return Number(n).toFixed(digits);
  }

  function formatPriceAdaptive(n) {
    const v = Number(n);
    if (!Number.isFinite(v)) return 'N/A';
    const abs = Math.abs(v);
    if (abs >= 1000) return v.toFixed(2);
    if (abs >= 100) return v.toFixed(3);
    if (abs >= 1) return v.toFixed(4);
    if (abs >= 0.1) return v.toFixed(5);
    if (abs >= 0.01) return v.toFixed(6);
    if (abs >= 0.001) return v.toFixed(7);
    return v.toFixed(8);
  }

  function formatPrice(n) {
    return formatPriceAdaptive(n);
  }

  function formatEtaSec(sec) {
    if (!Number.isFinite(Number(sec))) return 'N/A';
    const s = Math.max(0, Math.round(Number(sec)));
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const rs = s % 60;
    return rs ? `${m}m ${rs}s` : `${m}m`;
  }

  function formatAgoMs(ms) {
    if (!Number.isFinite(Number(ms))) return 'N/A';
    const v = Math.max(0, Math.round(Number(ms)));
    if (v < 1000) return `${v}ms`;
    const s = Math.floor(v / 1000);
    if (s < 60) return `${s}s`;
    const m = Math.floor(s / 60);
    const rs = s % 60;
    if (m < 60) return rs ? `${m}m ${rs}s` : `${m}m`;
    const h = Math.floor(m / 60);
    const rm = m % 60;
    return rm ? `${h}h ${rm}m` : `${h}h`;
  }

  function timeframeToSec(tf) {
    const m = String(tf || '').toLowerCase().match(/^(\d+)m$/);
    if (!m) return 60;
    const mins = Number(m[1] || 1);
    if (!Number.isFinite(mins) || mins <= 0) return 60;
    return mins * 60;
  }

  function formatCountdown(totalSec) {
    const s = Math.max(0, Math.round(Number(totalSec || 0)));
    const mm = Math.floor(s / 60)
      .toString()
      .padStart(2, '0');
    const ss = (s % 60).toString().padStart(2, '0');
    return `${mm}:${ss}`;
  }

  function getCurrentDisplayPrice() {
    const c = latestDisplayCandle();
    if (c && Number.isFinite(Number(c.close))) return Number(c.close);
    const p = Number(
      state.livePrice ||
        state.status?.orderBook?.lastPrice ||
        state.status?.orderBook?.lastTradePrice ||
        state.status?.orderBook?.markPrice ||
        state.candles[state.candles.length - 1]?.close ||
        0,
    );
    return Number.isFinite(p) ? p : 0;
  }

  function updateCandleCountdown() {
    const tfSec = timeframeToSec(state.timeframe);
    const nowSec = Math.floor(Date.now() / 1000);
    const last = latestDisplayCandle();

    let nextCloseSec;
    if (last && Number.isFinite(Number(last.time))) {
      const start = Number(last.time);
      nextCloseSec = start + tfSec;
      if (nextCloseSec <= nowSec) {
        nextCloseSec = Math.floor(nowSec / tfSec) * tfSec + tfSec;
      }
    } else {
      nextCloseSec = Math.floor(nowSec / tfSec) * tfSec + tfSec;
    }

    const remain = Math.max(0, nextCloseSec - nowSec);
    state.candleCountdownText = formatCountdown(remain);
    if (el.candleCountdownBadge) {
      el.candleCountdownBadge.textContent = `Nến còn: ${state.candleCountdownText}`;
      el.candleCountdownBadge.className = remain <= 10 ? 'badge warn' : 'badge neutral';
    }
  }

  function hasNum(v) {
    return Number.isFinite(Number(v));
  }

  function tipIcon(tipKey) {
    const txt = (state.glossary[tipKey]?.short || '').replaceAll('"', '&quot;');
    if (!txt) return '';
    return ` <span class="tip-badge" data-tip="${txt}">i</span>`;
  }

  function valueClassByText(value) {
    const s = String(value || '').toUpperCase();
    if (s.includes('LONG') || s.includes('UP') || s.includes('MẠNH_MUA') || s.includes('MUA TRỘI') || s === 'CÓ')
      return 'long';
    if (s.includes('SHORT') || s.includes('DOWN') || s.includes('YẾU') || s.includes('MẠNH_BÁN') || s.includes('BÁN TRỘI'))
      return 'short';
    if (s.includes('SIDEWAY') || s.includes('NO_TRADE') || s.includes('N/A')) return 'neutral';
    return '';
  }

  function getCurrentMarketPrice() {
    return Number(
      state.livePrice ||
        state.status?.orderBook?.lastPrice ||
        state.signal?.market?.lastPrice ||
        state.candles?.[state.candles.length - 1]?.close ||
        0,
    );
  }

  function getZoneByLabel(zones, label, fallbackIdx) {
    if (!Array.isArray(zones) || !zones.length) return null;
    return zones.find((z) => z?.label === label) || zones[fallbackIdx] || null;
  }

  function renderRsGuidePanel() {
    if (!el.rsMeaningGrid || !el.livePlanGrid || !el.rsScenarioBox || !el.rsGuidePriceNow) return;

    const signal = state.signal || {};
    const sr = state.srData || signal.market?.supportResistance || {};
    const supports = Array.isArray(sr.supports) ? sr.supports : [];
    const resistances = Array.isArray(sr.resistances) ? sr.resistances : [];
    const priceNow = getCurrentMarketPrice();

    el.rsGuidePriceNow.textContent = `Giá hiện tại: ${priceNow > 0 ? formatPrice(priceNow) : 'N/A'}`;

    const r1 = getZoneByLabel(resistances, 'R1', 0);
    const r2 = getZoneByLabel(resistances, 'R2', 1);
    const r3 = getZoneByLabel(resistances, 'R3', 2);
    const s1 = getZoneByLabel(supports, 'S1', 0);
    const s2 = getZoneByLabel(supports, 'S2', 1);
    const s3 = getZoneByLabel(supports, 'S3', 2);

    const meaningCards = [
      {
        title: 'R1 (Resistance/Kháng cự gần)',
        zone: r1,
        cls: 'short',
        note: 'Vùng kháng cự đầu tiên, giá chạm dễ bị chốt lời.',
      },
      {
        title: 'R2 (Resistance/Kháng cự giữa)',
        zone: r2,
        cls: 'short',
        note: 'Nếu vượt và giữ được R1, mục tiêu kế tiếp thường là R2.',
      },
      {
        title: 'R3 (Resistance/Kháng cự mạnh)',
        zone: r3,
        cls: 'short',
        note: 'Kháng cự xa và mạnh, rủi ro đảo chiều cao hơn.',
      },
      {
        title: 'S1 (Support/Hỗ trợ gần)',
        zone: s1,
        cls: 'long',
        note: 'Vùng hỗ trợ đầu tiên, thường có lực đỡ ngắn hạn.',
      },
      {
        title: 'S2 (Support/Hỗ trợ giữa)',
        zone: s2,
        cls: 'long',
        note: 'Nếu thủng S1 thì S2 là vùng đỡ tiếp theo.',
      },
      {
        title: 'S3 (Support/Hỗ trợ mạnh)',
        zone: s3,
        cls: 'long',
        note: 'Hỗ trợ sâu, thường là vùng phản ứng mạnh nếu giá giảm sâu.',
      },
    ];

    el.rsMeaningGrid.innerHTML = meaningCards
      .map((item) => {
        const price = item.zone?.price;
        const strength = item.zone?.strength || 'N/A';
        return `<div class="rs-card ${item.cls}">
          <div class="k">${item.title}</div>
          <div class="v">${Number.isFinite(Number(price)) ? formatPrice(price) : 'N/A'}${
            Number.isFinite(Number(price)) ? ` <small>(${strength})</small>` : ''
          }</div>
          <div class="note">${item.note}</div>
        </div>`;
      })
      .join('');

    const scenarioLines = [];
    if (priceNow > 0 && r1?.price) {
      if (priceNow < r1.price) {
        scenarioLines.push(`- Giá đang dưới R1: khi chạm R1 cần quan sát phản ứng nến/volume trước khi LONG đuổi.`);
      } else if (r2?.price && priceNow >= r1.price && priceNow < r2.price) {
        scenarioLines.push(`- Giá đã vượt R1: nếu retest giữ R1 tốt thì mục tiêu kế tiếp thường là R2.`);
      } else if (r2?.price && r3?.price && priceNow >= r2.price && priceNow < r3.price) {
        scenarioLines.push(`- Giá đang ở vùng R2: ưu tiên siết SL, không nên LONG muộn gần kháng cự.`);
      } else if (r3?.price && priceNow >= r3.price) {
        scenarioLines.push(`- Giá đang chạm/vượt R3: vùng nhạy cảm, ưu tiên bảo vệ lãi và tránh FOMO LONG.`);
      }
    }
    if (priceNow > 0 && s1?.price) {
      if (priceNow > s1.price) {
        scenarioLines.push(`- Giá còn trên S1: thủng S1 có thể rơi về S2, cần quản trị rủi ro chặt.`);
      } else if (s2?.price && priceNow <= s1.price && priceNow > s2.price) {
        scenarioLines.push(`- Giá đã thủng S1: vùng S2 là hỗ trợ kế tiếp để theo dõi phản ứng.`);
      } else if (s2?.price && s3?.price && priceNow <= s2.price && priceNow > s3.price) {
        scenarioLines.push(`- Giá gần S2: nếu không giữ được S2, xác suất về S3 tăng.`);
      } else if (s3?.price && priceNow <= s3.price) {
        scenarioLines.push(`- Giá chạm S3: vùng hỗ trợ sâu, dễ biến động mạnh hai chiều.`);
      }
    }
    if (!scenarioLines.length) {
      scenarioLines.push('- Chưa đủ dữ liệu R/S để mô phỏng kịch bản giá.');
    }

    el.rsScenarioBox.innerHTML = `<strong>Kịch bản nhanh theo giá hiện tại:</strong><br>${scenarioLines.join('<br>')}`;

    const hint = state.entryHint || buildRealtimeEntryHint();
    const side =
      (signal.side === 'LONG' || signal.side === 'SHORT') ? signal.side : hint?.side || 'NO_TRADE';
    const entryText =
      hasNum(signal.entryMin) && hasNum(signal.entryMax)
        ? `${formatPrice(signal.entryMin)} - ${formatPrice(signal.entryMax)}`
        : hasNum(hint?.price)
        ? formatPrice(hint.price)
        : 'N/A';
    const sl = hasNum(signal.stopLoss) ? signal.stopLoss : hint?.stopLoss;
    const tp1 = hasNum(signal.tp1) ? signal.tp1 : hint?.tp1;
    const tp2 = hasNum(signal.tp2) ? signal.tp2 : hint?.tp2;
    const tp3 = hasNum(signal.tp3) ? signal.tp3 : hint?.tp3;
    const leverage = state.status?.activePosition?.leverage || state.status?.binance?.leverage || 'N/A';
    const conf = hasNum(signal.confidence) ? `${formatNum(signal.confidence, 0)}/100` : 'N/A';
    const rr = hasNum(signal.rr) ? formatNum(signal.rr, 2) : 'N/A';

    const liveCards = [
      { k: 'Lệnh hiện tại', v: side, cls: valueClassByText(side) || 'neutral' },
      { k: 'Entry đề xuất', v: entryText, cls: 'neutral' },
      { k: 'SL đề xuất', v: formatPrice(sl), cls: 'short' },
      { k: 'TP1', v: formatPrice(tp1), cls: 'long' },
      { k: 'TP2', v: formatPrice(tp2), cls: 'long' },
      { k: 'TP3', v: formatPrice(tp3), cls: 'long' },
      { k: 'Đòn bẩy', v: leverage === 'N/A' ? 'N/A' : `x${leverage}`, cls: 'neutral' },
      { k: 'Confidence / RR', v: `${conf} | RR ${rr}`, cls: 'neutral' },
    ];

    el.livePlanGrid.innerHTML = liveCards
      .map(
        (item) => `<div class="rs-card ${item.cls}">
          <div class="k">${item.k}</div>
          <div class="v">${item.v}</div>
          <div class="note">${
            item.k === 'Lệnh hiện tại'
              ? 'Ưu tiên vào theo vùng đẹp, không đuổi giá.'
              : item.k === 'Đòn bẩy'
              ? 'Đòn bẩy cao tăng rủi ro cháy tài khoản.'
              : 'Thông số theo dữ liệu realtime hiện tại.'
          }</div>
        </div>`,
      )
      .join('');
  }

  function normalizeCandles(list, maxLen = 2000) {
    if (!Array.isArray(list) || list.length === 0) return [];
    const map = new Map();
    for (const c of list) {
      const t = Number(c?.time);
      if (!Number.isFinite(t)) continue;
      map.set(t, {
        time: t,
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close),
        volume: Number(c.volume || 0),
        quoteVolume: Number(c.quoteVolume || c.q || 0),
        isClosed: Boolean(c.isClosed),
      });
    }
    const sorted = Array.from(map.values()).sort((a, b) => a.time - b.time);
    return sorted.length > maxLen ? sorted.slice(sorted.length - maxLen) : sorted;
  }

  function getLatestIndicatorSnapshot() {
    if (!state.candles.length) return { rsi: null, ma7: null, ma25: null };
    const ma7 = calcSma(state.candles, 7);
    const ma25 = calcSma(state.candles, 25);
    const ma99 = calcSma(state.candles, 99);
    const ma200 = calcSma(state.candles, 200);
    const rsi = calcRsi(state.candles, 14);
    return {
      rsi: rsi.length ? rsi[rsi.length - 1].value : null,
      ma7: ma7.length ? ma7[ma7.length - 1].value : null,
      ma25: ma25.length ? ma25[ma25.length - 1].value : null,
      ma99: ma99.length ? ma99[ma99.length - 1].value : null,
      ma200: ma200.length ? ma200[ma200.length - 1].value : null,
    };
  }

  function recalcDisplayCandles() {
    if (state.timeframe === '1m') state.candles = state.candles1m.slice(-700);
    else if (state.timeframe === '3m') state.candles = state.candles3m.slice(-700);
    else if (state.timeframe === '5m') state.candles = state.candles5m.slice(-700);
    else if (state.timeframe === '15m') state.candles = state.candles15m.slice(-700);
    else state.candles = state.candles1m.slice(-700);
    state.candles = normalizeCandles(state.candles, 700);
  }

  function latestDisplayCandle() {
    if (state.timeframe === '3m') {
      if (state.candles3m.length) return state.candles3m[state.candles3m.length - 1];
      return null;
    }
    if (state.timeframe === '5m') {
      if (state.candles5m.length) return state.candles5m[state.candles5m.length - 1];
      return null;
    }
    if (state.timeframe === '15m') {
      if (state.candles15m.length) return state.candles15m[state.candles15m.length - 1];
      return null;
    }
    if (!state.candles1m.length) return null;
    return state.candles1m[state.candles1m.length - 1];
  }

  function updateRealtimeCandle() {
    const c = latestDisplayCandle();
    if (!c) return;

    if (Number.isFinite(Number(c.close))) {
      state.livePrice = Number(c.close);
    }

    candleSeries.update(c);
    if (state.toggles.volume) {
      volumeSeries.update({
        time: c.time,
        value: c.volume,
        color: c.close >= c.open ? 'rgba(22,196,127,0.45)' : 'rgba(234,84,85,0.45)',
      });
    }
    updateCandleCountdown();
    updateChartFootPriceOnly();
    drawCandleLiquidityTags();
  }

  function applyIncomingCandle(timeframe, candle) {
    if (!candle || !Number.isFinite(candle.time)) return;
    const list =
      timeframe === '1m'
        ? state.candles1m
        : timeframe === '3m'
        ? state.candles3m
        : timeframe === '5m'
        ? state.candles5m
        : timeframe === '15m'
        ? state.candles15m
        : state.candles1m;
    const last = list[list.length - 1];
    if (!last) {
      list.push(candle);
      return;
    }

    if (last.time === candle.time) {
      list[list.length - 1] = { ...last, ...candle };
      return;
    }

    if (candle.time > last.time) {
      list.push(candle);
      if (list.length > 2000) list.splice(0, list.length - 2000);
      return;
    }

    const idx = list.findIndex((c) => c.time === candle.time);
    if (idx >= 0) {
      list[idx] = candle;
    } else {
      list.push(candle);
      const normalized = normalizeCandles(list, 2000);
      list.length = 0;
      list.push(...normalized);
    }
  }

  function calcSma(candles, period) {
    const out = [];
    let sum = 0;
    for (let i = 0; i < candles.length; i += 1) {
      sum += candles[i].close;
      if (i >= period) sum -= candles[i - period].close;
      if (i >= period - 1) out.push({ time: candles[i].time, value: sum / period });
    }
    return out;
  }

  function calcRsi(candles, period = 14) {
    if (candles.length < period + 1) return [];
    const closes = candles.map((c) => c.close);

    let gain = 0;
    let loss = 0;
    for (let i = 1; i <= period; i += 1) {
      const diff = closes[i] - closes[i - 1];
      if (diff > 0) gain += diff;
      else loss += Math.abs(diff);
    }

    let avgGain = gain / period;
    let avgLoss = loss / period;

    const points = [{ time: candles[period].time, value: avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss) }];

    for (let i = period + 1; i < closes.length; i += 1) {
      const diff = closes[i] - closes[i - 1];
      const g = diff > 0 ? diff : 0;
      const l = diff < 0 ? Math.abs(diff) : 0;
      avgGain = (avgGain * (period - 1) + g) / period;
      avgLoss = (avgLoss * (period - 1) + l) / period;
      const rsi = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
      points.push({ time: candles[i].time, value: rsi });
    }

    return points;
  }

  function calcAtrPct(candles, period = 14) {
    if (!Array.isArray(candles) || candles.length < period + 1) return null;
    const trs = [];
    for (let i = 1; i < candles.length; i += 1) {
      const c = candles[i];
      const prev = candles[i - 1];
      const tr = Math.max(
        Number(c.high) - Number(c.low),
        Math.abs(Number(c.high) - Number(prev.close)),
        Math.abs(Number(c.low) - Number(prev.close)),
      );
      trs.push(tr);
    }
    if (trs.length < period) return null;
    const recent = trs.slice(-period);
    const atr = recent.reduce((s, v) => s + v, 0) / period;
    const close = Number(candles[candles.length - 1]?.close || 0);
    if (!(close > 0)) return null;
    return atr / close;
  }

  function buildLocalPressure(candles) {
    if (!Array.isArray(candles) || candles.length < 6) return {};
    const slice = candles.slice(-8);
    let buyRaw = 0;
    let sellRaw = 0;
    let bodyRatioSum = 0;
    let wickPenaltySum = 0;
    const vols = slice.map((c) => Number(c.volume || 0));

    for (const c of slice) {
      const open = Number(c.open);
      const close = Number(c.close);
      const high = Number(c.high);
      const low = Number(c.low);
      const vol = Number(c.volume || 0);
      const range = Math.max(high - low, 1e-8);
      const body = Math.abs(close - open);
      const closePos = (close - low) / range;
      const upperWick = Math.max(0, high - Math.max(open, close));
      const lowerWick = Math.max(0, Math.min(open, close) - low);

      bodyRatioSum += body / range;
      wickPenaltySum += Math.abs(upperWick - lowerWick) / range;

      const base = body * vol;
      buyRaw += base * closePos;
      sellRaw += base * (1 - closePos);
      if (close > open) buyRaw += base * 0.12;
      if (close < open) sellRaw += base * 0.12;
    }

    const total = Math.max(1e-8, buyRaw + sellRaw);
    const buyPressure = (buyRaw / total) * 100;
    const sellPressure = (sellRaw / total) * 100;

    const avgVol = vols.slice(0, -1).reduce((s, v) => s + v, 0) / Math.max(1, vols.length - 1);
    const lastVol = vols[vols.length - 1] || 0;
    const deltaVolumeTrend = avgVol > 0 ? ((lastVol - avgVol) / avgVol) * 100 : 0;

    const firstOpen = Number(slice[0]?.open || 0);
    const lastClose = Number(slice[slice.length - 1]?.close || 0);
    const priceSpeed = firstOpen > 0 ? ((lastClose - firstOpen) / firstOpen) / slice.length : 0;
    const candleStrength = Math.max(0, Math.min(100, (bodyRatioSum / slice.length) * 100));
    const absorptionScore = Math.max(0, Math.min(100, 100 - (wickPenaltySum / slice.length) * 100));

    let volumeStatus = 'TRUNG_BÌNH';
    if (deltaVolumeTrend >= 20) volumeStatus = 'CAO';
    else if (deltaVolumeTrend <= -20) volumeStatus = 'THẤP';

    return {
      buyPressure,
      sellPressure,
      candleStrength,
      deltaVolumeTrend,
      priceSpeed,
      absorptionScore,
      volumeStatus,
    };
  }

  function buildLocalSupportResistance(candles) {
    if (!Array.isArray(candles) || candles.length < 40) return null;
    const src = candles.slice(-260);
    const current = Number(src[src.length - 1]?.close || 0);
    if (!(current > 0)) return null;

    const highs = [];
    const lows = [];
    for (let i = 2; i < src.length - 2; i += 1) {
      const c = src[i];
      const h = Number(c.high);
      const l = Number(c.low);
      const isHigh =
        h > Number(src[i - 1].high) &&
        h > Number(src[i - 2].high) &&
        h >= Number(src[i + 1].high) &&
        h >= Number(src[i + 2].high);
      const isLow =
        l < Number(src[i - 1].low) &&
        l < Number(src[i - 2].low) &&
        l <= Number(src[i + 1].low) &&
        l <= Number(src[i + 2].low);
      if (isHigh) highs.push(h);
      if (isLow) lows.push(l);
    }

    const cluster = (levels, type) => {
      if (!levels.length) return [];
      const sorted = [...levels].sort((a, b) => (type === 'resistance' ? b - a : a - b));
      const groups = [];
      const band = current * 0.0012;
      for (const p of sorted) {
        const found = groups.find((g) => Math.abs(g.price - p) <= band);
        if (found) {
          found.tests += 1;
          found.price = (found.price * (found.tests - 1) + p) / found.tests;
        } else {
          groups.push({ price: p, tests: 1 });
        }
      }
      return groups.slice(0, 3).map((g, idx) => {
        const strength =
          g.tests >= 4 ? 'EXTREME' : g.tests >= 3 ? 'STRONG' : g.tests >= 2 ? 'NORMAL' : 'WEAK';
        return {
          id: `${type}-${idx + 1}-${Math.round(g.price)}`,
          label: `${type === 'support' ? 'S' : 'R'}${idx + 1}`,
          displayLabel: `${type === 'support' ? 'S' : 'R'}${idx + 1} ${
            type === 'support' ? 'Support (Hỗ trợ)' : 'Resistance (Kháng cự)'
          } (local)`,
          price: g.price,
          strength,
          tests: g.tests,
          timeframe: state.timeframe,
          type,
          top: g.price * 1.00045,
          bottom: g.price * 0.99955,
          opacity: 0.16,
          lineWidth: strength === 'EXTREME' ? 2 : 1,
        };
      });
    };

    const supports = cluster(lows, 'support').sort((a, b) => b.price - a.price);
    const resistances = cluster(highs, 'resistance').sort((a, b) => a.price - b.price);
    const zones = [...supports, ...resistances];

    const nearestSupport = supports.filter((s) => s.price <= current).sort((a, b) => b.price - a.price)[0] || null;
    const nearestResistance =
      resistances.filter((r) => r.price >= current).sort((a, b) => a.price - b.price)[0] || null;
    const distToSupportPct = nearestSupport ? Math.abs(current - nearestSupport.price) / current : null;
    const distToResistancePct = nearestResistance ? Math.abs(nearestResistance.price - current) / current : null;

    return {
      supports,
      resistances,
      zones,
      keyLevels: {
        support: nearestSupport?.price ?? null,
        resistance: nearestResistance?.price ?? null,
        distToSupportPct,
        distToResistancePct,
        nearStrongSupport:
          Boolean(nearestSupport) &&
          ['STRONG', 'EXTREME'].includes(nearestSupport.strength) &&
          distToSupportPct !== null &&
          distToSupportPct <= 0.0022,
        nearStrongResistance:
          Boolean(nearestResistance) &&
          ['STRONG', 'EXTREME'].includes(nearestResistance.strength) &&
          distToResistancePct !== null &&
          distToResistancePct <= 0.0022,
      },
      potential: {
        longZone:
          nearestSupport && distToSupportPct !== null && distToSupportPct <= 0.0032
            ? {
                ...nearestSupport,
                intent: 'LONG',
                blink: true,
                score: Math.max(60, 90 - Math.round(distToSupportPct * 10000)),
                mode: 'WATCH',
              }
            : null,
        shortZone:
          nearestResistance && distToResistancePct !== null && distToResistancePct <= 0.0032
            ? {
                ...nearestResistance,
                intent: 'SHORT',
                blink: true,
                score: Math.max(60, 90 - Math.round(distToResistancePct * 10000)),
                mode: 'WATCH',
              }
            : null,
      },
    };
  }

  function clearPriceLines() {
    for (const line of state.priceLines) {
      candleSeries.removePriceLine(line);
    }
    state.priceLines = [];
  }

  function clearSupportResistanceVisuals() {
    for (const line of state.srPriceLines) {
      candleSeries.removePriceLine(line);
    }
    state.srPriceLines = [];
    srZoneOverlay.innerHTML = '';
  }

  function clearMaMiniTags() {
    maTagOverlay.innerHTML = '';
  }

  function clearCandleLiquidityTags() {
    candleLiqOverlay.innerHTML = '';
  }

  function formatLiquidityShort(value) {
    const n = Number(value || 0);
    if (!Number.isFinite(n)) return 'N/A';
    const abs = Math.abs(n);
    if (abs >= 1e9) return `${(n / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `${(n / 1e6).toFixed(2)}M`;
    if (abs >= 1e3) return `${(n / 1e3).toFixed(2)}K`;
    if (abs >= 1) return n.toFixed(2);
    return n.toFixed(6);
  }

  function calcCandleLiquidityUSDT(candle) {
    if (!candle) return null;
    const q = Number(candle.quoteVolume || 0);
    if (Number.isFinite(q) && q > 0) return q;
    const vol = Number(candle.volume || 0);
    const px = Number(candle.close || 0);
    if (!(Number.isFinite(vol) && vol > 0 && Number.isFinite(px) && px > 0)) return null;
    return vol * px;
  }

  function drawCandleLiquidityTags(force = false) {
    const now = Date.now();
    if (!force && now - state.lastLiquidityDrawAt < 80) return;
    state.lastLiquidityDrawAt = now;

    clearCandleLiquidityTags();
    if (!state.toggles.candleLiquidity) return;
    if (!Array.isArray(state.candles) || !state.candles.length) return;

    const logical = chart.timeScale().getVisibleLogicalRange();
    const fullFrom = 0;
    const fullTo = state.candles.length - 1;
    let from = fullFrom;
    let to = fullTo;
    if (logical && Number.isFinite(logical.from) && Number.isFinite(logical.to)) {
      from = Math.max(fullFrom, Math.floor(logical.from) - 2);
      to = Math.min(fullTo, Math.ceil(logical.to) + 2);
    }

    const minSpacingPx = 14;
    let prevX = -Infinity;

    for (let i = from; i <= to; i += 1) {
      const c = state.candles[i];
      if (!c) continue;

      const x = chart.timeScale().timeToCoordinate(c.time);
      if (!Number.isFinite(x)) continue;
      const isLast = i === to;
      if (!isLast && x - prevX < minSpacingPx) continue;

      const bodyTop = Math.max(Number(c.open || 0), Number(c.close || 0));
      const yRaw = candleSeries.priceToCoordinate(bodyTop);
      if (!Number.isFinite(yRaw)) continue;

      const liq = calcCandleLiquidityUSDT(c);
      if (!(Number.isFinite(liq) && liq > 0)) continue;

      const tag = document.createElement('div');
      tag.className = `candle-liq-tag ${Number(c.close) >= Number(c.open) ? 'up' : 'down'}`;
      tag.style.left = `${x}px`;
      tag.style.top = `${Math.max(10, yRaw - 3)}px`;
      tag.textContent = `LQ ${formatLiquidityShort(liq)}`;

      const quote = Number(c.quoteVolume || 0);
      const exactLiq = quote > 0 ? quote : liq;
      tag.title = `Thanh khoản nến: ${exactLiq.toLocaleString('en-US', { maximumFractionDigits: 8 })} USDT`;

      candleLiqOverlay.appendChild(tag);
      prevX = x;
    }
  }

  function srColor(type, strength = 'NORMAL') {
    const alpha =
      strength === 'EXTREME' ? 0.95 : strength === 'STRONG' ? 0.82 : strength === 'NORMAL' ? 0.68 : 0.5;
    if (type === 'support') return `rgba(22,196,127,${alpha})`;
    return `rgba(234,84,85,${alpha})`;
  }

  function drawSupportResistance(srData) {
    clearSupportResistanceVisuals();
    if (!state.toggles.zones) return;
    if (!srData || !Array.isArray(srData.zones) || !srData.zones.length) return;

    const zones = srData.zones.slice(0, 8);
    for (const zone of zones) {
      const top = Number(zone.top);
      const bottom = Number(zone.bottom);
      const price = Number(zone.price);
      if (!Number.isFinite(top) || !Number.isFinite(bottom) || !Number.isFinite(price)) continue;

      const color = srColor(zone.type, zone.strength);
      const line = candleSeries.createPriceLine({
        price,
        color,
        lineWidth: zone.lineWidth || 1,
        lineStyle: LightweightCharts.LineStyle.Solid,
        axisLabelVisible: true,
        title: `${zone.displayLabel || zone.label || ''} ${zone.strength || ''}`.trim(),
      });
      state.srPriceLines.push(line);

      const yTop = candleSeries.priceToCoordinate(top);
      const yBottom = candleSeries.priceToCoordinate(bottom);
      if (!Number.isFinite(yTop) || !Number.isFinite(yBottom)) continue;

      const topPx = Math.min(yTop, yBottom);
      const heightPx = Math.max(2, Math.abs(yBottom - yTop));

      const zoneEl = document.createElement('div');
      zoneEl.className = `sr-zone ${zone.type === 'support' ? 'support' : 'resistance'} strength-${String(
        zone.strength || 'NORMAL',
      ).toLowerCase()}`;
      if (zone.intent === 'LONG' || zone.blink === true && zone.type === 'support') {
        zoneEl.classList.add('blink-long');
      }
      if (zone.intent === 'SHORT' || zone.blink === true && zone.type === 'resistance') {
        zoneEl.classList.add('blink-short');
      }
      zoneEl.style.top = `${topPx}px`;
      zoneEl.style.height = `${heightPx}px`;
      zoneEl.style.opacity = Number.isFinite(zone.opacity) ? String(zone.opacity) : '';

      const label = document.createElement('span');
      label.className = 'sr-zone-label';
      label.textContent = `${zone.displayLabel || zone.label || ''} ${zone.strength || ''}`.trim();
      zoneEl.appendChild(label);

      srZoneOverlay.appendChild(zoneEl);
    }

    drawPotentialTriggerLine(srData?.potential?.longZone, 'LONG');
    drawPotentialTriggerLine(srData?.potential?.shortZone, 'SHORT');
    drawEntryHintLine();
  }

  function drawPotentialTriggerLine(potentialZone, side) {
    if (!potentialZone) return;
    const price = Number(potentialZone.price);
    if (!Number.isFinite(price) || price <= 0) return;

    const y = candleSeries.priceToCoordinate(price);
    if (!Number.isFinite(y)) return;

    const lineEl = document.createElement('div');
    lineEl.className = `sr-signal-line ${side === 'LONG' ? 'long' : 'short'}`;
    lineEl.style.top = `${y}px`;

    const label = document.createElement('span');
    label.className = 'sr-signal-line-label';
    label.textContent = `${side} Trigger (Điểm bắt lệnh) ${formatPrice(price)} | ${potentialZone.mode || 'WATCH'} ${formatNum(
      potentialZone.score,
      0,
    )}`;
    lineEl.appendChild(label);
    srZoneOverlay.appendChild(lineEl);
  }

  function buildRealtimeEntryHint() {
    const signal = state.signal || {};
    const side = signal.side || 'NO_TRADE';
    const pre = signal.preSignal?.selected;

    if (
      (side === 'LONG' || side === 'SHORT') &&
      Number.isFinite(Number(signal.entryMin)) &&
      Number.isFinite(Number(signal.entryMax))
    ) {
      const price = (Number(signal.entryMin) + Number(signal.entryMax)) / 2;
      return {
        price,
        side,
        mode: 'PLAN',
        probability: Number(signal.confidence || 0),
        etaSec: 0,
        stopLoss: Number(signal.stopLoss),
        tp1: Number(signal.tp1),
        tp2: Number(signal.tp2),
        tp3: Number(signal.tp3),
      };
    }

    if (pre && Number.isFinite(Number(pre.triggerPrice)) && (pre.side === 'LONG' || pre.side === 'SHORT')) {
      return {
        price: Number(pre.triggerPrice),
        side: pre.side,
        mode: `DỰ BÁO ${pre.mode || 'WATCH'}`,
        probability: Number(pre.probability || 0),
        etaSec: Number.isFinite(Number(pre.etaSec)) ? Number(pre.etaSec) : null,
        stopLoss: Number(pre.stopLoss),
        tp1: Number(pre.tp1),
        tp2: Number(pre.tp2),
        tp3: Number(pre.tp3),
      };
    }

    const longZone = state.srData?.potential?.longZone;
    const shortZone = state.srData?.potential?.shortZone;
    const priceNow = Number(state.livePrice || state.candles?.[state.candles.length - 1]?.close || 0);

    const candidates = [longZone, shortZone]
      .filter((z) => z && Number.isFinite(Number(z.price)))
      .map((z) => ({
        price: Number(z.price),
        side: z.type === 'LONG_ZONE' ? 'LONG' : 'SHORT',
        score: Number(z.score || 0),
        distance: priceNow > 0 ? Math.abs(Number(z.price) - priceNow) / priceNow : 999,
        mode: z.mode || 'WATCH',
      }))
      .sort((a, b) => {
        if ((b.score || 0) !== (a.score || 0)) return (b.score || 0) - (a.score || 0);
        return a.distance - b.distance;
      });

    const picked = candidates.length
      ? candidates[0]
      : (() => {
          if (!(priceNow > 0)) return null;
          const local = getLatestIndicatorSnapshot();
          const ma7 = Number(local.ma7 || 0);
          const ma25 = Number(local.ma25 || 0);
          const sideByMa = ma7 > 0 && ma25 > 0 && ma7 >= ma25 ? 'LONG' : 'SHORT';
          return {
            price: priceNow,
            side: sideByMa,
            score: 55,
            distance: 0,
            mode: 'MA_HINT',
          };
        })();

    if (!picked) return null;
    const atr = Number(state.signal?.market?.atr2m || 0);
    const riskDistance = Math.max(
      picked.price * 0.0014,
      atr > 0 ? atr * 0.7 : 0,
    );
    const isLong = picked.side === 'LONG';
    const stopLoss = isLong ? picked.price - riskDistance : picked.price + riskDistance;
    const tp1 = isLong ? picked.price + riskDistance : picked.price - riskDistance;
    const tp2 = isLong ? picked.price + riskDistance * 2 : picked.price - riskDistance * 2;
    const tp3 = isLong ? picked.price + riskDistance * 3 : picked.price - riskDistance * 3;
    return {
      ...picked,
      probability: Number(picked.score || 0),
      etaSec: null,
      stopLoss,
      tp1,
      tp2,
      tp3,
    };
  }

  function drawEntryHintLine() {
    const hint = buildRealtimeEntryHint();
    state.entryHint = hint;
    if (!hint || !Number.isFinite(hint.price)) return;

    const y = candleSeries.priceToCoordinate(hint.price);
    if (!Number.isFinite(y)) return;

    const lineEl = document.createElement('div');
    lineEl.className = 'entry-hint-line';
    lineEl.style.top = `${y}px`;

    const label = document.createElement('span');
    label.className = 'entry-hint-label';
    const probText = Number.isFinite(Number(hint.probability)) ? ` | XS ${formatNum(hint.probability, 0)}%` : '';
    const etaText = Number.isFinite(Number(hint.etaSec)) ? ` | ETA ${formatEtaSec(hint.etaSec)}` : '';
    label.textContent = `Gợi ý bắt lệnh realtime: ${hint.side} @ ${formatPrice(hint.price)} (${hint.mode})${probText}${etaText} | SL ${formatPrice(
      hint.stopLoss,
    )} | TP1 ${formatPrice(hint.tp1)}`;
    lineEl.appendChild(label);

    srZoneOverlay.appendChild(lineEl);
    drawEntryHintTargetLine(hint.stopLoss, 'SL dự báo');
    drawEntryHintTargetLine(hint.tp1, 'TP1 dự báo');
    drawEntryHintTargetLine(hint.tp2, 'TP2 dự báo');
    drawEntryHintTargetLine(hint.tp3, 'TP3 dự báo');
  }

  function drawEntryHintTargetLine(price, text) {
    if (!Number.isFinite(Number(price))) return;
    const y = candleSeries.priceToCoordinate(Number(price));
    if (!Number.isFinite(y)) return;

    const lineEl = document.createElement('div');
    lineEl.className = 'entry-hint-subline';
    lineEl.style.top = `${y}px`;

    const label = document.createElement('span');
    label.className = 'entry-hint-sub-label';
    label.textContent = `${text}: ${formatPrice(price)}`;
    lineEl.appendChild(label);

    srZoneOverlay.appendChild(lineEl);
  }

  function drawMaMiniTags() {
    clearMaMiniTags();
    if (!state.toggles.ma) return;
    if (!state.candles?.length) return;

    const lastTime = state.latestIndicators.lastTime || state.candles[state.candles.length - 1]?.time;
    const xRaw = chart.timeScale().timeToCoordinate(lastTime);
    const containerWidth = el.chartContainer.clientWidth || 900;
    const baseX = Number.isFinite(xRaw) ? xRaw : containerWidth - 110;

    const tags = [
      { key: 'ma7', label: 'MA7', color: '#ffd166', value: state.latestIndicators.ma7 },
      { key: 'ma25', label: 'MA25', color: '#4ea5d9', value: state.latestIndicators.ma25 },
      { key: 'ma99', label: 'MA99', color: '#b28dff', value: state.latestIndicators.ma99 },
      { key: 'ma200', label: 'MA200', color: '#8f9db1', value: state.latestIndicators.ma200 },
    ];

    for (const t of tags) {
      const price = Number(t.value);
      if (!Number.isFinite(price)) continue;
      const y = candleSeries.priceToCoordinate(price);
      if (!Number.isFinite(y)) continue;

      const wrap = document.createElement('div');
      wrap.className = 'ma-mini-tag';
      wrap.style.top = `${y}px`;
      wrap.style.left = `${Math.max(10, Math.min(containerWidth - 160, baseX + 8))}px`;

      const dot = document.createElement('span');
      dot.className = 'ma-mini-dot';
      dot.style.background = t.color;
      dot.style.boxShadow = `0 0 10px ${t.color}`;

      const txt = document.createElement('span');
      txt.className = 'ma-mini-text';
      txt.style.borderColor = `${t.color}99`;
      txt.textContent = `${t.label} ${formatPrice(price)}`;

      wrap.appendChild(dot);
      wrap.appendChild(txt);
      maTagOverlay.appendChild(wrap);
    }
  }

  function drawTradeZones(signal) {
    clearPriceLines();
    if (!state.toggles.zones || !signal || signal.side === 'NO_TRADE') return;

    const createLine = (price, color, title) => {
      const line = candleSeries.createPriceLine({
        price,
        color,
        lineWidth: 1,
        lineStyle: LightweightCharts.LineStyle.Dashed,
        axisLabelVisible: true,
        title,
      });
      state.priceLines.push(line);
    };

    createLine(signal.entryMin, 'rgba(78,165,217,0.7)', 'Entry Min');
    createLine(signal.entryMax, 'rgba(78,165,217,0.7)', 'Entry Max');
    createLine(signal.stopLoss, 'rgba(234,84,85,0.85)', 'SL');
    createLine(signal.tp1, 'rgba(22,196,127,0.65)', 'TP1');
    createLine(signal.tp2, 'rgba(22,196,127,0.8)', 'TP2');
    createLine(signal.tp3, 'rgba(22,196,127,0.95)', 'TP3');
  }

  function setSeriesVisible(series, visible) {
    series.applyOptions({ visible });
  }

  function renderChart() {
    if (!state.candles.length) return;

    const candles = normalizeCandles(state.candles, 700);
    state.candles = candles;
    candleSeries.setData(candles);

    if (state.toggles.volume) {
      const v = candles.map((c) => ({
        time: c.time,
        value: c.volume,
        color: c.close >= c.open ? 'rgba(22,196,127,0.45)' : 'rgba(234,84,85,0.45)',
      }));
      volumeSeries.setData(v);
    } else {
      volumeSeries.setData([]);
    }
    const volumeContainer = document.getElementById('volumeContainer');
    if (volumeContainer) {
      volumeContainer.style.display = state.toggles.volume ? 'block' : 'none';
    }

    setSeriesVisible(ma7Series, state.toggles.ma);
    setSeriesVisible(ma25Series, state.toggles.ma);
    setSeriesVisible(ma99Series, state.toggles.ma);
    setSeriesVisible(ma200Series, state.toggles.ma);

    const ma7Data = calcSma(candles, 7);
    const ma25Data = calcSma(candles, 25);
    const ma99Data = calcSma(candles, 99);
    const ma200Data = calcSma(candles, 200);
    if (state.toggles.ma) {
      ma7Series.setData(ma7Data);
      ma25Series.setData(ma25Data);
      ma99Series.setData(ma99Data);
      ma200Series.setData(ma200Data);
    }

    const rsiPoints = calcRsi(candles, 14);
    setSeriesVisible(rsiSeries, state.toggles.rsi);
    if (state.toggles.rsi) {
      rsiSeries.setData(rsiPoints);
      const constants30 = rsiPoints.map((p) => ({ time: p.time, value: 30 }));
      const constants50 = rsiPoints.map((p) => ({ time: p.time, value: 50 }));
      const constants70 = rsiPoints.map((p) => ({ time: p.time, value: 70 }));
      rsiOverSold.setData(constants30);
      rsiOverBought.setData(constants70);
      rsiMid.setData(constants50);
    } else {
      rsiSeries.setData([]);
      rsiOverSold.setData([]);
      rsiOverBought.setData([]);
      rsiMid.setData([]);
    }

    rsiChart.applyOptions({ height: state.toggles.rsi ? 130 : 0 });

    if (state.toggles.markers && state.signalMarkers.length) {
      candleSeries.setMarkers(state.signalMarkers);
    } else {
      candleSeries.setMarkers([]);
    }

    drawTradeZones(state.signal);
    drawSupportResistance(state.srData);

    const ma7Latest = ma7Data.length ? ma7Data[ma7Data.length - 1].value : null;
    const ma25Latest = ma25Data.length ? ma25Data[ma25Data.length - 1].value : null;
    const ma99Latest = ma99Data.length ? ma99Data[ma99Data.length - 1].value : null;
    const ma200Latest = ma200Data.length ? ma200Data[ma200Data.length - 1].value : null;
    const rsiLatest = rsiPoints.length ? rsiPoints[rsiPoints.length - 1].value : null;
    const closeLatest = candles.length ? candles[candles.length - 1].close : null;
    const liqLatest = candles.length ? calcCandleLiquidityUSDT(candles[candles.length - 1]) : null;
    if (Number.isFinite(Number(closeLatest))) {
      state.livePrice = Number(closeLatest);
    }
    if (el.indicatorLegend) {
      el.indicatorLegend.textContent = [
        `Khung: ${state.timeframe}`,
        `Close: ${formatPrice(closeLatest)}`,
        `MA7: ${formatPrice(ma7Latest)}`,
        `MA25: ${formatPrice(ma25Latest)}`,
        `MA99: ${formatPrice(ma99Latest)}`,
        `MA200: ${formatPrice(ma200Latest)}`,
        `RSI14: ${formatNum(rsiLatest, 2)}`,
        `LQ nến: ${formatLiquidityShort(liqLatest)} USDT`,
      ].join(' | ');
    }

    state.latestIndicators = {
      ma7: ma7Latest,
      ma25: ma25Latest,
      ma99: ma99Latest,
      ma200: ma200Latest,
      lastTime: candles.length ? candles[candles.length - 1].time : null,
    };
    drawMaMiniTags();
    drawCandleLiquidityTags(true);
    updateCandleCountdown();

    window.__chartReady = true;
  }

  function scheduleChartRender(force = false) {
    const now = Date.now();
    if (!force && now - state.lastFullRenderAt < 25) return;
    if (state.pendingChartRefresh) return;
    state.pendingChartRefresh = true;
    requestAnimationFrame(() => {
      state.pendingChartRefresh = false;
      state.lastFullRenderAt = Date.now();
      recalcDisplayCandles();
      renderChart();
    });
  }

  function levelBadgeClass(side) {
    if (side === 'LONG') return 'badge long';
    if (side === 'SHORT') return 'badge short';
    return 'badge neutral';
  }

  function renderSignal(signal) {
    if (!signal) return;

    const side = signal.side || 'NO_TRADE';
    el.signalType.className = levelBadgeClass(side);
    el.signalType.textContent = side;

    const market = signal.market || {};
    const analysisTf = signal.signalTimeframe || state.timeframe || '1m';
    let sr = market.supportResistance || {};
    if ((!Array.isArray(sr.zones) || !sr.zones.length) && state.candles.length >= 40) {
      sr = buildLocalSupportResistance(state.candles) || sr;
    }
    state.srData = sr;
    state.entryHint = buildRealtimeEntryHint();
    const localPressure = buildLocalPressure(state.candles);
    const pressure = Object.keys(market.pressure || {}).length ? market.pressure || {} : localPressure;
    const volumeStatus = signal.volumeStatus || market.volumeStatus?.status || localPressure.volumeStatus || 'N/A';
    const localIndicators = getLatestIndicatorSnapshot();
    const atrPctFallback = calcAtrPct(state.candles, 14);
    const buyPressure = hasNum(signal.buyPressure) ? signal.buyPressure : pressure.buyPressure;
    const sellPressure = hasNum(signal.sellPressure) ? signal.sellPressure : pressure.sellPressure;
    const mainPressure =
      signal.pressureLevel ||
      (hasNum(buyPressure) && hasNum(sellPressure)
        ? buyPressure >= sellPressure
          ? 'MUA TRỘI'
          : 'BÁN TRỘI'
        : 'N/A');

    const entryText =
      hasNum(signal.entryMin) && hasNum(signal.entryMax)
        ? `${formatPrice(signal.entryMin)} - ${formatPrice(signal.entryMax)}`
        : hasNum(state.entryHint?.price)
        ? formatPrice(state.entryHint.price)
        : 'Chưa có vùng vào';

    const supportNear = sr.keyLevels?.support;
    const resistanceNear = sr.keyLevels?.resistance;
    const pa = market.priceAction || signal.priceAction || {};
    const longZoneText = sr.potential?.longZone
      ? `Có (${formatNum(sr.potential.longZone.score, 0)})`
      : 'Chưa';
    const shortZoneText = sr.potential?.shortZone
      ? `Có (${formatNum(sr.potential.shortZone.score, 0)})`
      : 'Chưa';
    const pre = signal.preSignal?.selected || null;
    const preSignalText = pre
      ? `${pre.side} ${pre.mode || 'WATCH'} (${formatNum(pre.probability, 0)}%)`
      : 'N/A';
    const preTriggerText = pre ? formatPrice(pre.triggerPrice) : 'N/A';
    const preEtaText = pre ? formatEtaSec(pre.etaSec) : 'N/A';
    const planStopLoss = hasNum(signal.stopLoss) ? signal.stopLoss : state.entryHint?.stopLoss;
    const planTp1 = hasNum(signal.tp1) ? signal.tp1 : state.entryHint?.tp1;
    const planTp2 = hasNum(signal.tp2) ? signal.tp2 : state.entryHint?.tp2;
    const planTp3 = hasNum(signal.tp3) ? signal.tp3 : state.entryHint?.tp3;

    const rows = [
      ['Confidence', `${formatNum(signal.confidence, 0)}/100`, 'CONFIDENCE'],
      ['Trend 5m', signal.trend5m || 'N/A', 'SIDEWAY'],
      ['Trend 15m', signal.trend15m || 'N/A', 'SIDEWAY'],
      ['Regime', signal.volatilityRegime || market.volatilityRegime || 'N/A', 'VOLUME'],
      ['Session', signal.session || market.session || 'N/A', 'VOLUME'],
      ['Entry', entryText, 'ENTRY'],
      ['SL', formatPrice(planStopLoss), 'SL'],
      ['TP1', formatPrice(planTp1), 'TP'],
      ['TP2', formatPrice(planTp2), 'TP'],
      ['TP3', formatPrice(planTp3), 'TP'],
      ['RR', formatNum(signal.rr), 'RR'],
      [
        `ATR % (${analysisTf})`,
        hasNum(market.atrPct2m) || hasNum(atrPctFallback)
          ? `${formatNum((hasNum(market.atrPct2m) ? market.atrPct2m : atrPctFallback) * 100, 3)}%`
          : 'N/A',
        'VOLUME',
      ],
      [`RSI14 (${analysisTf})`, formatNum(hasNum(market.rsi2m) ? market.rsi2m : localIndicators.rsi), 'RSI'],
      [`MA7 (${analysisTf})`, formatPrice(hasNum(market.ma2m?.ma7) ? market.ma2m?.ma7 : localIndicators.ma7), 'MA'],
      [`MA25 (${analysisTf})`, formatPrice(hasNum(market.ma2m?.ma25) ? market.ma2m?.ma25 : localIndicators.ma25), 'MA'],
      [`MA99 (${analysisTf})`, formatPrice(hasNum(market.ma2m?.ma99) ? market.ma2m?.ma99 : localIndicators.ma99), 'MA'],
      [`MA200 (${analysisTf})`, formatPrice(hasNum(market.ma2m?.ma200) ? market.ma2m?.ma200 : localIndicators.ma200), 'MA'],
      ['PA Structure', pa.structure || 'N/A', 'SIDEWAY'],
      ['PA Pullback', pa.pullbackQuality || 'N/A', 'ENTRY'],
      ['PA Candle Confirm', pa.candleConfirm ? 'Có' : 'Không', 'ENTRY'],
      ['Support gần (Hỗ trợ)', formatPrice(supportNear), 'SIDEWAY'],
      ['Resistance gần (Kháng cự)', formatPrice(resistanceNear), 'SIDEWAY'],
      ['Long Zone', longZoneText, 'ENTRY'],
      ['Short Zone', shortZoneText, 'ENTRY'],
      ['Dự báo sớm', preSignalText, 'CONFIDENCE'],
      ['Giá kích hoạt sớm', preTriggerText, 'ENTRY'],
      ['ETA kích hoạt', preEtaText, 'ENTRY'],
      ['Gợi ý bắt lệnh', state.entryHint ? `${state.entryHint.side} @ ${formatPrice(state.entryHint.price)}` : 'N/A', 'ENTRY'],
      ['Volume', volumeStatus, 'VOLUME'],
      ['Reject code', signal.rejectCode || signal.rejectCodes?.[0] || 'N/A', 'SIDEWAY'],
    ];

    el.signalGrid.innerHTML = rows
      .map(
        ([k, v, tipKey]) =>
          `<div class="item"><div class="k">${k}${tipIcon(tipKey)}</div><div class="v ${valueClassByText(v)}">${v}</div></div>`,
      )
      .join('');

    const reasons = signal.reasons?.length
      ? signal.reasons.map((r) => `- ${r}`).join('<br>')
      : `- ${signal.diagnostics?.rejects?.join('<br>- ') || 'Chưa đủ điều kiện'}`;
    const rejects = signal.rejects?.length
      ? `<br><strong>Reject gần nhất:</strong><br>- ${signal.rejects.join('<br>- ')}${
          signal.rejectCodes?.length
            ? `<br><strong>Reject code:</strong> ${signal.rejectCodes.join(', ')}`
            : ''
        }`
      : '';
    const preExplain =
      signal.preSignal?.selected?.reasons?.length
        ? `<br><br><strong>Dự báo sớm:</strong><br>- ${signal.preSignal.selected.reasons.join('<br>- ')}`
        : '';
    const bd = signal.confidenceBreakdown || {};
    const breakdown = [
      `Trend: ${formatNum(bd.trend, 0)}`,
      `Pullback: ${formatNum(bd.pullback, 0)}`,
      `MA: ${formatNum(bd.maMomentum, 0)}`,
      `RSI: ${formatNum(bd.rsi, 0)}`,
      `Volume: ${formatNum(bd.volume, 0)}`,
      `Candle: ${formatNum(bd.candle, 0)}`,
      `Pressure: ${formatNum(bd.pressure, 0)}`,
      `Structure: ${formatNum(bd.structure, 0)}`,
      `RiskPenalty: ${formatNum(bd.riskPenalty, 0)}`,
      `NoisePenalty: ${formatNum(bd.noisePenalty, 0)}`,
    ].join(' | ');
    el.signalReason.innerHTML = `<strong>Lý do:</strong><br>${reasons}${rejects}${preExplain}<br><br><strong>Breakdown:</strong><br>${breakdown}`;

    el.newbieHint.style.display = state.toggles.newbie ? 'block' : 'none';
    el.newbieHint.textContent = state.toggles.newbie
      ? signal.newbieNote || 'Không có ghi chú.'
      : 'Bật chế độ người mới để xem giải thích dễ hiểu.';

    const pressureRows = [
      ['Buy Pressure', formatNum(buyPressure, 1), 'BUY_PRESSURE'],
      ['Sell Pressure', formatNum(sellPressure, 1), 'SELL_PRESSURE'],
      ['Lực chính', mainPressure, 'BUY_PRESSURE'],
      ['Candle Strength', formatNum(pressure.candleStrength, 1), 'VOLUME'],
      ['Delta Volume', hasNum(pressure.deltaVolumeTrend) ? `${formatNum(pressure.deltaVolumeTrend, 1)}%` : 'N/A', 'VOLUME'],
      ['Price Speed', hasNum(pressure.priceSpeed) ? formatNum(pressure.priceSpeed, 5) : 'N/A', 'VOLUME'],
      ['Absorption', formatNum(pressure.absorptionScore, 1), 'VOLUME'],
      ['Volume trạng thái', volumeStatus, 'VOLUME'],
      ['Support Zones (Vùng hỗ trợ)', String(sr.supports?.length || 0), 'SIDEWAY'],
      ['Resistance Zones (Vùng kháng cự)', String(sr.resistances?.length || 0), 'SIDEWAY'],
      ['Sideway', market.sideway ? 'Có' : 'Không', 'SIDEWAY'],
    ];

    el.pressureGrid.innerHTML = pressureRows
      .map(
        ([k, v, tipKey]) =>
          `<div class="item"><div class="k">${k}${tipIcon(tipKey)}</div><div class="v ${valueClassByText(v)}">${v}</div></div>`,
      )
      .join('');

    const warnings = [];
    if (side === 'NO_TRADE') warnings.push('Thị trường chưa đẹp, bot đứng ngoài để tránh lệnh nhiễu.');
    if (pa.side === 'NONE') warnings.push('Price Action chưa xác nhận entry đẹp, tránh FOMO.');
    if (pre?.mode === 'ARMED') {
      warnings.unshift(
        `Pre-signal đang ARMED: ${pre.side} tại ${formatPrice(pre.triggerPrice)} (XS ${formatNum(
          pre.probability,
          0,
        )}%)`,
      );
    }
    if (state.status?.mode === 'AUTO_BOT' && market.sideway) warnings.push('AUTO đang bị chặn vì sideway.');
    if ((market.spreadPct || 0) > 0.0008) warnings.push('Spread đang cao, rủi ro scalp tăng.');
    if (signal.confidence < 75) warnings.push('Điểm tin cậy chưa cao, ưu tiên quan sát hoặc semi-auto.');

    el.quickWarning.innerHTML = warnings.length
      ? `<strong>Cảnh báo nhanh:</strong><br>${warnings.map((w) => `- ${w}`).join('<br>')}`
      : '<strong>Cảnh báo nhanh:</strong><br>- Điều kiện hiện tại ổn định.';

    drawSupportResistance(state.srData);
    renderRsGuidePanel();
  }

  function renderStatus(status) {
    if (!status) return;

    const badgeClass =
      status.stateMachine.includes('ERROR') || status.stateMachine.includes('PAUSED')
        ? 'badge warn'
        : status.activePosition
        ? 'badge long'
        : 'badge neutral';

    el.botStateBadge.className = badgeClass;
    el.botStateBadge.textContent = status.stateMachine;

    const staleMs = Date.now() - (state.lastTickTs || 0);
    const connected = (state.streamConnected || state.directKlineConnected) && staleMs < 10000;

    el.connectionBadge.className = connected ? 'badge long' : 'badge warn';
    if (connected) el.connectionBadge.textContent = 'Realtime tốt';
    else if (state.streamConnected) el.connectionBadge.textContent = 'Realtime trễ';
    else el.connectionBadge.textContent = 'Realtime lỗi';

    el.modeSelect.value = status.mode;
    if (status.priceSource) {
      state.priceSource = status.priceSource;
      el.priceSourceSelect.value = status.priceSource;
    }

    const cooldownText = status.cooldownActive
      ? `Cooldown đến ${formatByTimezone(new Date(status.cooldownUntil), false)}`
      : 'Không cooldown';

    const pos = status.activePosition
      ? `Vị thế: ${status.activePosition.side} @ ${formatPrice(status.activePosition.entryPrice)}`
      : 'Không có vị thế mở';

    const refPrice = getCurrentDisplayPrice();
    const priceText = refPrice ? `Giá(${state.priceSource}): ${formatPrice(refPrice)}` : `Giá(${state.priceSource}): N/A`;
    const i = getLatestIndicatorSnapshot();
    const indicatorText = `RSI14: ${formatNum(i.rsi, 2)} | MA7: ${formatPrice(i.ma7)} | MA25: ${formatPrice(i.ma25)} | MA99: ${formatPrice(i.ma99)} | MA200: ${formatPrice(i.ma200)}`;
    const tzText = `TZ: ${state.timezoneMode === 'VN' ? 'UTC+7' : state.timezoneMode}`;
    const marketSource = status.binance?.useTestnet ? 'FUTURES TESTNET' : 'FUTURES MAINNET';

    const riskLocked = status.stateMachine === 'PAUSED_BY_RISK' || status.risk?.lockedByRisk;
    el.killSwitchBtn.textContent = riskLocked ? 'Mở Khóa Risk Lock' : 'Kill Switch';

    updateCandleCountdown();
    el.chartFootNote.textContent = `${cooldownText} | Nến còn: ${state.candleCountdownText} | ${pos} | ${priceText} | ${indicatorText} | ${marketSource} | ${tzText} | Reconnect: ${status.reconnectCount}`;
    renderRsGuidePanel();
    renderFleet(status);
  }

  function updateChartFootCountdownOnly() {
    if (!el.chartFootNote) return;
    updateCandleCountdown();
    const text = String(el.chartFootNote.textContent || '');
    if (!text) return;
    if (text.includes('Nến còn:')) {
      el.chartFootNote.textContent = text.replace(/Nến còn:\s\d{2}:\d{2}/, `Nến còn: ${state.candleCountdownText}`);
    }
  }

  function updateChartFootPriceOnly() {
    if (!el.chartFootNote) return;
    const price = getCurrentDisplayPrice();
    if (!price) return;
    const text = String(el.chartFootNote.textContent || '');
    if (!text || !text.includes('Giá(')) return;
    const next = text.replace(/Giá\([A-Z]+\):\s[0-9.]+/, `Giá(${state.priceSource}): ${formatPrice(price)}`);
    if (next !== text) {
      el.chartFootNote.textContent = next;
    }
  }

  function syncSymbolSelectFromStatus(status) {
    const symbols = status?.scanner?.symbols;
    if (!Array.isArray(symbols) || !symbols.length || !el.symbolSelect) return;
    const normalized = symbols.map((s) => String(s || '').toUpperCase()).filter(Boolean);
    if (!normalized.length) return;
    const existing = Array.from(el.symbolSelect.options).map((o) => String(o.value || '').toUpperCase());
    if (
      existing.length === normalized.length &&
      existing.every((s, i) => s === normalized[i])
    ) {
      return;
    }

    el.symbolSelect.innerHTML = normalized.map((s) => `<option value="${s}">${s}</option>`).join('');
    if (normalized.includes(state.symbol)) {
      el.symbolSelect.value = state.symbol;
    } else {
      state.symbol = normalized[0];
      el.symbolSelect.value = state.symbol;
    }
  }

  function renderFleet(status) {
    if (!el.fleetSummary || !el.fleetActiveList || !el.fleetInactiveList || !el.fleetBadge) return;

    const renderFleetBlock = (fleetData, refs, options = {}) => {
      const {
        summaryEl,
        activeEl,
        inactiveEl,
        badgeEl,
      } = refs;
      if (!summaryEl || !activeEl || !inactiveEl || !badgeEl) return;

      const bots = Array.isArray(fleetData?.bots) ? fleetData.bots : [];
      const watchdog = fleetData?.watchdog || {};
      const staleMs = Number(fleetData?.staleMs || status?.scanner?.staleMs || 0);
      const emptyText = options.emptyText || 'Chưa có dữ liệu bot.';
      const activeEmptyText = options.activeEmptyText || 'Không có bot ACTIVE.';
      const inactiveEmptyText = options.inactiveEmptyText || 'Tất cả bot đang hoạt động ổn định.';
      const extraItems = Array.isArray(options.extraItems) ? options.extraItems : [];

      if (!bots.length) {
        badgeEl.className = 'badge warn';
        badgeEl.textContent = 'Chưa nhận fleet bot';
        summaryEl.innerHTML = `<div class="fleet-empty">${emptyText}</div>`;
        activeEl.innerHTML = `<div class="fleet-empty">N/A</div>`;
        inactiveEl.innerHTML = `<div class="fleet-empty">N/A</div>`;
        return;
      }

      const active = bots.filter((b) => b.health === 'ACTIVE');
      const stalled = bots.filter((b) => b.health === 'STALLED');
      const inactive = bots.filter((b) => b.health === 'INACTIVE');

      const requestedCount = Number(fleetData.requestedCount || bots.length);
      const runningCount = Number(fleetData.runningCount || active.length);
      const healthPct = requestedCount > 0 ? Math.round((runningCount / requestedCount) * 100) : 0;
      const good = inactive.length === 0 && stalled.length === 0;
      badgeEl.className = good ? 'badge long' : 'badge warn';
      badgeEl.textContent = good
        ? `Ổn định ${runningCount}/${requestedCount}`
        : `Cần phục hồi ${requestedCount - runningCount}/${requestedCount}`;

      const summaryRows = [
        ['Bot yêu cầu', `${requestedCount}`],
        ['Bot hoạt động', `${runningCount}`],
        ['Bot lỗi/stalled', `${inactive.length + stalled.length}`],
        ['Health', `${healthPct}%`],
        ['Watchdog', watchdog.enabled ? `BẬT (${formatAgoMs(watchdog.watchdogMs)})` : 'TẮT'],
        ['Đã tự phục hồi', `${watchdog.recoveryCount || 0}`],
        ['Stale ngưỡng', staleMs ? formatAgoMs(staleMs) : 'N/A'],
        ['Watchdog gần nhất', watchdog.lastRunAt ? formatByTimezone(new Date(watchdog.lastRunAt), true) : 'N/A'],
        ...extraItems,
      ];

      summaryEl.innerHTML = summaryRows
        .map(
          ([k, v]) => `<div class="item"><div class="k">${k}</div><div class="v ${valueClassByText(v)}">${v}</div></div>`,
        )
        .join('');

      const sortByRun = (a, b) => Number(b.lastRunAt || 0) - Number(a.lastRunAt || 0);
      const renderBotRow = (bot) => {
        const lastRunText = bot.lastRunAt ? formatByTimezone(new Date(bot.lastRunAt), true) : 'N/A';
        const staleText = Number.isFinite(bot.staleForMs) ? formatAgoMs(bot.staleForMs) : 'N/A';
        const healthClass =
          bot.health === 'ACTIVE' ? 'long' : bot.health === 'STALLED' ? 'warn' : 'short';
        const err = bot.lastError ? ` | lỗi: ${bot.lastError}` : '';
        const processedText = Number.isFinite(Number(bot.processed))
          ? ` | processed=${bot.processed || 0}`
          : '';
        return `<div class="fleet-row">
          <div class="head">
            <span class="symbol">${bot.symbol}</span>
            <span class="badge ${healthClass}">${bot.health || bot.state || 'N/A'}</span>
          </div>
          <div class="meta">
            state=${bot.state || 'N/A'} | tf=${bot.timeframe || 'N/A'} | chạy gần nhất=${lastRunText}<br>
            stale=${staleText} | check=${bot.checkCount || bot.analysisCount || 0} | open=${bot.openTracks || 0} | win=${bot.resolvedWins || 0} | loss=${bot.resolvedLosses || 0} | exp=${bot.resolvedExpired || 0}${processedText}${err}
          </div>
        </div>`;
      };

      const activeList = active.sort(sortByRun);
      const inactiveList = [...stalled, ...inactive].sort(sortByRun);

      activeEl.innerHTML = activeList.length
        ? activeList.map(renderBotRow).join('')
        : `<div class="fleet-empty">${activeEmptyText}</div>`;

      inactiveEl.innerHTML = inactiveList.length
        ? inactiveList.map(renderBotRow).join('')
        : `<div class="fleet-empty">${inactiveEmptyText}</div>`;
    };

    const direct = status?.fleet?.directBots || {};
    renderFleetBlock(
      direct,
      {
        summaryEl: el.fleetSummary,
        activeEl: el.fleetActiveList,
        inactiveEl: el.fleetInactiveList,
        badgeEl: el.fleetBadge,
      },
      {
        emptyText: 'Chưa có dữ liệu bot trực coin.',
        extraItems: [['Khởi động lại scanner', `${direct?.watchdog?.scannerRestartCount || 0}`]],
      },
    );

    if (el.outcomeFleetSummary && el.outcomeFleetActiveList && el.outcomeFleetInactiveList && el.outcomeFleetBadge) {
      const outcome = status?.fleet?.outcomeBots || {};
      renderFleetBlock(
        outcome,
        {
          summaryEl: el.outcomeFleetSummary,
          activeEl: el.outcomeFleetActiveList,
          inactiveEl: el.outcomeFleetInactiveList,
          badgeEl: el.outcomeFleetBadge,
        },
        {
          emptyText: 'Chưa có dữ liệu bot WinRate/TP-SL.',
        },
      );
    }
  }

  function renderLogs(logs) {
    state.logs = logs;

    el.logContainer.innerHTML = logs
      .slice(0, 120)
      .map((log) => {
        const cls = log.level || 'info';
        const logTime = Number.isFinite(Number(log.timestamp))
          ? formatByTimezone(new Date(Number(log.timestamp)), false)
          : log.time;
        return `<div class="log-line ${cls}"><span class="t">[${logTime}]</span> <strong>${log.type}</strong> ${log.message}</div>`;
      })
      .join('');

    const signalLogs = logs
      .filter((log) => log.type === 'signal' && log.context?.side && log.context.side !== 'NO_TRADE')
      .slice(0, 35)
      .reverse();

    state.signalMarkers = signalLogs
      .map((log) => {
        const side = log.context.side;
        return {
          time: Math.floor(log.timestamp / 1000),
          position: side === 'LONG' ? 'belowBar' : 'aboveBar',
          color: side === 'LONG' ? '#16c47f' : '#ea5455',
          shape: side === 'LONG' ? 'arrowUp' : 'arrowDown',
          text: `${side} ${log.context.confidence || ''}`,
        };
      })
      .filter((m) => Number.isFinite(m.time));

    if (state.toggles.markers) {
      candleSeries.setMarkers(state.signalMarkers);
    }
  }

  async function hydrateCandlesToBackend(symbol, timeframe, candles) {
    if (!Array.isArray(candles) || candles.length < 20) return;
    try {
      await api.post('/candles/hydrate', { symbol, timeframe, candles });
    } catch (error) {
      console.warn(`hydrate candles thất bại ở ${timeframe}`, error);
    }
  }

  async function hydrateScannerUniverseFromClient(status) {
    if (state.fleetHydrationInProgress) return;
    const symbols = Array.isArray(status?.scanner?.symbols)
      ? status.scanner.symbols.map((s) => String(s || '').toUpperCase()).filter(Boolean)
      : [];
    if (!symbols.length) return;

    const now = Date.now();
    if (state.fleetHydrationDone && now - state.fleetHydrationAt < 10 * 60 * 1000) {
      return;
    }

    state.fleetHydrationInProgress = true;
    const tfs = ['1m', '3m', '5m', '15m'];
    const batchSize = 5;

    try {
      for (let i = 0; i < symbols.length; i += batchSize) {
        const batch = symbols.slice(i, i + batchSize);
        await Promise.all(
          batch.map(async (symbol) => {
            for (const tf of tfs) {
              try {
                const direct = await fetchBinanceKlinesDirect(symbol, tf, 700);
                if (Array.isArray(direct) && direct.length >= 80) {
                  await hydrateCandlesToBackend(symbol, tf, direct);
                }
              } catch (error) {
                console.warn(`hydrate fleet lỗi ${symbol} ${tf}`, error);
              }
            }
          }),
        );
        if (i + batchSize < symbols.length) {
          await new Promise((resolve) => setTimeout(resolve, 80));
        }
      }
      state.fleetHydrationDone = true;
      state.fleetHydrationAt = Date.now();
      setTimeout(() => {
        refreshSignalStatus().catch(() => {});
      }, 350);
    } finally {
      state.fleetHydrationInProgress = false;
    }
  }

  async function loadCandlesPrimary(tf, forceFresh = false, strictFresh = false) {
    const freshQ = forceFresh ? '&fresh=1' : '';
    const useStrict = forceFresh && strictFresh;
    const strictQ = useStrict ? '&strict=1' : '';

    if (forceFresh) {
      try {
        const direct = await fetchBinanceKlinesDirect(state.symbol, tf, 700);
        if (Array.isArray(direct) && direct.length >= 80) {
          hydrateCandlesToBackend(state.symbol, tf, direct);
          return direct;
        }
      } catch (directError) {
        console.warn(`binance direct primary lỗi ở ${tf}`, directError);
      }
    }

    try {
      let data = await api.get(`/candles?symbol=${state.symbol}&timeframe=${tf}${freshQ}${strictQ}`);
      if (forceFresh && Array.isArray(data) && data.length < 80) {
        try {
          const direct = await fetchBinanceKlinesDirect(state.symbol, tf, 700);
          if (direct.length > data.length) {
            data = direct;
            hydrateCandlesToBackend(state.symbol, tf, direct);
          }
        } catch (directError) {
          console.warn(`binance direct fallback lỗi ở ${tf}`, directError);
        }
      }
      return data;
    } catch (error) {
      if (useStrict) {
        console.warn(`strict fresh lỗi ở ${tf}, fallback fresh non-strict`, error);
        let data = await api.get(`/candles?symbol=${state.symbol}&timeframe=${tf}${freshQ}`);
        if (forceFresh && Array.isArray(data) && data.length < 80) {
          try {
            const direct = await fetchBinanceKlinesDirect(state.symbol, tf, 700);
            if (direct.length > data.length) {
              data = direct;
              hydrateCandlesToBackend(state.symbol, tf, direct);
            }
          } catch (directError) {
            console.warn(`binance direct fallback lỗi ở ${tf}`, directError);
          }
        }
        return data;
      }
      throw error;
    }
  }

  async function loadInitialCandles(forceFresh = false, strictFresh = false) {
    const tfs = ['1m', '3m', '5m', '15m'];
    const byTf = await Promise.all(
      tfs.map(async (tf) => [
        tf,
        await loadCandlesPrimary(tf, forceFresh, strictFresh && tf === state.timeframe),
      ]),
    );
    const candleMap = Object.fromEntries(byTf);
    const candles1m = candleMap['1m'] || [];
    const candles3m = candleMap['3m'] || [];
    const candles5m = candleMap['5m'] || [];
    const candles15m = candleMap['15m'] || [];
    const normalize = (c) => ({
      time: Number(c.time),
      open: Number(c.open),
      high: Number(c.high),
      low: Number(c.low),
      close: Number(c.close),
      volume: Number(c.volume),
      isClosed: Boolean(c.isClosed),
    });
    state.candles1m = candles1m.map(normalize);
    state.candles3m = candles3m.map(normalize);
    state.candles5m = candles5m.map(normalize);
    state.candles15m = candles15m.map(normalize);
    state.candles1m = normalizeCandles(state.candles1m, 2000);
    state.candles3m = normalizeCandles(state.candles3m, 2000);
    state.candles5m = normalizeCandles(state.candles5m, 2000);
    state.candles15m = normalizeCandles(state.candles15m, 2000);
    state.lastTickTs = Date.now();
    recalcDisplayCandles();
    renderChart();
  }

  async function loadSingleTimeframe(tf, forceFresh = false, strictFresh = false) {
    const candles = await loadCandlesPrimary(tf, forceFresh, strictFresh);
    const normalized = normalizeCandles(
      candles.map((c) => ({
        time: Number(c.time),
        open: Number(c.open),
        high: Number(c.high),
        low: Number(c.low),
        close: Number(c.close),
        volume: Number(c.volume),
        isClosed: Boolean(c.isClosed),
      })),
      2000,
    );
    if (tf === '1m') state.candles1m = normalized;
    else if (tf === '3m') state.candles3m = normalized;
    else if (tf === '5m') state.candles5m = normalized;
    else if (tf === '15m') state.candles15m = normalized;
  }

  async function refreshSignalStatus() {
    try {
      const [signal, status, stats, logs] = await Promise.all([
        api.get('/signal'),
        api.get('/status'),
        api.get('/stats'),
        api.get('/logs?limit=300'),
      ]);

      state.signal = signal;
      state.status = status;
      state.stats = stats;
      syncSymbolSelectFromStatus(status);
      hydrateScannerUniverseFromClient(status).catch((error) =>
        console.warn('hydrate scanner universe lỗi', error),
      );
      if (status.priceSource) {
        state.priceSource = status.priceSource;
        el.priceSourceSelect.value = status.priceSource;
      }

      renderSignal(signal || { side: 'NO_TRADE' });
      renderStatus(status);
      statsPanel.render(stats);
      renderLogs(logs);
    } catch (error) {
      console.error(error);
    }
  }

  async function fetchGlossary() {
    try {
      const glossary = await api.get('/glossary');
      state.glossary = glossary;
      glossaryPanel.setData(glossary);
    } catch (error) {
      console.error(error);
    }
  }

  function getBinanceKlineWsBase() {
    const useTestnet = Boolean(state.status?.binance?.useTestnet);
    return useTestnet ? 'wss://stream.binancefuture.com/ws' : 'wss://fstream.binance.com/ws';
  }

  function getBinanceRestBase() {
    const useTestnet = Boolean(state.status?.binance?.useTestnet);
    return useTestnet ? 'https://testnet.binancefuture.com' : 'https://fapi.binance.com';
  }

  async function fetchBinanceKlinesDirect(symbol, timeframe, limit = 600) {
    const base = getBinanceRestBase();
    const url = `${base}/fapi/v1/klines?symbol=${encodeURIComponent(
      symbol,
    )}&interval=${encodeURIComponent(timeframe)}&limit=${Math.max(100, Math.min(1200, Number(limit || 600)))}`;
    const res = await fetch(url);
    if (!res.ok) {
      throw new Error(`Binance direct REST lỗi ${res.status}`);
    }
    const raw = await res.json();
    if (!Array.isArray(raw)) return [];
    const now = Date.now();
    return raw.map((k) => ({
      time: Math.floor(Number(k[0]) / 1000),
      open: Number(k[1]),
      high: Number(k[2]),
      low: Number(k[3]),
      close: Number(k[4]),
      volume: Number(k[5]),
      quoteVolume: Number(k[7] || 0),
      isClosed: Number(k[6]) <= now,
    }));
  }

  function stopDirectCandleReconciler() {
    if (!state.directReconcileTimer) return;
    clearInterval(state.directReconcileTimer);
    state.directReconcileTimer = null;
  }

  function startDirectCandleReconciler() {
    stopDirectCandleReconciler();
    state.directReconcileTimer = setInterval(async () => {
      try {
        const fresh = await fetchBinanceKlinesDirect(state.symbol, state.timeframe, 60);
        if (!Array.isArray(fresh) || !fresh.length) return;
        const normalized = normalizeCandles(fresh, 700);
        if (state.timeframe === '1m') state.candles1m = normalized.slice();
        else if (state.timeframe === '3m') state.candles3m = normalized.slice();
        else if (state.timeframe === '5m') state.candles5m = normalized.slice();
        else if (state.timeframe === '15m') state.candles15m = normalized.slice();
        recalcDisplayCandles();
        renderChart();
      } catch (_) {
        // im lặng: reconciliation chỉ là self-heal chống lệch
      }
    }, 6000);
  }

  function closeDirectKlineStream() {
    state.directKlineToken += 1;
    state.directKlineConnected = false;
    stopDirectCandleReconciler();
    if (state.directKlineWs) {
      try {
        state.directKlineWs.close();
      } catch (_) {
        // ignore
      }
      state.directKlineWs = null;
    }
  }

  function connectDirectKlineStream() {
    closeDirectKlineStream();
    const token = state.directKlineToken;
    const symbol = String(state.symbol || '').toLowerCase();
    const timeframe = String(state.timeframe || '1m').toLowerCase();
    const wsBase = getBinanceKlineWsBase();
    const wsUrl = `${wsBase}/${symbol}@kline_${timeframe}`;

    try {
      const ws = new WebSocket(wsUrl);
      state.directKlineWs = ws;

      ws.onopen = () => {
        if (token !== state.directKlineToken) return;
        state.directKlineConnected = true;
        startDirectCandleReconciler();
      };

      ws.onmessage = (event) => {
        if (token !== state.directKlineToken) return;
        try {
          const payload = JSON.parse(event.data);
          const k = payload?.k;
          if (!k) return;
          const tf = String(k.i || timeframe).toLowerCase();
          if (tf !== state.timeframe) return;
          const s = String(k.s || '').toUpperCase();
          if (s && s !== state.symbol) return;

          const candle = {
            time: Math.floor(Number(k.t) / 1000),
            open: Number(k.o),
            high: Number(k.h),
            low: Number(k.l),
            close: Number(k.c),
            volume: Number(k.v),
            quoteVolume: Number(k.q || 0),
            isClosed: Boolean(k.x),
          };
          if (!Number.isFinite(candle.time)) return;

          state.livePrice = Number(candle.close);

          applyIncomingCandle(tf, candle);
          state.lastTickTs = Date.now();
          updateRealtimeCandle();

          if (candle.isClosed) {
            const syncKey = `${state.symbol}:${tf}:${candle.time}`;
            if (state.lastClosedSyncKey !== syncKey) {
              state.lastClosedSyncKey = syncKey;
              loadSingleTimeframe(tf, true, true)
                .then(() => {
                  recalcDisplayCandles();
                  renderChart();
                })
                .catch((error) => console.error('sync nến đóng (direct ws) thất bại', error));
            } else {
              scheduleChartRender(true);
            }
          } else {
            scheduleChartRender(false);
          }
        } catch (error) {
          console.error('Lỗi parse direct kline ws', error);
        }
      };

      ws.onerror = () => {
        if (token !== state.directKlineToken) return;
        state.directKlineConnected = false;
      };

      ws.onclose = () => {
        if (token !== state.directKlineToken) return;
        state.directKlineConnected = false;
        setTimeout(() => {
          if (token === state.directKlineToken) connectDirectKlineStream();
        }, 350);
      };
    } catch (error) {
      console.error('Không thể mở direct kline ws', error);
    }
  }

  function connectStream() {
    if (stream) {
      stream.close();
      stream = null;
    }

    stream = new EventSource('/api/stream');

    stream.onopen = () => {
      state.streamConnected = true;
      state.lastTickTs = Date.now();
      el.connectionBadge.className = 'badge long';
      el.connectionBadge.textContent = 'Realtime tốt';
    };

    stream.addEventListener('tick', (e) => {
      const packet = JSON.parse(e.data);
      if (packet.symbol !== state.symbol) return;

      state.lastTickTs = Date.now();

      if (packet.type === 'kline') {
        const c = packet.payload;
        const tf = c.timeframe || '1m';
        if (state.directKlineConnected && tf === state.timeframe) {
          return;
        }
        applyIncomingCandle(tf, {
          time: Number(c.time),
          open: Number(c.open),
          high: Number(c.high),
          low: Number(c.low),
          close: Number(c.close),
          volume: Number(c.volume),
          quoteVolume: Number(c.quoteVolume || c.q || 0),
          isClosed: Boolean(c.isClosed),
        });
        updateRealtimeCandle();
        const impactCurrentTf = tf === state.timeframe;
        if (impactCurrentTf && c.isClosed) {
          const syncKey = `${state.symbol}:${tf}:${c.time}`;
          if (state.lastClosedSyncKey !== syncKey) {
            state.lastClosedSyncKey = syncKey;
            loadSingleTimeframe(tf, true, true)
              .then(() => {
                recalcDisplayCandles();
                renderChart();
              })
              .catch((error) => console.error('sync nến đóng thất bại', error));
          } else {
            scheduleChartRender(true);
          }
        } else if (impactCurrentTf) {
          scheduleChartRender(false);
        }
      } else if (packet.type === 'trade') {
        if (state.directKlineConnected && state.priceSource === 'LAST') return;
        const p = Number(packet.payload.price || 0);
        if (p > 0) state.livePrice = p;
        if (state.status) renderStatus(state.status);
      } else if (packet.type === 'book' || packet.type === 'mark') {
        if (state.directKlineConnected && state.priceSource === 'LAST') return;
        const p = Number(packet.payload.lastPrice || packet.payload.markPrice || 0);
        if (p > 0) state.livePrice = p;
        if (state.status) renderStatus(state.status);
      }
    });

    stream.addEventListener('signal', (e) => {
      const payload = JSON.parse(e.data);
      if (payload.symbol !== state.symbol) return;
      state.signal = payload;
      renderSignal(payload);
      refreshSignalStatus();
    });

    stream.addEventListener('status', (e) => {
      const payload = JSON.parse(e.data);
      state.status = payload;
      renderStatus(payload);
    });

    stream.addEventListener('heartbeat', () => {
      // heartbeat chỉ để giữ stream sống, không dùng làm mốc dữ liệu thị trường.
    });

    stream.onerror = () => {
      state.streamConnected = false;
      el.connectionBadge.className = 'badge warn';
      el.connectionBadge.textContent = 'Realtime lỗi';
    };
  }

  async function changeSymbol(symbol) {
    await api.post('/config', { symbol });
    closeDirectKlineStream();
    state.symbol = symbol;
    state.candles1m = [];
    state.candles3m = [];
    state.candles5m = [];
    state.candles15m = [];
    state.candles = [];
    state.livePrice = 0;
    state.lastClosedSyncKey = '';
    candleSeries.setData([]);
    volumeSeries.setData([]);
    rsiSeries.setData([]);
    drawTradeZones(null);
    clearSupportResistanceVisuals();
    clearMaMiniTags();
    clearCandleLiquidityTags();
    await loadInitialCandles(true, true);
    connectStream();
    connectDirectKlineStream();
    await refreshSignalStatus();
  }

  async function bindActions() {
    el.symbolSelect.addEventListener('change', async (e) => {
      await changeSymbol(e.target.value);
    });

    el.timeframeSelect.addEventListener('change', async (e) => {
      closeDirectKlineStream();
      state.timeframe = e.target.value;
      try {
        await loadSingleTimeframe(state.timeframe, true, true);
      } catch (error) {
        console.error('Không tải được nến fresh cho timeframe mới', error);
      }
      recalcDisplayCandles();
      renderChart();
      connectDirectKlineStream();
    });

    el.priceSourceSelect.addEventListener('change', async (e) => {
      state.priceSource = e.target.value;
      await api.post('/config', { trading: { priceSource: state.priceSource } });
      await refreshSignalStatus();
    });

    el.timezoneSelect.addEventListener('change', (e) => {
      state.timezoneMode = e.target.value;
      localStorage.setItem('APP_TIMEZONE_MODE', state.timezoneMode);
      applyTimezoneToChart();
      renderChart();
      if (state.status) renderStatus(state.status);
    });

    el.modeSelect.addEventListener('change', async (e) => {
      await api.post('/mode', { mode: e.target.value });
      await refreshSignalStatus();
    });

    el.startBtn.addEventListener('click', async () => {
      await api.post('/start');
      await refreshSignalStatus();
    });

    el.stopBtn.addEventListener('click', async () => {
      await api.post('/stop', { reason: 'MANUAL_STOP_FROM_UI' });
      await refreshSignalStatus();
    });

    el.confirmTradeBtn.addEventListener('click', async () => {
      try {
        await api.post('/trade');
      } catch (error) {
        alert(error.message);
      }
      await refreshSignalStatus();
    });

    el.killSwitchBtn.addEventListener('click', async () => {
      const riskLocked =
        state.status?.stateMachine === 'PAUSED_BY_RISK' || Boolean(state.status?.risk?.lockedByRisk);
      if (riskLocked) {
        await api.post('/emergency', { enabled: false, reason: 'MANUAL_RELEASE_FROM_UI' });
        await api.post('/start');
      } else {
        await api.post('/emergency', { enabled: true, reason: 'EMERGENCY_STOP_FROM_UI' });
        await api.post('/stop', { reason: 'EMERGENCY_STOP' });
      }
      await refreshSignalStatus();
    });

    el.applyConfigBtn.addEventListener('click', async () => {
      const payload = {
        risk: {
          defaultRiskPerTradePct: Number(el.riskPerTradeInput.value),
          dailyLossLimitPct: Number(el.dailyLossInput.value),
          maxTradesPerHour: Number(el.maxTradesHourInput.value),
        },
      };
      await api.post('/config', payload);
      await refreshSignalStatus();
    });

    el.sendTelegramTestBtn.addEventListener('click', async () => {
      const oldText = el.sendTelegramTestBtn.textContent;
      const diceValue = el.telegramDiceSelect.value || '';
      const payload = {
        message: String(el.telegramTestInput.value || '').trim(),
        route: el.telegramRouteSelect.value || 'ALL',
        symbol: state.symbol,
        sticker: String(el.telegramStickerInput.value || '').trim(),
        animation: String(el.telegramAnimationInput.value || '').trim(),
      };
      if (diceValue) payload.diceEmoji = diceValue;

      el.sendTelegramTestBtn.disabled = true;
      el.sendTelegramTestBtn.textContent = 'Đang gửi...';
      try {
        await api.post('/telegram/test', payload);
        alert('Đã gửi test Telegram thành công.');
      } catch (error) {
        alert(`Gửi test Telegram thất bại: ${error.message}`);
      } finally {
        el.sendTelegramTestBtn.disabled = false;
        el.sendTelegramTestBtn.textContent = oldText;
      }
    });

    el.toggleMA.addEventListener('change', (e) => {
      state.toggles.ma = e.target.checked;
      renderChart();
    });

    el.toggleRSI.addEventListener('change', (e) => {
      state.toggles.rsi = e.target.checked;
      renderChart();
    });

    el.toggleVolume.addEventListener('change', (e) => {
      state.toggles.volume = e.target.checked;
      renderChart();
    });

    el.toggleCandleLiquidity.addEventListener('change', (e) => {
      state.toggles.candleLiquidity = e.target.checked;
      renderChart();
    });

    el.toggleMarkers.addEventListener('change', (e) => {
      state.toggles.markers = e.target.checked;
      renderChart();
    });

    el.toggleZones.addEventListener('change', (e) => {
      state.toggles.zones = e.target.checked;
      renderChart();
    });

    el.toggleNewbie.addEventListener('change', (e) => {
      state.toggles.newbie = e.target.checked;
      renderSignal(state.signal || { side: 'NO_TRADE' });
    });
  }

  async function init() {
    const qp = new URLSearchParams(window.location.search);
    const adminToken = qp.get('adminToken');
    if (adminToken) {
      state.adminToken = adminToken;
      localStorage.setItem('ADMIN_TOKEN', adminToken);
    }
    const qpSymbol = String(qp.get('symbol') || '').toUpperCase();
    const qpTimeframe = qp.get('timeframe');
    if (qpSymbol) state.symbol = qpSymbol;
    if (['1m', '3m', '5m', '15m'].includes(qpTimeframe)) state.timeframe = qpTimeframe;
    if (el.symbolSelect && qpSymbol) el.symbolSelect.value = qpSymbol;
    if (el.timeframeSelect && qpTimeframe) el.timeframeSelect.value = qpTimeframe;

    await bindActions();
    if (el.timezoneSelect) {
      const allowed = new Set(['VN', 'LOCAL', 'UTC']);
      if (!allowed.has(state.timezoneMode)) state.timezoneMode = 'VN';
      el.timezoneSelect.value = state.timezoneMode;
    }
    applyTimezoneToChart();
    await fetchGlossary();
    try {
      const status = await api.get('/status');
      state.status = status;
    } catch (error) {
      console.warn('Không lấy được status trước khi tải nến', error);
    }
    await loadInitialCandles(true, true);
    connectStream();
    await refreshSignalStatus();
    connectDirectKlineStream();

    if (state.snapshotMode && qp.get('focusSignal') === '1') {
      const side = (qp.get('side') || 'NO_TRADE').toUpperCase();
      const entry = Number(qp.get('entry') || 0);
      const sl = Number(qp.get('sl') || 0);
      const tp1 = Number(qp.get('tp1') || 0);
      const tp2 = Number(qp.get('tp2') || 0);
      const tp3 = Number(qp.get('tp3') || 0);
      if (entry > 0 && sl > 0) {
        state.signal = {
          ...state.signal,
          symbol: state.symbol,
          side,
          entryMin: entry * 0.9995,
          entryMax: entry * 1.0005,
          entryPrice: entry,
          stopLoss: sl,
          tp1,
          tp2,
          tp3,
          confidence: state.signal?.confidence || 80,
          reasons: state.signal?.reasons || ['Snapshot focus signal'],
        };
        drawTradeZones(state.signal);
        renderSignal(state.signal);
      }
      scheduleChartRender(true);
    }

    setInterval(() => {
      updateChartFootCountdownOnly();
    }, 1000);

    // fallback nếu stream bị chập chờn
    setInterval(async () => {
      if (Date.now() - state.lastTickTs > 5000) {
        try {
          await loadSingleTimeframe(state.timeframe, true, true);
          recalcDisplayCandles();
          renderChart();
          await refreshSignalStatus();
        } catch (error) {
          console.error(error);
        }
      }
    }, 5000);
  }

  window.addEventListener('beforeunload', () => {
    closeDirectKlineStream();
    if (stream) stream.close();
  });

  init();
})();
