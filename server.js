require('dotenv').config();

const express = require('express');
const axios = require('axios');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const MONGODB_URI = process.env.MONGODB_URI;
const MONGODB_DB = process.env.MONGODB_DB || 'lomy';

const WS_BASE = 'wss://data-stream.binance.vision';
const REST_BASE = 'https://data-api.binance.vision';

// ============================================================
// LOMY V4.4 CLOUD PRECISION
// PAPER ONLY
// CLOUD MEMORY + SMART WARMUP + MARKET REGIME
// ============================================================

const C = {
  version: '4.4-CLOUD-PRECISION',

  paperTrading: true,
  startingBalance: 10000,

  interval: '5m',
  warmupCandles: 60,
  maxCandles: 100,

  universeSize: 300,
  minQuoteVolume: 500000,
  universeRefreshMs: 30 * 60 * 1000,

  maxPositions: 6,
  maxEntriesPerCycle: 2,

  minScore: 82,
  requiredGrade: 'A',

  candidateExpiryMs: 2 * 60 * 1000,
  maxPriceDriftPct: 0.20,

  feePct: 0.001,
  slippagePct: 0.0005,

  minStopPct: 0.006,
  maxStopPct: 0.012,
  atrStopMultiplier: 1.25,
  rewardRisk: 1.9,

  breakEvenAtR: 1.0,
  trailAtR: 1.45,
  trailLockR: 0.45,

  earlyFailureWindowMs: 15 * 60 * 1000,
  earlyFailureLossPct: 0.0035,

  dailyLossLimitPct: 0.04,

  entriesBeforeCooldown: 10,
  batchCooldownMs: 30 * 60 * 1000,

  lossStreakLimit: 3,
  lossCooldownMs: 60 * 60 * 1000,

  symbolLossCooldownMs: 2 * 60 * 60 * 1000,

  warmupConcurrency: 4,
  warmupDelayMs: 300,

  btcSymbol: 'BTCUSDT',

  stateSaveMs: 15000,
  candleCloudSaveMs: 60000,
  regimeRefreshMs: 60000,

  journalLimitDashboard: 100,
  historyLimitDashboard: 100
};

// ============================================================
// STATE
// ============================================================

let cash = C.startingBalance;
let positions = {};

let stats = {
  totalTrades: 0,
  wins: 0,
  losses: 0,

  grossProfit: 0,
  grossLoss: 0,
  netProfit: 0,

  fees: 0,

  bestTrade: 0,
  worstTrade: 0,

  maxDrawdown: 0,

  earlyFailureExits: 0,
  breakEvenMoves: 0,
  trailingActivations: 0
};

let dailyPnL = 0;
let dailyStartEquity = C.startingBalance;
let currentDay = utcDay();

let peakEquity = C.startingBalance;

let manualPause = false;
let dailyPause = false;

let cooldownUntil = 0;
let cooldownReason = null;

let entriesSinceCooldown = 0;
let lossStreak = 0;

const lastLossBySymbol = {};

const tickers = new Map();
const candles = {};
const lastAnalyzed = {};

let subscribed = new Set();

const candidatePool = new Map();

let latest = [];

let miniWs = null;
let klineWs = null;

let miniConnected = false;
let klineConnected = false;

let lastMiniMessage = 0;
let lastKlineMessage = 0;

let shuttingDown = false;

let mongoClient = null;
let db = null;

let cloudConnected = false;

const warmupLoaded = new Set();
const warmupLoading = new Set();

let warmupQueue = [];
let warmupWorkers = 0;

let warmupStats = {
  cloudLoaded: 0,
  restLoaded: 0,
  restRequests: 0,
  failed: 0
};

let marketRegime = {
  ready: false,
  btcBullish: false,
  breadth: 0,
  regime: 'WARMING',
  updatedAt: 0
};

let rankTimer = null;

// ============================================================
// HELPERS
// ============================================================

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

function n(v, fallback = 0) {
  const x = Number(v);
  return Number.isFinite(x) ? x : fallback;
}

function clamp(v, min, max) {
  return Math.max(min, Math.min(max, v));
}

function pct(diff, base) {
  return base ? (diff / base) * 100 : 0;
}

function utcDay() {
  return new Date().toISOString().slice(0, 10);
}

function ignored(symbol) {
  return new Set([
    'USDCUSDT',
    'FDUSDUSDT',
    'TUSDUSDT',
    'USDPUSDT',
    'BUSDUSDT',
    'DAIUSDT',
    'USDEUSDT',
    'USD1USDT'
  ]).has(symbol);
}

function sessionUTC() {
  const h = new Date().getUTCHours();

  if (h < 7) return 'ASIA';
  if (h < 13) return 'LONDON';
  if (h < 16) return 'LONDON_NY';
  if (h < 21) return 'NEW_YORK';

  return 'LATE_US';
}

function readyCount() {
  let count = 0;

  for (const symbol of subscribed) {
    if ((candles[symbol]?.length || 0) >= C.warmupCandles) {
      count++;
    }
  }

  return count;
}

// ============================================================
// TELEGRAM
// ============================================================

const tgQueue = [];
let tgBusy = false;

function tg(text) {
  if (!TELEGRAM_TOKEN || !CHAT_ID) return;
  tgQueue.push(text);
}

setInterval(async () => {
  if (tgBusy || !tgQueue.length) return;

  tgBusy = true;

  const text = tgQueue.shift();

  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text,
        parse_mode: 'HTML'
      },
      { timeout: 10000 }
    );
  } catch (e) {
    if (e.response?.status === 429) {
      tgQueue.unshift(text);
      await sleep(5000);
    } else {
      console.error('Telegram:', e.message);
    }
  } finally {
    tgBusy = false;
  }
}, 1200);

// ============================================================
// MONGODB
// ============================================================

async function connectCloud() {
  if (!MONGODB_URI) {
    throw new Error('MONGODB_URI missing');
  }

  mongoClient = new MongoClient(MONGODB_URI, {
    maxPoolSize: 10
  });

  await mongoClient.connect();

  db = mongoClient.db(MONGODB_DB);

  await db.command({ ping: 1 });

  cloudConnected = true;

  await Promise.all([
    db.collection('candles').createIndex(
      { symbol: 1 },
      { unique: true }
    ),

    db.collection('trades').createIndex(
      { exitTime: -1 }
    ),

    db.collection('journal').createIndex(
      { time: -1 }
    ),

    db.collection('journal').createIndex(
      { symbol: 1, time: -1 }
    )
  ]);

  console.log('MongoDB CLOUD CONNECTED');
}

async function loadCloudState() {
  const state = await db
    .collection('state')
    .findOne({ _id: 'main' });

  if (!state) {
    console.log('No cloud state. Fresh account.');
    return;
  }

  cash = n(state.cash, C.startingBalance);

  positions = state.positions || {};

  stats = {
    ...stats,
    ...(state.stats || {})
  };

  dailyPnL = n(state.dailyPnL);

  dailyStartEquity = n(
    state.dailyStartEquity,
    C.startingBalance
  );

  currentDay =
    state.currentDay || utcDay();

  peakEquity = n(
    state.peakEquity,
    C.startingBalance
  );

  manualPause = !!state.manualPause;
  dailyPause = !!state.dailyPause;

  cooldownUntil = n(state.cooldownUntil);
  cooldownReason = state.cooldownReason || null;

  entriesSinceCooldown =
    n(state.entriesSinceCooldown);

  lossStreak = n(state.lossStreak);

  Object.assign(
    lastLossBySymbol,
    state.lastLossBySymbol || {}
  );

  console.log(
    `CLOUD STATE RESTORED | Cash $${cash.toFixed(2)} | Open ${Object.keys(positions).length}`
  );
}

async function saveCloudState() {
  if (!cloudConnected) return;

  try {
    await db.collection('state').updateOne(
      { _id: 'main' },
      {
        $set: {
          version: C.version,
          updatedAt: Date.now(),

          cash,
          positions,
          stats,

          dailyPnL,
          dailyStartEquity,
          currentDay,

          peakEquity,

          manualPause,
          dailyPause,

          cooldownUntil,
          cooldownReason,

          entriesSinceCooldown,
          lossStreak,

          lastLossBySymbol
        }
      },
      { upsert: true }
    );
  } catch (e) {
    console.error('Cloud state save:', e.message);
  }
}

async function cloudJournal(row) {
  if (!cloudConnected) return;

  try {
    await db.collection('journal').insertOne({
      time: Date.now(),
      version: C.version,
      ...row
    });
  } catch (e) {
    console.error('Journal:', e.message);
  }
}

async function saveTrade(record) {
  if (!cloudConnected) return;

  try {
    await db.collection('trades').insertOne({
      version: C.version,
      ...record
    });
  } catch (e) {
    console.error('Trade save:', e.message);
  }
}

// ============================================================
// CLOUD CANDLE CACHE
// ============================================================

async function loadCachedCandles(symbol) {
  if (!cloudConnected) return false;

  try {
    const doc = await db
      .collection('candles')
      .findOne({ symbol });

    if (
      !doc ||
      !Array.isArray(doc.candles) ||
      doc.candles.length < C.warmupCandles
    ) {
      return false;
    }

    const arr = doc.candles
      .map(x => ({
        open: n(x.open),
        high: n(x.high),
        low: n(x.low),
        close: n(x.close),
        volume: n(x.volume),
        closeTime: n(x.closeTime)
      }))
      .sort((a, b) => a.closeTime - b.closeTime)
      .slice(-C.maxCandles);

    const newest =
      arr[arr.length - 1]?.closeTime || 0;

    const maxAge =
      15 * 60 * 1000;

    if (
      Date.now() - newest >
      maxAge
    ) {
      candles[symbol] = arr;
      return false;
    }

    candles[symbol] = arr;

    warmupLoaded.add(symbol);
    warmupStats.cloudLoaded++;

    return true;
  } catch (e) {
    console.error(
      `Cloud candle load ${symbol}:`,
      e.message
    );

    return false;
  }
}

async function saveSymbolCandles(symbol) {
  if (!cloudConnected) return;

  const arr =
    candles[symbol];

  if (!Array.isArray(arr) || !arr.length) {
    return;
  }

  try {
    await db.collection('candles').updateOne(
      { symbol },
      {
        $set: {
          symbol,
          interval: C.interval,
          updatedAt: Date.now(),
          candles:
            arr.slice(-C.maxCandles)
        }
      },
      { upsert: true }
    );
  } catch (e) {
    console.error(
      `Cloud candle save ${symbol}:`,
      e.message
    );
  }
}

// ============================================================
// HISTORICAL WARMUP
// ============================================================

function parseKline(row) {
  return {
    open: n(row[1]),
    high: n(row[2]),
    low: n(row[3]),
    close: n(row[4]),
    volume: n(row[5]),
    closeTime: n(row[6])
  };
}

function mergeCandles(symbol, incoming) {
  const map = new Map();

  for (const x of candles[symbol] || []) {
    map.set(x.closeTime, x);
  }

  for (const x of incoming || []) {
    map.set(x.closeTime, x);
  }

  candles[symbol] =
    Array.from(map.values())
      .sort((a, b) => a.closeTime - b.closeTime)
      .slice(-C.maxCandles);
}

async function fetchWarmup(symbol) {
  if (
    warmupLoaded.has(symbol) ||
    warmupLoading.has(symbol)
  ) {
    return;
  }

  warmupLoading.add(symbol);

  try {
    const fromCloud =
      await loadCachedCandles(symbol);

    if (fromCloud) {
      console.log(
        `☁️ CLOUD READY ${symbol} | ${candles[symbol].length}`
      );

      return;
    }

    warmupStats.restRequests++;

    const response = await axios.get(
      `${REST_BASE}/api/v3/klines`,
      {
        params: {
          symbol,
          interval: C.interval,
          limit: C.maxCandles
        },
        timeout: 12000
      }
    );

    const now = Date.now();

    const historical =
      response.data
        .map(parseKline)
        .filter(x => x.closeTime < now);

    mergeCandles(
      symbol,
      historical
    );

    if (
      (candles[symbol]?.length || 0) >=
      C.warmupCandles
    ) {
      warmupLoaded.add(symbol);

      warmupStats.restLoaded++;

      await saveSymbolCandles(symbol);

      console.log(
        `🌐 REST READY ${symbol} | ${candles[symbol].length}`
      );
    }
  } catch (e) {
    warmupStats.failed++;

    console.error(
      `Warmup ${symbol}:`,
      e.response?.status ||
      e.message
    );
  } finally {
    warmupLoading.delete(symbol);
  }
}

function queueWarmup(symbol) {
  if (
    warmupLoaded.has(symbol) ||
    warmupLoading.has(symbol) ||
    warmupQueue.includes(symbol)
  ) {
    return;
  }

  warmupQueue.push(symbol);

  processWarmupQueue();
}

function processWarmupQueue() {
  while (
    warmupWorkers <
      C.warmupConcurrency &&
    warmupQueue.length
  ) {
    const symbol =
      warmupQueue.shift();

    warmupWorkers++;

    (async () => {
      try {
        await fetchWarmup(symbol);

        await sleep(
          C.warmupDelayMs
        );
      } finally {
        warmupWorkers--;
        processWarmupQueue();
      }
    })();
  }
}

// ============================================================
// EQUITY
// ============================================================

function equity() {
  let value = cash;

  for (
    const position
    of Object.values(positions)
  ) {
    const price =
      tickers.get(position.symbol)?.price ||
      position.lastPrice ||
      position.entryPrice;

    value +=
      position.qty *
      price;
  }

  return value;
}

function updateDrawdown() {
  const e = equity();

  if (e > peakEquity) {
    peakEquity = e;
  }

  const dd =
    peakEquity
      ? (
          (peakEquity - e) /
          peakEquity
        ) * 100
      : 0;

  stats.maxDrawdown =
    Math.max(
      stats.maxDrawdown,
      dd
    );
}

// ============================================================
// DAILY PROTECTION
// ============================================================

function checkDay() {
  const today = utcDay();

  if (today === currentDay) {
    return;
  }

  currentDay = today;

  dailyPnL = 0;
  dailyPause = false;

  dailyStartEquity =
    equity();

  saveCloudState();
}

function checkDailyLoss() {
  const maxLoss =
    dailyStartEquity *
    C.dailyLossLimitPct;

  if (
    !dailyPause &&
    dailyPnL <= -maxLoss
  ) {
    dailyPause = true;

    candidatePool.clear();

    tg(
      `🛑 <b>DAILY PROTECTION</b>\n` +
      `PnL: $${dailyPnL.toFixed(2)}`
    );

    saveCloudState();
  }
}

// ============================================================
// COOLDOWN
// ============================================================

function cooldownActive() {
  if (!cooldownUntil) {
    return false;
  }

  if (
    Date.now() >=
    cooldownUntil
  ) {
    cooldownUntil = 0;
    cooldownReason = null;
    entriesSinceCooldown = 0;

    candidatePool.clear();

    tg(
      '▶️ <b>SMART COOLDOWN FINISHED</b>'
    );

    saveCloudState();

    return false;
  }

  return true;
}

function startCooldown(ms, reason) {
  const until =
    Date.now() + ms;

  if (until <= cooldownUntil) {
    return;
  }

  cooldownUntil = until;
  cooldownReason = reason;

  candidatePool.clear();

  tg(
    `⏸ <b>SMART COOLDOWN</b>\n` +
    `${reason}\n` +
    `${Math.ceil(ms / 60000)} minutes`
  );

  saveCloudState();
}

function symbolCooling(symbol) {
  const last =
    n(lastLossBySymbol[symbol]);

  if (!last) return false;

  return (
    Date.now() - last <
    C.symbolLossCooldownMs
  );
}

// ============================================================
// INDICATORS
// ============================================================

function sma(arr, period, key) {
  if (arr.length < period) {
    return null;
  }

  return (
    arr
      .slice(-period)
      .reduce(
        (sum, x) =>
          sum + x[key],
        0
      ) /
    period
  );
}

function ema(arr, period, key = 'close') {
  if (arr.length < period) {
    return null;
  }

  let result =
    arr
      .slice(0, period)
      .reduce(
        (sum, x) =>
          sum + x[key],
        0
      ) /
    period;

  const multiplier =
    2 / (period + 1);

  for (
    let i = period;
    i < arr.length;
    i++
  ) {
    result =
      (
        arr[i][key] -
        result
      ) *
      multiplier +
      result;
  }

  return result;
}

function cmo(arr, period = 9) {
  if (
    arr.length <
    period + 1
  ) {
    return null;
  }

  let up = 0;
  let down = 0;

  for (
    let i =
      arr.length - period;
    i < arr.length;
    i++
  ) {
    const d =
      arr[i].close -
      arr[i - 1].close;

    if (d > 0) {
      up += d;
    } else {
      down += Math.abs(d);
    }
  }

  const total =
    up + down;

  return total
    ? 100 *
        (
          (up - down) /
          total
        )
    : 0;
}

function atr(arr, period = 14) {
  if (
    arr.length <
    period + 1
  ) {
    return null;
  }

  const tr = [];

  for (
    let i = 1;
    i < arr.length;
    i++
  ) {
    tr.push(
      Math.max(
        arr[i].high -
          arr[i].low,

        Math.abs(
          arr[i].high -
            arr[i - 1].close
        ),

        Math.abs(
          arr[i].low -
            arr[i - 1].close
        )
      )
    );
  }

  const recent =
    tr.slice(-period);

  return (
    recent.reduce(
      (a, b) => a + b,
      0
    ) /
    recent.length
  );
}

function structure(arr) {
  if (arr.length < 3) {
    return 'NEUTRAL';
  }

  const a =
    arr[arr.length - 1];

  const b =
    arr[arr.length - 2];

  const c =
    arr[arr.length - 3];

  if (
    a.high > b.high &&
    a.low > b.low &&
    b.low >= c.low
  ) {
    return 'BULLISH';
  }

  if (
    a.high < b.high &&
    a.low < b.low
  ) {
    return 'BEARISH';
  }

  return 'NEUTRAL';
}

// ============================================================
// MARKET REGIME
// ============================================================

function calculateMarketRegime() {
  let ready = 0;
  let bullish = 0;

  for (const symbol of subscribed) {
    const arr =
      candles[symbol];

    if (
      !arr ||
      arr.length <
        C.warmupCandles
    ) {
      continue;
    }

    const e20 =
      ema(arr, 20);

    const close =
      arr[arr.length - 1].close;

    ready++;

    if (close > e20) {
      bullish++;
    }
  }

  const breadth =
    ready
      ? (
          bullish /
          ready
        ) * 100
      : 0;

  const btc =
    btcContext();

  let regime =
    'DEFENSIVE';

  if (
    btc.bullish &&
    breadth >= 55
  ) {
    regime = 'RISK_ON';
  } else if (
    btc.ready &&
    breadth >= 45
  ) {
    regime = 'NEUTRAL';
  }

  marketRegime = {
    ready:
      ready >= 20,

    btcBullish:
      btc.bullish,

    breadth:
      Number(
        breadth.toFixed(2)
      ),

    regime,

    updatedAt:
      Date.now()
  };
}

function btcContext() {
  const arr =
    candles[C.btcSymbol];

  if (
    !arr ||
    arr.length <
      C.warmupCandles
  ) {
    return {
      ready: false,
      bullish: false,
      score: 0
    };
  }

  const last =
    arr[arr.length - 1];

  const e20 =
    ema(arr, 20);

  const e50 =
    ema(arr, 50);

  const previous =
    arr[arr.length - 6];

  const change5 =
    previous
      ? pct(
          last.close -
            previous.close,
          previous.close
        )
      : 0;

  const s =
    structure(arr);

  const bullish =
    last.close > e20 &&
    e20 > e50 &&
    s !== 'BEARISH' &&
    change5 > -0.35;

  let score = 0;

  if (last.close > e20) {
    score += 30;
  }

  if (e20 > e50) {
    score += 30;
  }

  if (s === 'BULLISH') {
    score += 25;
  }

  if (change5 >= 0) {
    score += 15;
  }

  return {
    ready: true,
    bullish,
    score,
    price: last.close,
    change5,
    structure: s
  };
}

// ============================================================
// ANALYSIS
// ============================================================

function analyze(arr, symbol) {
  if (
    arr.length <
    C.warmupCandles
  ) {
    return null;
  }

  const x =
    arr[arr.length - 1];

  const e20 =
    ema(arr, 20);

  const e50 =
    ema(arr, 50);

  const volSma =
    sma(
      arr,
      20,
      'volume'
    );

  const momentum =
    cmo(arr, 9);

  const a =
    atr(arr, 14);

  if (
    [e20, e50, volSma, momentum, a]
      .some(v => v === null)
  ) {
    return null;
  }

  const range =
    x.high - x.low;

  if (range <= 0) {
    return null;
  }

  const body =
    Math.abs(
      x.close - x.open
    );

  const bodyRatio =
    body / range;

  const bullish =
    x.close > x.open;

  const upperWick =
    x.high -
    Math.max(
      x.open,
      x.close
    );

  const upperWickRatio =
    upperWick / range;

  const volumeRatio =
    volSma
      ? x.volume / volSma
      : 0;

  const emaDistance =
    pct(
      x.close - e20,
      e20
    );

  const atrPct =
    pct(
      a,
      x.close
    );

  const s =
    structure(arr);

  const prior =
    arr.slice(-21, -1);

  const resistance =
    Math.max(
      ...prior.map(
        c => c.high
      )
    );

  const breakout =
    x.close >
    resistance;

  const breakoutPct =
    pct(
      x.close -
        resistance,
      resistance
    );

  const ext5 =
    pct(
      x.close -
        arr[
          arr.length - 6
        ].close,
      arr[
        arr.length - 6
      ].close
    );

  const ext10 =
    pct(
      x.close -
        arr[
          arr.length - 11
        ].close,
      arr[
        arr.length - 11
      ].close
    );

  const ema20Old =
    ema(
      arr.slice(0, -3),
      20
    );

  const emaSlope =
    ema20Old
      ? pct(
          e20 - ema20Old,
          ema20Old
        )
      : 0;

  let score = 0;

  const reasons = [];
  const warnings = [];

  if (
    x.close > e20 &&
    e20 > e50
  ) {
    score += 18;
    reasons.push('TREND');
  }

  if (
    emaSlope > 0.05
  ) {
    score += 7;
    reasons.push('EMA_SLOPE');
  }

  if (
    s === 'BULLISH'
  ) {
    score += 8;
    reasons.push('STRUCTURE');
  }

  if (
    bullish &&
    bodyRatio >= 0.58
  ) {
    score += 10;
    reasons.push('BODY');
  }

  if (
    upperWickRatio <= 0.28
  ) {
    score += 6;
    reasons.push('LOW_WICK');
  }

  if (
    momentum >= 55 &&
    momentum <= 72
  ) {
    score += 12;
    reasons.push('CMO_SWEET');
  } else if (
    momentum > 82
  ) {
    score -= 12;
    warnings.push('CMO_OVERHEAT');
  }

  if (
    volumeRatio >= 1.6 &&
    volumeRatio <= 2.8
  ) {
    score += 15;
    reasons.push('VOLUME_SWEET');
  } else if (
    volumeRatio >= 3.2
  ) {
    score -= 15;
    warnings.push('VOLUME_EXHAUSTION');
  }

  if (
    emaDistance >= 0.35 &&
    emaDistance <= 1.55
  ) {
    score += 8;
    reasons.push('EMA_ZONE');
  } else if (
    emaDistance > 2.1
  ) {
    score -= 15;
    warnings.push('CHASE');
  }

  if (
    breakout &&
    breakoutPct >= 0.04 &&
    breakoutPct <= 0.45
  ) {
    score += 12;
    reasons.push('BREAKOUT');
  } else if (
    breakoutPct > 0.70
  ) {
    score -= 12;
    warnings.push('BREAKOUT_EXTENDED');
  }

  if (
    atrPct >= 0.15 &&
    atrPct <= 1.8
  ) {
    score += 4;
  }

  if (
    ext5 > 2.0
  ) {
    score -= 12;
    warnings.push('EXT5');
  }

  if (
    ext10 > 4.0
  ) {
    score -= 10;
    warnings.push('EXT10');
  }

  if (
    marketRegime.regime ===
    'RISK_ON'
  ) {
    score += 10;
    reasons.push('MARKET_RISK_ON');
  } else if (
    marketRegime.regime ===
    'DEFENSIVE'
  ) {
    score -= 18;
    warnings.push('MARKET_DEFENSIVE');
  }

  score =
    clamp(
      score,
      0,
      100
    );

  let grade = 'C';

  if (
    score >= 82 &&
    marketRegime.regime ===
      'RISK_ON' &&
    !warnings.includes(
      'VOLUME_EXHAUSTION'
    ) &&
    !warnings.includes(
      'CHASE'
    )
  ) {
    grade = 'A';
  } else if (
    score >= 72
  ) {
    grade = 'B';
  }

  const eligible =
    grade ===
      C.requiredGrade &&

    score >=
      C.minScore &&

    bullish &&

    s ===
      'BULLISH' &&

    x.close > e20 &&

    e20 > e50 &&

    momentum >= 55 &&

    momentum <= 82 &&

    volumeRatio >= 1.4 &&

    volumeRatio < 3.2 &&

    bodyRatio >= 0.58 &&

    upperWickRatio <= 0.32 &&

    breakout &&

    breakoutPct >= 0.04 &&

    breakoutPct <= 0.60 &&

    emaDistance <= 2.1 &&

    ext5 <= 2.2 &&

    ext10 <= 4.0 &&

    marketRegime.regime ===
      'RISK_ON';

  return {
    symbol,
    score,
    grade,
    eligible,

    price: x.close,

    cmo: momentum,
    volumeRatio,

    emaDistance,
    emaSlope,

    breakoutPct,
    resistance,

    atr: a,
    atrPct,

    ext5,
    ext10,

    bodyRatio,
    upperWickRatio,

    structure: s,

    reasons,
    warnings,

    regime:
      marketRegime.regime,

    breadth:
      marketRegime.breadth
  };
}

// ============================================================
// CANDIDATES
// ============================================================

function addCandidate(
  symbol,
  analysis,
  closeTime
) {
  const marketPrice =
    tickers.get(symbol)?.price ||
    analysis.price;

  candidatePool.set(
    symbol,
    {
      ...analysis,

      signalPrice:
        marketPrice,

      closeTime,

      createdAt:
        Date.now(),

      expiresAt:
        Date.now() +
        C.candidateExpiryMs
    }
  );

  cloudJournal({
    type: 'CANDIDATE',
    symbol,
    score: analysis.score,
    grade: analysis.grade
  });

  scheduleRanking();
}

function scheduleRanking() {
  if (rankTimer) return;

  rankTimer =
    setTimeout(() => {
      rankTimer = null;
      rankAndExecute();
    }, 2000);
}

function candidateRank(x) {
  let rank =
    x.score;

  if (
    x.volumeRatio >= 1.7 &&
    x.volumeRatio <= 2.6
  ) {
    rank += 5;
  }

  if (
    x.emaDistance >= 0.6 &&
    x.emaDistance <= 1.4
  ) {
    rank += 4;
  }

  if (
    x.breakoutPct >= 0.08 &&
    x.breakoutPct <= 0.35
  ) {
    rank += 4;
  }

  if (
    x.cmo >= 58 &&
    x.cmo <= 70
  ) {
    rank += 3;
  }

  return rank;
}

// ============================================================
// PAPER ENTRY
// ============================================================

function openTrade(
  candidate,
  marketPrice
) {
  if (
    manualPause ||
    dailyPause ||
    cooldownActive()
  ) {
    return false;
  }

  if (
    positions[candidate.symbol]
  ) {
    return false;
  }

  if (
    symbolCooling(
      candidate.symbol
    )
  ) {
    return false;
  }

  if (
    Object.keys(
      positions
    ).length >=
    C.maxPositions
  ) {
    return false;
  }

  const entry =
    marketPrice *
    (1 + C.slippagePct);

  const atrStopPct =
    candidate.atr
      ? (
          candidate.atr *
          C.atrStopMultiplier
        ) /
        entry
      : C.minStopPct;

  const stopPct =
    clamp(
      atrStopPct,
      C.minStopPct,
      C.maxStopPct
    );

  const targetPct =
    stopPct *
    C.rewardRisk;

  let allocation =
    equity() /
    C.maxPositions;

  allocation =
    Math.min(
      allocation,
      cash
    );

  if (allocation < 50) {
    return false;
  }

  const buyFee =
    allocation *
    C.feePct;

  const usable =
    allocation -
    buyFee;

  const qty =
    usable /
    entry;

  cash -= allocation;

  stats.fees += buyFee;

  positions[
    candidate.symbol
  ] = {
    symbol:
      candidate.symbol,

    entryPrice:
      entry,

    qty,

    invested:
      allocation,

    initialStopPct:
      stopPct,

    stopLoss:
      entry *
      (1 - stopPct),

    takeProfit:
      entry *
      (1 + targetPct),

    riskPrice:
      entry * stopPct,

    lastPrice:
      marketPrice,

    highestPrice:
      marketPrice,

    lowestPrice:
      marketPrice,

    mfePct: 0,
    maePct: 0,

    breakEvenMoved: false,
    trailingActive: false,

    score:
      candidate.score,

    grade:
      candidate.grade,

    cmo:
      candidate.cmo,

    volumeRatio:
      candidate.volumeRatio,

    emaDistance:
      candidate.emaDistance,

    breakoutPct:
      candidate.breakoutPct,

    resistance:
      candidate.resistance,

    atrPct:
      candidate.atrPct,

    ext5:
      candidate.ext5,

    ext10:
      candidate.ext10,

    regime:
      candidate.regime,

    breadth:
      candidate.breadth,

    reasons:
      candidate.reasons,

    warnings:
      candidate.warnings,

    session:
      sessionUTC(),

    signalPrice:
      candidate.signalPrice,

    signalAgeMs:
      Date.now() -
      candidate.createdAt,

    entryTime:
      Date.now()
  };

  entriesSinceCooldown++;

  cloudJournal({
    type: 'ENTRY',
    symbol:
      candidate.symbol,

    score:
      candidate.score,

    entry,

    stopPct,
    targetPct,

    regime:
      candidate.regime
  });

  tg(
    `🟢 <b>LOMY V4.4 BUY</b>\n\n` +
    `<b>${candidate.symbol}</b>\n` +
    `Grade: ${candidate.grade}\n` +
    `Score: ${candidate.score}/100\n` +
    `Regime: ${candidate.regime}\n` +
    `Breadth: ${candidate.breadth}%\n` +
    `Volume: ${candidate.volumeRatio.toFixed(2)}x\n` +
    `CMO: ${candidate.cmo.toFixed(1)}\n\n` +
    `Entry: ${entry.toFixed(8)}\n` +
    `SL: ${positions[candidate.symbol].stopLoss.toFixed(8)}\n` +
    `TP: ${positions[candidate.symbol].takeProfit.toFixed(8)}`
  );

  saveCloudState();

  if (
    entriesSinceCooldown >=
    C.entriesBeforeCooldown
  ) {
    startCooldown(
      C.batchCooldownMs,
      '10_ENTRY_BATCH'
    );
  }

  return true;
}

function rankAndExecute() {
  if (
    manualPause ||
    dailyPause ||
    cooldownActive()
  ) {
    candidatePool.clear();
    return;
  }

  const free =
    C.maxPositions -
    Object.keys(
      positions
    ).length;

  if (free <= 0) {
    candidatePool.clear();
    return;
  }

  const valid = [];

  for (
    const candidate
    of candidatePool.values()
  ) {
    const price =
      tickers.get(
        candidate.symbol
      )?.price;

    if (!price) continue;

    if (
      Date.now() >
      candidate.expiresAt
    ) {
      continue;
    }

    const drift =
      pct(
        price -
          candidate.signalPrice,
        candidate.signalPrice
      );

    if (
      Math.abs(drift) >
      C.maxPriceDriftPct
    ) {
      cloudJournal({
        type: 'STALE_REJECT',
        symbol:
          candidate.symbol,

        drift
      });

      continue;
    }

    if (
      symbolCooling(
        candidate.symbol
      )
    ) {
      continue;
    }

    valid.push({
      candidate,
      price,
      rank:
        candidateRank(
          candidate
        )
    });
  }

  candidatePool.clear();

  valid.sort(
    (a, b) =>
      b.rank - a.rank
  );

  const take =
    Math.min(
      C.maxEntriesPerCycle,
      free
    );

  for (
    const x
    of valid.slice(0, take)
  ) {
    openTrade(
      x.candidate,
      x.price
    );
  }
}

// ============================================================
// POSITION MANAGEMENT
// ============================================================

function managePosition(
  symbol,
  price
) {
  const p =
    positions[symbol];

  if (!p) return;

  p.lastPrice = price;

  p.highestPrice =
    Math.max(
      p.highestPrice,
      price
    );

  p.lowestPrice =
    Math.min(
      p.lowestPrice,
      price
    );

  p.mfePct =
    Math.max(
      p.mfePct,

      pct(
        p.highestPrice -
          p.entryPrice,
        p.entryPrice
      )
    );

  p.maePct =
    Math.min(
      p.maePct,

      pct(
        p.lowestPrice -
          p.entryPrice,
        p.entryPrice
      )
    );

  const move =
    price -
    p.entryPrice;

  const r =
    p.riskPrice
      ? move /
        p.riskPrice
      : 0;

  // Move to break-even around +1R.
  if (
    !p.breakEvenMoved &&
    r >= C.breakEvenAtR
  ) {
    p.breakEvenMoved = true;

    p.stopLoss =
      Math.max(
        p.stopLoss,
        p.entryPrice *
        1.0015
      );

    stats.breakEvenMoves++;
  }

  // Trail only after stronger movement.
  if (
    r >= C.trailAtR
  ) {
    if (!p.trailingActive) {
      p.trailingActive = true;
      stats.trailingActivations++;
    }

    const trail =
      p.highestPrice -
      p.riskPrice *
      C.trailLockR;

    p.stopLoss =
      Math.max(
        p.stopLoss,
        trail
      );
  }

  if (
    price <=
    p.stopLoss
  ) {
    closeTrade(
      symbol,
      price,
      p.trailingActive
        ? 'TRAIL_STOP'
        : p.breakEvenMoved
          ? 'BREAK_EVEN_STOP'
          : 'STOP_LOSS'
    );

    return;
  }

  if (
    price >=
    p.takeProfit
  ) {
    closeTrade(
      symbol,
      price,
      'TAKE_PROFIT'
    );

    return;
  }

  // Early failure.
  const age =
    Date.now() -
    p.entryTime;

  const movePct =
    (
      price -
      p.entryPrice
    ) /
    p.entryPrice;

  if (
    age <=
      C.earlyFailureWindowMs &&

    movePct <=
      -C.earlyFailureLossPct &&

    price <
      p.resistance
  ) {
    closeTrade(
      symbol,
      price,
      'EARLY_FAILURE'
    );
  }
}

async function closeTrade(
  symbol,
  marketPrice,
  reason
) {
  const p =
    positions[symbol];

  if (!p) return;

  const exit =
    marketPrice *
    (1 - C.slippagePct);

  const gross =
    p.qty * exit;

  const fee =
    gross *
    C.feePct;

  const net =
    gross - fee;

  const profit =
    net -
    p.invested;

  const profitPct =
    pct(
      profit,
      p.invested
    );

  cash += net;

  stats.totalTrades++;
  stats.fees += fee;
  stats.netProfit += profit;

  if (profit > 0) {
    stats.wins++;

    stats.grossProfit +=
      profit;

    stats.bestTrade =
      Math.max(
        stats.bestTrade,
        profit
      );

    lossStreak = 0;
  } else {
    stats.losses++;

    stats.grossLoss +=
      Math.abs(profit);

    stats.worstTrade =
      Math.min(
        stats.worstTrade,
        profit
      );

    if (
      reason ===
        'STOP_LOSS' ||
      reason ===
        'EARLY_FAILURE'
    ) {
      lossStreak++;

      lastLossBySymbol[
        symbol
      ] =
        Date.now();
    }
  }

  if (
    reason ===
    'EARLY_FAILURE'
  ) {
    stats.earlyFailureExits++;
  }

  dailyPnL += profit;

  const record = {
    symbol,

    score: p.score,
    grade: p.grade,

    entryPrice:
      p.entryPrice,

    exitPrice:
      exit,

    invested:
      p.invested,

    profit,
    profitPct,

    reason,

    mfePct:
      p.mfePct,

    maePct:
      p.maePct,

    cmo:
      p.cmo,

    volumeRatio:
      p.volumeRatio,

    emaDistance:
      p.emaDistance,

    breakoutPct:
      p.breakoutPct,

    atrPct:
      p.atrPct,

    ext5:
      p.ext5,

    ext10:
      p.ext10,

    regime:
      p.regime,

    breadth:
      p.breadth,

    session:
      p.session,

    reasons:
      p.reasons,

    warnings:
      p.warnings,

    holdingMinutes:
      (
        Date.now() -
        p.entryTime
      ) /
      60000,

    entryTime:
      p.entryTime,

    exitTime:
      Date.now()
  };

  delete positions[symbol];

  updateDrawdown();

  checkDailyLoss();

  await saveTrade(record);

  await cloudJournal({
    type: 'CLOSE',
    ...record
  });

  await saveCloudState();

  tg(
    `${profit >= 0 ? '✅' : '❌'} <b>LOMY V4.4 CLOSE</b>\n\n` +
    `<b>${symbol}</b>\n` +
    `Reason: ${reason}\n` +
    `PnL: $${profit.toFixed(2)}\n` +
    `MFE: ${p.mfePct.toFixed(2)}%\n` +
    `MAE: ${p.maePct.toFixed(2)}%\n` +
    `Cash: $${cash.toFixed(2)}`
  );

  if (
    lossStreak >=
    C.lossStreakLimit
  ) {
    lossStreak = 0;

    startCooldown(
      C.lossCooldownMs,
      '3_LOSS_STREAK'
    );
  }
}

// ============================================================
// CLOSED CANDLE ANALYSIS
// ============================================================

function analyzeClosed(
  symbol,
  closeTime
) {
  const arr =
    candles[symbol];

  if (
    !arr ||
    arr.length <
      C.warmupCandles
  ) {
    return;
  }

  if (
    lastAnalyzed[symbol] ===
    closeTime
  ) {
    return;
  }

  lastAnalyzed[symbol] =
    closeTime;

  const a =
    analyze(
      arr,
      symbol
    );

  if (!a) return;

  let decision =
    'REJECT';

  if (
    a.eligible &&
    !manualPause &&
    !dailyPause &&
    !cooldownActive() &&
    !positions[symbol] &&
    !symbolCooling(symbol)
  ) {
    addCandidate(
      symbol,
      a,
      closeTime
    );

    decision =
      'POOL';
  }

  latest.push({
    symbol,
    score:
      a.score,

    grade:
      a.grade,

    decision,

    cmo:
      a.cmo.toFixed(1),

    volume:
      a.volumeRatio.toFixed(2),

    ema:
      a.emaDistance.toFixed(2),

    breakout:
      a.breakoutPct.toFixed(2),

    regime:
      a.regime,

    warnings:
      a.warnings.join(', ')
  });

  latest =
    latest
      .sort(
        (x, y) =>
          y.score -
          x.score
      )
      .slice(0, 60);

  cloudJournal({
    type: 'ANALYSIS',
    symbol,

    decision,

    score:
      a.score,

    grade:
      a.grade,

    cmo:
      a.cmo,

    volumeRatio:
      a.volumeRatio,

    emaDistance:
      a.emaDistance,

    breakoutPct:
      a.breakoutPct,

    regime:
      a.regime,

    breadth:
      a.breadth,

    warnings:
      a.warnings
  });
}

// ============================================================
// MINI TICKER
// ============================================================

function connectMini() {
  if (shuttingDown) return;

  if (
    miniWs &&
    (
      miniWs.readyState ===
        WebSocket.OPEN ||
      miniWs.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  miniWs =
    new WebSocket(
      `${WS_BASE}/ws/!miniTicker@arr`
    );

  miniWs.on('open', () => {
    miniConnected = true;
    lastMiniMessage = Date.now();

    console.log('MINI LIVE');
  });

  miniWs.on('message', raw => {
    lastMiniMessage = Date.now();

    let data;

    try {
      data =
        JSON.parse(
          raw.toString()
        );
    } catch {
      return;
    }

    if (!Array.isArray(data)) {
      return;
    }

    for (const item of data) {
      const symbol =
        item.s;

      if (
        !symbol ||
        !symbol.endsWith('USDT') ||
        ignored(symbol)
      ) {
        continue;
      }

      const price =
        n(item.c);

      const quoteVolume =
        n(item.q);

      if (price <= 0) {
        continue;
      }

      tickers.set(
        symbol,
        {
          price,
          quoteVolume,
          updatedAt:
            Date.now()
        }
      );

      if (
        positions[symbol]
      ) {
        managePosition(
          symbol,
          price
        );
      }
    }

    if (
      !subscribed.size &&
      tickers.size > 100
    ) {
      setTimeout(
        rebalanceUniverse,
        2000
      );
    }
  });

  miniWs.on('close', () => {
    miniConnected = false;

    setTimeout(
      connectMini,
      5000
    );
  });

  miniWs.on('error', e => {
    console.error(
      'MINI:',
      e.message
    );
  });
}

// ============================================================
// KLINE WEBSOCKET
// ============================================================

const controlQueue = [];
let controlBusy = false;

function queueControl(message) {
  controlQueue.push(message);
}

setInterval(async () => {
  if (
    controlBusy ||
    !controlQueue.length ||
    !klineWs ||
    klineWs.readyState !==
      WebSocket.OPEN
  ) {
    return;
  }

  controlBusy = true;

  const msg =
    controlQueue.shift();

  try {
    klineWs.send(
      JSON.stringify(msg)
    );
  } catch {}

  await sleep(1000);

  controlBusy = false;
}, 250);

function connectKline() {
  if (shuttingDown) return;

  if (
    klineWs &&
    (
      klineWs.readyState ===
        WebSocket.OPEN ||
      klineWs.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  klineWs =
    new WebSocket(
      `${WS_BASE}/ws`
    );

  klineWs.on('open', () => {
    klineConnected = true;
    lastKlineMessage = Date.now();

    console.log('KLINE LIVE');

    if (subscribed.size) {
      queueControl({
        method: 'SUBSCRIBE',

        params:
          Array.from(subscribed)
            .map(
              s =>
                `${s.toLowerCase()}@kline_${C.interval}`
            ),

        id:
          Date.now()
      });
    }
  });

  klineWs.on('message', raw => {
    lastKlineMessage =
      Date.now();

    let event;

    try {
      event =
        JSON.parse(
          raw.toString()
        );
    } catch {
      return;
    }

    if (
      Object.prototype
        .hasOwnProperty
        .call(event, 'result')
    ) {
      return;
    }

    if (
      event.e !== 'kline' ||
      !event.k ||
      event.k.x !== true
    ) {
      return;
    }

    const symbol =
      event.s;

    const k =
      event.k;

    const candle = {
      open: n(k.o),
      high: n(k.h),
      low: n(k.l),
      close: n(k.c),
      volume: n(k.v),
      closeTime: n(k.T)
    };

    mergeCandles(
      symbol,
      [candle]
    );

    warmupLoaded.add(symbol);

    analyzeClosed(
      symbol,
      candle.closeTime
    );

    saveSymbolCandles(
      symbol
    );
  });

  klineWs.on('close', () => {
    klineConnected = false;

    setTimeout(
      connectKline,
      5000
    );
  });

  klineWs.on('error', e => {
    console.error(
      'KLINE:',
      e.message
    );
  });
}

// ============================================================
// UNIVERSE
// ============================================================

function topSymbols() {
  let result =
    Array.from(
      tickers.entries()
    )
      .filter(
        ([symbol, t]) =>
          symbol.endsWith('USDT') &&
          !ignored(symbol) &&
          t.quoteVolume >=
            C.minQuoteVolume
      )
      .sort(
        (a, b) =>
          b[1].quoteVolume -
          a[1].quoteVolume
      )
      .slice(
        0,
        C.universeSize
      )
      .map(x => x[0]);

  if (
    !result.includes(
      C.btcSymbol
    )
  ) {
    result.unshift(
      C.btcSymbol
    );
  }

  return result.slice(
    0,
    C.universeSize
  );
}

function rebalanceUniverse() {
  const wanted =
    new Set(
      topSymbols()
    );

  if (!wanted.size) {
    return;
  }

  const add = [];
  const remove = [];

  for (const symbol of wanted) {
    if (
      !subscribed.has(symbol)
    ) {
      add.push(symbol);
    }
  }

  for (const symbol of subscribed) {
    if (
      !wanted.has(symbol)
    ) {
      if (
        positions[symbol]
      ) {
        wanted.add(symbol);
      } else {
        remove.push(symbol);
      }
    }
  }

  if (remove.length) {
    queueControl({
      method:
        'UNSUBSCRIBE',

      params:
        remove.map(
          s =>
            `${s.toLowerCase()}@kline_${C.interval}`
        ),

      id:
        Date.now()
    });
  }

  if (add.length) {
    for (const symbol of add) {
      queueWarmup(symbol);
    }

    queueControl({
      method:
        'SUBSCRIBE',

      params:
        add.map(
          s =>
            `${s.toLowerCase()}@kline_${C.interval}`
        ),

      id:
        Date.now() + 1
    });
  }

  subscribed = wanted;

  console.log(
    `Universe ${subscribed.size} | Ready ${readyCount()} | Cloud ${warmupStats.cloudLoaded} | REST ${warmupStats.restLoaded}`
  );
}

setInterval(
  rebalanceUniverse,
  C.universeRefreshMs
);

// ============================================================
// PERIODIC TASKS
// ============================================================

setInterval(() => {
  checkDay();
  cooldownActive();
  updateDrawdown();

  saveCloudState();
}, C.stateSaveMs);

setInterval(() => {
  calculateMarketRegime();
}, C.regimeRefreshMs);

setInterval(() => {
  for (const symbol of subscribed) {
    if (
      candles[symbol]?.length
    ) {
      saveSymbolCandles(symbol);
    }
  }
}, C.candleCloudSaveMs);

setInterval(() => {
  if (
    miniConnected &&
    Date.now() -
      lastMiniMessage >
      90000
  ) {
    try {
      miniWs.terminate();
    } catch {}
  }

  if (
    klineConnected &&
    Date.now() -
      lastKlineMessage >
      10 * 60 * 1000
  ) {
    try {
      klineWs.terminate();
    } catch {}
  }

  if (!miniConnected) {
    connectMini();
  }

  if (!klineConnected) {
    connectKline();
  }
}, 30000);

// ============================================================
// API
// ============================================================

app.post('/api/pause', async (req, res) => {
  manualPause = true;
  candidatePool.clear();

  await saveCloudState();

  res.json({
    success: true
  });
});

app.post('/api/resume', async (req, res) => {
  manualPause = false;
  candidatePool.clear();

  await saveCloudState();

  res.json({
    success: true
  });
});

app.post('/api/emergency-close', async (req, res) => {
  const list =
    Object.keys(positions);

  let closed = 0;

  for (const symbol of list) {
    const price =
      tickers.get(symbol)?.price ||
      positions[symbol].lastPrice;

    if (!price) continue;

    await closeTrade(
      symbol,
      price,
      'EMERGENCY_CLOSE'
    );

    closed++;
  }

  res.json({
    success: true,
    closed
  });
});

app.get('/api/data', async (req, res) => {
  const closed =
    stats.wins +
    stats.losses;

  const winRate =
    closed
      ? (
          stats.wins /
          closed
        ) * 100
      : 0;

  const profitFactor =
    stats.grossLoss
      ? stats.grossProfit /
        stats.grossLoss
      : stats.grossProfit
        ? 999
        : 0;

  let recentTrades = [];
  let recentJournal = [];

  if (cloudConnected) {
    try {
      recentTrades =
        await db
          .collection('trades')
          .find({})
          .sort({
            exitTime: -1
          })
          .limit(
            C.historyLimitDashboard
          )
          .toArray();

      recentJournal =
        await db
          .collection('journal')
          .find({})
          .sort({
            time: -1
          })
          .limit(
            C.journalLimitDashboard
          )
          .toArray();
    } catch {}
  }

  res.json({
    version:
      C.version,

    cloudConnected,

    miniConnected,
    klineConnected,

    cash:
      cash.toFixed(2),

    equity:
      equity().toFixed(2),

    open:
      Object.keys(
        positions
      ).length,

    positions:
      Object.values(
        positions
      ),

    symbols:
      subscribed.size,

    ready:
      readyCount(),

    cloudReady:
      warmupStats.cloudLoaded,

    restReady:
      warmupStats.restLoaded,

    restRequests:
      warmupStats.restRequests,

    warmupQueue:
      warmupQueue.length,

    marketRegime,

    dailyPnL:
      dailyPnL.toFixed(2),

    cooldown:
      cooldownActive(),

    cooldownReason,

    cooldownMinutes:
      Math.ceil(
        Math.max(
          0,
          cooldownUntil -
            Date.now()
        ) /
        60000
      ),

    entriesSinceCooldown,

    lossStreak,

    stats: {
      ...stats,

      winRate:
        Number(
          winRate.toFixed(2)
        ),

      profitFactor:
        Number(
          profitFactor.toFixed(2)
        )
    },

    latest,
    recentTrades,
    recentJournal
  });
});

app.get('/health', (req, res) => {
  res.json({
    status:
      cloudConnected &&
      miniConnected &&
      klineConnected
        ? 'OK'
        : 'DEGRADED',

    version:
      C.version,

    cloud:
      cloudConnected,

    websocket:
      miniConnected &&
      klineConnected,

    ready:
      readyCount(),

    symbols:
      subscribed.size,

    regime:
      marketRegime,

    equity:
      equity().toFixed(2)
  });
});

// ============================================================
// DASHBOARD
// ============================================================

app.get('/', (req, res) => {
  res.send(`
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOMY V4.4</title>
<style>
*{box-sizing:border-box}
body{margin:0;background:#0b0e11;color:#eaecef;font-family:Arial;padding:16px;text-align:center}
h1{color:#f3ba2f}
.banner{background:#f3ba2f;color:#111;padding:12px;border-radius:10px;font-weight:bold;max-width:1200px;margin:auto}
.status{margin:10px;font-weight:bold}
.green{color:#0ecb81}.red{color:#f6465d}.yellow{color:#f3ba2f}.blue{color:#4da3ff}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px;max-width:1300px;margin:18px auto}
.card{background:#1e2329;border:1px solid #2b3139;padding:14px;border-radius:10px}
.label{font-size:11px;color:#848e9c}
.value{font-size:21px;font-weight:bold;margin-top:7px}
button{border:0;padding:12px 18px;margin:5px;border-radius:8px;font-weight:bold;cursor:pointer}
.pause{background:#f3ba2f}.resume{background:#0ecb81}.close{background:#f6465d;color:#fff}
table{width:100%;max-width:1400px;margin:20px auto;border-collapse:collapse;background:#1e2329}
th{background:#2b3139;color:#848e9c}
td,th{font-size:11px;padding:8px;border-bottom:1px solid #2b3139}
</style>
</head>
<body>

<h1>🤖 LOMY V4.4 CLOUD PRECISION</h1>

<div class="banner">
CLOUD MEMORY • SMART WARMUP • MARKET REGIME • PRECISION ENTRY • PAPER ONLY
</div>

<div id="cloud" class="status">Cloud...</div>
<div id="ws" class="status">Market...</div>
<div id="regime" class="status">Market regime...</div>
<div id="cooldown" class="status">Cooldown...</div>

<div class="grid">

<div class="card"><div class="label">CASH</div><div class="value" id="cash">$0</div></div>
<div class="card"><div class="label">EQUITY</div><div class="value" id="equity">$0</div></div>

<div class="card"><div class="label">CLOSED</div><div class="value" id="closed">0</div></div>
<div class="card"><div class="label">WIN RATE</div><div class="value" id="win">0%</div></div>

<div class="card"><div class="label">NET PROFIT</div><div class="value" id="profit">$0</div></div>
<div class="card"><div class="label">PROFIT FACTOR</div><div class="value" id="pf">0</div></div>

<div class="card"><div class="label">OPEN</div><div class="value" id="open">0</div></div>
<div class="card"><div class="label">READY</div><div class="value" id="ready">0</div></div>

<div class="card"><div class="label">WS SYMBOLS</div><div class="value" id="symbols">0</div></div>
<div class="card"><div class="label">CLOUD READY</div><div class="value" id="cloudReady">0</div></div>

<div class="card"><div class="label">REST RECOVERY</div><div class="value" id="rest">0</div></div>
<div class="card"><div class="label">BREADTH</div><div class="value" id="breadth">0%</div></div>

<div class="card"><div class="label">TODAY PNL</div><div class="value" id="daily">$0</div></div>
<div class="card"><div class="label">MAX DD</div><div class="value" id="dd">0%</div></div>

<div class="card"><div class="label">BATCH</div><div class="value" id="batch">0/10</div></div>
<div class="card"><div class="label">LOSS STREAK</div><div class="value" id="loss">0/3</div></div>

<div class="card"><div class="label">EARLY EXITS</div><div class="value" id="early">0</div></div>
<div class="card"><div class="label">BREAK EVEN</div><div class="value" id="be">0</div></div>

</div>

<button class="pause" onclick="post('/api/pause')">⏸ PAUSE</button>
<button class="resume" onclick="post('/api/resume')">▶ RESUME</button>
<button class="close" onclick="closeAll()">🚨 CLOSE ALL</button>

<div style="overflow-x:auto">
<table>
<thead>
<tr>
<th>Symbol</th>
<th>Grade</th>
<th>Score</th>
<th>Status</th>
<th>CMO</th>
<th>Volume</th>
<th>EMA%</th>
<th>Breakout%</th>
<th>Regime</th>
<th>Warnings</th>
</tr>
</thead>
<tbody id="rows">
<tr><td colspan="10">Loading...</td></tr>
</tbody>
</table>
</div>

<script>

async function post(url){
 await fetch(url,{method:'POST'});
 load();
}

async function closeAll(){
 if(!confirm('Close all PAPER positions?')) return;
 await post('/api/emergency-close');
}

async function load(){
 try{
  const r=await fetch('/api/data');
  const d=await r.json();

  cash.innerText='$'+d.cash;
  equity.innerText='$'+d.equity;

  closed.innerText=d.stats.totalTrades;
  win.innerText=d.stats.winRate+'%';

  profit.innerText='$'+Number(d.stats.netProfit).toFixed(2);
  pf.innerText=d.stats.profitFactor;

  open.innerText=d.open;
  ready.innerText=d.ready;

  symbols.innerText=d.symbols;
  cloudReady.innerText=d.cloudReady;

  rest.innerText=d.restReady;
  breadth.innerText=d.marketRegime.breadth+'%';

  daily.innerText='$'+d.dailyPnL;
  dd.innerText=Number(d.stats.maxDrawdown).toFixed(2)+'%';

  batch.innerText=d.entriesSinceCooldown+'/10';
  loss.innerText=d.lossStreak+'/3';

  early.innerText=d.stats.earlyFailureExits;
  be.innerText=d.stats.breakEvenMoves;

  cloud.innerText=d.cloudConnected
   ?'☁️ CLOUD DB CONNECTED'
   :'🔴 CLOUD DB DISCONNECTED';

  cloud.className=d.cloudConnected
   ?'status green'
   :'status red';

  ws.innerText=(d.miniConnected&&d.klineConnected)
   ?'🟢 MARKET WEBSOCKETS LIVE'
   :'🔴 MARKET CONNECTING';

  ws.className=(d.miniConnected&&d.klineConnected)
   ?'status green'
   :'status red';

  regime.innerText=
   'MARKET REGIME: '+d.marketRegime.regime+
   ' • BREADTH '+d.marketRegime.breadth+'%';

  regime.className=
   d.marketRegime.regime==='RISK_ON'
   ?'status green'
   :d.marketRegime.regime==='NEUTRAL'
   ?'status yellow'
   :'status red';

  cooldown.innerText=d.cooldown
   ?'🧠 COOLDOWN '+d.cooldownReason+' • '+d.cooldownMinutes+' MIN'
   :'✅ SMART COOLDOWN READY';

  cooldown.className=d.cooldown
   ?'status yellow'
   :'status green';

  rows.innerHTML='';

  if(!d.latest.length){
   rows.innerHTML=
   '<tr><td colspan="10">Warmup '+d.ready+' / '+d.symbols+'</td></tr>';
   return;
  }

  d.latest.forEach(x=>{
   rows.innerHTML+=
   '<tr>'+
   '<td><b>'+x.symbol+'</b></td>'+
   '<td>'+x.grade+'</td>'+
   '<td>'+x.score+'</td>'+
   '<td>'+x.decision+'</td>'+
   '<td>'+x.cmo+'</td>'+
   '<td>'+x.volume+'x</td>'+
   '<td>'+x.ema+'%</td>'+
   '<td>'+x.breakout+'%</td>'+
   '<td>'+x.regime+'</td>'+
   '<td>'+x.warnings+'</td>'+
   '</tr>';
  });

 }catch(e){
  console.error(e);
 }
}

setInterval(load,3000);
load();

</script>
</body>
</html>
  `);
});

// ============================================================
// SHUTDOWN
// ============================================================

async function shutdown(signal) {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(
    `${signal} - saving cloud memory`
  );

  try {
    await saveCloudState();

    for (
      const symbol
      of subscribed
    ) {
      if (
        candles[symbol]?.length
      ) {
        await saveSymbolCandles(
          symbol
        );
      }
    }
  } catch {}

  try {
    miniWs?.close();
  } catch {}

  try {
    klineWs?.close();
  } catch {}

  try {
    await mongoClient?.close();
  } catch {}

  process.exit(0);
}

process.on(
  'SIGTERM',
  () => shutdown('SIGTERM')
);

process.on(
  'SIGINT',
  () => shutdown('SIGINT')
);

process.on(
  'unhandledRejection',
  e => console.error(
    'UNHANDLED:',
    e
  )
);

process.on(
  'uncaughtException',
  e => console.error(
    'UNCAUGHT:',
    e
  )
);

// ============================================================
// START
// ============================================================

app.listen(PORT, async () => {
  console.log('');
  console.log('===============================================');
  console.log('LOMY V4.4 CLOUD PRECISION');
  console.log('===============================================');
  console.log('Execution: PAPER ONLY');
  console.log('Cloud Memory: MongoDB');
  console.log('Historical Recovery: Binance REST');
  console.log('Live Market: Binance WebSocket');
  console.log(`Universe: TOP ${C.universeSize}`);
  console.log(`Max Positions: ${C.maxPositions}`);
  console.log('Market Regime: ON');
  console.log('BTC Context: ON');
  console.log('Dynamic Ranking: ON');
  console.log('ATR Risk: ON');
  console.log('Break Even: ON');
  console.log('Trailing Protection: ON');
  console.log('Anti Chase: ON');
  console.log('Smart Cooldown: ON');
  console.log('===============================================');

  try {
    await connectCloud();

    await loadCloudState();

    connectMini();
    connectKline();

    calculateMarketRegime();

    tg(
      `🚀 <b>LOMY V4.4 CLOUD PRECISION</b>\n\n` +
      `☁️ Cloud Memory: <b>CONNECTED</b>\n` +
      `📡 Live Market: <b>WEBSOCKET</b>\n` +
      `📚 Smart Warmup: <b>ON</b>\n` +
      `🧠 Market Regime: <b>ON</b>\n` +
      `🛡 Risk Engine: <b>ON</b>\n` +
      `💰 Execution: <b>PAPER ONLY</b>\n\n` +
      `Equity: $${equity().toFixed(2)}`
    );

  } catch (e) {
    console.error(
      'STARTUP FAILED:',
      e
    );

    tg(
      `🔴 <b>LOMY CLOUD STARTUP FAILED</b>\n` +
      e.message
    );
  }
});
