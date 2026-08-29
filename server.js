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
// LOMY V5.1 BANDWIDTH OPTIMIZED — PAPER ONLY
// ============================================================
const C = {
  version: '5.1-BANDWIDTH-OPTIMIZED',
  stateKey: 'main-v51',
  paperTrading: true,
  startingBalance: 10000,

  interval: '5m',
  warmupCandles: 60,
  maxCandles: 100,
  universeSize: 220,
  minQuoteVolume: 500000,
  universeRefreshMs: 30 * 60 * 1000,

  maxPositions: 6,
  maxEntriesPerCycle: 2,
  candidateExpiryMs: 2 * 60 * 1000,
  maxPriceDriftPct: 0.22,
  liveChasePct: 0.16,

  cmoCoreMin: 50,
  cmoCoreMax: 65,
  cmoSoftMin: 48,
  cmoSoftMax: 68,

  minTakerBuyRatio: 0.60,
  eliteTakerBuyRatio: 0.80,
  minFlowMomentum: 0.50,

  minAtrPct: 0.18,
  maxAtrPct: 0.65,
  atrSweetMin: 0.22,
  atrSweetMax: 0.55,

  minVolumeRatio: 1.05,
  maxVolumeRatio: 2.80,
  volumeAccelSweetMin: 0.75,
  volumeAccelSweetMax: 2.50,
  volumeClimaxAcceleration: 3.00,

  maxEmaDistancePct: 1.35,
  maxExt5Pct: 1.80,
  maxExt10Pct: 3.00,

  minBreakoutAcceptance: 55,
  eliteBreakoutAcceptance: 78,
  retestTolerancePct: 0.18,

  minEdgeScoreRiskOn: 70,
  minEdgeScoreNeutral: 73,
  minEdgeScoreDefensive: 88,
  minFreshnessScore: 58,
  minFlowScore: 55,

  feePct: 0.001,
  slippagePct: 0.0005,
  minStopPct: 0.0055,
  maxStopPct: 0.0105,
  atrStopMultiplier: 1.20,
  rewardRisk: 1.90,

  profitLockMfePct: 0.35,
  profitLockBufferPct: 0.0022,
  breakEvenAtR: 0.55,
  breakEvenBufferPct: 0.0025,
  trailAtR: 0.95,
  trailLockR: 0.30,

  earlyFailureWindowMs: 10 * 60 * 1000,
  earlyFailureLossPct: 0.0035,
  earlyFailureMfeGuardPct: 0.25,

  dailyLossLimitPct: 0.035,
  entriesBeforeCooldown: 12,
  batchCooldownMs: 20 * 60 * 1000,
  lossStreakLimit: 3,
  lossCooldownMs: 60 * 60 * 1000,
  symbolLossCooldownMs: 90 * 60 * 1000,

  asiaRiskOnPenalty: 12,
  overextendedRiskOnBreadth: 78,
  warmupConcurrency: 3,
  warmupDelayMs: 400,
  btcSymbol: 'BTCUSDT',

  stateSaveMs: 60000,
  regimeRefreshMs: 60000,
  journalLimitDashboard: 100,
  historyLimitDashboard: 100,

  // Bandwidth protection
  persistCandlesToCloud: false,
  analysisJournalSampleRate: 0.01,
  networkAlertMB: 250,
  networkReportMs: 30 * 60 * 1000
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
  trailingActivations: 0,
  profitLocks: 0
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
let rankTimer = null;

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
let warmupStats = { restLoaded: 0, restRequests: 0, failed: 0 };

let marketRegime = {
  ready: false,
  btcBullish: false,
  btcScore: 0,
  btcFreshness: 0,
  breadth: 0,
  regime: 'WARMING',
  overextended: false,
  updatedAt: 0
};

// ============================================================
// BANDWIDTH METER
// App-level estimate of outbound payloads produced by this app.
// Render's own Usage page remains the final billing source.
// ============================================================
const networkMeter = {
  startedAt: Date.now(),
  telegramBytes: 0,
  mongoWriteBytes: 0,
  mongoStateBytes: 0,
  mongoJournalBytes: 0,
  mongoTradeBytes: 0,
  restRequestBytes: 0,
  wsControlBytes: 0,
  journalWrites: 0,
  tradeWrites: 0,
  stateWrites: 0,
  skippedAnalysisLogs: 0,
  candleCloudWrites: 0,
  lastAlertBytes: 0
};

// ============================================================
// HELPERS
// ============================================================
const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

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

function byteLen(v) {
  try {
    return Buffer.byteLength(
      typeof v === 'string' ? v : JSON.stringify(v),
      'utf8'
    );
  } catch {
    return 0;
  }
}

function networkTotalBytes() {
  return (
    networkMeter.telegramBytes +
    networkMeter.mongoWriteBytes +
    networkMeter.restRequestBytes +
    networkMeter.wsControlBytes
  );
}

function networkMB(bytes) {
  return +(bytes / 1024 / 1024).toFixed(2);
}

function networkSnapshot() {
  return {
    estimatedOutboundMB: networkMB(networkTotalBytes()),
    telegramMB: networkMB(networkMeter.telegramBytes),
    mongoWriteMB: networkMB(networkMeter.mongoWriteBytes),
    mongoStateMB: networkMB(networkMeter.mongoStateBytes),
    mongoJournalMB: networkMB(networkMeter.mongoJournalBytes),
    mongoTradeMB: networkMB(networkMeter.mongoTradeBytes),
    restRequestMB: networkMB(networkMeter.restRequestBytes),
    wsControlMB: networkMB(networkMeter.wsControlBytes),
    journalWrites: networkMeter.journalWrites,
    tradeWrites: networkMeter.tradeWrites,
    stateWrites: networkMeter.stateWrites,
    skippedAnalysisLogs: networkMeter.skippedAnalysisLogs,
    candleCloudWrites: networkMeter.candleCloudWrites,
    uptimeHours: +(
      (Date.now() - networkMeter.startedAt) /
      3600000
    ).toFixed(2)
  };
}

const IGNORED = new Set([
  'USDCUSDT',
  'FDUSDUSDT',
  'TUSDUSDT',
  'USDPUSDT',
  'BUSDUSDT',
  'DAIUSDT',
  'USDEUSDT',
  'USD1USDT'
]);

function ignored(symbol) {
  return IGNORED.has(symbol);
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
  if (TELEGRAM_TOKEN && CHAT_ID) {
    tgQueue.push(text);
  }
}

setInterval(async () => {
  if (tgBusy || !tgQueue.length) {
    return;
  }

  tgBusy = true;

  const text = tgQueue.shift();

  try {
    const payload = {
      chat_id: CHAT_ID,
      text,
      parse_mode: 'HTML'
    };

    networkMeter.telegramBytes += byteLen(payload);

    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
      payload,
      { timeout: 10000 }
    );
  } catch (error) {
    if (error.response?.status === 429) {
      tgQueue.unshift(text);
      await sleep(5000);
    } else {
      console.error('Telegram:', error.message);
    }
  } finally {
    tgBusy = false;
  }
}, 1200);

// ============================================================
// MONGODB — NO CANDLE ARRAY WRITES IN V5.1
// ============================================================
async function connectCloud() {
  if (!MONGODB_URI) {
    throw new Error('MONGODB_URI missing');
  }

  mongoClient = new MongoClient(
    MONGODB_URI,
    { maxPoolSize: 10 }
  );

  await mongoClient.connect();

  db = mongoClient.db(MONGODB_DB);

  await db.command({ ping: 1 });

  cloudConnected = true;

  await Promise.all([
    db
      .collection('trades')
      .createIndex({
        version: 1,
        exitTime: -1
      }),

    db
      .collection('journal')
      .createIndex({
        version: 1,
        time: -1
      }),

    db
      .collection('journal')
      .createIndex({
        symbol: 1,
        time: -1
      })
  ]);

  console.log('MongoDB CLOUD CONNECTED');
}

async function loadCloudState() {
  const state =
    await db
      .collection('state')
      .findOne({
        _id: C.stateKey
      });

  if (!state) {
    console.log('Fresh V5.1 PAPER account.');
    return;
  }

  cash = n(
    state.cash,
    C.startingBalance
  );

  positions =
    state.positions || {};

  stats = {
    ...stats,
    ...(state.stats || {})
  };

  dailyPnL =
    n(state.dailyPnL);

  dailyStartEquity =
    n(
      state.dailyStartEquity,
      C.startingBalance
    );

  currentDay =
    state.currentDay ||
    utcDay();

  peakEquity =
    n(
      state.peakEquity,
      C.startingBalance
    );

  manualPause =
    !!state.manualPause;

  dailyPause =
    !!state.dailyPause;

  cooldownUntil =
    n(state.cooldownUntil);

  cooldownReason =
    state.cooldownReason ||
    null;

  entriesSinceCooldown =
    n(state.entriesSinceCooldown);

  lossStreak =
    n(state.lossStreak);

  Object.assign(
    lastLossBySymbol,
    state.lastLossBySymbol || {}
  );

  console.log(
    `V5.1 STATE RESTORED | Cash $${cash.toFixed(2)} | Open ${Object.keys(positions).length}`
  );
}

async function saveCloudState() {
  if (!cloudConnected) {
    return;
  }

  try {
    const payload = {
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
    };

    const bytes =
      byteLen(payload);

    networkMeter.mongoWriteBytes +=
      bytes;

    networkMeter.mongoStateBytes +=
      bytes;

    networkMeter.stateWrites++;

    await db
      .collection('state')
      .updateOne(
        {
          _id: C.stateKey
        },
        {
          $set: payload
        },
        {
          upsert: true
        }
      );
  } catch (error) {
    console.error(
      'State save:',
      error.message
    );
  }
}

async function cloudJournal(row) {
  if (!cloudConnected) {
    return;
  }

  try {
    const doc = {
      time: Date.now(),
      version: C.version,
      ...row
    };

    const bytes =
      byteLen(doc);

    networkMeter.mongoWriteBytes +=
      bytes;

    networkMeter.mongoJournalBytes +=
      bytes;

    networkMeter.journalWrites++;

    await db
      .collection('journal')
      .insertOne(doc);
  } catch (error) {
    console.error(
      'Journal:',
      error.message
    );
  }
}

async function saveTrade(record) {
  if (!cloudConnected) {
    return;
  }

  try {
    const doc = {
      version: C.version,
      ...record
    };

    const bytes =
      byteLen(doc);

    networkMeter.mongoWriteBytes +=
      bytes;

    networkMeter.mongoTradeBytes +=
      bytes;

    networkMeter.tradeWrites++;

    await db
      .collection('trades')
      .insertOne(doc);
  } catch (error) {
    console.error(
      'Trade save:',
      error.message
    );
  }
}

// ============================================================
// CANDLES / WARMUP
// Candles remain in RAM. REST is used only for initial warmup.
// ============================================================
function parseKline(row) {
  return {
    open: n(row[1]),
    high: n(row[2]),
    low: n(row[3]),
    close: n(row[4]),
    volume: n(row[5]),
    closeTime: n(row[6]),
    quoteVolume: n(row[7]),
    trades: n(row[8]),
    takerBuyBase: n(row[9]),
    takerBuyQuote: n(row[10])
  };
}

function mergeCandles(symbol, incoming) {
  const map = new Map();

  for (const c of candles[symbol] || []) {
    map.set(c.closeTime, c);
  }

  for (const c of incoming || []) {
    map.set(c.closeTime, c);
  }

  candles[symbol] = [...map.values()]
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
    warmupStats.restRequests++;

    const requestDesc =
      `${REST_BASE}/api/v3/klines?symbol=${symbol}&interval=${C.interval}&limit=${C.maxCandles}`;

    networkMeter.restRequestBytes +=
      byteLen(requestDesc);

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

    mergeCandles(
      symbol,
      response.data
        .map(parseKline)
        .filter(c => c.closeTime < now)
    );

    if (
      (candles[symbol]?.length || 0) >=
      C.warmupCandles
    ) {
      warmupLoaded.add(symbol);
      warmupStats.restLoaded++;
    }

  } catch (error) {

    warmupStats.failed++;

    console.error(
      `Warmup ${symbol}:`,
      error.response?.status ||
      error.message
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
// RISK / EQUITY
// ============================================================

function equity() {
  let value = cash;

  for (
    const p
    of Object.values(positions)
  ) {
    const price =
      tickers.get(p.symbol)?.price ||
      p.lastPrice ||
      p.entryPrice;

    value +=
      p.qty *
      price;
  }

  return value;
}

function updateDrawdown() {
  const e =
    equity();

  if (
    e >
    peakEquity
  ) {
    peakEquity =
      e;
  }

  const dd =
    peakEquity
      ? (
          (
            peakEquity -
            e
          ) /
          peakEquity
        ) *
        100
      : 0;

  stats.maxDrawdown =
    Math.max(
      stats.maxDrawdown,
      dd
    );
}

function checkDay() {
  const day =
    utcDay();

  if (
    day ===
    currentDay
  ) {
    return;
  }

  currentDay =
    day;

  dailyPnL =
    0;

  dailyPause =
    false;

  dailyStartEquity =
    equity();

  saveCloudState();
}

function checkDailyLoss() {
  const limit =
    dailyStartEquity *
    C.dailyLossLimitPct;

  if (
    !dailyPause &&
    dailyPnL <=
      -limit
  ) {
    dailyPause =
      true;

    candidatePool.clear();

    tg(
      `🛑 <b>LOMY V5.1 DAILY PROTECTION</b>
PnL: $${dailyPnL.toFixed(2)}`
    );

    saveCloudState();
  }
}

function cooldownActive() {
  if (
    !cooldownUntil
  ) {
    return false;
  }

  if (
    Date.now() >=
    cooldownUntil
  ) {
    cooldownUntil =
      0;

    cooldownReason =
      null;

    entriesSinceCooldown =
      0;

    candidatePool.clear();

    saveCloudState();

    return false;
  }

  return true;
}

function startCooldown(
  ms,
  reason
) {
  const until =
    Date.now() +
    ms;

  if (
    until <=
    cooldownUntil
  ) {
    return;
  }

  cooldownUntil =
    until;

  cooldownReason =
    reason;

  candidatePool.clear();

  tg(
    `⏸ <b>LOMY V5.1 COOLDOWN</b>
${reason}
${Math.ceil(ms / 60000)} minutes`
  );

  saveCloudState();
}

function symbolCooling(symbol) {
  const lastLoss =
    n(
      lastLossBySymbol[symbol]
    );

  return (
    !!lastLoss &&
    Date.now() -
      lastLoss <
      C.symbolLossCooldownMs
  );
}

// ============================================================
// INDICATORS
// ============================================================

function sma(
  arr,
  period,
  key = 'close'
) {
  if (
    arr.length <
    period
  ) {
    return null;
  }

  return (
    arr
      .slice(-period)
      .reduce(
        (
          s,
          x
        ) =>
          s +
          n(x[key]),
        0
      ) /
    period
  );
}

function ema(
  arr,
  period,
  key = 'close'
) {
  if (
    arr.length <
    period
  ) {
    return null;
  }

  let value =
    arr
      .slice(
        0,
        period
      )
      .reduce(
        (
          s,
          x
        ) =>
          s +
          n(x[key]),
        0
      ) /
    period;

  const m =
    2 /
    (
      period +
      1
    );

  for (
    let i =
      period;
    i <
    arr.length;
    i++
  ) {
    value =
      (
        n(
          arr[i][key]
        ) -
        value
      ) *
      m +
      value;
  }

  return value;
}

function cmo(
  arr,
  period = 9
) {
  if (
    arr.length <
    period +
      1
  ) {
    return null;
  }

  let up = 0;
  let down = 0;

  for (
    let i =
      arr.length -
      period;
    i <
    arr.length;
    i++
  ) {
    const d =
      arr[i].close -
      arr[i - 1].close;

    if (
      d >
      0
    ) {
      up +=
        d;

    } else {

      down +=
        Math.abs(d);
    }
  }

  return (
    up +
    down
  )
    ? (
        100 *
        (
          up -
          down
        )
      ) /
      (
        up +
        down
      )
    : 0;
}

function atr(
  arr,
  period = 14
) {
  if (
    arr.length <
    period +
      1
  ) {
    return null;
  }

  const ranges =
    [];

  for (
    let i = 1;
    i < arr.length;
    i++
  ) {
    const a =
      arr[i];

    const b =
      arr[i - 1];

    ranges.push(
      Math.max(
        a.high -
          a.low,

        Math.abs(
          a.high -
            b.close
        ),

        Math.abs(
          a.low -
            b.close
        )
      )
    );
  }

  const recent =
    ranges.slice(
      -period
    );

  return (
    recent.reduce(
      (
        s,
        x
      ) =>
        s +
        x,
      0
    ) /
    recent.length
  );
}

function structure(arr) {
  if (
    arr.length <
    4
  ) {
    return 'NEUTRAL';
  }

  const a =
    arr.at(-1);

  const b =
    arr.at(-2);

  const c =
    arr.at(-3);

  if (
    a.high >
      b.high &&
    a.low >
      b.low &&
    b.low >=
      c.low
  ) {
    return 'BULLISH';
  }

  if (
    a.high <
      b.high &&
    a.low <
      b.low
  ) {
    return 'BEARISH';
  }

  return 'NEUTRAL';
}

function volumeAcceleration(arr) {
  if (
    arr.length <
    7
  ) {
    return 0;
  }

  const recent =
    arr
      .slice(-3)
      .reduce(
        (
          s,
          c
        ) =>
          s +
          c.volume,
        0
      ) /
    3;

  const previous =
    arr
      .slice(
        -6,
        -3
      )
      .reduce(
        (
          s,
          c
        ) =>
          s +
          c.volume,
        0
      ) /
    3;

  return previous >
    0
    ? recent /
      previous
    : 0;
}

function priceAcceleration(arr) {
  if (
    arr.length <
    4
  ) {
    return 0;
  }

  const a =
    arr.at(-1).close;

  const b =
    arr.at(-2).close;

  const c =
    arr.at(-3).close;

  return (
    pct(
      a -
        b,
      b
    ) -
    pct(
      b -
        c,
      c
    )
  );
}

function takerBuyRatio(candle) {
  return (
    candle.volume >
      0 &&
    candle.takerBuyBase >
      0
  )
    ? clamp(
        candle.takerBuyBase /
          candle.volume,
        0,
        1
      )
    : null;
}

function orderFlowMomentum(arr) {
  let buy = 0;
  let total = 0;
  let valid = 0;

  for (
    const c
    of arr.slice(-3)
  ) {
    if (
      c.volume >
        0 &&
      c.takerBuyBase >
        0
    ) {
      buy +=
        c.takerBuyBase;

      total +=
        c.volume;

      valid++;
    }
  }

  return (
    valid &&
    total >
      0
  )
    ? clamp(
        buy /
          total,
        0,
        1
      )
    : null;
}

// ============================================================
// BREAKOUT ACCEPTANCE
// ============================================================

function breakoutAcceptance(
  arr,
  resistance,
  atrPct
) {
  const current =
    arr.at(-1);

  const previous =
    arr.at(-2);

  let score = 0;

  const reasons = [];

  const range =
    Math.max(
      current.high -
        current.low,
      Number.EPSILON
    );

  const body =
    Math.abs(
      current.close -
        current.open
    ) /
    range;

  const closeLocation =
    (
      current.close -
      current.low
    ) /
    range;

  const breakoutPct =
    pct(
      current.close -
        resistance,
      resistance
    );

  const previousNear =
    Math.abs(
      pct(
        previous.close -
          resistance,
        resistance
      )
    ) <=
    C.retestTolerancePct;

  if (
    current.close >
    resistance
  ) {
    score += 25;

    reasons.push(
      'CLOSE_ABOVE_RES'
    );
  }

  if (
    closeLocation >=
    0.72
  ) {
    score += 15;

    reasons.push(
      'CLOSE_HIGH'
    );
  }

  if (
    body >=
    0.55
  ) {
    score += 15;

    reasons.push(
      'STRONG_BODY'
    );
  }

  if (
    breakoutPct >=
      0 &&
    breakoutPct <=
      Math.max(
        0.55,
        atrPct *
          1.4
      )
  ) {
    score += 15;

    reasons.push(
      'CONTROLLED_BREAKOUT'
    );
  }

  if (
    previousNear &&
    previous.low <=
      resistance *
      (
        1 +
        C.retestTolerancePct /
          100
      )
  ) {
    score += 20;

    reasons.push(
      'RETEST_ACCEPTED'
    );
  }

  if (
    previous.close >
      resistance &&
    current.close >
      previous.close
  ) {
    score += 10;

    reasons.push(
      'SECOND_CLOSE_CONFIRM'
    );
  }

  score =
    clamp(
      score,
      0,
      100
    );

  return {
    score,
    reasons,
    breakoutPct,
    accepted:
      score >=
      C.minBreakoutAcceptance
  };
}

// ============================================================
// MARKET REGIME
// ============================================================

function btcContext() {
  const arr =
    candles[
      C.btcSymbol
    ];

  if (
    !arr ||
    arr.length <
      C.warmupCandles
  ) {
    return {
      ready: false,
      bullish: false,
      score: 0,
      freshness: 0
    };
  }

  const current =
    arr.at(-1);

  const e20 =
    ema(
      arr,
      20
    );

  const e50 =
    ema(
      arr,
      50
    );

  const momentum =
    cmo(
      arr,
      9
    );

  const atrValue =
    atr(
      arr,
      14
    );

  const emaDistance =
    pct(
      current.close -
        e20,
      e20
    );

  const atrPct =
    pct(
      atrValue,
      current.close
    );

  const ext5 =
    pct(
      current.close -
        arr.at(-6).close,
      arr.at(-6).close
    );

  let score = 0;
  let freshness = 100;

  if (
    current.close >
    e20
  ) {
    score += 30;
  }

  if (
    e20 >
    e50
  ) {
    score += 30;
  }

  if (
    structure(arr) ===
    'BULLISH'
  ) {
    score += 20;
  }

  if (
    momentum >=
      45 &&
    momentum <=
      70
  ) {
    score += 20;
  }

  if (
    emaDistance >
    1.2
  ) {
    freshness -= 30;
  }

  if (
    ext5 >
    2.0
  ) {
    freshness -= 30;
  }

  if (
    momentum >
    72
  ) {
    freshness -= 25;
  }

  if (
    atrPct >
    C.maxAtrPct
  ) {
    freshness -= 15;
  }

  return {
    ready: true,

    bullish:
      current.close >
        e20 &&
      e20 >
        e50,

    score,

    freshness:
      clamp(
        freshness,
        0,
        100
      ),

    emaDistance,
    ext5,
    cmo: momentum
  };
}

function calculateMarketRegime() {
  let ready = 0;
  let bullish = 0;

  for (
    const symbol
    of subscribed
  ) {
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
      ema(
        arr,
        20
      );

    ready++;

    if (
      e20 &&
      arr.at(-1).close >
        e20
    ) {
      bullish++;
    }
  }

  const breadth =
    ready
      ? (
          bullish /
          ready
        ) *
        100
      : 0;

  const btc =
    btcContext();

  let regime =
    'DEFENSIVE';

  if (
    btc.bullish &&
    breadth >=
      55
  ) {
    regime =
      'RISK_ON';

  } else if (
    btc.ready &&
    breadth >=
      45
  ) {
    regime =
      'NEUTRAL';
  }

  const overextended =
    regime ===
      'RISK_ON' &&
    (
      breadth >=
        C.overextendedRiskOnBreadth ||
      btc.freshness <
        55
    );

  marketRegime = {
    ready:
      ready >=
      20,

    btcBullish:
      btc.bullish,

    btcScore:
      btc.score,

    btcFreshness:
      btc.freshness,

    breadth:
      +breadth.toFixed(2),

    regime,

    overextended,

    updatedAt:
      Date.now()
  };
}

// ============================================================
// V5.1 SCORE
// ============================================================

function buildV5Score(ctx) {
  const {
    arr,
    x,
    e20,
    e50,
    momentum,
    atrPct,
    volumeRatio,
    volumeAccel,
    takerRatio,
    flowMomentum,
    emaDistance,
    ext5,
    ext10,
    marketStructure,
    acceptance,
    bodyRatio,
    upperWickRatio,
    closeLocation
  } = ctx;

  let trend = 0;
  let freshness = 100;
  let flow = 0;
  let momentumScore = 0;
  let volatility = 0;
  let structureScore = 0;
  let regimeScore = 0;

  const reasons = [];
  const warnings = [];

  if (
    x.close >
    e20
  ) {
    trend += 35;
  }

  if (
    e20 >
    e50
  ) {
    trend += 35;
  }

  if (
    marketStructure ===
    'BULLISH'
  ) {
    trend += 20;
  }

  const oldEma =
    ema(
      arr.slice(
        0,
        -3
      ),
      20
    );

  if (
    oldEma &&
    pct(
      e20 -
        oldEma,
      oldEma
    ) >
      0.02
  ) {
    trend += 10;
  }

  trend =
    clamp(
      trend,
      0,
      100
    );

  if (
    emaDistance >
    C.maxEmaDistancePct
  ) {
    freshness -= 55;

    warnings.push(
      'EMA_CHASE'
    );

  } else if (
    emaDistance >
    1.0
  ) {
    freshness -= 18;
  }

  if (
    ext5 >
    C.maxExt5Pct
  ) {
    freshness -= 35;

    warnings.push(
      'EXT5_CHASE'
    );
  }

  if (
    ext10 >
    C.maxExt10Pct
  ) {
    freshness -= 30;

    warnings.push(
      'EXT10_CHASE'
    );
  }

  if (
    volumeAccel >
    C.volumeClimaxAcceleration
  ) {
    freshness -= 40;

    warnings.push(
      'VOLUME_CLIMAX'
    );
  }

  if (
    momentum >
    C.cmoCoreMax
  ) {
    freshness -= 30;

    warnings.push(
      'CMO_OVERHEAT'
    );
  }

  if (
    priceAcceleration(arr) >
    1.0
  ) {
    freshness -= 20;

    warnings.push(
      'PRICE_ACCEL_CHASE'
    );
  }

  freshness =
    clamp(
      freshness,
      0,
      100
    );

  if (
    takerRatio !==
    null
  ) {
    if (
      takerRatio >=
      C.eliteTakerBuyRatio
    ) {
      flow += 70;

      reasons.push(
        'ELITE_TAKER_FLOW'
      );

    } else if (
      takerRatio >=
      C.minTakerBuyRatio
    ) {
      flow += 42;
    }

    if (
      takerRatio >=
        0.70 &&
      takerRatio <
        0.80
    ) {
      flow -= 15;

      warnings.push(
        'MID_HIGH_FLOW_RISK'
      );
    }
  }

  if (
    flowMomentum !==
    null
  ) {
    if (
      flowMomentum >=
      0.72
    ) {
      flow += 30;

    } else if (
      flowMomentum >=
      C.minFlowMomentum
    ) {
      flow += 18;

    } else {

      flow -= 20;
    }
  }

  flow =
    clamp(
      flow,
      0,
      100
    );

  if (
    momentum >=
      C.cmoCoreMin &&
    momentum <=
      C.cmoCoreMax
  ) {
    momentumScore =
      100;

    reasons.push(
      'CMO_CORE'
    );

  } else if (
    momentum >=
      C.cmoSoftMin &&
    momentum <=
      C.cmoSoftMax
  ) {
    momentumScore =
      68;

  } else if (
    momentum >
    68
  ) {
    momentumScore =
      25;

  } else {

    momentumScore =
      20;
  }

  if (
    atrPct >=
      C.atrSweetMin &&
    atrPct <=
      C.atrSweetMax
  ) {
    volatility += 45;

  } else if (
    atrPct >=
      C.minAtrPct &&
    atrPct <=
      C.maxAtrPct
  ) {
    volatility += 28;

  } else {

    warnings.push(
      'ATR_OUTSIDE_EDGE'
    );
  }

  if (
    volumeRatio >=
      1.05 &&
    volumeRatio <=
      1.80
  ) {
    volatility += 30;

  } else if (
    volumeRatio <=
    C.maxVolumeRatio
  ) {
    volatility += 18;
  }

  if (
    volumeAccel >=
      C.volumeAccelSweetMin &&
    volumeAccel <=
      C.volumeAccelSweetMax
  ) {
    volatility += 25;

  } else if (
    volumeAccel >
    C.volumeClimaxAcceleration
  ) {
    volatility -= 25;
  }

  volatility =
    clamp(
      volatility,
      0,
      100
    );

  structureScore =
    acceptance.score *
    0.65;

  if (
    bodyRatio >=
    0.55
  ) {
    structureScore += 15;
  }

  if (
    upperWickRatio <=
    0.30
  ) {
    structureScore += 10;
  }

  if (
    closeLocation >=
    0.72
  ) {
    structureScore += 10;
  }

  structureScore =
    clamp(
      structureScore,
      0,
      100
    );

  if (
    marketRegime.regime ===
    'RISK_ON'
  ) {
    regimeScore =
      85;

  } else if (
    marketRegime.regime ===
    'NEUTRAL'
  ) {
    regimeScore =
      70;

  } else {

    regimeScore =
      35;
  }

  if (
    marketRegime.overextended
  ) {
    regimeScore -= 30;

    warnings.push(
      'RISK_ON_OVEREXTENDED'
    );
  }

  if (
    sessionUTC() ===
      'ASIA' &&
    marketRegime.regime ===
      'RISK_ON'
  ) {
    regimeScore -=
      C.asiaRiskOnPenalty;

    warnings.push(
      'ASIA_RISK_ON_PENALTY'
    );
  }

  regimeScore =
    clamp(
      regimeScore,
      0,
      100
    );

  const edgeScore =
    Math.round(
      trend *
        0.15 +
      freshness *
        0.22 +
      flow *
        0.20 +
      momentumScore *
        0.15 +
      volatility *
        0.10 +
      structureScore *
        0.10 +
      regimeScore *
        0.08
    );

  const shadowEdge =
    momentum >=
      50 &&
    momentum <=
      65 &&
    takerRatio !==
      null &&
    takerRatio >=
      0.80 &&
    atrPct >=
      0.18;

  return {
    edgeScore,

    trendScore:
      Math.round(trend),

    freshnessScore:
      Math.round(freshness),

    flowScore:
      Math.round(flow),

    momentumScore:
      Math.round(momentumScore),

    volatilityScore:
      Math.round(volatility),

    structureScore:
      Math.round(structureScore),

    regimeScore:
      Math.round(regimeScore),

    shadowEdge,

    reasons,

    warnings
  };
}

function analyze(
  arr,
  symbol
) {
  if (
    !arr ||
    arr.length <
      C.warmupCandles
  ) {
    return null;
  }

  const current =
    arr.at(-1);

  const e20 =
    ema(
      arr,
      20
    );

  const e50 =
    ema(
      arr,
      50
    );

  const momentum =
    cmo(
      arr,
      9
    );

  const atrValue =
    atr(
      arr,
      14
    );

  const volumeSma =
    sma(
      arr,
      10,
      'volume'
    );

  if (
    [
      e20,
      e50,
      momentum,
      atrValue,
      volumeSma
    ].some(
      v =>
        v ===
        null
    )
  ) {
    return null;
  }

  const range =
    current.high -
    current.low;

  if (
    range <=
    0
  ) {
    return null;
  }

  const bodyRatio =
    Math.abs(
      current.close -
      current.open
    ) /
    range;

  const upperWickRatio =
    (
      current.high -
      Math.max(
        current.open,
        current.close
      )
    ) /
    range;

  const closeLocation =
    (
      current.close -
      current.low
    ) /
    range;

  const bullish =
    current.close >
    current.open;

  const volumeRatio =
    volumeSma >
      0
      ? current.volume /
        volumeSma
      : 0;

  const volumeAccel =
    volumeAcceleration(arr);

  const priceAccel =
    priceAcceleration(arr);

  const takerRatio =
    takerBuyRatio(current);

  const flowMomentum =
    orderFlowMomentum(arr);
    const atrPct =
    pct(
      atrValue,
      current.close
    );

  const emaDistance =
    pct(
      current.close -
        e20,
      e20
    );

  const ext5Base =
    arr.at(-6)?.close ||
    current.close;

  const ext10Base =
    arr.at(-11)?.close ||
    current.close;

  const ext5 =
    pct(
      current.close -
        ext5Base,
      ext5Base
    );

  const ext10 =
    pct(
      current.close -
        ext10Base,
      ext10Base
    );

  const marketStructure =
    structure(arr);

  const prior =
    arr.slice(
      0,
      -1
    );

  const resistance =
    prior
      .slice(-20)
      .reduce(
        (
          max,
          candle
        ) =>
          Math.max(
            max,
            candle.high
          ),
        0
      );

  const acceptance =
    breakoutAcceptance(
      arr,
      resistance,
      atrPct
    );

  const score =
    buildV5Score({
      arr,
      x: current,
      e20,
      e50,
      momentum,
      atrPct,
      volumeRatio,
      volumeAccel,
      takerRatio,
      flowMomentum,
      emaDistance,
      ext5,
      ext10,
      marketStructure,
      acceptance,
      bodyRatio,
      upperWickRatio,
      closeLocation
    });

  let threshold =
    C.minEdgeScoreDefensive;

  if (
    marketRegime.regime ===
    'RISK_ON'
  ) {
    threshold =
      C.minEdgeScoreRiskOn;

  } else if (
    marketRegime.regime ===
    'NEUTRAL'
  ) {
    threshold =
      C.minEdgeScoreNeutral;
  }

  const hardChecks = {
    bullish,

    trend:
      current.close >
        e20 &&
      e20 >
        e50,

    cmo:
      momentum >=
        C.cmoSoftMin &&
      momentum <=
        C.cmoSoftMax,

    atr:
      atrPct >=
        C.minAtrPct &&
      atrPct <=
        C.maxAtrPct,

    volume:
      volumeRatio >=
        C.minVolumeRatio &&
      volumeRatio <=
        C.maxVolumeRatio,

    noClimax:
      volumeAccel <=
        C.volumeClimaxAcceleration,

    noEmaChase:
      emaDistance <=
        C.maxEmaDistancePct,

    noExt5Chase:
      ext5 <=
        C.maxExt5Pct,

    noExt10Chase:
      ext10 <=
        C.maxExt10Pct,

    freshness:
      score.freshnessScore >=
        C.minFreshnessScore,

    flow:
      score.flowScore >=
        C.minFlowScore,

    breakout:
      acceptance.accepted,

    score:
      score.edgeScore >=
        threshold
  };

  const eligible =
    Object.values(
      hardChecks
    ).every(Boolean);

  return {
    symbol,

    closeTime:
      current.closeTime,

    price:
      current.close,

    quoteVolume:
      tickers.get(symbol)
        ?.quoteVolume ||
      current.quoteVolume ||
      0,

    e20,
    e50,

    momentum,

    atrPct,

    volumeRatio,

    volumeAccel,

    priceAccel,

    takerRatio,

    flowMomentum,

    emaDistance,

    ext5,

    ext10,

    marketStructure,

    resistance,

    breakoutPct:
      acceptance.breakoutPct,

    breakoutAcceptance:
      acceptance.score,

    breakoutReasons:
      acceptance.reasons,

    bodyRatio,

    upperWickRatio,

    closeLocation,

    threshold,

    eligible,

    hardChecks,

    ...score,

    session:
      sessionUTC(),

    regime:
      marketRegime.regime,

    breadth:
      marketRegime.breadth,

    btcBullish:
      marketRegime.btcBullish,

    createdAt:
      Date.now()
  };
}

// ============================================================
// ANALYSIS JOURNAL
// ============================================================

function compactAnalysisRow(
  result
) {
  return {
    type:
      'ANALYSIS',

    symbol:
      result.symbol,

    eligible:
      result.eligible,

    edgeScore:
      result.edgeScore,

    threshold:
      result.threshold,

    cmo:
      +n(
        result.momentum
      ).toFixed(2),

    atrPct:
      +n(
        result.atrPct
      ).toFixed(4),

    volumeRatio:
      +n(
        result.volumeRatio
      ).toFixed(3),

    volumeAccel:
      +n(
        result.volumeAccel
      ).toFixed(3),

    takerBuy:
      result.takerRatio ===
      null
        ? null
        : +result.takerRatio.toFixed(
            4
          ),

    flowMomentum:
      result.flowMomentum ===
      null
        ? null
        : +result.flowMomentum.toFixed(
            4
          ),

    emaDistance:
      +n(
        result.emaDistance
      ).toFixed(4),

    ext5:
      +n(
        result.ext5
      ).toFixed(4),

    breakoutAcceptance:
      result.breakoutAcceptance,

    freshnessScore:
      result.freshnessScore,

    flowScore:
      result.flowScore,

    shadowEdge:
      result.shadowEdge,

    regime:
      result.regime,

    session:
      result.session
  };
}

function journalAnalysis(
  result
) {
  if (
    result.eligible ||
    result.shadowEdge
  ) {
    cloudJournal(
      compactAnalysisRow(
        result
      )
    );

    return;
  }

  if (
    Math.random() <
    C.analysisJournalSampleRate
  ) {
    cloudJournal(
      compactAnalysisRow(
        result
      )
    );

  } else {

    networkMeter
      .skippedAnalysisLogs++;
  }
}

// ============================================================
// CANDIDATE POOL
// ============================================================

function candidateRank(
  x
) {
  let score =
    x.edgeScore;

  score +=
    x.freshnessScore *
    0.08;

  score +=
    x.flowScore *
    0.10;

  score +=
    x.breakoutAcceptance *
    0.06;

  if (
    x.shadowEdge
  ) {
    score +=
      7;
  }

  if (
    x.takerRatio !==
      null &&
    x.takerRatio >=
      C.eliteTakerBuyRatio
  ) {
    score +=
      4;
  }

  if (
    x.momentum >=
      C.cmoCoreMin &&
    x.momentum <=
      C.cmoCoreMax
  ) {
    score +=
      4;
  }

  return score;
}

function addCandidate(
  result
) {
  if (
    !result ||
    !result.eligible
  ) {
    return;
  }

  if (
    positions[
      result.symbol
    ]
  ) {
    return;
  }

  if (
    symbolCooling(
      result.symbol
    )
  ) {
    return;
  }

  const candidate = {
    ...result,

    rank:
      candidateRank(
        result
      ),

    candidateAt:
      Date.now(),

    referencePrice:
      result.price
  };

  candidatePool.set(
    result.symbol,
    candidate
  );

  cloudJournal({
    type:
      'CANDIDATE',

    symbol:
      result.symbol,

    price:
      result.price,

    edgeScore:
      result.edgeScore,

    rank:
      candidate.rank,

    cmo:
      result.momentum,

    atrPct:
      result.atrPct,

    takerBuy:
      result.takerRatio,

    flowMomentum:
      result.flowMomentum,

    freshnessScore:
      result.freshnessScore,

    breakoutAcceptance:
      result.breakoutAcceptance,

    shadowEdge:
      result.shadowEdge,

    regime:
      result.regime,

    session:
      result.session
  });

  scheduleRanking();
}

function purgeCandidates() {
  const now =
    Date.now();

  for (
    const [
      symbol,
      candidate
    ]
    of candidatePool
  ) {
    if (
      now -
        candidate.candidateAt >
      C.candidateExpiryMs
    ) {
      candidatePool.delete(
        symbol
      );
    }
  }
}

function scheduleRanking() {
  if (
    rankTimer
  ) {
    return;
  }

  rankTimer =
    setTimeout(
      async () => {
        rankTimer =
          null;

        await processCandidates();

      },
      1500
    );
}

// ============================================================
// POSITION SIZE
// ============================================================

function positionAllocation() {
  const open =
    Object.keys(
      positions
    ).length;

  const slots =
    Math.max(
      1,
      C.maxPositions -
        open
    );

  const available =
    Math.max(
      0,
      cash
    );

  const base =
    available /
    Math.max(
      slots,
      1
    );

  const cap =
    equity() /
    C.maxPositions;

  return Math.min(
    base,
    cap
  );
}

// ============================================================
// ENTRY
// ============================================================

async function openPaperTrade(
  candidate,
  livePrice
) {
  if (
    !C.paperTrading
  ) {
    return false;
  }

  if (
    positions[
      candidate.symbol
    ]
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

  const allocation =
    positionAllocation();

  if (
    allocation <
    10
  ) {
    return false;
  }

  const entryPrice =
    livePrice *
    (
      1 +
      C.slippagePct
    );

  const stopPct =
    clamp(
      (
        candidate.atrPct /
        100
      ) *
      C.atrStopMultiplier,

      C.minStopPct,

      C.maxStopPct
    );

  const riskPerUnit =
    entryPrice *
    stopPct;

  const stopLoss =
    entryPrice -
    riskPerUnit;

  const takeProfit =
    entryPrice +
    riskPerUnit *
      C.rewardRisk;

  const entryFee =
    allocation *
    C.feePct;

  const spend =
    allocation -
    entryFee;

  if (
    spend <=
    0 ||
    cash <
      allocation
  ) {
    return false;
  }

  const qty =
    spend /
    entryPrice;

  cash -=
    allocation;

  const id =
    `${candidate.symbol}-${Date.now()}`;

  positions[
    candidate.symbol
  ] = {
    id,

    symbol:
      candidate.symbol,

    entryTime:
      Date.now(),

    entryPrice,

    qty,

    allocation,

    entryFee,

    stopLoss,

    takeProfit,

    initialStop:
      stopLoss,

    initialRisk:
      entryPrice -
      stopLoss,

    lastPrice:
      entryPrice,

    highestPrice:
      entryPrice,

    mfePct:
      0,

    maePct:
      0,

    breakEven:
      false,

    trailing:
      false,

    profitLocked:
      false,

    entrySnapshot: {
      edgeScore:
        candidate.edgeScore,

      rank:
        candidate.rank,

      cmo:
        candidate.momentum,

      atrPct:
        candidate.atrPct,

      volumeRatio:
        candidate.volumeRatio,

      volumeAccel:
        candidate.volumeAccel,

      takerBuy:
        candidate.takerRatio,

      flowMomentum:
        candidate.flowMomentum,

      emaDistance:
        candidate.emaDistance,

      ext5:
        candidate.ext5,

      ext10:
        candidate.ext10,

      breakoutAcceptance:
        candidate.breakoutAcceptance,

      freshnessScore:
        candidate.freshnessScore,

      flowScore:
        candidate.flowScore,

      shadowEdge:
        candidate.shadowEdge,

      regime:
        candidate.regime,

      breadth:
        candidate.breadth,

      btcBullish:
        candidate.btcBullish,

      session:
        candidate.session
    }
  };

  entriesSinceCooldown++;

  await cloudJournal({
    type:
      'ENTRY',

    id,

    symbol:
      candidate.symbol,

    entryPrice,

    allocation,

    qty,

    stopLoss,

    takeProfit,

    ...positions[
      candidate.symbol
    ].entrySnapshot
  });

  tg(
    `🟢 <b>LOMY V5.1 PAPER BUY</b>
${candidate.symbol}
Entry: ${entryPrice.toFixed(8)}
Score: ${candidate.edgeScore}
Fresh: ${candidate.freshnessScore}
Flow: ${candidate.flowScore}
CMO: ${candidate.momentum.toFixed(1)}
ATR: ${candidate.atrPct.toFixed(3)}%
Taker: ${
      candidate.takerRatio ===
      null
        ? 'N/A'
        : (
            candidate.takerRatio *
            100
          ).toFixed(1) +
          '%'
    }
Breakout: ${candidate.breakoutAcceptance}
Regime: ${candidate.regime}
SL: ${stopLoss.toFixed(8)}
TP: ${takeProfit.toFixed(8)}`
  );

  await saveCloudState();

  return true;
}

// ============================================================
// RANK / REVALIDATE / EXECUTE
// ============================================================

async function processCandidates() {
  checkDay();

  purgeCandidates();

  if (
    manualPause ||
    dailyPause ||
    cooldownActive()
  ) {
    return;
  }

  if (
    !marketRegime.ready
  ) {
    return;
  }

  const openCount =
    Object.keys(
      positions
    ).length;

  if (
    openCount >=
    C.maxPositions
  ) {
    return;
  }

  const sorted =
    [...candidatePool.values()]
      .sort(
        (
          a,
          b
        ) =>
          b.rank -
          a.rank
      );

  let opened =
    0;

  for (
    const candidate
    of sorted
  ) {
    if (
      opened >=
      C.maxEntriesPerCycle
    ) {
      break;
    }

    if (
      Object.keys(
        positions
      ).length >=
      C.maxPositions
    ) {
      break;
    }

    const symbol =
      candidate.symbol;

    if (
      positions[
        symbol
      ] ||
      symbolCooling(
        symbol
      )
    ) {
      candidatePool.delete(
        symbol
      );

      continue;
    }

    const ticker =
      tickers.get(
        symbol
      );

    if (
      !ticker ||
      !ticker.price
    ) {
      continue;
    }

    const livePrice =
      ticker.price;

    const drift =
      Math.abs(
        pct(
          livePrice -
            candidate.referencePrice,
          candidate.referencePrice
        )
      );

    const chase =
      pct(
        livePrice -
          candidate.referencePrice,
        candidate.referencePrice
      );

    if (
      drift >
      C.maxPriceDriftPct
    ) {
      candidatePool.delete(
        symbol
      );

      await cloudJournal({
        type:
          'RANK_REJECT',

        symbol,

        reason:
          'PRICE_DRIFT',

        drift,

        edgeScore:
          candidate.edgeScore
      });

      continue;
    }

    if (
      chase >
      C.liveChasePct
    ) {
      candidatePool.delete(
        symbol
      );

      await cloudJournal({
        type:
          'RANK_REJECT',

        symbol,

        reason:
          'LIVE_CHASE',

        chase,

        edgeScore:
          candidate.edgeScore
      });

      continue;
    }

    const arr =
      candles[
        symbol
      ];

    if (
      !arr ||
      arr.length <
        C.warmupCandles
    ) {
      continue;
    }

    const fresh =
      analyze(
        arr,
        symbol
      );

    if (
      !fresh ||
      !fresh.eligible
    ) {
      candidatePool.delete(
        symbol
      );

      continue;
    }

    const success =
      await openPaperTrade(
        {
          ...fresh,

          rank:
            candidateRank(
              fresh
            )
        },

        livePrice
      );

    candidatePool.delete(
      symbol
    );

    if (
      success
    ) {
      opened++;
    }
  }

  if (
    entriesSinceCooldown >=
    C.entriesBeforeCooldown
  ) {
    startCooldown(
      C.batchCooldownMs,
      'ENTRY_BATCH_LIMIT'
    );
  }
}

// ============================================================
// POSITION MANAGEMENT
// ============================================================

function positionR(
  position,
  price
) {
  if (
    !position.initialRisk
  ) {
    return 0;
  }

  return (
    price -
    position.entryPrice
  ) /
  position.initialRisk;
}

async function closePaperTrade(
  symbol,
  exitPriceRaw,
  reason
) {
  const p =
    positions[
      symbol
    ];

  if (
    !p
  ) {
    return;
  }

  const exitPrice =
    exitPriceRaw *
    (
      1 -
      C.slippagePct
    );

  const grossValue =
    p.qty *
    exitPrice;

  const exitFee =
    grossValue *
    C.feePct;

  const netValue =
    grossValue -
    exitFee;

  cash +=
    netValue;

  const totalFees =
    p.entryFee +
    exitFee;

  const grossPnL =
    (
      exitPrice -
      p.entryPrice
    ) *
    p.qty;

  const netPnL =
    netValue -
    p.allocation;

  const pnlPct =
    p.allocation
      ? (
          netPnL /
          p.allocation
        ) *
        100
      : 0;

  stats.totalTrades++;

  stats.fees +=
    totalFees;

  stats.netProfit +=
    netPnL;

  stats.bestTrade =
    stats.totalTrades ===
      1
      ? netPnL
      : Math.max(
          stats.bestTrade,
          netPnL
        );

  stats.worstTrade =
    stats.totalTrades ===
      1
      ? netPnL
      : Math.min(
          stats.worstTrade,
          netPnL
        );

  if (
    netPnL >
    0
  ) {
    stats.wins++;

    stats.grossProfit +=
      netPnL;

    lossStreak =
      0;

  } else {

    stats.losses++;

    stats.grossLoss +=
      Math.abs(
        netPnL
      );

    lossStreak++;

    lastLossBySymbol[
      symbol
    ] =
      Date.now();
  }

  if (
    reason ===
    'EARLY_FAILURE'
  ) {
    stats.earlyFailureExits++;
  }

  dailyPnL +=
    netPnL;

  const record = {
    id:
      p.id,

    symbol,

    entryTime:
      p.entryTime,

    exitTime:
      Date.now(),

    holdMinutes:
      (
        Date.now() -
        p.entryTime
      ) /
      60000,

    entryPrice:
      p.entryPrice,

    exitPrice,

    qty:
      p.qty,

    allocation:
      p.allocation,

    grossPnL,

    netPnL,

    pnlPct,

    totalFees,

    reason,

    mfePct:
      p.mfePct,

    maePct:
      p.maePct,

    breakEven:
      p.breakEven,

    trailing:
      p.trailing,

    profitLocked:
      p.profitLocked,

    ...p.entrySnapshot
  };

  delete positions[
    symbol
  ];

  await saveTrade(
    record
  );

  await cloudJournal({
    type:
      'CLOSE',

    ...record
  });

  updateDrawdown();

  checkDailyLoss();

  if (
    lossStreak >=
    C.lossStreakLimit
  ) {
    startCooldown(
      C.lossCooldownMs,
      `LOSS_STREAK_${lossStreak}`
    );
  }

  tg(
    `${
      netPnL >=
      0
        ? '✅'
        : '🔴'
    } <b>LOMY V5.1 PAPER CLOSE</b>
${symbol}
Reason: ${reason}
PnL: $${netPnL.toFixed(2)} (${pnlPct.toFixed(2)}%)
MFE: ${p.mfePct.toFixed(2)}%
MAE: ${p.maePct.toFixed(2)}%
Cash: $${cash.toFixed(2)}`
  );

  await saveCloudState();
}

async function managePosition(
  symbol,
  price
) {
  const p =
    positions[
      symbol
    ];

  if (
    !p ||
    !price
  ) {
    return;
  }

  p.lastPrice =
    price;

  p.highestPrice =
    Math.max(
      p.highestPrice ||
        p.entryPrice,
      price
    );

  const movePct =
    pct(
      price -
        p.entryPrice,
      p.entryPrice
    );

  p.mfePct =
    Math.max(
      p.mfePct || 0,
      movePct
    );

  p.maePct =
    Math.min(
      p.maePct || 0,
      movePct
    );

  const r =
    positionR(
      p,
      price
    );

  if (
    !p.profitLocked &&
    p.mfePct >=
      C.profitLockMfePct
  ) {
    const locked =
      p.entryPrice *
      (
        1 +
        C.profitLockBufferPct
      );

    if (
      locked >
      p.stopLoss
    ) {
      p.stopLoss =
        locked;

      p.profitLocked =
        true;

      stats.profitLocks++;

      await cloudJournal({
        type:
          'PROFIT_LOCK',

        symbol,

        price,

        newStop:
          p.stopLoss,

        mfePct:
          p.mfePct
      });
    }
  }

  if (
    !p.breakEven &&
    r >=
      C.breakEvenAtR
  ) {
    const be =
      p.entryPrice *
      (
        1 +
        C.breakEvenBufferPct
      );

    if (
      be >
      p.stopLoss
    ) {
      p.stopLoss =
        be;

      p.breakEven =
        true;

      stats.breakEvenMoves++;

      await cloudJournal({
        type:
          'BREAK_EVEN',

        symbol,

        price,

        newStop:
          p.stopLoss,

        r
      });
    }
  }

  if (
    r >=
    C.trailAtR
  ) {
    const trail =
      p.entryPrice +
      p.initialRisk *
      C.trailLockR;

    if (
      trail >
      p.stopLoss
    ) {
      p.stopLoss =
        trail;

      if (
        !p.trailing
      ) {
        p.trailing =
          true;

        stats.trailingActivations++;
      }

      await cloudJournal({
        type:
          'TRAIL',

        symbol,

        price,

        newStop:
          p.stopLoss,

        r
      });
    }
  }

  const age =
    Date.now() -
    p.entryTime;

  if (
    age <=
      C.earlyFailureWindowMs &&
    movePct <=
      -C.earlyFailureLossPct *
        100 &&
    p.mfePct <
      C.earlyFailureMfeGuardPct
  ) {
    await closePaperTrade(
      symbol,
      price,
      'EARLY_FAILURE'
    );

    return;
  }

  if (
    price <=
    p.stopLoss
  ) {
    let reason =
      'STOP_LOSS';

    if (
      p.trailing
    ) {
      reason =
        'TRAIL_STOP';

    } else if (
      p.breakEven ||
      p.profitLocked
    ) {
      reason =
        'BREAK_EVEN_STOP';
    }

    await closePaperTrade(
      symbol,
      price,
      reason
    );

    return;
  }

  if (
    price >=
    p.takeProfit
  ) {
    await closePaperTrade(
      symbol,
      price,
      'TAKE_PROFIT'
    );
  }
}

// ============================================================
// CLOSED CANDLE PROCESSING
// ============================================================

async function processClosedCandle(
  symbol,
  candle
) {
  if (
    !subscribed.has(
      symbol
    )
  ) {
    return;
  }

  mergeCandles(
    symbol,
    [candle]
  );

  if (
    (
      candles[
        symbol
      ]?.length ||
      0
    ) <
    C.warmupCandles
  ) {
    queueWarmup(
      symbol
    );

    return;
  }

  warmupLoaded.add(
    symbol
  );

  if (
    lastAnalyzed[
      symbol
    ] ===
    candle.closeTime
  ) {
    return;
  }

  lastAnalyzed[
    symbol
  ] =
    candle.closeTime;

  if (
    symbol ===
    C.btcSymbol
  ) {
    calculateMarketRegime();
  }

  const result =
    analyze(
      candles[
        symbol
      ],
      symbol
    );

  if (
    !result
  ) {
    return;
  }

  journalAnalysis(
    result
  );

  if (
    result.eligible
  ) {
    addCandidate(
      result
    );
  }
}

// ============================================================
// WEBSOCKET HELPERS
// ============================================================

function safeCloseWs(ws) {
  if (
    !ws
  ) {
    return;
  }

  try {
    ws.removeAllListeners();
  } catch {}

  try {
    ws.terminate();
  } catch {}
}

function miniStreamUrl() {
  return `${WS_BASE}/ws/!miniTicker@arr`;
}

function klineStreamUrl(
  symbols
) {
  const streams =
    [...symbols]
      .map(
        symbol =>
          `${symbol.toLowerCase()}@kline_${C.interval}`
      )
      .join('/');

  return `${WS_BASE}/stream?streams=${streams}`;
}

// ============================================================
// MINI TICKER
// ============================================================

function connectMiniTicker() {
  if (
    shuttingDown
  ) {
    return;
  }

  safeCloseWs(
    miniWs
  );

  miniWs =
    new WebSocket(
      miniStreamUrl()
    );

  miniWs.on(
    'open',
    () => {
      miniConnected =
        true;

      console.log(
        'MiniTicker WebSocket LIVE'
      );
    }
  );

  miniWs.on(
    'message',
    async raw => {
      lastMiniMessage =
        Date.now();

      let rows;

      try {
        rows =
          JSON.parse(
            raw.toString()
          );

      } catch {

        return;
      }

      if (
        !Array.isArray(
          rows
        )
      ) {
        return;
      }

      for (
        const row
        of rows
      ) {
        const symbol =
          row.s;

        if (
          !symbol ||
          !symbol.endsWith(
            'USDT'
          ) ||
          ignored(
            symbol
          )
        ) {
          continue;
        }

        const price =
          n(row.c);

        const quoteVolume =
          n(row.q);

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
          positions[
            symbol
          ]
        ) {
          managePosition(
            symbol,
            price
          ).catch(
            error =>
              console.error(
                'Manage position:',
                error.message
              )
          );
        }
      }
    }
  );

  miniWs.on(
    'close',
    () => {
      miniConnected =
        false;

      if (
        !shuttingDown
      ) {
        setTimeout(
          connectMiniTicker,
          5000
        );
      }
    }
  );

  miniWs.on(
    'error',
    error => {
      miniConnected =
        false;

      console.error(
        'MiniTicker WS:',
        error.message
      );
    }
  );
}

// ============================================================
// KLINE WEBSOCKET
// ============================================================

function connectKlines() {
  if (
    shuttingDown ||
    !subscribed.size
  ) {
    return;
  }

  safeCloseWs(
    klineWs
  );

  const url =
    klineStreamUrl(
      subscribed
    );

  networkMeter.wsControlBytes +=
    byteLen(url);

  klineWs =
    new WebSocket(
      url
    );

  klineWs.on(
    'open',
    () => {
      klineConnected =
        true;

      console.log(
        `Kline WebSocket LIVE | ${subscribed.size} symbols`
      );
    }
  );

  klineWs.on(
    'message',
    raw => {
      lastKlineMessage =
        Date.now();

      let packet;

      try {
        packet =
          JSON.parse(
            raw.toString()
          );

      } catch {

        return;
      }

      const data =
        packet.data ||
        packet;

      const k =
        data.k;

      if (
        !k ||
        !k.s
      ) {
        return;
      }

      if (
        !k.x
      ) {
        return;
      }

      const candle = {
        open:
          n(k.o),

        high:
          n(k.h),

        low:
          n(k.l),

        close:
          n(k.c),

        volume:
          n(k.v),

        closeTime:
          n(k.T),

        quoteVolume:
          n(k.q),

        trades:
          n(k.n),

        takerBuyBase:
          n(k.V),

        takerBuyQuote:
          n(k.Q)
      };

      processClosedCandle(
        k.s,
        candle
      ).catch(
        error =>
          console.error(
            'Closed candle:',
            error.message
          )
      );
    }
  );

  klineWs.on(
    'close',
    () => {
      klineConnected =
        false;

      if (
        !shuttingDown
      ) {
        setTimeout(
          connectKlines,
          5000
        );
      }
    }
  );

  klineWs.on(
    'error',
    error => {
      klineConnected =
        false;

      console.error(
        'Kline WS:',
        error.message
      );
    }
  );
}

// ============================================================
// UNIVERSE
// ============================================================

function buildUniverse() {
  const ranked =
    [...tickers.entries()]
      .filter(
        (
          [
            symbol,
            ticker
          ]
        ) =>
          symbol.endsWith(
            'USDT'
          ) &&
          !ignored(
            symbol
          ) &&
          ticker.price >
            0 &&
          ticker.quoteVolume >=
            C.minQuoteVolume
      )
      .sort(
        (
          a,
          b
        ) =>
          b[1]
            .quoteVolume -
          a[1]
            .quoteVolume
      )
      .slice(
        0,
        C.universeSize
      )
      .map(
        (
          [
            symbol
          ]
        ) =>
          symbol
      );

  if (
    !ranked.includes(
      C.btcSymbol
    )
  ) {
    ranked.unshift(
      C.btcSymbol
    );
  }

  return new Set(
    ranked.slice(
      0,
      C.universeSize
    )
  );
}

function sameSet(
  a,
  b
) {
  if (
    a.size !==
    b.size
  ) {
    return false;
  }

  for (
    const x
    of a
  ) {
    if (
      !b.has(x)
    ) {
      return false;
    }
  }

  return true;
}

async function refreshUniverse(
  force = false
) {
  if (
    tickers.size <
    50
  ) {
    return;
  }

  const next =
    buildUniverse();

  if (
    !next.size
  ) {
    return;
  }

  const changed =
    !sameSet(
      next,
      subscribed
    );

  if (
    !force &&
    !changed
  ) {
    return;
  }

  subscribed =
    next;

  console.log(
    `Universe refreshed: ${subscribed.size} symbols`
  );

  for (
    const symbol
    of subscribed
  ) {
    if (
      (
        candles[
          symbol
        ]?.length ||
        0
      ) <
      C.warmupCandles
    ) {
      queueWarmup(
        symbol
      );
    }
  }

  connectKlines();

  calculateMarketRegime();
}

// ============================================================
// WATCHDOG
// ============================================================

setInterval(
  () => {
    if (
      shuttingDown
    ) {
      return;
    }

    const now =
      Date.now();

    if (
      !miniConnected ||
      (
        lastMiniMessage &&
        now -
          lastMiniMessage >
          60000
      )
    ) {
      console.log(
        'MiniTicker watchdog reconnect'
      );

      connectMiniTicker();
    }

    if (
      subscribed.size &&
      (
        !klineConnected ||
        (
          lastKlineMessage &&
          now -
            lastKlineMessage >
            12 *
            60 *
            1000
        )
      )
    ) {
      console.log(
        'Kline watchdog reconnect'
      );

      connectKlines();
    }
  },
  60000
);

// ============================================================
// PERIODIC JOBS
// ============================================================

setInterval(
  () => {
    checkDay();

    updateDrawdown();

    checkDailyLoss();

    purgeCandidates();
  },
  15000
);

setInterval(
  () => {
    calculateMarketRegime();
  },
  C.regimeRefreshMs
);

setInterval(
  () => {
    refreshUniverse(
      false
    ).catch(
      error =>
        console.error(
          'Universe refresh:',
          error.message
        )
    );
  },
  C.universeRefreshMs
);

setInterval(
  () => {
    saveCloudState();
  },
  C.stateSaveMs
);

// ============================================================
// NETWORK REPORT
// ============================================================

setInterval(
  () => {
    const net =
      networkSnapshot();

    console.log(
      `NETWORK ESTIMATE | Total ${net.estimatedOutboundMB} MB | Mongo ${net.mongoWriteMB} MB | Telegram ${net.telegramMB} MB | REST ${net.restRequestMB} MB | Journal writes ${net.journalWrites}`
    );

    const bytes =
      networkTotalBytes();

    const alertBytes =
      C.networkAlertMB *
      1024 *
      1024;

    if (
      bytes -
      networkMeter.lastAlertBytes >=
      alertBytes
    ) {
      networkMeter.lastAlertBytes =
        bytes;

      tg(
        `📡 <b>LOMY NETWORK GUARD</b>
Estimated app outbound: ${networkMB(bytes)} MB
Mongo: ${networkMB(networkMeter.mongoWriteBytes)} MB
Telegram: ${networkMB(networkMeter.telegramBytes)} MB
REST requests: ${networkMB(networkMeter.restRequestBytes)} MB
Candle cloud writes: ${networkMeter.candleCloudWrites}`
      );
    }
  },
  C.networkReportMs
);
// ============================================================
// DASHBOARD DATA
// ============================================================

function positionRows() {
  return Object.values(
    positions
  )
    .map(
      p => {
        const live =
          tickers.get(
            p.symbol
          )?.price ||
          p.lastPrice ||
          p.entryPrice;

        const currentValue =
          p.qty *
          live;

        const pnl =
          currentValue -
          p.allocation;

        const pnlPct =
          p.allocation
            ? (
                pnl /
                p.allocation
              ) *
              100
            : 0;

        return {
          symbol:
            p.symbol,

          entryPrice:
            p.entryPrice,

          livePrice:
            live,

          stopLoss:
            p.stopLoss,

          takeProfit:
            p.takeProfit,

          allocation:
            p.allocation,

          pnl,

          pnlPct,

          mfePct:
            p.mfePct,

          maePct:
            p.maePct,

          breakEven:
            p.breakEven,

          trailing:
            p.trailing,

          profitLocked:
            p.profitLocked,

          ageMinutes:
            (
              Date.now() -
              p.entryTime
            ) /
            60000,

          snapshot:
            p.entrySnapshot
        };
      }
    )
    .sort(
      (
        a,
        b
      ) =>
        b.pnlPct -
        a.pnlPct
    );
}

function candidateRows() {
  purgeCandidates();

  return [...candidatePool.values()]
    .sort(
      (
        a,
        b
      ) =>
        b.rank -
        a.rank
    )
    .slice(
      0,
      25
    )
    .map(
      x => ({
        symbol:
          x.symbol,

        price:
          x.price,

        edgeScore:
          x.edgeScore,

        rank:
          x.rank,

        freshnessScore:
          x.freshnessScore,

        flowScore:
          x.flowScore,

        cmo:
          x.momentum,

        atrPct:
          x.atrPct,

        takerBuy:
          x.takerRatio,

        breakoutAcceptance:
          x.breakoutAcceptance,

        shadowEdge:
          x.shadowEdge,

        ageSeconds:
          (
            Date.now() -
            x.candidateAt
          ) /
          1000
      })
    );
}

async function latestTrades() {
  if (
    !cloudConnected
  ) {
    return [];
  }

  try {
    return await db
      .collection(
        'trades'
      )
      .find({
        version:
          C.version
      })
      .sort({
        exitTime:
          -1
      })
      .limit(
        C.historyLimitDashboard
      )
      .toArray();

  } catch (error) {

    console.error(
      'Latest trades:',
      error.message
    );

    return [];
  }
}

async function latestJournal() {
  if (
    !cloudConnected
  ) {
    return [];
  }

  try {
    return await db
      .collection(
        'journal'
      )
      .find({
        version:
          C.version
      })
      .sort({
        time:
          -1
      })
      .limit(
        C.journalLimitDashboard
      )
      .toArray();

  } catch (error) {

    console.error(
      'Latest journal:',
      error.message
    );

    return [];
  }
}

function botStatus() {
  let status =
    'RUNNING';

  if (
    manualPause
  ) {
    status =
      'MANUAL_PAUSE';

  } else if (
    dailyPause
  ) {
    status =
      'DAILY_PROTECTION';

  } else if (
    cooldownActive()
  ) {
    status =
      'COOLDOWN';

  } else if (
    !marketRegime.ready
  ) {
    status =
      'WARMING';
  }

  return status;
}

function winRate() {
  return stats.totalTrades
    ? (
        stats.wins /
        stats.totalTrades
      ) *
        100
    : 0;
}

function profitFactor() {
  return stats.grossLoss >
    0
    ? stats.grossProfit /
      stats.grossLoss
    : stats.grossProfit >
      0
      ? Infinity
      : 0;
}

// ============================================================
// API
// ============================================================

app.get(
  '/health',
  (
    req,
    res
  ) => {
    res.json({
      ok:
        true,

      name:
        'LOMY',

      version:
        C.version,

      paperTrading:
        C.paperTrading,

      cloudConnected,

      miniConnected,

      klineConnected,

      subscribed:
        subscribed.size,

      ready:
        readyCount(),

      timestamp:
        Date.now()
    });
  }
);

app.get(
  '/api/data',
  async (
    req,
    res
  ) => {
    try {
      checkDay();

      const e =
        equity();

      const pf =
        profitFactor();

      res.json({
        ok:
          true,

        version:
          C.version,

        mode:
          'PAPER',

        status:
          botStatus(),

        cash,

        equity:
          e,

        dailyPnL,

        dailyStartEquity,

        dailyPnLPct:
          dailyStartEquity
            ? (
                dailyPnL /
                dailyStartEquity
              ) *
                100
            : 0,

        peakEquity,

        stats: {
          ...stats,

          winRate:
            winRate(),

          profitFactor:
            Number.isFinite(
              pf
            )
              ? pf
              : null
        },

        marketRegime,

        session:
          sessionUTC(),

        positions:
          positionRows(),

        candidates:
          candidateRows(),

        candidateCount:
          candidatePool.size,

        universe: {
          subscribed:
            subscribed.size,

          ready:
            readyCount(),

          tickerCount:
            tickers.size,

          warmupQueue:
            warmupQueue.length,

          warmupWorkers,

          warmupStats
        },

        websocket: {
          miniConnected,

          klineConnected,

          lastMiniMessage,

          lastKlineMessage
        },

        protection: {
          manualPause,

          dailyPause,

          cooldown:
            cooldownActive(),

          cooldownReason,

          cooldownUntil,

          entriesSinceCooldown,

          lossStreak
        },

        network:
          networkSnapshot(),

        cloudConnected,

        uptimeSeconds:
          Math.round(
            process.uptime()
          ),

        timestamp:
          Date.now()
      });

    } catch (error) {

      console.error(
        '/api/data:',
        error.message
      );

      res.status(
        500
      ).json({
        ok:
          false,

        error:
          error.message
      });
    }
  }
);

app.get(
  '/api/trades',
  async (
    req,
    res
  ) => {
    try {
      const rows =
        await latestTrades();

      res.json({
        ok:
          true,

        count:
          rows.length,

        trades:
          rows
      });

    } catch (error) {

      res.status(
        500
      ).json({
        ok:
          false,

        error:
          error.message
      });
    }
  }
);

app.get(
  '/api/journal',
  async (
    req,
    res
  ) => {
    try {
      const rows =
        await latestJournal();

      res.json({
        ok:
          true,

        count:
          rows.length,

        journal:
          rows
      });

    } catch (error) {

      res.status(
        500
      ).json({
        ok:
          false,

        error:
          error.message
      });
    }
  }
);

app.get(
  '/api/network',
  (
    req,
    res
  ) => {
    res.json({
      ok:
        true,

      network:
        networkSnapshot()
    });
  }
);

app.post(
  '/api/pause',
  async (
    req,
    res
  ) => {
    manualPause =
      true;

    candidatePool.clear();

    await saveCloudState();

    tg(
      '⏸ <b>LOMY V5.1 MANUAL PAUSE</b>'
    );

    res.json({
      ok:
        true,

      manualPause
    });
  }
);

app.post(
  '/api/resume',
  async (
    req,
    res
  ) => {
    manualPause =
      false;

    await saveCloudState();

    tg(
      '▶️ <b>LOMY V5.1 RESUMED</b>'
    );

    res.json({
      ok:
        true,

      manualPause
    });
  }
);

app.post(
  '/api/emergency-close',
  async (
    req,
    res
  ) => {
    try {
      manualPause =
        true;

      candidatePool.clear();

      const symbols =
        Object.keys(
          positions
        );

      const closed =
        [];

      for (
        const symbol
        of symbols
      ) {
        const p =
          positions[
            symbol
          ];

        const price =
          tickers.get(
            symbol
          )?.price ||
          p.lastPrice ||
          p.entryPrice;

        await closePaperTrade(
          symbol,
          price,
          'EMERGENCY_CLOSE'
        );

        closed.push(
          symbol
        );
      }

      await saveCloudState();

      tg(
        `🚨 <b>LOMY EMERGENCY CLOSE</b>
Closed: ${closed.length}
Bot paused.`
      );

      res.json({
        ok:
          true,

        closed,

        manualPause
      });

    } catch (error) {

      console.error(
        'Emergency close:',
        error.message
      );

      res.status(
        500
      ).json({
        ok:
          false,

        error:
          error.message
      });
    }
  }
);

// ============================================================
// DASHBOARD
// ============================================================

app.get(
  '/',
  (
    req,
    res
  ) => {
    res.send(`
<!DOCTYPE html>
<html lang="en">
<head>

<meta charset="UTF-8">
<meta
  name="viewport"
  content="width=device-width, initial-scale=1.0"
/>

<title>LOMY V5.1</title>

<style>

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: #090d12;
  color: #e9eef5;
  font-family: Arial, Helvetica, sans-serif;
}

.container {
  width: min(1500px, 96%);
  margin: auto;
  padding: 22px 0 60px;
}

h1 {
  margin: 0 0 4px;
  font-size: 28px;
}

.subtitle {
  opacity: .7;
  margin-bottom: 20px;
}

.toolbar {
  display: flex;
  flex-wrap: wrap;
  gap: 10px;
  margin-bottom: 18px;
}

button {
  border: 0;
  border-radius: 7px;
  padding: 10px 16px;
  font-weight: bold;
  cursor: pointer;
}

button.pause {
  background: #e9b949;
  color: #111;
}

button.resume {
  background: #36c98f;
  color: #08110d;
}

button.emergency {
  background: #e55353;
  color: white;
}

.cards {
  display: grid;
  grid-template-columns:
    repeat(
      auto-fit,
      minmax(165px, 1fr)
    );
  gap: 10px;
  margin-bottom: 18px;
}

.card {
  background: #111821;
  border: 1px solid #1e2a38;
  border-radius: 10px;
  padding: 14px;
}

.label {
  font-size: 12px;
  opacity: .68;
  margin-bottom: 6px;
}

.value {
  font-size: 21px;
  font-weight: bold;
}

.section {
  background: #111821;
  border: 1px solid #1e2a38;
  border-radius: 10px;
  padding: 14px;
  margin-top: 14px;
  overflow-x: auto;
}

.section h2 {
  margin: 0 0 12px;
  font-size: 18px;
}

table {
  width: 100%;
  border-collapse: collapse;
  min-width: 850px;
}

th,
td {
  text-align: left;
  padding: 9px;
  border-bottom: 1px solid #1c2734;
  font-size: 13px;
}

th {
  opacity: .7;
}

.green {
  color: #42d99a;
}

.red {
  color: #ff6767;
}

.yellow {
  color: #f2c94c;
}

.small {
  font-size: 12px;
  opacity: .75;
}

</style>

</head>

<body>

<div class="container">

  <h1>
    LOMY V5.1
  </h1>

  <div class="subtitle">
    Entry Intelligence · Bandwidth Optimized · PAPER ONLY
  </div>

  <div class="toolbar">

    <button
      class="pause"
      onclick="action('/api/pause')"
    >
      PAUSE
    </button>

    <button
      class="resume"
      onclick="action('/api/resume')"
    >
      RESUME
    </button>

    <button
      class="emergency"
      onclick="emergencyClose()"
    >
      EMERGENCY CLOSE
    </button>

  </div>

  <div
    class="cards"
    id="cards"
  ></div>

  <div class="section">

    <h2>
      Market Intelligence
    </h2>

    <div
      id="market"
      class="small"
    >
      Loading...
    </div>

  </div>

  <div class="section">

    <h2>
      Open Paper Positions
    </h2>

    <table>

      <thead>
        <tr>
          <th>Symbol</th>
          <th>Entry</th>
          <th>Live</th>
          <th>PnL</th>
          <th>PnL %</th>
          <th>MFE</th>
          <th>MAE</th>
          <th>SL</th>
          <th>TP</th>
          <th>Age</th>
        </tr>
      </thead>

      <tbody
        id="positions"
      ></tbody>

    </table>

  </div>

  <div class="section">

    <h2>
      Candidate Ranking
    </h2>

    <table>

      <thead>
        <tr>
          <th>Symbol</th>
          <th>Score</th>
          <th>Rank</th>
          <th>Fresh</th>
          <th>Flow</th>
          <th>CMO</th>
          <th>ATR %</th>
          <th>Taker</th>
          <th>Breakout</th>
          <th>Age</th>
        </tr>
      </thead>

      <tbody
        id="candidates"
      ></tbody>

    </table>

  </div>

  <div class="section">

    <h2>
      Bandwidth Guard
    </h2>

    <div
      id="network"
      class="small"
    >
      Loading...
    </div>

  </div>

</div>

<script>

function num(
  x,
  digits = 2
) {
  const n =
    Number(x);

  return Number.isFinite(n)
    ? n.toFixed(digits)
    : '-';
}

function money(x) {
  return '$' + num(
    x,
    2
  );
}

function pnlClass(x) {
  return Number(x) >= 0
    ? 'green'
    : 'red';
}

async function action(url) {
  try {
    await fetch(
      url,
      {
        method:
          'POST'
      }
    );

    await load();

  } catch (error) {

    alert(
      error.message
    );
  }
}

async function emergencyClose() {
  const ok =
    confirm(
      'Close ALL paper positions and pause the bot?'
    );

  if (
    !ok
  ) {
    return;
  }

  await action(
    '/api/emergency-close'
  );
}

function renderCards(data) {
  const values = [
    [
      'Status',
      data.status
    ],

    [
      'Mode',
      data.mode
    ],

    [
      'Cash',
      money(
        data.cash
      )
    ],

    [
      'Equity',
      money(
        data.equity
      )
    ],

    [
      'Daily PnL',
      money(
        data.dailyPnL
      )
    ],

    [
      'Closed',
      data.stats.totalTrades
    ],

    [
      'Wins',
      data.stats.wins
    ],

    [
      'Losses',
      data.stats.losses
    ],

    [
      'Win Rate',
      num(
        data.stats.winRate,
        2
      ) + '%'
    ],

    [
      'Net Profit',
      money(
        data.stats.netProfit
      )
    ],

    [
      'Max DD',
      num(
        data.stats.maxDrawdown,
        2
      ) + '%'
    ],

    [
      'Open',
      data.positions.length
    ],

    [
      'Candidates',
      data.candidateCount
    ],

    [
      'Ready Symbols',
      data.universe.ready +
      '/' +
      data.universe.subscribed
    ]
  ];

  document.getElementById(
    'cards'
  ).innerHTML =
    values
      .map(
        ([label, value]) =>
          `
          <div class="card">
            <div class="label">
              ${label}
            </div>

            <div class="value">
              ${value}
            </div>
          </div>
          `
      )
      .join('');
}

function renderPositions(rows) {
  const el =
    document.getElementById(
      'positions'
    );

  if (
    !rows.length
  ) {
    el.innerHTML =
      '<tr><td colspan="10">No open positions</td></tr>';

    return;
  }

  el.innerHTML =
    rows
      .map(
        p =>
          `
          <tr>

            <td>
              ${p.symbol}
            </td>

            <td>
              ${num(p.entryPrice, 8)}
            </td>

            <td>
              ${num(p.livePrice, 8)}
            </td>

            <td class="${pnlClass(p.pnl)}">
              ${money(p.pnl)}
            </td>

            <td class="${pnlClass(p.pnlPct)}">
              ${num(p.pnlPct, 2)}%
            </td>

            <td>
              ${num(p.mfePct, 2)}%
            </td>

            <td>
              ${num(p.maePct, 2)}%
            </td>

            <td>
              ${num(p.stopLoss, 8)}
            </td>

            <td>
              ${num(p.takeProfit, 8)}
            </td>

            <td>
              ${num(p.ageMinutes, 1)}m
            </td>

          </tr>
          `
      )
      .join('');
}

function renderCandidates(rows) {
  const el =
    document.getElementById(
      'candidates'
    );

  if (
    !rows.length
  ) {
    el.innerHTML =
      '<tr><td colspan="10">No active candidates</td></tr>';

    return;
  }

  el.innerHTML =
    rows
      .map(
        x =>
          `
          <tr>

            <td>
              ${x.symbol}
            </td>

            <td>
              ${x.edgeScore}
            </td>

            <td>
              ${num(x.rank, 1)}
            </td>

            <td>
              ${x.freshnessScore}
            </td>

            <td>
              ${x.flowScore}
            </td>

            <td>
              ${num(x.cmo, 1)}
            </td>

            <td>
              ${num(x.atrPct, 3)}
            </td>

            <td>
              ${
                x.takerBuy === null
                  ? '-'
                  : num(
                      x.takerBuy *
                      100,
                      1
                    ) + '%'
              }
            </td>

            <td>
              ${x.breakoutAcceptance}
            </td>

            <td>
              ${num(x.ageSeconds, 0)}s
            </td>

          </tr>
          `
      )
      .join('');
}

function renderMarket(data) {
  const m =
    data.marketRegime;

  document.getElementById(
    'market'
  ).innerHTML =
    `
    Regime:
    <b>${m.regime}</b>
    &nbsp; | &nbsp;

    Breadth:
    <b>${num(m.breadth, 1)}%</b>
    &nbsp; | &nbsp;

    BTC Bullish:
    <b>${m.btcBullish}</b>
    &nbsp; | &nbsp;

    BTC Score:
    <b>${m.btcScore}</b>
    &nbsp; | &nbsp;

    BTC Freshness:
    <b>${m.btcFreshness}</b>
    &nbsp; | &nbsp;

    Session:
    <b>${data.session}</b>
    &nbsp; | &nbsp;

    Mini WS:
    <b>${data.websocket.miniConnected}</b>
    &nbsp; | &nbsp;

    Kline WS:
    <b>${data.websocket.klineConnected}</b>
    `;
}

function renderNetwork(data) {
  const n =
    data.network;

  document.getElementById(
    'network'
  ).innerHTML =
    `
    Estimated app outbound:
    <b>${n.estimatedOutboundMB} MB</b>
    &nbsp; | &nbsp;

    Mongo:
    <b>${n.mongoWriteMB} MB</b>
    &nbsp; | &nbsp;

    Telegram:
    <b>${n.telegramMB} MB</b>
    &nbsp; | &nbsp;

    REST request estimate:
    <b>${n.restRequestMB} MB</b>
    <br><br>

    State writes:
    <b>${n.stateWrites}</b>
    &nbsp; | &nbsp;

    Journal writes:
    <b>${n.journalWrites}</b>
    &nbsp; | &nbsp;

    Trade writes:
    <b>${n.tradeWrites}</b>
    &nbsp; | &nbsp;

    Skipped routine analysis logs:
    <b>${n.skippedAnalysisLogs}</b>
    &nbsp; | &nbsp;

    Candle cloud writes:
    <b>${n.candleCloudWrites}</b>

    <br><br>

    <b>
      Candle cloud persistence is disabled in V5.1.
    </b>
    `;
}

async function load() {
  try {
    const response =
      await fetch(
        '/api/data',
        {
          cache:
            'no-store'
        }
      );

    const data =
      await response.json();

    if (
      !data.ok
    ) {
      return;
    }

    renderCards(
      data
    );

    renderPositions(
      data.positions
    );

    renderCandidates(
      data.candidates
    );

    renderMarket(
      data
    );

    renderNetwork(
      data
    );

  } catch (error) {

    console.error(
      error
    );
  }
}

load();

setInterval(
  load,
  5000
);

</script>

</body>
</html>
    `);
  }
);

// ============================================================
// STARTUP
// ============================================================

async function start() {
  console.log(
    '======================================='
  );

  console.log(
    'LOMY V5.1 BANDWIDTH OPTIMIZED'
  );

  console.log(
    'PAPER ONLY'
  );

  console.log(
    '======================================='
  );

  try {
    await connectCloud();

    await loadCloudState();

  } catch (error) {

    console.error(
      'MongoDB startup error:',
      error.message
    );

    process.exit(
      1
    );
  }

  connectMiniTicker();

  // Give miniTicker time to build the first universe.
  await sleep(
    6000
  );

  try {
    await refreshUniverse(
      true
    );

  } catch (error) {

    console.error(
      'Initial universe:',
      error.message
    );
  }

  setTimeout(
    () => {
      calculateMarketRegime();

      console.log(
        `LOMY READY | Universe ${subscribed.size} | Warmup queue ${warmupQueue.length}`
      );

      tg(
        `🤖 <b>LOMY V5.1 STARTED</b>
PAPER ONLY
Universe: ${subscribed.size}
Candle cloud writes: OFF
Analysis sampling: ${(
          C.analysisJournalSampleRate *
          100
        ).toFixed(1)}%
State save: ${C.stateSaveMs / 1000}s`
      );
    },
    10000
  );
}

// ============================================================
// SERVER
// ============================================================

const server =
  app.listen(
    PORT,
    () => {
      console.log(
        `LOMY dashboard running on port ${PORT}`
      );
    }
  );

start().catch(
  error => {
    console.error(
      'Fatal startup:',
      error
    );

    process.exit(
      1
    );
  }
);

// ============================================================
// GRACEFUL SHUTDOWN
// ============================================================

async function shutdown(
  signal
) {
  if (
    shuttingDown
  ) {
    return;
  }

  shuttingDown =
    true;

  console.log(
    `${signal} received. Saving state...`
  );

  try {
    await saveCloudState();
  } catch {}

  try {
    safeCloseWs(
      miniWs
    );
  } catch {}

  try {
    safeCloseWs(
      klineWs
    );
  } catch {}

  try {
    server.close();
  } catch {}

  try {
    if (
      mongoClient
    ) {
      await mongoClient.close();
    }
  } catch {}

  process.exit(
    0
  );
}

process.on(
  'SIGTERM',
  () =>
    shutdown(
      'SIGTERM'
    )
);

process.on(
  'SIGINT',
  () =>
    shutdown(
      'SIGINT'
    )
);

process.on(
  'uncaughtException',
  error => {
    console.error(
      'UNCAUGHT EXCEPTION:',
      error
    );
  }
);

process.on(
  'unhandledRejection',
  error => {
    console.error(
      'UNHANDLED REJECTION:',
      error
    );
  }
);
