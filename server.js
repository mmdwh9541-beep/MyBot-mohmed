require('dotenv').config();

const express = require('express');
const axios = require('axios');
const WebSocket = require('ws');
const { MongoClient } = require('mongodb');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 5000);
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN || '';
const CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const MONGODB_URI = process.env.MONGODB_URI || '';
const MONGODB_DB = process.env.MONGODB_DB || 'lomy';

const REST_BASE = 'https://data-api.binance.vision';
const WS_BASE = 'wss://data-stream.binance.vision';

const C = Object.freeze({
  version: '5.1.1-WARMUP-GUARD',
  stateKey: 'main-v511',
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
  btcSymbol: 'BTCUSDT',

  warmupConcurrency: 2,
  warmupDelayMs: 1200,
  warmupRetryBaseMs: 5000,
  warmupRetryMaxMs: 120000,
  warmupMaxRetries: 6,
  warmup429MinPauseMs: 30000,

  wsReconnectBaseMs: 5000,
  wsReconnectMaxMs: 60000,
  controlChunkSize: 40,

  stateSaveMs: 60000,
  regimeRefreshMs: 60000,
  rankRefreshMs: 5000,
  analysisJournalSampleRate: 0.01,
  networkReportMs: 30 * 60 * 1000,
  networkAlertMB: 250
});

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
const candidatePool = new Map();

let subscribed = new Set();
let latest = [];

let mongoClient = null;
let db = null;
let cloudConnected = false;

let miniWs = null;
let klineWs = null;

let miniConnected = false;
let klineConnected = false;

let miniReconnectAttempts = 0;
let klineReconnectAttempts = 0;

let shuttingDown = false;

const warmupLoaded = new Set();
const warmupLoading = new Set();
const warmupRetryCount = new Map();

let warmupQueue = [];
let warmupWorkers = 0;
let warmupBlockedUntil = 0;
let warmupResumeTimer = null;

const warmupStats = {
  restLoaded: 0,
  restRequests: 0,
  failed: 0,
  rateLimited: 0,
  retries: 0,
  last429: 0
};

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

const networkMeter = {
  startedAt: Date.now(),
  telegramBytes: 0,
  mongoWriteBytes: 0,
  restRequestBytes: 0,
  wsControlBytes: 0,
  journalWrites: 0,
  tradeWrites: 0,
  stateWrites: 0,
  skippedAnalysisLogs: 0,
  lastAlertMB: 0
};

const sleep = ms =>
  new Promise(resolve => setTimeout(resolve, ms));

const n = (value, fallback = 0) =>
  Number.isFinite(Number(value))
    ? Number(value)
    : fallback;

const clamp = (value, min, max) =>
  Math.max(min, Math.min(max, value));

const pct = (diff, base) =>
  base
    ? (diff / base) * 100
    : 0;

const utcDay = () =>
  new Date()
    .toISOString()
    .slice(0, 10);

const byteLen = value => {
  try {
    return Buffer.byteLength(
      typeof value === 'string'
        ? value
        : JSON.stringify(value),
      'utf8'
    );
  } catch {
    return 0;
  }
};

const networkMB = bytes =>
  +(bytes / 1024 / 1024).toFixed(2);

function sessionUTC() {
  const h =
    new Date().getUTCHours();

  if (h < 7) return 'ASIA';
  if (h < 13) return 'LONDON';
  if (h < 16) return 'LONDON_NY';
  if (h < 21) return 'NEW_YORK';

  return 'LATE_US';
}

function networkSnapshot() {
  return {
    estimatedOutboundMB:
      networkMB(
        networkMeter.telegramBytes +
        networkMeter.mongoWriteBytes +
        networkMeter.restRequestBytes +
        networkMeter.wsControlBytes
      ),

    telegramMB:
      networkMB(
        networkMeter.telegramBytes
      ),

    mongoWriteMB:
      networkMB(
        networkMeter.mongoWriteBytes
      ),

    restRequestMB:
      networkMB(
        networkMeter.restRequestBytes
      ),

    wsControlMB:
      networkMB(
        networkMeter.wsControlBytes
      ),

    journalWrites:
      networkMeter.journalWrites,

    tradeWrites:
      networkMeter.tradeWrites,

    stateWrites:
      networkMeter.stateWrites,

    skippedAnalysisLogs:
      networkMeter.skippedAnalysisLogs,

    uptimeHours:
      +(
        (
          Date.now() -
          networkMeter.startedAt
        ) /
        3600000
      ).toFixed(2)
  };
}

// ============================================================
// TELEGRAM
// ============================================================

const tgQueue = [];

let tgBusy = false;

function tg(text) {
  if (
    TELEGRAM_TOKEN &&
    CHAT_ID
  ) {
    tgQueue.push(
      String(text)
    );
  }
}

setInterval(
  async () => {

    if (
      tgBusy ||
      !tgQueue.length
    ) {
      return;
    }

    tgBusy = true;

    const text =
      tgQueue.shift();

    try {

      const payload = {
        chat_id:
          CHAT_ID,

        text,

        parse_mode:
          'HTML'
      };

      networkMeter.telegramBytes +=
        byteLen(payload);

      await axios.post(
        `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
        payload,
        {
          timeout: 10000
        }
      );

    } catch (error) {

      if (
        error.response?.status ===
        429
      ) {

        tgQueue.unshift(
          text
        );

        await sleep(
          5000
        );

      } else {

        console.error(
          'Telegram:',
          error.message
        );
      }

    } finally {

      tgBusy = false;
    }

  },
  1200
);

// ============================================================
// MONGODB
// ============================================================

async function connectCloud() {

  if (!MONGODB_URI) {
    throw new Error(
      'MONGODB_URI missing'
    );
  }

  mongoClient =
    new MongoClient(
      MONGODB_URI,
      {
        maxPoolSize: 8
      }
    );

  await mongoClient.connect();

  db =
    mongoClient.db(
      MONGODB_DB
    );

  await db.command({
    ping: 1
  });

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

  console.log(
    'MongoDB CLOUD CONNECTED'
  );
}

async function loadCloudState() {

  const state =
    await db
      .collection('state')
      .findOne({
        _id:
          C.stateKey
      });

  if (!state) {

    console.log(
      'Fresh V5.1.1 PAPER account.'
    );

    return;
  }

  cash =
    n(
      state.cash,
      C.startingBalance
    );

  positions =
    state.positions ||
    {};

  stats = {
    ...stats,
    ...(state.stats || {})
  };

  dailyPnL =
    n(
      state.dailyPnL
    );

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
    n(
      state.cooldownUntil
    );

  cooldownReason =
    state.cooldownReason ||
    null;

  entriesSinceCooldown =
    n(
      state.entriesSinceCooldown
    );

  lossStreak =
    n(
      state.lossStreak
    );

  Object.assign(
    lastLossBySymbol,
    state.lastLossBySymbol ||
    {}
  );

  console.log(
    `STATE RESTORED | Cash $${cash.toFixed(
      2
    )} | Open ${
      Object.keys(
        positions
      ).length
    }`
  );
}

async function saveCloudState() {

  if (!cloudConnected) {
    return;
  }

  try {

    const payload = {
      version:
        C.version,

      updatedAt:
        Date.now(),

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

    networkMeter.mongoWriteBytes +=
      byteLen(payload);

    networkMeter.stateWrites++;

    await db
      .collection('state')
      .updateOne(
        {
          _id:
            C.stateKey
        },
        {
          $set:
            payload
        },
        {
          upsert:
            true
        }
      );

  } catch (error) {

    console.error(
      'State save:',
      error.message
    );
  }
}

async function cloudJournal(
  row,
  force = false
) {

  if (!cloudConnected) {
    return;
  }

  if (
    !force &&
    row.type ===
      'ANALYSIS' &&
    Math.random() >
      C.analysisJournalSampleRate
  ) {

    networkMeter.skippedAnalysisLogs++;

    return;
  }

  try {

    const doc = {
      time:
        Date.now(),

      version:
        C.version,

      ...row
    };

    networkMeter.mongoWriteBytes +=
      byteLen(doc);

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
      version:
        C.version,

      ...record
    };

    networkMeter.mongoWriteBytes +=
      byteLen(doc);

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
// ============================================================

function parseKline(row) {

  return {
    open:
      n(row[1]),

    high:
      n(row[2]),

    low:
      n(row[3]),

    close:
      n(row[4]),

    volume:
      n(row[5]),

    closeTime:
      n(row[6]),

    quoteVolume:
      n(row[7]),

    trades:
      n(row[8]),

    takerBuyBase:
      n(row[9]),

    takerBuyQuote:
      n(row[10])
  };
}

function mergeCandles(
  symbol,
  incoming
) {

  const map =
    new Map();

  for (
    const candle
    of candles[symbol] || []
  ) {

    map.set(
      candle.closeTime,
      candle
    );
  }

  for (
    const candle
    of incoming || []
  ) {

    map.set(
      candle.closeTime,
      candle
    );
  }

  candles[symbol] =
    [...map.values()]
      .sort(
        (a, b) =>
          a.closeTime -
          b.closeTime
      )
      .slice(
        -C.maxCandles
      );
}

function scheduleWarmupResume(ms) {

  clearTimeout(
    warmupResumeTimer
  );

  warmupResumeTimer =
    setTimeout(
      () =>
        processWarmupQueue(),
      Math.max(
        1000,
        ms
      )
    );
}

async function fetchWarmup(symbol) {

  if (
    warmupLoaded.has(symbol) ||
    warmupLoading.has(symbol)
  ) {
    return;
  }

  warmupLoading.add(
    symbol
  );

  try {

    warmupStats.restRequests++;

    const requestDesc =
      `${REST_BASE}/api/v3/klines?symbol=${symbol}&interval=${C.interval}&limit=${C.maxCandles}`;

    networkMeter.restRequestBytes +=
      byteLen(
        requestDesc
      );

    const response =
      await axios.get(
        `${REST_BASE}/api/v3/klines`,
        {
          params: {
            symbol,
            interval:
              C.interval,
            limit:
              C.maxCandles
          },

          timeout:
            12000
        }
      );

    const now =
      Date.now();

    mergeCandles(
      symbol,

      response.data
        .map(
          parseKline
        )
        .filter(
          candle =>
            candle.closeTime <
            now
        )
    );

    if (
      (
        candles[symbol]?.length ||
        0
      ) >=
      C.warmupCandles
    ) {

      warmupLoaded.add(
        symbol
      );

      warmupRetryCount.delete(
        symbol
      );

      warmupStats.restLoaded++;

    } else {

      throw new Error(
        'WARMUP_INCOMPLETE'
      );
    }

  } catch (error) {

    const status =
      error.response?.status;

    const retry =
      n(
        warmupRetryCount.get(
          symbol
        )
      ) +
      1;

    warmupRetryCount.set(
      symbol,
      retry
    );

    warmupStats.failed++;

    if (
      status ===
        429 ||
      status ===
        418
    ) {

      warmupStats.rateLimited++;

      warmupStats.last429 =
        Date.now();

      const retryAfterSec =
        n(
          error.response?.headers?.[
            'retry-after'
          ]
        );

      const backoff =
        Math.min(
          C.warmupRetryMaxMs,

          Math.max(
            C.warmup429MinPauseMs,

            retryAfterSec *
              1000 ||
            C.warmupRetryBaseMs *
              (
                2 **
                Math.min(
                  retry,
                  5
                )
              )
          )
        );

      warmupBlockedUntil =
        Date.now() +
        backoff;

      console.warn(
        `Warmup rate limited ${status} | pause ${Math.ceil(
          backoff /
          1000
        )}s`
      );

    } else {

      console.warn(
        `Warmup ${symbol}:`,
        status ||
        error.message
      );
    }

    if (
      retry <=
        C.warmupMaxRetries &&
      subscribed.has(
        symbol
      )
    ) {

      warmupStats.retries++;

      const delay =
        Math.min(
          C.warmupRetryMaxMs,

          C.warmupRetryBaseMs *
            (
              2 **
              Math.min(
                retry -
                1,
                5
              )
            )
        );

      setTimeout(
        () =>
          queueWarmup(
            symbol
          ),
        delay
      );
    }

  } finally {

    warmupLoading.delete(
      symbol
    );
  }
}

function queueWarmup(symbol) {

  if (
    !subscribed.has(symbol) ||
    warmupLoaded.has(symbol) ||
    warmupLoading.has(symbol) ||
    warmupQueue.includes(symbol)
  ) {
    return;
  }

  warmupQueue.push(
    symbol
  );

  processWarmupQueue();
}

function processWarmupQueue() {

  if (
    Date.now() <
    warmupBlockedUntil
  ) {

    scheduleWarmupResume(
      warmupBlockedUntil -
      Date.now()
    );

    return;
  }

  while (
    warmupWorkers <
      C.warmupConcurrency &&
    warmupQueue.length
  ) {

    const symbol =
      warmupQueue.shift();

    warmupWorkers++;

    (
      async () => {

        try {

          await fetchWarmup(
            symbol
          );

          await sleep(
            C.warmupDelayMs
          );

        } finally {

          warmupWorkers--;

          processWarmupQueue();
        }

      }
    )();
  }
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
      .slice(
        -period
      )
      .reduce(
        (
          sum,
          item
        ) =>
          sum +
          n(
            item[key]
          ),
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
          sum,
          item
        ) =>
          sum +
          n(
            item[key]
          ),
        0
      ) /
    period;

  const multiplier =
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
      multiplier +
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

    const diff =
      arr[i].close -
      arr[
        i -
        1
      ].close;

    if (
      diff >
      0
    ) {

      up +=
        diff;

    } else {

      down +=
        Math.abs(
          diff
        );
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
    i <
      arr.length;
    i++
  ) {

    const current =
      arr[i];

    const previous =
      arr[
        i -
        1
      ];

    ranges.push(
      Math.max(
        current.high -
        current.low,

        Math.abs(
          current.high -
          previous.close
        ),

        Math.abs(
          current.low -
          previous.close
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
        sum,
        value
      ) =>
        sum +
        value,
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
          sum,
          candle
        ) =>
          sum +
          candle.volume,
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
          sum,
          candle
        ) =>
          sum +
          candle.volume,
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

  let buy =
    0;

  let total =
    0;

  let valid =
    0;

  for (
    const candle
    of arr.slice(-3)
  ) {

    if (
      candle.volume >
        0 &&
      candle.takerBuyBase >
        0
    ) {

      buy +=
        candle.takerBuyBase;

      total +=
        candle.volume;

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

function breakoutAcceptance(
  arr,
  resistance,
  atrPct
) {

  const current =
    arr.at(-1);

  const previous =
    arr.at(-2);

  let score =
    0;

  const reasons =
    [];

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

    score +=
      25;

    reasons.push(
      'CLOSE_ABOVE_RES'
    );
  }

  if (
    closeLocation >=
    0.72
  ) {

    score +=
      15;

    reasons.push(
      'CLOSE_HIGH'
    );
  }

  if (
    body >=
    0.55
  ) {

    score +=
      15;

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

    score +=
      15;

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

    score +=
      20;

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

    score +=
      10;

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

  let score =
    0;

  let freshness =
    100;

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
    structure(
      arr
    ) ===
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
    2
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
      )
  };
}

function calculateMarketRegime() {

  let ready =
    0;

  let bullish =
    0;

  for (
    const symbol
    of subscribed
  ) {

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
      +breadth.toFixed(
        2
      ),

    regime:
      (
        marketRegime.ready ||
        ready >=
          20
      )
        ? regime
        : 'WARMING',

    overextended,

    updatedAt:
      Date.now()
  };
}

function buildScore(ctx) {

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

  let trend =
    0;

  let freshness =
    100;

  let flow =
    0;

  let momentumScore =
    0;

  let volatility =
    0;

  let structureScore =
    0;

  let regimeScore =
    0;

  const reasons =
    [];

  const warnings =
    [];

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

    freshness -=
      55;

    warnings.push(
      'EMA_CHASE'
    );

  } else if (
    emaDistance >
    1.0
  ) {

    freshness -=
      18;
  }

  if (
    ext5 >
    C.maxExt5Pct
  ) {

    freshness -=
      35;

    warnings.push(
      'EXT5_CHASE'
    );
  }

  if (
    ext10 >
    C.maxExt10Pct
  ) {

    freshness -=
      30;

    warnings.push(
      'EXT10_CHASE'
    );
  }

  if (
    volumeAccel >
    C.volumeClimaxAcceleration
  ) {

    freshness -=
      40;

    warnings.push(
      'VOLUME_CLIMAX'
    );
  }

  if (
    momentum >
    C.cmoCoreMax
  ) {

    freshness -=
      30;

    warnings.push(
      'CMO_OVERHEAT'
    );
  }

  if (
    priceAcceleration(
      arr
    ) >
    1.0
  ) {

    freshness -=
      20;

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

      flow +=
        70;

      reasons.push(
        'ELITE_TAKER_FLOW'
      );

    } else if (
      takerRatio >=
      C.minTakerBuyRatio
    ) {

      flow +=
        42;
    }

    if (
      takerRatio >=
        0.70 &&
      takerRatio <
        0.80
    ) {

      flow -=
        15;

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

      flow +=
        30;

    } else if (
      flowMomentum >=
      C.minFlowMomentum
    ) {

      flow +=
        18;

    } else {

      flow -=
        20;
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
      C.cmoSoftMax
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

    volatility +=
      45;

  } else if (
    atrPct >=
      C.minAtrPct &&
    atrPct <=
      C.maxAtrPct
  ) {

    volatility +=
      28;

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

    volatility +=
      30;

  } else if (
    volumeRatio <=
    C.maxVolumeRatio
  ) {

    volatility +=
      18;
  }

  if (
    volumeAccel >=
      C.volumeAccelSweetMin &&
    volumeAccel <=
      C.volumeAccelSweetMax
  ) {

    volatility +=
      25;

  } else if (
    volumeAccel >
    C.volumeClimaxAcceleration
  ) {

    volatility -=
      25;
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

    regimeScore -=
      30;

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
      Math.round(
        trend
      ),

    freshnessScore:
      Math.round(
        freshness
      ),

    flowScore:
      Math.round(
        flow
      ),

    momentumScore:
      Math.round(
        momentumScore
      ),

    volatilityScore:
      Math.round(
        volatility
      ),

    structureScore:
      Math.round(
        structureScore
      ),

    regimeScore:
      Math.round(
        regimeScore
      ),

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
      value =>
        value ===
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
    volumeAcceleration(
      arr
    );

  const takerRatio =
    takerBuyRatio(
      current
    );

  const flowMomentum =
    orderFlowMomentum(
      arr
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

  const marketStructure =
    structure(
      arr
    );

  const prior =
    arr.slice(
      -21,
      -1
    );

  const resistance =
    Math.max(
      ...prior.map(
        candle =>
          candle.high
      )
    );

  const support =
    Math.min(
      ...prior.map(
        candle =>
          candle.low
      )
    );

  const ext5 =
    pct(
      current.close -
      arr.at(-6).close,
      arr.at(-6).close
    );

  const ext10 =
    pct(
      current.close -
      arr.at(-11).close,
      arr.at(-11).close
    );

  const acceptance =
    breakoutAcceptance(
      arr,
      resistance,
      atrPct
    );

  const score =
    buildScore({
      arr,
      x:
        current,
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

  const hardBlocks =
    [];

  if (!bullish) {
    hardBlocks.push(
      'NOT_BULLISH'
    );
  }

  if (
    !(
      current.close >
        e20 &&
      e20 >
        e50
    )
  ) {

    hardBlocks.push(
      'TREND_FAIL'
    );
  }

  if (
    marketStructure ===
    'BEARISH'
  ) {

    hardBlocks.push(
      'BEARISH_STRUCTURE'
    );
  }

  if (
    momentum <
      C.cmoSoftMin ||
    momentum >
      C.cmoSoftMax
  ) {

    hardBlocks.push(
      'CMO_OUTSIDE'
    );
  }

  if (
    atrPct <
      C.minAtrPct ||
    atrPct >
      C.maxAtrPct
  ) {

    hardBlocks.push(
      'ATR_OUTSIDE'
    );
  }

  if (
    volumeRatio <
      C.minVolumeRatio ||
    volumeRatio >
      C.maxVolumeRatio
  ) {

    hardBlocks.push(
      'VOLUME_OUTSIDE'
    );
  }

  if (
    volumeAccel >
    C.volumeClimaxAcceleration
  ) {

    hardBlocks.push(
      'VOLUME_CLIMAX'
    );
  }

  if (
    emaDistance >
    C.maxEmaDistancePct
  ) {

    hardBlocks.push(
      'EMA_CHASE'
    );
  }

  if (
    ext5 >
    C.maxExt5Pct
  ) {

    hardBlocks.push(
      'EXT5_CHASE'
    );
  }

  if (
    ext10 >
    C.maxExt10Pct
  ) {

    hardBlocks.push(
      'EXT10_CHASE'
    );
  }

  if (
    takerRatio !==
      null &&
    takerRatio <
      C.minTakerBuyRatio
  ) {

    hardBlocks.push(
      'FLOW_WEAK'
    );
  }

  if (
    flowMomentum !==
      null &&
    flowMomentum <
      C.minFlowMomentum
  ) {

    hardBlocks.push(
      'FLOW_MOMENTUM_WEAK'
    );
  }

  if (
    score.freshnessScore <
    C.minFreshnessScore
  ) {

    hardBlocks.push(
      'STALE_MOMENTUM'
    );
  }

  if (
    score.flowScore <
    C.minFlowScore
  ) {

    hardBlocks.push(
      'FLOW_SCORE_LOW'
    );
  }

  const threshold =
    marketRegime.regime ===
      'RISK_ON'
      ? C.minEdgeScoreRiskOn
      : marketRegime.regime ===
          'NEUTRAL'
        ? C.minEdgeScoreNeutral
        : C.minEdgeScoreDefensive;

  if (
    score.edgeScore <
    threshold
  ) {

    hardBlocks.push(
      'EDGE_SCORE_LOW'
    );
  }

  if (
    marketRegime.regime ===
      'DEFENSIVE' &&
    !score.shadowEdge
  ) {

    hardBlocks.push(
      'DEFENSIVE_NO_ELITE_EDGE'
    );
  }

  const nearBreakout =
    acceptance.breakoutPct >=
    -0.20;

  if (
    !acceptance.accepted &&
    !(
      nearBreakout &&
      score.shadowEdge
    )
  ) {

    hardBlocks.push(
      'NO_BREAKOUT_ACCEPTANCE'
    );
  }

  const grade =
    (
      score.edgeScore >=
        84 &&
      score.freshnessScore >=
        75 &&
      score.flowScore >=
        75
    )
      ? 'A'
      : score.edgeScore >=
          72
        ? 'B'
        : 'C';

  return {
    symbol,

    eligible:
      hardBlocks.length ===
      0,

    grade,

    score:
      score.edgeScore,

    edgeScore:
      score.edgeScore,

    trendScore:
      score.trendScore,

    freshnessScore:
      score.freshnessScore,

    flowScore:
      score.flowScore,

    momentumScore:
      score.momentumScore,

    volatilityScore:
      score.volatilityScore,

    structureScore:
      score.structureScore,

    regimeScore:
      score.regimeScore,

    shadowEdge:
      score.shadowEdge,

    reasons:
      score.reasons,

    warnings:
      score.warnings,

    price:
      current.close,

    signalPrice:
      current.close,

    candleTime:
      current.closeTime,

    bullish,
    bodyRatio,
    upperWickRatio,
    closeLocation,

    ema20:
      e20,

    ema50:
      e50,

    emaDistance,

    cmo:
      momentum,

    atr:
      atrValue,

    atrPct,
    volumeRatio,

    volumeAcceleration:
      volumeAccel,

    priceAcceleration:
      priceAcceleration(
        arr
      ),

    takerBuyRatio:
      takerRatio,

    orderFlowMomentum:
      flowMomentum,

    structure:
      marketStructure,

    resistance,
    support,

    breakoutPct:
      acceptance.breakoutPct,

    breakoutAcceptance:
      acceptance.score,

    acceptanceReasons:
      acceptance.reasons,

    ext5,
    ext10,

    regime:
      marketRegime.regime,

    breadth:
      marketRegime.breadth,

    marketOverextended:
      marketRegime.overextended,

    hardBlocks
  };
}
// ============================================================
// RISK / PAPER TRADING
// ============================================================

function equity() {

  let value =
    cash;

  for (
    const position
    of Object.values(
      positions
    )
  ) {

    const price =
      tickers.get(
        position.symbol
      )?.price ||
      position.lastPrice ||
      position.entryPrice;

    value +=
      position.qty *
      price;
  }

  return value;
}

function updateDrawdown() {

  const currentEquity =
    equity();

  if (
    currentEquity >
    peakEquity
  ) {

    peakEquity =
      currentEquity;
  }

  const drawdown =
    peakEquity
      ? (
          (
            peakEquity -
            currentEquity
          ) /
          peakEquity
        ) *
        100
      : 0;

  stats.maxDrawdown =
    Math.max(
      stats.maxDrawdown,
      drawdown
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
      `🛑 <b>LOMY V5.1.1 DAILY PROTECTION</b>
PnL: $${dailyPnL.toFixed(
        2
      )}`
    );

    saveCloudState();
  }
}

function cooldownActive() {

  if (!cooldownUntil) {
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
    `⏸ <b>LOMY V5.1.1 COOLDOWN</b>
${reason}
${Math.ceil(
      ms /
      60000
    )} minutes`
  );

  saveCloudState();
}

function symbolCooling(symbol) {

  const lastLoss =
    n(
      lastLossBySymbol[
        symbol
      ]
    );

  return (
    !!lastLoss &&
    Date.now() -
      lastLoss <
      C.symbolLossCooldownMs
  );
}

function paperAllocation() {

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

  return Math.max(
    0,

    Math.min(
      cash,

      equity() /
      Math.max(
        C.maxPositions,
        slots
      )
    )
  );
}

function candidateValid(candidate) {

  if (
    !candidate ||
    !candidatePool.has(
      candidate.symbol
    )
  ) {

    return {
      ok: false,
      reason:
        'MISSING'
    };
  }

  if (
    Date.now() -
      candidate.createdAt >
      C.candidateExpiryMs
  ) {

    return {
      ok: false,
      reason:
        'EXPIRED'
    };
  }

  if (
    positions[
      candidate.symbol
    ]
  ) {

    return {
      ok: false,
      reason:
        'ALREADY_OPEN'
    };
  }

  if (
    symbolCooling(
      candidate.symbol
    )
  ) {

    return {
      ok: false,
      reason:
        'SYMBOL_COOLDOWN'
    };
  }

  const live =
    tickers.get(
      candidate.symbol
    )?.price ||
    candidate.signalPrice;

  const drift =
    pct(
      live -
      candidate.signalPrice,
      candidate.signalPrice
    );

  if (
    drift >
      C.maxPriceDriftPct ||
    drift >
      C.liveChasePct
  ) {

    return {
      ok: false,
      reason:
        'LIVE_CHASE',
      drift
    };
  }

  return {
    ok: true,
    price:
      live,
    drift
  };
}

function openPaper(candidate) {

  if (
    !C.paperTrading ||
    manualPause ||
    dailyPause ||
    cooldownActive()
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

  const validation =
    candidateValid(
      candidate
    );

  if (!validation.ok) {

    candidatePool.delete(
      candidate.symbol
    );

    return false;
  }

  const allocation =
    paperAllocation();

  if (
    allocation <
      10 ||
    cash <
      10
  ) {
    return false;
  }

  const rawEntry =
    validation.price;

  const entry =
    rawEntry *
    (
      1 +
      C.slippagePct
    );

  const buyFee =
    allocation *
    C.feePct;

  const netInvest =
    allocation -
    buyFee;

  const qty =
    netInvest /
    entry;

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

  const targetPct =
    stopPct *
    C.rewardRisk;

  const riskPrice =
    entry *
    stopPct;

  cash -=
    allocation;

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

    buyFee,

    stopLoss:
      entry *
      (
        1 -
        stopPct
      ),

    takeProfit:
      entry *
      (
        1 +
        targetPct
      ),

    initialStop:
      entry *
      (
        1 -
        stopPct
      ),

    riskPrice,

    highestPrice:
      entry,

    lowestPrice:
      entry,

    lastPrice:
      entry,

    mfePct:
      0,

    maePct:
      0,

    profitLockActive:
      false,

    breakEvenMoved:
      false,

    trailingActive:
      false,

    closing:
      false,

    grade:
      candidate.grade,

    edgeScore:
      candidate.edgeScore,

    freshnessScore:
      candidate.freshnessScore,

    flowScore:
      candidate.flowScore,

    cmo:
      candidate.cmo,

    atrPct:
      candidate.atrPct,

    takerBuyRatio:
      candidate.takerBuyRatio,

    breakoutAcceptance:
      candidate.breakoutAcceptance,

    warnings:
      candidate.warnings,

    reasons:
      candidate.reasons,

    regime:
      candidate.regime,

    signalPrice:
      candidate.signalPrice,

    signalAgeMs:
      Date.now() -
      candidate.createdAt,

    entryDriftPct:
      validation.drift,

    entryTime:
      Date.now()
  };

  entriesSinceCooldown++;

  candidatePool.delete(
    candidate.symbol
  );

  cloudJournal(
    {
      type:
        'ENTRY',

      symbol:
        candidate.symbol,

      entry,
      stopPct,
      targetPct,

      edgeScore:
        candidate.edgeScore,

      grade:
        candidate.grade
    },
    true
  );

  tg(
    `🟢 <b>LOMY V5.1.1 BUY</b>
<b>${candidate.symbol}</b>
Edge: ${candidate.edgeScore}/100 | ${candidate.grade}
Fresh: ${candidate.freshnessScore} | Flow: ${candidate.flowScore}
CMO: ${candidate.cmo.toFixed(1)} | ATR: ${candidate.atrPct.toFixed(2)}%
Entry: ${entry.toFixed(8)}`
  );

  if (
    entriesSinceCooldown >=
    C.entriesBeforeCooldown
  ) {

    startCooldown(
      C.batchCooldownMs,
      'BATCH_LIMIT'
    );
  }

  saveCloudState();

  return true;
}

function closePaper(
  symbol,
  marketPrice,
  reason
) {

  const position =
    positions[
      symbol
    ];

  if (
    !position ||
    position.closing
  ) {
    return;
  }

  position.closing =
    true;

  const exitPrice =
    marketPrice *
    (
      1 -
      C.slippagePct
    );

  const grossValue =
    position.qty *
    exitPrice;

  const sellFee =
    grossValue *
    C.feePct;

  const returned =
    grossValue -
    sellFee;

  const profit =
    returned -
    position.invested;

  const profitPct =
    position.invested
      ? (
          profit /
          position.invested
        ) *
        100
      : 0;

  cash +=
    returned;

  delete positions[
    symbol
  ];

  stats.totalTrades++;

  stats.fees +=
    position.buyFee +
    sellFee;

  stats.netProfit +=
    profit;

  dailyPnL +=
    profit;

  stats.bestTrade =
    stats.totalTrades ===
      1
      ? profit
      : Math.max(
          stats.bestTrade,
          profit
        );

  stats.worstTrade =
    stats.totalTrades ===
      1
      ? profit
      : Math.min(
          stats.worstTrade,
          profit
        );

  if (
    profit >
    0
  ) {

    stats.wins++;

    stats.grossProfit +=
      profit;

    lossStreak =
      0;

  } else {

    stats.losses++;

    stats.grossLoss +=
      Math.abs(
        profit
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

  const record = {

    symbol,

    entryPrice:
      position.entryPrice,

    exitPrice,

    qty:
      position.qty,

    investedUSDT:
      position.invested,

    profit,
    profitPct,
    reason,

    buyFee:
      position.buyFee,

    sellFee,

    totalFees:
      position.buyFee +
      sellFee,

    mfePct:
      position.mfePct,

    maePct:
      position.maePct,

    edgeScore:
      position.edgeScore,

    grade:
      position.grade,

    freshnessScore:
      position.freshnessScore,

    flowScore:
      position.flowScore,

    cmo:
      position.cmo,

    atrPct:
      position.atrPct,

    regime:
      position.regime,

    warnings:
      position.warnings,

    entryTime:
      position.entryTime,

    exitTime:
      Date.now()
  };

  saveTrade(
    record
  );

  cloudJournal(
    {
      type:
        'EXIT',

      ...record
    },
    true
  );

  tg(
    `${profit >= 0 ? '✅' : '🔴'} <b>LOMY V5.1.1 EXIT</b>
<b>${symbol}</b>
Reason: ${reason}
PnL: $${profit.toFixed(2)} (${profitPct.toFixed(2)}%)`
  );

  if (
    lossStreak >=
    C.lossStreakLimit
  ) {

    startCooldown(
      C.lossCooldownMs,
      'LOSS_STREAK'
    );
  }

  updateDrawdown();

  checkDailyLoss();

  saveCloudState();
}

function managePosition(
  symbol,
  price
) {

  const position =
    positions[
      symbol
    ];

  if (
    !position ||
    position.closing ||
    !Number.isFinite(
      price
    ) ||
    price <=
      0
  ) {
    return;
  }

  position.lastPrice =
    price;

  position.highestPrice =
    Math.max(
      position.highestPrice,
      price
    );

  position.lowestPrice =
    Math.min(
      position.lowestPrice,
      price
    );

  position.mfePct =
    Math.max(
      position.mfePct,

      pct(
        position.highestPrice -
        position.entryPrice,
        position.entryPrice
      )
    );

  position.maePct =
    Math.min(
      position.maePct,

      pct(
        position.lowestPrice -
        position.entryPrice,
        position.entryPrice
      )
    );

  const move =
    price -
    position.entryPrice;

  const r =
    position.riskPrice
      ? move /
        position.riskPrice
      : 0;

  if (
    !position.profitLockActive &&
    position.mfePct >=
      C.profitLockMfePct
  ) {

    position.profitLockActive =
      true;

    position.stopLoss =
      Math.max(
        position.stopLoss,

        position.entryPrice *
        (
          1 +
          C.profitLockBufferPct
        )
      );

    stats.profitLocks++;

    cloudJournal(
      {
        type:
          'PROFIT_LOCK',

        symbol,

        mfePct:
          position.mfePct,

        newStop:
          position.stopLoss
      },
      true
    );
  }

  if (
    !position.breakEvenMoved &&
    r >=
      C.breakEvenAtR
  ) {

    position.breakEvenMoved =
      true;

    position.stopLoss =
      Math.max(
        position.stopLoss,

        position.entryPrice *
        (
          1 +
          C.breakEvenBufferPct
        )
      );

    stats.breakEvenMoves++;
  }

  if (
    r >=
    C.trailAtR
  ) {

    if (
      !position.trailingActive
    ) {

      position.trailingActive =
        true;

      stats.trailingActivations++;
    }

    const trailingStop =
      position.entryPrice +
      position.riskPrice *
      C.trailLockR;

    position.stopLoss =
      Math.max(
        position.stopLoss,
        trailingStop
      );
  }

  const age =
    Date.now() -
    position.entryTime;

  const earlyFailPrice =
    position.entryPrice *
    (
      1 -
      C.earlyFailureLossPct
    );

  if (
    age <=
      C.earlyFailureWindowMs &&
    price <=
      earlyFailPrice &&
    position.mfePct <
      C.earlyFailureMfeGuardPct
  ) {

    return closePaper(
      symbol,
      price,
      'EARLY_FAILURE'
    );
  }

  if (
    price <=
    position.stopLoss
  ) {

    return closePaper(
      symbol,
      price,

      position.breakEvenMoved ||
      position.profitLockActive
        ? 'PROTECTED_STOP'
        : 'STOP_LOSS'
    );
  }

  if (
    price >=
    position.takeProfit
  ) {

    return closePaper(
      symbol,
      price,
      'TAKE_PROFIT'
    );
  }
}

// ============================================================
// CANDIDATES
// ============================================================

function addCandidate(analysis) {

  if (
    !analysis?.eligible ||
    positions[
      analysis.symbol
    ] ||
    symbolCooling(
      analysis.symbol
    )
  ) {
    return;
  }

  candidatePool.set(
    analysis.symbol,
    {
      ...analysis,

      createdAt:
        Date.now()
    }
  );
}

function pruneCandidates() {

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
        candidate.createdAt >
        C.candidateExpiryMs ||
      !subscribed.has(
        symbol
      ) ||
      positions[
        symbol
      ]
    ) {

      candidatePool.delete(
        symbol
      );
    }
  }
}

function executeCandidates() {

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

  pruneCandidates();

  const slots =
    C.maxPositions -
    Object.keys(
      positions
    ).length;

  if (
    slots <=
    0
  ) {
    return;
  }

  const candidates =
    [...candidatePool.values()]
      .sort(
        (a, b) =>
          b.edgeScore -
            a.edgeScore ||
          b.freshnessScore -
            a.freshnessScore ||
          b.flowScore -
            a.flowScore
      );

  let opened =
    0;

  for (
    const candidate
    of candidates
  ) {

    if (
      opened >=
      Math.min(
        slots,
        C.maxEntriesPerCycle
      )
    ) {
      break;
    }

    if (
      openPaper(
        candidate
      )
    ) {

      opened++;
    }
  }
}

// ============================================================
// CLOSED CANDLE HANDLING
// ============================================================

function onClosedKline(
  symbol,
  kline
) {

  const candle = {

    open:
      n(kline.o),

    high:
      n(kline.h),

    low:
      n(kline.l),

    close:
      n(kline.c),

    volume:
      n(kline.v),

    closeTime:
      n(kline.T),

    quoteVolume:
      n(kline.q),

    trades:
      n(kline.n),

    takerBuyBase:
      n(kline.V),

    takerBuyQuote:
      n(kline.Q)
  };

  mergeCandles(
    symbol,
    [
      candle
    ]
  );

  if (
    (
      candles[symbol]?.length ||
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

  const analysis =
    analyze(
      candles[
        symbol
      ],
      symbol
    );

  if (!analysis) {
    return;
  }

  cloudJournal({
    type:
      'ANALYSIS',

    symbol,

    decision:
      analysis.eligible
        ? 'ELIGIBLE'
        : 'REJECT',

    edgeScore:
      analysis.edgeScore,

    grade:
      analysis.grade,

    cmo:
      analysis.cmo,

    atrPct:
      analysis.atrPct,

    volumeRatio:
      analysis.volumeRatio,

    blockers:
      analysis.hardBlocks,

    warnings:
      analysis.warnings,

    regime:
      analysis.regime
  });

  if (
    analysis.eligible
  ) {

    addCandidate(
      analysis
    );
  }

  executeCandidates();
}

// ============================================================
// WEBSOCKET
// ============================================================

function wsSend(
  ws,
  object
) {

  if (
    !ws ||
    ws.readyState !==
      WebSocket.OPEN
  ) {
    return false;
  }

  const text =
    JSON.stringify(
      object
    );

  networkMeter.wsControlBytes +=
    byteLen(text);

  ws.send(
    text
  );

  return true;
}

function chunks(
  array,
  size
) {

  const output =
    [];

  for (
    let i = 0;
    i <
      array.length;
    i +=
      size
  ) {

    output.push(
      array.slice(
        i,
        i +
        size
      )
    );
  }

  return output;
}

function connectMiniWs() {

  if (
    shuttingDown
  ) {
    return;
  }

  try {
    miniWs?.terminate();
  } catch {}

  miniWs =
    new WebSocket(
      `${WS_BASE}/ws/!miniTicker@arr`
    );

  miniWs.on(
    'open',
    () => {

      miniConnected =
        true;

      miniReconnectAttempts =
        0;

      console.log(
        'MINI LIVE'
      );
    }
  );

  miniWs.on(
    'message',
    raw => {

      try {

        const data =
          JSON.parse(
            raw.toString()
          );

        if (
          !Array.isArray(
            data
          )
        ) {
          return;
        }

        for (
          const row
          of data
        ) {

          const symbol =
            String(
              row.s ||
              ''
            );

          if (
            !symbol.endsWith(
              'USDT'
            ) ||
            IGNORED.has(
              symbol
            )
          ) {
            continue;
          }

          const price =
            n(
              row.c
            );

          const quoteVolume =
            n(
              row.q
            );

          if (
            price <=
            0
          ) {
            continue;
          }

          tickers.set(
            symbol,
            {
              price,
              quoteVolume,
              time:
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
            );
          }
        }

      } catch (error) {

        console.error(
          'MINI parse:',
          error.message
        );
      }
    }
  );

  miniWs.on(
    'close',
    () => {

      miniConnected =
        false;

      if (
        shuttingDown
      ) {
        return;
      }

      miniReconnectAttempts++;

      const delay =
        Math.min(
          C.wsReconnectMaxMs,

          C.wsReconnectBaseMs *
          (
            2 **
            Math.min(
              miniReconnectAttempts -
              1,
              5
            )
          )
        );

      setTimeout(
        connectMiniWs,
        delay
      );
    }
  );

  miniWs.on(
    'error',
    error =>
      console.error(
        'MINI WS:',
        error.message
      )
  );
}

function connectKlineWs() {

  if (
    shuttingDown
  ) {
    return;
  }

  try {
    klineWs?.terminate();
  } catch {}

  klineWs =
    new WebSocket(
      `${WS_BASE}/ws`
    );

  klineWs.on(
    'open',
    async () => {

      klineConnected =
        true;

      klineReconnectAttempts =
        0;

      console.log(
        'KLINE LIVE'
      );

      const params =
        [...subscribed]
          .map(
            symbol =>
              `${symbol.toLowerCase()}@kline_${C.interval}`
          );

      let id =
        1;

      for (
        const group
        of chunks(
          params,
          C.controlChunkSize
        )
      ) {

        wsSend(
          klineWs,
          {
            method:
              'SUBSCRIBE',

            params:
              group,

            id:
              id++
          }
        );

        await sleep(
          250
        );
      }
    }
  );

  klineWs.on(
    'message',
    raw => {

      try {

        const data =
          JSON.parse(
            raw.toString()
          );

        const payload =
          data.data ||
          data;

        if (
          !payload?.k ||
          !payload.s
        ) {
          return;
        }

        if (
          payload.k.x
        ) {

          onClosedKline(
            payload.s,
            payload.k
          );
        }

      } catch (error) {

        console.error(
          'KLINE parse:',
          error.message
        );
      }
    }
  );

  klineWs.on(
    'close',
    () => {

      klineConnected =
        false;

      if (
        shuttingDown
      ) {
        return;
      }

      klineReconnectAttempts++;

      const delay =
        Math.min(
          C.wsReconnectMaxMs,

          C.wsReconnectBaseMs *
          (
            2 **
            Math.min(
              klineReconnectAttempts -
              1,
              5
            )
          )
        );

      setTimeout(
        connectKlineWs,
        delay
      );
    }
  );

  klineWs.on(
    'error',
    error =>
      console.error(
        'KLINE WS:',
        error.message
      )
  );
}

// ============================================================
// UNIVERSE
// ============================================================

async function refreshUniverse() {

  try {

    const url =
      `${REST_BASE}/api/v3/ticker/24hr`;

    networkMeter.restRequestBytes +=
      byteLen(url);

    const response =
      await axios.get(
        url,
        {
          timeout:
            15000
        }
      );

    const rows =
      Array.isArray(
        response.data
      )
        ? response.data
        : [];

    const ranked =
      rows

        .filter(
          row =>
            String(
              row.symbol ||
              ''
            ).endsWith(
              'USDT'
            )
        )

        .filter(
          row =>
            !IGNORED.has(
              row.symbol
            )
        )

        .filter(
          row =>
            n(
              row.quoteVolume
            ) >=
            C.minQuoteVolume
        )

        .sort(
          (a, b) =>
            n(
              b.quoteVolume
            ) -
            n(
              a.quoteVolume
            )
        )

        .slice(
          0,
          C.universeSize
        )

        .map(
          row =>
            row.symbol
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

    const next =
      new Set(
        ranked.slice(
          0,
          C.universeSize
        )
      );

    const changed =
      next.size !==
        subscribed.size ||
      [...next].some(
        symbol =>
          !subscribed.has(
            symbol
          )
      );

    subscribed =
      next;

    for (
      const symbol
      of subscribed
    ) {

      queueWarmup(
        symbol
      );
    }

    if (
      changed &&
      klineConnected
    ) {

      connectKlineWs();
    }

    console.log(
      `UNIVERSE ${subscribed.size} | READY ${readyCount()}`
    );

  } catch (error) {

    console.error(
      'Universe:',
      error.response?.status ||
      error.message
    );
  }
}

function readyCount() {

  let count =
    0;

  for (
    const symbol
    of subscribed
  ) {

    if (
      (
        candles[symbol]?.length ||
        0
      ) >=
      C.warmupCandles
    ) {

      count++;
    }
  }

  return count;
}

function buildLatest() {

  const rows =
    [];

  for (
    const symbol
    of subscribed
  ) {

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

    const analysis =
      analyze(
        arr,
        symbol
      );

    if (!analysis) {
      continue;
    }

    rows.push(
      analysis
    );
  }

  latest =
    rows
      .sort(
        (a, b) =>
          b.edgeScore -
          a.edgeScore
      )
      .slice(
        0,
        50
      );
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
      ok: true,
      version:
        C.version,

      paperTrading:
        C.paperTrading,

      cloudConnected,
      miniConnected,
      klineConnected,

      ready:
        readyCount(),

      symbols:
        subscribed.size
    });
  }
);

app.get(
  '/api/data',
  (
    req,
    res
  ) => {

    const grossLoss =
      stats.grossLoss ||
      0;

    const winRate =
      stats.totalTrades
        ? (
            stats.wins /
            stats.totalTrades
          ) *
          100
        : 0;

    const profitFactor =
      grossLoss >
        0
        ? stats.grossProfit /
          grossLoss
        : stats.grossProfit >
            0
          ? 999
          : 0;

    res.json({

      version:
        C.version,

      paperTrading:
        C.paperTrading,

      cash:
        +cash.toFixed(
          2
        ),

      equity:
        +equity().toFixed(
          2
        ),

      positions,
      stats,

      winRate:
        +winRate.toFixed(
          2
        ),

      profitFactor:
        +profitFactor.toFixed(
          2
        ),

      dailyPnL:
        +dailyPnL.toFixed(
          2
        ),

      manualPause,
      dailyPause,

      cooldown:
        cooldownActive(),

      cooldownReason,

      cooldownMinutes:
        cooldownUntil
          ? Math.max(
              0,

              Math.ceil(
                (
                  cooldownUntil -
                  Date.now()
                ) /
                60000
              )
            )
          : 0,

      cloudConnected,
      miniConnected,
      klineConnected,

      marketRegime,

      ready:
        readyCount(),

      symbols:
        subscribed.size,

      pool:
        candidatePool.size,

      latest:
        latest.map(
          analysis => ({

            symbol:
              analysis.symbol,

            grade:
              analysis.grade,

            edge:
              analysis.edgeScore,

            fresh:
              analysis.freshnessScore,

            flow:
              analysis.flowScore,

            decision:
              analysis.eligible
                ? 'ELIGIBLE'
                : 'REJECT',

            cmo:
              +analysis.cmo.toFixed(
                1
              ),

            atr:
              +analysis.atrPct.toFixed(
                2
              ),

            volume:
              +analysis.volumeRatio.toFixed(
                2
              ),

            volAccel:
              +analysis.volumeAcceleration.toFixed(
                2
              ),

            buyFlow:
              analysis.takerBuyRatio ===
                null
                ? 'N/A'
                : +(
                    analysis.takerBuyRatio *
                    100
                  ).toFixed(
                    1
                  ),

            accept:
              analysis.breakoutAcceptance,

            shadow:
              analysis.shadowEdge
                ? 'YES'
                : 'NO',

            regime:
              analysis.regime,

            warnings:
              [
                ...analysis.hardBlocks,
                ...analysis.warnings
              ].join(
                ', '
              )
          })
        ),

      warmupStats,

      network:
        networkSnapshot()
    });
  }
);

app.post(
  '/api/pause',
  (
    req,
    res
  ) => {

    manualPause =
      true;

    candidatePool.clear();

    saveCloudState();

    res.json({
      ok: true,
      paused: true
    });
  }
);

app.post(
  '/api/resume',
  (
    req,
    res
  ) => {

    manualPause =
      false;

    saveCloudState();

    res.json({
      ok: true,
      paused: false
    });
  }
);

app.post(
  '/api/emergency-close',
  (
    req,
    res
  ) => {

    const symbols =
      Object.keys(
        positions
      );

    for (
      const symbol
      of symbols
    ) {

      const price =
        tickers.get(
          symbol
        )?.price ||
        positions[
          symbol
        ].lastPrice ||
        positions[
          symbol
        ].entryPrice;

      closePaper(
        symbol,
        price,
        'EMERGENCY_CLOSE'
      );
    }

    res.json({
      ok: true,

      closed:
        symbols.length
    });
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

    res
      .type(
        'html'
      )
      .send(
`<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta
  name="viewport"
  content="width=device-width,initial-scale=1"
>
<title>
LOMY V5.1.1
</title>

<style>
body {
  font-family: Arial, sans-serif;
  background: #0b1020;
  color: #eef2ff;
  margin: 0;
  padding: 20px;
}

h1 {
  margin: 0 0 8px;
}

.banner,
.status {
  padding: 10px 12px;
  border-radius: 10px;
  background: #151d33;
  margin: 8px 0;
}

.grid {
  display: grid;
  grid-template-columns:
    repeat(
      auto-fit,
      minmax(
        130px,
        1fr
      )
    );
  gap: 8px;
  margin: 14px 0;
}

.card {
  background: #151d33;
  padding: 12px;
  border-radius: 10px;
}

.label {
  font-size: 12px;
  opacity: .7;
}

.value {
  font-size: 22px;
  font-weight: bold;
  margin-top: 5px;
}

button {
  padding: 10px 14px;
  border: 0;
  border-radius: 8px;
  margin-right: 6px;
  cursor: pointer;
}

table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 15px;
  font-size: 12px;
}

th,
td {
  padding: 7px;
  border-bottom:
    1px solid #26324f;
  text-align: left;
  white-space: nowrap;
}

.green {
  color: #4ade80;
}

.red {
  color: #fb7185;
}

.yellow {
  color: #facc15;
}
</style>
</head>

<body>

<h1>
🤖 LOMY V5.1.1 WARMUP GUARD
</h1>

<div class="banner">
PAPER ONLY • FRESH MOMENTUM • FLOW QUALITY • ANTI-CHASE • BREAKOUT ACCEPTANCE
</div>

<div
  id="cloud"
  class="status"
>
Cloud...
</div>

<div
  id="ws"
  class="status"
>
Market...
</div>

<div
  id="regime"
  class="status"
>
Regime...
</div>

<div
  id="cooldown"
  class="status"
>
Cooldown...
</div>

<div
  class="grid"
  id="cards"
></div>

<button
  onclick="post('/api/pause')"
>
⏸ PAUSE
</button>

<button
  onclick="post('/api/resume')"
>
▶ RESUME
</button>

<button
  onclick="closeAll()"
>
🚨 CLOSE ALL
</button>

<div style="overflow-x:auto">

<table>

<thead>
<tr>
<th>Symbol</th>
<th>Grade</th>
<th>Edge</th>
<th>Fresh</th>
<th>Flow</th>
<th>Status</th>
<th>CMO</th>
<th>ATR%</th>
<th>Volume</th>
<th>VolAccel</th>
<th>BuyFlow</th>
<th>Acceptance</th>
<th>Shadow</th>
<th>Regime</th>
<th>Warnings</th>
</tr>
</thead>

<tbody id="rows">

<tr>
<td colspan="15">
Loading...
</td>
</tr>

</tbody>

</table>

</div>

<script>

const defs = [
  ['cash', 'CASH'],
  ['equity', 'EQUITY'],
  ['closed', 'CLOSED'],
  ['win', 'WIN RATE'],
  ['profit', 'NET PROFIT'],
  ['pf', 'PROFIT FACTOR'],
  ['open', 'OPEN'],
  ['ready', 'READY'],
  ['symbols', 'WS SYMBOLS'],
  ['breadth', 'BREADTH'],
  ['pool', 'CANDIDATE POOL'],
  ['daily', 'TODAY PNL'],
  ['dd', 'MAX DD'],
  ['early', 'EARLY EXITS'],
  ['be', 'BREAK EVEN'],
  ['trail', 'TRAILING'],
  ['locks', 'PROFIT LOCKS'],
  ['netmb', 'EST. OUT MB']
];

document
  .getElementById(
    'cards'
  )
  .innerHTML =
    defs
      .map(
        function (item) {

          return (
            '<div class="card">' +
            '<div class="label">' +
            item[1] +
            '</div>' +
            '<div class="value" id="' +
            item[0] +
            '">' +
            '0' +
            '</div>' +
            '</div>'
          );
        }
      )
      .join('');

async function post(url) {

  await fetch(
    url,
    {
      method:
        'POST'
    }
  );

  await load();
}

async function closeAll() {

  if (
    confirm(
      'Close all PAPER positions?'
    )
  ) {

    await post(
      '/api/emergency-close'
    );
  }
}

async function load() {

  try {

    const response =
      await fetch(
        '/api/data'
      );

    const d =
      await response.json();

    const set =
      function (
        id,
        value
      ) {

        document
          .getElementById(
            id
          )
          .innerText =
            value;
      };

    set(
      'cash',
      '$' +
      d.cash
    );

    set(
      'equity',
      '$' +
      d.equity
    );

    set(
      'closed',
      d.stats.totalTrades
    );

    set(
      'win',
      d.winRate +
      '%'
    );

    set(
      'profit',
      '$' +
      d.stats.netProfit.toFixed(
        2
      )
    );

    set(
      'pf',
      d.profitFactor
    );

    set(
      'open',
      Object.keys(
        d.positions
      ).length
    );

    set(
      'ready',
      d.ready
    );

    set(
      'symbols',
      d.symbols
    );

    set(
      'breadth',
      d.marketRegime.breadth +
      '%'
    );

    set(
      'pool',
      d.pool
    );

    set(
      'daily',
      '$' +
      d.dailyPnL
    );

    set(
      'dd',
      d.stats.maxDrawdown.toFixed(
        2
      ) +
      '%'
    );

    set(
      'early',
      d.stats.earlyFailureExits
    );

    set(
      'be',
      d.stats.breakEvenMoves
    );

    set(
      'trail',
      d.stats.trailingActivations
    );

    set(
      'locks',
      d.stats.profitLocks
    );

    set(
      'netmb',
      d.network.estimatedOutboundMB +
      ' MB'
    );

    const cloud =
      document.getElementById(
        'cloud'
      );

    cloud.innerText =
      d.cloudConnected
        ? '🟢 CLOUD CONNECTED'
        : '🔴 CLOUD OFF';

    cloud.className =
      'status ' +
      (
        d.cloudConnected
          ? 'green'
          : 'red'
      );

    const ws =
      document.getElementById(
        'ws'
      );

    ws.innerText =
      (
        d.miniConnected &&
        d.klineConnected
      )
        ? '🟢 MARKET WEBSOCKETS LIVE'
        : '🔴 MARKET CONNECTING';

    ws.className =
      'status ' +
      (
        d.miniConnected &&
        d.klineConnected
          ? 'green'
          : 'red'
      );

    const regime =
      document.getElementById(
        'regime'
      );

    regime.innerText =
      'MARKET REGIME: ' +
      d.marketRegime.regime +
      ' • BREADTH ' +
      d.marketRegime.breadth +
      '% • BTC FRESH ' +
      d.marketRegime.btcFreshness;

    regime.className =
      'status ' +
      (
        d.marketRegime.regime ===
          'RISK_ON'
          ? 'green'
          : d.marketRegime.regime ===
              'NEUTRAL'
            ? 'yellow'
            : 'red'
      );

    const cooldown =
      document.getElementById(
        'cooldown'
      );

    cooldown.innerText =
      d.cooldown
        ? (
            '🧠 COOLDOWN ' +
            d.cooldownReason +
            ' • ' +
            d.cooldownMinutes +
            ' MIN'
          )
        : '✅ SMART COOLDOWN READY';

    cooldown.className =
      'status ' +
      (
        d.cooldown
          ? 'yellow'
          : 'green'
      );

    const rows =
      document.getElementById(
        'rows'
      );

    rows.innerHTML =
      '';

    if (
      !d.latest.length
    ) {

      rows.innerHTML =
        '<tr><td colspan="15">' +
        'Warmup ' +
        d.ready +
        ' / ' +
        d.symbols +
        '</td></tr>';

      return;
    }

    d.latest.forEach(
      function (item) {

        rows.innerHTML +=
          '<tr>' +

          '<td><b>' +
          item.symbol +
          '</b></td>' +

          '<td>' +
          item.grade +
          '</td>' +

          '<td>' +
          item.edge +
          '</td>' +

          '<td>' +
          item.fresh +
          '</td>' +

          '<td>' +
          item.flow +
          '</td>' +

          '<td>' +
          item.decision +
          '</td>' +

          '<td>' +
          item.cmo +
          '</td>' +

          '<td>' +
          item.atr +
          '</td>' +

          '<td>' +
          item.volume +
          'x</td>' +

          '<td>' +
          item.volAccel +
          'x</td>' +

          '<td>' +
          item.buyFlow +
          (
            item.buyFlow ===
              'N/A'
              ? ''
              : '%'
          ) +
          '</td>' +

          '<td>' +
          item.accept +
          '</td>' +

          '<td>' +
          item.shadow +
          '</td>' +

          '<td>' +
          item.regime +
          '</td>' +

          '<td>' +
          item.warnings +
          '</td>' +

          '</tr>';
      }
    );

  } catch (error) {

    console.error(
      error
    );
  }
}

setInterval(
  load,
  3000
);

load();

</script>

</body>
</html>`
      );
  }
);

// ============================================================
// SHUTDOWN
// ============================================================

async function shutdown(signal) {

  if (
    shuttingDown
  ) {
    return;
  }

  shuttingDown =
    true;

  console.log(
    `${signal} - saving V5.1.1`
  );

  try {
    await saveCloudState();
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
  'unhandledRejection',
  error =>
    console.error(
      'UNHANDLED REJECTION:',
      error
    )
);

process.on(
  'uncaughtException',
  error =>
    console.error(
      'UNCAUGHT EXCEPTION:',
      error
    )
);

// ============================================================
// START
// ============================================================

async function start() {

  console.log(
    `LOMY ${C.version} | PAPER ONLY`
  );

  await connectCloud();

  await loadCloudState();

  connectMiniWs();

  await refreshUniverse();

  connectKlineWs();

  calculateMarketRegime();

  buildLatest();

  setInterval(
    checkDay,
    30000
  );

  setInterval(
    () => {

      calculateMarketRegime();

      updateDrawdown();

    },
    C.regimeRefreshMs
  );

  setInterval(
    buildLatest,
    C.rankRefreshMs
  );

  setInterval(
    refreshUniverse,
    C.universeRefreshMs
  );

  setInterval(
    saveCloudState,
    C.stateSaveMs
  );

  setInterval(
    () => {

      const snapshot =
        networkSnapshot();

      console.log(
        `NETWORK ~${snapshot.estimatedOutboundMB} MB | REST ${snapshot.restRequestMB} | WS CTRL ${snapshot.wsControlMB} | MONGO ${snapshot.mongoWriteMB}`
      );

      if (
        snapshot.estimatedOutboundMB -
          networkMeter.lastAlertMB >=
        C.networkAlertMB
      ) {

        networkMeter.lastAlertMB =
          snapshot.estimatedOutboundMB;
      }

    },
    C.networkReportMs
  );

  app.listen(
    PORT,
    '0.0.0.0',
    () => {

      console.log(
        `HTTP LIVE :${PORT}`
      );

      tg(
        `🤖 <b>LOMY ${C.version}</b>
PAPER ONLY
Server started.`
      );
    }
  );
}

start()
  .catch(
    error => {

      console.error(
        'STARTUP FATAL:',
        error
      );

      process.exit(1);
    }
  );
