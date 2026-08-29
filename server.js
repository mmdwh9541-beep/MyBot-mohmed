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
// LOMY V5.0 ENTRY INTELLIGENCE - PAPER ONLY
// ============================================================
const C = {
  version: '5.0-ENTRY-INTELLIGENCE',
  stateKey: 'main-v50',
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

  warmupConcurrency: 4,
  warmupDelayMs: 250,
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

// ============================================================
// MARKET MEMORY
// ============================================================
const tickers = new Map();

const candles = {};

const lastAnalyzed = {};

let subscribed = new Set();

const candidatePool = new Map();

let latest = [];

let rankTimer = null;

// ============================================================
// WEBSOCKET
// ============================================================
let miniWs = null;
let klineWs = null;

let miniConnected = false;
let klineConnected = false;

let lastMiniMessage = 0;
let lastKlineMessage = 0;

let shuttingDown = false;

// ============================================================
// DATABASE
// ============================================================
let mongoClient = null;
let db = null;

let cloudConnected = false;

// ============================================================
// WARMUP
// ============================================================
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

// ============================================================
// MARKET REGIME
// ============================================================
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
// HELPERS
// ============================================================
const sleep = ms =>
  new Promise(resolve =>
    setTimeout(resolve, ms)
  );

function n(value, fallback = 0) {
  const number = Number(value);

  return Number.isFinite(number)
    ? number
    : fallback;
}

function clamp(value, min, max) {
  return Math.max(
    min,
    Math.min(max, value)
  );
}

function pct(diff, base) {
  return base
    ? (diff / base) * 100
    : 0;
}

function utcDay() {
  return new Date()
    .toISOString()
    .slice(0, 10);
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
  const hour = new Date().getUTCHours();

  if (hour < 7) {
    return 'ASIA';
  }

  if (hour < 13) {
    return 'LONDON';
  }

  if (hour < 16) {
    return 'LONDON_NY';
  }

  if (hour < 21) {
    return 'NEW_YORK';
  }

  return 'LATE_US';
}

function readyCount() {
  let count = 0;

  for (const symbol of subscribed) {
    if (
      (candles[symbol]?.length || 0) >=
      C.warmupCandles
    ) {
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
  if (
    TELEGRAM_TOKEN &&
    CHAT_ID
  ) {
    tgQueue.push(text);
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
      await axios.post(
        `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
        {
          chat_id: CHAT_ID,
          text,
          parse_mode: 'HTML'
        },
        {
          timeout: 10000
        }
      );
    } catch (error) {
      if (
        error.response?.status === 429
      ) {
        tgQueue.unshift(text);

        await sleep(5000);
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
        maxPoolSize: 10
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
    db.collection('candles')
      .createIndex(
        {
          symbol: 1
        },
        {
          unique: true
        }
      ),

    db.collection('trades')
      .createIndex({
        version: 1,
        exitTime: -1
      }),

    db.collection('journal')
      .createIndex({
        version: 1,
        time: -1
      }),

    db.collection('journal')
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
        _id: C.stateKey
      });

  if (!state) {
    console.log(
      'Fresh V5 PAPER account.'
    );

    return;
  }

  cash =
    n(
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
    n(
      state.entriesSinceCooldown
    );

  lossStreak =
    n(state.lossStreak);

  Object.assign(
    lastLossBySymbol,
    state.lastLossBySymbol || {}
  );

  console.log(
    `V5 STATE RESTORED | Cash $${cash.toFixed(
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
    await db
      .collection('state')
      .updateOne(
        {
          _id: C.stateKey
        },
        {
          $set: {
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
          }
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
    await db
      .collection('journal')
      .insertOne({
        time: Date.now(),
        version: C.version,
        ...row
      });
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
    await db
      .collection('trades')
      .insertOne({
        version: C.version,
        ...record
      });
  } catch (error) {
    console.error(
      'Trade save:',
      error.message
    );
  }
}

// ============================================================
// CANDLE CACHE / WARMUP
// ============================================================
async function loadCachedCandles(
  symbol
) {
  if (!cloudConnected) {
    return false;
  }

  try {
    const doc =
      await db
        .collection('candles')
        .findOne({
          symbol
        });

    if (
      !doc ||
      !Array.isArray(
        doc.candles
      ) ||
      doc.candles.length <
        C.warmupCandles
    ) {
      return false;
    }

    const arr =
      doc.candles
        .map(x => ({
          open:
            n(x.open),

          high:
            n(x.high),

          low:
            n(x.low),

          close:
            n(x.close),

          volume:
            n(x.volume),

          quoteVolume:
            n(
              x.quoteVolume
            ),

          trades:
            n(x.trades),

          takerBuyBase:
            n(
              x.takerBuyBase
            ),

          takerBuyQuote:
            n(
              x.takerBuyQuote
            ),

          closeTime:
            n(x.closeTime)
        }))
        .sort(
          (a, b) =>
            a.closeTime -
            b.closeTime
        )
        .slice(
          -C.maxCandles
        );

    candles[symbol] =
      arr;

    const newest =
      arr[
        arr.length - 1
      ]?.closeTime || 0;

    if (
      Date.now() -
        newest >
      15 * 60 * 1000
    ) {
      return false;
    }

    warmupLoaded.add(
      symbol
    );

    warmupStats.cloudLoaded++;

    return true;
  } catch (error) {
    console.error(
      `Cloud candle ${symbol}:`,
      error.message
    );

    return false;
  }
}

async function saveSymbolCandles(
  symbol
) {
  if (
    !cloudConnected ||
    !candles[symbol]?.length
  ) {
    return;
  }

  try {
    await db
      .collection('candles')
      .updateOne(
        {
          symbol
        },
        {
          $set: {
            symbol,

            interval:
              C.interval,

            updatedAt:
              Date.now(),

            candles:
              candles[
                symbol
              ].slice(
                -C.maxCandles
              )
          }
        },
        {
          upsert: true
        }
      );
  } catch (error) {
    console.error(
      `Save candles ${symbol}:`,
      error.message
    );
  }
}

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

async function fetchWarmup(
  symbol
) {
  if (
    warmupLoaded.has(
      symbol
    ) ||
    warmupLoading.has(
      symbol
    )
  ) {
    return;
  }

  warmupLoading.add(
    symbol
  );

  try {
    if (
      await loadCachedCandles(
        symbol
      )
    ) {
      return;
    }

    warmupStats.restRequests++;

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

          timeout: 12000
        }
      );

    const now =
      Date.now();

    mergeCandles(
      symbol,

      response.data
        .map(parseKline)
        .filter(
          candle =>
            candle.closeTime <
            now
        )
    );

    if (
      (
        candles[
          symbol
        ]?.length ||
        0
      ) >=
      C.warmupCandles
    ) {
      warmupLoaded.add(
        symbol
      );

      warmupStats.restLoaded++;

      await saveSymbolCandles(
        symbol
      );
    }
  } catch (error) {
    warmupStats.failed++;

    console.error(
      `Warmup ${symbol}:`,
      error.response?.status ||
        error.message
    );
  } finally {
    warmupLoading.delete(
      symbol
    );
  }
}

function queueWarmup(
  symbol
) {
  if (
    warmupLoaded.has(
      symbol
    ) ||
    warmupLoading.has(
      symbol
    ) ||
    warmupQueue.includes(
      symbol
    )
  ) {
    return;
  }

  warmupQueue.push(
    symbol
  );

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
// RISK / EQUITY
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
        ) * 100
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

  dailyPnL = 0;

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
      `🛑 <b>LOMY V5 DAILY PROTECTION</b>\nPnL: $${dailyPnL.toFixed(
        2
      )}`
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
    cooldownUntil = 0;

    cooldownReason =
      null;

    entriesSinceCooldown = 0;

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
    Date.now() + ms;

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
    `⏸ <b>LOMY V5 COOLDOWN</b>\n${reason}\n${Math.ceil(
      ms / 60000
    )} minutes`
  );

  saveCloudState();
}

function symbolCooling(
  symbol
) {
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
          n(item[key]),
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
          n(item[key]),
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
    let i = period;
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
    period + 1
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
        i - 1
      ].close;

    if (
      diff > 0
    ) {
      up += diff;
    } else {
      down +=
        Math.abs(diff);
    }
  }

  return up + down
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
    period + 1
  ) {
    return null;
  }

  const ranges = [];

  for (
    let i = 1;
    i <
    arr.length;
    i++
  ) {
    const current =
      arr[i];

    const previous =
      arr[i - 1];

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

function structure(
  arr
) {
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

function volumeAcceleration(
  arr
) {
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

  return previous > 0
    ? recent /
        previous
    : 0;
}

function priceAcceleration(
  arr
) {
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
      a - b,
      b
    ) -
    pct(
      b - c,
      c
    )
  );
}

function takerBuyRatio(
  candle
) {
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

function orderFlowMomentum(
  arr
) {
  let buy = 0;
  let total = 0;
  let valid = 0;

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
    total > 0
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
// V5 BREAKOUT ACCEPTANCE
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
        atrPct * 1.4
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

  return {
    score:
      clamp(
        score,
        0,
        100
      ),

    reasons,

    breakoutPct,

    accepted:
      score >=
      C.minBreakoutAcceptance
  };
}

// ============================================================
// V5 MARKET REGIME
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

    cmo:
      momentum
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
        ) * 100
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
      ready >= 20,

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

    regime,

    overextended,

    updatedAt:
      Date.now()
  };
}

// ============================================================
// V5 SCORE
// ============================================================
function buildV5Score({
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
}) {
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
    priceAcceleration(
      arr
    ) >
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
    regimeScore = 85;
  } else if (
    marketRegime.regime ===
    'NEUTRAL'
  ) {
    regimeScore = 70;
  } else {
    regimeScore = 35;
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
      trend * 0.15 +
      freshness * 0.22 +
      flow * 0.20 +
      momentumScore * 0.15 +
      volatility * 0.10 +
      structureScore * 0.10 +
      regimeScore * 0.08
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

// ============================================================
// MAIN ANALYSIS
// ============================================================
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
    range <= 0
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

  const priceAccel =
    priceAcceleration(
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

  const v5 =
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

  const hardBlocks =
    [];

  if (
    !bullish
  ) {
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
    v5.freshnessScore <
    C.minFreshnessScore
  ) {
    hardBlocks.push(
      'STALE_MOMENTUM'
    );
  }

  if (
    v5.flowScore <
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
    v5.edgeScore <
    threshold
  ) {
    hardBlocks.push(
      'EDGE_SCORE_LOW'
    );
  }

  if (
    marketRegime.regime ===
      'DEFENSIVE' &&
    !v5.shadowEdge
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
      v5.shadowEdge
    )
  ) {
    hardBlocks.push(
      'NO_BREAKOUT_ACCEPTANCE'
    );
  }

  const eligible =
    hardBlocks.length ===
    0;

  const grade =
    v5.edgeScore >=
      84 &&
    v5.freshnessScore >=
      75 &&
    v5.flowScore >=
      75
      ? 'A'
      : v5.edgeScore >=
          72
        ? 'B'
        : 'C';

  return {
    symbol,

    eligible,

    grade,

    score:
      v5.edgeScore,

    edgeScore:
      v5.edgeScore,

    trendScore:
      v5.trendScore,

    freshnessScore:
      v5.freshnessScore,

    flowScore:
      v5.flowScore,

    momentumScore:
      v5.momentumScore,

    volatilityScore:
      v5.volatilityScore,

    structureScore:
      v5.structureScore,

    regimeScore:
      v5.regimeScore,

    shadowEdge:
      v5.shadowEdge,

    price:
      current.close,

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
      priceAccel,

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

    btcFreshness:
      marketRegime.btcFreshness,

    reasons:
      v5.reasons,

    warnings: [
      ...v5.warnings,
      ...hardBlocks
    ]
  };
}

// ============================================================
// CANDIDATE RANKING
// ============================================================
function candidateRank(
  candidate
) {
  let rank =
    candidate.edgeScore +
    candidate.freshnessScore *
      0.18 +
    candidate.flowScore *
      0.18 +
    candidate.structureScore *
      0.10;

  if (
    candidate.shadowEdge
  ) {
    rank += 10;
  }

  if (
    candidate.breakoutAcceptance >=
    C.eliteBreakoutAcceptance
  ) {
    rank += 5;
  }

  if (
    candidate.cmo >=
      52 &&
    candidate.cmo <=
      62
  ) {
    rank += 4;
  }

  if (
    candidate.takerBuyRatio !==
      null &&
    candidate.takerBuyRatio >=
      0.85
  ) {
    rank += 5;
  }

  if (
    candidate.volumeAcceleration >
    2.5
  ) {
    rank -= 5;
  }

  if (
    candidate.marketOverextended
  ) {
    rank -= 8;
  }

  return rank;
}

// ============================================================
// REVALIDATION
// ============================================================
function revalidateCandidate(
  candidate,
  price
) {
  const blockers = [];

  if (
    !candidate ||
    !price
  ) {
    return {
      valid: false,
      blockers: [
        'MISSING_DATA'
      ],
      drift: 0
    };
  }

  if (
    Date.now() >
    candidate.expiresAt
  ) {
    blockers.push(
      'EXPIRED'
    );
  }

  const drift =
    pct(
      price -
        candidate.signalPrice,

      candidate.signalPrice
    );

  if (
    Math.abs(
      drift
    ) >
    C.maxPriceDriftPct
  ) {
    blockers.push(
      'PRICE_DRIFT'
    );
  }

  if (
    drift >
    C.liveChasePct
  ) {
    blockers.push(
      'LIVE_CHASE'
    );
  }

  if (
    drift <
    -0.28
  ) {
    blockers.push(
      'SIGNAL_FAILURE'
    );
  }

  if (
    symbolCooling(
      candidate.symbol
    )
  ) {
    blockers.push(
      'SYMBOL_COOLDOWN'
    );
  }

  if (
    candidate.freshnessScore <
    C.minFreshnessScore
  ) {
    blockers.push(
      'STALE'
    );
  }

  return {
    valid:
      blockers.length ===
      0,

    blockers,

    drift
  };
}

// ============================================================
// CANDIDATE POOL
// ============================================================
function addCandidate(
  symbol,
  analysis,
  closeTime
) {
  const now =
    Date.now();

  const price =
    tickers.get(
      symbol
    )?.price ||
    analysis.price;

  candidatePool.set(
    symbol,
    {
      ...analysis,

      signalPrice:
        price,

      closeTime,

      createdAt:
        now,

      expiresAt:
        now +
        C.candidateExpiryMs
    }
  );

  cloudJournal({
    type:
      'CANDIDATE',

    symbol,

    edgeScore:
      analysis.edgeScore,

    grade:
      analysis.grade,

    shadowEdge:
      analysis.shadowEdge,

    freshnessScore:
      analysis.freshnessScore,

    flowScore:
      analysis.flowScore,

    cmo:
      analysis.cmo,

    takerBuyRatio:
      analysis.takerBuyRatio,

    atrPct:
      analysis.atrPct,

    volumeRatio:
      analysis.volumeRatio,

    volumeAcceleration:
      analysis.volumeAcceleration,

    breakoutAcceptance:
      analysis.breakoutAcceptance,

    regime:
      analysis.regime
  });

  scheduleRanking();
}

function scheduleRanking() {
  if (
    rankTimer
  ) {
    return;
  }

  rankTimer =
    setTimeout(
      () => {
        rankTimer = null;

        rankAndExecute();
      },
      1200
    );
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

  const freeSlots =
    C.maxPositions -
    Object.keys(
      positions
    ).length;

  if (
    freeSlots <= 0
  ) {
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

    if (
      !price
    ) {
      continue;
    }

    const validation =
      revalidateCandidate(
        candidate,
        price
      );

    if (
      !validation.valid
    ) {
      cloudJournal({
        type:
          'REVALIDATION_REJECT',

        symbol:
          candidate.symbol,

        blockers:
          validation.blockers,

        drift:
          validation.drift,

        edgeScore:
          candidate.edgeScore
      });

      continue;
    }

    if (
      !positions[
        candidate.symbol
      ]
    ) {
      valid.push({
        candidate,
        price,
        rank:
          candidateRank(
            candidate
          )
      });
    }
  }

  candidatePool.clear();

  valid.sort(
    (a, b) =>
      b.rank -
      a.rank
  );

  let opened = 0;

  for (
    const opportunity
    of valid
  ) {
    if (
      opened >=
      Math.min(
        C.maxEntriesPerCycle,
        freeSlots
      )
    ) {
      break;
    }

    if (
      openTrade(
        opportunity.candidate,
        opportunity.price
      )
    ) {
      opened++;
    }
  }
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
    cooldownActive() ||
    positions[
      candidate.symbol
    ] ||
    symbolCooling(
      candidate.symbol
    ) ||
    Object.keys(
      positions
    ).length >=
      C.maxPositions
  ) {
    return false;
  }

  const validation =
    revalidateCandidate(
      candidate,
      marketPrice
    );

  if (
    !validation.valid
  ) {
    return false;
  }

  const entry =
    marketPrice *
    (
      1 +
      C.slippagePct
    );

  const stopPct =
    clamp(
      (
        candidate.atr *
        C.atrStopMultiplier
      ) /
        entry,

      C.minStopPct,

      C.maxStopPct
    );

  const targetPct =
    stopPct *
    C.rewardRisk;

  const allocation =
    Math.min(
      equity() /
        C.maxPositions,

      cash
    );

  if (
    allocation <
    50
  ) {
    return false;
  }

  const buyFee =
    allocation *
    C.feePct;

  const qty =
    (
      allocation -
      buyFee
    ) /
    entry;

  cash -=
    allocation;

  stats.fees +=
    buyFee;

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

    riskPrice:
      entry *
      stopPct,

    lastPrice:
      marketPrice,

    highestPrice:
      marketPrice,

    lowestPrice:
      marketPrice,

    mfePct: 0,

    maePct: 0,

    breakEvenMoved:
      false,

    trailingActive:
      false,

    profitLockActive:
      false,

    edgeScore:
      candidate.edgeScore,

    grade:
      candidate.grade,

    shadowEdge:
      candidate.shadowEdge,

    trendScore:
      candidate.trendScore,

    freshnessScore:
      candidate.freshnessScore,

    flowScore:
      candidate.flowScore,

    momentumScore:
      candidate.momentumScore,

    volatilityScore:
      candidate.volatilityScore,

    structureScore:
      candidate.structureScore,

    regimeScore:
      candidate.regimeScore,

    cmo:
      candidate.cmo,

    volumeRatio:
      candidate.volumeRatio,

    volumeAcceleration:
      candidate.volumeAcceleration,

    priceAcceleration:
      candidate.priceAcceleration,

    takerBuyRatio:
      candidate.takerBuyRatio,

    orderFlowMomentum:
      candidate.orderFlowMomentum,

    emaDistance:
      candidate.emaDistance,

    breakoutPct:
      candidate.breakoutPct,

    breakoutAcceptance:
      candidate.breakoutAcceptance,

    atrPct:
      candidate.atrPct,

    ext5:
      candidate.ext5,

    ext10:
      candidate.ext10,

    resistance:
      candidate.resistance,

    support:
      candidate.support,

    regime:
      candidate.regime,

    breadth:
      candidate.breadth,

    marketOverextended:
      candidate.marketOverextended,

    session:
      sessionUTC(),

    signalPrice:
      candidate.signalPrice,

    signalAgeMs:
      Date.now() -
      candidate.createdAt,

    entryDriftPct:
      validation.drift,

    reasons:
      candidate.reasons,

    warnings:
      candidate.warnings,

    entryTime:
      Date.now()
  };

  entriesSinceCooldown++;

  cloudJournal({
    type: 'ENTRY',

    symbol:
      candidate.symbol,

    edgeScore:
      candidate.edgeScore,

    grade:
      candidate.grade,

    shadowEdge:
      candidate.shadowEdge,

    entry,

    stopPct,

    targetPct,

    freshnessScore:
      candidate.freshnessScore,

    flowScore:
      candidate.flowScore,

    cmo:
      candidate.cmo,

    takerBuyRatio:
      candidate.takerBuyRatio,

    atrPct:
      candidate.atrPct,

    volumeAcceleration:
      candidate.volumeAcceleration,

    breakoutAcceptance:
      candidate.breakoutAcceptance,

    regime:
      candidate.regime
  });

  tg(
    `🟢 <b>LOMY V5 BUY</b>\n<b>${candidate.symbol}</b>\nEdge: ${candidate.edgeScore}/100 | ${candidate.grade}\nFresh: ${candidate.freshnessScore} | Flow: ${candidate.flowScore}\nCMO: ${candidate.cmo.toFixed(
      1
    )} | ATR: ${candidate.atrPct.toFixed(
      2
    )}%\nBuy Flow: ${
      candidate.takerBuyRatio === null
        ? 'N/A'
        : (
            candidate.takerBuyRatio *
            100
          ).toFixed(
            1
          ) + '%'
    }\nAcceptance: ${candidate.breakoutAcceptance}\nShadow Edge: ${
      candidate.shadowEdge
        ? 'YES'
        : 'NO'
    }\nEntry: ${entry.toFixed(
      8
    )}`
  );

  saveCloudState();

  if (
    entriesSinceCooldown >=
    C.entriesBeforeCooldown
  ) {
    startCooldown(
      C.batchCooldownMs,
      'V5_BATCH'
    );
  }

  return true;
}

// ============================================================
// POSITION MANAGEMENT
// ============================================================
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
    position.closing
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

  const movePct =
    pct(
      move,
      position.entryPrice
    );

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

    cloudJournal({
      type:
        'PROFIT_LOCK',

      symbol,

      mfePct:
        position.mfePct,

      newStop:
        position.stopLoss
    });
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

    cloudJournal({
      type:
        'BREAK_EVEN_MOVE',

      symbol,

      r,

      mfePct:
        position.mfePct,

      stop:
        position.stopLoss
    });
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

      cloudJournal({
        type:
          'TRAIL_ACTIVATED',

        symbol,

        r,

        mfePct:
          position.mfePct
      });
    }

    position.stopLoss =
      Math.max(
        position.stopLoss,

        position.highestPrice -
          position.riskPrice *
            C.trailLockR
      );
  }

  if (
    price <=
    position.stopLoss
  ) {
    let reason =
      'STOP_LOSS';

    if (
      position.trailingActive
    ) {
      reason =
        'TRAIL_STOP';
    } else if (
      position.breakEvenMoved
    ) {
      reason =
        'BREAK_EVEN_STOP';
    } else if (
      position.profitLockActive
    ) {
      reason =
        'PROFIT_LOCK_STOP';
    }

    closeTrade(
      symbol,
      price,
      reason
    );

    return;
  }

  if (
    price >=
    position.takeProfit
  ) {
    closeTrade(
      symbol,
      price,
      'TAKE_PROFIT'
    );

    return;
  }

  const age =
    Date.now() -
    position.entryTime;

  const failedBreakout =
    position.resistance
      ? price <
        position.resistance
      : false;

  const neverWorked =
    position.mfePct <
    C.earlyFailureMfeGuardPct;

  if (
    age <=
      C.earlyFailureWindowMs &&
    movePct <=
      -(
        C.earlyFailureLossPct *
        100
      ) &&
    failedBreakout &&
    neverWorked
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

  const exit =
    marketPrice *
    (
      1 -
      C.slippagePct
    );

  const gross =
    position.qty *
    exit;

  const fee =
    gross *
    C.feePct;

  const net =
    gross -
    fee;

  const profit =
    net -
    position.invested;

  const profitPct =
    pct(
      profit,
      position.invested
    );

  cash +=
    net;

  stats.totalTrades++;

  stats.fees +=
    fee;

  stats.netProfit +=
    profit;

  if (
    profit > 0
  ) {
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
      Math.abs(
        profit
      );

    stats.worstTrade =
      Math.min(
        stats.worstTrade,
        profit
      );

    if (
      [
        'STOP_LOSS',
        'EARLY_FAILURE'
      ].includes(
        reason
      )
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

  dailyPnL +=
    profit;

  const record = {
    ...position,

    exitPrice:
      exit,

    profit,

    profitPct,

    reason,

    holdingMinutes:
      (
        Date.now() -
        position.entryTime
      ) /
      60000,

    exitTime:
      Date.now()
  };

  delete record.closing;

  delete positions[
    symbol
  ];

  updateDrawdown();

  checkDailyLoss();

  await saveTrade(
    record
  );

  await cloudJournal({
    type: 'CLOSE',
    ...record
  });

  await saveCloudState();

  tg(
    `${
      profit >= 0
        ? '✅'
        : '❌'
    } <b>LOMY V5 CLOSE</b>\n<b>${symbol}</b>\nReason: ${reason}\nPnL: $${profit.toFixed(
      2
    )} (${profitPct.toFixed(
      2
    )}%)\nMFE: ${position.mfePct.toFixed(
      2
    )}% | MAE: ${position.maePct.toFixed(
      2
    )}%\nEdge: ${position.edgeScore}`
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
    candles[
      symbol
    ];

  if (
    !arr ||
    arr.length <
      C.warmupCandles ||
    lastAnalyzed[
      symbol
    ] ===
      closeTime
  ) {
    return;
  }

  lastAnalyzed[
    symbol
  ] =
    closeTime;

  const analysis =
    analyze(
      arr,
      symbol
    );

  if (
    !analysis
  ) {
    return;
  }

  let decision =
    'REJECT';

  if (
    analysis.eligible &&
    !manualPause &&
    !dailyPause &&
    !cooldownActive() &&
    !positions[
      symbol
    ] &&
    !symbolCooling(
      symbol
    )
  ) {
    addCandidate(
      symbol,
      analysis,
      closeTime
    );

    decision =
      'POOL';
  }

  latest.push({
    symbol,

    grade:
      analysis.grade,

    edgeScore:
      analysis.edgeScore,

    fresh:
      analysis.freshnessScore,

    flow:
      analysis.flowScore,

    cmo:
      analysis.cmo.toFixed(
        1
      ),

    atr:
      analysis.atrPct.toFixed(
        2
      ),

    volume:
      analysis.volumeRatio.toFixed(
        2
      ),

    volAccel:
      analysis.volumeAcceleration.toFixed(
        2
      ),

    buyFlow:
      analysis.takerBuyRatio ===
        null
        ? 'N/A'
        : (
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

    decision,

    regime:
      analysis.regime,

    warnings:
      analysis.warnings.join(
        ', '
      )
  });

  latest =
    latest
      .sort(
        (a, b) =>
          b.edgeScore -
          a.edgeScore
      )
      .slice(
        0,
        80
      );

  cloudJournal({
    type:
      'ANALYSIS',

    symbol,

    decision,

    eligible:
      analysis.eligible,

    grade:
      analysis.grade,

    edgeScore:
      analysis.edgeScore,

    trendScore:
      analysis.trendScore,

    freshnessScore:
      analysis.freshnessScore,

    flowScore:
      analysis.flowScore,

    momentumScore:
      analysis.momentumScore,

    volatilityScore:
      analysis.volatilityScore,

    structureScore:
      analysis.structureScore,

    regimeScore:
      analysis.regimeScore,

    shadowEdge:
      analysis.shadowEdge,

    cmo:
      analysis.cmo,

    atrPct:
      analysis.atrPct,

    volumeRatio:
      analysis.volumeRatio,

    volumeAcceleration:
      analysis.volumeAcceleration,

    priceAcceleration:
      analysis.priceAcceleration,

    takerBuyRatio:
      analysis.takerBuyRatio,

    orderFlowMomentum:
      analysis.orderFlowMomentum,

    emaDistance:
      analysis.emaDistance,

    breakoutPct:
      analysis.breakoutPct,

    breakoutAcceptance:
      analysis.breakoutAcceptance,

    ext5:
      analysis.ext5,

    ext10:
      analysis.ext10,

    regime:
      analysis.regime,

    breadth:
      analysis.breadth,

    warnings:
      analysis.warnings
  });
}

// ============================================================
// MINI TICKER WEBSOCKET
// ============================================================
function connectMini() {
  if (
    shuttingDown
  ) {
    return;
  }

  if (
    miniWs &&
    [
      WebSocket.OPEN,
      WebSocket.CONNECTING
    ].includes(
      miniWs.readyState
    )
  ) {
    return;
  }

  miniWs =
    new WebSocket(
      `${WS_BASE}/ws/!miniTicker@arr`
    );

  miniWs.on(
    'open',
    () => {
      miniConnected =
        true;

      lastMiniMessage =
        Date.now();

      console.log(
        'MINI LIVE'
      );
    }
  );

  miniWs.on(
    'message',
    raw => {
      lastMiniMessage =
        Date.now();

      let data;

      try {
        data =
          JSON.parse(
            raw.toString()
          );
      } catch {
        return;
      }

      if (
        !Array.isArray(
          data
        )
      ) {
        return;
      }

      for (
        const item
        of data
      ) {
        const symbol =
          item.s;

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
          n(item.c);

        const quoteVolume =
          n(item.q);

        if (
          price <= 0
        ) {
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

      if (
        !subscribed.size &&
        tickers.size >
          100
      ) {
        setTimeout(
          rebalanceUniverse,
          1500
        );
      }
    }
  );

  miniWs.on(
    'close',
    () => {
      miniConnected =
        false;

      setTimeout(
        connectMini,
        5000
      );
    }
  );

  miniWs.on(
    'error',
    error =>
      console.error(
        'MINI:',
        error.message
      )
  );
}

// ============================================================
// KLINE CONTROL QUEUE
// ============================================================
const controlQueue = [];

let controlBusy =
  false;

function queueControl(
  message
) {
  controlQueue.push(
    message
  );
}

setInterval(
  async () => {
    if (
      controlBusy ||
      !controlQueue.length ||
      !klineWs ||
      klineWs.readyState !==
        WebSocket.OPEN
    ) {
      return;
    }

    controlBusy =
      true;

    const message =
      controlQueue.shift();

    try {
      klineWs.send(
        JSON.stringify(
          message
        )
      );
    } catch {}

    await sleep(
      1000
    );

    controlBusy =
      false;
  },
  250
);

// ============================================================
// KLINE WEBSOCKET
// ============================================================
function connectKline() {
  if (
    shuttingDown
  ) {
    return;
  }

  if (
    klineWs &&
    [
      WebSocket.OPEN,
      WebSocket.CONNECTING
    ].includes(
      klineWs.readyState
    )
  ) {
    return;
  }

  klineWs =
    new WebSocket(
      `${WS_BASE}/ws`
    );

  klineWs.on(
    'open',
    () => {
      klineConnected =
        true;

      lastKlineMessage =
        Date.now();

      console.log(
        'KLINE LIVE'
      );

      if (
        subscribed.size
      ) {
        queueControl({
          method:
            'SUBSCRIBE',

          params:
            [...subscribed]
              .map(
                symbol =>
                  `${symbol.toLowerCase()}@kline_${C.interval}`
              ),

          id:
            Date.now()
        });
      }
    }
  );

  klineWs.on(
    'message',
    raw => {
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
        Object.prototype.hasOwnProperty.call(
          event,
          'result'
        )
      ) {
        return;
      }

      if (
        event.e !==
          'kline' ||
        !event.k ||
        event.k.x !==
          true
      ) {
        return;
      }

      const symbol =
        event.s;

      const kline =
        event.k;

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

        quoteVolume:
          n(kline.q),

        trades:
          n(kline.n),

        takerBuyBase:
          n(kline.V),

        takerBuyQuote:
          n(kline.Q),

        closeTime:
          n(kline.T)
      };

      mergeCandles(
        symbol,
        [
          candle
        ]
      );

      warmupLoaded.add(
        symbol
      );

      analyzeClosed(
        symbol,
        candle.closeTime
      );

      saveSymbolCandles(
        symbol
      );
    }
  );

  klineWs.on(
    'close',
    () => {
      klineConnected =
        false;

      setTimeout(
        connectKline,
        5000
      );
    }
  );

  klineWs.on(
    'error',
    error =>
      console.error(
        'KLINE:',
        error.message
      )
  );
}

// ============================================================
// UNIVERSE
// ============================================================
function topSymbols() {
  let result =
    [...tickers.entries()]
      .filter(
        ([
          symbol,
          ticker
        ]) =>
          symbol.endsWith(
            'USDT'
          ) &&
          !ignored(
            symbol
          ) &&
          ticker.quoteVolume >=
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
      .map(
        item =>
          item[0]
      );

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

  if (
    !wanted.size
  ) {
    return;
  }

  const add = [];
  const remove = [];

  for (
    const symbol
    of wanted
  ) {
    if (
      !subscribed.has(
        symbol
      )
    ) {
      add.push(
        symbol
      );
    }
  }

  for (
    const symbol
    of subscribed
  ) {
    if (
      !wanted.has(
        symbol
      )
    ) {
      if (
        positions[
          symbol
        ]
      ) {
        wanted.add(
          symbol
        );
      } else {
        remove.push(
          symbol
        );
      }
    }
  }

  if (
    remove.length
  ) {
    queueControl({
      method:
        'UNSUBSCRIBE',

      params:
        remove.map(
          symbol =>
            `${symbol.toLowerCase()}@kline_${C.interval}`
        ),

      id:
        Date.now()
    });
  }

  if (
    add.length
  ) {
    for (
      const symbol
      of add
    ) {
      queueWarmup(
        symbol
      );
    }

    queueControl({
      method:
        'SUBSCRIBE',

      params:
        add.map(
          symbol =>
            `${symbol.toLowerCase()}@kline_${C.interval}`
        ),

      id:
        Date.now() +
        1
    });
  }

  subscribed =
    wanted;

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
setInterval(
  () => {
    checkDay();

    cooldownActive();

    updateDrawdown();

    saveCloudState();
  },
  C.stateSaveMs
);

setInterval(
  calculateMarketRegime,
  C.regimeRefreshMs
);

setInterval(
  () => {
    for (
      const symbol
      of subscribed
    ) {
      if (
        candles[
          symbol
        ]?.length
      ) {
        saveSymbolCandles(
          symbol
        );
      }
    }
  },
  C.candleCloudSaveMs
);

// ============================================================
// WATCHDOG
// ============================================================
setInterval(
  () => {
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
        10 *
          60 *
          1000
    ) {
      try {
        klineWs.terminate();
      } catch {}
    }

    if (
      !miniConnected
    ) {
      connectMini();
    }

    if (
      !klineConnected
    ) {
      connectKline();
    }
  },
  30000
);

// ============================================================
// API
// ============================================================
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

    res.json({
      success: true
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

    candidatePool.clear();

    await saveCloudState();

    res.json({
      success: true
    });
  }
);

app.post(
  '/api/emergency-close',
  async (
    req,
    res
  ) => {
    let closed = 0;

    for (
      const symbol
      of Object.keys(
        positions
      )
    ) {
      const price =
        tickers.get(
          symbol
        )?.price ||
        positions[
          symbol
        ].lastPrice;

      if (
        price
      ) {
        await closeTrade(
          symbol,
          price,
          'EMERGENCY_CLOSE'
        );

        closed++;
      }
    }

    res.json({
      success: true,
      closed
    });
  }
);

app.get(
  '/health',
  (
    req,
    res
  ) =>
    res.json({
      status:
        cloudConnected &&
        miniConnected &&
        klineConnected
          ? 'OK'
          : 'DEGRADED',

      version:
        C.version,

      execution:
        'PAPER',

      cloud:
        cloudConnected,

      websocket:
        miniConnected &&
        klineConnected,

      ready:
        readyCount(),

      symbols:
        subscribed.size,

      candidatePool:
        candidatePool.size,

      regime:
        marketRegime,

      equity:
        +equity().toFixed(
          2
        )
    })
);

app.get(
  '/api/data',
  async (
    req,
    res
  ) => {
    const closed =
      n(
        stats.totalTrades
      );

    const winRate =
      closed
        ? (
            stats.wins /
            closed
          ) *
          100
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

    if (
      cloudConnected
    ) {
      try {
        recentTrades =
          await db
            .collection(
              'trades'
            )
            .find({
              version:
                C.version
            })
            .sort({
              exitTime: -1
            })
            .limit(
              C.historyLimitDashboard
            )
            .toArray();

        recentJournal =
          await db
            .collection(
              'journal'
            )
            .find({
              version:
                C.version
            })
            .sort({
              time: -1
            })
            .limit(
              C.journalLimitDashboard
            )
            .toArray();
      } catch (error) {
        console.error(
          'Dashboard DB:',
          error.message
        );
      }
    }

    res.json({
      version:
        C.version,

      cloudConnected,

      miniConnected,

      klineConnected,

      cash:
        cash.toFixed(
          2
        ),

      equity:
        equity().toFixed(
          2
        ),

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

      candidatePool:
        candidatePool.size,

      marketRegime,

      dailyPnL:
        dailyPnL.toFixed(
          2
        ),

      manualPause,

      dailyPause,

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

      entriesBeforeCooldown:
        C.entriesBeforeCooldown,

      maxPositions:
        C.maxPositions,

      maxEntriesPerCycle:
        C.maxEntriesPerCycle,

      lossStreak,

      stats: {
        ...stats,

        totalTrades:
          closed,

        winRate:
          +winRate.toFixed(
            2
          ),

        profitFactor:
          +profitFactor.toFixed(
            2
          )
      },

      latest,

      recentTrades,

      recentJournal
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
  ) =>
    res.send(`
<!doctype html>

<html>

<head>

<meta charset="utf-8">

<meta name="viewport" content="width=device-width,initial-scale=1">

<title>LOMY V5</title>

<style>

* {
  box-sizing: border-box;
}

body {
  margin: 0;
  background: #0b0e11;
  color: #eaecef;
  font-family: Arial;
  padding: 14px;
  text-align: center;
}

h1 {
  color: #f3ba2f;
}

.banner {
  background: #f3ba2f;
  color: #111;
  padding: 11px;
  border-radius: 10px;
  font-weight: bold;
  max-width: 1500px;
  margin: auto;
}

.status {
  margin: 9px;
  font-weight: bold;
}

.green {
  color: #0ecb81;
}

.red {
  color: #f6465d;
}

.yellow {
  color: #f3ba2f;
}

.grid {
  display: grid;
  grid-template-columns: repeat(auto-fit,minmax(125px,1fr));
  gap: 9px;
  max-width: 1500px;
  margin: 16px auto;
}

.card {
  background: #1e2329;
  border: 1px solid #2b3139;
  padding: 12px;
  border-radius: 9px;
}

.label {
  font-size: 10px;
  color: #848e9c;
}

.value {
  font-size: 19px;
  font-weight: bold;
  margin-top: 6px;
}

button {
  border: 0;
  padding: 11px 16px;
  margin: 4px;
  border-radius: 8px;
  font-weight: bold;
  cursor: pointer;
}

.pause {
  background: #f3ba2f;
}

.resume {
  background: #0ecb81;
}

.close {
  background: #f6465d;
  color: white;
}

table {
  width: 100%;
  max-width: 1700px;
  margin: 18px auto;
  border-collapse: collapse;
  background: #1e2329;
}

th {
  background: #2b3139;
  color: #848e9c;
}

td,
th {
  font-size: 10px;
  padding: 7px;
  border-bottom: 1px solid #2b3139;
  white-space: nowrap;
}

</style>

</head>

<body>

<h1>🤖 LOMY V5 ENTRY INTELLIGENCE</h1>

<div class="banner">
FRESH MOMENTUM • FLOW QUALITY • ANTI-CHASE • CLIMAX FILTER • BREAKOUT ACCEPTANCE • PAPER ONLY
</div>

<div id="cloud" class="status">
Cloud...
</div>

<div id="ws" class="status">
Market...
</div>

<div id="regime" class="status">
Regime...
</div>

<div id="cooldown" class="status">
Cooldown...
</div>

<div class="grid">

${[
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
  ['batch', 'BATCH'],
  ['loss', 'LOSS STREAK'],
  ['early', 'EARLY EXITS'],
  ['be', 'BREAK EVEN'],
  ['trail', 'TRAILING'],
  ['locks', 'PROFIT LOCKS']
]
  .map(
    ([id, label]) =>
      `<div class="card">
        <div class="label">${label}</div>
        <div class="value" id="${id}">0</div>
      </div>`
  )
  .join('')}

</div>

<button class="pause" onclick="post('/api/pause')">
⏸ PAUSE
</button>

<button class="resume" onclick="post('/api/resume')">
▶ RESUME
</button>

<button class="close" onclick="closeAll()">
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

async function post(url) {
  await fetch(
    url,
    {
      method: 'POST'
    }
  );

  load();
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

    cash.innerText =
      '$' +
      d.cash;

    equity.innerText =
      '$' +
      d.equity;

    closed.innerText =
      d.stats.totalTrades;

    win.innerText =
      d.stats.winRate +
      '%';

    profit.innerText =
      '$' +
      Number(
        d.stats.netProfit
      ).toFixed(
        2
      );

    pf.innerText =
      d.stats.profitFactor;

    open.innerText =
      d.open +
      '/' +
      d.maxPositions;

    ready.innerText =
      d.ready;

    symbols.innerText =
      d.symbols;

    breadth.innerText =
      d.marketRegime.breadth +
      '%';

    pool.innerText =
      d.candidatePool;

    daily.innerText =
      '$' +
      d.dailyPnL;

    dd.innerText =
      Number(
        d.stats.maxDrawdown
      ).toFixed(
        2
      ) +
      '%';

    batch.innerText =
      d.entriesSinceCooldown +
      '/' +
      d.entriesBeforeCooldown;

    loss.innerText =
      d.lossStreak +
      '/3';

    early.innerText =
      d.stats.earlyFailureExits;

    be.innerText =
      d.stats.breakEvenMoves;

    trail.innerText =
      d.stats.trailingActivations;

    locks.innerText =
      d.stats.profitLocks ||
      0;

    cloud.innerText =
      d.cloudConnected
        ? '☁️ CLOUD DB CONNECTED'
        : '🔴 CLOUD DB DISCONNECTED';

    cloud.className =
      d.cloudConnected
        ? 'status green'
        : 'status red';

    ws.innerText =
      (
        d.miniConnected &&
        d.klineConnected
      )
        ? '🟢 MARKET WEBSOCKETS LIVE'
        : '🔴 MARKET CONNECTING';

    ws.className =
      (
        d.miniConnected &&
        d.klineConnected
      )
        ? 'status green'
        : 'status red';

    regime.innerText =
      'MARKET REGIME: ' +
      d.marketRegime.regime +
      ' • BREADTH ' +
      d.marketRegime.breadth +
      '% • BTC FRESH ' +
      d.marketRegime.btcFreshness +
      ' • OVEREXTENDED ' +
      (
        d.marketRegime.overextended
          ? 'YES'
          : 'NO'
      );

    regime.className =
      d.marketRegime.regime ===
      'RISK_ON'
        ? 'status green'
        : d.marketRegime.regime ===
          'NEUTRAL'
          ? 'status yellow'
          : 'status red';

    cooldown.innerText =
      d.cooldown
        ? '🧠 COOLDOWN ' +
          d.cooldownReason +
          ' • ' +
          d.cooldownMinutes +
          ' MIN'
        : '✅ SMART COOLDOWN READY';

    cooldown.className =
      d.cooldown
        ? 'status yellow'
        : 'status green';

    rows.innerHTML =
      '';

    if (
      !d.latest.length
    ) {
      rows.innerHTML =
        '<tr><td colspan="15">Warmup ' +
        d.ready +
        ' / ' +
        d.symbols +
        '</td></tr>';

      return;
    }

    d.latest.forEach(
      x => {

        rows.innerHTML +=
          '<tr>' +

          '<td><b>' +
          x.symbol +
          '</b></td>' +

          '<td>' +
          x.grade +
          '</td>' +

          '<td>' +
          x.edgeScore +
          '</td>' +

          '<td>' +
          x.fresh +
          '</td>' +

          '<td>' +
          x.flow +
          '</td>' +

          '<td>' +
          x.decision +
          '</td>' +

          '<td>' +
          x.cmo +
          '</td>' +

          '<td>' +
          x.atr +
          '</td>' +

          '<td>' +
          x.volume +
          'x</td>' +

          '<td>' +
          x.volAccel +
          'x</td>' +

          '<td>' +
          x.buyFlow +
          (
            x.buyFlow ===
            'N/A'
              ? ''
              : '%'
          ) +
          '</td>' +

          '<td>' +
          x.accept +
          '</td>' +

          '<td>' +
          x.shadow +
          '</td>' +

          '<td>' +
          x.regime +
          '</td>' +

          '<td>' +
          x.warnings +
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

</html>
`)
);

// ============================================================
// SHUTDOWN
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
    signal,
    '- saving V5'
  );

  try {
    await saveCloudState();

    for (
      const symbol
      of subscribed
    ) {
      if (
        candles[
          symbol
        ]?.length
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
      'UNHANDLED:',
      error
    )
);

process.on(
  'uncaughtException',
  error =>
    console.error(
      'UNCAUGHT:',
      error
    )
);

// ============================================================
// START
// ============================================================
app.listen(
  PORT,
  async () => {

    console.log(
      '\n================================================='
    );

    console.log(
      'LOMY V5.0 ENTRY INTELLIGENCE'
    );

    console.log(
      'PAPER ONLY | Fresh Momentum | Flow Quality'
    );

    console.log(
      'Anti-Chase | Climax Filter | Breakout Acceptance'
    );

    console.log(
      '================================================='
    );

    try {

      await connectCloud();

      await loadCloudState();

      connectMini();

      connectKline();

      calculateMarketRegime();

      tg(
        `🚀 <b>LOMY V5 ENTRY INTELLIGENCE</b>
☁️ Cloud: CONNECTED
📡 WebSocket: ON
🧠 Fresh Entry Engine: ON
🐳 Flow Quality: ON
🛡 Anti-Chase: ON
⚠️ Climax Detection: ON
📈 Breakout Acceptance: ON
🧪 Shadow Edge: ON
💰 PAPER ONLY
Equity: $${equity().toFixed(
          2
        )}`
      );

    } catch (error) {

      console.error(
        'STARTUP FAILED:',
        error
      );

      tg(
        `🔴 <b>LOMY V5 STARTUP FAILED</b>
${error.message}`
      );

    }

  }
);
