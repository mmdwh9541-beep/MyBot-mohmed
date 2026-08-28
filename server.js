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
// LOMY V4.5 HIGH-THROUGHPUT SCALP
// PAPER ONLY
// CLOUD MEMORY + SMART WARMUP + MARKET REGIME + SCALP CONFIRMATION
// ============================================================

const C = {
  version: '4.5-HIGH-THROUGHPUT-SCALP',
  stateKey: 'main-v45',

  paperTrading: true,
  startingBalance: 10000,

  interval: '5m',
  warmupCandles: 60,
  maxCandles: 100,

  universeSize: 300,
  minQuoteVolume: 500000,
  universeRefreshMs: 30 * 60 * 1000,

  // Increased throughput
  maxPositions: 8,
  maxEntriesPerCycle: 3,

  // Main opportunity score
  minScore: 78,

  // Elite opportunities allowed in NEUTRAL market
  neutralMinScore: 88,

  // Scalp confirmation layer
  minScalpScore: 62,
  neutralMinScalpScore: 76,

  requiredGrade: 'B',

  // Freshness
  candidateExpiryMs: 3 * 60 * 1000,
  maxPriceDriftPct: 0.28,

  // Simulated costs
  feePct: 0.001,
  slippagePct: 0.0005,

  // Risk
  minStopPct: 0.006,
  maxStopPct: 0.012,
  atrStopMultiplier: 1.25,
  rewardRisk: 1.9,

  // Earlier profit protection
  breakEvenAtR: 0.72,
  trailAtR: 1.10,
  trailLockR: 0.35,

  // Early failure protection
  earlyFailureWindowMs: 12 * 60 * 1000,
  earlyFailureLossPct: 0.0042,
  earlyFailureMfeGuardPct: 0.35,

  dailyLossLimitPct: 0.04,

  // More trades before batch cooldown
  entriesBeforeCooldown: 16,
  batchCooldownMs: 20 * 60 * 1000,

  lossStreakLimit: 3,
  lossCooldownMs: 60 * 60 * 1000,

  symbolLossCooldownMs: 90 * 60 * 1000,

  // ==========================================================
  // ULTRA-FAST SCALP CONFIRMATION
  // ==========================================================

  scalpCmoLen: 9,
  scalpVolLen: 10,

  scalpBodyMin: 0.50,

  scalpVolumeSpike: 1.30,

  scalpCmoMin: 42,
  scalpCmoHot: 84,

  orderFlowMin: 0.50,

  // Warmup
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

let dailyStartEquity =
  C.startingBalance;

let currentDay =
  utcDay();

let peakEquity =
  C.startingBalance;

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

const tickers =
  new Map();

const candles = {};

const lastAnalyzed = {};

let subscribed =
  new Set();

const candidatePool =
  new Map();

let latest = [];

// ============================================================
// WEBSOCKET STATE
// ============================================================

let miniWs = null;

let klineWs = null;

let miniConnected = false;

let klineConnected = false;

let lastMiniMessage = 0;

let lastKlineMessage = 0;

let shuttingDown = false;

// ============================================================
// CLOUD STATE
// ============================================================

let mongoClient = null;

let db = null;

let cloudConnected = false;

// ============================================================
// WARMUP STATE
// ============================================================

const warmupLoaded =
  new Set();

const warmupLoading =
  new Set();

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

  breadth: 0,

  regime: 'WARMING',

  updatedAt: 0
};

let rankTimer = null;

// ============================================================
// HELPERS
// ============================================================

const sleep = ms =>
  new Promise(
    resolve =>
      setTimeout(
        resolve,
        ms
      )
  );

function n(
  value,
  fallback = 0
) {

  const number =
    Number(value);

  return Number.isFinite(
    number
  )
    ? number
    : fallback;
}

function clamp(
  value,
  min,
  max
) {

  return Math.max(
    min,
    Math.min(
      max,
      value
    )
  );
}

function pct(
  diff,
  base
) {

  return base
    ? (
        diff /
        base
      ) * 100
    : 0;
}

function utcDay() {

  return new Date()
    .toISOString()
    .slice(
      0,
      10
    );
}

// ============================================================
// STABLE / NON-TRADED PAIRS
// ============================================================

function ignored(
  symbol
) {

  return new Set([
    'USDCUSDT',
    'FDUSDUSDT',
    'TUSDUSDT',
    'USDPUSDT',
    'BUSDUSDT',
    'DAIUSDT',
    'USDEUSDT',
    'USD1USDT'
  ]).has(
    symbol
  );
}

// ============================================================
// SESSION
// ============================================================

function sessionUTC() {

  const h =
    new Date()
      .getUTCHours();

  if (h < 7) {
    return 'ASIA';
  }

  if (h < 13) {
    return 'LONDON';
  }

  if (h < 16) {
    return 'LONDON_NY';
  }

  if (h < 21) {
    return 'NEW_YORK';
  }

  return 'LATE_US';
}

// ============================================================
// READY SYMBOL COUNT
// ============================================================

function readyCount() {

  let count = 0;

  for (
    const symbol
    of subscribed
  ) {

    if (
      (
        candles[symbol]
          ?.length ||
        0
      ) >=
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

function tg(
  text
) {

  if (
    !TELEGRAM_TOKEN ||
    !CHAT_ID
  ) {
    return;
  }

  tgQueue.push(
    text
  );
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
          chat_id:
            CHAT_ID,

          text,

          parse_mode:
            'HTML'
        },
        {
          timeout:
            10000
        }
      );

    } catch (error) {

      if (
        error.response
          ?.status ===
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

  if (
    !MONGODB_URI
  ) {

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

  await mongoClient
    .connect();

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
      .collection(
        'candles'
      )
      .createIndex(
        {
          symbol: 1
        },
        {
          unique: true
        }
      ),

    db
      .collection(
        'trades'
      )
      .createIndex({
        exitTime: -1
      }),

    db
      .collection(
        'journal'
      )
      .createIndex({
        time: -1
      }),

    db
      .collection(
        'journal'
      )
      .createIndex({
        symbol: 1,
        time: -1
      })

  ]);

  console.log(
    'MongoDB CLOUD CONNECTED'
  );
}

// ============================================================
// LOAD V4.5 CLOUD STATE
// ============================================================

async function loadCloudState() {

  const state =
    await db
      .collection(
        'state'
      )
      .findOne({
        _id:
          C.stateKey
      });

  if (
    !state
  ) {

    console.log(
      'No V4.5 cloud state. Fresh PAPER account.'
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
    ...(
      state.stats ||
      {}
    )
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
    `CLOUD STATE RESTORED | Cash $${cash.toFixed(2)} | Open ${Object.keys(positions).length}`
  );
}

// ============================================================
// SAVE CLOUD STATE
// ============================================================

async function saveCloudState() {

  if (
    !cloudConnected
  ) {
    return;
  }

  try {

    await db
      .collection(
        'state'
      )
      .updateOne(
        {
          _id:
            C.stateKey
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
      'Cloud state save:',
      error.message
    );
  }
}

// ============================================================
// CLOUD JOURNAL
// ============================================================

async function cloudJournal(
  row
) {

  if (
    !cloudConnected
  ) {
    return;
  }

  try {

    await db
      .collection(
        'journal'
      )
      .insertOne({
        time:
          Date.now(),

        version:
          C.version,

        ...row
      });

  } catch (error) {

    console.error(
      'Journal:',
      error.message
    );
  }
}

// ============================================================
// SAVE CLOSED TRADE
// ============================================================

async function saveTrade(
  record
) {

  if (
    !cloudConnected
  ) {
    return;
  }

  try {

    await db
      .collection(
        'trades'
      )
      .insertOne({

        version:
          C.version,

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
// CLOUD CANDLE CACHE
// ============================================================

async function loadCachedCandles(
  symbol
) {

  if (
    !cloudConnected
  ) {
    return false;
  }

  try {

    const doc =
      await db
        .collection(
          'candles'
        )
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
        .map(
          candle => ({

            open:
              n(
                candle.open
              ),

            high:
              n(
                candle.high
              ),

            low:
              n(
                candle.low
              ),

            close:
              n(
                candle.close
              ),

            volume:
              n(
                candle.volume
              ),

            // New V4.5 fields
            quoteVolume:
              n(
                candle.quoteVolume
              ),

            takerBuyBase:
              n(
                candle.takerBuyBase
              ),

            takerBuyQuote:
              n(
                candle.takerBuyQuote
              ),

            closeTime:
              n(
                candle.closeTime
              )
          })
        )
        .sort(
          (a, b) =>
            a.closeTime -
            b.closeTime
        )
        .slice(
          -C.maxCandles
        );

    const newest =
      arr[
        arr.length - 1
      ]?.closeTime ||
      0;

    const maxAge =
      15 *
      60 *
      1000;

    if (
      Date.now() -
        newest >
      maxAge
    ) {

      candles[
        symbol
      ] = arr;

      return false;
    }

    candles[
      symbol
    ] = arr;

    warmupLoaded.add(
      symbol
    );

    warmupStats
      .cloudLoaded++;

    return true;

  } catch (error) {

    console.error(
      `Cloud candle load ${symbol}:`,
      error.message
    );

    return false;
  }
}
// ============================================================
// SAVE SYMBOL CANDLES
// ============================================================

async function saveSymbolCandles(symbol) {

  if (!cloudConnected) {
    return;
  }

  const arr = candles[symbol];

  if (
    !Array.isArray(arr) ||
    !arr.length
  ) {
    return;
  }

  try {

    await db
      .collection('candles')
      .updateOne(
        { symbol },
        {
          $set: {
            symbol,
            interval: C.interval,
            updatedAt: Date.now(),

            candles:
              arr.slice(
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
      `Cloud candle save ${symbol}:`,
      error.message
    );
  }
}

// ============================================================
// HISTORICAL KLINE PARSER
// Binance REST Kline:
// 0 Open time
// 1 Open
// 2 High
// 3 Low
// 4 Close
// 5 Base volume
// 6 Close time
// 7 Quote volume
// 8 Trades
// 9 Taker buy base
// 10 Taker buy quote
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

// ============================================================
// MERGE CANDLES
// ============================================================

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
    Array
      .from(
        map.values()
      )
      .sort(
        (a, b) =>
          a.closeTime -
          b.closeTime
      )
      .slice(
        -C.maxCandles
      );
}

// ============================================================
// SMART CLOUD / REST WARMUP
// ============================================================

async function fetchWarmup(
  symbol
) {

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

    const fromCloud =
      await loadCachedCandles(
        symbol
      );

    if (fromCloud) {

      console.log(
        `☁️ CLOUD READY ${symbol} | ${candles[symbol].length}`
      );

      return;
    }

    warmupStats
      .restRequests++;

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

    const historical =
      response.data
        .map(
          parseKline
        )
        .filter(
          candle =>
            candle.closeTime <
            now
        );

    mergeCandles(
      symbol,
      historical
    );

    if (
      (
        candles[symbol]
          ?.length ||
        0
      ) >=
      C.warmupCandles
    ) {

      warmupLoaded.add(
        symbol
      );

      warmupStats
        .restLoaded++;

      await saveSymbolCandles(
        symbol
      );

      console.log(
        `🌐 REST READY ${symbol} | ${candles[symbol].length}`
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

// ============================================================
// WARMUP QUEUE
// ============================================================

function queueWarmup(
  symbol
) {

  if (
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

    })();
  }
}

// ============================================================
// EQUITY
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

// ============================================================
// DRAWDOWN
// ============================================================

function updateDrawdown() {

  const current =
    equity();

  if (
    current >
    peakEquity
  ) {

    peakEquity =
      current;
  }

  const drawdown =
    peakEquity
      ? (
          (
            peakEquity -
            current
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

// ============================================================
// DAILY RESET
// ============================================================

function checkDay() {

  const today =
    utcDay();

  if (
    today ===
    currentDay
  ) {
    return;
  }

  currentDay =
    today;

  dailyPnL = 0;

  dailyPause = false;

  dailyStartEquity =
    equity();

  saveCloudState();
}

// ============================================================
// DAILY LOSS PROTECTION
// ============================================================

function checkDailyLoss() {

  const maxLoss =
    dailyStartEquity *
    C.dailyLossLimitPct;

  if (
    !dailyPause &&
    dailyPnL <=
      -maxLoss
  ) {

    dailyPause = true;

    candidatePool.clear();

    tg(
      `🛑 <b>LOMY DAILY PROTECTION</b>\n\n` +
      `PnL: $${dailyPnL.toFixed(2)}`
    );

    saveCloudState();
  }
}

// ============================================================
// SMART COOLDOWN
// ============================================================

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

    cooldownReason = null;

    entriesSinceCooldown = 0;

    candidatePool.clear();

    tg(
      '▶️ <b>LOMY SMART COOLDOWN FINISHED</b>'
    );

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
    `⏸ <b>LOMY SMART COOLDOWN</b>\n\n` +
    `${reason}\n` +
    `${Math.ceil(ms / 60000)} minutes`
  );

  saveCloudState();
}

// ============================================================
// SYMBOL LOSS COOLDOWN
// ============================================================

function symbolCooling(
  symbol
) {

  const last =
    n(
      lastLossBySymbol[
        symbol
      ]
    );

  if (!last) {
    return false;
  }

  return (
    Date.now() -
      last <
    C.symbolLossCooldownMs
  );
}

// ============================================================
// BASIC INDICATORS
// ============================================================

function sma(
  arr,
  period,
  key
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
          total,
          candle
        ) =>
          total +
          n(candle[key]),
        0
      ) /
    period
  );
}

// ============================================================
// EMA
// ============================================================

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

  let result =
    arr
      .slice(
        0,
        period
      )
      .reduce(
        (
          total,
          candle
        ) =>
          total +
          n(candle[key]),
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
    i < arr.length;
    i++
  ) {

    result =
      (
        n(arr[i][key]) -
        result
      ) *
      multiplier +
      result;
  }

  return result;
}

// ============================================================
// CHANDE MOMENTUM OSCILLATOR
// Ultra-Fast layer: CMO(9)
// ============================================================

function cmo(
  arr,
  period =
    C.scalpCmoLen
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

    const change =
      arr[i].close -
      arr[i - 1].close;

    if (
      change > 0
    ) {

      up +=
        change;

    } else {

      down +=
        Math.abs(
          change
        );
    }
  }

  const total =
    up +
    down;

  if (!total) {
    return 0;
  }

  return (
    100 *
    (
      (
        up -
        down
      ) /
      total
    )
  );
}

// ============================================================
// ATR
// ============================================================

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

  const trueRanges = [];

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

    trueRanges.push(
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
    trueRanges.slice(
      -period
    );

  return (
    recent.reduce(
      (
        total,
        value
      ) =>
        total +
        value,
      0
    ) /
    recent.length
  );
}

// ============================================================
// MARKET STRUCTURE
// ============================================================

function structure(
  arr
) {

  if (
    arr.length <
    3
  ) {
    return 'NEUTRAL';
  }

  const current =
    arr[
      arr.length - 1
    ];

  const previous =
    arr[
      arr.length - 2
    ];

  const third =
    arr[
      arr.length - 3
    ];

  if (
    current.high >
      previous.high &&

    current.low >
      previous.low &&

    previous.low >=
      third.low
  ) {

    return 'BULLISH';
  }

  if (
    current.high <
      previous.high &&

    current.low <
      previous.low
  ) {

    return 'BEARISH';
  }

  return 'NEUTRAL';
}

// ============================================================
// VOLUME ACCELERATION
//
// > 1 = current short-term volume stronger
//       than preceding short-term volume.
//
// This is different from normal Volume Ratio.
// ============================================================

function volumeAcceleration(
  arr
) {

  if (
    arr.length <
    8
  ) {
    return 0;
  }

  const current3 =
    arr
      .slice(-3)
      .reduce(
        (
          total,
          candle
        ) =>
          total +
          candle.volume,
        0
      ) /
    3;

  const previous3 =
    arr
      .slice(
        -6,
        -3
      )
      .reduce(
        (
          total,
          candle
        ) =>
          total +
          candle.volume,
        0
      ) /
    3;

  if (
    previous3 <= 0
  ) {
    return 0;
  }

  return (
    current3 /
    previous3
  );
}

// ============================================================
// PRICE ACCELERATION
//
// Compares latest return against previous return.
// Positive result = bullish acceleration.
// ============================================================

function priceAcceleration(
  arr
) {

  if (
    arr.length <
    5
  ) {
    return 0;
  }

  const x0 =
    arr[
      arr.length - 1
    ].close;

  const x1 =
    arr[
      arr.length - 2
    ].close;

  const x2 =
    arr[
      arr.length - 3
    ].close;

  const latestMove =
    pct(
      x0 -
      x1,
      x1
    );

  const previousMove =
    pct(
      x1 -
      x2,
      x2
    );

  return (
    latestMove -
    previousMove
  );
}

// ============================================================
// TAKER BUY RATIO / ORDER FLOW
//
// Binance gives taker-buy base volume.
// 0.50 = balanced
// >0.50 = aggressive buyers dominate
// <0.50 = aggressive sellers dominate
// ============================================================

function takerBuyRatio(
  candle
) {

  const totalVolume =
    n(
      candle.volume
    );

  const takerBuy =
    n(
      candle.takerBuyBase
    );

  if (
    totalVolume <= 0 ||
    takerBuy <= 0
  ) {

    // Old cloud candles may not contain
    // taker-buy fields yet.
    return null;
  }

  return clamp(
    takerBuy /
      totalVolume,
    0,
    1
  );
}

// ============================================================
// ORDER FLOW MOMENTUM
//
// Uses last 3 candles instead of trusting one candle.
// ============================================================

function orderFlowMomentum(
  arr
) {

  const recent =
    arr.slice(-3);

  let buyVolume = 0;

  let totalVolume = 0;

  let valid = 0;

  for (
    const candle
    of recent
  ) {

    const volume =
      n(
        candle.volume
      );

    const buy =
      n(
        candle.takerBuyBase
      );

    if (
      volume > 0 &&
      buy > 0
    ) {

      buyVolume +=
        buy;

      totalVolume +=
        volume;

      valid++;
    }
  }

  if (
    !valid ||
    totalVolume <= 0
  ) {
    return null;
  }

  return clamp(
    buyVolume /
      totalVolume,
    0,
    1
  );
}

// ============================================================
// BREAKOUT QUALITY
//
// Measures:
// - actual breakout
// - close position inside candle
// - upper wick
// - breakout distance relative to ATR
//
// Returns score 0 - 100.
// ============================================================

function breakoutQuality({
  breakout,
  breakoutPct,
  atrPct,
  bodyRatio,
  upperWickRatio,
  closeLocation
}) {

  if (!breakout) {
    return 0;
  }

  let score = 25;

  if (
    breakoutPct >= 0.02 &&
    breakoutPct <= 0.50
  ) {
    score += 20;
  }

  if (
    atrPct > 0
  ) {

    const relativeBreakout =
      breakoutPct /
      atrPct;

    if (
      relativeBreakout >= 0.10 &&
      relativeBreakout <= 1.20
    ) {

      score += 20;

    } else if (
      relativeBreakout > 2.0
    ) {

      score -= 15;
    }
  }

  if (
    bodyRatio >= 0.50
  ) {
    score += 15;
  }

  if (
    upperWickRatio <= 0.30
  ) {
    score += 10;
  }

  if (
    closeLocation >= 0.70
  ) {
    score += 10;
  }

  return clamp(
    score,
    0,
    100
  );
}

// ============================================================
// BTC CONTEXT
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
      score: 0
    };
  }

  const last =
    arr[
      arr.length - 1
    ];

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

  const previous =
    arr[
      arr.length - 6
    ];

  const change5 =
    previous
      ? pct(
          last.close -
          previous.close,
          previous.close
        )
      : 0;

  const s =
    structure(
      arr
    );

  const bullish =
    last.close >
      e20 &&

    e20 >
      e50 &&

    s !==
      'BEARISH' &&

    change5 >
      -0.35;

  let score = 0;

  if (
    last.close >
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
    s ===
    'BULLISH'
  ) {
    score += 25;
  }

  if (
    change5 >= 0
  ) {
    score += 15;
  }

  return {
    ready: true,
    bullish,
    score,
    price:
      last.close,
    change5,
    structure:
      s
  };
}

// ============================================================
// MARKET REGIME
// ============================================================

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

    const close =
      arr[
        arr.length - 1
      ].close;

    ready++;

    if (
      e20 &&
      close >
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
    breadth >= 55
  ) {

    regime =
      'RISK_ON';

  } else if (
    btc.ready &&
    breadth >= 45
  ) {

    regime =
      'NEUTRAL';
  }

  marketRegime = {

    ready:
      ready >= 20,

    btcBullish:
      btc.bullish,

    btcScore:
      btc.score,

    breadth:
      Number(
        breadth.toFixed(2)
      ),

    regime,

    updatedAt:
      Date.now()
  };
}
// ============================================================
// ULTRA-FAST SCALP CONFIRMATION SCORE
// ============================================================

function scalpConfirmationScore({
  bullish,
  bodyRatio,
  upperWickRatio,
  closeLocation,
  momentum,
  volumeRatio,
  volumeAccel,
  priceAccel,
  takerRatio,
  flowMomentum,
  breakout,
  breakoutScore,
  emaDistance,
  structureState
}) {

  let score = 0;

  const reasons = [];
  const warnings = [];

  // ----------------------------------------------------------
  // 1. STRONG BULLISH CANDLE
  // ----------------------------------------------------------

  if (
    bullish &&
    bodyRatio >=
      C.scalpBodyMin
  ) {

    score += 14;

    reasons.push(
      'SCALP_STRONG_BODY'
    );
  }

  if (
    bodyRatio >= 0.65
  ) {

    score += 6;

    reasons.push(
      'SCALP_BODY_POWER'
    );
  }

  // ----------------------------------------------------------
  // 2. LOW UPPER WICK
  // ----------------------------------------------------------

  if (
    upperWickRatio <= 0.25
  ) {

    score += 8;

    reasons.push(
      'SCALP_LOW_WICK'
    );

  } else if (
    upperWickRatio > 0.45
  ) {

    score -= 8;

    warnings.push(
      'SCALP_REJECTION_WICK'
    );
  }

  // ----------------------------------------------------------
  // 3. CLOSE LOCATION
  // ----------------------------------------------------------

  if (
    closeLocation >= 0.75
  ) {

    score += 7;

    reasons.push(
      'SCALP_CLOSE_HIGH'
    );
  }

  // ----------------------------------------------------------
  // 4. CMO MOMENTUM
  // ----------------------------------------------------------

  if (
    momentum >= 48 &&
    momentum <= 72
  ) {

    score += 14;

    reasons.push(
      'SCALP_CMO_SWEET'
    );

  } else if (
    momentum >=
      C.scalpCmoMin &&
    momentum < 48
  ) {

    score += 7;

    reasons.push(
      'SCALP_CMO_BUILDING'
    );

  } else if (
    momentum >
      C.scalpCmoHot
  ) {

    score -= 12;

    warnings.push(
      'SCALP_CMO_OVERHEAT'
    );
  }

  // ----------------------------------------------------------
  // 5. VOLUME SPIKE
  // ----------------------------------------------------------

  if (
    volumeRatio >= 1.30 &&
    volumeRatio <= 2.80
  ) {

    score += 12;

    reasons.push(
      'SCALP_VOLUME'
    );

  } else if (
    volumeRatio > 3.50
  ) {

    score -= 10;

    warnings.push(
      'SCALP_VOLUME_EXHAUSTION'
    );
  }

  // ----------------------------------------------------------
  // 6. VOLUME ACCELERATION
  // ----------------------------------------------------------

  if (
    volumeAccel >= 1.10 &&
    volumeAccel <= 2.40
  ) {

    score += 9;

    reasons.push(
      'VOLUME_ACCELERATION'
    );

  } else if (
    volumeAccel > 3.0
  ) {

    score -= 6;

    warnings.push(
      'VOLUME_ACCEL_OVERHEAT'
    );
  }

  // ----------------------------------------------------------
  // 7. PRICE ACCELERATION
  // ----------------------------------------------------------

  if (
    priceAccel > 0 &&
    priceAccel <= 0.60
  ) {

    score += 8;

    reasons.push(
      'PRICE_ACCELERATION'
    );

  } else if (
    priceAccel > 1.20
  ) {

    score -= 7;

    warnings.push(
      'PRICE_ACCEL_CHASE'
    );
  }

  // ----------------------------------------------------------
  // 8. CURRENT TAKER BUY FLOW
  // ----------------------------------------------------------

  if (
    takerRatio !== null
  ) {

    if (
      takerRatio >= 0.53 &&
      takerRatio <= 0.72
    ) {

      score += 10;

      reasons.push(
        'TAKER_BUY_DOMINANCE'
      );

    } else if (
      takerRatio < 0.43
    ) {

      score -= 10;

      warnings.push(
        'SELL_FLOW_DOMINANCE'
      );
    }
  }

  // ----------------------------------------------------------
  // 9. 3-CANDLE FLOW CONFIRMATION
  // ----------------------------------------------------------

  if (
    flowMomentum !== null
  ) {

    if (
      flowMomentum >= 0.52
    ) {

      score += 8;

      reasons.push(
        'FLOW_CONFIRMATION'
      );

    } else if (
      flowMomentum < 0.44
    ) {

      score -= 8;

      warnings.push(
        'FLOW_WEAK'
      );
    }
  }

  // ----------------------------------------------------------
  // 10. BREAKOUT QUALITY
  // ----------------------------------------------------------

  if (
    breakout &&
    breakoutScore >= 60
  ) {

    score += 10;

    reasons.push(
      'QUALITY_BREAKOUT'
    );

  } else if (
    breakout &&
    breakoutScore < 40
  ) {

    score -= 8;

    warnings.push(
      'WEAK_BREAKOUT'
    );
  }

  // ----------------------------------------------------------
  // 11. EMA DISTANCE / ANTI-CHASE
  // ----------------------------------------------------------

  if (
    emaDistance >= 0.20 &&
    emaDistance <= 1.40
  ) {

    score += 7;

    reasons.push(
      'SCALP_EMA_ZONE'
    );

  } else if (
    emaDistance > 2.20
  ) {

    score -= 12;

    warnings.push(
      'SCALP_CHASE'
    );
  }

  // ----------------------------------------------------------
  // 12. MARKET STRUCTURE
  // ----------------------------------------------------------

  if (
    structureState ===
    'BULLISH'
  ) {

    score += 7;

    reasons.push(
      'SCALP_STRUCTURE'
    );

  } else if (
    structureState ===
    'BEARISH'
  ) {

    score -= 15;

    warnings.push(
      'SCALP_STRUCTURE_BEARISH'
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
    warnings
  };
}

// ============================================================
// MAIN V4.5 ANALYSIS ENGINE
// ============================================================

function analyze(
  arr,
  symbol
) {

  if (
    !Array.isArray(arr) ||
    arr.length <
      C.warmupCandles
  ) {
    return null;
  }

  const x =
    arr[
      arr.length - 1
    ];

  const previous =
    arr[
      arr.length - 2
    ];

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

  // Ultra-Fast uses Volume SMA(10)
  const volSma10 =
    sma(
      arr,
      C.scalpVolLen,
      'volume'
    );

  // Longer context
  const volSma20 =
    sma(
      arr,
      20,
      'volume'
    );

  const momentum =
    cmo(
      arr,
      C.scalpCmoLen
    );

  const a =
    atr(
      arr,
      14
    );

  if (
    [
      e20,
      e50,
      volSma10,
      volSma20,
      momentum,
      a
    ].some(
      value =>
        value === null
    )
  ) {
    return null;
  }

  // ==========================================================
  // CANDLE GEOMETRY
  // ==========================================================

  const range =
    x.high -
    x.low;

  if (
    range <= 0
  ) {
    return null;
  }

  const body =
    Math.abs(
      x.close -
      x.open
    );

  const bodyRatio =
    body /
    range;

  const bullish =
    x.close >
    x.open;

  const upperWick =
    x.high -
    Math.max(
      x.open,
      x.close
    );

  const lowerWick =
    Math.min(
      x.open,
      x.close
    ) -
    x.low;

  const upperWickRatio =
    upperWick /
    range;

  const lowerWickRatio =
    lowerWick /
    range;

  const closeLocation =
    (
      x.close -
      x.low
    ) /
    range;

  // ==========================================================
  // VOLUME
  // ==========================================================

  const volumeRatio =
    volSma10 > 0
      ? x.volume /
        volSma10
      : 0;

  const volumeRatio20 =
    volSma20 > 0
      ? x.volume /
        volSma20
      : 0;

  const volumeAccel =
    volumeAcceleration(
      arr
    );

  // ==========================================================
  // PRICE ACCELERATION
  // ==========================================================

  const priceAccel =
    priceAcceleration(
      arr
    );

  // ==========================================================
  // ORDER FLOW
  // ==========================================================

  const takerRatio =
    takerBuyRatio(
      x
    );

  const flowMomentum =
    orderFlowMomentum(
      arr
    );

  // ==========================================================
  // EMA DISTANCE
  // ==========================================================

  const emaDistance =
    pct(
      x.close -
      e20,
      e20
    );

  const ema50Distance =
    pct(
      x.close -
      e50,
      e50
    );

  // ==========================================================
  // ATR
  // ==========================================================

  const atrPct =
    pct(
      a,
      x.close
    );

  // ==========================================================
  // MARKET STRUCTURE
  // ==========================================================

  const s =
    structure(
      arr
    );

  // ==========================================================
  // SUPPORT / RESISTANCE / BREAKOUT
  // ==========================================================

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

  const breakout =
    x.close >
    resistance;

  const breakoutPct =
    pct(
      x.close -
      resistance,
      resistance
    );

  // ==========================================================
  // EXTENSION
  // ==========================================================

  const ref5 =
    arr[
      arr.length - 6
    ];

  const ref10 =
    arr[
      arr.length - 11
    ];

  const ext5 =
    ref5
      ? pct(
          x.close -
          ref5.close,
          ref5.close
        )
      : 0;

  const ext10 =
    ref10
      ? pct(
          x.close -
          ref10.close,
          ref10.close
        )
      : 0;

  // ==========================================================
  // EMA SLOPE
  // ==========================================================

  const oldArr =
    arr.slice(
      0,
      -3
    );

  const ema20Old =
    oldArr.length >= 20
      ? ema(
          oldArr,
          20
        )
      : null;

  const emaSlope =
    ema20Old
      ? pct(
          e20 -
          ema20Old,
          ema20Old
        )
      : 0;

  // ==========================================================
  // SHORT PRICE MOMENTUM
  // ==========================================================

  const oneCandleMove =
    previous
      ? pct(
          x.close -
          previous.close,
          previous.close
        )
      : 0;

  // ==========================================================
  // BREAKOUT QUALITY SCORE
  // ==========================================================

  const breakoutScore =
    breakoutQuality({
      breakout,
      breakoutPct,
      atrPct,
      bodyRatio,
      upperWickRatio,
      closeLocation
    });

  // ==========================================================
  // SCALP CONFIRMATION
  // ==========================================================

  const scalp =
    scalpConfirmationScore({
      bullish,
      bodyRatio,
      upperWickRatio,
      closeLocation,
      momentum,
      volumeRatio,
      volumeAccel,
      priceAccel,
      takerRatio,
      flowMomentum,
      breakout,
      breakoutScore,
      emaDistance,
      structureState: s
    });

  // ==========================================================
  // MAIN OPPORTUNITY SCORE
  // ==========================================================

  let score = 0;

  const reasons = [];

  const warnings = [];

  // ----------------------------------------------------------
  // TREND
  // ----------------------------------------------------------

  if (
    x.close >
      e20 &&
    e20 >
      e50
  ) {

    score += 16;

    reasons.push(
      'TREND'
    );
  }

  // ----------------------------------------------------------
  // EMA SLOPE
  // ----------------------------------------------------------

  if (
    emaSlope > 0.03
  ) {

    score += 7;

    reasons.push(
      'EMA_SLOPE'
    );
  }

  // ----------------------------------------------------------
  // STRUCTURE
  // ----------------------------------------------------------

  if (
    s ===
    'BULLISH'
  ) {

    score += 8;

    reasons.push(
      'STRUCTURE'
    );
  }

  // ----------------------------------------------------------
  // BODY
  // ----------------------------------------------------------

  if (
    bullish &&
    bodyRatio >= 0.50
  ) {

    score += 9;

    reasons.push(
      'BODY'
    );
  }

  // ----------------------------------------------------------
  // WICK
  // ----------------------------------------------------------

  if (
    upperWickRatio <= 0.32
  ) {

    score += 5;

    reasons.push(
      'LOW_WICK'
    );
  }

  // ----------------------------------------------------------
  // CMO
  // ----------------------------------------------------------

  if (
    momentum >= 48 &&
    momentum <= 74
  ) {

    score += 11;

    reasons.push(
      'CMO_SWEET'
    );

  } else if (
    momentum >= 40 &&
    momentum < 48
  ) {

    score += 5;

    reasons.push(
      'CMO_BUILDING'
    );

  } else if (
    momentum > 84
  ) {

    score -= 10;

    warnings.push(
      'CMO_OVERHEAT'
    );
  }

  // ----------------------------------------------------------
  // VOLUME
  // ----------------------------------------------------------

  if (
    volumeRatio >= 1.30 &&
    volumeRatio <= 2.80
  ) {

    score += 12;

    reasons.push(
      'VOLUME_SWEET'
    );

  } else if (
    volumeRatio >= 2.80 &&
    volumeRatio < 3.50
  ) {

    score += 4;

    reasons.push(
      'VOLUME_HIGH'
    );

  } else if (
    volumeRatio >= 3.50
  ) {

    score -= 12;

    warnings.push(
      'VOLUME_EXHAUSTION'
    );
  }

  // ----------------------------------------------------------
  // VOLUME ACCELERATION
  // ----------------------------------------------------------

  if (
    volumeAccel >= 1.08 &&
    volumeAccel <= 2.40
  ) {

    score += 7;

    reasons.push(
      'VOLUME_ACCEL'
    );
  }

  // ----------------------------------------------------------
  // PRICE ACCELERATION
  // ----------------------------------------------------------

  if (
    priceAccel > 0 &&
    priceAccel <= 0.70
  ) {

    score += 6;

    reasons.push(
      'PRICE_ACCEL'
    );
  }

  // ----------------------------------------------------------
  // ORDER FLOW
  // ----------------------------------------------------------

  if (
    takerRatio !== null
  ) {

    if (
      takerRatio >= 0.52
    ) {

      score += 6;

      reasons.push(
        'BUY_FLOW'
      );

    } else if (
      takerRatio < 0.43
    ) {

      score -= 8;

      warnings.push(
        'SELL_FLOW'
      );
    }
  }

  // ----------------------------------------------------------
  // EMA ZONE
  // ----------------------------------------------------------

  if (
    emaDistance >= 0.20 &&
    emaDistance <= 1.60
  ) {

    score += 7;

    reasons.push(
      'EMA_ZONE'
    );

  } else if (
    emaDistance > 2.20
  ) {

    score -= 14;

    warnings.push(
      'CHASE'
    );
  }

  // ----------------------------------------------------------
  // BREAKOUT
  // ----------------------------------------------------------

  if (
    breakout &&
    breakoutScore >= 55
  ) {

    score += 10;

    reasons.push(
      'BREAKOUT'
    );

  } else if (
    breakout &&
    breakoutScore < 40
  ) {

    score -= 6;

    warnings.push(
      'WEAK_BREAKOUT'
    );
  }

  // ----------------------------------------------------------
  // ATR
  // ----------------------------------------------------------

  if (
    atrPct >= 0.12 &&
    atrPct <= 2.00
  ) {

    score += 4;

    reasons.push(
      'ATR_OK'
    );
  }

  // ----------------------------------------------------------
  // EXTENSION FILTERS
  // ----------------------------------------------------------

  if (
    ext5 > 2.4
  ) {

    score -= 10;

    warnings.push(
      'EXT5'
    );
  }

  if (
    ext10 > 4.5
  ) {

    score -= 8;

    warnings.push(
      'EXT10'
    );
  }

  // ----------------------------------------------------------
  // MARKET REGIME
  // ----------------------------------------------------------

  if (
    marketRegime.regime ===
    'RISK_ON'
  ) {

    score += 10;

    reasons.push(
      'MARKET_RISK_ON'
    );

  } else if (
    marketRegime.regime ===
    'NEUTRAL'
  ) {

    score += 2;

    reasons.push(
      'MARKET_NEUTRAL'
    );

  } else {

    score -= 18;

    warnings.push(
      'MARKET_DEFENSIVE'
    );
  }

  // ----------------------------------------------------------
  // SCALP SCORE CONTRIBUTION
  // ----------------------------------------------------------

  if (
    scalp.score >= 80
  ) {

    score += 10;

    reasons.push(
      'SCALP_ELITE'
    );

  } else if (
    scalp.score >= 65
  ) {

    score += 6;

    reasons.push(
      'SCALP_CONFIRMED'
    );

  } else if (
    scalp.score < 45
  ) {

    score -= 8;

    warnings.push(
      'SCALP_WEAK'
    );
  }

  score =
    clamp(
      score,
      0,
      100
    );

  // ==========================================================
  // GRADE
  // ==========================================================

  let grade =
    'C';

  if (
    score >= 88 &&
    scalp.score >= 72
  ) {

    grade =
      'A';

  } else if (
    score >= 78 &&
    scalp.score >= 60
  ) {

    grade =
      'B';
  }

  // ==========================================================
  // HARD SAFETY BLOCKERS
  // ==========================================================

  const hardBlocked =
    warnings.includes(
      'VOLUME_EXHAUSTION'
    ) ||

    warnings.includes(
      'MARKET_DEFENSIVE'
    ) ||

    warnings.includes(
      'SELL_FLOW'
    ) ||

    scalp.warnings.includes(
      'SCALP_STRUCTURE_BEARISH'
    ) ||

    scalp.warnings.includes(
      'SCALP_CHASE'
    );

  // ==========================================================
  // RISK-ON ENTRY PATH
  // ==========================================================

  const riskOnEligible =

    marketRegime.regime ===
      'RISK_ON' &&

    score >=
      C.minScore &&

    scalp.score >=
      C.minScalpScore &&

    grade !==
      'C' &&

    bullish &&

    x.close >
      e20 &&

    e20 >
      e50 &&

    s !==
      'BEARISH' &&

    momentum >=
      42 &&

    momentum <=
      C.scalpCmoHot &&

    volumeRatio >=
      1.20 &&

    volumeRatio <
      3.50 &&

    bodyRatio >=
      0.48 &&

    upperWickRatio <=
      0.42 &&

    emaDistance <=
      2.20 &&

    ext5 <=
      2.60 &&

    ext10 <=
      4.80 &&

    !hardBlocked;

  // ==========================================================
  // NEUTRAL MARKET ELITE ENTRY PATH
  //
  // This is intentionally stricter than RISK_ON.
  // ==========================================================

  const neutralEligible =

    marketRegime.regime ===
      'NEUTRAL' &&

    score >=
      C.neutralMinScore &&

    scalp.score >=
      C.neutralMinScalpScore &&

    grade ===
      'A' &&

    bullish &&

    x.close >
      e20 &&

    e20 >
      e50 &&

    s ===
      'BULLISH' &&

    momentum >= 48 &&

    momentum <= 76 &&

    volumeRatio >= 1.30 &&

    volumeRatio <= 2.80 &&

    bodyRatio >= 0.55 &&

    upperWickRatio <= 0.30 &&

    breakout &&

    breakoutScore >= 65 &&

    emaDistance <= 1.60 &&

    ext5 <= 2.00 &&

    ext10 <= 3.50 &&

    !hardBlocked;

  const eligible =
    riskOnEligible ||
    neutralEligible;

  // ==========================================================
  // FINAL RESULT
  // ==========================================================

  return {

    symbol,

    score,

    grade,

    eligible,

    price:
      x.close,

    open:
      x.open,

    high:
      x.high,

    low:
      x.low,

    close:
      x.close,

    previousClose:
      previous?.close ||
      x.close,

    // Candle
    bullish,

    bodyRatio,

    upperWickRatio,

    lowerWickRatio,

    closeLocation,

    // Trend
    ema20:
      e20,

    ema50:
      e50,

    emaDistance,

    ema50Distance,

    emaSlope,

    // Momentum
    cmo:
      momentum,

    oneCandleMove,

    priceAcceleration:
      priceAccel,

    // Volume
    volumeRatio,

    volumeRatio20,

    volumeAcceleration:
      volumeAccel,

    // Order flow
    takerBuyRatio:
      takerRatio,

    orderFlowMomentum:
      flowMomentum,

    // Structure
    structure:
      s,

    // Breakout
    breakout,

    breakoutPct,

    breakoutQuality:
      breakoutScore,

    resistance,

    support,

    // Volatility
    atr:
      a,

    atrPct,

    // Extension
    ext5,

    ext10,

    // Scalp Engine
    scalpScore:
      scalp.score,

    scalpReasons:
      scalp.reasons,

    scalpWarnings:
      scalp.warnings,

    // Main Engine
    reasons,

    warnings,

    // Market
    regime:
      marketRegime.regime,

    breadth:
      marketRegime.breadth,

    btcBullish:
      marketRegime.btcBullish
  };
}

// ============================================================
// FRESH CANDIDATE REVALIDATION
//
// Re-checks the live price immediately before entry.
// Prevents stale / chase entries.
// ============================================================

function revalidateCandidate(
  candidate,
  marketPrice
) {

  const blockers = [];

  if (
    !candidate ||
    !marketPrice
  ) {

    blockers.push(
      'MISSING_DATA'
    );

    return {
      valid: false,
      blockers
    };
  }

  // ----------------------------------------------------------
  // AGE
  // ----------------------------------------------------------

  if (
    Date.now() >
    candidate.expiresAt
  ) {

    blockers.push(
      'EXPIRED'
    );
  }

  // ----------------------------------------------------------
  // PRICE DRIFT
  // ----------------------------------------------------------

  const drift =
    pct(
      marketPrice -
      candidate.signalPrice,
      candidate.signalPrice
    );

  if (
    Math.abs(drift) >
    C.maxPriceDriftPct
  ) {

    blockers.push(
      'PRICE_DRIFT'
    );
  }

  // ----------------------------------------------------------
  // UPWARD CHASE GUARD
  // ----------------------------------------------------------

  if (
    drift > 0.22
  ) {

    blockers.push(
      'LIVE_CHASE'
    );
  }

  // ----------------------------------------------------------
  // PRICE FALLING BELOW SIGNAL
  // ----------------------------------------------------------

  if (
    drift < -0.35
  ) {

    blockers.push(
      'SIGNAL_FAILURE'
    );
  }

  // ----------------------------------------------------------
  // SYMBOL COOLDOWN
  // ----------------------------------------------------------

  if (
    symbolCooling(
      candidate.symbol
    )
  ) {

    blockers.push(
      'SYMBOL_COOLDOWN'
    );
  }

  // ----------------------------------------------------------
  // HARD ANALYSIS WARNINGS
  // ----------------------------------------------------------

  if (
    candidate.warnings
      ?.includes(
        'VOLUME_EXHAUSTION'
      )
  ) {

    blockers.push(
      'VOLUME_EXHAUSTION'
    );
  }

  if (
    candidate.warnings
      ?.includes(
        'CHASE'
      )
  ) {

    blockers.push(
      'CHASE'
    );
  }

  if (
    candidate.scalpWarnings
      ?.includes(
        'SCALP_CHASE'
      )
  ) {

    blockers.push(
      'SCALP_CHASE'
    );
  }

  return {

    valid:
      blockers.length === 0,

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

  const marketPrice =
    tickers.get(
      symbol
    )?.price ||
    analysis.price;

  const now =
    Date.now();

  candidatePool.set(
    symbol,
    {

      ...analysis,

      signalPrice:
        marketPrice,

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

    score:
      analysis.score,

    grade:
      analysis.grade,

    scalpScore:
      analysis.scalpScore,

    cmo:
      analysis.cmo,

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

    breakoutQuality:
      analysis.breakoutQuality,

    regime:
      analysis.regime,

    breadth:
      analysis.breadth
  });

  scheduleRanking();
}

// ============================================================
// RANK SCHEDULER
// ============================================================

function scheduleRanking() {

  if (
    rankTimer
  ) {
    return;
  }

  rankTimer =
    setTimeout(
      () => {

        rankTimer =
          null;

        rankAndExecute();

      },
      1500
    );
}

// ============================================================
// CANDIDATE RANKING
// ============================================================

function candidateRank(
  candidate
) {

  let rank =
    candidate.score;

  // Scalp quality carries real weight
  rank +=
    candidate.scalpScore *
    0.20;

  if (
    candidate.volumeRatio >= 1.4 &&
    candidate.volumeRatio <= 2.6
  ) {

    rank += 5;
  }

  if (
    candidate.volumeAcceleration >= 1.1 &&
    candidate.volumeAcceleration <= 2.2
  ) {

    rank += 5;
  }

  if (
    candidate.priceAcceleration > 0 &&
    candidate.priceAcceleration <= 0.6
  ) {

    rank += 4;
  }

  if (
    candidate.takerBuyRatio !== null &&
    candidate.takerBuyRatio >= 0.54
  ) {

    rank += 5;
  }

  if (
    candidate.orderFlowMomentum !== null &&
    candidate.orderFlowMomentum >= 0.52
  ) {

    rank += 4;
  }

  if (
    candidate.breakoutQuality >= 70
  ) {

    rank += 5;
  }

  if (
    candidate.emaDistance >= 0.35 &&
    candidate.emaDistance <= 1.30
  ) {

    rank += 4;
  }

  if (
    candidate.cmo >= 52 &&
    candidate.cmo <= 70
  ) {

    rank += 3;
  }

  if (
    candidate.regime ===
    'RISK_ON'
  ) {

    rank += 5;
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
    positions[
      candidate.symbol
    ]
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

  // ==========================================================
  // FINAL LIVE REVALIDATION
  // ==========================================================

  const validation =
    revalidateCandidate(
      candidate,
      marketPrice
    );

  if (
    !validation.valid
  ) {

    cloudJournal({
      type:
        'ENTRY_REVALIDATION_REJECT',

      symbol:
        candidate.symbol,

      score:
        candidate.score,

      scalpScore:
        candidate.scalpScore,

      drift:
        validation.drift,

      blockers:
        validation.blockers,

      regime:
        candidate.regime
    });

    return false;
  }

  // ==========================================================
  // SIMULATED ENTRY
  // ==========================================================

  const entry =
    marketPrice *
    (
      1 +
      C.slippagePct
    );

  // ==========================================================
  // ATR DYNAMIC STOP
  // ==========================================================

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

  // ==========================================================
  // POSITION SIZE
  // ==========================================================

  let allocation =
    equity() /
    C.maxPositions;

  allocation =
    Math.min(
      allocation,
      cash
    );

  if (
    allocation < 50
  ) {
    return false;
  }

  // ==========================================================
  // ENTRY COST
  // ==========================================================

  const buyFee =
    allocation *
    C.feePct;

  const usable =
    allocation -
    buyFee;

  const qty =
    usable /
    entry;

  cash -=
    allocation;

  stats.fees +=
    buyFee;

  // ==========================================================
  // CREATE POSITION
  // ==========================================================

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

    // ========================================================
    // ENTRY INTELLIGENCE
    // ========================================================

    score:
      candidate.score,

    scalpScore:
      candidate.scalpScore,

    grade:
      candidate.grade,

    cmo:
      candidate.cmo,

    volumeRatio:
      candidate.volumeRatio,

    volumeRatio20:
      candidate.volumeRatio20,

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

    emaSlope:
      candidate.emaSlope,

    breakoutPct:
      candidate.breakoutPct,

    breakoutQuality:
      candidate.breakoutQuality,

    resistance:
      candidate.resistance,

    support:
      candidate.support,

    atrPct:
      candidate.atrPct,

    ext5:
      candidate.ext5,

    ext10:
      candidate.ext10,

    bodyRatio:
      candidate.bodyRatio,

    upperWickRatio:
      candidate.upperWickRatio,

    closeLocation:
      candidate.closeLocation,

    regime:
      candidate.regime,

    breadth:
      candidate.breadth,

    btcBullish:
      candidate.btcBullish,

    reasons:
      candidate.reasons,

    warnings:
      candidate.warnings,

    scalpReasons:
      candidate.scalpReasons,

    scalpWarnings:
      candidate.scalpWarnings,

    session:
      sessionUTC(),

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

  // ==========================================================
  // JOURNAL
  // ==========================================================

  cloudJournal({

    type:
      'ENTRY',

    symbol:
      candidate.symbol,

    score:
      candidate.score,

    scalpScore:
      candidate.scalpScore,

    grade:
      candidate.grade,

    entry,

    stopPct,

    targetPct,

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

    breakoutQuality:
      candidate.breakoutQuality,

    drift:
      validation.drift,

    regime:
      candidate.regime,

    breadth:
      candidate.breadth
  });

  // ==========================================================
  // TELEGRAM
  // ==========================================================

  const flowText =
    candidate.takerBuyRatio ===
      null
      ? 'N/A'
      : (
          candidate.takerBuyRatio *
          100
        ).toFixed(1) +
        '%';

  tg(
    `🟢 <b>LOMY V4.5 BUY</b>\n\n` +

    `<b>${candidate.symbol}</b>\n` +

    `Grade: ${candidate.grade}\n` +

    `Opportunity: ${candidate.score}/100\n` +

    `Scalp: ${candidate.scalpScore}/100\n` +

    `Regime: ${candidate.regime}\n` +

    `Breadth: ${candidate.breadth}%\n\n` +

    `CMO: ${candidate.cmo.toFixed(1)}\n` +

    `Volume: ${candidate.volumeRatio.toFixed(2)}x\n` +

    `Vol Accel: ${candidate.volumeAcceleration.toFixed(2)}x\n` +

    `Buy Flow: ${flowText}\n` +

    `Breakout Quality: ${candidate.breakoutQuality}/100\n\n` +

    `Entry: ${entry.toFixed(8)}\n` +

    `SL: ${positions[candidate.symbol].stopLoss.toFixed(8)}\n` +

    `TP: ${positions[candidate.symbol].takeProfit.toFixed(8)}`
  );

  saveCloudState();

  // ==========================================================
  // CONTROLLED BATCH COOLDOWN
  // ==========================================================

  if (
    entriesSinceCooldown >=
    C.entriesBeforeCooldown
  ) {

    startCooldown(
      C.batchCooldownMs,
      'HIGH_THROUGHPUT_BATCH'
    );
  }

  return true;
}

// ============================================================
// GLOBAL RANK + EXECUTION
// ============================================================

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

  if (
    free <= 0
  ) {

    candidatePool.clear();

    return;
  }

  const valid = [];

  // ==========================================================
  // REVALIDATE EVERY CANDIDATE
  // ==========================================================

  for (
    const candidate
    of candidatePool.values()
  ) {

    const price =
      tickers.get(
        candidate.symbol
      )?.price;

    if (!price) {

      cloudJournal({
        type:
          'RANK_REJECT',

        symbol:
          candidate.symbol,

        blocker:
          'NO_LIVE_PRICE'
      });

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
          'RANK_REJECT',

        symbol:
          candidate.symbol,

        score:
          candidate.score,

        scalpScore:
          candidate.scalpScore,

        blockers:
          validation.blockers,

        drift:
          validation.drift
      });

      continue;
    }

    if (
      positions[
        candidate.symbol
      ]
    ) {
      continue;
    }

    valid.push({

      candidate,

      price,

      validation,

      rank:
        candidateRank(
          candidate
        )
    });
  }

  candidatePool.clear();

  // ==========================================================
  // BEST OPPORTUNITY FIRST
  // ==========================================================

  valid.sort(
    (
      a,
      b
    ) =>
      b.rank -
      a.rank
  );

  const take =
    Math.min(
      C.maxEntriesPerCycle,
      free
    );

  let opened = 0;

  for (
    const opportunity
    of valid
  ) {

    if (
      opened >=
      take
    ) {
      break;
    }

    const success =
      openTrade(
        opportunity.candidate,
        opportunity.price
      );

    if (
      success
    ) {

      opened++;
    }
  }
}

// ============================================================
// POSITION MANAGEMENT
// V4.5 PROFIT PROTECTION ENGINE
// ============================================================

function managePosition(
  symbol,
  price
) {

  const p =
    positions[
      symbol
    ];

  if (!p) {
    return;
  }

  p.lastPrice =
    price;

  // ==========================================================
  // HIGH / LOW WATERMARKS
  // ==========================================================

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

  // ==========================================================
  // MFE / MAE
  // ==========================================================

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

  const movePct =
    pct(
      move,
      p.entryPrice
    );

  const r =
    p.riskPrice
      ? move /
        p.riskPrice
      : 0;

  // ==========================================================
  // STAGE 1:
  // EARLY PROFIT LOCK
  //
  // Important for trades that move significantly in our
  // direction but reverse before reaching old +1R BE.
  // ==========================================================

  if (
    !p.profitLockActive &&
    p.mfePct >= 0.55
  ) {

    p.profitLockActive =
      true;

    // Protect approximately costs + tiny buffer.
    const protectedPrice =
      p.entryPrice *
      1.0025;

    p.stopLoss =
      Math.max(
        p.stopLoss,
        protectedPrice
      );

    cloudJournal({

      type:
        'PROFIT_LOCK',

      symbol,

      mfePct:
        p.mfePct,

      newStop:
        p.stopLoss
    });
  }

  // ==========================================================
  // STAGE 2:
  // BREAK EVEN
  // ==========================================================

  if (
    !p.breakEvenMoved &&
    r >=
      C.breakEvenAtR
  ) {

    p.breakEvenMoved =
      true;

    // Includes buffer for simulated costs.
    const breakEvenPrice =
      p.entryPrice *
      1.0030;

    p.stopLoss =
      Math.max(
        p.stopLoss,
        breakEvenPrice
      );

    stats.breakEvenMoves++;

    cloudJournal({

      type:
        'BREAK_EVEN_MOVE',

      symbol,

      r,

      mfePct:
        p.mfePct,

      stop:
        p.stopLoss
    });
  }

  // ==========================================================
  // STAGE 3:
  // TRAILING
  // ==========================================================

  if (
    r >=
    C.trailAtR
  ) {

    if (
      !p.trailingActive
    ) {

      p.trailingActive =
        true;

      stats
        .trailingActivations++;

      cloudJournal({

        type:
          'TRAIL_ACTIVATED',

        symbol,

        r,

        mfePct:
          p.mfePct
      });
    }

    const trail =
      p.highestPrice -
      (
        p.riskPrice *
        C.trailLockR
      );

    p.stopLoss =
      Math.max(
        p.stopLoss,
        trail
      );
  }

  // ==========================================================
  // STOP / PROFIT LOCK / TRAILING
  // ==========================================================

  if (
    price <=
    p.stopLoss
  ) {

    let reason =
      'STOP_LOSS';

    if (
      p.trailingActive
    ) {

      reason =
        'TRAIL_STOP';

    } else if (
      p.breakEvenMoved
    ) {

      reason =
        'BREAK_EVEN_STOP';

    } else if (
      p.profitLockActive
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

  // ==========================================================
  // TAKE PROFIT
  // ==========================================================

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

  // ==========================================================
  // SMART EARLY FAILURE
  //
  // We only abandon an early loser when:
  // - still inside early window
  // - trade is losing meaningfully
  // - breakout has failed
  // - trade NEVER showed enough favorable excursion
  //
  // This prevents a JUP-like trade from being classified
  // as a simple early failure after showing real momentum.
  // ==========================================================

  const age =
    Date.now() -
    p.entryTime;

  const failedBreakout =
    p.resistance
      ? price <
        p.resistance
      : false;

  const neverWorked =
    p.mfePct <
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

    return;
  }
}

// ============================================================
// CLOSE TRADE
// ============================================================

async function closeTrade(
  symbol,
  marketPrice,
  reason
) {

  const p =
    positions[
      symbol
    ];

  if (!p) {
    return;
  }

  // Prevent duplicate close from rapid ticker messages.
  if (
    p.closing
  ) {
    return;
  }

  p.closing =
    true;

  const exit =
    marketPrice *
    (
      1 -
      C.slippagePct
    );

  const gross =
    p.qty *
    exit;

  const fee =
    gross *
    C.feePct;

  const net =
    gross -
    fee;

  const profit =
    net -
    p.invested;

  const profitPct =
    pct(
      profit,
      p.invested
    );

  cash +=
    net;

  stats.totalTrades++;

  stats.fees +=
    fee;

  stats.netProfit +=
    profit;

  // ==========================================================
  // WIN / LOSS
  // ==========================================================

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

    // Only genuine failed trades count toward loss streak.
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

    stats
      .earlyFailureExits++;
  }

  dailyPnL +=
    profit;

  // ==========================================================
  // COMPLETE TRADE RECORD
  // ==========================================================

  const record = {

    symbol,

    score:
      p.score,

    scalpScore:
      p.scalpScore,

    grade:
      p.grade,

    entryPrice:
      p.entryPrice,

    exitPrice:
      exit,

    invested:
      p.invested,

    profit,

    profitPct,

    reason,

    // --------------------------------------------------------
    // EXCURSION
    // --------------------------------------------------------

    mfePct:
      p.mfePct,

    maePct:
      p.maePct,

    // --------------------------------------------------------
    // MOMENTUM
    // --------------------------------------------------------

    cmo:
      p.cmo,

    priceAcceleration:
      p.priceAcceleration,

    // --------------------------------------------------------
    // VOLUME
    // --------------------------------------------------------

    volumeRatio:
      p.volumeRatio,

    volumeRatio20:
      p.volumeRatio20,

    volumeAcceleration:
      p.volumeAcceleration,

    // --------------------------------------------------------
    // ORDER FLOW
    // --------------------------------------------------------

    takerBuyRatio:
      p.takerBuyRatio,

    orderFlowMomentum:
      p.orderFlowMomentum,

    // --------------------------------------------------------
    // TREND
    // --------------------------------------------------------

    emaDistance:
      p.emaDistance,

    emaSlope:
      p.emaSlope,

    // --------------------------------------------------------
    // BREAKOUT
    // --------------------------------------------------------

    breakoutPct:
      p.breakoutPct,

    breakoutQuality:
      p.breakoutQuality,

    // --------------------------------------------------------
    // VOLATILITY / EXTENSION
    // --------------------------------------------------------

    atrPct:
      p.atrPct,

    ext5:
      p.ext5,

    ext10:
      p.ext10,

    // --------------------------------------------------------
    // CANDLE QUALITY
    // --------------------------------------------------------

    bodyRatio:
      p.bodyRatio,

    upperWickRatio:
      p.upperWickRatio,

    closeLocation:
      p.closeLocation,

    // --------------------------------------------------------
    // MARKET
    // --------------------------------------------------------

    regime:
      p.regime,

    breadth:
      p.breadth,

    btcBullish:
      p.btcBullish,

    session:
      p.session,

    // --------------------------------------------------------
    // ENTRY INFO
    // --------------------------------------------------------

    signalPrice:
      p.signalPrice,

    signalAgeMs:
      p.signalAgeMs,

    entryDriftPct:
      p.entryDriftPct,

    // --------------------------------------------------------
    // EXIT ENGINE
    // --------------------------------------------------------

    profitLockActive:
      p.profitLockActive,

    breakEvenMoved:
      p.breakEvenMoved,

    trailingActive:
      p.trailingActive,

    // --------------------------------------------------------
    // EXPLANATION
    // --------------------------------------------------------

    reasons:
      p.reasons,

    warnings:
      p.warnings,

    scalpReasons:
      p.scalpReasons,

    scalpWarnings:
      p.scalpWarnings,

    // --------------------------------------------------------
    // TIME
    // --------------------------------------------------------

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

  // ==========================================================
  // DELETE POSITION
  // ==========================================================

  delete positions[
    symbol
  ];

  updateDrawdown();

  checkDailyLoss();

  // ==========================================================
  // CLOUD SAVE
  // ==========================================================

  await saveTrade(
    record
  );

  await cloudJournal({

    type:
      'CLOSE',

    ...record
  });

  await saveCloudState();

  // ==========================================================
  // TELEGRAM CLOSE
  // ==========================================================

  tg(
    `${profit >= 0 ? '✅' : '❌'} <b>LOMY V4.5 CLOSE</b>\n\n` +

    `<b>${symbol}</b>\n` +

    `Reason: ${reason}\n` +

    `PnL: $${profit.toFixed(2)}\n` +

    `PnL %: ${profitPct.toFixed(2)}%\n\n` +

    `MFE: ${p.mfePct.toFixed(2)}%\n` +

    `MAE: ${p.maePct.toFixed(2)}%\n` +

    `Scalp: ${p.scalpScore}/100\n` +

    `Cash: $${cash.toFixed(2)}`
  );

  // ==========================================================
  // LOSS-STREAK PROTECTION
  // ==========================================================

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

  const analysis =
    analyze(
      arr,
      symbol
    );

  if (!analysis) {
    return;
  }

  let decision =
    'REJECT';

  // ==========================================================
  // FINAL PRE-POOL CONDITIONS
  // ==========================================================

  if (
    analysis.eligible &&
    !manualPause &&
    !dailyPause &&
    !cooldownActive() &&
    !positions[symbol] &&
    !symbolCooling(symbol)
  ) {

    addCandidate(
      symbol,
      analysis,
      closeTime
    );

    decision =
      'POOL';
  }

  // ==========================================================
  // DASHBOARD ROW
  // ==========================================================

  const takerText =
    analysis.takerBuyRatio ===
      null
      ? 'N/A'
      : (
          analysis.takerBuyRatio *
          100
        ).toFixed(1);

  latest.push({

    symbol,

    score:
      analysis.score,

    scalpScore:
      analysis.scalpScore,

    grade:
      analysis.grade,

    decision,

    cmo:
      analysis.cmo.toFixed(1),

    volume:
      analysis.volumeRatio.toFixed(2),

    volumeAcceleration:
      analysis.volumeAcceleration.toFixed(2),

    priceAcceleration:
      analysis.priceAcceleration.toFixed(2),

    takerBuy:
      takerText,

    ema:
      analysis.emaDistance.toFixed(2),

    breakout:
      analysis.breakoutPct.toFixed(2),

    breakoutQuality:
      analysis.breakoutQuality,

    regime:
      analysis.regime,

    warnings:
      [
        ...analysis.warnings,
        ...analysis.scalpWarnings
      ].join(', ')
  });

  latest =
    latest
      .sort(
        (a, b) => {

          const scoreA =
            a.score +
            a.scalpScore * 0.20;

          const scoreB =
            b.score +
            b.scalpScore * 0.20;

          return (
            scoreB -
            scoreA
          );
        }
      )
      .slice(
        0,
        80
      );

  // ==========================================================
  // FULL CLOUD JOURNAL
  // ==========================================================

  cloudJournal({

    type:
      'ANALYSIS',

    symbol,

    decision,

    score:
      analysis.score,

    scalpScore:
      analysis.scalpScore,

    grade:
      analysis.grade,

    eligible:
      analysis.eligible,

    cmo:
      analysis.cmo,

    volumeRatio:
      analysis.volumeRatio,

    volumeRatio20:
      analysis.volumeRatio20,

    volumeAcceleration:
      analysis.volumeAcceleration,

    priceAcceleration:
      analysis.priceAcceleration,

    takerBuyRatio:
      analysis.takerBuyRatio,

    orderFlowMomentum:
      analysis.orderFlowMomentum,

    bodyRatio:
      analysis.bodyRatio,

    upperWickRatio:
      analysis.upperWickRatio,

    closeLocation:
      analysis.closeLocation,

    emaDistance:
      analysis.emaDistance,

    emaSlope:
      analysis.emaSlope,

    breakoutPct:
      analysis.breakoutPct,

    breakoutQuality:
      analysis.breakoutQuality,

    atrPct:
      analysis.atrPct,

    ext5:
      analysis.ext5,

    ext10:
      analysis.ext10,

    regime:
      analysis.regime,

    breadth:
      analysis.breadth,

    reasons:
      analysis.reasons,

    warnings:
      analysis.warnings,

    scalpReasons:
      analysis.scalpReasons,

    scalpWarnings:
      analysis.scalpWarnings
  });
}

// ============================================================
// MINI TICKER WEBSOCKET
// Live price + quote volume + position management
// ============================================================

function connectMini() {

  if (
    shuttingDown
  ) {
    return;
  }

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
          ignored(symbol)
        ) {
          continue;
        }

        const price =
          n(
            item.c
          );

        const quoteVolume =
          n(
            item.q
          );

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
          2000
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
    error => {

      console.error(
        'MINI:',
        error.message
      );
    }
  );
}

// ============================================================
// KLINE WEBSOCKET CONTROL
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
            Array
              .from(
                subscribed
              )
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
        Object
          .prototype
          .hasOwnProperty
          .call(
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

      const k =
        event.k;

      // ========================================================
      // IMPORTANT V4.5:
      // Store Binance taker-buy flow from closed kline.
      // ========================================================

      const candle = {

        open:
          n(
            k.o
          ),

        high:
          n(
            k.h
          ),

        low:
          n(
            k.l
          ),

        close:
          n(
            k.c
          ),

        volume:
          n(
            k.v
          ),

        quoteVolume:
          n(
            k.q
          ),

        trades:
          n(
            k.n
          ),

        takerBuyBase:
          n(
            k.V
          ),

        takerBuyQuote:
          n(
            k.Q
          ),

        closeTime:
          n(
            k.T
          )
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
    error => {

      console.error(
        'KLINE:',
        error.message
      );
    }
  );
}

// ============================================================
// UNIVERSE SELECTION
// ============================================================

function topSymbols() {

  let result =
    Array
      .from(
        tickers.entries()
      )
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
        row =>
          row[0]
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

// ============================================================
// REBALANCE UNIVERSE
// ============================================================

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

        // Never unsubscribe a live position.
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
// PERIODIC CLOUD / RISK TASKS
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
  () => {

    calculateMarketRegime();

  },
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
// WEBSOCKET WATCHDOG
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

        miniWs
          .terminate();

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

        klineWs
          .terminate();

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
// API - PAUSE
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
      success:
        true
    });
  }
);

// ============================================================
// API - RESUME
// ============================================================

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
      success:
        true
    });
  }
);

// ============================================================
// API - EMERGENCY CLOSE
// ============================================================

app.post(
  '/api/emergency-close',
  async (
    req,
    res
  ) => {

    const list =
      Object.keys(
        positions
      );

    let closed = 0;

    for (
      const symbol
      of list
    ) {

      const price =
        tickers.get(
          symbol
        )?.price ||
        positions[
          symbol
        ].lastPrice;

      if (
        !price
      ) {
        continue;
      }

      await closeTrade(
        symbol,
        price,
        'EMERGENCY_CLOSE'
      );

      closed++;
    }

    res.json({

      success:
        true,

      closed
    });
  }
);

// ============================================================
// API DATA
// ============================================================

app.get(
  '/api/data',
  async (
    req,
    res
  ) => {

    // ========================================================
    // FIXED CLOSED COUNT
    // Use totalTrades as canonical counter.
    // ========================================================

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
              exitTime:
                -1
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
              time:
                -1
            })
            .limit(
              C.journalLimitDashboard
            )
            .toArray();

      } catch (
        error
      ) {

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
        equity()
          .toFixed(
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
        warmupStats
          .cloudLoaded,

      restReady:
        warmupStats
          .restLoaded,

      restRequests:
        warmupStats
          .restRequests,

      warmupQueue:
        warmupQueue.length,

      candidatePool:
        candidatePool.size,

      marketRegime,

      dailyPnL:
        dailyPnL
          .toFixed(
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
          Number(
            winRate.toFixed(
              2
            )
          ),

        profitFactor:
          Number(
            profitFactor.toFixed(
              2
            )
          )
      },

      latest,

      recentTrades,

      recentJournal
    });
  }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
  '/health',
  (
    req,
    res
  ) => {

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
        C.paperTrading
          ? 'PAPER'
          : 'DISABLED',

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
        equity()
          .toFixed(
            2
          )
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

    res.send(`
<!doctype html>

<html>

<head>

<meta charset="utf-8">

<meta
  name="viewport"
  content="width=device-width,initial-scale=1"
>

<title>
LOMY V4.5
</title>

<style>

*{
  box-sizing:border-box
}

body{
  margin:0;
  background:#0b0e11;
  color:#eaecef;
  font-family:Arial;
  padding:16px;
  text-align:center
}

h1{
  color:#f3ba2f
}

.banner{
  background:#f3ba2f;
  color:#111;
  padding:12px;
  border-radius:10px;
  font-weight:bold;
  max-width:1400px;
  margin:auto
}

.status{
  margin:10px;
  font-weight:bold
}

.green{
  color:#0ecb81
}

.red{
  color:#f6465d
}

.yellow{
  color:#f3ba2f
}

.blue{
  color:#4da3ff
}

.grid{
  display:grid;
  grid-template-columns:
    repeat(
      auto-fit,
      minmax(
        140px,
        1fr
      )
    );
  gap:10px;
  max-width:1400px;
  margin:18px auto
}

.card{
  background:#1e2329;
  border:1px solid #2b3139;
  padding:14px;
  border-radius:10px
}

.label{
  font-size:11px;
  color:#848e9c
}

.value{
  font-size:21px;
  font-weight:bold;
  margin-top:7px
}

button{
  border:0;
  padding:12px 18px;
  margin:5px;
  border-radius:8px;
  font-weight:bold;
  cursor:pointer
}

.pause{
  background:#f3ba2f
}

.resume{
  background:#0ecb81
}

.close{
  background:#f6465d;
  color:#fff
}

table{
  width:100%;
  max-width:1600px;
  margin:20px auto;
  border-collapse:collapse;
  background:#1e2329
}

th{
  background:#2b3139;
  color:#848e9c
}

td,
th{
  font-size:11px;
  padding:8px;
  border-bottom:
    1px solid #2b3139;
  white-space:nowrap
}

</style>

</head>

<body>

<h1>
🤖 LOMY V4.5 HIGH-THROUGHPUT SCALP
</h1>

<div class="banner">
CLOUD MEMORY • ULTRA-FAST SCALP • VOLUME ACCELERATION • PRICE ACCELERATION • ORDER FLOW • BREAKOUT QUALITY • PAPER ONLY
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
Market regime...
</div>

<div
  id="cooldown"
  class="status"
>
Cooldown...
</div>

<div class="grid">

<div class="card">
<div class="label">CASH</div>
<div class="value" id="cash">$0</div>
</div>

<div class="card">
<div class="label">EQUITY</div>
<div class="value" id="equity">$0</div>
</div>

<div class="card">
<div class="label">CLOSED</div>
<div class="value" id="closed">0</div>
</div>

<div class="card">
<div class="label">WIN RATE</div>
<div class="value" id="win">0%</div>
</div>

<div class="card">
<div class="label">NET PROFIT</div>
<div class="value" id="profit">$0</div>
</div>

<div class="card">
<div class="label">PROFIT FACTOR</div>
<div class="value" id="pf">0</div>
</div>

<div class="card">
<div class="label">OPEN</div>
<div class="value" id="open">0</div>
</div>

<div class="card">
<div class="label">MAX POSITIONS</div>
<div class="value" id="maxPositions">8</div>
</div>

<div class="card">
<div class="label">READY</div>
<div class="value" id="ready">0</div>
</div>

<div class="card">
<div class="label">WS SYMBOLS</div>
<div class="value" id="symbols">0</div>
</div>

<div class="card">
<div class="label">CLOUD READY</div>
<div class="value" id="cloudReady">0</div>
</div>

<div class="card">
<div class="label">REST RECOVERY</div>
<div class="value" id="rest">0</div>
</div>

<div class="card">
<div class="label">BREADTH</div>
<div class="value" id="breadth">0%</div>
</div>

<div class="card">
<div class="label">CANDIDATE POOL</div>
<div class="value" id="pool">0</div>
</div>

<div class="card">
<div class="label">TODAY PNL</div>
<div class="value" id="daily">$0</div>
</div>

<div class="card">
<div class="label">MAX DD</div>
<div class="value" id="dd">0%</div>
</div>

<div class="card">
<div class="label">BATCH</div>
<div class="value" id="batch">0/16</div>
</div>

<div class="card">
<div class="label">LOSS STREAK</div>
<div class="value" id="loss">0/3</div>
</div>

<div class="card">
<div class="label">EARLY EXITS</div>
<div class="value" id="early">0</div>
</div>

<div class="card">
<div class="label">BREAK EVEN</div>
<div class="value" id="be">0</div>
</div>

<div class="card">
<div class="label">TRAILING</div>
<div class="value" id="trail">0</div>
</div>

</div>

<button
  class="pause"
  onclick="post('/api/pause')"
>
⏸ PAUSE
</button>

<button
  class="resume"
  onclick="post('/api/resume')"
>
▶ RESUME
</button>

<button
  class="close"
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
<th>Score</th>
<th>Scalp</th>
<th>Status</th>
<th>CMO</th>
<th>Volume</th>
<th>Vol Accel</th>
<th>Price Accel</th>
<th>Buy Flow</th>
<th>EMA%</th>
<th>Breakout%</th>
<th>BO Quality</th>
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

async function post(
  url
){

  await fetch(
    url,
    {
      method:
        'POST'
    }
  );

  load();
}

async function closeAll(){

  if(
    !confirm(
      'Close all PAPER positions?'
    )
  ){
    return;
  }

  await post(
    '/api/emergency-close'
  );
}

async function load(){

  try{

    const response=
      await fetch(
        '/api/data'
      );

    const data=
      await response.json();

    document
      .getElementById(
        'cash'
      )
      .innerText=
        '$'+data.cash;

    document
      .getElementById(
        'equity'
      )
      .innerText=
        '$'+data.equity;

    document
      .getElementById(
        'closed'
      )
      .innerText=
        data.stats.totalTrades;

    document
      .getElementById(
        'win'
      )
      .innerText=
        data.stats.winRate+'%';

    document
      .getElementById(
        'profit'
      )
      .innerText=
        '$'+
        Number(
          data.stats.netProfit
        ).toFixed(2);

    document
      .getElementById(
        'pf'
      )
      .innerText=
        data.stats.profitFactor;

    document
      .getElementById(
        'open'
      )
      .innerText=
        data.open;

    document
      .getElementById(
        'maxPositions'
      )
      .innerText=
        data.maxPositions;

    document
      .getElementById(
        'ready'
      )
      .innerText=
        data.ready;

    document
      .getElementById(
        'symbols'
      )
      .innerText=
        data.symbols;

    document
      .getElementById(
        'cloudReady'
      )
      .innerText=
        data.cloudReady;

    document
      .getElementById(
        'rest'
      )
      .innerText=
        data.restReady;

    document
      .getElementById(
        'breadth'
      )
      .innerText=
        data.marketRegime.breadth+
        '%';

    document
      .getElementById(
        'pool'
      )
      .innerText=
        data.candidatePool;

    document
      .getElementById(
        'daily'
      )
      .innerText=
        '$'+data.dailyPnL;

    document
      .getElementById(
        'dd'
      )
      .innerText=
        Number(
          data.stats.maxDrawdown
        ).toFixed(2)+
        '%';

    document
      .getElementById(
        'batch'
      )
      .innerText=
        data.entriesSinceCooldown+
        '/'+
        data.entriesBeforeCooldown;

    document
      .getElementById(
        'loss'
      )
      .innerText=
        data.lossStreak+
        '/3';

    document
      .getElementById(
        'early'
      )
      .innerText=
        data.stats.earlyFailureExits;

    document
      .getElementById(
        'be'
      )
      .innerText=
        data.stats.breakEvenMoves;

    document
      .getElementById(
        'trail'
      )
      .innerText=
        data.stats.trailingActivations;

    const cloudEl=
      document.getElementById(
        'cloud'
      );

    cloudEl.innerText=
      data.cloudConnected
        ? '☁️ CLOUD DB CONNECTED'
        : '🔴 CLOUD DB DISCONNECTED';

    cloudEl.className=
      data.cloudConnected
        ? 'status green'
        : 'status red';

    const wsEl=
      document.getElementById(
        'ws'
      );

    wsEl.innerText=
      (
        data.miniConnected &&
        data.klineConnected
      )
        ? '🟢 MARKET WEBSOCKETS LIVE'
        : '🔴 MARKET CONNECTING';

    wsEl.className=
      (
        data.miniConnected &&
        data.klineConnected
      )
        ? 'status green'
        : 'status red';

    const regimeEl=
      document.getElementById(
        'regime'
      );

    regimeEl.innerText=
      'MARKET REGIME: '+
      data.marketRegime.regime+
      ' • BREADTH '+
      data.marketRegime.breadth+
      '%';

    regimeEl.className=
      data.marketRegime.regime===
        'RISK_ON'
        ? 'status green'
        : data.marketRegime.regime===
          'NEUTRAL'
          ? 'status yellow'
          : 'status red';

    const cooldownEl=
      document.getElementById(
        'cooldown'
      );

    cooldownEl.innerText=
      data.cooldown
        ? '🧠 COOLDOWN '+
          data.cooldownReason+
          ' • '+
          data.cooldownMinutes+
          ' MIN'
        : '✅ SMART COOLDOWN READY';

    cooldownEl.className=
      data.cooldown
        ? 'status yellow'
        : 'status green';

    const rows=
      document.getElementById(
        'rows'
      );

    rows.innerHTML='';

    if(
      !data.latest.length
    ){

      rows.innerHTML=
        '<tr>'+
        '<td colspan="15">'+
        'Warmup '+
        data.ready+
        ' / '+
        data.symbols+
        '</td>'+
        '</tr>';

      return;
    }

    data.latest.forEach(
      item=>{

        rows.innerHTML+=
          '<tr>'+

          '<td><b>'+
          item.symbol+
          '</b></td>'+

          '<td>'+
          item.grade+
          '</td>'+

          '<td>'+
          item.score+
          '</td>'+

          '<td>'+
          item.scalpScore+
          '</td>'+

          '<td>'+
          item.decision+
          '</td>'+

          '<td>'+
          item.cmo+
          '</td>'+

          '<td>'+
          item.volume+
          'x</td>'+

          '<td>'+
          item.volumeAcceleration+
          'x</td>'+

          '<td>'+
          item.priceAcceleration+
          '</td>'+

          '<td>'+
          item.takerBuy+
          '%</td>'+

          '<td>'+
          item.ema+
          '%</td>'+

          '<td>'+
          item.breakout+
          '%</td>'+

          '<td>'+
          item.breakoutQuality+
          '</td>'+

          '<td>'+
          item.regime+
          '</td>'+

          '<td>'+
          item.warnings+
          '</td>'+

          '</tr>';
      }
    );

  }catch(error){

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
    `);
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
    `${signal} - saving V4.5 cloud memory`
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

    await mongoClient
      ?.close();

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

    console.log('');
    console.log(
      '================================================='
    );
    console.log(
      'LOMY V4.5 HIGH-THROUGHPUT SCALP'
    );
    console.log(
      '================================================='
    );

    console.log(
      'Execution: PAPER ONLY'
    );

    console.log(
      'Cloud Memory: MongoDB'
    );

    console.log(
      'Historical Recovery: Binance REST'
    );

    console.log(
      'Live Market: Binance WebSocket'
    );

    console.log(
      `Universe: TOP ${C.universeSize}`
    );

    console.log(
      `Max Positions: ${C.maxPositions}`
    );

    console.log(
      `Max Entries / Cycle: ${C.maxEntriesPerCycle}`
    );

    console.log(
      'CMO(9): ON'
    );

    console.log(
      'Volume SMA(10): ON'
    );

    console.log(
      'Volume Acceleration: ON'
    );

    console.log(
      'Price Acceleration: ON'
    );

    console.log(
      'Taker Buy Order Flow: ON'
    );

    console.log(
      'Breakout Quality: ON'
    );

    console.log(
      'Scalp Confirmation: ON'
    );

    console.log(
      'Market Regime: ON'
    );

    console.log(
      'BTC Context: ON'
    );

    console.log(
      'Fresh Revalidation: ON'
    );

    console.log(
      'Dynamic Ranking: ON'
    );

    console.log(
      'ATR Risk: ON'
    );

    console.log(
      'Profit Lock: ON'
    );

    console.log(
      'Break Even: ON'
    );

    console.log(
      'Trailing Protection: ON'
    );

    console.log(
      'Anti Chase: ON'
    );

    console.log(
      'Smart Cooldown: ON'
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
        `🚀 <b>LOMY V4.5 HIGH-THROUGHPUT SCALP</b>\n\n` +

        `☁️ Cloud Memory: <b>CONNECTED</b>\n` +

        `📡 Live Market: <b>WEBSOCKET</b>\n` +

        `📚 Smart Warmup: <b>ON</b>\n` +

        `⚡ Scalp Engine: <b>ON</b>\n` +

        `📈 Volume Acceleration: <b>ON</b>\n` +

        `🚀 Price Acceleration: <b>ON</b>\n` +

        `🐳 Order Flow: <b>ON</b>\n` +

        `🧠 Market Regime: <b>ON</b>\n` +

        `🛡 Risk Engine: <b>ON</b>\n` +

        `💰 Execution: <b>PAPER ONLY</b>\n\n` +

        `Equity: $${equity().toFixed(2)}`
      );

    } catch (
      error
    ) {

      console.error(
        'STARTUP FAILED:',
        error
      );

      tg(
        `🔴 <b>LOMY V4.5 CLOUD STARTUP FAILED</b>\n` +
        error.message
      );
    }
  }
);
