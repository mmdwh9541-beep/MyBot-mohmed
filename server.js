'use strict';

require('dotenv').config();

const express = require('express');
const axios = require('axios');
const { MongoClient } = require('mongodb');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 5000);
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN || '';
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID || '';
const MONGODB_URI = process.env.MONGODB_URI || '';
const MONGODB_DB = process.env.MONGODB_DB || 'lomy';

// ============================================================
// LOMY V6.3.2 — PROFIT MANAGER
// ENTRY STRATEGY = V6.3.1 FROZEN / UNCHANGED
// POST-ENTRY CHANGES ONLY:
// 1) Cost-aware capital protection
// 2) Staged dynamic trailing
// 3) Conservative failed-follow-through stop
// 4) Entry-candle + position-path telemetry
// PAPER ONLY
// ============================================================

const BINANCE_REST = 'https://data-api.binance.vision';

const C = Object.freeze({
  version: '6.3.2-PROFIT-MANAGER',
  stateKey: 'main-v632',

  paperTrading: true,

  // ==========================================================
  // STARTING CAPITAL
  // ==========================================================
  startingBalance: 1000,

  // ==========================================================
  // MARKET — SAME AS V6.3.1
  // ==========================================================
  universeSize: 140,
  minQuoteVolume: 500000,

  scanEveryMs: 2 * 60 * 1000,
  scanConcurrency: 4,
  maxEntriesPerScan: 2,

  universeRefreshMs: 10 * 60 * 1000,
  requestTimeoutMs: 10000,
  requestGapMs: 80,

  // ==========================================================
  // CYCLE — SAME
  // ==========================================================
  maxEntriesPerCycle: 20,
  maxPositions: 6,

  // ==========================================================
  // TIMEFRAMES — SAME
  // ==========================================================
  entryInterval: '15m',
  contextInterval: '1h',

  klineLimit15m: 80,
  klineLimit1h: 80,

  // ==========================================================
  // FROZEN V6.3.1 ENTRY CORE — DO NOT CHANGE
  // ==========================================================
  emaFast: 9,
  emaSlow: 21,

  cmoLength: 9,
  cmoBuyMin: 30,

  volumeSmaLength: 10,
  momentumVolumeMultiplier: 1.30,

  minBodyRatio: 0.50,

  breakoutLookback: 20,
  breakoutBufferPct: 0.03,

  maxMomentumExtensionAtr: 0.85,

  retestTolerancePct: 0.20,
  retestLookbackBars: 3,
  retestMinVolumeRatio: 1.00,

  maxEntryDriftPct: 0.18,

  // ==========================================================
  // ORIGINAL RISK MODEL
  // HARD EMERGENCY STOP IS STILL AVAILABLE
  // ==========================================================
  feePct: 0.001,
  slippagePct: 0.0005,

  riskPerTradePct: 0.0045,
  maxAllocationPct: 0.22,

  minEmergencyStopPct: 0.0065,
  maxEmergencyStopPct: 0.018,

  atrStopMultiplier: 1.65,
  structureBufferAtr: 0.20,

  // ==========================================================
  // V6.3.2 PROFIT MANAGER
  // ==========================================================

  // CAPITAL PROTECTION
  capitalProtectionTriggerPct: 0.60,

  // Target actual modeled net profit after fees/slippage.
  capitalNetLockPct: 0.12,

  // ==========================================================
  // STAGED TRAILING
  // ==========================================================
  trailingStartPct: 0.85,

  trailingAtrMultiplier: 0.90,

  trailingGapMinPct: 0.28,
  trailingGapMaxPct: 0.55,

  // Stage thresholds based on MFE
  trailStage2MfePct: 1.25,
  trailStage3MfePct: 1.75,
  trailStage4MfePct: 2.50,
  trailStage5MfePct: 3.50,

  // Maximum trail gap by stage
  trailStage1MaxGapPct: 0.55,
  trailStage2MaxGapPct: 0.48,
  trailStage3MaxGapPct: 0.42,
  trailStage4MaxGapPct: 0.36,
  trailStage5MaxGapPct: 0.32,

  // Minimum fraction of MFE that must be protected
  trailStage1LockFraction: 0.40,
  trailStage2LockFraction: 0.50,
  trailStage3LockFraction: 0.58,
  trailStage4LockFraction: 0.65,
  trailStage5LockFraction: 0.70,

  // ==========================================================
  // FAILED FOLLOW-THROUGH CONTROL
  // ==========================================================
  failureStopEnabled: true,

  // Do not judge the trade too early
  failureMinAgeMinutes: 60,

  // Trade must have failed to make meaningful progress
  failureMaxMfePct: 0.25,

  // Close before the original full emergency loss
  failureDrawdownPct: 1.35,

  // ==========================================================
  // TELEMETRY ONLY
  // ==========================================================
  positionTelemetryEveryMs: 5 * 60 * 1000,

  // ==========================================================
  // EXECUTION QUALITY — SAME AS V6.3.1
  // ==========================================================
  maxEntrySpreadBps: 7.0,

  depthLimit: 50,
  aggTradeLimit: 100,
  microTopLevels: 12,

  microFlowWindowMs: 45 * 1000,
  microMinRecentTrades: 6,

  stressedSpreadBps: 18,
  stressedMinSideDepthUSDT: 1200,

  severeSellBookImbalance: -0.35,
  severeMicropriceEdgeBps: -6,

  sarReduceFromBps: 12,
  sarRejectBps: 25,
  sarMinSizeScale: 0.40,

  // ==========================================================
  // SHADOW ONLY — DOES NOT BLOCK ENTRY
  // ==========================================================
  atrPctShadowThreshold: 0.50,

  // ==========================================================
  // ACCOUNT
  // ==========================================================
  dailyLossLimitPct: 0.035,
  maxAccountDrawdownPct: 0.07,

  symbolLossCooldownMs: 45 * 60 * 1000,

  // ==========================================================
  // BINANCE HEALTH
  // ==========================================================
  binance429PauseMs: 5 * 60 * 1000,
  binance418PauseMs: 30 * 60 * 1000,

  // ==========================================================
  // RUNTIME
  // ==========================================================
  pricePollMs: 5000,
  stateSaveMs: 30000
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


const sleep =
  ms =>
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

  const x =
    Number(
      value
    );

  return Number.isFinite(
    x
  )
    ? x
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
      ) *
      100
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
// MODELED TRADE COST
// ============================================================

function grossMoveNeededForNetPct(
  targetNetPct
) {

  const target =
    1 +
    targetNetPct /
    100;


  const costFactor =

    (
      1 -
      C.feePct
    )

    *

    (
      1 -
      C.slippagePct
    )

    *

    (
      1 -
      C.feePct
    );


  return (

    (
      target /
      costFactor
    )

    -

    1

  ) *
    100;
}


function modeledNetPctAtRawMove(
  rawMovePct
) {

  const factor =

    (
      1 -
      C.feePct
    )

    *

    (
      1 -
      C.slippagePct
    )

    *

    (
      1 -
      C.feePct
    )

    *

    (
      1 +
      rawMovePct /
      100
    );


  return (

    factor -
    1

  ) *
    100;
}


// ============================================================
// ACCOUNT
// ============================================================

let cash =
  C.startingBalance;


let positions =
  {};


let stats = {

  totalTrades:
    0,

  wins:
    0,

  losses:
    0,

  grossProfit:
    0,

  grossLoss:
    0,

  netProfit:
    0,

  fees:
    0,

  maxDrawdown:
    0,

  breakEvenMoves:
    0,

  trailingActivations:
    0,

  failureStops:
    0,

  cycleCount:
    1,

  setupStats:
    {},

  exitStats:
    {}
};


let currentDay =
  utcDay();


let dailyStartEquity =
  C.startingBalance;


let dailyPnL =
  0;


let peakEquity =
  C.startingBalance;


let manualPause =
  false;


let dailyPause =
  false;


let drawdownPause =
  false;


// ============================================================
// CYCLE
// ============================================================

let cycle = {

  id:
    1,

  state:
    'SCANNING',

  entries:
    0,

  startedAt:
    Date.now(),

  lastFreshScanAt:
    0,

  lastScanCandidates:
    0,

  lastUniverseSource:
    null,

  lastScanned:
    0
};


const lastLossBySymbol =
  {};


let universe =
  [];


let universeUpdatedAt =
  0;


let scanning =
  false;


let positionManagerRunning =
  false;


let shuttingDown =
  false;


// ============================================================
// BINANCE HEALTH
// ============================================================

const exchangeHealth = {

  BINANCE: {

    blockedUntil:
      0,

    failures:
      0,

    lastError:
      null,

    lastSuccess:
      0
  }
};


// ============================================================
// DATABASE
// ============================================================

let mongoClient =
  null;


let db =
  null;


let cloudConnected =
  false;


// ============================================================
// EQUITY
// ============================================================

function equity(
  prices = {}
) {

  let value =
    cash;


  for (
    const position
    of Object.values(
      positions
    )
  ) {

    const price =

      n(

        prices[
          position.symbol
        ],

        position.lastPrice ||
        position.entryPrice
      );


    value +=

      position.qty *
      price;
  }


  return value;
}


// ============================================================
// ACCOUNT GUARDS
// ============================================================

function resetDailyIfNeeded(
  prices = {}
) {

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


  dailyStartEquity =
    equity(
      prices
    );


  dailyPnL =
    0;


  dailyPause =
    false;
}


function updateAccountGuards(
  prices = {}
) {

  const eq =
    equity(
      prices
    );


  peakEquity =

    Math.max(

      peakEquity,

      eq
    );


  const ddPct =

    peakEquity >
    0

      ? (
          (
            peakEquity -
            eq
          ) /
          peakEquity
        ) *
        100

      : 0;


  stats.maxDrawdown =

    Math.max(

      stats.maxDrawdown,

      ddPct
    );


  if (

    dailyStartEquity >
      0

    &&

    -dailyPnL /
      dailyStartEquity >=
      C.dailyLossLimitPct

  ) {

    dailyPause =
      true;
  }


  if (

    ddPct /
      100 >=
      C.maxAccountDrawdownPct

  ) {

    drawdownPause =
      true;
  }
}


function entryBlocked() {

  return (

    manualPause ||

    dailyPause ||

    drawdownPause ||

    cycle.state !==
      'SCANNING'
  );
}


// ============================================================
// INDICATORS
// ============================================================

function ema(
  values,
  period
) {

  if (

    !Array.isArray(
      values
    )

    ||

    values.length <
      period

  ) {

    return null;
  }


  let result =

    values
      .slice(
        0,
        period
      )
      .reduce(
        (
          sum,
          value
        ) =>
          sum +
          value,
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
      values.length;

    i++

  ) {

    result =

      (
        values[i] -
        result
      ) *

      multiplier +

      result;
  }


  return result;
}


function cmo(
  closes,
  period = 9
) {

  if (

    !Array.isArray(
      closes
    )

    ||

    closes.length <
      period +
      1

  ) {

    return null;
  }


  let up =
    0;


  let down =
    0;


  for (

    let i =
      closes.length -
      period;

    i <
      closes.length;

    i++

  ) {

    const difference =

      closes[i] -

      closes[
        i - 1
      ];


    if (
      difference >
      0
    ) {

      up +=
        difference;

    } else {

      down +=

        Math.abs(
          difference
        );
    }
  }


  const total =
    up +
    down;


  if (
    total ===
    0
  ) {

    return 0;
  }


  return (

    (
      up -
      down
    ) /

    total

  ) *
    100;
}


function atr(
  candles,
  period = 14
) {

  if (

    !Array.isArray(
      candles
    )

    ||

    candles.length <
      period +
      1

  ) {

    return null;
  }


  const values =
    [];


  for (

    let i =
      candles.length -
      period;

    i <
      candles.length;

    i++

  ) {

    const current =
      candles[i];


    const previous =

      candles[
        i - 1
      ];


    values.push(

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


  return (

    values.reduce(
      (
        sum,
        value
      ) =>
        sum +
        value,
      0
    ) /

    values.length
  );
}


function candleBodyRatio(
  candle
) {

  const range =

    candle.high -
    candle.low;


  if (
    range <=
    0
  ) {

    return 0;
  }


  return (

    Math.abs(

      candle.close -
      candle.open

    ) /

    range
  );
}


function smaVolume(
  candles,
  period
) {

  const base =

    candles.slice(
      0,
      -1
    );


  if (
    base.length <
    period
  ) {

    return null;
  }


  const rows =

    base.slice(
      -period
    );


  return (

    rows.reduce(
      (
        sum,
        candle
      ) =>
        sum +
        n(
          candle.volume
        ),
      0
    ) /

    rows.length
  );
}


// ============================================================
// BINANCE PUBLIC REST
// ============================================================

function exchangeAvailable() {

  return (

    Date.now() >=

    exchangeHealth
      .BINANCE
      .blockedUntil
  );
}


function markExchangeSuccess() {

  exchangeHealth
    .BINANCE
    .failures =
      0;


  exchangeHealth
    .BINANCE
    .lastError =
      null;


  exchangeHealth
    .BINANCE
    .lastSuccess =
      Date.now();
}


function markBinanceFailure(
  error
) {

  const health =
    exchangeHealth.BINANCE;


  health.failures++;


  health.lastError =

    error.message ||

    String(
      error
    );


  const status =
    error.response?.status;


  const retryAfter =

    Number(

      error.response
        ?.headers
        ?.['retry-after']
    );


  let pause =
    0;


  if (
    status ===
    418
  ) {

    pause =

      Number.isFinite(
        retryAfter
      )

      &&

      retryAfter >
      0

        ? retryAfter *
          1000

        : C.binance418PauseMs;

  } else if (
    status ===
    429
  ) {

    pause =

      Number.isFinite(
        retryAfter
      )

      &&

      retryAfter >
      0

        ? retryAfter *
          1000

        : C.binance429PauseMs;

  } else if (

    !status ||

    status >=
      500

  ) {

    pause =
      20 *
      1000;
  }


  if (
    pause >
    0
  ) {

    health.blockedUntil =

      Math.max(

        health.blockedUntil,

        Date.now() +
        pause
      );


    console.warn(

      `BINANCE paused ` +

      `${Math.ceil(
        pause /
        1000
      )}s | ` +

      `${status || error.message}`
    );
  }
}


async function binanceGet(
  path,
  params = {}
) {

  if (
    !exchangeAvailable()
  ) {

    throw new Error(
      'BINANCE_TEMP_BLOCKED'
    );
  }


  try {

    const response =

      await axios.get(

        `${BINANCE_REST}${path}`,

        {

          params,

          timeout:
            C.requestTimeoutMs
        }
      );


    markExchangeSuccess();


    return response.data;

  } catch (
    error
  ) {

    markBinanceFailure(
      error
    );


    throw error;
  }
}


// ============================================================
// KLINES
// ============================================================

function mapKline(
  row
) {

  return {

    openTime:
      n(
        row[0]
      ),

    open:
      n(
        row[1]
      ),

    high:
      n(
        row[2]
      ),

    low:
      n(
        row[3]
      ),

    close:
      n(
        row[4]
      ),

    volume:
      n(
        row[5]
      ),

    closeTime:
      n(
        row[6]
      ),

    quoteVolume:
      n(
        row[7]
      ),

    source:
      'BINANCE_PUBLIC'
  };
}


async function fetchClosedKlines(
  symbol,
  interval,
  limit
) {

  const data =

    await binanceGet(

      '/api/v3/klines',

      {

        symbol,

        interval,

        limit
      }
    );


  const now =
    Date.now();


  const rows =

    data
      .map(
        mapKline
      )
      .filter(
        candle =>
          candle.closeTime <
          now
      );


  if (
    rows.length <
    30
  ) {

    throw new Error(

      `BINANCE_KLINES_NOT_READY ${symbol} ${interval}`
    );
  }


  return {

    source:
      'BINANCE_PUBLIC',

    candles:
      rows
  };
}


// ============================================================
// ENTRY CANDLE TELEMETRY
// DOES NOT BLOCK OR CHANGE ENTRY
// ============================================================

async function fetchLiveEntryCandle(
  symbol
) {

  const data =

    await binanceGet(

      '/api/v3/klines',

      {

        symbol,

        interval:
          C.entryInterval,

        limit:
          2
      }
    );


  if (

    !Array.isArray(
      data
    )

    ||

    !data.length

  ) {

    return null;
  }


  return mapKline(
    data.at(
      -1
    )
  );
}


function entryCandleTelemetry(
  candle,
  entryPrice
) {

  if (
    !candle
  ) {

    return null;
  }


  const range =

    candle.high -
    candle.low;


  const entryPositionPct =

    range >
    0

      ? (
          (
            entryPrice -
            candle.low
          ) /
          range
        ) *
        100

      : null;


  const moveFromOpenPct =

    candle.open >
    0

      ? pct(

          entryPrice -
          candle.open,

          candle.open
        )

      : null;


  const distanceFromHighPct =

    candle.high >
    0

      ? pct(

          candle.high -
          entryPrice,

          candle.high
        )

      : null;


  return {

    observedAt:
      Date.now(),

    openTime:
      candle.openTime,

    closeTime:
      candle.closeTime,

    open:
      candle.open,

    highSoFar:
      candle.high,

    lowSoFar:
      candle.low,

    lastAtObservation:
      candle.close,

    entryPrice,

    entryPositionPct,

    moveFromOpenPct,

    distanceFromHighPct,

    note:
      'LIVE_15M_SNAPSHOT_AFTER_ENTRY_NO_LOOKAHEAD'
  };
}


// ============================================================
// UNIVERSE
// ============================================================

async function universeFromBinance() {

  const rows =

    await binanceGet(
      '/api/v3/ticker/24hr'
    );


  return rows

    .filter(
      row =>
        row.symbol
          ?.endsWith(
            'USDT'
          )
    )

    .filter(
      row =>
        !IGNORED.has(
          row.symbol
        )
    )

    .map(
      row => ({

        symbol:
          row.symbol,

        quoteVolume:
          n(
            row.quoteVolume
          )
      })
    )

    .filter(
      row =>
        row.quoteVolume >=
        C.minQuoteVolume
    )

    .sort(
      (
        a,
        b
      ) =>
        b.quoteVolume -
        a.quoteVolume
    )

    .slice(
      0,
      C.universeSize
    )

    .map(
      row =>
        row.symbol
    );
}


async function refreshUniverse(
  force = false
) {

  if (

    !force

    &&

    universe.length

    &&

    Date.now() -
      universeUpdatedAt <
      C.universeRefreshMs

  ) {

    return universe;
  }


  universe =

    await universeFromBinance();


  universeUpdatedAt =
    Date.now();


  cycle.lastUniverseSource =
    'BINANCE_PUBLIC';


  console.log(

    `UNIVERSE BINANCE_PUBLIC | ` +

    `${universe.length} symbols`
  );


  return universe;
}


// ============================================================
// PRICE
// ============================================================

async function getCurrentPrice(
  symbol
) {

  const row =

    await binanceGet(

      '/api/v3/ticker/price',

      {
        symbol
      }
    );


  const price =

    n(
      row.price
    );


  if (
    !price
  ) {

    throw new Error(
      'BINANCE_INVALID_PRICE'
    );
  }


  return {

    price,

    source:
      'BINANCE_PUBLIC'
  };
}


// ============================================================
// MICROSTRUCTURE
// EXECUTION SAFETY ONLY
// ============================================================

async function fetchDepth(
  symbol
) {

  const data =

    await binanceGet(

      '/api/v3/depth',

      {

        symbol,

        limit:
          C.depthLimit
      }
    );


  return {

    bids:

      (
        data.bids ||
        []
      )

        .map(
          row => [

            n(
              row[0]
            ),

            n(
              row[1]
            )
          ]
        )

        .filter(
          row =>
            row[0] >
              0 &&
            row[1] >
              0
        ),


    asks:

      (
        data.asks ||
        []
      )

        .map(
          row => [

            n(
              row[0]
            ),

            n(
              row[1]
            )
          ]
        )

        .filter(
          row =>
            row[0] >
              0 &&
            row[1] >
              0
        )
  };
}


async function fetchAggTrades(
  symbol
) {

  const rows =

    await binanceGet(

      '/api/v3/aggTrades',

      {

        symbol,

        limit:
          C.aggTradeLimit
      }
    );


  const cutoff =

    Date.now() -

    C.microFlowWindowMs;


  return (

    rows ||
    []

  )

    .map(
      row => ({

        price:
          n(
            row.p
          ),

        qty:
          n(
            row.q
          ),

        time:
          n(
            row.T
          ),

        buyerMaker:
          !!row.m
      })
    )

    .filter(
      row =>

        row.price >
          0

        &&

        row.qty >
          0

        &&

        row.time >=
          cutoff
    );
}


function depthMetrics(
  book
) {

  const bids =

    book.bids.slice(
      0,
      C.microTopLevels
    );


  const asks =

    book.asks.slice(
      0,
      C.microTopLevels
    );


  if (

    !bids.length ||

    !asks.length

  ) {

    return {

      ok:
        false,

      reason:
        'EMPTY_BOOK'
    };
  }


  const bestBid =
    bids[0][0];


  const bestAsk =
    asks[0][0];


  const mid =

    (
      bestBid +
      bestAsk
    ) /
    2;


  const spreadBps =

    mid >
    0

      ? (
          (
            bestAsk -
            bestBid
          ) /
          mid
        ) *
        10000

      : 99999;


  const weightedDepth =

    rows =>

      rows.reduce(
        (
          sum,
          [
            price,
            qty
          ],
          index
        ) =>

          sum +

          price *
          qty *

          (
            1 /
            (
              1 +
              index *
              0.12
            )
          ),

        0
      );


  const bidDepthUSDT =

    weightedDepth(
      bids
    );


  const askDepthUSDT =

    weightedDepth(
      asks
    );


  const totalDepth =

    bidDepthUSDT +
    askDepthUSDT;


  const imbalance =

    totalDepth >
    0

      ? (
          bidDepthUSDT -
          askDepthUSDT
        ) /
        totalDepth

      : 0;


  const bestBidQty =
    bids[0][1];


  const bestAskQty =
    asks[0][1];


  const micropriceDen =

    bestBidQty +
    bestAskQty;


  const microprice =

    micropriceDen >
    0

      ? (

          bestAsk *
          bestBidQty

          +

          bestBid *
          bestAskQty

        ) /
        micropriceDen

      : mid;


  const micropriceEdgeBps =

    mid >
    0

      ? (
          (
            microprice -
            mid
          ) /
          mid
        ) *
        10000

      : 0;


  const minSideDepth =

    Math.min(

      bidDepthUSDT,

      askDepthUSDT
    );


  let state =
    'NORMAL';


  if (

    spreadBps >=
      C.stressedSpreadBps

    ||

    minSideDepth <
      C.stressedMinSideDepthUSDT

  ) {

    state =
      'STRESSED';

  } else if (

    spreadBps <=
      5

    &&

    minSideDepth >=
      C.stressedMinSideDepthUSDT *
      5

  ) {

    state =
      'CALM';
  }


  return {

    ok:
      true,

    state,

    bestBid,

    bestAsk,

    mid,

    spreadBps,

    bidDepthUSDT,

    askDepthUSDT,

    imbalance,

    microprice,

    micropriceEdgeBps
  };
}


function aggressiveFlow(
  trades
) {

  if (

    !Array.isArray(
      trades
    )

    ||

    trades.length <
      C.microMinRecentTrades

  ) {

    return {

      sufficient:
        false,

      tradeCount:

        Array.isArray(
          trades
        )

          ? trades.length

          : 0,

      buyUSDT:
        0,

      sellUSDT:
        0,

      imbalance:
        0
    };
  }


  let buyUSDT =
    0;


  let sellUSDT =
    0;


  for (
    const trade
    of trades
  ) {

    const value =

      trade.price *
      trade.qty;


    if (
      trade.buyerMaker
    ) {

      sellUSDT +=
        value;

    } else {

      buyUSDT +=
        value;
    }
  }


  const total =

    buyUSDT +
    sellUSDT;


  return {

    sufficient:
      true,

    tradeCount:
      trades.length,

    buyUSDT,

    sellUSDT,

    imbalance:

      total >
      0

        ? (
            buyUSDT -
            sellUSDT
          ) /
          total

        : 0
  };
}


function slippageAtRisk(
  asks,
  quoteAmount
) {

  if (

    !Array.isArray(
      asks
    )

    ||

    !asks.length

    ||

    quoteAmount <=
    0

  ) {

    return {

      ok:
        false,

      reason:
        'NO_ASK_LIQUIDITY',

      bps:
        99999,

      scale:
        0
    };
  }


  let remaining =
    quoteAmount;


  let acquiredQty =
    0;


  let spent =
    0;


  const bestAsk =
    asks[0][0];


  for (
    const [
      price,
      qty
    ]
    of asks
  ) {

    const levelQuote =

      price *
      qty;


    const takeQuote =

      Math.min(

        remaining,

        levelQuote
      );


    const takeQty =

      price >
      0

        ? takeQuote /
          price

        : 0;


    spent +=
      takeQuote;


    acquiredQty +=
      takeQty;


    remaining -=
      takeQuote;


    if (
      remaining <=
      1e-8
    ) {

      break;
    }
  }


  if (

    remaining >

      Math.max(

        0.01,

        quoteAmount *
        0.001
      )

    ||

    acquiredQty <=
    0

  ) {

    return {

      ok:
        false,

      reason:
        'INSUFFICIENT_DEPTH',

      bps:
        99999,

      scale:
        0
    };
  }


  const vwap =

    spent /
    acquiredQty;


  const bps =

    bestAsk >
    0

      ? (
          (
            vwap -
            bestAsk
          ) /
          bestAsk
        ) *
        10000

      : 99999;


  if (
    bps >
    C.sarRejectBps
  ) {

    return {

      ok:
        false,

      reason:
        'SAR_TOO_HIGH',

      bps,

      vwap,

      scale:
        0
    };
  }


  let scale =
    1;


  if (
    bps >
    C.sarReduceFromBps
  ) {

    const span =

      C.sarRejectBps -

      C.sarReduceFromBps;


    const fraction =

      span >
      0

        ? (
            bps -
            C.sarReduceFromBps
          ) /
          span

        : 1;


    scale =

      clamp(

        1 -

        fraction *

        (
          1 -
          C.sarMinSizeScale
        ),

        C.sarMinSizeScale,

        1
      );
  }


  return {

    ok:
      true,

    bps,

    vwap,

    scale
  };
}


async function executionQualityGate(
  symbol,
  quoteAmount
) {

  const [
    rawBook,
    flowRows
  ] =

    await Promise.all([

      fetchDepth(
        symbol
      ),

      fetchAggTrades(
        symbol
      )
    ]);


  const book =

    depthMetrics(
      rawBook
    );


  const flow =

    aggressiveFlow(
      flowRows
    );


  const sar =

    slippageAtRisk(

      rawBook.asks,

      quoteAmount
    );


  if (
    !book.ok
  ) {

    return {

      pass:
        false,

      reason:
        book.reason,

      book,

      flow,

      sar
    };
  }


  if (
    book.state ===
    'STRESSED'
  ) {

    return {

      pass:
        false,

      reason:
        'LIQUIDITY_STRESSED',

      book,

      flow,

      sar
    };
  }


  if (
    book.spreadBps >
    C.maxEntrySpreadBps
  ) {

    return {

      pass:
        false,

      reason:
        'SPREAD_TOO_WIDE',

      book,

      flow,

      sar
    };
  }


  if (
    !sar.ok
  ) {

    return {

      pass:
        false,

      reason:
        sar.reason,

      book,

      flow,

      sar
    };
  }


  const severeAdverseBook =

    book.imbalance <=
      C.severeSellBookImbalance

    &&

    book.micropriceEdgeBps <=
      C.severeMicropriceEdgeBps;


  if (
    severeAdverseBook
  ) {

    return {

      pass:
        false,

      reason:
        'SEVERE_ADVERSE_BOOK',

      book,

      flow,

      sar
    };
  }


  return {

    pass:
      true,

    reason:
      'EXECUTION_OK',

    book,

    flow,

    sar
  };
}


// ============================================================
// 1H CONTEXT
// ============================================================

function analyze1h(
  candles
) {

  if (
    candles.length <
    30
  ) {

    return {

      ok:
        false,

      reason:
        '1H_NOT_READY'
    };
  }


  const closes =

    candles.map(
      candle =>
        candle.close
    );


  const ema9 =

    ema(
      closes,
      C.emaFast
    );


  const ema21 =

    ema(
      closes,
      C.emaSlow
    );


  const current =

    candles.at(
      -1
    );


  const bullish =

    ema9 >
      ema21

    &&

    current.close >=
      ema9;


  return {

    ok:
      bullish,

    ema9,

    ema21,

    close:
      current.close,

    bullish
  };
}


// ============================================================
// RETEST
// ============================================================

function detectRecentBreakout(
  candles,
  resistance
) {

  const recent =

    candles.slice(

      -(
        C.retestLookbackBars +
        1
      ),

      -1
    );


  for (

    let i =
      recent.length -
      1;

    i >=
      0;

    i--

  ) {

    const candle =
      recent[i];


    if (

      candle.close >

      resistance *

      (
        1 +
        C.breakoutBufferPct /
        100
      )

    ) {

      return {

        found:
          true,

        candle,

        barsAgo:
          recent.length -
          i
      };
    }
  }


  return {

    found:
      false
  };
}


// ============================================================
// ORIGINAL V6.3.1 15M CORE
// ENTRY LOGIC UNCHANGED
// ============================================================

function analyze15m(
  candles
) {

  if (
    candles.length <
    35
  ) {

    return {

      eligible:
        false,

      reason:
        '15M_NOT_READY'
    };
  }


  const current =
    candles.at(
      -1
    );


  const previous =
    candles.at(
      -2
    );


  const closes =

    candles.map(
      candle =>
        candle.close
    );


  const ema9 =

    ema(
      closes,
      C.emaFast
    );


  const ema21 =

    ema(
      closes,
      C.emaSlow
    );


  const momentum =

    cmo(
      closes,
      C.cmoLength
    );


  const atrValue =

    atr(
      candles,
      14
    );


  const averageVolume =

    smaVolume(

      candles,

      C.volumeSmaLength
    );


  const volumeRatio =

    averageVolume >
    0

      ? current.volume /
        averageVolume

      : 0;


  const bodyRatio =

    candleBodyRatio(
      current
    );


  const prior =

    candles.slice(

      -(
        C.breakoutLookback +
        2
      ),

      -2
    );


  const resistance =

    Math.max(

      ...prior.map(
        candle =>
          candle.high
      )
    );


  const swingLow =

    Math.min(

      ...candles
        .slice(
          -8
        )
        .map(
          candle =>
            candle.low
        )
    );


  const trendOk =

    ema9 >
    ema21;


  const cmoOk =

    momentum >
    C.cmoBuyMin;


  const candleOk =

    current.close >
      current.open

    &&

    bodyRatio >=
      C.minBodyRatio;


  const cleanBreakout =

    current.close >

    resistance *

    (
      1 +
      C.breakoutBufferPct /
      100
    );


  const breakoutDistanceAtr =

    atrValue >
    0

      ? (
          current.close -
          resistance
        ) /
        atrValue

      : 999;


  const notExtended =

    breakoutDistanceAtr <=
    C.maxMomentumExtensionAtr;


  const momentumVolumeOk =

    volumeRatio >=
    C.momentumVolumeMultiplier;


  const momentumEntry =

    trendOk

    &&

    cmoOk

    &&

    candleOk

    &&

    momentumVolumeOk

    &&

    cleanBreakout

    &&

    notExtended;


  const recentBreakout =

    detectRecentBreakout(

      candles,

      resistance
    );


  const touchedRetest =

    current.low <=

      resistance *

      (
        1 +
        C.retestTolerancePct /
        100
      )

    &&

    current.low >=

      resistance *

      (
        1 -
        C.retestTolerancePct /
        100
      );


  const heldRetest =

    current.close >
      resistance

    &&

    current.close >
      previous.close;


  const retestVolumeOk =

    volumeRatio >=
    C.retestMinVolumeRatio;


  const retestEntry =

    trendOk

    &&

    cmoOk

    &&

    candleOk

    &&

    recentBreakout.found

    &&

    touchedRetest

    &&

    heldRetest

    &&

    retestVolumeOk;


  let entryType =
    'NONE';


  if (
    momentumEntry
  ) {

    entryType =
      'MOMENTUM_ENTRY';

  } else if (
    retestEntry
  ) {

    entryType =
      'RETEST_ENTRY';
  }


  const rankScore =

    (
      entryType !==
      'NONE'

        ? 40

        : 0
    )

    +

    clamp(

      (
        momentum -
        30
      ) *
      0.8,

      0,

      20
    )

    +

    clamp(

      (
        volumeRatio -
        1
      ) *
      10,

      0,

      20
    )

    +

    clamp(

      bodyRatio *
      15,

      0,

      15
    )

    +

    (
      trendOk
        ? 5
        : 0
    );


  const atrPct =

    pct(
      atrValue,
      current.close
    );


  return {

    eligible:
      entryType !==
      'NONE',

    entryType,

    signalTime:
      current.closeTime,

    signalPrice:
      current.close,

    // Telemetry only
    signalCandle: {

      openTime:
        current.openTime,

      closeTime:
        current.closeTime,

      open:
        current.open,

      high:
        current.high,

      low:
        current.low,

      close:
        current.close,

      volume:
        current.volume
    },

    ema9,

    ema21,

    cmo:
      momentum,

    atr:
      atrValue,

    atrPct,

    atrShadowPass:

      atrPct >=
      C.atrPctShadowThreshold,

    volumeRatio,

    bodyRatio,

    resistance,

    swingLow,

    breakoutDistanceAtr,

    rankScore:

      +rankScore.toFixed(
        2
      ),

    checks: {

      trendOk,

      cmoOk,

      candleOk,

      momentumVolumeOk,

      cleanBreakout,

      notExtended,

      recentBreakout:
        recentBreakout.found,

      touchedRetest,

      heldRetest,

      retestVolumeOk
    }
  };
}


// ============================================================
// STAGED ANALYSIS
// ============================================================

function lossCooldownActive(
  symbol
) {

  const lastLoss =

    n(
      lastLossBySymbol[
        symbol
      ]
    );


  return (

    !!lastLoss

    &&

    Date.now() -
      lastLoss <
      C.symbolLossCooldownMs
  );
}


async function analyze15mFresh(
  symbol
) {

  if (

    positions[
      symbol
    ]

    ||

    lossCooldownActive(
      symbol
    )

  ) {

    return null;
  }


  const entryResult =

    await fetchClosedKlines(

      symbol,

      C.entryInterval,

      C.klineLimit15m
    );


  const signal =

    analyze15m(
      entryResult.candles
    );


  if (
    !signal.eligible
  ) {

    return null;
  }


  return {

    symbol,

    analyzedAt:
      Date.now(),

    sources: {

      entry:
        entryResult.source
    },

    signal
  };
}


async function add1hContext(
  candidate
) {

  const contextResult =

    await fetchClosedKlines(

      candidate.symbol,

      C.contextInterval,

      C.klineLimit1h
    );


  const context =

    analyze1h(
      contextResult.candles
    );


  if (
    !context.ok
  ) {

    return null;
  }


  return {

    ...candidate,

    sources: {

      ...candidate.sources,

      context:
        contextResult.source
    },

    context
  };
}


async function analyzeSymbolFresh(
  symbol
) {

  const base =

    await analyze15mFresh(
      symbol
    );


  if (
    !base
  ) {

    return null;
  }


  return add1hContext(
    base
  );
}


// ============================================================
// CONCURRENCY
// ============================================================

async function mapLimit(
  items,
  limit,
  worker
) {

  const results =
    [];


  let index =
    0;


  async function runner() {

    while (
      true
    ) {

      const i =
        index++;


      if (
        i >=
        items.length
      ) {

        return;
      }


      try {

        const result =

          await worker(
            items[i]
          );


        if (
          result
        ) {

          results.push(
            result
          );
        }

      } catch (
        error
      ) {

        console.warn(

          `SCAN ${

            items[i]
              ?.symbol ||

            items[i]

          } | ${error.message}`
        );
      }


      await sleep(
        C.requestGapMs
      );
    }
  }


  await Promise.all(

    Array.from(

      {

        length:

          Math.min(

            limit,

            items.length
          )
      },

      runner
    )
  );


  return results;
}


// ============================================================
// ORIGINAL HARD EMERGENCY STOP
// ============================================================

function emergencyStopFrom(
  signal,
  entryPrice
) {

  const atrStop =

    entryPrice -

    signal.atr *
    C.atrStopMultiplier;


  const structureStop =

    signal.swingLow -

    signal.atr *
    C.structureBufferAtr;


  let stop =

    Math.min(

      atrStop,

      structureStop
    );


  let stopPct =

    (
      entryPrice -
      stop
    ) /

    entryPrice;


  stopPct =

    clamp(

      stopPct,

      C.minEmergencyStopPct,

      C.maxEmergencyStopPct
    );


  stop =

    entryPrice *

    (
      1 -
      stopPct
    );


  return {

    stop,

    stopPct
  };
}


// ============================================================
// POSITION SIZE
// ============================================================

function positionSize(
  stopPct
) {

  const eq =
    equity();


  const riskBudget =

    eq *
    C.riskPerTradePct;


  const byRisk =

    stopPct >
    0

      ? riskBudget /
        stopPct

      : 0;


  const maxAllocation =

    eq *
    C.maxAllocationPct;


  const allocation =

    Math.min(

      byRisk,

      maxAllocation,

      cash *
      0.98
    );


  return allocation >=
    10

      ? allocation

      : null;
}


// ============================================================
// DATABASE
// ============================================================

async function connectCloud() {

  if (
    !MONGODB_URI
  ) {

    console.warn(
      'MONGODB_URI missing.'
    );


    return;
  }


  mongoClient =

    new MongoClient(

      MONGODB_URI,

      {

        maxPoolSize:
          4
      }
    );


  await mongoClient
    .connect();


  db =

    mongoClient.db(
      MONGODB_DB
    );


  await db.command({

    ping:
      1
  });


  cloudConnected =
    true;


  await Promise.all([

    db
      .collection(
        'trades'
      )
      .createIndex({

        version:
          1,

        exitTime:
          -1
      }),


    db
      .collection(
        'journal'
      )
      .createIndex({

        version:
          1,

        time:
          -1
      }),


    db
      .collection(
        'journal'
      )
      .createIndex({

        cycleId:
          1,

        time:
          -1
      })
  ]);


  console.log(
    'MongoDB CONNECTED'
  );
}


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

        cycleId:
          cycle.id,

        ...row
      });

  } catch (
    error
  ) {

    console.error(

      'Journal:',

      error.message
    );
  }
}


async function saveTrade(
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
        'trades'
      )
      .insertOne({

        version:
          C.version,

        cycleId:
          row.cycleId,

        ...row
      });

  } catch (
    error
  ) {

    console.error(

      'Trade save:',

      error.message
    );
  }
}


// ============================================================
// STATE
// ============================================================

async function loadState() {

  if (
    !cloudConnected
  ) {

    return;
  }


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

    ||

    state.version !==
      C.version

  ) {

    console.log(

      'FRESH V6.3.2 PAPER ACCOUNT | $1000 | Cycle 1 | 0/20'
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


  currentDay =

    state.currentDay ||

    utcDay();


  dailyStartEquity =

    n(

      state.dailyStartEquity,

      C.startingBalance
    );


  dailyPnL =

    n(
      state.dailyPnL
    );


  peakEquity =

    n(

      state.peakEquity,

      C.startingBalance
    );


  manualPause =
    !!state.manualPause;


  dailyPause =
    !!state.dailyPause;


  drawdownPause =
    !!state.drawdownPause;


  cycle = {

    ...cycle,

    ...(
      state.cycle ||
      {}
    )
  };


  Object.assign(

    lastLossBySymbol,

    state.lastLossBySymbol ||
    {}
  );
}


async function saveState() {

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

            cash,

            positions,

            stats,

            currentDay,

            dailyStartEquity,

            dailyPnL,

            peakEquity,

            manualPause,

            dailyPause,

            drawdownPause,

            cycle,

            lastLossBySymbol,

            updatedAt:
              Date.now()
          }
        },

        {

          upsert:
            true
        }
      );

  } catch (
    error
  ) {

    console.error(

      'State save:',

      error.message
    );
  }
}


// ============================================================
// TELEGRAM
// ============================================================

async function tg(
  text
) {

  if (

    !TELEGRAM_TOKEN

    ||

    !TELEGRAM_CHAT_ID

  ) {

    return;
  }


  try {

    await axios.post(

      `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,

      {

        chat_id:
          TELEGRAM_CHAT_ID,

        text,

        parse_mode:
          'HTML',

        disable_web_page_preview:
          true
      },

      {

        timeout:
          8000
      }
    );

  } catch (
    error
  ) {

    console.warn(

      'Telegram:',

      error.message
    );
  }
}


// ============================================================
// CYCLE
// ============================================================

async function startNewCycle(
  reason =
    'PREVIOUS_CYCLE_COMPLETE'
) {

  const previousId =
    cycle.id;


  cycle = {

    id:
      previousId +
      1,

    state:
      'SCANNING',

    entries:
      0,

    startedAt:
      Date.now(),

    lastFreshScanAt:
      0,

    lastScanCandidates:
      0,

    lastUniverseSource:
      cycle.lastUniverseSource,

    lastScanned:
      0
  };


  stats.cycleCount =
    cycle.id;


  await cloudJournal({

    type:
      'NEW_CYCLE',

    reason,

    cycleId:
      cycle.id
  });


  tg(

    `🔄 <b>LOMY NEW CYCLE</b>\n` +

    `Cycle ${cycle.id}\n` +

    `Reason: ${reason}`
  );
}


async function maybeStartNewCycle() {

  if (

    cycle.state ===
      'WAITING_CLOSE'

    &&

    Object.keys(
      positions
    ).length ===
      0

  ) {

    await startNewCycle();
  }
}


// ============================================================
// OPEN PAPER TRADE
// ENTRY DECISION LOGIC UNCHANGED
// ============================================================

async function openPaperTrade(
  candidate
) {

  if (
    entryBlocked()
  ) {

    return false;
  }


  if (
    cycle.entries >=
    C.maxEntriesPerCycle
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


  if (
    positions[
      candidate.symbol
    ]
  ) {

    return false;
  }


  const fresh =

    await analyzeSymbolFresh(
      candidate.symbol
    );


  if (

    !fresh

    ||

    !fresh.signal.eligible

  ) {

    return false;
  }


  if (

    fresh.signal.entryType !==
      candidate.signal.entryType

    ||

    fresh.signal.signalTime !==
      candidate.signal.signalTime

  ) {

    await cloudJournal({

      type:
        'REVALIDATION_REJECT',

      symbol:
        candidate.symbol,

      reason:
        'SIGNAL_CHANGED'
    });


    return false;
  }


  const priceData =

    await getCurrentPrice(
      candidate.symbol
    );


  const livePrice =
    priceData.price;


  const drift =

    Math.abs(

      pct(

        livePrice -
        fresh.signal.signalPrice,

        fresh.signal.signalPrice
      )
    );


  if (
    drift >
    C.maxEntryDriftPct
  ) {

    await cloudJournal({

      type:
        'REVALIDATION_REJECT',

      symbol:
        candidate.symbol,

      reason:
        'PRICE_DRIFT',

      drift
    });


    return false;
  }


  const entryPrice =

    livePrice *

    (
      1 +
      C.slippagePct
    );


  const stop =

    emergencyStopFrom(

      fresh.signal,

      entryPrice
    );


  let allocation =

    positionSize(
      stop.stopPct
    );


  if (
    !allocation
  ) {

    return false;
  }


  const micro =

    await executionQualityGate(

      candidate.symbol,

      allocation
    );


  await cloudJournal({

    type:

      micro.pass

        ? 'MICRO_PASS'

        : 'MICRO_REJECT',

    symbol:
      candidate.symbol,

    reason:
      micro.reason,

    micro
  });


  if (
    !micro.pass
  ) {

    return false;
  }


  allocation *=
    micro.sar.scale;


  if (
    allocation <
    10
  ) {

    return false;
  }


  const buyFee =

    allocation *
    C.feePct;


  const netAssetValue =

    allocation -
    buyFee;


  const qty =

    netAssetValue /
    entryPrice;


  const tradeId =

    `${candidate.symbol}-${Date.now()}-${Math.random()
      .toString(36)
      .slice(2, 8)}`;


  cash -=
    allocation;


  positions[
    candidate.symbol
  ] = {

    tradeId,

    cycleId:
      cycle.id,

    symbol:
      candidate.symbol,

    entryType:
      fresh.signal.entryType,

    entryPrice,

    entryTime:
      Date.now(),

    signalPrice:
      fresh.signal.signalPrice,

    signalTime:
      fresh.signal.signalTime,

    priceSource:
      priceData.source,

    qty,

    investedUSDT:
      allocation,

    buyFee,

    emergencyStop:
      stop.stop,

    initialRiskPct:
      stop.stopPct *
      100,

    protectionStop:
      null,

    breakEvenActive:
      false,

    capitalProtectionGrossLockPct:
      null,

    capitalProtectionNetLockPct:
      null,

    capitalProtectionArmedAt:
      null,

    trailingActive:
      false,

    trailStage:
      0,

    trailGapPct:
      null,

    trailLockedMfeFraction:
      null,

    trailingActivatedAt:
      null,

    failureControlEligible:
      true,

    failureControlTriggered:
      false,

    highestPrice:
      entryPrice,

    lastPrice:
      entryPrice,

    mfePct:
      0,

    maePct:
      0,

    atrPct:
      fresh.signal.atrPct,

    atrShadowPass:
      fresh.signal.atrShadowPass,

    lastTelemetryAt:
      0,

    entryCandleTelemetry:
      null,

    snapshot:
      fresh,

    microstructure:
      micro
  };


  cycle.entries++;


  await cloudJournal({

    type:
      'ENTRY',

    tradeId,

    symbol:
      candidate.symbol,

    entryType:
      fresh.signal.entryType,

    entryPrice,

    signalPrice:
      fresh.signal.signalPrice,

    signalTime:
      fresh.signal.signalTime,

    priceSource:
      priceData.source,

    allocation,

    initialRiskPct:
      stop.stopPct *
      100,

    emergencyStop:
      stop.stop,

    atrPct:
      fresh.signal.atrPct,

    atrShadowPass:
      fresh.signal.atrShadowPass,

    snapshot:
      fresh,

    microstructure:
      micro
  });


  // ==========================================================
  // ENTRY CANDLE TELEMETRY
  // AFTER ENTRY ONLY
  // ==========================================================

  try {

    const liveCandle =

      await fetchLiveEntryCandle(
        candidate.symbol
      );


    const telemetry =

      entryCandleTelemetry(

        liveCandle,

        entryPrice
      );


    const p =

      positions[
        candidate.symbol
      ];


    if (
      p
    ) {

      p.entryCandleTelemetry =
        telemetry;
    }


    await cloudJournal({

      type:
        'ENTRY_CANDLE_TELEMETRY',

      tradeId,

      symbol:
        candidate.symbol,

      telemetry
    });

  } catch (
    error
  ) {

    await cloudJournal({

      type:
        'ENTRY_CANDLE_TELEMETRY_ERROR',

      tradeId,

      symbol:
        candidate.symbol,

      error:
        error.message
    });
  }


  tg(

    `🟢 <b>LOMY V6.3.2 PAPER ENTRY</b>\n` +

    `${candidate.symbol}\n` +

    `Type: ${fresh.signal.entryType}\n` +

    `Price source: ${priceData.source}\n` +

    `15m CMO: ${fresh.signal.cmo.toFixed(1)}\n` +

    `Volume: ${fresh.signal.volumeRatio.toFixed(2)}x\n` +

    `ATR%: ${fresh.signal.atrPct.toFixed(3)} ` +

    `(${fresh.signal.atrShadowPass ? 'shadow-pass' : 'shadow-low'})\n` +

    `Initial risk: ${(stop.stopPct * 100).toFixed(2)}%\n` +

    `Spread: ${micro.book.spreadBps.toFixed(2)}bps | ` +

    `SaR ${micro.sar.bps.toFixed(2)}bps\n` +

    `Cycle: ${cycle.entries}/${C.maxEntriesPerCycle}`
  );


  if (
    cycle.entries >=
    C.maxEntriesPerCycle
  ) {

    cycle.state =
      'WAITING_CLOSE';


    await cloudJournal({

      type:
        'CYCLE_FULL',

      openPositions:

        Object.keys(
          positions
        ).length
    });


    tg(

      `⏳ Cycle ${cycle.id} reached ` +

      `${C.maxEntriesPerCycle} entries.\n` +

      `Waiting for all positions to close.`
    );
  }


  return true;
}


// ============================================================
// CLOSE PAPER TRADE
// ============================================================

async function closePaperTrade(
  symbol,
  rawExitPrice,
  reason,
  source
) {

  const position =

    positions[
      symbol
    ];


  if (
    !position
  ) {

    return;
  }


  const exitPrice =

    rawExitPrice *

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


  const netValue =

    grossValue -
    sellFee;


  const profit =

    netValue -
    position.investedUSDT;


  const profitPct =

    position.investedUSDT >
    0

      ? (
          profit /
          position.investedUSDT
        ) *
        100

      : 0;


  cash +=
    netValue;


  dailyPnL +=
    profit;


  stats.totalTrades++;


  stats.netProfit +=
    profit;


  stats.fees +=

    position.buyFee +
    sellFee;


  const setup =

    stats.setupStats[
      position.entryType
    ]

    ||

    {

      trades:
        0,

      wins:
        0,

      losses:
        0,

      net:
        0
    };


  setup.trades++;


  setup.net +=
    profit;


  const exitBucket =

    stats.exitStats[
      reason
    ]

    ||

    {

      trades:
        0,

      wins:
        0,

      losses:
        0,

      net:
        0
    };


  exitBucket.trades++;


  exitBucket.net +=
    profit;


  if (
    profit >
    0
  ) {

    stats.wins++;


    stats.grossProfit +=
      profit;


    setup.wins++;


    exitBucket.wins++;

  } else {

    stats.losses++;


    stats.grossLoss +=

      Math.abs(
        profit
      );


    setup.losses++;


    exitBucket.losses++;


    lastLossBySymbol[
      symbol
    ] =
      Date.now();
  }


  stats.setupStats[
    position.entryType
  ] =
    setup;


  stats.exitStats[
    reason
  ] =
    exitBucket;


  const record = {

    ...position,

    rawExitPrice,

    exitPrice,

    exitSource:
      source,

    sellFee,

    totalFees:

      position.buyFee +
      sellFee,

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


  delete positions[
    symbol
  ];


  await saveTrade(
    record
  );


  await cloudJournal({

    type:
      'EXIT',

    tradeId:
      position.tradeId,

    symbol,

    reason,

    profit,

    profitPct,

    rawExitPrice,

    exitPrice,

    exitSource:
      source,

    mfePct:
      position.mfePct,

    maePct:
      position.maePct,

    atrPct:
      position.atrPct,

    atrShadowPass:
      position.atrShadowPass,

    initialRiskPct:
      position.initialRiskPct,

    trailStage:
      position.trailStage,

    protectionStop:
      position.protectionStop
  });


  tg(

    `${profit >= 0 ? '✅' : '🔴'} ` +

    `<b>${symbol} CLOSED</b>\n` +

    `${reason}\n` +

    `Source: ${source}\n` +

    `PnL: $${profit.toFixed(2)} ` +

    `(${profitPct.toFixed(2)}%)\n` +

    `MFE ${position.mfePct.toFixed(2)}% | ` +

    `MAE ${position.maePct.toFixed(2)}%`
  );


  updateAccountGuards();


  await maybeStartNewCycle();
}


// ============================================================
// TRAIL PROFILE
// ============================================================

function trailProfile(
  mfePct
) {

  if (
    mfePct >=
    C.trailStage5MfePct
  ) {

    return {

      stage:
        5,

      maxGapPct:
        C.trailStage5MaxGapPct,

      lockFraction:
        C.trailStage5LockFraction
    };
  }


  if (
    mfePct >=
    C.trailStage4MfePct
  ) {

    return {

      stage:
        4,

      maxGapPct:
        C.trailStage4MaxGapPct,

      lockFraction:
        C.trailStage4LockFraction
    };
  }


  if (
    mfePct >=
    C.trailStage3MfePct
  ) {

    return {

      stage:
        3,

      maxGapPct:
        C.trailStage3MaxGapPct,

      lockFraction:
        C.trailStage3LockFraction
    };
  }


  if (
    mfePct >=
    C.trailStage2MfePct
  ) {

    return {

      stage:
        2,

      maxGapPct:
        C.trailStage2MaxGapPct,

      lockFraction:
        C.trailStage2LockFraction
    };
  }


  return {

    stage:
      1,

    maxGapPct:
      C.trailStage1MaxGapPct,

    lockFraction:
      C.trailStage1LockFraction
  };
}


// ============================================================
// CAPITAL PROTECTION CALCULATION
// ============================================================

function capitalProtectionGrossLockPct() {

  return grossMoveNeededForNetPct(

    C.capitalNetLockPct
  );
}


// ============================================================
// ACTIVE STOP
// ============================================================

function activeStopInfo(
  position
) {

  let activeStop =
    position.emergencyStop;


  let reason =
    'EMERGENCY_STOP';


  if (
    position.protectionStop
  ) {

    activeStop =

      Math.max(

        activeStop,

        position.protectionStop
      );


    reason =

      position.trailingActive

        ? 'DYNAMIC_TRAIL'

        : 'CAPITAL_PROTECTION_STOP';
  }


  return {

    activeStop,

    reason
  };
}


// ============================================================
// FAILED FOLLOW-THROUGH RULE
// ============================================================

function failureControlTriggered(
  position,
  movePct,
  ageMinutes
) {

  if (
    !C.failureStopEnabled
  ) {

    return false;
  }


  if (

    position.breakEvenActive

    ||

    position.trailingActive

  ) {

    return false;
  }


  return (

    ageMinutes >=
      C.failureMinAgeMinutes

    &&

    position.mfePct <
      C.failureMaxMfePct

    &&

    movePct <=
      -C.failureDrawdownPct
  );
}


// ============================================================
// POSITION TELEMETRY
// ============================================================

async function maybeJournalTelemetry(
  position,
  symbol,
  movePct,
  ageMinutes,
  activeStop,
  stopReason
) {

  const now =
    Date.now();


  if (

    now -

      n(
        position.lastTelemetryAt
      )

    <

    C.positionTelemetryEveryMs

  ) {

    return;
  }


  position.lastTelemetryAt =
    now;


  const stopDistancePct =

    position.entryPrice >
    0

      ? pct(

          activeStop -
          position.entryPrice,

          position.entryPrice
        )

      : null;


  await cloudJournal({

    type:
      'POSITION_TELEMETRY',

    tradeId:
      position.tradeId,

    symbol,

    ageMinutes,

    currentMovePct:
      movePct,

    mfePct:
      position.mfePct,

    maePct:
      position.maePct,

    highestPrice:
      position.highestPrice,

    lastPrice:
      position.lastPrice,

    emergencyStop:
      position.emergencyStop,

    protectionStop:
      position.protectionStop,

    activeStop,

    activeStopReason:
      stopReason,

    stopDistanceFromEntryPct:
      stopDistancePct,

    breakEvenActive:
      position.breakEvenActive,

    trailingActive:
      position.trailingActive,

    trailStage:
      position.trailStage,

    trailGapPct:
      position.trailGapPct,

    initialRiskPct:
      position.initialRiskPct
  });
}


// ============================================================
// POSITION MANAGER
// ============================================================

async function managePositions() {

  if (

    positionManagerRunning

    ||

    shuttingDown

  ) {

    return;
  }


  positionManagerRunning =
    true;


  const prices =
    {};


  try {

    resetDailyIfNeeded(
      prices
    );


    updateAccountGuards(
      prices
    );


    const symbols =

      Object.keys(
        positions
      );


    if (
      !symbols.length
    ) {

      await maybeStartNewCycle();


      return;
    }


    for (
      const symbol
      of symbols
    ) {

      try {

        const priceData =

          await getCurrentPrice(
            symbol
          );


        prices[
          symbol
        ] =
          priceData.price;


        const position =

          positions[
            symbol
          ];


        if (
          !position
        ) {

          continue;
        }


        position.lastPrice =
          priceData.price;


        position.lastPriceSource =
          priceData.source;


        position.highestPrice =

          Math.max(

            position.highestPrice,

            priceData.price
          );


        const movePct =

          pct(

            priceData.price -
            position.entryPrice,

            position.entryPrice
          );


        position.mfePct =

          Math.max(

            position.mfePct,

            movePct
          );


        position.maePct =

          Math.min(

            position.maePct,

            movePct
          );


        const ageMinutes =

          (
            Date.now() -
            position.entryTime
          ) /
          60000;


        // ======================================================
        // 1) COST-AWARE CAPITAL PROTECTION
        // ======================================================

        if (

          !position.breakEvenActive

          &&

          movePct >=
            C.capitalProtectionTriggerPct

        ) {

          const grossLockPct =

            capitalProtectionGrossLockPct();


          position.protectionStop =

            position.entryPrice *

            (
              1 +
              grossLockPct /
              100
            );


          position.breakEvenActive =
            true;


          position.capitalProtectionGrossLockPct =
            grossLockPct;


          position.capitalProtectionNetLockPct =
            C.capitalNetLockPct;


          position.capitalProtectionArmedAt =
            Date.now();


          position.failureControlEligible =
            false;


          stats.breakEvenMoves++;


          await cloudJournal({

            type:
              'CAPITAL_PROTECTION',

            tradeId:
              position.tradeId,

            symbol,

            triggerMovePct:
              movePct,

            protectionStop:
              position.protectionStop,

            grossLockPct,

            modeledNetLockPct:

              modeledNetPctAtRawMove(
                grossLockPct
              ),

            targetNetLockPct:
              C.capitalNetLockPct
          });
        }


        // ======================================================
        // 2) TRAILING ACTIVATION
        // ======================================================

        if (

          !position.trailingActive

          &&

          movePct >=
            C.trailingStartPct

        ) {

          position.trailingActive =
            true;


          position.trailingActivatedAt =
            Date.now();


          stats.trailingActivations++;


          await cloudJournal({

            type:
              'TRAIL_ACTIVATED',

            tradeId:
              position.tradeId,

            symbol,

            triggerMovePct:
              movePct,

            mfePct:
              position.mfePct
          });
        }


        // ======================================================
        // 3) STAGED DYNAMIC TRAIL
        // ======================================================

        if (
          position.trailingActive
        ) {

          const profile =

            trailProfile(
              position.mfePct
            );


          const previousStage =

            n(
              position.trailStage
            );


          const atrGapPct =

            clamp(

              position.atrPct *

              C.trailingAtrMultiplier,

              C.trailingGapMinPct,

              profile.maxGapPct
            );


          const atrTrailingStop =

            position.highestPrice *

            (
              1 -
              atrGapPct /
              100
            );


          const mfeLockedPct =

            Math.max(

              capitalProtectionGrossLockPct(),

              position.mfePct *
              profile.lockFraction
            );


          const mfeLockStop =

            position.entryPrice *

            (
              1 +
              mfeLockedPct /
              100
            );


          const newStop =

            Math.max(

              position.protectionStop ||
              0,

              atrTrailingStop,

              mfeLockStop
            );


          position.protectionStop =
            newStop;


          position.trailStage =
            profile.stage;


          position.trailGapPct =
            atrGapPct;


          position.trailLockedMfeFraction =
            profile.lockFraction;


          if (

            profile.stage >
            previousStage

          ) {

            await cloudJournal({

              type:
                'TRAIL_STAGE_UP',

              tradeId:
                position.tradeId,

              symbol,

              fromStage:
                previousStage,

              toStage:
                profile.stage,

              mfePct:
                position.mfePct,

              gapPct:
                atrGapPct,

              lockFraction:
                profile.lockFraction,

              protectionStop:
                position.protectionStop
            });
          }
        }


        // ======================================================
        // 4) FAILED FOLLOW-THROUGH STOP
        // ======================================================

        if (

          failureControlTriggered(

            position,

            movePct,

            ageMinutes
          )

        ) {

          position.failureControlTriggered =
            true;


          stats.failureStops++;


          await cloudJournal({

            type:
              'FAILED_FOLLOW_THROUGH_TRIGGER',

            tradeId:
              position.tradeId,

            symbol,

            ageMinutes,

            movePct,

            mfePct:
              position.mfePct,

            maePct:
              position.maePct,

            originalEmergencyStop:
              position.emergencyStop,

            originalInitialRiskPct:
              position.initialRiskPct,

            rule: {

              minAgeMinutes:
                C.failureMinAgeMinutes,

              maxMfePct:
                C.failureMaxMfePct,

              drawdownPct:
                C.failureDrawdownPct
            }
          });


          await closePaperTrade(

            symbol,

            priceData.price,

            'FAILED_FOLLOW_THROUGH_STOP',

            priceData.source
          );


          await sleep(
            80
          );


          continue;
        }


        const {

          activeStop,

          reason:
            stopReason

        } =

          activeStopInfo(
            position
          );


        await maybeJournalTelemetry(

          position,

          symbol,

          movePct,

          ageMinutes,

          activeStop,

          stopReason
        );


        if (

          priceData.price <=
          activeStop

        ) {

          await closePaperTrade(

            symbol,

            priceData.price,

            stopReason,

            priceData.source
          );
        }

      } catch (
        error
      ) {

        console.error(

          `POSITION ${symbol}:`,

          error.message
        );
      }


      await sleep(
        80
      );
    }


    resetDailyIfNeeded(
      prices
    );


    updateAccountGuards(
      prices
    );

  } finally {

    positionManagerRunning =
      false;
  }
}


// ============================================================
// FRESH SCAN
// ============================================================

async function runFreshScan() {

  if (

    scanning

    ||

    shuttingDown

    ||

    entryBlocked()

  ) {

    return;
  }


  scanning =
    true;


  try {

    const symbols =

      await refreshUniverse();


    cycle.lastFreshScanAt =
      Date.now();


    cycle.lastScanned =
      symbols.length;


    // STAGE 1 — 15M ONLY

    const rawCandidates =

      await mapLimit(

        symbols,

        C.scanConcurrency,

        analyze15mFresh
      );


    rawCandidates.sort(

      (
        a,
        b
      ) =>

        b.signal.rankScore -

        a.signal.rankScore
    );


    // STAGE 2 — 1H CONTEXT

    const candidates =

      await mapLimit(

        rawCandidates,

        Math.min(
          3,
          C.scanConcurrency
        ),

        add1hContext
      );


    candidates.sort(

      (
        a,
        b
      ) =>

        b.signal.rankScore -

        a.signal.rankScore
    );


    cycle.lastScanCandidates =
      candidates.length;


    await cloudJournal({

      type:
        'FRESH_SCAN',

      scanned:
        symbols.length,

      rawQualified:
        rawCandidates.length,

      qualified:
        candidates.length,

      universeSource:
        cycle.lastUniverseSource,

      top:

        candidates
          .slice(
            0,
            10
          )
          .map(
            candidate => ({

              symbol:
                candidate.symbol,

              entryType:
                candidate.signal.entryType,

              rankScore:
                candidate.signal.rankScore,

              cmo:
                candidate.signal.cmo,

              volumeRatio:
                candidate.signal.volumeRatio,

              bodyRatio:
                candidate.signal.bodyRatio,

              atrPct:
                candidate.signal.atrPct,

              atrShadowPass:
                candidate.signal.atrShadowPass,

              sources:
                candidate.sources
            })
          )
    });


    console.log(

      `SCAN DONE | ` +

      `${symbols.length} checked | ` +

      `${rawCandidates.length} 15m setup | ` +

      `${candidates.length} context-pass`
    );


    let opened =
      0;


    for (
      const candidate
      of candidates
    ) {

      if (
        opened >=
        C.maxEntriesPerScan
      ) {

        break;
      }


      if (
        cycle.entries >=
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


      if (
        entryBlocked()
      ) {

        break;
      }


      const result =

        await openPaperTrade(
          candidate
        );


      if (
        result
      ) {

        opened++;
      }
    }

  } catch (
    error
  ) {

    console.error(

      'FRESH SCAN:',

      error.message
    );

  } finally {

    scanning =
      false;
  }
}


// ============================================================
// DASHBOARD DATA
// ============================================================

function dashboardData() {

  const eq =
    equity();


  const winRate =

    stats.totalTrades >
    0

      ? (
          stats.wins /
          stats.totalTrades
        ) *
        100

      : 0;


  const profitFactor =

    stats.grossLoss >
    0

      ? stats.grossProfit /
        stats.grossLoss

      : stats.grossProfit >
        0

        ? 999

        : 0;


  return {

    version:
      C.version,

    mode:
      'PAPER ONLY',

    cash:

      +cash.toFixed(
        2
      ),

    equity:

      +eq.toFixed(
        2
      ),

    positions,

    stats,

    dailyPnL:

      +dailyPnL.toFixed(
        2
      ),

    winRate:

      +winRate.toFixed(
        2
      ),

    profitFactor:

      +profitFactor.toFixed(
        2
      ),

    cycle,

    exchanges: {

      BINANCE: {

        available:
          exchangeAvailable(),

        blockedForSeconds:

          Math.max(

            0,

            Math.ceil(

              (
                exchangeHealth
                  .BINANCE
                  .blockedUntil -

                Date.now()

              ) /
              1000
            )
          ),

        lastError:

          exchangeHealth
            .BINANCE
            .lastError,

        lastSuccess:

          exchangeHealth
            .BINANCE
            .lastSuccess
      }
    },

    guards: {

      manualPause,

      dailyPause,

      drawdownPause
    },

    config: {

      startingBalance:
        C.startingBalance,

      entryTimeframe:
        C.entryInterval,

      contextTimeframe:
        C.contextInterval,

      maxEntriesPerCycle:
        C.maxEntriesPerCycle,

      maxPositions:
        C.maxPositions,

      marketData:
        'BINANCE_PUBLIC_DATA_API',

      spreadGuardBps:
        C.maxEntrySpreadBps,

      atrShadowThreshold:
        C.atrPctShadowThreshold,

      entryStrategy:
        'V6.3.1 FROZEN — MOMENTUM + RETEST UNCHANGED',

      profitManager: {

        capitalTriggerPct:
          C.capitalProtectionTriggerPct,

        targetNetLockPct:
          C.capitalNetLockPct,

        grossMoveNeededForNetLockPct:

          +capitalProtectionGrossLockPct()
            .toFixed(
              4
            ),

        trailingStartPct:
          C.trailingStartPct,

        stagedTrail:
          true,

        failureControl: {

          enabled:
            C.failureStopEnabled,

          minAgeMinutes:
            C.failureMinAgeMinutes,

          maxMfePct:
            C.failureMaxMfePct,

          drawdownPct:
            C.failureDrawdownPct
        }
      },

      telemetry:
        'ENTRY_CANDLE + POSITION_PATH_EVERY_5M',

      microstructure:
        'EXECUTION_SAFETY_ONLY + FLOW_TELEMETRY + SAR'
    }
  };
}


// ============================================================
// API
// ============================================================

app.get(

  '/api/data',

  (
    req,
    res
  ) => {

    res.json(
      dashboardData()
    );
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


    await saveState();


    res.json({

      ok:
        true
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


    await saveState();


    res.json({

      ok:
        true
    });
  }
);


app.post(

  '/api/scan-now',

  (
    req,
    res
  ) => {

    setTimeout(
      runFreshScan,
      0
    );


    res.json({

      ok:
        true
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

      for (
        const symbol
        of Object.keys(
          positions
        )
      ) {

        const priceData =

          await getCurrentPrice(
            symbol
          );


        await closePaperTrade(

          symbol,

          priceData.price,

          'MANUAL_EMERGENCY_CLOSE',

          priceData.source
        );
      }


      res.json({

        ok:
          true
      });

    } catch (
      error
    ) {

      res
        .status(
          500
        )
        .json({

          ok:
            false,

          error:
            error.message
        });
    }
  }
);


// ============================================================
// DASHBOARD HTML
// ============================================================

app.get(

  '/',

  (
    req,
    res
  ) => {

    res.type(
      'html'
    )
    .send(`

<!doctype html>

<html>

<head>

<meta charset="utf-8">

<meta
  name="viewport"
  content="width=device-width,initial-scale=1"
>

<title>
LOMY V6.3.2
</title>

<style>

body {
  font-family: Arial;
  background: #111;
  color: #eee;
  margin: 18px;
}

.grid {
  display: grid;
  grid-template-columns:
    repeat(auto-fit,minmax(150px,1fr));
  gap: 10px;
  margin-top: 15px;
}

.card {
  background: #1d1d1d;
  padding: 14px;
  border-radius: 10px;
}

.label {
  color: #aaa;
  font-size: 12px;
}

.value {
  font-size: 22px;
  font-weight: bold;
  margin-top: 4px;
}

button {
  padding: 10px 14px;
  margin: 5px;
  border: 0;
  border-radius: 8px;
  cursor: pointer;
}

pre {
  background: #181818;
  padding: 12px;
  border-radius: 10px;
  overflow: auto;
}

</style>

</head>

<body>

<h2>
LOMY V6.3.2 Profit Manager
</h2>

<div>
15M Entry + 1H Context • Binance Public Data • PAPER ONLY •
Capital $1000 • V6.3.1 Entry Frozen • Cost-Aware Protection • Staged Trail
</div>

<div
  id="cards"
  class="grid"
></div>

<p>

<button onclick="post('/api/scan-now')">
Fresh Scan
</button>

<button onclick="post('/api/pause')">
Pause
</button>

<button onclick="post('/api/resume')">
Resume
</button>

<button onclick="post('/api/emergency-close')">
Close All
</button>

</p>

<pre id="detail"></pre>

<script>

async function post(
  url
) {

  await fetch(

    url,

    {
      method:
        'POST'
    }
  );


  setTimeout(
    load,
    500
  );
}


async function load() {

  const response =

    await fetch(
      '/api/data'
    );


  const d =
    await response.json();


  const items = [

    [
      'Equity',
      '$' +
      d.equity
    ],

    [
      'Cash',
      '$' +
      d.cash
    ],

    [
      'Open',
      Object.keys(
        d.positions
      ).length
    ],

    [
      'Closed',
      d.stats.totalTrades
    ],

    [
      'Win Rate',
      d.winRate +
      '%'
    ],

    [
      'Net P/L',
      '$' +
      d.stats.netProfit.toFixed(
        2
      )
    ],

    [
      'PF',
      d.profitFactor
    ],

    [
      'Failure Stops',
      d.stats.failureStops ||
      0
    ],

    [
      'Candidates',
      d.cycle.lastScanCandidates
    ],

    [
      'Scanned',
      d.cycle.lastScanned
    ],

    [
      'Cycle',
      d.cycle.entries +
      '/' +
      d.config.maxEntriesPerCycle
    ],

    [
      'Universe',
      d.cycle.lastUniverseSource ||
      '-'
    ]
  ];


  document
    .getElementById(
      'cards'
    )
    .innerHTML =

      items
        .map(
          item =>

            '<div class="card">' +

            '<div class="label">' +
            item[0] +
            '</div>' +

            '<div class="value">' +
            item[1] +
            '</div>' +

            '</div>'
        )
        .join(
          ''
        );


  document
    .getElementById(
      'detail'
    )
    .textContent =

      JSON.stringify(

        {

          guards:
            d.guards,

          setupStats:
            d.stats.setupStats,

          exitStats:
            d.stats.exitStats,

          config:
            d.config,

          positions:
            d.positions,

          exchange:
            d.exchanges
        },

        null,

        2
      );
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
// MAIN
// ============================================================

async function main() {

  console.log(
    '=============================================='
  );


  console.log(
    `LOMY ${C.version}`
  );


  console.log(
    'PAPER ONLY'
  );


  console.log(
    'STARTING CAPITAL: $1000'
  );


  console.log(
    '15M ENTRY + 1H CONTEXT'
  );


  console.log(
    'V6.3.1 ENTRY STRATEGY FROZEN / UNCHANGED'
  );


  console.log(
    'MOMENTUM ENTRY + RETEST ENTRY ONLY'
  );


  console.log(
    'BINANCE PUBLIC DATA-API'
  );


  console.log(
    'SPREAD GUARD + LIQUIDITY + SAR'
  );


  console.log(
    'ATR% 0.50 = SHADOW ONLY, NOT ENTRY FILTER'
  );


  console.log(
    'V6.3.2 COST-AWARE CAPITAL PROTECTION'
  );


  console.log(
    'V6.3.2 STAGED DYNAMIC TRAIL'
  );


  console.log(
    'V6.3.2 CONSERVATIVE FAILURE CONTROL'
  );


  console.log(
    'ENTRY CANDLE + POSITION PATH TELEMETRY'
  );


  console.log(

    `CAPITAL NET TARGET: ` +

    `${C.capitalNetLockPct.toFixed(2)}% | ` +

    `RAW LOCK NEEDED: ` +

    `${capitalProtectionGrossLockPct().toFixed(3)}%`
  );


  console.log(
    '=============================================='
  );


  await connectCloud();


  await loadState();


  app.listen(

    PORT,

    () => {

      console.log(

        `SERVER LISTENING ${PORT}`
      );
    }
  );


  await cloudJournal({

    type:
      'BOOT',

    startingState: {

      cash,

      startingBalance:
        C.startingBalance,

      openPositions:

        Object.keys(
          positions
        ).length,

      cycle:
        cycle.id,

      cycleEntries:
        cycle.entries
    },

    architecture: {

      entryTimeframe:
        C.entryInterval,

      contextTimeframe:
        C.contextInterval,

      cycleLimit:
        C.maxEntriesPerCycle,

      marketData:
        'BINANCE_PUBLIC_DATA_API',

      universeSize:
        C.universeSize,

      spreadGuardBps:
        C.maxEntrySpreadBps,

      atrPctShadowThreshold:
        C.atrPctShadowThreshold,

      entryStrategy:
        'V6.3.1_FROZEN',

      profitManager:
        'COST_AWARE_PROTECTION + STAGED_TRAIL + CONSERVATIVE_FAILURE_CONTROL',

      telemetry:
        'ENTRY_CANDLE + POSITION_PATH_5M',

      microstructure:
        'EXECUTION_SAFETY_ONLY + FLOW_TELEMETRY + SAR'
    },

    managerConfig: {

      capitalProtectionTriggerPct:
        C.capitalProtectionTriggerPct,

      capitalNetLockPct:
        C.capitalNetLockPct,

      capitalGrossLockPct:
        capitalProtectionGrossLockPct(),

      trailingStartPct:
        C.trailingStartPct,

      failureMinAgeMinutes:
        C.failureMinAgeMinutes,

      failureMaxMfePct:
        C.failureMaxMfePct,

      failureDrawdownPct:
        C.failureDrawdownPct
    }
  });


  tg(

    `🚀 <b>LOMY ${C.version}</b>\n` +

    `PAPER ONLY\n` +

    `Capital: $1000\n` +

    `15m + 1H | Momentum + Retest\n` +

    `V6.3.1 ENTRY FROZEN\n` +

    `Cost-aware protection + staged trail\n` +

    `Public Binance Data | Spread + SaR\n` +

    `Cycle ${cycle.id}: ${cycle.entries}/${C.maxEntriesPerCycle}`
  );


  if (

    cycle.state ===
      'WAITING_CLOSE'

    &&

    Object.keys(
      positions
    ).length ===
      0

  ) {

    await startNewCycle(
      'RECOVERED_EMPTY_CYCLE'
    );

  } else {

    setTimeout(
      runFreshScan,
      1500
    );
  }


  setInterval(

    runFreshScan,

    C.scanEveryMs
  );


  setInterval(

    managePositions,

    C.pricePollMs
  );


  setInterval(

    saveState,

    C.stateSaveMs
  );
}


// ============================================================
// SAFE SHUTDOWN
// ============================================================

async function shutdown() {

  if (
    shuttingDown
  ) {

    return;
  }


  shuttingDown =
    true;


  try {

    await saveState();

  } catch (
    error
  ) {

  }


  try {

    if (
      mongoClient
    ) {

      await mongoClient.close();
    }

  } catch (
    error
  ) {

  }


  process.exit(
    0
  );
}


process.on(
  'SIGTERM',
  shutdown
);


process.on(
  'SIGINT',
  shutdown
);


main()
  .catch(
    error => {

      console.error(

        'FATAL:',

        error
      );


      process.exit(
        1
      );
    }
  );
