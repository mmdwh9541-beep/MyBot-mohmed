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
// LOMY V6.3
// Public Binance market data + PAPER execution only.
// Designed to stay light on Render Free.
// ============================================================

const BINANCE_PUBLIC_REST = 'https://api.binance.com';

const C = Object.freeze({

  version:
    '6.3-5M-1H-PUBLICDATA-LIGHT-MICRO',

  stateKey:
    'main-v63',

  paperTrading:
    true,

  startingBalance:
    10000,


  // ==========================================================
  // MARKET / RUNTIME
  // ==========================================================

  universeSize:
    140,

  minQuoteVolume:
    1500000,

  scanEveryMs:
    90 * 1000,

  scanConcurrency:
    5,

  maxEntriesPerScan:
    3,

  maxEntriesPerCycle:
    120,

  maxPositions:
    8,

  universeRefreshMs:
    10 * 60 * 1000,

  pricePollMs:
    5000,

  stateSaveMs:
    45000,

  requestTimeoutMs:
    10000,

  requestGapMs:
    45,


  // ==========================================================
  // TIMEFRAMES
  // ==========================================================

  entryInterval:
    '5m',

  contextInterval:
    '1h',

  klineLimitEntry:
    90,

  klineLimitContext:
    70,


  // ==========================================================
  // CORE
  // ==========================================================

  emaFast:
    9,

  emaSlow:
    21,

  cmoLength:
    9,

  cmoBuyMin:
    28,

  cmoBuyMax:
    88,

  volumeSmaLength:
    12,

  momentumVolumeMin:
    1.20,

  momentumVolumeMax:
    7.50,

  minBodyRatio:
    0.48,


  // ==========================================================
  // BREAKOUT / RETEST / PULLBACK
  // ==========================================================

  breakoutLookback:
    20,

  breakoutBufferPct:
    0.02,

  minBreakoutDistanceAtr:
    0.08,

  maxBreakoutDistanceAtr:
    0.78,

  retestLookbackBars:
    4,

  retestTolerancePct:
    0.22,

  retestMinVolumeRatio:
    0.90,

  pullbackMaxDistanceEmaAtr:
    0.45,

  pullbackMinVolumeRatio:
    1.00,


  // ==========================================================
  // ENTRY QUALITY
  // ==========================================================

  maxChasePct:
    0.16,

  maxNegativeDriftPct:
    0.45,

  minContextEmaGapPct:
    0.05,

  maxContextExtensionPct:
    3.5,


  // ==========================================================
  // PAPER COSTS / RISK
  // ==========================================================

  feePct:
    0.001,

  slippagePct:
    0.0005,

  riskPerTradePct:
    0.0035,

  maxAllocationPct:
    0.16,

  minEmergencyStopPct:
    0.0060,

  maxEmergencyStopPct:
    0.0135,

  atrStopMultiplier:
    1.45,

  structureBufferAtr:
    0.15,


  // ==========================================================
  // PROFIT MANAGEMENT
  // ==========================================================

  breakEvenTriggerPct:
    0.48,

  breakEvenExtraPct:
    0.03,

  trailingStartPct:
    0.72,

  trailingAtrMultiplier:
    0.90,

  trailingGapMinPct:
    0.22,

  trailingGapMaxPct:
    0.52,


  // ==========================================================
  // FOLLOW THROUGH MANAGER
  // ==========================================================

  followThroughCheck1Min:
    18,

  followThroughMinMfe1:
    0.22,

  followThroughCurrentFloor1:
    -0.10,

  followThroughCheck2Min:
    35,

  followThroughMinMfe2:
    0.45,

  followThroughCurrentFloor2:
    0.05,


  // ==========================================================
  // MICROSTRUCTURE
  // Only final entry candidates reach this stage.
  // ==========================================================

  depthLimit:
    50,

  aggTradeLimit:
    100,

  microTopLevels:
    12,

  microFlowWindowMs:
    45 * 1000,

  microMinRecentTrades:
    6,

  maxEntrySpreadBps:
    7.0,

  stressedSpreadBps:
    14,

  stressedMinSideDepthUSDT:
    5000,

  severeSellBookImbalance:
    -0.35,

  severeMicropriceEdgeBps:
    -4,

  minFlowImbalance:
    -0.18,

  sarReduceFromBps:
    10,

  sarRejectBps:
    20,

  sarMinSizeScale:
    0.50,


  // ==========================================================
  // ACCOUNT GUARDS
  // ==========================================================

  dailyLossLimitPct:
    0.035,

  maxAccountDrawdownPct:
    0.07,

  symbolLossCooldownMs:
    45 * 60 * 1000,

  emergencyLossCooldownMs:
    90 * 60 * 1000,


  // ==========================================================
  // BINANCE HEALTH
  // ==========================================================

  binance429PauseMs:
    5 * 60 * 1000,

  binance418PauseMs:
    30 * 60 * 1000
});


const IGNORED =
  new Set([

    'USDCUSDT',
    'FDUSDUSDT',
    'TUSDUSDT',
    'USDPUSDT',
    'BUSDUSDT',
    'DAIUSDT',
    'USDEUSDT',
    'USD1USDT',
    'EURUSDT',
    'AEURUSDT',
    'TRYUSDT',
    'BRLUSDT'

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


function roundTripCostPct() {

  return (
    C.feePct +
    C.slippagePct
  ) *
    2 *
    100;
}


// ============================================================
// ACCOUNT STATE
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

  earlyCuts:
    0,

  setupStats:
    {},

  cycleCount:
    1
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

  scanned:
    0
};


const lastLossBySymbol =
  {};

const lastEmergencyLossBySymbol =
  {};

const lastClosedSignalBySymbol =
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


let mongoClient =
  null;

let db =
  null;

let cloudConnected =
  false;


// ============================================================
// EQUITY / GUARDS
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

        `${BINANCE_PUBLIC_REST}${path}`,

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
// MARKET DATA
// ============================================================

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
        row => ({

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
        })
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

      `KLINES_NOT_READY ${symbol} ${interval}`
    );
  }


  return {

    source:
      'BINANCE_PUBLIC',

    candles:
      rows
  };
}


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
          ),

        count:
          n(
            row.count
          )
      })
    )

    .filter(
      row =>

        row.quoteVolume >=
          C.minQuoteVolume

        &&

        row.count >
          500
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

    `UNIVERSE PUBLIC | ${universe.length} symbols`
  );


  return universe;
}


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
      'INVALID_PRICE'
    );
  }


  return {

    price,

    source:
      'BINANCE_PUBLIC'
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


  const gapPct =

    pct(
      ema9 -
      ema21,
      ema21
    );


  const extensionPct =

    pct(
      current.close -
      ema9,
      ema9
    );


  const bullish =

    ema9 >
      ema21

    &&

    current.close >=
      ema9

    &&

    gapPct >=
      C.minContextEmaGapPct

    &&

    extensionPct <=
      C.maxContextExtensionPct;


  return {

    ok:
      bullish,

    bullish,

    ema9,

    ema21,

    close:
      current.close,

    gapPct,

    extensionPct
  };
}


// ============================================================
// RECENT BREAKOUT
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
// ENTRY ENGINE
// MOMENTUM + RETEST + PULLBACK CONTINUATION
// ============================================================

function analyzeEntry(
  candles
) {

  if (
    candles.length <
    40
  ) {

    return {

      eligible:
        false,

      reason:
        'ENTRY_NOT_READY'
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


  // ==========================================================
  // BASE QUALITY
  // ==========================================================

  const trendOk =

    ema9 >
    ema21;


  const cmoOk =

    momentum >=
      C.cmoBuyMin

    &&

    momentum <=
      C.cmoBuyMax;


  const candleOk =

    current.close >
      current.open

    &&

    bodyRatio >=
      C.minBodyRatio;


  /*
    Very high volume was not automatically better
    in the V6.2.1 results.

    Extreme spike can be exhaustion.
  */

  const volumeNotExhausted =

    volumeRatio <=
      C.momentumVolumeMax;


  // ==========================================================
  // MOMENTUM CONTINUATION
  // ==========================================================

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


  const breakoutLocationOk =

    breakoutDistanceAtr >=
      C.minBreakoutDistanceAtr

    &&

    breakoutDistanceAtr <=
      C.maxBreakoutDistanceAtr;


  const momentumVolumeOk =

    volumeRatio >=
      C.momentumVolumeMin

    &&

    volumeNotExhausted;


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

    breakoutLocationOk;


  // ==========================================================
  // RETEST
  // ==========================================================

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
      C.retestMinVolumeRatio

    &&

    volumeNotExhausted;


  const retestEntry =

    trendOk

    &&

    cmoOk

    &&

    current.close >
      current.open

    &&

    recentBreakout.found

    &&

    touchedRetest

    &&

    heldRetest

    &&

    retestVolumeOk;


  // ==========================================================
  // PULLBACK CONTINUATION
  // Adds frequency without allowing random counter-trend trades.
  // ==========================================================

  const distanceFromEmaAtr =

    atrValue >
    0

      ? Math.abs(
          current.low -
          ema9
        ) /
        atrValue

      : 999;


  const pullbackTouched =

    distanceFromEmaAtr <=
      C.pullbackMaxDistanceEmaAtr;


  const reclaim =

    current.close >
      ema9

    &&

    current.close >
      previous.close

    &&

    current.close >
      current.open;


  const pullbackVolumeOk =

    volumeRatio >=
      C.pullbackMinVolumeRatio

    &&

    volumeNotExhausted;


  const pullbackContinuation =

    trendOk

    &&

    cmoOk

    &&

    candleOk

    &&

    pullbackTouched

    &&

    reclaim

    &&

    pullbackVolumeOk

    &&

    !cleanBreakout;


  // ==========================================================
  // ENTRY TYPE
  // ==========================================================

  let entryType =
    'NONE';


  /*
    Retest receives priority because if the current candle
    qualifies as a true retest we want it classified correctly.
  */

  if (
    retestEntry
  ) {

    entryType =
      'RETEST_ENTRY';

  } else if (
    momentumEntry
  ) {

    entryType =
      'MOMENTUM_CONTINUATION';

  } else if (
    pullbackContinuation
  ) {

    entryType =
      'PULLBACK_CONTINUATION';
  }


  /*
    Ranking does not turn an invalid setup
    into a valid trade.
  */

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
        C.cmoBuyMin
      ) *
      0.45,

      0,

      16
    )

    +

    clamp(

      (
        Math.min(
          volumeRatio,
          4
        ) -
        1
      ) *
      7,

      0,

      18
    )

    +

    clamp(

      bodyRatio *
      14,

      0,

      14
    )

    +

    (
      trendOk
        ? 7
        : 0
    )

    +

    (
      entryType ===
      'RETEST_ENTRY'
        ? 5
        : 0
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

    ema9,

    ema21,

    cmo:
      momentum,

    atr:
      atrValue,

    atrPct:

      pct(
        atrValue,
        current.close
      ),

    volumeRatio,

    bodyRatio,

    resistance,

    swingLow,

    breakoutDistanceAtr,

    distanceFromEmaAtr,

    rankScore:
      +rankScore.toFixed(
        2
      ),

    checks: {

      trendOk,

      cmoOk,

      candleOk,

      volumeNotExhausted,

      cleanBreakout,

      breakoutLocationOk,

      recentBreakout:
        recentBreakout.found,

      touchedRetest,

      heldRetest,

      pullbackTouched,

      reclaim
    }
  };
}


// ============================================================
// STAGED ANALYSIS
// ============================================================

async function analyzeEntryOnly(
  symbol
) {

  if (
    positions[
      symbol
    ]
  ) {

    return null;
  }


  const lastLoss =

    n(
      lastLossBySymbol[
        symbol
      ]
    );


  const lastEmergency =

    n(
      lastEmergencyLossBySymbol[
        symbol
      ]
    );


  if (

    lastEmergency

    &&

    Date.now() -
      lastEmergency <
      C.emergencyLossCooldownMs

  ) {

    return null;
  }


  if (

    lastLoss

    &&

    Date.now() -
      lastLoss <
      C.symbolLossCooldownMs

  ) {

    return null;
  }


  const entryResult =

    await fetchClosedKlines(

      symbol,

      C.entryInterval,

      C.klineLimitEntry
    );


  const signal =

    analyzeEntry(
      entryResult.candles
    );


  if (
    !signal.eligible
  ) {

    return null;
  }


  /*
    Never reopen the exact same closed signal.
  */

  if (

    lastClosedSignalBySymbol[
      symbol
    ]

    &&

    lastClosedSignalBySymbol[
      symbol
    ] ===
      signal.signalTime

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


async function addContext(
  candidate
) {

  const contextResult =

    await fetchClosedKlines(

      candidate.symbol,

      C.contextInterval,

      C.klineLimitContext
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

    await analyzeEntryOnly(
      symbol
    );


  if (
    !base
  ) {

    return null;
  }


  return addContext(
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

        if (

          !String(
            error.message
          ).includes(
            'TEMP_BLOCKED'
          )

        ) {

          console.warn(

            `SCAN ${items[i]} | ${error.message}`
          );
        }
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
// MICROSTRUCTURE
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


  const weighted =
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
              0.15
            )
          ),

        0
      );


  const bidDepthUSDT =
    weighted(
      bids
    );


  const askDepthUSDT =
    weighted(
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
      4.5

    &&

    minSideDepth >=
      C.stressedMinSideDepthUSDT *
      4

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


    /*
      Binance:
      buyerMaker=true means seller was aggressive.
    */

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

    const fraction =

      (
        bps -
        C.sarReduceFromBps
      ) /

      (
        C.sarRejectBps -
        C.sarReduceFromBps
      );


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


async function stateFirstMicrostructureGate(
  symbol,
  quoteAmount
) {

  const [
    bookRaw,
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
      bookRaw
    );


  const flow =
    aggressiveFlow(
      flowRows
    );


  const sar =
    slippageAtRisk(

      bookRaw.asks,

      quoteAmount
    );


  // ==========================================================
  // BOOK VALIDITY
  // ==========================================================

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


  // ==========================================================
  // STRESSED LIQUIDITY
  // ==========================================================

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


  // ==========================================================
  // SPREAD
  // ==========================================================

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


  // ==========================================================
  // SAR
  // ==========================================================

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


  // ==========================================================
  // SEVERE ADVERSE BOOK
  // ==========================================================

  if (

    book.imbalance <=
      C.severeSellBookImbalance

    &&

    book.micropriceEdgeBps <=
      C.severeMicropriceEdgeBps

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


  /*
    IMPORTANT V6.3 FIX:

    CALM does NOT automatically mean BUY confirmation.

    This fixes the old condition where:
    book.state === CALM
    could make MICRO pass by itself.
  */

  if (

    flow.sufficient

    &&

    flow.imbalance <
      C.minFlowImbalance

  ) {

    return {

      pass:
        false,

      reason:
        'NEGATIVE_AGGRESSIVE_FLOW',

      book,

      flow,

      sar
    };
  }


  const directionalConfirmations =

    [

      (
        flow.sufficient

        &&

        flow.imbalance >=
          0.05
      ),

      (
        book.imbalance >=
          0.08

        &&

        book.micropriceEdgeBps >=
          0
      )

    ].filter(
      Boolean
    ).length;


  /*
    If actual aggressive flow is temporarily sparse,
    do not call it directional confirmation.

    We only allow execution if spread/depth/SaR
    are healthy, and record it explicitly as NEUTRAL.
  */

  return {

    pass:
      true,

    reason:

      directionalConfirmations >
      0

        ? 'MICRO_CONFIRMED'

        : 'MICRO_EXECUTION_OK_FLOW_NEUTRAL',

    confirmations:
      directionalConfirmations,

    book,

    flow,

    sar
  };
}


// ============================================================
// RISK
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


  /*
    V6.2.1 used Math.min() here.

    That selected the farther stop,
    frequently allowing losses to expand.

    V6.3 selects the tighter valid defensive stop
    and then applies min/max risk boundaries.
  */

  let stop =

    Math.max(

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


  Object.assign(

    lastEmergencyLossBySymbol,

    state.lastEmergencyLossBySymbol ||
    {}
  );


  Object.assign(

    lastClosedSignalBySymbol,

    state.lastClosedSignalBySymbol ||
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

            lastEmergencyLossBySymbol,

            lastClosedSignalBySymbol,

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
// OPEN PAPER TRADE
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


  // ==========================================================
  // FRESH REVALIDATION
  // ==========================================================

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


  /*
    Positive drift means we're chasing upward.
    Negative drift can be a better entry,
    within reasonable limits.
  */

  const drift =

    pct(

      livePrice -
      fresh.signal.signalPrice,

      fresh.signal.signalPrice
    );


  if (

    drift >
      C.maxChasePct

    ||

    drift <
      -C.maxNegativeDriftPct

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


  // ==========================================================
  // MICROSTRUCTURE ONLY FOR FINAL ENTRY
  // ==========================================================

  const micro =

    await stateFirstMicrostructureGate(

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

    trailingActive:
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

    snapshot:
      fresh,

    microstructure:
      micro,

    followThroughStage:
      0
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

    allocation,

    snapshot:
      fresh,

    microstructure:
      micro
  });


  tg(

    `🟢 <b>LOMY V6.3 PAPER ENTRY</b>\n` +

    `${candidate.symbol}\n` +

    `${fresh.signal.entryType}\n` +

    `Rank ${fresh.signal.rankScore}\n` +

    `CMO ${fresh.signal.cmo.toFixed(1)} | ` +

    `Vol ${fresh.signal.volumeRatio.toFixed(2)}x\n` +

    `Spread ${micro.book.spreadBps.toFixed(2)}bps | ` +

    `Flow ${

      micro.flow.sufficient

        ? micro.flow.imbalance.toFixed(
            2
          )

        : 'NEUTRAL'

    }\n` +

    `Cycle ${cycle.entries}/${C.maxEntriesPerCycle}`
  );


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


  if (
    profit >
    0
  ) {

    stats.wins++;

    stats.grossProfit +=
      profit;

    setup.wins++;

  } else {

    stats.losses++;

    stats.grossLoss +=
      Math.abs(
        profit
      );

    setup.losses++;

    lastLossBySymbol[
      symbol
    ] =
      Date.now();
  }


  stats.setupStats[
    position.entryType
  ] =
    setup;


  if (
    reason ===
    'EMERGENCY_STOP'
  ) {

    lastEmergencyLossBySymbol[
      symbol
    ] =
      Date.now();
  }


  if (

    reason ===
      'EARLY_NO_FOLLOW_THROUGH'

    ||

    reason ===
      'STALE_MOMENTUM'

  ) {

    stats.earlyCuts++;
  }


  lastClosedSignalBySymbol[
    symbol
  ] =
    position.signalTime;


  const record = {

    ...position,

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

    exitSource:
      source,

    mfePct:
      position.mfePct,

    maePct:
      position.maePct
  });


  tg(

    `${profit >= 0 ? '✅' : '🔴'} ` +

    `<b>${symbol} CLOSED</b>\n` +

    `${reason}\n` +

    `PnL $${profit.toFixed(2)} ` +

    `(${profitPct.toFixed(2)}%)\n` +

    `MFE ${position.mfePct.toFixed(2)}% | ` +

    `MAE ${position.maePct.toFixed(2)}%`
  );


  updateAccountGuards();
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


        const heldMinutes =

          (
            Date.now() -
            position.entryTime
          ) /
          60000;


        // ======================================================
        // FOLLOW THROUGH CHECK #1
        // ======================================================

        if (

          heldMinutes >=
            C.followThroughCheck1Min

          &&

          position.followThroughStage <
            1

        ) {

          position.followThroughStage =
            1;


          if (

            position.mfePct <
              C.followThroughMinMfe1

            &&

            movePct <=
              C.followThroughCurrentFloor1

          ) {

            await closePaperTrade(

              symbol,

              priceData.price,

              'EARLY_NO_FOLLOW_THROUGH',

              priceData.source
            );


            continue;
          }
        }


        // ======================================================
        // FOLLOW THROUGH CHECK #2
        // ======================================================

        if (

          heldMinutes >=
            C.followThroughCheck2Min

          &&

          position.followThroughStage <
            2

        ) {

          position.followThroughStage =
            2;


          if (

            position.mfePct <
              C.followThroughMinMfe2

            &&

            movePct <=
              C.followThroughCurrentFloor2

          ) {

            await closePaperTrade(

              symbol,

              priceData.price,

              'STALE_MOMENTUM',

              priceData.source
            );


            continue;
          }
        }


        // ======================================================
        // CAPITAL PROTECTION
        // ======================================================

        if (

          !position.breakEvenActive

          &&

          movePct >=
            C.breakEvenTriggerPct

        ) {

          const protectPct =

            roundTripCostPct() +

            C.breakEvenExtraPct;


          position.protectionStop =

            position.entryPrice *

            (
              1 +
              protectPct /
              100
            );


          position.breakEvenActive =
            true;


          stats.breakEvenMoves++;


          await cloudJournal({

            type:
              'CAPITAL_PROTECTION',

            tradeId:
              position.tradeId,

            symbol,

            protectionStop:
              position.protectionStop
          });
        }


        // ======================================================
        // TRAIL ACTIVATION
        // ======================================================

        if (

          !position.trailingActive

          &&

          movePct >=
            C.trailingStartPct

        ) {

          position.trailingActive =
            true;


          stats.trailingActivations++;


          await cloudJournal({

            type:
              'TRAIL_ACTIVATED',

            tradeId:
              position.tradeId,

            symbol
          });
        }


        // ======================================================
        // DYNAMIC TRAIL
        // ======================================================

        if (
          position.trailingActive
        ) {

          const gapPct =

            clamp(

              position.atrPct *
              C.trailingAtrMultiplier,

              C.trailingGapMinPct,

              C.trailingGapMaxPct
            );


          const trailingStop =

            position.highestPrice *

            (
              1 -
              gapPct /
              100
            );


          position.protectionStop =

            Math.max(

              position.protectionStop ||
              0,

              trailingStop
            );
        }


        // ======================================================
        // ACTIVE STOP
        // ======================================================

        let activeStop =
          position.emergencyStop;


        let exitReason =
          'EMERGENCY_STOP';


        if (
          position.protectionStop
        ) {

          activeStop =

            Math.max(

              activeStop,

              position.protectionStop
            );


          exitReason =

            position.trailingActive

              ? 'DYNAMIC_TRAIL'

              : 'CAPITAL_PROTECTION_STOP';
        }


        if (

          priceData.price <=
          activeStop

        ) {

          await closePaperTrade(

            symbol,

            priceData.price,

            exitReason,

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
        60
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


    cycle.scanned =
      symbols.length;


    /*
      STAGE 1:
      Only fetch 5m data for the whole universe.

      This is much lighter than downloading
      5m + 1h for every symbol.
    */

    const rawCandidates =

      await mapLimit(

        symbols,

        C.scanConcurrency,

        analyzeEntryOnly
      );


    rawCandidates.sort(

      (
        a,
        b
      ) =>

        b.signal.rankScore -

        a.signal.rankScore
    );


    /*
      STAGE 2:
      Only the strongest potential candidates
      get the 1H context request.
    */

    const contextPool =

      rawCandidates.slice(

        0,

        Math.min(

          30,

          rawCandidates.length
        )
      );


    const candidates =

      await mapLimit(

        contextPool,

        3,

        addContext
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
                candidate.signal.bodyRatio
            })
          )
    });


    console.log(

      `SCAN DONE | ` +

      `${symbols.length} checked | ` +

      `${rawCandidates.length} setup | ` +

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


      await sleep(
        80
      );
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

      entryTimeframe:
        C.entryInterval,

      contextTimeframe:
        C.contextInterval,

      universeSize:
        C.universeSize,

      maxPositions:
        C.maxPositions,

      marketData:
        'BINANCE_PUBLIC',

      microstructure:
        'LIGHT_EXECUTION_GATE + REAL_AGG_FLOW + SAR'
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
// DASHBOARD
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
LOMY V6.3
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
    repeat(auto-fit,minmax(160px,1fr));
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
LOMY V6.3
</h2>

<div>
5M Entry + 1H Context • Binance Public Data • PAPER ONLY • Light Micro + Real Flow + SaR
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
      'Candidates',
      d.cycle.lastScanCandidates
    ],

    [
      'Scanned',
      d.cycle.scanned
    ],

    [
      'Universe',
      d.cycle.lastUniverseSource ||
      '-'
    ],

    [
      'Early Cuts',
      d.stats.earlyCuts ||
      0
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
    'BINANCE PUBLIC MARKET DATA'
  );

  console.log(
    '5M ENTRY + 1H CONTEXT'
  );

  console.log(
    'MOMENTUM + RETEST + PULLBACK CONTINUATION'
  );

  console.log(
    'LIGHT MICRO + REAL AGG FLOW + SAR'
  );

  console.log(
    'FOLLOW-THROUGH LOSS CUTTER'
  );

  console.log(
    'RENDER-FREE FRIENDLY STAGED SCANNER'
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

    architecture: {

      entryTimeframe:
        C.entryInterval,

      contextTimeframe:
        C.contextInterval,

      marketData:
        'BINANCE_PUBLIC',

      universeSize:
        C.universeSize,

      microstructure:
        'LIGHT_REAL_FLOW_SAR'
    }
  });


  tg(

    `🚀 <b>LOMY ${C.version}</b>\n` +

    `PAPER ONLY\n` +

    `Binance Public Market Data\n` +

    `5m + 1H\n` +

    `Light Micro + Real Flow + SaR`
  );


  setTimeout(
    runFreshScan,
    1500
  );


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
