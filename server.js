require('dotenv').config();

const express = require('express');
const axios = require('axios');
const { MongoClient } = require('mongodb');

const app = express();
app.use(express.json());

const PORT = Number(process.env.PORT || 5000);

const TELEGRAM_TOKEN =
  process.env.TELEGRAM_TOKEN || '';

const TELEGRAM_CHAT_ID =
  process.env.TELEGRAM_CHAT_ID || '';

const MONGODB_URI =
  process.env.MONGODB_URI || '';

const MONGODB_DB =
  process.env.MONGODB_DB || 'lomy';

const REST_BASE =
  'https://data-api.binance.vision';

// ============================================================
// CONFIG
// ============================================================

const C = Object.freeze({

  version:
    '6.1.0-15M-CYCLE20',

  stateKey:
    'main-v610',

  paperTrading:
    true,

  startingBalance:
    10000,

  // ==========================================================
  // MARKET DISCOVERY
  // ==========================================================

  universeSize:
    100,

  minQuoteVolume:
    500000,

  scanEveryMs:
    2 * 60 * 1000,

  scanConcurrency:
    6,

  maxEntriesPerScan:
    2,

  // ==========================================================
  // CYCLE ENGINE
  // ==========================================================

  maxEntriesPerCycle:
    20,

  maxPositions:
    6,

  // ==========================================================
  // TIMEFRAMES
  // ==========================================================

  entryInterval:
    '15m',

  contextInterval:
    '1h',

  klineLimit15m:
    80,

  klineLimit1h:
    80,

  // ==========================================================
  // ULTRA FAST CORE
  // ==========================================================

  emaFast:
    9,

  emaSlow:
    21,

  cmoLength:
    9,

  cmoBuyMin:
    30,

  volumeSmaLength:
    10,

  momentumVolumeMultiplier:
    1.30,

  minBodyRatio:
    0.50,

  // ==========================================================
  // MOMENTUM / RETEST
  // ==========================================================

  breakoutLookback:
    20,

  breakoutBufferPct:
    0.03,

  maxMomentumExtensionAtr:
    0.85,

  retestTolerancePct:
    0.20,

  retestLookbackBars:
    3,

  retestMinVolumeRatio:
    1.00,

  maxEntryDriftPct:
    0.18,

  // ==========================================================
  // RISK + EXIT
  // ==========================================================

  feePct:
    0.001,

  slippagePct:
    0.0005,

  riskPerTradePct:
    0.0045,

  maxAllocationPct:
    0.22,

  minEmergencyStopPct:
    0.0065,

  maxEmergencyStopPct:
    0.018,

  atrStopMultiplier:
    1.65,

  structureBufferAtr:
    0.20,

  breakEvenTriggerPct:
    0.50,

  breakEvenExtraPct:
    0.02,

  trailingStartPct:
    0.70,

  trailingAtrMultiplier:
    0.90,

  trailingGapMinPct:
    0.22,

  trailingGapMaxPct:
    0.55,

  // ==========================================================
  // ACCOUNT PROTECTION
  // ==========================================================

  dailyLossLimitPct:
    0.035,

  maxAccountDrawdownPct:
    0.07,

  symbolLossCooldownMs:
    45 * 60 * 1000,

  // ==========================================================
  // RUNTIME
  // ==========================================================

  pricePollMs:
    5000,

  stateSaveMs:
    30000,

  universeRefreshMs:
    15 * 60 * 1000,

  requestTimeoutMs:
    12000
});


// ============================================================
// IGNORED PAIRS
// ============================================================

const IGNORED =
  new Set([
    'USDCUSDT',
    'FDUSDUSDT',
    'TUSDUSDT',
    'USDPUSDT',
    'BUSDUSDT',
    'DAIUSDT',
    'USDEUSDT',
    'USD1USDT'
  ]);


// ============================================================
// HELPERS
// ============================================================

const sleep =
  ms =>
    new Promise(
      resolve =>
        setTimeout(
          resolve,
          ms
        )
    );


const n =
  (
    value,
    fallback = 0
  ) => {

    const number =
      Number(
        value
      );

    return Number.isFinite(
      number
    )
      ? number
      : fallback;
  };


const clamp =
  (
    value,
    min,
    max
  ) =>
    Math.max(
      min,
      Math.min(
        max,
        value
      )
    );


const pct =
  (
    diff,
    base
  ) =>
    base
      ? (
          diff /
          base
        ) *
        100
      : 0;


const utcDay =
  () =>
    new Date()
      .toISOString()
      .slice(
        0,
        10
      );


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

  cycleCount:
    0
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
// MARKET STATE
// ============================================================

let universe =
  [];


let universeUpdatedAt =
  0;


let scanning =
  false;


let lastScanAt =
  0;


let shuttingDown =
  false;


// ============================================================
// CYCLE STATE
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
    0
};


const lastLossBySymbol =
  {};


// ============================================================
// MONGODB
// ============================================================

let mongoClient =
  null;


let db =
  null;


let cloudConnected =
  false;


// ============================================================
// COSTS
// ============================================================

function roundTripCostPct() {

  return (
    C.feePct +
    C.slippagePct
  ) *
    2 *
    100;
}


// ============================================================
// EQUITY
// ============================================================

function equity(
  markPrices = {}
) {

  let value =
    cash;

  for (
    const position
    of Object.values(
      positions
    )
  ) {

    const mark =
      n(
        markPrices[
          position.symbol
        ],
        position.lastPrice ||
        position.entryPrice
      );

    value +=
      position.qty *
      mark;
  }

  return value;
}


// ============================================================
// DAILY RESET
// ============================================================

function resetDailyIfNeeded(
  markPrices = {}
) {

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

  dailyStartEquity =
    equity(
      markPrices
    );

  dailyPnL =
    0;

  dailyPause =
    false;
}


// ============================================================
// ACCOUNT GUARDS
// ============================================================

function updateAccountGuards(
  markPrices = {}
) {

  const eq =
    equity(
      markPrices
    );

  peakEquity =
    Math.max(
      peakEquity,
      eq
    );

  const dd =
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
      dd
    );

  const dailyLoss =
    dailyStartEquity >
    0
      ? (
          -dailyPnL /
          dailyStartEquity
        )
      : 0;

  if (
    dailyLoss >=
    C.dailyLossLimitPct
  ) {

    dailyPause =
      true;
  }

  if (
    dd /
    100 >=
    C.maxAccountDrawdownPct
  ) {

    drawdownPause =
      true;
  }
}


// ============================================================
// ENTRY BLOCK
// ============================================================

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
// EMA
// ============================================================

function ema(
  values,
  period
) {

  if (
    !Array.isArray(
      values
    ) ||
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
      values[i] *
        multiplier +
      result *
        (
          1 -
          multiplier
        );
  }

  return result;
}


// ============================================================
// CMO
// ============================================================

function cmo(
  closes,
  period = 9
) {

  if (
    !Array.isArray(
      closes
    ) ||
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

    const diff =
      closes[i] -
      closes[
        i - 1
      ];

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

  const total =
    up +
    down;

  if (
    !total
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


// ============================================================
// ATR
// ============================================================

function atr(
  candles,
  period = 14
) {

  if (
    !Array.isArray(
      candles
    ) ||
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

    const tr =
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
      );

    values.push(
      tr
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


// ============================================================
// VOLUME SMA
// ============================================================

function smaVolume(
  candles,
  period,
  excludeLast = true
) {

  const base =
    excludeLast
      ? candles.slice(
          0,
          -1
        )
      : candles.slice();

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
// CANDLE BODY
// ============================================================

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


// ============================================================
// BINANCE KLINE PARSER
// ============================================================

function parseKline(
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

    trades:
      n(
        row[8]
      ),

    takerBuyBase:
      n(
        row[9]
      ),

    takerBuyQuote:
      n(
        row[10]
      )
  };
}


// ============================================================
// HTTP
// ============================================================

async function getJson(
  path,
  params = {}
) {

  const response =
    await axios.get(

      `${REST_BASE}${path}`,

      {
        params,

        timeout:
          C.requestTimeoutMs
      }
    );

  return response.data;
}


// ============================================================
// KLINES
// ============================================================

async function fetchClosedKlines(
  symbol,
  interval,
  limit
) {

  const data =
    await getJson(

      '/api/v3/klines',

      {
        symbol,
        interval,
        limit
      }
    );

  const now =
    Date.now();

  return data
    .map(
      parseKline
    )
    .filter(
      candle =>
        candle.closeTime <
        now
    );
}


// ============================================================
// FRESH UNIVERSE
// ============================================================

async function refreshUniverse(
  force = false
) {

  if (
    !force &&
    universe.length &&
    Date.now() -
      universeUpdatedAt <
      C.universeRefreshMs
  ) {

    return universe;
  }

  const rows =
    await getJson(
      '/api/v3/ticker/24hr'
    );

  universe =
    rows
      .filter(
        row =>
          row.symbol.endsWith(
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
        (
          a,
          b
        ) =>
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

  universeUpdatedAt =
    Date.now();

  return universe;
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
      ema21 &&
    current.close >=
      ema9;

  return {

    ok:
      bullish,

    ema9,

    ema21,

    close:
      current.close,

    reason:
      bullish
        ? '1H_BULLISH'
        : '1H_NOT_BULLISH'
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
// 15M ENTRY ENGINE
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

  const atrPct =
    pct(
      atrValue,
      current.close
    );

  const averageVolume =
    smaVolume(

      candles,

      C.volumeSmaLength,

      true
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

  const bullishCandle =
    current.close >
    current.open;

  // ----------------------------------------------------------
  // Resistance is calculated BEFORE the current + previous bar
  // ----------------------------------------------------------

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
  // ULTRA FAST HARD CONDITIONS
  // ==========================================================

  const trendOk =
    ema9 >
    ema21;

  const momentumOk =
    momentum >
    C.cmoBuyMin;

  const candleOk =
    bullishCandle &&
    bodyRatio >=
      C.minBodyRatio;

  // ==========================================================
  // MOMENTUM ENTRY
  // ==========================================================

  const breakoutDistance =
    current.close -
    resistance;

  const breakoutDistanceAtr =
    atrValue >
    0
      ? breakoutDistance /
        atrValue
      : 999;

  const cleanBreakout =
    current.close >
    resistance *
      (
        1 +
        C.breakoutBufferPct /
        100
      );

  const notExtended =
    breakoutDistanceAtr <=
    C.maxMomentumExtensionAtr;

  const volumeMomentumOk =
    volumeRatio >=
    C.momentumVolumeMultiplier;

  const momentumEntry =

    trendOk &&

    momentumOk &&

    candleOk &&

    volumeMomentumOk &&

    cleanBreakout &&

    notExtended;

  // ==========================================================
  // RETEST ENTRY
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

    current.volume >=
      previous.volume;


  const retestEntry =

    trendOk &&

    momentumOk &&

    candleOk &&

    recentBreakout.found &&

    touchedRetest &&

    heldRetest &&

    retestVolumeOk;


  // ==========================================================
  // FINAL ENTRY TYPE
  // ==========================================================

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


  // ==========================================================
  // RANKING ONLY
  // DOES NOT CREATE A TRADE BY ITSELF
  // ==========================================================

  const ranking =

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
      12,
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
      ema9 >
      ema21
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

    atrPct,

    volumeRatio,

    bodyRatio,

    resistance,

    swingLow,

    breakoutDistanceAtr,

    rankScore:
      +ranking.toFixed(
        2
      ),

    checks: {

      trendOk,

      momentumOk,

      candleOk,

      volumeMomentumOk,

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
// FULL FRESH ANALYSIS
// ============================================================

async function analyzeSymbolFresh(
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

  if (
    lastLoss &&
    Date.now() -
      lastLoss <
      C.symbolLossCooldownMs
  ) {

    return null;
  }

  const [
    candles15m,
    candles1h
  ] =
    await Promise.all([

      fetchClosedKlines(
        symbol,
        C.entryInterval,
        C.klineLimit15m
      ),

      fetchClosedKlines(
        symbol,
        C.contextInterval,
        C.klineLimit1h
      )
    ]);


  // ==========================================================
  // 1H MUST SUPPORT BUY
  // ==========================================================

  const context =
    analyze1h(
      candles1h
    );

  if (
    !context.ok
  ) {

    return null;
  }


  // ==========================================================
  // 15M ENTRY
  // ==========================================================

  const signal =
    analyze15m(
      candles15m
    );

  if (
    !signal.eligible
  ) {

    return null;
  }


  return {

    symbol,

    context,

    signal,

    analyzedAt:
      Date.now()
  };
}


// ============================================================
// CONCURRENCY MAP
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


  async function run() {

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

        const value =
          await worker(
            items[i],
            i
          );

        if (
          value
        ) {

          results.push(
            value
          );
        }

      } catch (
        error
      ) {

        console.warn(

          `Scan ${items[i]}:`,

          error.message
        );
      }
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

      run
    )
  );


  return results;
}


// ============================================================
// EMERGENCY STOP
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
  entryPrice,
  stopPct
) {

  const accountEquity =
    equity();


  const riskBudget =
    accountEquity *
    C.riskPerTradePct;


  const byRisk =
    stopPct >
    0
      ? riskBudget /
        stopPct
      : 0;


  const maximumAllocation =
    accountEquity *
    C.maxAllocationPct;


  const allocation =
    Math.min(

      byRisk,

      maximumAllocation,

      cash *
      0.98
    );


  if (
    allocation <
    10
  ) {

    return null;
  }


  return allocation;
}


// ============================================================
// JOURNAL
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


// ============================================================
// SAVE TRADE
// ============================================================

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
          cycle.id,

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
// TELEGRAM
// ============================================================

const tgQueue =
  [];


let tgBusy =
  false;


function tg(
  text
) {

  if (
    TELEGRAM_TOKEN &&
    TELEGRAM_CHAT_ID
  ) {

    tgQueue.push(
      String(
        text
      )
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


    tgBusy =
      true;


    const text =
      tgQueue.shift();


    try {

      await axios.post(

        `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,

        {

          chat_id:
            TELEGRAM_CHAT_ID,

          text,

          parse_mode:
            'HTML'
        },

        {
          timeout:
            10000
        }
      );

    } catch (
      error
    ) {

      console.error(
        'Telegram:',
        error.message
      );

    } finally {

      tgBusy =
        false;
    }

  },

  1200
);


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
  // FRESH REVALIDATION BEFORE EVERY SINGLE ENTRY
  // ==========================================================

  const fresh =
    await analyzeSymbolFresh(
      candidate.symbol
    );


  if (
    !fresh ||
    !fresh.signal.eligible
  ) {

    return false;
  }


  // ==========================================================
  // OLD RESEARCH IS NOT ACCEPTED
  // ==========================================================

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

      oldType:
        candidate.signal.entryType,

      newType:
        fresh.signal.entryType,

      oldSignalTime:
        candidate.signal.signalTime,

      newSignalTime:
        fresh.signal.signalTime
    });


    return false;
  }


  // ==========================================================
  // LIVE PRICE
  // ==========================================================

  const ticker =
    await getJson(

      '/api/v3/ticker/price',

      {
        symbol:
          candidate.symbol
      }
    );


  const livePrice =
    n(
      ticker.price
    );


  if (
    !livePrice
  ) {

    return false;
  }


  // ==========================================================
  // ANTI STALE / PRICE DRIFT
  // ==========================================================

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


  // ==========================================================
  // PAPER ENTRY
  // ==========================================================

  const simulatedEntry =
    livePrice *
    (
      1 +
      C.slippagePct
    );


  const stop =
    emergencyStopFrom(

      fresh.signal,

      simulatedEntry
    );


  const allocation =
    positionSize(

      simulatedEntry,

      stop.stopPct
    );


  if (
    !allocation
  ) {

    return false;
  }


  const buyFee =
    allocation *
    C.feePct;


  const netForAsset =
    allocation -
    buyFee;


  const qty =
    netForAsset /
    simulatedEntry;


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

    symbol:
      candidate.symbol,

    cycleId:
      cycle.id,

    entryType:
      fresh.signal.entryType,

    entryPrice:
      simulatedEntry,

    signalPrice:
      fresh.signal.signalPrice,

    signalTime:
      fresh.signal.signalTime,

    entryTime:
      Date.now(),

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
      simulatedEntry,

    lastPrice:
      simulatedEntry,

    mfePct:
      0,

    maePct:
      0,

    atrPct:
      fresh.signal.atrPct,

    snapshot:
      fresh
  };


  // ==========================================================
  // CYCLE COUNT
  // ==========================================================

  cycle.entries++;


  await cloudJournal({

    type:
      'ENTRY',

    tradeId,

    symbol:
      candidate.symbol,

    entryType:
      fresh.signal.entryType,

    entryPrice:
      simulatedEntry,

    allocation,

    emergencyStop:
      stop.stop,

    initialRiskPct:
      stop.stopPct *
      100,

    snapshot:
      fresh
  });


  tg(

    `🟢 <b>LOMY PAPER ENTRY</b>\n` +

    `${candidate.symbol}\n` +

    `Type: ${fresh.signal.entryType}\n` +

    `15m CMO: ${fresh.signal.cmo.toFixed(1)}\n` +

    `Volume: ${fresh.signal.volumeRatio.toFixed(2)}x\n` +

    `Cycle: ${cycle.entries}/${C.maxEntriesPerCycle}`
  );


  // ==========================================================
  // CYCLE FULL
  // ==========================================================

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

      `⏳ Cycle ${cycle.id} reached 20 entries.\n` +

      `Waiting for every open trade to close before a new cycle.`
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
  reason
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


  if (
    profit >
    0
  ) {

    stats.wins++;

    stats.grossProfit +=
      profit;

  } else {

    stats.losses++;

    stats.grossLoss +=
      Math.abs(
        profit
      );

    lastLossBySymbol[
      symbol
    ] =
      Date.now();
  }


  const record = {

    ...position,

    exitPrice,

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

    ...record
  });


  tg(

    `${profit >= 0 ? '✅' : '🔴'} <b>${symbol} CLOSED</b>\n` +

    `${reason}\n` +

    `PnL: $${profit.toFixed(2)} (${profitPct.toFixed(2)}%)\n` +

    `MFE: ${position.mfePct.toFixed(2)}% | ` +

    `MAE: ${position.maePct.toFixed(2)}%`
  );


  updateAccountGuards();


  await maybeStartNewCycle();
}


// ============================================================
// POSITION MANAGER
// ============================================================

async function managePositions() {

  if (
    !Object.keys(
      positions
    ).length
  ) {

    await maybeStartNewCycle();

    return;
  }


  try {

    const rows =
      await getJson(
        '/api/v3/ticker/price'
      );


    const prices =
      {};


    for (
      const row
      of rows
    ) {

      prices[
        row.symbol
      ] =
        n(
          row.price
        );
    }


    resetDailyIfNeeded(
      prices
    );


    for (
      const [
        symbol,
        position
      ]
      of Object.entries(
        positions
      )
    ) {

      const price =
        n(
          prices[
            symbol
          ]
        );


      if (
        !price
      ) {

        continue;
      }


      position.lastPrice =
        price;


      position.highestPrice =
        Math.max(

          position.highestPrice,

          price
        );


      const movePct =
        pct(

          price -
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


      // ======================================================
      // CAPITAL PROTECTION AT +0.5%
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
      // DYNAMIC TRAIL FROM +0.7%
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
      // TRAILING UPDATE
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


        const trailStop =

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

            trailStop
          );
      }


      // ======================================================
      // ACTIVE STOP
      // ======================================================

      let stop =
        position.emergencyStop;


      let exitReason =
        'EMERGENCY_STOP';


      if (
        position.protectionStop
      ) {

        stop =
          Math.max(

            stop,

            position.protectionStop
          );


        exitReason =
          position.trailingActive

            ? 'DYNAMIC_TRAIL'

            : 'CAPITAL_PROTECTION_STOP';
      }


      // ======================================================
      // EXIT
      // ======================================================

      if (
        price <=
        stop
      ) {

        await closePaperTrade(

          symbol,

          price,

          exitReason
        );
      }
    }


    updateAccountGuards(
      prices
    );

  } catch (
    error
  ) {

    console.error(
      'Position manager:',
      error.message
    );
  }
}


// ============================================================
// NEW CYCLE
// ============================================================

async function startNewCycle(
  reason =
    'PREVIOUS_CYCLE_COMPLETE'
) {

  cycle = {

    id:
      cycle.id +
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
      0
  };


  stats.cycleCount++;


  // ==========================================================
  // DELETE OLD RESEARCH
  // ==========================================================

  universe =
    [];


  universeUpdatedAt =
    0;


  await cloudJournal({

    type:
      'CYCLE_START',

    reason
  });


  tg(

    `🔄 <b>New LOMY cycle ${cycle.id}</b>\n` +

    `Fresh market research started.\n` +

    `Old candidates discarded.`
  );


  // ==========================================================
  // IMMEDIATE FRESH SCAN
  // ==========================================================

  setTimeout(
    runFreshScan,
    1000
  );
}


// ============================================================
// AUTO NEW CYCLE
// ============================================================

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
// FRESH MARKET SCAN
// ============================================================

async function runFreshScan() {

  if (
    scanning ||
    shuttingDown ||
    entryBlocked()
  ) {

    return;
  }


  if (
    cycle.entries >=
    C.maxEntriesPerCycle
  ) {

    cycle.state =
      'WAITING_CLOSE';

    return;
  }


  scanning =
    true;


  lastScanAt =
    Date.now();


  cycle.lastFreshScanAt =
    lastScanAt;


  try {

    // ========================================================
    // ABSOLUTELY FRESH MARKET UNIVERSE
    // ========================================================

    const symbols =
      await refreshUniverse(
        true
      );


    // ========================================================
    // THERE IS NO PERSISTENT CANDIDATE POOL
    // ========================================================

    const candidates =
      await mapLimit(

        symbols,

        C.scanConcurrency,

        analyzeSymbolFresh
      );


    // ========================================================
    // CHOOSE STRONGEST VALID OPPORTUNITIES
    // Ranking cannot make a bad setup valid.
    // ========================================================

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


    // ========================================================
    // JOURNAL THE FRESH SEARCH
    // ========================================================

    await cloudJournal({

      type:
        'FRESH_SCAN',

      scanned:
        symbols.length,

      qualified:
        candidates.length,

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


    // ========================================================
    // OPEN ONLY THE BEST CURRENT OPPORTUNITIES
    // ========================================================

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


      // ======================================================
      // THIS FUNCTION REANALYZES AGAIN BEFORE ENTRY
      // ======================================================

      const openedTrade =
        await openPaperTrade(
          candidate
        );


      if (
        openedTrade
      ) {

        opened++;
      }
    }

  } catch (
    error
  ) {

    console.error(
      'Fresh scan:',
      error.message
    );

  } finally {

    scanning =
      false;
  }
}


// ============================================================
// MONGODB CONNECT
// ============================================================

async function connectCloud() {

  if (
    !MONGODB_URI
  ) {

    console.warn(
      'MONGODB_URI missing. Running without persistence.'
    );

    return;
  }


  mongoClient =
    new MongoClient(

      MONGODB_URI,

      {
        maxPoolSize:
          8
      }
    );


  await mongoClient.connect();


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


// ============================================================
// LOAD STATE
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
  ) {

    console.log(
      'Fresh V6.1 PAPER state.'
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


  console.log(

    `STATE RESTORED | ` +

    `Cash $${cash.toFixed(2)} | ` +

    `Cycle ${cycle.id} | ` +

    `${cycle.entries}/20 | ` +

    `Open ${Object.keys(positions).length}`
  );
}


// ============================================================
// SAVE STATE
// ============================================================

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

            updatedAt:
              Date.now(),

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

            lastLossBySymbol
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
// DASHBOARD DATA
// ============================================================

function dashboardData() {

  const accountEquity =
    equity();


  const profitFactor =

    stats.grossLoss >
    0

      ? stats.grossProfit /
        stats.grossLoss

      : stats.grossProfit >
        0

        ? 999

        : 0;


  const winRate =

    stats.totalTrades

      ? (
          stats.wins /
          stats.totalTrades
        ) *
        100

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
      +accountEquity.toFixed(
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

    cycle,

    limits: {

      maxEntriesPerCycle:
        C.maxEntriesPerCycle,

      maxPositions:
        C.maxPositions,

      entryTimeframe:
        C.entryInterval,

      contextTimeframe:
        C.contextInterval
    },

    guards: {

      manualPause,

      dailyPause,

      drawdownPause
    },

    scan: {

      scanning,

      lastScanAt,

      universe:
        universe.length
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


// ============================================================
// PAUSE
// ============================================================

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
        true,

      manualPause
    });
  }
);


// ============================================================
// RESUME
// ============================================================

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
        true,

      manualPause
    });
  }
);


// ============================================================
// MANUAL FRESH SCAN
// ============================================================

app.post(

  '/api/scan-now',

  (
    req,
    res
  ) => {

    runFreshScan();


    res.json({

      ok:
        true,

      started:
        true
    });
  }
);


// ============================================================
// EMERGENCY CLOSE
// ============================================================

app.post(

  '/api/emergency-close',

  async (
    req,
    res
  ) => {

    try {

      const rows =
        await getJson(
          '/api/v3/ticker/price'
        );


      const prices =
        {};


      for (
        const row
        of rows
      ) {

        prices[
          row.symbol
        ] =
          n(
            row.price
          );
      }


      for (
        const symbol
        of Object.keys(
          positions
        )
      ) {

        const price =
          prices[
            symbol
          ];


        if (
          price
        ) {

          await closePaperTrade(

            symbol,

            price,

            'MANUAL_EMERGENCY_CLOSE'
          );
        }
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

    res
      .type(
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
LOMY V6.1
</title>

<style>

body {
  font-family: Arial;
  background: #111;
  color: #eee;
  margin: 20px;
}

.grid {
  display: grid;
  grid-template-columns:
    repeat(
      auto-fit,
      minmax(
        170px,
        1fr
      )
    );
  gap: 10px;
}

.card {
  background: #1c1c1c;
  padding: 14px;
  border-radius: 10px;
}

.v {
  font-size: 24px;
  font-weight: 700;
}

.muted {
  color: #aaa;
}

button {
  padding: 10px 14px;
  margin: 4px;
  border: 0;
  border-radius: 8px;
  cursor: pointer;
}

pre {
  white-space: pre-wrap;
  background: #181818;
  padding: 12px;
  border-radius: 10px;
  overflow: auto;
}

</style>

</head>

<body>

<h2>
LOMY V6.1 — 15M + Cycle20
</h2>

<div class="muted">
PAPER ONLY
</div>

<div
  class="grid"
  id="cards"
></div>

<p>

<button
  onclick="post('/api/scan-now')"
>
Fresh Scan
</button>

<button
  onclick="post('/api/pause')"
>
Pause
</button>

<button
  onclick="post('/api/resume')"
>
Resume
</button>

<button
  onclick="post('/api/emergency-close')"
>
Close All
</button>

</p>

<pre id="detail"></pre>

<script>

async function post(url) {

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

  const data =
    await response.json();


  const values = [

    [
      'Equity',
      '$' +
      data.equity
    ],

    [
      'Cash',
      '$' +
      data.cash
    ],

    [
      'Cycle',
      data.cycle.id
    ],

    [
      'Cycle Entries',
      data.cycle.entries +
      '/20'
    ],

    [
      'Cycle State',
      data.cycle.state
    ],

    [
      'Open',
      Object.keys(
        data.positions
      ).length
    ],

    [
      'Closed',
      data.stats.totalTrades
    ],

    [
      'Win Rate',
      data.winRate +
      '%'
    ],

    [
      'Net P/L',
      '$' +
      data.stats.netProfit.toFixed(
        2
      )
    ],

    [
      'PF',
      data.profitFactor
    ],

    [
      'Fresh Candidates',
      data.cycle.lastScanCandidates
    ]
  ];


  document
    .getElementById(
      'cards'
    )
    .innerHTML =

      values
        .map(
          item =>

            '<div class="card">' +

            '<div class="muted">' +
            item[0] +
            '</div>' +

            '<div class="v">' +
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
        data.positions,
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
    ''
  );

  console.log(
    '========================================'
  );

  console.log(
    `LOMY ${C.version}`
  );

  console.log(
    '15M ENTRY ENGINE'
  );

  console.log(
    '1H MARKET CONTEXT'
  );

  console.log(
    'MOMENTUM + RETEST'
  );

  console.log(
    '20 TRADES PER CYCLE MAX'
  );

  console.log(
    'FRESH RESEARCH EVERY SCAN'
  );

  console.log(
    'NO OLD CANDIDATE STORAGE'
  );

  console.log(
    'CAPITAL PROTECTION + DYNAMIC TRAIL'
  );

  console.log(
    'PAPER TRADING ONLY'
  );

  console.log(
    '========================================'
  );


  // ==========================================================
  // DATABASE
  // ==========================================================

  await connectCloud();


  // ==========================================================
  // RESTORE THIS VERSION STATE ONLY
  // ==========================================================

  await loadState();


  // ==========================================================
  // SERVER
  // ==========================================================

  app.listen(

    PORT,

    () => {

      console.log(
        `Dashboard listening on port ${PORT}`
      );
    }
  );


  // ==========================================================
  // FIX RECOVERED EMPTY CYCLE
  // ==========================================================

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
      'RECOVERED_EMPTY_WAITING_CYCLE'
    );
  }


  // ==========================================================
  // BOOT JOURNAL
  // ==========================================================

  await cloudJournal({

    type:
      'BOOT',

    config: {

      entryInterval:
        C.entryInterval,

      contextInterval:
        C.contextInterval,

      maxEntriesPerCycle:
        C.maxEntriesPerCycle,

      maxPositions:
        C.maxPositions,

      emaFast:
        C.emaFast,

      emaSlow:
        C.emaSlow,

      cmoBuyMin:
        C.cmoBuyMin,

      volumeMultiplier:
        C.momentumVolumeMultiplier,

      minBodyRatio:
        C.minBodyRatio,

      breakEvenTriggerPct:
        C.breakEvenTriggerPct,

      trailingStartPct:
        C.trailingStartPct
    }
  });


  // ==========================================================
  // INITIAL FRESH SCAN
  // ==========================================================

  runFreshScan();


  // ==========================================================
  // CONTINUOUS FRESH SCANS
  // ==========================================================

  setInterval(

    runFreshScan,

    C.scanEveryMs
  );


  // ==========================================================
  // POSITION MANAGEMENT
  // ==========================================================

  setInterval(

    managePositions,

    C.pricePollMs
  );


  // ==========================================================
  // CLOUD SAVE
  // ==========================================================

  setInterval(

    saveState,

    C.stateSaveMs
  );
}


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
    `Shutdown ${signal}`
  );


  await saveState();


  if (
    mongoClient
  ) {

    await mongoClient
      .close()
      .catch(
        () => {}
      );
  }


  process.exit(
    0
  );
}


// ============================================================
// PROCESS HANDLERS
// ============================================================

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
      'uncaughtException',
      error
    );
  }
);


process.on(

  'unhandledRejection',

  error => {

    console.error(
      'unhandledRejection',
      error
    );
  }
);


// ============================================================
// START
// ============================================================

main()
  .catch(
    error => {

      console.error(
        'MAIN:',
        error
      );

      process.exit(
        1
      );
    }
  );
