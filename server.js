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


// ============================================================
// EXCHANGES
// ============================================================

const BINANCE_REST =
  'https://data-api.binance.vision';

const BYBIT_REST =
  'https://api.bybit.com';

const OKX_REST =
  'https://www.okx.com';


// ============================================================
// CONFIG
// ============================================================

const C = Object.freeze({

  version:
    '6.1.1-15M-CYCLE20-MULTISOURCE',

  // NEW KEY = NO OLD STATE
  stateKey:
    'main-v611',

  paperTrading:
    true,

  startingBalance:
    10000,

  // ==========================================================
  // MARKET
  // ==========================================================

  universeSize:
    100,

  minQuoteVolume:
    500000,

  scanEveryMs:
    2 * 60 * 1000,

  scanConcurrency:
    4,

  maxEntriesPerScan:
    2,

  // ==========================================================
  // CYCLE
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
  // ENTRY
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
  // RISK
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

  // ==========================================================
  // PROFIT MANAGEMENT
  // ==========================================================

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
  // ACCOUNT GUARDS
  // ==========================================================

  dailyLossLimitPct:
    0.035,

  maxAccountDrawdownPct:
    0.07,

  symbolLossCooldownMs:
    45 * 60 * 1000,

  // ==========================================================
  // EXCHANGE FAILOVER
  // ==========================================================

  binance429PauseMs:
    5 * 60 * 1000,

  binance418PauseMs:
    30 * 60 * 1000,

  exchangeFailurePauseMs:
    60 * 1000,

  requestTimeoutMs:
    12000,

  // ==========================================================
  // RUNTIME
  // ==========================================================

  pricePollMs:
    5000,

  stateSaveMs:
    30000,

  universeRefreshMs:
    10 * 60 * 1000
});


// ============================================================
// STABLECOIN / FIAT PAIRS TO IGNORE
// ============================================================

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


function n(
  value,
  fallback = 0
) {

  const number =
    Number(
      value
    );

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
    0,

  lastUniverseSource:
    null
};


const lastLossBySymbol =
  {};


// ============================================================
// MARKET STATE
// ============================================================

let universe =
  [];

let universeUpdatedAt =
  0;

let scanning =
  false;

let shuttingDown =
  false;


// ============================================================
// EXCHANGE HEALTH
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
  },

  BYBIT: {

    blockedUntil:
      0,

    failures:
      0,

    lastError:
      null,

    lastSuccess:
      0
  },

  OKX: {

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
      0 &&
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
// EXCHANGE HEALTH HELPERS
// ============================================================

function exchangeAvailable(
  name
) {

  return (
    Date.now() >=
    exchangeHealth[
      name
    ].blockedUntil
  );
}


function markExchangeSuccess(
  name
) {

  const health =
    exchangeHealth[
      name
    ];

  health.failures =
    0;

  health.lastError =
    null;

  health.lastSuccess =
    Date.now();
}


function markExchangeFailure(
  name,
  error
) {

  const health =
    exchangeHealth[
      name
    ];


  health.failures++;


  health.lastError =
    error.message ||
    String(
      error
    );


  let pause =
    C.exchangeFailurePauseMs;


  const status =
    error.response?.status;


  if (
    name ===
      'BINANCE' &&
    status ===
      418
  ) {

    pause =
      C.binance418PauseMs;

  } else if (
    name ===
      'BINANCE' &&
    status ===
      429
  ) {

    pause =
      C.binance429PauseMs;
  }


  health.blockedUntil =
    Date.now() +
    pause;


  console.warn(

    `${name} paused ` +

    `${Math.ceil(pause / 60000)}m | ` +

    `${
      status ||
      error.message
    }`
  );
}


// ============================================================
// BINANCE
// ============================================================

async function binanceGet(
  path,
  params = {}
) {

  if (
    !exchangeAvailable(
      'BINANCE'
    )
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


    markExchangeSuccess(
      'BINANCE'
    );


    return response.data;

  } catch (
    error
  ) {

    markExchangeFailure(
      'BINANCE',
      error
    );

    throw error;
  }
}


// ============================================================
// BYBIT
// ============================================================

async function bybitGet(
  path,
  params = {}
) {

  if (
    !exchangeAvailable(
      'BYBIT'
    )
  ) {

    throw new Error(
      'BYBIT_TEMP_BLOCKED'
    );
  }


  try {

    const response =
      await axios.get(

        `${BYBIT_REST}${path}`,

        {
          params,

          timeout:
            C.requestTimeoutMs
        }
      );


    if (
      n(
        response.data?.retCode
      ) !==
      0
    ) {

      throw new Error(

        `BYBIT_${
          response.data?.retCode
        }_${
          response.data?.retMsg
        }`
      );
    }


    markExchangeSuccess(
      'BYBIT'
    );


    return response.data;

  } catch (
    error
  ) {

    markExchangeFailure(
      'BYBIT',
      error
    );

    throw error;
  }
}


// ============================================================
// OKX
// ============================================================

async function okxGet(
  path,
  params = {}
) {

  if (
    !exchangeAvailable(
      'OKX'
    )
  ) {

    throw new Error(
      'OKX_TEMP_BLOCKED'
    );
  }


  try {

    const response =
      await axios.get(

        `${OKX_REST}${path}`,

        {
          params,

          timeout:
            C.requestTimeoutMs
        }
      );


    if (
      String(
        response.data?.code
      ) !==
      '0'
    ) {

      throw new Error(

        `OKX_${
          response.data?.code
        }_${
          response.data?.msg
        }`
      );
    }


    markExchangeSuccess(
      'OKX'
    );


    return response.data;

  } catch (
    error
  ) {

    markExchangeFailure(
      'OKX',
      error
    );

    throw error;
  }
}


// ============================================================
// BINANCE KLINES
// ============================================================

async function fetchBinanceKlines(
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


  return data
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
          'BINANCE'
      })
    )
    .filter(
      candle =>
        candle.closeTime <
        now
    );
}


// ============================================================
// BYBIT KLINES
// ============================================================

function bybitInterval(
  interval
) {

  if (
    interval ===
    '15m'
  ) {

    return '15';
  }


  if (
    interval ===
    '1h'
  ) {

    return '60';
  }


  throw new Error(
    `UNSUPPORTED_BYBIT_INTERVAL_${interval}`
  );
}


async function fetchBybitKlines(
  symbol,
  interval,
  limit
) {

  const data =
    await bybitGet(

      '/v5/market/kline',

      {
        category:
          'spot',

        symbol,

        interval:
          bybitInterval(
            interval
          ),

        limit
      }
    );


  const rows =
    data.result?.list ||
    [];


  const intervalMs =
    interval ===
      '15m'
      ? 15 * 60 * 1000
      : 60 * 60 * 1000;


  const now =
    Date.now();


  return rows
    .map(
      row => {

        const openTime =
          n(
            row[0]
          );


        return {

          openTime,

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

          quoteVolume:
            n(
              row[6]
            ),

          closeTime:
            openTime +
            intervalMs -
            1,

          source:
            'BYBIT'
        };
      }
    )
    .filter(
      candle =>
        candle.closeTime <
        now
    )
    .sort(
      (
        a,
        b
      ) =>
        a.openTime -
        b.openTime
    );
}


// ============================================================
// OKX KLINES
// ============================================================

function okxSymbol(
  symbol
) {

  if (
    symbol.endsWith(
      'USDT'
    )
  ) {

    return (
      symbol.slice(
        0,
        -4
      ) +
      '-USDT'
    );
  }


  return symbol;
}


function okxInterval(
  interval
) {

  if (
    interval ===
    '15m'
  ) {

    return '15m';
  }


  if (
    interval ===
    '1h'
  ) {

    return '1H';
  }


  throw new Error(
    `UNSUPPORTED_OKX_INTERVAL_${interval}`
  );
}


async function fetchOkxKlines(
  symbol,
  interval,
  limit
) {

  const data =
    await okxGet(

      '/api/v5/market/candles',

      {
        instId:
          okxSymbol(
            symbol
          ),

        bar:
          okxInterval(
            interval
          ),

        limit:
          Math.min(
            limit,
            100
          )
      }
    );


  const rows =
    data.data ||
    [];


  const intervalMs =
    interval ===
      '15m'
      ? 15 * 60 * 1000
      : 60 * 60 * 1000;


  const now =
    Date.now();


  return rows
    .map(
      row => {

        const openTime =
          n(
            row[0]
          );


        return {

          openTime,

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

          quoteVolume:
            n(
              row[7] ||
              row[6]
            ),

          closeTime:
            openTime +
            intervalMs -
            1,

          source:
            'OKX'
        };
      }
    )
    .filter(
      candle =>
        candle.closeTime <
        now
    )
    .sort(
      (
        a,
        b
      ) =>
        a.openTime -
        b.openTime
    );
}


// ============================================================
// MULTI SOURCE KLINES
// ============================================================

async function fetchClosedKlines(
  symbol,
  interval,
  limit
) {

  const errors =
    [];


  // 1. BINANCE
  if (
    exchangeAvailable(
      'BINANCE'
    )
  ) {

    try {

      const rows =
        await fetchBinanceKlines(

          symbol,

          interval,

          limit
        );


      if (
        rows.length >=
        30
      ) {

        return {

          source:
            'BINANCE',

          candles:
            rows
        };
      }

    } catch (
      error
    ) {

      errors.push(
        `BINANCE=${error.message}`
      );
    }
  }


  // 2. BYBIT
  if (
    exchangeAvailable(
      'BYBIT'
    )
  ) {

    try {

      const rows =
        await fetchBybitKlines(

          symbol,

          interval,

          limit
        );


      if (
        rows.length >=
        30
      ) {

        return {

          source:
            'BYBIT',

          candles:
            rows
        };
      }

    } catch (
      error
    ) {

      errors.push(
        `BYBIT=${error.message}`
      );
    }
  }


  // 3. OKX
  if (
    exchangeAvailable(
      'OKX'
    )
  ) {

    try {

      const rows =
        await fetchOkxKlines(

          symbol,

          interval,

          limit
        );


      if (
        rows.length >=
        30
      ) {

        return {

          source:
            'OKX',

          candles:
            rows
        };
      }

    } catch (
      error
    ) {

      errors.push(
        `OKX=${error.message}`
      );
    }
  }


  throw new Error(

    `NO_KLINE_SOURCE ${symbol} ${interval} | ` +

    errors.join(
      ' | '
    )
  );
}


// ============================================================
// UNIVERSE - BINANCE
// ============================================================

async function universeFromBinance() {

  const rows =
    await binanceGet(
      '/api/v3/ticker/24hr'
    );


  return rows
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
    .map(
      row => ({

        symbol:
          row.symbol,

        quoteVolume:
          n(
            row.quoteVolume
          )
      })
    );
}


// ============================================================
// UNIVERSE - BYBIT
// ============================================================

async function universeFromBybit() {

  const data =
    await bybitGet(

      '/v5/market/tickers',

      {
        category:
          'spot'
      }
    );


  return (
    data.result?.list ||
    []
  )
    .filter(
      row =>
        row.symbol?.endsWith(
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
            row.turnover24h
          )
      })
    );
}


// ============================================================
// UNIVERSE - OKX
// ============================================================

async function universeFromOkx() {

  const data =
    await okxGet(

      '/api/v5/market/tickers',

      {
        instType:
          'SPOT'
      }
    );


  return (
    data.data ||
    []
  )
    .filter(
      row =>
        row.instId?.endsWith(
          '-USDT'
        )
    )
    .map(
      row => {

        const symbol =
          row.instId.replace(
            '-',
            ''
          );


        return {

          symbol,

          quoteVolume:
            n(
              row.volCcy24h
            )
        };
      }
    )
    .filter(
      row =>
        !IGNORED.has(
          row.symbol
        )
    );
}


// ============================================================
// MULTI SOURCE UNIVERSE
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


  let rows =
    null;

  let source =
    null;


  if (
    exchangeAvailable(
      'BINANCE'
    )
  ) {

    try {

      rows =
        await universeFromBinance();

      source =
        'BINANCE';

    } catch {}
  }


  if (
    !rows &&
    exchangeAvailable(
      'BYBIT'
    )
  ) {

    try {

      rows =
        await universeFromBybit();

      source =
        'BYBIT';

    } catch {}
  }


  if (
    !rows &&
    exchangeAvailable(
      'OKX'
    )
  ) {

    try {

      rows =
        await universeFromOkx();

      source =
        'OKX';

    } catch {}
  }


  if (
    !rows
  ) {

    throw new Error(
      'ALL_UNIVERSE_SOURCES_FAILED'
    );
  }


  universe =
    rows
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


  universeUpdatedAt =
    Date.now();


  cycle.lastUniverseSource =
    source;


  console.log(

    `UNIVERSE ${source} | ` +

    `${universe.length} symbols`
  );


  return universe;
}


// ============================================================
// PRICE - BINANCE
// ============================================================

async function priceFromBinance(
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


  return price;
}


// ============================================================
// PRICE - BYBIT
// ============================================================

async function priceFromBybit(
  symbol
) {

  const data =
    await bybitGet(

      '/v5/market/tickers',

      {
        category:
          'spot',

        symbol
      }
    );


  const price =
    n(
      data.result?.list?.[
        0
      ]?.lastPrice
    );


  if (
    !price
  ) {

    throw new Error(
      'BYBIT_INVALID_PRICE'
    );
  }


  return price;
}


// ============================================================
// PRICE - OKX
// ============================================================

async function priceFromOkx(
  symbol
) {

  const data =
    await okxGet(

      '/api/v5/market/ticker',

      {
        instId:
          okxSymbol(
            symbol
          )
      }
    );


  const price =
    n(
      data.data?.[
        0
      ]?.last
    );


  if (
    !price
  ) {

    throw new Error(
      'OKX_INVALID_PRICE'
    );
  }


  return price;
}


// ============================================================
// MULTI SOURCE PRICE
// ============================================================

async function getCurrentPrice(
  symbol
) {

  const errors =
    [];


  if (
    exchangeAvailable(
      'BINANCE'
    )
  ) {

    try {

      return {

        price:
          await priceFromBinance(
            symbol
          ),

        source:
          'BINANCE'
      };

    } catch (
      error
    ) {

      errors.push(
        `BINANCE=${error.message}`
      );
    }
  }


  if (
    exchangeAvailable(
      'BYBIT'
    )
  ) {

    try {

      return {

        price:
          await priceFromBybit(
            symbol
          ),

        source:
          'BYBIT'
      };

    } catch (
      error
    ) {

      errors.push(
        `BYBIT=${error.message}`
      );
    }
  }


  if (
    exchangeAvailable(
      'OKX'
    )
  ) {

    try {

      return {

        price:
          await priceFromOkx(
            symbol
          ),

        source:
          'OKX'
      };

    } catch (
      error
    ) {

      errors.push(
        `OKX=${error.message}`
      );
    }
  }


  throw new Error(

    `NO_PRICE_SOURCE ${symbol} | ` +

    errors.join(
      ' | '
    )
  );
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
// RETEST BREAKOUT DETECTOR
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
      recent[
        i
      ];


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
  // ULTRA FAST CORE - HARD CONDITIONS
  // ==========================================================

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


  // ==========================================================
  // MOMENTUM ENTRY
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


  const notExtended =
    breakoutDistanceAtr <=
    C.maxMomentumExtensionAtr;


  const momentumVolumeOk =
    volumeRatio >=
    C.momentumVolumeMultiplier;


  const momentumEntry =

    trendOk &&

    cmoOk &&

    candleOk &&

    momentumVolumeOk &&

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
      C.retestMinVolumeRatio;


  const retestEntry =

    trendOk &&

    cmoOk &&

    candleOk &&

    recentBreakout.found &&

    touchedRetest &&

    heldRetest &&

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


  // Ranking only.
  // Ranking NEVER makes an invalid setup valid.

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
// FRESH SYMBOL ANALYSIS
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
    entryResult,
    contextResult
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


  const context =
    analyze1h(
      contextResult.candles
    );


  if (
    !context.ok
  ) {

    return null;
  }


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
        entryResult.source,

      context:
        contextResult.source
    },

    context,

    signal
  };
}


// ============================================================
// LIMITED CONCURRENCY
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

          `SCAN ${items[i]} | ` +

          error.message
        );
      }


      await sleep(
        80
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
// MONGODB
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

    console.log(
      'No MongoDB state. Fresh account.'
    );

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
      'FRESH V6.1.1 PAPER ACCOUNT | $10000 | Cycle 1 | 0/20'
    );

    return;
  }


  // Extra protection:
  // never restore another version.

  if (
    state.version !==
    C.version
  ) {

    console.warn(
      'STATE VERSION MISMATCH - IGNORED'
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
  // FRESH REVALIDATION - OLD RESEARCH NOT ACCEPTED
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


  const allocation =
    positionSize(
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
      fresh
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

    priceSource:
      priceData.source,

    allocation,

    snapshot:
      fresh
  });


  tg(

    `🟢 <b>LOMY PAPER ENTRY</b>\n` +

    `${candidate.symbol}\n` +

    `Type: ${fresh.signal.entryType}\n` +

    `Price source: ${priceData.source}\n` +

    `15m CMO: ${fresh.signal.cmo.toFixed(1)}\n` +

    `Volume: ${fresh.signal.volumeRatio.toFixed(2)}x\n` +

    `Cycle: ${cycle.entries}/20`
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

      `⏳ Cycle ${cycle.id} reached 20 entries.\n` +

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

    `${profit >= 0 ? '✅' : '🔴'} <b>${symbol} CLOSED</b>\n` +

    `${reason}\n` +

    `Source: ${source}\n` +

    `PnL: $${profit.toFixed(2)} (${profitPct.toFixed(2)}%)`
  );


  updateAccountGuards();


  await maybeStartNewCycle();
}


// ============================================================
// POSITION MANAGER
// ============================================================

async function managePositions() {

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


  const prices =
    {};


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


      // ======================================================
      // +0.5% CAPITAL PROTECTION
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
      // +0.7% TRAILING
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
      80
    );
  }


  resetDailyIfNeeded(
    prices
  );


  updateAccountGuards(
    prices
  );
}


// ============================================================
// NEW CYCLE
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
      null
  };


  stats.cycleCount++;


  // Explicitly destroy old research.

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

    `🔄 <b>Cycle ${cycle.id}</b>\n` +

    `Fresh research started.\n` +

    `All previous candidates discarded.`
  );


  setTimeout(
    runFreshScan,
    1000
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
// FRESH SCAN
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


  cycle.lastFreshScanAt =
    Date.now();


  try {

    console.log(
      `FRESH SCAN | Cycle ${cycle.id} | ${cycle.entries}/20`
    );


    // New research every scan.
    // No persistent candidate pool.

    const symbols =
      await refreshUniverse(
        true
      );


    const candidates =
      await mapLimit(

        symbols,

        C.scanConcurrency,

        analyzeSymbolFresh
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

              sources:
                candidate.sources
            })
          )
    });


    console.log(

      `SCAN DONE | ` +

      `${symbols.length} checked | ` +

      `${candidates.length} qualified`
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
// DASHBOARD
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
          exchangeAvailable(
            'BINANCE'
          ),

        blockedForSeconds:
          Math.max(

            0,

            Math.ceil(

              (
                exchangeHealth.BINANCE.blockedUntil -
                Date.now()
              ) /
              1000
            )
          ),

        lastError:
          exchangeHealth.BINANCE.lastError
      },


      BYBIT: {

        available:
          exchangeAvailable(
            'BYBIT'
          ),

        lastError:
          exchangeHealth.BYBIT.lastError
      },


      OKX: {

        available:
          exchangeAvailable(
            'OKX'
          ),

        lastError:
          exchangeHealth.OKX.lastError
      }
    },

    guards: {

      manualPause,

      dailyPause,

      drawdownPause
    },

    config: {

      entryTimeframe:
        '15m',

      contextTimeframe:
        '1h',

      maxEntriesPerCycle:
        20,

      maxPositions:
        C.maxPositions
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
// SIMPLE DASHBOARD
// ============================================================

app.get(

  '/',

  (
    req,
    res
  ) => {

    res.type(
      'html'
    ).send(`

<!doctype html>

<html>

<head>

<meta charset="utf-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1"
>

<title>
LOMY V6.1.1
</title>

<style>

body {
  font-family: Arial;
  background: #111;
  color: #eee;
  margin: 18px;
}

h2 {
  margin-bottom: 4px;
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
  font-size: 23px;
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
LOMY V6.1.1
</h2>

<div>
15M + 1H • Cycle20 • Binance / Bybit / OKX
</div>

<div>
PAPER ONLY
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

  const d =
    await response.json();


  const items = [

    [
      'Equity',
      '$' + d.equity
    ],

    [
      'Cash',
      '$' + d.cash
    ],

    [
      'Cycle',
      d.cycle.id
    ],

    [
      'Entries',
      d.cycle.entries + '/20'
    ],

    [
      'Cycle State',
      d.cycle.state
    ],

    [
      'Open',
      Object.keys(d.positions).length
    ],

    [
      'Closed',
      d.stats.totalTrades
    ],

    [
      'Win Rate',
      d.winRate + '%'
    ],

    [
      'Net P/L',
      '$' + d.stats.netProfit.toFixed(2)
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
      'Universe Source',
      d.cycle.lastUniverseSource || '-'
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
        .join('');


  document
    .getElementById(
      'detail'
    )
    .textContent =

      JSON.stringify(
        {
          exchanges:
            d.exchanges,

          guards:
            d.guards,

          positions:
            d.positions
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

  console.log('');
  console.log('==============================================');
  console.log(`LOMY ${C.version}`);
  console.log('PAPER ONLY');
  console.log('15M ENTRY + 1H CONTEXT');
  console.log('ULTRA FAST CORE');
  console.log('MOMENTUM ENTRY + RETEST ENTRY');
  console.log('20 ENTRIES MAX PER CYCLE');
  console.log('FRESH RESEARCH - NO OLD CANDIDATES');
  console.log('BINANCE -> BYBIT -> OKX FAILOVER');
  console.log('CAPITAL PROTECTION + DYNAMIC TRAIL');
  console.log('==============================================');


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
        '15m',

      contextTimeframe:
        '1h',

      cycleLimit:
        20,

      dataFailover: [
        'BINANCE',
        'BYBIT',
        'OKX'
      ]
    }
  });


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
    `SHUTDOWN ${signal}`
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


process.once(

  'SIGTERM',

  () =>
    shutdown(
      'SIGTERM'
    )
);


process.once(

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
