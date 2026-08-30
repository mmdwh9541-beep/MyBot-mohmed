require('dotenv').config();

const express = require('express');
const axios = require('axios');
const WebSocket = require('ws');
const crypto = require('crypto');
const { MongoClient } = require('mongodb');

const app = express();
app.use(express.json());

const PORT = Number(
  process.env.PORT || 5000
);

const TELEGRAM_TOKEN =
  process.env.TELEGRAM_TOKEN || '';

const CHAT_ID =
  process.env.TELEGRAM_CHAT_ID || '';

const MONGODB_URI =
  process.env.MONGODB_URI || '';

const MONGODB_DB =
  process.env.MONGODB_DB || 'lomy';

const REST_BASE =
  'https://data-api.binance.vision';

const WS_BASE =
  'wss://data-stream.binance.vision';


// ============================================================
// LOMY V6 CONFIG
// ============================================================

const C = Object.freeze({

  version:
    '6.0.0-EARLY-CONFIRM-TRAIL',

  stateKey:
    'main-v600',

  paperTrading:
    true,

  startingBalance:
    10000,


  // ==========================================================
  // MULTI TIMEFRAME MARKET ENGINE
  // ==========================================================

  timeframes:
    [
      '5m',
      '15m',
      '1h'
    ],

  entryTf:
    '5m',

  structureTf:
    '15m',

  trendTf:
    '1h',

  warmupCandles:
    80,

  maxCandles:
    140,

  liveBinanceCandlesRequired:
    10,

  flowBinanceCandlesRequired:
    4,

  universeSize:
    220,

  minQuoteVolume:
    500000,

  universeRefreshMs:
    30 * 60 * 1000,


  // ==========================================================
  // ENTRY CAPACITY
  // لا يوجد Target إجباري للصفقات
  // ==========================================================

  maxPositions:
    6,

  maxEntriesPerCycle:
    2,

  maxDailyEntries:
    30,

  candidateExpiryMs:
    70 * 1000,

  maxPriceDriftPct:
    0.18,


  // ==========================================================
  // EARLY ENTRY + ANTI CHASE
  // ==========================================================

  preBreakoutDistanceAtr:
    0.35,

  maxControlledBreakoutAtr:
    0.85,

  retestTolerancePct:
    0.20,

  maxEmaDistancePct:
    0.95,

  maxExt5Pct:
    1.30,

  maxExt10Pct:
    2.20,

  hardChaseEmaPct:
    1.45,

  hardChaseExt5Pct:
    2.10,


  // ==========================================================
  // MOMENTUM
  // ==========================================================

  cmoMin:
    46,

  cmoIdealMin:
    50,

  cmoIdealMax:
    64,

  cmoHot:
    68,


  // ==========================================================
  // ORDER FLOW
  // ==========================================================

  minTakerBuyRatio:
    0.58,

  eliteTakerBuyRatio:
    0.80,

  minFlowMomentum:
    0.54,


  // ==========================================================
  // VOLUME
  // ==========================================================

  minVolumeRatio:
    0.95,

  maxHealthyVolumeRatio:
    2.45,

  volumeClimaxRatio:
    2.90,

  volumeAccelMin:
    0.80,

  volumeAccelClimax:
    2.35,


  // ==========================================================
  // ATR / VOLATILITY
  // ==========================================================

  minAtrPct:
    0.16,

  atrSweetMin:
    0.22,

  atrSweetMax:
    0.50,

  maxAtrPct:
    0.80,


  // ==========================================================
  // QUALITY
  //
  // Score أصبح Ranking وليس وحده بوابة دخول.
  // ==========================================================

  baseMinQuality:
    64,

  neutralMinQuality:
    66,

  defensiveMinQuality:
    73,

  asiaQualityPenalty:
    3,


  // ==========================================================
  // PAPER COSTS
  // ==========================================================

  feePct:
    0.001,

  slippagePct:
    0.0005,


  // ==========================================================
  // RISK BASED POSITION SIZING
  // ==========================================================

  riskPerTradePct:
    0.0045,

  maxPositionAllocationPct:
    0.22,


  // ==========================================================
  // EMERGENCY STOP
  //
  // ليس Fixed Stop تقليدي.
  // مبني على ATR + Structure.
  // ==========================================================

  minEmergencyStopPct:
    0.0065,

  maxEmergencyStopPct:
    0.018,

  atrEmergencyMultiplier:
    1.65,

  structureStopBufferAtr:
    0.20,


  // ==========================================================
  // CAPITAL PROTECTION
  // ==========================================================

  breakEvenTriggerPct:
    0.50,

  extraBreakEvenBufferPct:
    0.0002,


  // ==========================================================
  // DYNAMIC TRAILING
  // ==========================================================

  trailingStartPct:
    0.70,

  trailAtrMultiplier:
    0.90,

  trailGapMinPct:
    0.22,

  trailGapMaxPct:
    0.55,


  // ==========================================================
  // EARLY FAILURE
  // ==========================================================

  earlyFailureWindowMs:
    12 * 60 * 1000,

  earlyFailureLossPct:
    0.0040,

  earlyFailureMaxMfePct:
    0.18,


  // ==========================================================
  // ACCOUNT PROTECTION
  //
  // مفيش توقف ساعة بعد 3 خسائر.
  // مفيش Batch cooldown.
  // ==========================================================

  dailyLossLimitPct:
    0.035,

  maxAccountDrawdownPct:
    0.07,

  symbolLossCooldownMs:
    45 * 60 * 1000,


  // ==========================================================
  // ADAPTIVE PROTECTION
  // ==========================================================

  lossRiskStep1:
    2,

  lossRiskStep2:
    4,

  lossRiskMultiplier1:
    0.75,

  lossRiskMultiplier2:
    0.55,

  lossQualityPenalty1:
    4,

  lossQualityPenalty2:
    8,


  // ==========================================================
  // REGIME
  // ==========================================================

  btcSymbol:
    'BTCUSDT',

  overextendedBreadth:
    82,


  // ==========================================================
  // SYSTEM
  // ==========================================================

  warmupConcurrency:
    3,

  warmupDelayMs:
    500,

  wsReconnectBaseMs:
    5000,

  wsReconnectMaxMs:
    60000,

  controlChunkSize:
    80,

  stateSaveMs:
    60000,

  regimeRefreshMs:
    60000,

  rankRefreshMs:
    3000,

  analysisJournalSampleRate:
    0.02
});


const BUILD_HASH =
  crypto
    .createHash('sha256')
    .update(
      JSON.stringify(C)
    )
    .digest('hex')
    .slice(
      0,
      12
    );


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
  ) =>
    Number.isFinite(
      Number(value)
    )
      ? Number(value)
      : fallback;


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


function sessionUTC() {

  const h =
    new Date()
      .getUTCHours();

  if (
    h <
    7
  ) {

    return 'ASIA';
  }

  if (
    h <
    13
  ) {

    return 'LONDON';
  }

  if (
    h <
    16
  ) {

    return 'LONDON_NY';
  }

  if (
    h <
    21
  ) {

    return 'NEW_YORK';
  }

  return 'LATE_US';
}


function makeTradeId(
  symbol
) {

  return (
    `${symbol}-${Date.now()}-${Math.random()
      .toString(36)
      .slice(
        2,
        8
      )}`
  );
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

  bestTrade:
    0,

  worstTrade:
    0,

  maxDrawdown:
    0,

  earlyFailureExits:
    0,

  breakEvenMoves:
    0,

  trailingActivations:
    0
};


let dailyPnL =
  0;


let dailyStartEquity =
  C.startingBalance;


let dailyEntries =
  0;


let currentDay =
  utcDay();


let peakEquity =
  C.startingBalance;


let manualPause =
  false;


let dailyPause =
  false;


let drawdownPause =
  false;


let lossStreak =
  0;


const lastLossBySymbol =
  {};


// ============================================================
// MARKET STATE
// ============================================================

const tickers =
  new Map();


const candles =
  Object.fromEntries(

    C.timeframes.map(
      tf =>
        [
          tf,
          {}
        ]
    )
  );


const warmupLoaded =
  Object.fromEntries(

    C.timeframes.map(
      tf =>
        [
          tf,
          new Set()
        ]
    )
  );


const warmupLoading =
  new Set();


let warmupQueue =
  [];


let warmupWorkers =
  0;


const subscribed =
  new Set();


const candidatePool =
  new Map();


const lastAnalyzed =
  {};


let latest =
  [];


let marketRegime = {

  ready:
    false,

  regime:
    'WARMING',

  breadth:
    0,

  btcBullish:
    false,

  btcScore:
    0,

  overextended:
    false,

  updatedAt:
    0
};


let binanceRestBlockedUntil =
  0;


// ============================================================
// WEBSOCKET STATE
// ============================================================

let miniWs =
  null;


let klineWs =
  null;


let miniConnected =
  false;


let klineConnected =
  false;


let miniReconnectAttempts =
  0;


let klineReconnectAttempts =
  0;


let shuttingDown =
  false;


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


    tgBusy =
      true;


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


    } catch (
      error
    ) {

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

      tgBusy =
        false;
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
        tradeId:
          1,

        time:
          1
      })

  ]);


  console.log(
    'MongoDB CLOUD CONNECTED'
  );
}


// ============================================================
// JOURNAL
// ============================================================

async function cloudJournal(
  row,
  force = false
) {

  if (
    !cloudConnected
  ) {

    return;
  }


  if (
    !force &&
    row.type ===
      'ANALYSIS' &&
    Math.random() >
      C.analysisJournalSampleRate
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

        buildHash:
          BUILD_HASH,

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

        buildHash:
          BUILD_HASH,

        ...record
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

            buildHash:
              BUILD_HASH,

            updatedAt:
              Date.now(),

            cash,

            positions,

            stats,

            dailyPnL,

            dailyStartEquity,

            dailyEntries,

            currentDay,

            peakEquity,

            manualPause,

            dailyPause,

            drawdownPause,

            lossStreak,

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
// LOAD STATE
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
      'Fresh V6 PAPER account'
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


  dailyEntries =
    n(
      state.dailyEntries
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


  drawdownPause =
    !!state.drawdownPause;


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


// ============================================================
// DATA SOURCE HELPERS
// ============================================================

function isBinanceCandle(
  candle
) {

  return (
    candle?.source ===
      'BINANCE_LIVE' ||
    candle?.source ===
      'BINANCE_REST'
  );
}


function tfArr(
  tf,
  symbol
) {

  return (
    candles[
      tf
    ]?.[
      symbol
    ] ||
    []
  );
}


function totalBinanceCount(
  symbol
) {

  return tfArr(
    C.entryTf,
    symbol
  )
    .filter(
      isBinanceCandle
    )
    .length;
}


function symbolLiveReady(
  symbol
) {

  return (
    totalBinanceCount(
      symbol
    ) >=
    C.liveBinanceCandlesRequired
  );
}


// ============================================================
// MERGE CANDLES
// ============================================================

function mergeCandles(
  tf,
  symbol,
  incoming
) {

  const map =
    new Map(

      tfArr(
        tf,
        symbol
      )
        .map(
          candle =>
            [
              candle.closeTime,
              candle
            ]
        )
    );


  for (
    const candle
    of incoming ||
    []
  ) {

    const old =
      map.get(
        candle.closeTime
      );


    if (
      old &&
      isBinanceCandle(
        old
      ) &&
      !isBinanceCandle(
        candle
      )
    ) {

      continue;
    }


    map.set(
      candle.closeTime,
      candle
    );
  }


  candles[
    tf
  ][
    symbol
  ] =
    [...map.values()]
      .sort(
        (
          a,
          b
        ) =>
          a.closeTime -
          b.closeTime
      )
      .slice(
        -C.maxCandles
      );
}


// ============================================================
// BINANCE KLINE PARSER
// ============================================================

function parseBinanceKline(
  row,
  source =
    'BINANCE_REST'
) {

  return {

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
      ),

    source
  };
}


// ============================================================
// INTERVAL HELPERS
// ============================================================

function intervalMilliseconds(
  tf
) {

  const match =
    String(
      tf
    ).match(
      /^(\d+)(m|h)$/i
    );


  if (
    !match
  ) {

    return (
      5 *
      60 *
      1000
    );
  }


  const value =
    Number(
      match[1]
    );


  if (
    match[2]
      .toLowerCase() ===
    'h'
  ) {

    return (
      value *
      60 *
      60 *
      1000
    );
  }


  return (
    value *
    60 *
    1000
  );
}


function bybitInterval(
  tf
) {

  const match =
    String(
      tf
    ).match(
      /^(\d+)(m|h)$/i
    );


  if (
    !match
  ) {

    return '5';
  }


  const value =
    Number(
      match[1]
    );


  return (
    match[2]
      .toLowerCase() ===
    'h'
  )
    ? String(
        value *
        60
      )
    : String(
        value
      );
}


// ============================================================
// EXTERNAL OHLC WARMUP
// ============================================================

async function fetchExternal(
  symbol,
  tf
) {

  const intervalMs =
    intervalMilliseconds(
      tf
    );


  try {

    const response =
      await axios.get(

        'https://api.bybit.com/v5/market/kline',

        {

          params: {

            category:
              'spot',

            symbol,

            interval:
              bybitInterval(
                tf
              ),

            limit:
              Math.min(
                C.maxCandles,
                200
              )
          },

          timeout:
            12000
        }
      );


    if (
      n(
        response.data?.retCode,
        -1
      ) !==
        0 ||
      !Array.isArray(
        response.data
          ?.result
          ?.list
      )
    ) {

      throw new Error(
        'BYBIT_INVALID'
      );
    }


    const now =
      Date.now();


    const rows =
      response.data
        .result
        .list

        .map(
          row => {

            const openTime =
              n(
                row[0]
              );


            return {

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

              trades:
                0,

              takerBuyBase:
                0,

              takerBuyQuote:
                0,

              source:
                'BYBIT_OHLC'
            };
          }
        )

        .filter(
          candle =>
            candle.closeTime <
              now &&
            candle.close >
              0
        )

        .sort(
          (
            a,
            b
          ) =>
            a.closeTime -
            b.closeTime
        );


    if (
      rows.length <
      C.warmupCandles
    ) {

      throw new Error(
        'BYBIT_INCOMPLETE'
      );
    }


    return rows;


  } catch (
    bybitError
  ) {

    const instId =
      symbol.endsWith(
        'USDT'
      )
        ? `${
            symbol.slice(
              0,
              -4
            )
          }-USDT`
        : symbol;


    const okxBar =
      tf.endsWith(
        'h'
      )
        ? tf.toUpperCase()
        : tf;


    const response =
      await axios.get(

        'https://www.okx.com/api/v5/market/candles',

        {

          params: {

            instId,

            bar:
              okxBar,

            limit:
              Math.min(
                C.maxCandles,
                100
              )
          },

          timeout:
            12000
        }
      );


    if (
      String(
        response.data?.code
      ) !==
        '0' ||
      !Array.isArray(
        response.data?.data
      )
    ) {

      throw bybitError;
    }


    const now =
      Date.now();


    const rows =
      response.data
        .data

        .map(
          row => {

            const openTime =
              n(
                row[0]
              );


            return {

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

              trades:
                0,

              takerBuyBase:
                0,

              takerBuyQuote:
                0,

              source:
                'OKX_OHLC'
            };
          }
        )

        .filter(
          candle =>
            candle.closeTime <
              now &&
            candle.close >
              0
        )

        .sort(
          (
            a,
            b
          ) =>
            a.closeTime -
            b.closeTime
        );


    if (
      rows.length <
      C.warmupCandles
    ) {

      throw new Error(
        'OKX_INCOMPLETE'
      );
    }


    return rows;
  }
}


// ============================================================
// WARMUP
// ============================================================

async function fetchWarmup(
  symbol,
  tf
) {

  const key =
    `${tf}:${symbol}`;


  if (
    warmupLoading.has(
      key
    ) ||
    warmupLoaded[
      tf
    ].has(
      symbol
    ) ||
    !subscribed.has(
      symbol
    )
  ) {

    return;
  }


  warmupLoading.add(
    key
  );


  try {

    let rows =
      null;


    let source =
      '';


    // --------------------------------------------------------
    // 1. BINANCE
    // --------------------------------------------------------

    if (
      Date.now() >=
      binanceRestBlockedUntil
    ) {

      try {

        const response =
          await axios.get(

            `${REST_BASE}/api/v3/klines`,

            {

              params: {

                symbol,

                interval:
                  tf,

                limit:
                  C.maxCandles
              },

              timeout:
                12000
            }
          );


        if (
          !Array.isArray(
            response.data
          )
        ) {

          throw new Error(
            'BINANCE_INVALID'
          );
        }


        rows =
          response.data

            .map(
              row =>
                parseBinanceKline(
                  row,
                  'BINANCE_REST'
                )
            )

            .filter(
              candle =>
                candle.closeTime <
                Date.now()
            );


        source =
          'BINANCE_REST';


      } catch (
        error
      ) {

        if (
          [
            418,
            429
          ].includes(
            error.response?.status
          )
        ) {

          binanceRestBlockedUntil =
            Date.now() +
            (
              error.response.status ===
                418
                ? 30
                : 5
            ) *
            60 *
            1000;
        }
      }
    }


    // --------------------------------------------------------
    // 2. BYBIT / OKX
    // --------------------------------------------------------

    if (
      !rows
    ) {

      rows =
        await fetchExternal(
          symbol,
          tf
        );


      source =
        rows[
          0
        ]?.source ||
        'EXTERNAL';
    }


    mergeCandles(
      tf,
      symbol,
      rows
    );


    if (
      tfArr(
        tf,
        symbol
      ).length >=
      C.warmupCandles
    ) {

      warmupLoaded[
        tf
      ].add(
        symbol
      );


      console.log(
        `WARMUP ${tf} READY ${symbol} | ${source}`
      );
    }


  } catch (
    error
  ) {

    console.warn(
      `Warmup ${tf} ${symbol}: ${error.message}`
    );


  } finally {

    warmupLoading.delete(
      key
    );
  }
}


// ============================================================
// WARMUP QUEUE
// ============================================================

function queueWarmup(
  symbol
) {

  for (
    const tf
    of C.timeframes
  ) {

    const key =
      `${tf}:${symbol}`;


    if (
      !warmupLoaded[
        tf
      ].has(
        symbol
      ) &&
      !warmupLoading.has(
        key
      ) &&
      !warmupQueue.some(
        item =>
          item.key ===
          key
      )
    ) {

      warmupQueue.push({

        key,

        symbol,

        tf
      });
    }
  }


  processWarmupQueue();
}


function processWarmupQueue() {

  while (
    warmupWorkers <
      C.warmupConcurrency &&
    warmupQueue.length
  ) {

    const job =
      warmupQueue.shift();


    warmupWorkers++;


    (
      async () => {

        try {

          await fetchWarmup(
            job.symbol,
            job.tf
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
// EMA
// ============================================================

function ema(
  arr,
  period
) {

  if (
    !arr ||
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
          candle
        ) =>
          sum +
          candle.close,
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
        arr[
          i
        ].close -
        value
      ) *
      multiplier +
      value;
  }


  return value;
}


function emaAt(
  arr,
  period,
  trim =
    0
) {

  return ema(

    trim
      ? arr.slice(
          0,
          -trim
        )
      : arr,

    period
  );
}


// ============================================================
// CMO
// ============================================================

function cmo(
  arr,
  period =
    9
) {

  if (
    !arr ||
    arr.length <
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
      arr.length -
      period;

    i <
      arr.length;

    i++
  ) {

    const diff =
      arr[
        i
      ].close -
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


// ============================================================
// ATR
// ============================================================

function atr(
  arr,
  period =
    14
) {

  if (
    !arr ||
    arr.length <
    period +
    1
  ) {

    return null;
  }


  const ranges =
    [];


  for (
    let i =
      1;

    i <
      arr.length;

    i++
  ) {

    ranges.push(

      Math.max(

        arr[
          i
        ].high -
        arr[
          i
        ].low,

        Math.abs(
          arr[
            i
          ].high -
          arr[
            i -
            1
          ].close
        ),

        Math.abs(
          arr[
            i
          ].low -
          arr[
            i -
            1
          ].close
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


// ============================================================
// STRUCTURE
// ============================================================

function structure(
  arr
) {

  if (
    !arr ||
    arr.length <
    5
  ) {

    return 'NEUTRAL';
  }


  const current =
    arr.at(
      -1
    );


  const previous =
    arr.at(
      -2
    );


  const third =
    arr.at(
      -3
    );


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
// BINANCE VOLUME
// ============================================================

function volumeRatio(
  binanceArr,
  period =
    12
) {

  if (
    binanceArr.length <
    period +
    1
  ) {

    return null;
  }


  const current =
    binanceArr.at(
      -1
    ).volume;


  const average =
    binanceArr
      .slice(
        -(
          period +
          1
        ),
        -1
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
    period;


  return average >
    0
      ? current /
        average
      : null;
}


function volumeAcceleration(
  binanceArr
) {

  if (
    binanceArr.length <
    7
  ) {

    return null;
  }


  const recent =
    binanceArr
      .slice(
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


  const previous =
    binanceArr
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
      : null;
}


// ============================================================
// TAKER BUY RATIO
// ============================================================

function takerBuyRatio(
  candle
) {

  if (
    !candle ||
    !isBinanceCandle(
      candle
    ) ||
    candle.volume <=
      0
  ) {

    return null;
  }


  return clamp(

    candle.takerBuyBase /
    candle.volume,

    0,
    1
  );
}


// ============================================================
// ORDER FLOW PERSISTENCE
// ============================================================

function orderFlowMomentum(
  binanceArr
) {

  if (
    binanceArr.length <
    C.flowBinanceCandlesRequired
  ) {

    return null;
  }


  const recent =
    binanceArr.slice(
      -C.flowBinanceCandlesRequired
    );


  let buy =
    0;


  let total =
    0;


  for (
    const candle
    of recent
  ) {

    if (
      !isBinanceCandle(
        candle
      ) ||
      candle.volume <=
        0
    ) {

      return null;
    }


    buy +=
      candle.takerBuyBase;


    total +=
      candle.volume;
  }


  return total >
    0
      ? clamp(
          buy /
          total,
          0,
          1
        )
      : null;
}


// ============================================================
// CANDLE SHAPE
// ============================================================

function candleShape(
  candle
) {

  const range =
    Math.max(

      candle.high -
      candle.low,

      Number.EPSILON
    );


  const bodyRatio =
    Math.abs(
      candle.close -
      candle.open
    ) /
    range;


  const upperWickRatio =
    (
      candle.high -
      Math.max(
        candle.open,
        candle.close
      )
    ) /
    range;


  const closeLocation =
    (
      candle.close -
      candle.low
    ) /
    range;


  return {

    bodyRatio,

    upperWickRatio,

    closeLocation,

    range
  };
}


// ============================================================
// SWING LEVELS
// ============================================================

function swingResistance(
  arr,
  lookback =
    12
) {

  /*
   * Exclude current AND previous candle.
   * This lets us detect a genuine previous breakout + current retest.
   */

  const sample =
    arr.slice(
      -(
        lookback +
        2
      ),
      -2
    );


  return sample.length
    ? Math.max(
        ...sample.map(
          candle =>
            candle.high
        )
      )
    : null;
}


function recentSwingLow(
  arr,
  lookback =
    8
) {

  const sample =
    arr.slice(
      -lookback
    );


  return sample.length
    ? Math.min(
        ...sample.map(
          candle =>
            candle.low
        )
      )
    : null;
}


// ============================================================
// TIMEFRAME STATE
// ============================================================

function timeframeState(
  arr
) {

  if (
    !arr ||
    arr.length <
    C.warmupCandles
  ) {

    return null;
  }


  const close =
    arr.at(
      -1
    ).close;


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


  const e20Previous =
    emaAt(
      arr,
      20,
      3
    );


  if (
    !e20 ||
    !e50 ||
    !e20Previous
  ) {

    return null;
  }


  const slopePct =
    pct(
      e20 -
      e20Previous,
      e20Previous
    ) /
    3;


  return {

    close,

    e20,

    e50,

    slopePct,

    structure:
      structure(
        arr
      ),

    bullish:
      (
        close >
          e20 &&
        e20 >
          e50 &&
        slopePct >
          0
      ),

    bearish:
      (
        close <
          e20 &&
        e20 <
          e50 &&
        slopePct <
          0
      )
  };
}


// ============================================================
// MARKET REGIME
// ============================================================

function buildRegime() {

  let ready =
    0;


  let bullish =
    0;


  for (
    const symbol
    of subscribed
  ) {

    const state =
      timeframeState(

        tfArr(
          C.trendTf,
          symbol
        )
      );


    if (
      !state
    ) {

      continue;
    }


    ready++;


    if (
      state.bullish
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
    timeframeState(

      tfArr(
        C.trendTf,
        C.btcSymbol
      )
    );


  const regime =
    !ready
      ? 'WARMING'
      : breadth >=
          58
        ? 'RISK_ON'
        : breadth >=
            38
          ? 'NEUTRAL'
          : 'DEFENSIVE';


  marketRegime = {

    ready:
      ready >
      20,

    regime,

    breadth:
      +breadth.toFixed(
        2
      ),

    btcBullish:
      !!btc?.bullish,

    btcScore:
      btc?.bullish
        ? 100
        : btc?.bearish
          ? 0
          : 50,

    overextended:
      breadth >=
      C.overextendedBreadth,

    updatedAt:
      Date.now()
  };
}


// ============================================================
// ADAPTIVE RISK
// ============================================================

function adaptiveRisk() {

  if (
    lossStreak >=
    C.lossRiskStep2
  ) {

    return {

      riskMultiplier:
        C.lossRiskMultiplier2,

      qualityPenalty:
        C.lossQualityPenalty2,

      label:
        'CAUTION_2'
    };
  }


  if (
    lossStreak >=
    C.lossRiskStep1
  ) {

    return {

      riskMultiplier:
        C.lossRiskMultiplier1,

      qualityPenalty:
        C.lossQualityPenalty1,

      label:
        'CAUTION_1'
    };
  }


  return {

    riskMultiplier:
      1,

    qualityPenalty:
      0,

    label:
      'NORMAL'
  };
}


// ============================================================
// V6 ENTRY ANALYSIS
// ============================================================

function analyze(
  symbol
) {

  const arr5 =
    tfArr(
      C.entryTf,
      symbol
    );


  const arr15 =
    tfArr(
      C.structureTf,
      symbol
    );


  const arr1h =
    tfArr(
      C.trendTf,
      symbol
    );


  if (
    arr5.length <
      C.warmupCandles ||
    arr15.length <
      C.warmupCandles ||
    arr1h.length <
      C.warmupCandles
  ) {

    return null;
  }


  const liveReady =
    symbolLiveReady(
      symbol
    );


  const binanceArr =
    arr5.filter(
      isBinanceCandle
    );


  const current =
    arr5.at(
      -1
    );


  const previous =
    arr5.at(
      -2
    );


  const state15 =
    timeframeState(
      arr15
    );


  const state1h =
    timeframeState(
      arr1h
    );


  const state5 =
    timeframeState(
      arr5
    );


  if (
    !state15 ||
    !state1h ||
    !state5
  ) {

    return null;
  }


  const atrValue =
    atr(
      arr5,
      14
    );


  const momentum =
    cmo(
      arr5,
      9
    );


  const volRatio =
    volumeRatio(
      binanceArr
    );


  const volAcceleration =
    volumeAcceleration(
      binanceArr
    );


  const taker =
    takerBuyRatio(
      binanceArr.at(
        -1
      )
    );


  const flow =
    orderFlowMomentum(
      binanceArr
    );


  const shape =
    candleShape(
      current
    );


  const resistance =
    swingResistance(
      arr5,
      12
    );


  const swingLow =
    recentSwingLow(
      arr5,
      8
    );


  if (
    !atrValue ||
    momentum ===
      null ||
    !resistance
  ) {

    return null;
  }


  const atrPct =
    pct(
      atrValue,
      current.close
    );


  const emaDistance =
    pct(

      current.close -
      state5.e20,

      state5.e20
    );


  const ext5 =
    pct(

      current.close -
      arr5.at(
        -6
      ).close,

      arr5.at(
        -6
      ).close
    );


  const ext10 =
    pct(

      current.close -
      arr5.at(
        -11
      ).close,

      arr5.at(
        -11
      ).close
    );


  const breakoutAtr =
    (
      current.close -
      resistance
    ) /
    atrValue;


  const preBreakoutAtr =
    (
      resistance -
      current.close
    ) /
    atrValue;


  // ==========================================================
  // ENTRY TIMING
  // ==========================================================

  const previousBroke =
    previous.close >
    resistance;


  const retest =
    (
      previousBroke &&

      current.low <=
        resistance *
        (
          1 +
          C.retestTolerancePct /
          100
        ) &&

      current.close >
        resistance &&

      shape.closeLocation >=
        0.62
    );


  const controlledBreakout =
    (
      current.close >
        resistance &&

      breakoutAtr >=
        0 &&

      breakoutAtr <=
        C.maxControlledBreakoutAtr &&

      shape.bodyRatio <=
        0.88
    );


  /*
   * Early Build:
   *
   * السعر لم ينفجر بعد.
   * قريب من المقاومة.
   * شمعة إيجابية.
   * نسمح بالدخول مبكرًا إذا باقي السياق قوي.
   */

  const earlyBuild =
    (
      current.close <=
        resistance &&

      preBreakoutAtr >=
        0 &&

      preBreakoutAtr <=
        C.preBreakoutDistanceAtr &&

      current.close >
        current.open &&

      shape.closeLocation >=
        0.65
    );


  const triggerType =
    retest
      ? 'RETEST_CONTINUATION'
      : controlledBreakout
        ? 'CONTROLLED_BREAKOUT'
        : earlyBuild
          ? 'EARLY_BUILD'
          : 'NONE';


  // ==========================================================
  // FLOW PRICE RESPONSE
  // ==========================================================

  const currentReturnPct =
    pct(

      current.close -
      current.open,

      current.open
    );


  const priceProgress3 =
    pct(

      current.close -
      arr5.at(
        -4
      ).close,

      arr5.at(
        -4
      ).close
    );


  const flowEfficiency =
    flow !==
      null
      ? (
          priceProgress3 /
          Math.max(
            (
              flow -
              0.5
            ) *
            2,
            0.08
          )
        )
      : 0;


  // ==========================================================
  // ABSORPTION DETECTOR
  //
  // Aggressive buying without price response = danger.
  // ==========================================================

  const absorption =
    (
      taker !==
        null &&

      taker >=
        C.eliteTakerBuyRatio &&

      (
        (
          currentReturnPct <
            0.03 &&
          shape.closeLocation <
            0.72
        ) ||

        shape.upperWickRatio >
          0.34 ||

        priceProgress3 <
          0.08
      )
    );


  // ==========================================================
  // CLIMAX / EXHAUSTION
  // ==========================================================

  const climax =
    (
      (
        volRatio !==
          null &&
        volRatio >=
          C.volumeClimaxRatio
      ) ||

      (
        volAcceleration !==
          null &&

        volAcceleration >=
          C.volumeAccelClimax &&

        shape.bodyRatio >=
          0.82
      )
    );


  /*
   * أهم فلتر جديد لعلاج دخول البوت بعد نهاية الحركة.
   */

  const lateCandle =
    (
      shape.bodyRatio >=
        0.88 &&

      shape.closeLocation >=
        0.90 &&

      (
        emaDistance >
          0.75 ||
        ext5 >
          1.00
      ) &&

      (
        volAcceleration ||
        0
      ) >
        1.45
    );


  const hardChase =
    (
      emaDistance >
        C.hardChaseEmaPct ||

      ext5 >
        C.hardChaseExt5Pct
    );


  // ==========================================================
  // QUALITY ENGINE
  // ==========================================================

  let quality =
    0;


  const reasons =
    [];


  const warnings =
    [];


  const hardBlocks =
    [];


  // ----------------------------------------------------------
  // 1H
  // ----------------------------------------------------------

  if (
    state1h.bullish
  ) {

    quality +=
      18;


    reasons.push(
      '1H_TREND'
    );


  } else if (
    state1h.bearish
  ) {

    hardBlocks.push(
      '1H_BEARISH'
    );


  } else {

    quality +=
      7;


    warnings.push(
      '1H_NEUTRAL'
    );
  }


  // ----------------------------------------------------------
  // 15M
  // ----------------------------------------------------------

  if (
    state15.bullish ||
    state15.structure ===
      'BULLISH'
  ) {

    quality +=
      14;


    reasons.push(
      '15M_STRUCTURE'
    );


  } else {

    quality +=
      5;


    warnings.push(
      '15M_NOT_STRONG'
    );
  }


  // ----------------------------------------------------------
  // 5M EMA
  // ----------------------------------------------------------

  if (
    state5.slopePct >
    0
  ) {

    quality +=
      7;


    reasons.push(
      '5M_EMA_SLOPE'
    );
  }


  // ----------------------------------------------------------
  // TRIGGER
  // ----------------------------------------------------------

  if (
    triggerType ===
    'RETEST_CONTINUATION'
  ) {

    quality +=
      18;


    reasons.push(
      triggerType
    );


  } else if (
    triggerType ===
    'CONTROLLED_BREAKOUT'
  ) {

    quality +=
      14;


    reasons.push(
      triggerType
    );


  } else if (
    triggerType ===
    'EARLY_BUILD'
  ) {

    quality +=
      12;


    reasons.push(
      triggerType
    );


  } else {

    hardBlocks.push(
      'NO_EARLY_OR_RETEST_TRIGGER'
    );
  }


  // ----------------------------------------------------------
  // CMO
  // ----------------------------------------------------------

  if (
    momentum >=
      C.cmoIdealMin &&
    momentum <=
      C.cmoIdealMax
  ) {

    quality +=
      10;


    reasons.push(
      'CMO_IDEAL'
    );


  } else if (
    momentum >=
      C.cmoMin &&
    momentum <
      C.cmoHot
  ) {

    quality +=
      5;


    warnings.push(
      'CMO_EDGE'
    );


  } else if (
    momentum >=
    C.cmoHot
  ) {

    quality -=
      8;


    warnings.push(
      'CMO_HOT'
    );


  } else {

    quality -=
      6;


    warnings.push(
      'CMO_WEAK'
    );
  }


  // ----------------------------------------------------------
  // ATR
  // ----------------------------------------------------------

  if (
    atrPct >=
      C.atrSweetMin &&
    atrPct <=
      C.atrSweetMax
  ) {

    quality +=
      8;


    reasons.push(
      'ATR_SWEET'
    );


  } else if (
    atrPct >=
      C.minAtrPct &&
    atrPct <=
      C.maxAtrPct
  ) {

    quality +=
      3;


    warnings.push(
      'ATR_EDGE'
    );


  } else {

    hardBlocks.push(
      'ATR_INVALID'
    );
  }


  // ----------------------------------------------------------
  // VOLUME
  // ----------------------------------------------------------

  if (
    volRatio !==
      null &&

    volRatio >=
      C.minVolumeRatio &&

    volRatio <=
      C.maxHealthyVolumeRatio
  ) {

    quality +=
      7;


    reasons.push(
      'HEALTHY_VOLUME'
    );


  } else if (
    volRatio !==
      null &&

    volRatio <
      C.minVolumeRatio
  ) {

    quality -=
      4;


    warnings.push(
      'VOLUME_LIGHT'
    );
  }


  if (
    volAcceleration !==
      null &&

    volAcceleration >=
      C.volumeAccelMin &&

    volAcceleration <
      C.volumeAccelClimax
  ) {

    quality +=
      5;


    reasons.push(
      'VOLUME_ACCEL_OK'
    );
  }


  // ----------------------------------------------------------
  // TAKER BUY
  // ----------------------------------------------------------

  if (
    taker !==
      null &&

    taker >=
      C.eliteTakerBuyRatio
  ) {

    quality +=
      9;


    reasons.push(
      'ELITE_TAKER_FLOW'
    );


  } else if (
    taker !==
      null &&

    taker >=
      C.minTakerBuyRatio
  ) {

    quality +=
      5;


    reasons.push(
      'BUY_FLOW'
    );


  } else {

    quality -=
      5;


    warnings.push(
      'TAKER_WEAK'
    );
  }


  // ----------------------------------------------------------
  // FLOW PERSISTENCE
  // ----------------------------------------------------------

  if (
    flow !==
      null &&

    flow >=
      0.70
  ) {

    quality +=
      8;


    reasons.push(
      'FLOW_PERSISTENT'
    );


  } else if (
    flow !==
      null &&

    flow >=
      C.minFlowMomentum
  ) {

    quality +=
      4;


    reasons.push(
      'FLOW_OK'
    );


  } else {

    quality -=
      5;


    warnings.push(
      'FLOW_WEAK'
    );
  }


  // ----------------------------------------------------------
  // ANTI CHASE
  // ----------------------------------------------------------

  if (
    emaDistance <=
      C.maxEmaDistancePct &&

    ext5 <=
      C.maxExt5Pct &&

    ext10 <=
      C.maxExt10Pct
  ) {

    quality +=
      6;


    reasons.push(
      'NOT_CHASING'
    );


  } else {

    quality -=
      8;


    warnings.push(
      'EXTENDED'
    );
  }


  // ----------------------------------------------------------
  // FLOW PRICE RESPONSE
  // ----------------------------------------------------------

  if (
    flowEfficiency >
    0.18
  ) {

    quality +=
      4;


    reasons.push(
      'FLOW_PRICE_RESPONSE'
    );
  }


  // ----------------------------------------------------------
  // REGIME
  // ----------------------------------------------------------

  if (
    marketRegime.btcBullish
  ) {

    quality +=
      3;
  }


  if (
    marketRegime.regime ===
    'RISK_ON'
  ) {

    quality +=
      3;
  }


  if (
    marketRegime.overextended
  ) {

    quality -=
      5;


    warnings.push(
      'BREADTH_OVEREXTENDED'
    );
  }


  if (
    sessionUTC() ===
    'ASIA'
  ) {

    quality -=
      C.asiaQualityPenalty;


    warnings.push(
      'ASIA'
    );
  }


  // ==========================================================
  // HARD BLOCKS
  // ==========================================================

  if (
    !liveReady
  ) {

    hardBlocks.push(

      `BINANCE_DATA_${totalBinanceCount(
        symbol
      )}/${C.liveBinanceCandlesRequired}`
    );
  }


  if (
    absorption
  ) {

    hardBlocks.push(
      'ABSORPTION'
    );
  }


  if (
    climax
  ) {

    hardBlocks.push(
      'VOLUME_CLIMAX'
    );
  }


  if (
    lateCandle
  ) {

    hardBlocks.push(
      'LATE_EXHAUSTION_CANDLE'
    );
  }


  if (
    hardChase
  ) {

    hardBlocks.push(
      'HARD_CHASE'
    );
  }


  if (
    current.close <=
    current.open
  ) {

    hardBlocks.push(
      'NO_BULLISH_TRIGGER'
    );
  }


  quality =
    clamp(
      Math.round(
        quality
      ),
      0,
      100
    );


  // ==========================================================
  // DYNAMIC THRESHOLD
  // ==========================================================

  const adaptive =
    adaptiveRisk();


  let minQuality =

    marketRegime.regime ===
      'DEFENSIVE'
      ? C.defensiveMinQuality

      : marketRegime.regime ===
          'NEUTRAL'
        ? C.neutralMinQuality

        : C.baseMinQuality;


  minQuality +=
    adaptive.qualityPenalty;


  const eligible =
    (
      hardBlocks.length ===
        0 &&

      quality >=
        minQuality
    );


  return {

    symbol,

    eligible,

    quality,

    minQuality,

    grade:
      quality >=
        82
        ? 'A+'

        : quality >=
            74
          ? 'A'

          : quality >=
              66
            ? 'B'

            : 'C',

    triggerType,

    signalPrice:
      current.close,

    signalTime:
      current.closeTime,

    liveDataReady:
      liveReady,

    binanceCandles:
      totalBinanceCount(
        symbol
      ),

    cmo:
      momentum,

    atrPct,

    volumeRatio:
      volRatio,

    volumeAcceleration:
      volAcceleration,

    takerBuyRatio:
      taker,

    orderFlowMomentum:
      flow,

    flowEfficiency,

    emaDistance,

    ext5,

    ext10,

    bodyRatio:
      shape.bodyRatio,

    upperWickRatio:
      shape.upperWickRatio,

    closeLocation:
      shape.closeLocation,

    resistance,

    swingLow,

    trend1h:
      state1h,

    structure15m:
      state15,

    trend5m:
      state5,

    regime:
      marketRegime.regime,

    breadth:
      marketRegime.breadth,

    btcBullish:
      marketRegime.btcBullish,

    absorption,

    climax,

    lateCandle,

    reasons,

    warnings,

    hardBlocks,

    adaptiveRisk:
      adaptive
  };
}


// ============================================================
// ACCOUNT EQUITY
// ============================================================

function equity() {

  let total =
    cash;


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
      tickers.get(
        symbol
      )?.price ||
      position.entryPrice;


    total +=
      position.qty *
      price *
      (
        1 -
        C.feePct
      );
  }


  return total;
}


// ============================================================
// DAILY RESET
// ============================================================

function resetDailyIfNeeded() {

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


  dailyPnL =
    0;


  dailyEntries =
    0;


  dailyStartEquity =
    equity();


  dailyPause =
    false;


  console.log(
    'DAILY RESET'
  );


  saveCloudState();
}


// ============================================================
// DRAWDOWN
// ============================================================

function updateDrawdown() {

  const current =
    equity();


  peakEquity =
    Math.max(
      peakEquity,
      current
    );


  const drawdown =
    peakEquity
      ? pct(
          peakEquity -
          current,
          peakEquity
        )
      : 0;


  stats.maxDrawdown =
    Math.max(
      stats.maxDrawdown,
      drawdown
    );


  if (
    drawdown >=
    C.maxAccountDrawdownPct *
    100
  ) {

    drawdownPause =
      true;


    tg(
      `🛑 <b>V6 MAX DRAWDOWN GUARD</b>
DD: ${drawdown.toFixed(2)}%`
    );
  }
}


// ============================================================
// DAILY LOSS GUARD
// ============================================================

function checkDailyLoss() {

  resetDailyIfNeeded();


  const limit =
    dailyStartEquity *
    C.dailyLossLimitPct;


  if (
    dailyPnL <=
    -limit
  ) {

    dailyPause =
      true;


    tg(
      `🛑 <b>V6 DAILY LOSS GUARD</b>
PnL: $${dailyPnL.toFixed(2)}`
    );
  }
}


// ============================================================
// CAN TRADE
// ============================================================

function canTrade() {

  resetDailyIfNeeded();


  return (
    !manualPause &&
    !dailyPause &&
    !drawdownPause &&
    dailyEntries <
      C.maxDailyEntries
  );
}


// ============================================================
// SYMBOL COOLDOWN
// ============================================================

function symbolCooling(
  symbol
) {

  return (
    Date.now() -
    n(
      lastLossBySymbol[
        symbol
      ]
    )
  ) <
  C.symbolLossCooldownMs;
}


// ============================================================
// DYNAMIC EMERGENCY STOP
// ============================================================

function emergencyStopPct(
  analysis
) {

  return clamp(

    (
      analysis.atrPct /
      100
    ) *
    C.atrEmergencyMultiplier,

    C.minEmergencyStopPct,

    C.maxEmergencyStopPct
  );
}


// ============================================================
// RISK BASED ALLOCATION
// ============================================================

function allocationFor(
  analysis,
  stopPct
) {

  const accountEquity =
    equity();


  const adaptive =
    adaptiveRisk();


  const riskBudget =
    accountEquity *
    C.riskPerTradePct *
    adaptive.riskMultiplier;


  const byRisk =
    riskBudget /
    Math.max(
      stopPct,
      0.0001
    );


  const maximumAllocation =
    accountEquity *
    C.maxPositionAllocationPct;


  return Math.min(

    byRisk,

    maximumAllocation,

    cash *
    0.985
  );
}


// ============================================================
// TRUE BREAK EVEN
// ============================================================

function realBreakEvenPrice(
  entryPrice
) {

  /*
   * حماية رأس المال بعد الرسوم والانزلاق.
   */

  const roundTripCost =
    C.feePct *
      2 +

    C.slippagePct *
      2 +

    C.extraBreakEvenBufferPct;


  return (
    entryPrice *
    (
      1 +
      roundTripCost
    )
  );
}


// ============================================================
// CANDIDATE
// ============================================================

function addCandidate(
  analysis
) {

  candidatePool.set(

    analysis.symbol,

    {

      ...analysis,

      addedAt:
        Date.now()
    }
  );
}


function candidateStillValid(
  candidate
) {

  if (
    Date.now() -
      candidate.addedAt >
    C.candidateExpiryMs
  ) {

    return false;
  }


  const price =
    tickers.get(
      candidate.symbol
    )?.price;


  if (
    !price
  ) {

    return false;
  }


  return (
    Math.abs(

      pct(

        price -
        candidate.signalPrice,

        candidate.signalPrice
      )
    ) <=
    C.maxPriceDriftPct
  );
}


// ============================================================
// ENTRY REVALIDATION
// ============================================================

function revalidateCandidate(
  candidate
) {

  /*
   * أهم فرق عن النسخ القديمة:
   *
   * قبل التنفيذ نعيد تحليل الفرصة مرة ثانية.
   * لا نعتمد على Candidate قديم.
   */

  const fresh =
    analyze(
      candidate.symbol
    );


  if (
    !fresh?.eligible
  ) {

    return null;
  }


  const price =
    tickers.get(
      candidate.symbol
    )?.price;


  if (
    !price
  ) {

    return null;
  }


  if (
    Math.abs(

      pct(

        price -
        fresh.signalPrice,

        fresh.signalPrice
      )
    ) >
    C.maxPriceDriftPct
  ) {

    return null;
  }


  return fresh;
}


// ============================================================
// OPEN PAPER
// ============================================================

function openPaper(
  candidate
) {

  if (
    !C.paperTrading ||
    !canTrade() ||
    positions[
      candidate.symbol
    ] ||
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


  const analysis =
    revalidateCandidate(
      candidate
    );


  if (
    !analysis
  ) {

    return false;
  }


  const marketPrice =
    tickers.get(
      analysis.symbol
    )?.price ||
    analysis.signalPrice;


  const entryPrice =
    marketPrice *
    (
      1 +
      C.slippagePct
    );


  // ==========================================================
  // EMERGENCY STRUCTURAL STOP
  // ==========================================================

  let stopPct =
    emergencyStopPct(
      analysis
    );


  let emergencyStop =
    entryPrice *
    (
      1 -
      stopPct
    );


  if (
    analysis.swingLow &&
    analysis.swingLow <
      entryPrice
  ) {

    const structureStop =

      analysis.swingLow *

      (
        1 -
        (
          analysis.atrPct /
          100
        ) *
        C.structureStopBufferAtr
      );


    const structureRisk =
      (
        entryPrice -
        structureStop
      ) /
      entryPrice;


    if (
      structureRisk >=
        C.minEmergencyStopPct &&

      structureRisk <=
        C.maxEmergencyStopPct
    ) {

      stopPct =
        structureRisk;


      emergencyStop =
        structureStop;
    }
  }


  const invested =
    allocationFor(
      analysis,
      stopPct
    );


  if (
    invested <
    10
  ) {

    return false;
  }


  const buyFee =
    invested *
    C.feePct;


  const netInvestment =
    invested -
    buyFee;


  const qty =
    netInvestment /
    entryPrice;


  cash -=
    invested;


  dailyEntries++;


  const id =
    makeTradeId(
      analysis.symbol
    );


  positions[
    analysis.symbol
  ] = {

    tradeId:
      id,

    symbol:
      analysis.symbol,

    entryPrice,

    qty,

    investedUSDT:
      invested,

    buyFee,

    entryTime:
      Date.now(),

    signalPrice:
      analysis.signalPrice,

    signalTime:
      analysis.signalTime,

    quality:
      analysis.quality,

    grade:
      analysis.grade,

    triggerType:
      analysis.triggerType,

    emergencyStop,

    initialRiskPct:
      stopPct *
      100,

    protectionStop:
      null,

    breakEvenActive:
      false,

    trailingActive:
      false,

    highestPrice:
      entryPrice,

    mfePct:
      0,

    maePct:
      0,

    snapshot:
      analysis
  };


  cloudJournal({

    type:
      'ENTRY',

    tradeId:
      id,

    symbol:
      analysis.symbol,

    entryPrice,

    investedUSDT:
      invested,

    initialRiskPct:
      stopPct *
      100,

    emergencyStop,

    quality:
      analysis.quality,

    grade:
      analysis.grade,

    triggerType:
      analysis.triggerType,

    snapshot:
      analysis

  }, true);


  tg(
    `🟢 <b>LOMY V6 PAPER ENTRY</b>
<b>${analysis.symbol}</b>
Trigger: ${analysis.triggerType}
Quality: ${analysis.quality}/${analysis.minQuality}
Risk state: ${analysis.adaptiveRisk.label}`
  );


  saveCloudState();


  return true;
}


// ============================================================
// CLOSE PAPER
// ============================================================

async function closePaper(
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


  const gross =
    position.qty *
    exitPrice;


  const sellFee =
    gross *
    C.feePct;


  const net =
    gross -
    sellFee;


  const profit =
    net -
    position.investedUSDT;


  const profitPct =
    pct(
      profit,
      position.investedUSDT
    );


  cash +=
    net;


  dailyPnL +=
    profit;


  stats.totalTrades++;


  stats.fees +=
    position.buyFee +
    sellFee;


  stats.netProfit +=
    profit;


  if (
    profit >
    0
  ) {

    stats.wins++;


    stats.grossProfit +=
      profit;


    stats.bestTrade =
      Math.max(
        stats.bestTrade,
        profit
      );


    /*
     * الصفقة الناجحة تعيد Adaptive Risk للوضع الطبيعي.
     */

    lossStreak =
      0;


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

    type:
      'EXIT',

    ...record

  }, true);


  await saveCloudState();


  tg(
    `${
      profit >=
      0
        ? '✅'
        : '❌'
    } <b>LOMY V6 EXIT</b>
<b>${symbol}</b>
${reason}
PnL: $${profit.toFixed(2)} (${profitPct.toFixed(2)}%)
MFE: ${position.mfePct.toFixed(2)}% | MAE: ${position.maePct.toFixed(2)}%`
  );
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
    position.closing ||
    !price
  ) {

    return;
  }


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


  // ==========================================================
  // PHASE 1
  // EMERGENCY PROTECTION
  // ==========================================================

  if (
    price <=
      position.emergencyStop &&
    !position.breakEvenActive
  ) {

    closePaper(
      symbol,
      price,
      'EMERGENCY_STOP'
    );


    return;
  }


  // ==========================================================
  // PHASE 2
  // +0.50% = CAPITAL PROTECTION
  // ==========================================================

  if (
    !position.breakEvenActive &&
    position.mfePct >=
      C.breakEvenTriggerPct
  ) {

    position.breakEvenActive =
      true;


    position.protectionStop =
      realBreakEvenPrice(
        position.entryPrice
      );


    stats.breakEvenMoves++;


    cloudJournal({

      type:
        'CAPITAL_PROTECTION',

      tradeId:
        position.tradeId,

      symbol,

      protectionStop:
        position.protectionStop,

      mfePct:
        position.mfePct

    }, true);


    tg(
      `🛡️ <b>${symbol} CAPITAL PROTECTED</b>
MFE: ${position.mfePct.toFixed(2)}%`
    );
  }


  // ==========================================================
  // PHASE 3
  // DYNAMIC TRAILING
  // ==========================================================

  if (
    position.breakEvenActive &&
    position.mfePct >=
      C.trailingStartPct
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

        tradeId:
          position.tradeId,

        symbol,

        mfePct:
          position.mfePct

      }, true);
    }


    const atrPctNow =
      n(
        position.snapshot?.atrPct,
        0.30
      );


    let gapPct =
      clamp(

        atrPctNow *
        C.trailAtrMultiplier,

        C.trailGapMinPct,

        C.trailGapMaxPct
      );


    /*
     * كلما تكبر الأرباح،
     * نضيّق الـTrail تدريجيًا.
     */

    if (
      position.mfePct >=
      2.00
    ) {

      gapPct *=
        0.65;


    } else if (
      position.mfePct >=
      1.25
    ) {

      gapPct *=
        0.78;
    }


    const priceTrail =

      position.highestPrice *

      (
        1 -
        gapPct /
        100
      );


    /*
     * Structure Trail
     */

    const structureLow =
      recentSwingLow(

        tfArr(
          C.entryTf,
          symbol
        ),

        5
      );


    const structureTrail =

      (
        structureLow &&
        structureLow >
          position.entryPrice
      )

        ? structureLow

        : null;


    const nextStop =
      Math.max(

        position.protectionStop ||
        0,

        priceTrail,

        structureTrail ||
        0
      );


    if (
      nextStop >
      (
        position.protectionStop ||
        0
      )
    ) {

      position.protectionStop =
        nextStop;
    }
  }


  // ==========================================================
  // DYNAMIC EXIT
  // ==========================================================

  if (
    position.breakEvenActive &&
    position.protectionStop &&
    price <=
      position.protectionStop
  ) {

    closePaper(

      symbol,

      price,

      position.trailingActive
        ? 'DYNAMIC_TRAIL'
        : 'CAPITAL_PROTECTION_STOP'
    );


    return;
  }


  // ==========================================================
  // EARLY FAILED SETUP
  // ==========================================================

  const age =
    Date.now() -
    position.entryTime;


  const failedLevel =
    (
      position.snapshot
        ?.resistance &&

      price <
      position.snapshot
        .resistance
    );


  if (
    age <=
      C.earlyFailureWindowMs &&

    movePct <=
      -(
        C.earlyFailureLossPct *
        100
      ) &&

    position.mfePct <=
      C.earlyFailureMaxMfePct &&

    failedLevel
  ) {

    closePaper(

      symbol,

      price,

      'EARLY_FAILURE'
    );
  }
}


// ============================================================
// EXECUTE BEST CANDIDATES
// ============================================================

function executeCandidates() {

  if (
    !canTrade()
  ) {

    return;
  }


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

      .filter(
        candidateStillValid
      )

      .sort(
        (
          a,
          b
        ) =>
          b.quality -
          a.quality
      );


  candidatePool.clear();


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
// CLOSED KLINE
// ============================================================

function onClosedKline(
  symbol,
  tf,
  kline
) {

  const candle = {

    open:
      n(
        kline.o
      ),

    high:
      n(
        kline.h
      ),

    low:
      n(
        kline.l
      ),

    close:
      n(
        kline.c
      ),

    volume:
      n(
        kline.v
      ),

    closeTime:
      n(
        kline.T
      ),

    quoteVolume:
      n(
        kline.q
      ),

    trades:
      n(
        kline.n
      ),

    takerBuyBase:
      n(
        kline.V
      ),

    takerBuyQuote:
      n(
        kline.Q
      ),

    source:
      'BINANCE_LIVE'
  };


  mergeCandles(

    tf,

    symbol,

    [
      candle
    ]
  );


  if (
    tfArr(
      tf,
      symbol
    ).length >=
    C.warmupCandles
  ) {

    warmupLoaded[
      tf
    ].add(
      symbol
    );


  } else {

    queueWarmup(
      symbol
    );
  }


  /*
   * الاستراتيجية تدخل فقط عند إغلاق 5m.
   * 15m / 1h تحدث Context فقط.
   */

  if (
    tf !==
    C.entryTf
  ) {

    return;
  }


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
      symbol
    );


  if (
    !analysis
  ) {

    return;
  }


  cloudJournal({

    type:
      'ANALYSIS',

    symbol,

    decision:
      analysis.eligible
        ? 'ELIGIBLE'
        : analysis.liveDataReady
          ? 'REJECT'
          : 'LIVE_WARMUP',

    quality:
      analysis.quality,

    minQuality:
      analysis.minQuality,

    triggerType:
      analysis.triggerType,

    cmo:
      analysis.cmo,

    atrPct:
      analysis.atrPct,

    volumeRatio:
      analysis.volumeRatio,

    volumeAcceleration:
      analysis.volumeAcceleration,

    takerBuyRatio:
      analysis.takerBuyRatio,

    orderFlowMomentum:
      analysis.orderFlowMomentum,

    flowEfficiency:
      analysis.flowEfficiency,

    absorption:
      analysis.absorption,

    lateCandle:
      analysis.lateCandle,

    hardBlocks:
      analysis.hardBlocks,

    warnings:
      analysis.warnings,

    regime:
      analysis.regime,

    breadth:
      analysis.breadth
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
// WEBSOCKET HELPERS
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


  ws.send(
    JSON.stringify(
      object
    )
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
    let i =
      0;

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


// ============================================================
// MINI TICKER
// ============================================================

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


      } catch (
        error
      ) {

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


      const delay =
        Math.min(

          C.wsReconnectMaxMs,

          C.wsReconnectBaseMs *
          (
            2 **
            Math.min(
              miniReconnectAttempts++,
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


// ============================================================
// MULTI TIMEFRAME KLINE WS
// ============================================================

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
        'KLINE MULTI-TF LIVE'
      );


      const params =
        [];


      for (
        const symbol
        of subscribed
      ) {

        for (
          const tf
          of C.timeframes
        ) {

          params.push(

            `${
              symbol.toLowerCase()
            }@kline_${tf}`
          );
        }
      }


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
          200
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
          payload?.k?.x &&
          payload.s
        ) {

          onClosedKline(

            payload.s,

            payload.k.i,

            payload.k
          );
        }


      } catch (
        error
      ) {

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


      const delay =
        Math.min(

          C.wsReconnectMaxMs,

          C.wsReconnectBaseMs *
          (
            2 **
            Math.min(
              klineReconnectAttempts++,
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

  let ranked =
    [];


  // ==========================================================
  // BINANCE REST
  // ==========================================================

  if (
    Date.now() >=
    binanceRestBlockedUntil
  ) {

    try {

      const response =
        await axios.get(

          `${REST_BASE}/api/v3/ticker/24hr`,

          {

            timeout:
              15000
          }
        );


      ranked =
        (
          Array.isArray(
            response.data
          )
            ? response.data
            : []
        )

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


    } catch (
      error
    ) {

      if (
        [
          418,
          429
        ].includes(
          error.response?.status
        )
      ) {

        binanceRestBlockedUntil =
          Date.now() +
          (
            error.response.status ===
              418
              ? 30
              : 5
          ) *
          60 *
          1000;
      }
    }
  }


  // ==========================================================
  // MINI WS FALLBACK
  // ==========================================================

  if (
    !ranked.length
  ) {

    ranked =
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
            !IGNORED.has(
              symbol
            ) &&
            n(
              ticker.quoteVolume
            ) >=
              C.minQuoteVolume
        )

        .sort(
          (
            a,
            b
          ) =>
            b[
              1
            ].quoteVolume -
            a[
              1
            ].quoteVolume
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
  }


  if (
    !ranked.includes(
      C.btcSymbol
    )
  ) {

    ranked.unshift(
      C.btcSymbol
    );
  }


  const changed =
    (
      ranked.length !==
        subscribed.size ||

      ranked.some(
        symbol =>
          !subscribed.has(
            symbol
          )
      )
    );


  subscribed.clear();


  for (
    const symbol
    of ranked
  ) {

    subscribed.add(
      symbol
    );
  }


  for (
    const symbol
    of subscribed
  ) {

    queueWarmup(
      symbol
    );
  }


  /*
   * Universe changed:
   * reconnect Kline WS with new subscriptions.
   */

  if (
    changed &&
    klineWs?.readyState ===
      WebSocket.OPEN
  ) {

    try {

      klineWs.terminate();

    } catch {}
  }


  latest =
    ranked;


  console.log(
    `UNIVERSE ${ranked.length}`
  );
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

    const currentEquity =
      equity();


    const adaptive =
      adaptiveRisk();


    res.json({

      version:
        C.version,

      buildHash:
        BUILD_HASH,

      paperTrading:
        C.paperTrading,

      cloudConnected,

      miniConnected,

      klineConnected,

      marketRegime,

      cash,

      equity:
        currentEquity,

      dailyPnL,

      dailyEntries,

      maxDailyEntries:
        C.maxDailyEntries,

      lossStreak,

      adaptiveRisk:
        adaptive,

      manualPause,

      dailyPause,

      drawdownPause,

      positions,

      stats,

      subscribed:
        subscribed.size,

      warmup: {

        '5m':
          warmupLoaded[
            '5m'
          ].size,

        '15m':
          warmupLoaded[
            '15m'
          ].size,

        '1h':
          warmupLoaded[
            '1h'
          ].size
      },

      binanceReady:
        [...subscribed]
          .filter(
            symbolLiveReady
          )
          .length,

      candidates:
        [...candidatePool.values()]
          .sort(
            (
              a,
              b
            ) =>
              b.quality -
              a.quality
          )
          .slice(
            0,
            20
          )
    });
  }
);


// ============================================================
// PAUSE
// ============================================================

app.post(
  '/api/pause',
  (
    req,
    res
  ) => {

    manualPause =
      true;


    saveCloudState();


    res.json({
      ok:
        true
    });
  }
);


// ============================================================
// RESUME
// ============================================================

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
      ok:
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
        ].entryPrice;


      await closePaper(

        symbol,

        price,

        'MANUAL_EMERGENCY'
      );
    }


    res.json({
      ok:
        true
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

    res.send(
`<!doctype html>

<html>

<head>

<meta charset="utf-8">

<meta name="viewport"
content="width=device-width,initial-scale=1">

<title>LOMY V6</title>

<style>

body {
  font-family: Arial;
  background: #0d1117;
  color: #e6edf3;
  margin: 20px;
}

.card {
  background: #161b22;
  padding: 14px;
  border-radius: 12px;
  margin: 8px 0;
}

pre {
  white-space: pre-wrap;
}

</style>

</head>

<body>

<h2>
LOMY V6 — Early Confirm + Dynamic Trail
</h2>

<div id="dashboard">
Loading...
</div>

<script>

async function updateDashboard() {

  const data =
    await fetch('/api/data')
      .then(
        response =>
          response.json()
      );


  document.getElementById(
    'dashboard'
  ).innerHTML =

  '<div class="card">' +

  'Version ' +
  data.version +

  ' | Build ' +
  data.buildHash +

  ' | ' +
  (
    data.paperTrading
      ? 'PAPER'
      : 'LIVE'
  ) +

  '</div>' +


  '<div class="card">' +

  'Equity $' +
  data.equity.toFixed(2) +

  ' | Daily P/L $' +
  data.dailyPnL.toFixed(2) +

  ' | Entries ' +
  data.dailyEntries +
  '/' +
  data.maxDailyEntries +

  ' | Loss streak ' +
  data.lossStreak +

  ' | Risk ' +
  data.adaptiveRisk.label +

  '</div>' +


  '<div class="card">' +

  'Regime ' +
  data.marketRegime.regime +

  ' | Breadth ' +
  data.marketRegime.breadth +
  '%' +

  ' | BTC ' +
  (
    data.marketRegime.btcBullish
      ? 'Bullish'
      : 'Not bullish'
  ) +

  '</div>' +


  '<div class="card">' +

  'WS ' +
  data.klineConnected +

  ' | Binance ready ' +
  data.binanceReady +
  '/' +
  data.subscribed +

  ' | Warmup 5m ' +
  data.warmup['5m'] +

  ' | 15m ' +
  data.warmup['15m'] +

  ' | 1h ' +
  data.warmup['1h'] +

  '</div>' +


  '<pre class="card">' +

  JSON.stringify(
    data.positions,
    null,
    2
  ) +

  '</pre>';
}


updateDashboard();


setInterval(
  updateDashboard,
  3000
);

</script>

</body>

</html>`
    );
  }
);


// ============================================================
// START
// ============================================================

async function start() {

  try {

    await connectCloud();


    await loadCloudState();


    resetDailyIfNeeded();


    connectMiniWs();


    /*
     * Give MINI ticker time to populate
     * if Binance REST is blocked.
     */

    await sleep(
      3500
    );


    await refreshUniverse();


    connectKlineWs();


    buildRegime();


    // --------------------------------------------------------
    // Fast control cycle
    // --------------------------------------------------------

    setInterval(

      () => {

        resetDailyIfNeeded();

        buildRegime();

        executeCandidates();
      },

      C.rankRefreshMs
    );


    // --------------------------------------------------------
    // Market regime
    // --------------------------------------------------------

    setInterval(

      buildRegime,

      C.regimeRefreshMs
    );


    // --------------------------------------------------------
    // Universe refresh
    // --------------------------------------------------------

    setInterval(

      refreshUniverse,

      C.universeRefreshMs
    );


    // --------------------------------------------------------
    // State
    // --------------------------------------------------------

    setInterval(

      saveCloudState,

      C.stateSaveMs
    );


    app.listen(

      PORT,

      () => {

        console.log(

          `LOMY ${C.version} | BUILD ${BUILD_HASH} | PORT ${PORT} | PAPER ONLY`
        );
      }
    );


    tg(
      `🚀 <b>LOMY ${C.version}</b>
Build: ${BUILD_HASH}
PAPER ONLY
Early-confirm + multi-TF + dynamic capital protection`
    );


  } catch (
    error
  ) {

    console.error(
      'START FAILED:',
      error
    );


    process.exit(
      1
    );
  }
}


// ============================================================
// SHUTDOWN
// ============================================================

process.on(
  'SIGTERM',
  async () => {

    shuttingDown =
      true;


    try {

      await saveCloudState();

      await mongoClient
        ?.close();

    } catch {}


    process.exit(
      0
    );
  }
);


process.on(
  'SIGINT',
  async () => {

    shuttingDown =
      true;


    try {

      await saveCloudState();

      await mongoClient
        ?.close();

    } catch {}


    process.exit(
      0
    );
  }
);


start();
