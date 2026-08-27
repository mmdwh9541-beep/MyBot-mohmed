require('dotenv').config();

const express = require('express');
const axios = require('axios');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const WS_BASE = 'wss://data-stream.binance.vision';

// ============================================================
// LOMY V4.3 PRECISION+
// PAPER TRADING ONLY
// BINANCE WEBSOCKET MARKET DATA
// ============================================================

const C = {
    version: '4.3-PRECISION-PLUS',

    paperTrading: true,

    startingBalance: 10000,

    // --------------------------------------------------------
    // Portfolio
    // --------------------------------------------------------

    maxPositions: 6,

    maxEntriesPerCycle: 2,

    minAllocation: 50,

    // --------------------------------------------------------
    // Execution simulation
    // --------------------------------------------------------

    feePct: 0.001,

    slippagePct: 0.0005,

    stopLossPct: 0.01,

    takeProfitPct: 0.02,

    // --------------------------------------------------------
    // Early failure protection
    // --------------------------------------------------------

    earlyFailureEnabled: true,

    earlyFailureWindowMs:
        20 * 60 * 1000,

    earlyFailureLossPct:
        0.0045,

    earlyFailureBreakoutLossPct:
        0.0025,

    // --------------------------------------------------------
    // Daily protection
    // --------------------------------------------------------

    dailyLossLimitPct: 0.05,

    // --------------------------------------------------------
    // Market
    // --------------------------------------------------------

    interval: '5m',

    warmup: 55,

    maxCandles: 90,

    universeSize: 300,

    minQuoteVolume: 500000,

    universeRefreshMs:
        30 * 60 * 1000,

    // --------------------------------------------------------
    // Candidate freshness
    // --------------------------------------------------------

    candidateExpiryMs:
        6 * 60 * 1000,

    maxPriceDriftPct:
        0.25,

    rankingDelayMs:
        2500,

    // --------------------------------------------------------
    // Quality filters
    // --------------------------------------------------------

    minGradeScore: 76,

    // CMO
    minCMO: 54,
    idealCMOMin: 56,
    idealCMOMax: 72,
    maxCMO: 82,

    // Volume
    hardMinVolume: 1.40,
    idealVolumeMin: 1.75,
    idealVolumeMax: 2.75,
    cautionVolume: 3.20,
    hardMaxVolume: 4.00,

    // EMA20 distance
    minEma20Distance: 0.25,
    idealEmaMin: 0.90,
    idealEmaMax: 1.55,
    maxEma20Distance: 2.10,

    // Breakout
    minBreakoutDistance: 0.03,
    idealBreakoutMin: 0.08,
    idealBreakoutMax: 0.45,
    maxBreakoutDistance: 0.65,

    // Extension
    maxExtension5: 2.20,
    maxExtension10: 4.00,

    // ATR
    minATR: 0.15,
    maxATR: 1.80,

    // Candle body
    minBodyRatio: 0.58,

    // Upper wick
    maxUpperWickRatio: 0.32,

    // Support
    maxSupportDistance: 7.0,

    // --------------------------------------------------------
    // BTC context
    // --------------------------------------------------------

    btcSymbol: 'BTCUSDT',

    requireBTCContext: true,

    maxBTCNegativeMomentum: -0.35,

    // --------------------------------------------------------
    // Cooldown
    // --------------------------------------------------------

    entriesBeforeCooldown: 10,

    normalCooldownMs:
        20 * 60 * 1000,

    lossStreakLimit: 3,

    lossCooldownMs:
        60 * 60 * 1000,

    symbolLossCooldownMs:
        60 * 60 * 1000,

    // --------------------------------------------------------
    // Journal
    // --------------------------------------------------------

    journalLimit: 25000,

    historyLimit: 5000,

    shadowLimit: 5000,

    stateFile:
        path.join(
            __dirname,
            'paper-state-v43.json'
        )
};

// ============================================================
// GLOBAL STATE
// ============================================================

let cash =
    C.startingBalance;

let positions = {};

let history = [];

let journal = [];

let shadowTrades = [];

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

    earlyFailureExits: 0
};

let peakEquity =
    C.startingBalance;

let dailyPnL = 0;

let dailyStartEquity =
    C.startingBalance;

let currentDay =
    getUtcDay();

let manualPause = false;

let dailyPause = false;

let cooldownUntil = 0;

let cooldownReason = null;

let entriesSinceCooldown = 0;

let lossStreak = 0;

const lastStop =
    {};

const tickers =
    new Map();

const candles =
    {};

const lastAnalyzed =
    {};

const candidatePool =
    new Map();

const shadowOpen =
    new Map();

let subscribed =
    new Set();

let latest =
    [];

let miniWs = null;

let klineWs = null;

let miniConnected = false;

let klineConnected = false;

let lastMiniMessage = 0;

let lastKlineMessage = 0;

let universeReady = false;

let shuttingDown = false;

let rankTimer = null;

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

function num(
    value,
    fallback = 0
) {
    const n =
        Number(value);

    return Number.isFinite(n)
        ? n
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
    difference,
    base
) {
    return base
        ? (
            difference /
            base
          ) * 100
        : 0;
}

function getUtcDay() {
    return new Date()
        .toISOString()
        .slice(
            0,
            10
        );
}

function sessionUTC() {
    const hour =
        new Date()
            .getUTCHours();

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

function ignored(
    symbol
) {
    const set =
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

    return set.has(
        symbol
    );
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
// SAVE / LOAD
// ============================================================

let saveTimer = null;

function scheduleSave() {

    if (
        saveTimer
    ) {
        return;
    }

    saveTimer =
        setTimeout(
            () => {

                saveTimer =
                    null;

                saveState();

            },
            20000
        );
}

function saveState() {

    try {

        const storedCandles =
            {};

        for (
            const [
                symbol,
                arr
            ]
            of Object.entries(
                candles
            )
        ) {

            if (
                Array.isArray(
                    arr
                )
            ) {

                storedCandles[
                    symbol
                ] =
                    arr.slice(
                        -C.maxCandles
                    );
            }
        }

        const state = {
            cash,

            positions,

            history:
                history.slice(
                    -C.historyLimit
                ),

            journal:
                journal.slice(
                    -C.journalLimit
                ),

            shadowTrades:
                shadowTrades.slice(
                    -C.shadowLimit
                ),

            stats,

            peakEquity,

            dailyPnL,

            dailyStartEquity,

            currentDay,

            manualPause,

            dailyPause,

            cooldownUntil,

            cooldownReason,

            entriesSinceCooldown,

            lossStreak,

            lastStop,

            candles:
                storedCandles,

            lastAnalyzed
        };

        fs.writeFileSync(
            C.stateFile,
            JSON.stringify(
                state
            )
        );

    } catch (
        error
    ) {

        console.error(
            'SAVE:',
            error.message
        );
    }
}

function loadState() {

    try {

        if (
            !fs.existsSync(
                C.stateFile
            )
        ) {

            console.log(
                'No V4.3 state. Starting fresh.'
            );

            return;
        }

        const state =
            JSON.parse(
                fs.readFileSync(
                    C.stateFile,
                    'utf8'
                )
            );

        cash =
            num(
                state.cash,
                C.startingBalance
            );

        positions =
            state.positions ||
            {};

        history =
            state.history ||
            [];

        journal =
            state.journal ||
            [];

        shadowTrades =
            state.shadowTrades ||
            [];

        stats = {
            ...stats,
            ...(state.stats || {})
        };

        peakEquity =
            num(
                state.peakEquity,
                C.startingBalance
            );

        dailyPnL =
            num(
                state.dailyPnL
            );

        dailyStartEquity =
            num(
                state.dailyStartEquity,
                C.startingBalance
            );

        currentDay =
            state.currentDay ||
            getUtcDay();

        manualPause =
            !!state.manualPause;

        dailyPause =
            !!state.dailyPause;

        cooldownUntil =
            num(
                state.cooldownUntil
            );

        cooldownReason =
            state.cooldownReason ||
            null;

        entriesSinceCooldown =
            num(
                state.entriesSinceCooldown
            );

        lossStreak =
            num(
                state.lossStreak
            );

        Object.assign(
            lastStop,
            state.lastStop ||
            {}
        );

        Object.assign(
            lastAnalyzed,
            state.lastAnalyzed ||
            {}
        );

        for (
            const [
                symbol,
                arr
            ]
            of Object.entries(
                state.candles ||
                {}
            )
        ) {

            if (
                Array.isArray(
                    arr
                )
            ) {

                candles[
                    symbol
                ] =
                    arr.slice(
                        -C.maxCandles
                    );
            }
        }

        console.log(
            `State restored | Cash $${cash.toFixed(2)} | Open ${Object.keys(positions).length}`
        );

    } catch (
        error
    ) {

        console.error(
            'LOAD:',
            error.message
        );
    }
}

// ============================================================
// JOURNAL
// ============================================================

function logJournal(
    row
) {

    journal.push({
        time:
            Date.now(),

        ...row
    });

    if (
        journal.length >
        C.journalLimit
    ) {

        journal =
            journal.slice(
                -C.journalLimit
            );
    }

    scheduleSave();
}

// ============================================================
// EQUITY
// ============================================================

function equity() {

    let total =
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

        total +=
            position.qty *
            price;
    }

    return total;
}

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

    const dd =
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
            dd
        );
}

// ============================================================
// DAILY RISK
// ============================================================

function checkNewDay() {

    const today =
        getUtcDay();

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

    tg(
        `🌅 <b>NEW V4.3 PAPER DAY</b>\n` +
        `Equity: $${dailyStartEquity.toFixed(2)}`
    );

    scheduleSave();
}

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
            `🛑 <b>DAILY LOSS LIMIT</b>\n\n` +
            `PnL: $${dailyPnL.toFixed(2)}\n` +
            `Entries paused.`
        );

        scheduleSave();
    }
}

// ============================================================
// COOLDOWN
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
            `▶️ <b>V4.3 COOLDOWN FINISHED</b>\n` +
            `Fresh candidates only.`
        );

        scheduleSave();

        return false;
    }

    return true;
}

function startCooldown(
    duration,
    reason
) {

    const until =
        Date.now() +
        duration;

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
        `⏸ <b>V4.3 SMART COOLDOWN</b>\n\n` +
        `Reason: ${reason}\n` +
        `Duration: ${Math.ceil(duration / 60000)} min`
    );

    scheduleSave();
}

function symbolCooling(
    symbol
) {

    const time =
        num(
            lastStop[
                symbol
            ]
        );

    if (!time) {
        return false;
    }

    return (
        Date.now() -
        time
    ) <
        C.symbolLossCooldownMs;
}

// ============================================================
// INDICATORS
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

    const slice =
        arr.slice(
            -period
        );

    return (
        slice.reduce(
            (
                sum,
                item
            ) =>
                sum +
                item[
                    key
                ],
            0
        ) /
        period
    );
}

function ema(
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

    let result =
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
                    item[
                        key
                    ],
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

        result =
            (
                arr[
                    i
                ][
                    key
                ] -
                result
            ) *
            multiplier +
            result;
    }

    return result;
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

    const total =
        up +
        down;

    return total
        ? 100 *
            (
                (
                    up -
                    down
                ) /
                total
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

        const high =
            arr[
                i
            ].high;

        const low =
            arr[
                i
            ].low;

        const prev =
            arr[
                i -
                1
            ].close;

        ranges.push(
            Math.max(
                high -
                low,

                Math.abs(
                    high -
                    prev
                ),

                Math.abs(
                    low -
                    prev
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
                a,
                b
            ) =>
                a +
                b,
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
        arr.length <
        3
    ) {
        return 'NEUTRAL';
    }

    const a =
        arr[
            arr.length -
            1
        ];

    const b =
        arr[
            arr.length -
            2
        ];

    const c =
        arr[
            arr.length -
            3
        ];

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

// ============================================================
// SUPPORT / RESISTANCE
// ============================================================

function supportResistance(
    arr,
    period = 20
) {

    const recent =
        arr.slice(
            -period
        );

    if (
        !recent.length
    ) {
        return null;
    }

    return {
        support:
            Math.min(
                ...recent.map(
                    x =>
                        x.low
                )
            ),

        resistance:
            Math.max(
                ...recent.map(
                    x =>
                        x.high
                )
            )
    };
}

// ============================================================
// BTC MARKET CONTEXT
// ============================================================

function getBTCContext() {

    const arr =
        candles[
            C.btcSymbol
        ];

    if (
        !arr ||
        arr.length <
            C.warmup
    ) {

        return {
            ready:
                false,

            bullish:
                false,

            score:
                0,

            change5:
                0,

            structure:
                'UNKNOWN'
        };
    }

    const current =
        arr[
            arr.length -
            1
        ];

    const e20 =
        ema(
            arr,
            20,
            'close'
        );

    const e50 =
        ema(
            arr,
            50,
            'close'
        );

    const struct =
        structure(
            arr
        );

    const previous5 =
        arr[
            arr.length -
            6
        ];

    const change5 =
        previous5
            ? pct(
                current.close -
                    previous5.close,
                previous5.close
            )
            : 0;

    let score = 0;

    if (
        current.close >
        e20
    ) {
        score += 35;
    }

    if (
        e20 >
        e50
    ) {
        score += 35;
    }

    if (
        struct !==
        'BEARISH'
    ) {
        score += 20;
    }

    if (
        change5 >
        C.maxBTCNegativeMomentum
    ) {
        score += 10;
    }

    const bullish =
        current.close >
            e20 &&
        e20 >
            e50 &&
        struct !==
            'BEARISH' &&
        change5 >
            C.maxBTCNegativeMomentum;

    return {
        ready:
            true,

        bullish,

        score,

        change5,

        structure:
            struct,

        price:
            current.close,

        ema20:
            e20,

        ema50:
            e50
    };
}

// ============================================================
// SCORE HELPERS
// ============================================================

function triangularScore(
    value,
    min,
    idealMin,
    idealMax,
    max,
    points
) {

    if (
        value <
            min ||
        value >
            max
    ) {
        return 0;
    }

    if (
        value >=
            idealMin &&
        value <=
            idealMax
    ) {
        return points;
    }

    if (
        value <
        idealMin
    ) {

        return points *
            (
                (
                    value -
                    min
                ) /
                (
                    idealMin -
                    min
                )
            );
    }

    return points *
        (
            (
                max -
                value
            ) /
            (
                max -
                idealMax
            )
        );
}

// ============================================================
// PRECISION ANALYSIS
// ============================================================

function analyzeSetup(
    arr,
    symbol
) {

    if (
        arr.length <
        C.warmup
    ) {
        return null;
    }

    const candle =
        arr[
            arr.length -
            1
        ];

    const e20 =
        ema(
            arr,
            20,
            'close'
        );

    const e50 =
        ema(
            arr,
            50,
            'close'
        );

    const volumeSMA =
        sma(
            arr,
            20,
            'volume'
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

    if (
        [
            e20,
            e50,
            volumeSMA,
            momentum,
            atrValue
        ].some(
            x =>
                x ===
                null
        )
    ) {
        return null;
    }

    const range =
        candle.high -
        candle.low;

    const body =
        Math.abs(
            candle.close -
            candle.open
        );

    const bodyRatio =
        range >
            0
            ? body /
                range
            : 0;

    const bullish =
        candle.close >
        candle.open;

    const upperWick =
        bullish
            ? candle.high -
                candle.close
            : candle.high -
                candle.open;

    const upperWickRatio =
        range >
            0
            ? upperWick /
                range
            : 1;

    const volumeRatio =
        volumeSMA >
            0
            ? candle.volume /
                volumeSMA
            : 0;

    const ema20Distance =
        pct(
            candle.close -
                e20,
            e20
        );

    const atrPct =
        pct(
            atrValue,
            candle.close
        );

    const struct =
        structure(
            arr
        );

    const sr =
        supportResistance(
            arr,
            20
        );

    const supportDistance =
        sr
            ? pct(
                candle.close -
                    sr.support,
                sr.support
            )
            : 999;

    const previousCandles =
        arr.slice(
            -21,
            -1
        );

    const previousResistance =
        Math.max(
            ...previousCandles.map(
                x =>
                    x.high
            )
        );

    const breakout =
        candle.close >
        previousResistance;

    const breakoutDistance =
        pct(
            candle.close -
                previousResistance,
            previousResistance
        );

    const ext5 =
        arr.length >=
            6
            ? pct(
                candle.close -
                    arr[
                        arr.length -
                        6
                    ].close,
                arr[
                    arr.length -
                    6
                ].close
            )
            : 0;

    const ext10 =
        arr.length >=
            11
            ? pct(
                candle.close -
                    arr[
                        arr.length -
                        11
                    ].close,
                arr[
                    arr.length -
                        11
                ].close
            )
            : 0;

    const btc =
        getBTCContext();

    // ========================================================
    // DYNAMIC SCORE
    // ========================================================

    let score = 0;

    const reasons =
        [];

    const warnings =
        [];

    // --------------------------------------------------------
    // TREND 20
    // --------------------------------------------------------

    if (
        candle.close >
        e20
    ) {

        score += 5;

        reasons.push(
            'ABOVE_EMA20'
        );
    }

    if (
        e20 >
        e50
    ) {

        score += 10;

        reasons.push(
            'EMA_TREND'
        );
    }

    if (
        struct ===
        'BULLISH'
    ) {

        score += 5;

        reasons.push(
            'BULLISH_STRUCTURE'
        );
    }

    // --------------------------------------------------------
    // CMO 15
    // --------------------------------------------------------

    const cmoScore =
        triangularScore(
            momentum,
            C.minCMO,
            C.idealCMOMin,
            C.idealCMOMax,
            C.maxCMO,
            15
        );

    score +=
        cmoScore;

    if (
        cmoScore >=
        12
    ) {

        reasons.push(
            'CMO_SWEET'
        );

    } else {

        warnings.push(
            'CMO_WEAK'
        );
    }

    // --------------------------------------------------------
    // VOLUME 20
    // --------------------------------------------------------

    const volumeScore =
        triangularScore(
            volumeRatio,
            C.hardMinVolume,
            C.idealVolumeMin,
            C.idealVolumeMax,
            C.hardMaxVolume,
            20
        );

    score +=
        volumeScore;

    if (
        volumeRatio >=
            C.idealVolumeMin &&
        volumeRatio <=
            C.idealVolumeMax
    ) {

        reasons.push(
            'VOLUME_SWEET'
        );
    }

    if (
        volumeRatio >=
        C.cautionVolume
    ) {

        score -= 12;

        warnings.push(
            'VOLUME_EXHAUSTION'
        );
    }

    // --------------------------------------------------------
    // EMA DISTANCE 15
    // --------------------------------------------------------

    const emaScore =
        triangularScore(
            ema20Distance,
            C.minEma20Distance,
            C.idealEmaMin,
            C.idealEmaMax,
            C.maxEma20Distance,
            15
        );

    score +=
        emaScore;

    if (
        ema20Distance >=
            C.idealEmaMin &&
        ema20Distance <=
            C.idealEmaMax
    ) {

        reasons.push(
            'EMA_ZONE'
        );
    }

    // --------------------------------------------------------
    // BREAKOUT 15
    // --------------------------------------------------------

    const breakoutScore =
        triangularScore(
            breakoutDistance,
            C.minBreakoutDistance,
            C.idealBreakoutMin,
            C.idealBreakoutMax,
            C.maxBreakoutDistance,
            15
        );

    if (
        breakout
    ) {

        score +=
            breakoutScore;

        if (
            breakoutScore >=
            11
        ) {

            reasons.push(
                'QUALITY_BREAKOUT'
            );
        }

    } else {

        warnings.push(
            'NO_BREAKOUT'
        );
    }

    // --------------------------------------------------------
    // CANDLE QUALITY 10
    // --------------------------------------------------------

    if (
        bullish &&
        bodyRatio >=
            C.minBodyRatio
    ) {

        score += 6;

        reasons.push(
            'STRONG_BODY'
        );
    }

    if (
        upperWickRatio <=
        C.maxUpperWickRatio
    ) {

        score += 4;

        reasons.push(
            'LOW_REJECTION'
        );

    } else {

        score -= 6;

        warnings.push(
            'UPPER_WICK'
        );
    }

    // --------------------------------------------------------
    // ATR 5
    // --------------------------------------------------------

    if (
        atrPct >=
            C.minATR &&
        atrPct <=
            C.maxATR
    ) {

        score += 5;

        reasons.push(
            'ATR_OK'
        );

    } else {

        warnings.push(
            'ATR_BAD'
        );
    }

    // --------------------------------------------------------
    // ANTI CHASE
    // --------------------------------------------------------

    if (
        ext5 >
        C.maxExtension5
    ) {

        score -= 12;

        warnings.push(
            'EXT5_HIGH'
        );
    }

    if (
        ext10 >
        C.maxExtension10
    ) {

        score -= 10;

        warnings.push(
            'EXT10_HIGH'
        );
    }

    if (
        supportDistance >
        C.maxSupportDistance
    ) {

        score -= 6;

        warnings.push(
            'FAR_SUPPORT'
        );
    }

    // --------------------------------------------------------
    // BTC MARKET CONTEXT 10
    // --------------------------------------------------------

    if (
        btc.ready &&
        btc.bullish
    ) {

        score += 10;

        reasons.push(
            'BTC_CONTEXT_OK'
        );

    } else if (
        C.requireBTCContext
    ) {

        score -= 15;

        warnings.push(
            'BTC_CONTEXT_BAD'
        );
    }

    // --------------------------------------------------------
    // Exhaustion cluster
    // --------------------------------------------------------

    const exhaustionCount =
        [
            volumeRatio >=
                C.cautionVolume,

            momentum >=
                76,

            ema20Distance >=
                1.75,

            ext5 >=
                1.70,

            upperWickRatio >
                C.maxUpperWickRatio
        ]
        .filter(
            Boolean
        )
        .length;

    if (
        exhaustionCount >=
        3
    ) {

        score -= 18;

        warnings.push(
            'EXHAUSTION_CLUSTER'
        );
    }

    score =
        clamp(
            score,
            0,
            100
        );

    // ========================================================
    // GRADE
    // ========================================================

    let grade =
        'C';

    if (
        score >=
            84 &&
        !warnings.includes(
            'EXHAUSTION_CLUSTER'
        ) &&
        !warnings.includes(
            'BTC_CONTEXT_BAD'
        ) &&
        volumeRatio <
            C.cautionVolume
    ) {

        grade =
            'A';

    } else if (
        score >=
        72
    ) {

        grade =
            'B';
    }

    // ========================================================
    // HARD CONDITIONS
    // ========================================================

    const hardConditions =
        bullish &&

        struct ===
            'BULLISH' &&

        candle.close >
            e20 &&

        e20 >
            e50 &&

        momentum >=
            C.minCMO &&

        momentum <=
            C.maxCMO &&

        volumeRatio >=
            C.hardMinVolume &&

        volumeRatio <
            C.hardMaxVolume &&

        bodyRatio >=
            C.minBodyRatio &&

        upperWickRatio <=
            C.maxUpperWickRatio &&

        breakout &&

        breakoutDistance >=
            C.minBreakoutDistance &&

        breakoutDistance <=
            C.maxBreakoutDistance &&

        ema20Distance >=
            C.minEma20Distance &&

        ema20Distance <=
            C.maxEma20Distance &&

        ext5 <=
            C.maxExtension5 &&

        ext10 <=
            C.maxExtension10 &&

        atrPct >=
            C.minATR &&

        atrPct <=
            C.maxATR &&

        (
            !C.requireBTCContext ||
            btc.bullish
        );

    const eligible =
        hardConditions &&
        grade ===
            'A' &&
        score >=
            C.minGradeScore;

    return {
        symbol,

        eligible,

        grade,

        score:
            Number(
                score.toFixed(
                    2
                )
            ),

        price:
            candle.close,

        ema20:
            e20,

        ema50:
            e50,

        cmo:
            momentum,

        volumeRatio,

        ema20Distance,

        breakoutDistance,

        ext5,

        ext10,

        atrPct,

        supportDistance,

        bodyRatio,

        upperWickRatio,

        structure:
            struct,

        previousResistance,

        reasons,

        warnings,

        exhaustionCount,

        btcContext:
            btc
    };
}

// ============================================================
// SHADOW TRADES
// ============================================================

function createShadow(
    candidate,
    reason
) {

    if (
        shadowOpen.has(
            candidate.symbol
        )
    ) {
        return;
    }

    shadowOpen.set(
        candidate.symbol,
        {
            symbol:
                candidate.symbol,

            signalTime:
                Date.now(),

            signalPrice:
                candidate.signalPrice,

            stopLoss:
                candidate.signalPrice *
                (
                    1 -
                    C.stopLossPct
                ),

            takeProfit:
                candidate.signalPrice *
                (
                    1 +
                    C.takeProfitPct
                ),

            score:
                candidate.score,

            grade:
                candidate.grade,

            blockedBy:
                reason
        }
    );
}

function manageShadow(
    symbol,
    price
) {

    const s =
        shadowOpen.get(
            symbol
        );

    if (!s) {
        return;
    }

    let result =
        null;

    if (
        price <=
        s.stopLoss
    ) {

        result =
            'SHADOW_LOSS';

    } else if (
        price >=
        s.takeProfit
    ) {

        result =
            'SHADOW_WIN';
    }

    if (!result) {
        return;
    }

    shadowTrades.push({
        ...s,

        exitPrice:
            price,

        result,

        exitTime:
            Date.now()
    });

    if (
        shadowTrades.length >
        C.shadowLimit
    ) {

        shadowTrades =
            shadowTrades.slice(
                -C.shadowLimit
            );
    }

    shadowOpen.delete(
        symbol
    );

    scheduleSave();
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
        )?.price;

    if (
        !marketPrice
    ) {
        return;
    }

    const candidate = {
        symbol,

        createdAt:
            Date.now(),

        expiresAt:
            Date.now() +
            C.candidateExpiryMs,

        closeTime,

        signalPrice:
            marketPrice,

        score:
            analysis.score,

        grade:
            analysis.grade,

        cmo:
            analysis.cmo,

        volumeRatio:
            analysis.volumeRatio,

        ema20Distance:
            analysis.ema20Distance,

        breakoutDistance:
            analysis.breakoutDistance,

        ext5:
            analysis.ext5,

        ext10:
            analysis.ext10,

        atrPct:
            analysis.atrPct,

        supportDistance:
            analysis.supportDistance,

        bodyRatio:
            analysis.bodyRatio,

        upperWickRatio:
            analysis.upperWickRatio,

        previousResistance:
            analysis.previousResistance,

        structure:
            analysis.structure,

        exhaustionCount:
            analysis.exhaustionCount,

        reasons:
            analysis.reasons,

        warnings:
            analysis.warnings,

        btcContext:
            analysis.btcContext
    };

    candidatePool.set(
        symbol,
        candidate
    );

    logJournal({
        type:
            'CANDIDATE',

        decision:
            'POOL',

        ...candidate
    });

    scheduleRanking();
}

function validateCandidate(
    candidate
) {

    const reasons =
        [];

    if (
        Date.now() >
        candidate.expiresAt
    ) {

        reasons.push(
            'EXPIRED'
        );
    }

    if (
        positions[
            candidate.symbol
        ]
    ) {

        reasons.push(
            'ALREADY_OPEN'
        );
    }

    if (
        symbolCooling(
            candidate.symbol
        )
    ) {

        reasons.push(
            'SYMBOL_COOLDOWN'
        );
    }

    const marketPrice =
        tickers.get(
            candidate.symbol
        )?.price;

    if (
        !marketPrice
    ) {

        reasons.push(
            'NO_PRICE'
        );

        return {
            valid:
                false,

            price:
                0,

            drift:
                0,

            reasons
        };
    }

    const drift =
        pct(
            marketPrice -
                candidate.signalPrice,
            candidate.signalPrice
        );

    if (
        Math.abs(
            drift
        ) >
        C.maxPriceDriftPct
    ) {

        reasons.push(
            'PRICE_MOVED'
        );
    }

    const btc =
        getBTCContext();

    if (
        C.requireBTCContext &&
        (
            !btc.ready ||
            !btc.bullish
        )
    ) {

        reasons.push(
            'BTC_CHANGED'
        );
    }

    return {
        valid:
            reasons.length ===
            0,

        price:
            marketPrice,

        drift,

        reasons
    };
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

                rankTimer =
                    null;

                rankAndExecute();

            },
            C.rankingDelayMs
        );
}

// ============================================================
// PAPER BUY
// ============================================================

function paperBuy(
    candidate,
    marketPrice
) {

    if (
        !C.paperTrading
    ) {
        return 'DISABLED';
    }

    if (
        manualPause ||
        dailyPause ||
        cooldownActive()
    ) {
        return 'PAUSED';
    }

    if (
        positions[
            candidate.symbol
        ]
    ) {
        return 'OPEN';
    }

    if (
        Object.keys(
            positions
        ).length >=
        C.maxPositions
    ) {
        return 'MAX_POSITIONS';
    }

    if (
        candidate.grade !==
        'A'
    ) {
        return 'GRADE_REJECT';
    }

    const currentEquity =
        equity();

    let allocation =
        currentEquity /
        C.maxPositions;

    allocation =
        Math.min(
            allocation,
            cash
        );

    if (
        allocation <
        C.minAllocation
    ) {
        return 'NO_BALANCE';
    }

    const entryPrice =
        marketPrice *
        (
            1 +
            C.slippagePct
        );

    const buyFee =
        allocation *
        C.feePct;

    const usable =
        allocation -
        buyFee;

    const qty =
        usable /
        entryPrice;

    cash -=
        allocation;

    stats.fees +=
        buyFee;

    positions[
        candidate.symbol
    ] = {
        symbol:
            candidate.symbol,

        entryPrice,

        qty,

        invested:
            allocation,

        stopLoss:
            entryPrice *
            (
                1 -
                C.stopLossPct
            ),

        takeProfit:
            entryPrice *
            (
                1 +
                C.takeProfitPct
            ),

        lastPrice:
            marketPrice,

        highestPrice:
            marketPrice,

        lowestPrice:
            marketPrice,

        mfePct:
            0,

        maePct:
            0,

        score:
            candidate.score,

        grade:
            candidate.grade,

        cmo:
            candidate.cmo,

        volumeRatio:
            candidate.volumeRatio,

        ema20Distance:
            candidate.ema20Distance,

        breakoutDistance:
            candidate.breakoutDistance,

        ext5:
            candidate.ext5,

        ext10:
            candidate.ext10,

        atrPct:
            candidate.atrPct,

        supportDistance:
            candidate.supportDistance,

        bodyRatio:
            candidate.bodyRatio,

        upperWickRatio:
            candidate.upperWickRatio,

        previousResistance:
            candidate.previousResistance,

        structure:
            candidate.structure,

        exhaustionCount:
            candidate.exhaustionCount,

        reasons:
            candidate.reasons,

        warnings:
            candidate.warnings,

        btcContext:
            candidate.btcContext,

        session:
            sessionUTC(),

        signalPrice:
            candidate.signalPrice,

        candidateCreatedAt:
            candidate.createdAt,

        entryTime:
            Date.now()
    };

    entriesSinceCooldown++;

    logJournal({
        type:
            'ENTRY',

        decision:
            'PAPER_BOUGHT',

        symbol:
            candidate.symbol,

        grade:
            candidate.grade,

        score:
            candidate.score,

        entryPrice,

        signalPrice:
            candidate.signalPrice,

        cmo:
            candidate.cmo,

        volumeRatio:
            candidate.volumeRatio,

        ema20Distance:
            candidate.ema20Distance,

        breakoutDistance:
            candidate.breakoutDistance,

        btcContext:
            candidate.btcContext
    });

    tg(
        `🟢 <b>LOMY V4.3 BUY</b>\n\n` +
        `<b>${candidate.symbol}</b>\n` +
        `Grade: ${candidate.grade}\n` +
        `Score: ${candidate.score}/100\n` +
        `Volume: ${candidate.volumeRatio.toFixed(2)}x\n` +
        `CMO: ${candidate.cmo.toFixed(1)}\n` +
        `EMA20: ${candidate.ema20Distance.toFixed(2)}%\n` +
        `Breakout: ${candidate.breakoutDistance.toFixed(2)}%\n\n` +
        `Amount: $${allocation.toFixed(2)}\n` +
        `Entry: ${entryPrice.toFixed(8)}\n` +
        `SL: ${positions[candidate.symbol].stopLoss.toFixed(8)}\n` +
        `TP: ${positions[candidate.symbol].takeProfit.toFixed(8)}`
    );

    scheduleSave();

    if (
        entriesSinceCooldown >=
        C.entriesBeforeCooldown
    ) {

        startCooldown(
            C.normalCooldownMs,
            'BATCH_COMPLETE'
        );
    }

    return 'PAPER_BOUGHT';
}

// ============================================================
// RANKING ENGINE
// ============================================================

function candidateRank(
    candidate
) {

    let rank =
        candidate.score;

    // Favor the sweet volume area.
    if (
        candidate.volumeRatio >=
            C.idealVolumeMin &&
        candidate.volumeRatio <=
            C.idealVolumeMax
    ) {

        rank += 6;
    }

    // Favor tested EMA area.
    if (
        candidate.ema20Distance >=
            C.idealEmaMin &&
        candidate.ema20Distance <=
            C.idealEmaMax
    ) {

        rank += 5;
    }

    // Favor clean breakout.
    if (
        candidate.breakoutDistance >=
            C.idealBreakoutMin &&
        candidate.breakoutDistance <=
            C.idealBreakoutMax
    ) {

        rank += 4;
    }

    rank -=
        candidate.exhaustionCount *
        4;

    return rank;
}

function rankAndExecute() {

    const isCooldown =
        cooldownActive();

    if (
        manualPause ||
        dailyPause ||
        isCooldown
    ) {

        for (
            const candidate
            of candidatePool.values()
        ) {

            createShadow(
                candidate,
                manualPause
                    ? 'MANUAL_PAUSE'
                    : dailyPause
                        ? 'DAILY_PAUSE'
                        : 'SMART_COOLDOWN'
            );
        }

        candidatePool.clear();

        return;
    }

    const freeSlots =
        C.maxPositions -
        Object.keys(
            positions
        ).length;

    if (
        freeSlots <=
        0
    ) {

        for (
            const candidate
            of candidatePool.values()
        ) {

            createShadow(
                candidate,
                'MAX_POSITIONS'
            );
        }

        candidatePool.clear();

        return;
    }

    const valid =
        [];

    for (
        const [
            symbol,
            candidate
        ]
        of candidatePool
    ) {

        const check =
            validateCandidate(
                candidate
            );

        if (
            !check.valid
        ) {

            logJournal({
                type:
                    'REJECT_AT_EXECUTION',

                symbol,

                score:
                    candidate.score,

                grade:
                    candidate.grade,

                reasons:
                    check.reasons
            });

            createShadow(
                candidate,
                check.reasons.join(
                    ','
                )
            );

            candidatePool.delete(
                symbol
            );

            continue;
        }

        valid.push({
            candidate,

            price:
                check.price,

            drift:
                check.drift,

            rank:
                candidateRank(
                    candidate
                )
        });
    }

    valid.sort(
        (
            a,
            b
        ) =>
            b.rank -
            a.rank
    );

    const maxNew =
        Math.min(
            C.maxEntriesPerCycle,
            freeSlots
        );

    let opened = 0;

    for (
        const item
        of valid
    ) {

        if (
            opened >=
            maxNew
        ) {
            break;
        }

        const result =
            paperBuy(
                item.candidate,
                item.price
            );

        if (
            result ===
            'PAPER_BOUGHT'
        ) {

            opened++;

            candidatePool.delete(
                item.candidate.symbol
            );
        }
    }

    // Valid but not selected = shadow candidates.
    for (
        const item
        of valid.slice(
            opened
        )
    ) {

        if (
            candidatePool.has(
                item.candidate.symbol
            )
        ) {

            createShadow(
                item.candidate,
                'RANK_NOT_SELECTED'
            );

            candidatePool.delete(
                item.candidate.symbol
            );
        }
    }
}

// ============================================================
// CLOSE POSITION
// ============================================================

function closePosition(
    symbol,
    marketPrice,
    reason
) {

    const position =
        positions[
            symbol
        ];

    if (!position) {
        return null;
    }

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
            reason ===
                'STOP_LOSS' ||
            reason ===
                'EARLY_FAILURE'
        ) {

            lossStreak++;

            lastStop[
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

    const holdingMinutes =
        (
            Date.now() -
            position.entryTime
        ) /
        60000;

    const record = {
        symbol,

        grade:
            position.grade,

        score:
            position.score,

        entryPrice:
            position.entryPrice,

        exitPrice,

        invested:
            position.invested,

        profit,

        profitPct,

        reason,

        cmo:
            position.cmo,

        volumeRatio:
            position.volumeRatio,

        ema20Distance:
            position.ema20Distance,

        breakoutDistance:
            position.breakoutDistance,

        ext5:
            position.ext5,

        ext10:
            position.ext10,

        atrPct:
            position.atrPct,

        supportDistance:
            position.supportDistance,

        bodyRatio:
            position.bodyRatio,

        upperWickRatio:
            position.upperWickRatio,

        exhaustionCount:
            position.exhaustionCount,

        mfePct:
            position.mfePct,

        maePct:
            position.maePct,

        highestPrice:
            position.highestPrice,

        lowestPrice:
            position.lowestPrice,

        btcContext:
            position.btcContext,

        session:
            position.session,

        reasons:
            position.reasons,

        warnings:
            position.warnings,

        buyFee:
            position.invested *
            C.feePct,

        sellFee,

        holdingMinutes,

        entryTime:
            position.entryTime,

        exitTime:
            Date.now()
    };

    history.push(
        record
    );

    if (
        history.length >
        C.historyLimit
    ) {

        history =
            history.slice(
                -C.historyLimit
            );
    }

    logJournal({
        type:
            'TRADE_CLOSE',

        ...record
    });

    delete positions[
        symbol
    ];

    updateDrawdown();

    checkDailyLoss();

    tg(
        `${profit >= 0 ? '✅' : '❌'} <b>LOMY V4.3 CLOSE</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Reason: ${reason}\n` +
        `PnL: $${profit.toFixed(2)}\n` +
        `MFE: ${position.mfePct.toFixed(2)}%\n` +
        `MAE: ${position.maePct.toFixed(2)}%\n` +
        `Holding: ${holdingMinutes.toFixed(1)} min\n` +
        `Loss Streak: ${lossStreak}\n` +
        `Cash: $${cash.toFixed(2)}`
    );

    if (
        lossStreak >=
        C.lossStreakLimit
    ) {

        startCooldown(
            C.lossCooldownMs,
            'LOSS_STREAK'
        );

        lossStreak = 0;
    }

    scheduleSave();

    scheduleRanking();

    return profit;
}

// ============================================================
// POSITION MONITOR
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

    // --------------------------------------------------------
    // Normal SL
    // --------------------------------------------------------

    if (
        price <=
        p.stopLoss
    ) {

        closePosition(
            symbol,
            price,
            'STOP_LOSS'
        );

        return;
    }

    // --------------------------------------------------------
    // Normal TP
    // --------------------------------------------------------

    if (
        price >=
        p.takeProfit
    ) {

        closePosition(
            symbol,
            price,
            'TAKE_PROFIT'
        );

        return;
    }

    // --------------------------------------------------------
    // Early failure
    // --------------------------------------------------------

    if (
        !C.earlyFailureEnabled
    ) {
        return;
    }

    const age =
        Date.now() -
        p.entryTime;

    if (
        age >
        C.earlyFailureWindowMs
    ) {
        return;
    }

    const currentLoss =
        pct(
            price -
                p.entryPrice,
            p.entryPrice
        );

    const lostBreakout =
        price <
        p.previousResistance *
        (
            1 -
            C.earlyFailureBreakoutLossPct
        );

    if (
        currentLoss <=
            -C.earlyFailureLossPct *
                100 &&
        lostBreakout
    ) {

        closePosition(
            symbol,
            price,
            'EARLY_FAILURE'
        );
    }
}

// ============================================================
// ANALYZE CLOSED CANDLE
// ============================================================

function analyzeClosedCandle(
    symbol,
    closeTime
) {

    const arr =
        candles[
            symbol
        ];

    if (
        !Array.isArray(
            arr
        ) ||
        arr.length <
            C.warmup
    ) {

        return;
    }

    if (
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
        analyzeSetup(
            arr,
            symbol
        );

    if (!analysis) {
        return;
    }

    let decision =
        'REJECTED';

    const blockers =
        [];

    if (
        manualPause
    ) {

        blockers.push(
            'MANUAL_PAUSE'
        );
    }

    if (
        dailyPause
    ) {

        blockers.push(
            'DAILY_PAUSE'
        );
    }

    if (
        cooldownActive()
    ) {

        blockers.push(
            'SMART_COOLDOWN'
        );
    }

    if (
        positions[
            symbol
        ]
    ) {

        blockers.push(
            'ALREADY_OPEN'
        );
    }

    if (
        symbolCooling(
            symbol
        )
    ) {

        blockers.push(
            'SYMBOL_COOLDOWN'
        );
    }

    if (
        analysis.eligible
    ) {

        const marketPrice =
            tickers.get(
                symbol
            )?.price ||
            analysis.price;

        const shadowCandidate = {
            symbol,

            signalPrice:
                marketPrice,

            score:
                analysis.score,

            grade:
                analysis.grade
        };

        if (
            blockers.length
        ) {

            decision =
                'SHADOW_BLOCKED';

            createShadow(
                shadowCandidate,
                blockers.join(
                    ','
                )
            );

        } else {

            decision =
                'POOL';

            addCandidate(
                symbol,
                analysis,
                closeTime
            );
        }
    }

    logJournal({
        type:
            'ANALYSIS',

        symbol,

        decision,

        blockers,

        eligible:
            analysis.eligible,

        grade:
            analysis.grade,

        score:
            analysis.score,

        cmo:
            analysis.cmo,

        volumeRatio:
            analysis.volumeRatio,

        ema20Distance:
            analysis.ema20Distance,

        breakoutDistance:
            analysis.breakoutDistance,

        ext5:
            analysis.ext5,

        ext10:
            analysis.ext10,

        atrPct:
            analysis.atrPct,

        bodyRatio:
            analysis.bodyRatio,

        upperWickRatio:
            analysis.upperWickRatio,

        exhaustionCount:
            analysis.exhaustionCount,

        btc:
            analysis.btcContext,

        warnings:
            analysis.warnings
    });

    latest.push({
        symbol,

        score:
            analysis.score,

        grade:
            analysis.grade,

        decision,

        cmo:
            analysis.cmo.toFixed(
                1
            ),

        volume:
            analysis.volumeRatio.toFixed(
                2
            ),

        ema:
            analysis.ema20Distance.toFixed(
                2
            ),

        breakout:
            analysis.breakoutDistance.toFixed(
                2
            ),

        ext5:
            analysis.ext5.toFixed(
                2
            ),

        btc:
            analysis.btcContext.bullish
                ? 'OK'
                : 'BAD',

        warnings:
            analysis.warnings.join(
                ', '
            )
    });

    latest =
        latest
            .sort(
                (
                    a,
                    b
                ) =>
                    b.score -
                    a.score
            )
            .slice(
                0,
                60
            );

    scheduleSave();
}

// ============================================================
// MINI TICKER
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

    console.log(
        'Connecting MINI TICKER...'
    );

    miniWs =
        new WebSocket(
            `${WS_BASE}/ws/!miniTicker@arr`
        );

    miniWs.on(
        'open',
        () => {

            miniConnected = true;

            lastMiniMessage =
                Date.now();

            console.log(
                'MINI ticker connected.'
            );

            tg(
                '🟢 <b>LOMY V4.3 Price Stream Connected</b>'
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
                    num(
                        item.c
                    );

                const quoteVolume =
                    num(
                        item.q
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

                if (
                    shadowOpen.has(
                        symbol
                    )
                ) {

                    manageShadow(
                        symbol,
                        price
                    );
                }
            }

            if (
                !universeReady &&
                tickers.size >
                    100
            ) {

                universeReady =
                    true;

                setTimeout(
                    rebalanceUniverse,
                    3000
                );
            }
        }
    );

    miniWs.on(
        'close',
        () => {

            miniConnected =
                false;

            console.log(
                'MINI disconnected.'
            );

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
// WEBSOCKET CONTROL
// ============================================================

const controlQueue =
    [];

let controlBusy =
    false;

function sendControl(
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

        } catch (
            error
        ) {

            console.error(
                'WS CONTROL:',
                error.message
            );
        }

        await sleep(
            1000
        );

        controlBusy =
            false;

    },
    250
);

// ============================================================
// KLINE
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

    console.log(
        'Connecting KLINE...'
    );

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
                'KLINE connected.'
            );

            if (
                subscribed.size
            ) {

                sendControl({
                    method:
                        'SUBSCRIBE',

                    params:
                        Array.from(
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
                Object.prototype
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

            if (
                !symbol
            ) {
                return;
            }

            const candle = {
                open:
                    num(
                        k.o
                    ),

                high:
                    num(
                        k.h
                    ),

                low:
                    num(
                        k.l
                    ),

                close:
                    num(
                        k.c
                    ),

                volume:
                    num(
                        k.v
                    ),

                closeTime:
                    k.T
            };

            if (
                !candles[
                    symbol
                ]
            ) {

                candles[
                    symbol
                ] =
                    [];
            }

            const arr =
                candles[
                    symbol
                ];

            const index =
                arr.findIndex(
                    x =>
                        x.closeTime ===
                        candle.closeTime
                );

            if (
                index >=
                0
            ) {

                arr[
                    index
                ] =
                    candle;

            } else {

                arr.push(
                    candle
                );
            }

            candles[
                symbol
            ] =
                arr
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

            analyzeClosedCandle(
                symbol,
                candle.closeTime
            );
        }
    );

    klineWs.on(
        'close',
        () => {

            klineConnected =
                false;

            console.log(
                'KLINE disconnected.'
            );

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
// UNIVERSE
// ============================================================

function topSymbols() {

    const result =
        Array.from(
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
            row =>
                row[
                    0
                ]
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

    const add =
        [];

    const remove =
        [];

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

        sendControl({
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

        sendControl({
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
        `Universe ${subscribed.size} | +${add.length} | -${remove.length}`
    );
}

setInterval(
    rebalanceUniverse,
    C.universeRefreshMs
);

// ============================================================
// WATCHDOG
// ============================================================

setInterval(
    () => {

        checkNewDay();

        cooldownActive();

        updateDrawdown();

        const now =
            Date.now();

        if (
            miniConnected &&
            now -
                lastMiniMessage >
                90000
        ) {

            try {

                miniWs.terminate();

            } catch {}
        }

        if (
            klineConnected &&
            now -
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
// API - PAUSE
// ============================================================

app.post(
    '/api/pause',
    (
        req,
        res
    ) => {

        manualPause =
            true;

        candidatePool.clear();

        scheduleSave();

        tg(
            '⏸ <b>V4.3 NEW ENTRIES PAUSED</b>'
        );

        res.json({
            success:
                true,

            msg:
                'New entries paused.'
        });
    }
);

// ============================================================
// API - RESUME
// ============================================================

app.post(
    '/api/resume',
    (
        req,
        res
    ) => {

        manualPause =
            false;

        candidatePool.clear();

        scheduleSave();

        tg(
            '▶️ <b>V4.3 NEW ENTRIES RESUMED</b>'
        );

        res.json({
            success:
                true,

            msg:
                'Fresh entries resumed.'
        });
    }
);

// ============================================================
// API - CLOSE ALL
// ============================================================

app.post(
    '/api/emergency-close',
    (
        req,
        res
    ) => {

        let closed =
            0;

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
                !price
            ) {

                continue;
            }

            closePosition(
                symbol,
                price,
                'EMERGENCY_CLOSE'
            );

            closed++;
        }

        res.json({
            success:
                true,

            msg:
                `Closed ${closed} paper positions.`
        });
    }
);

// ============================================================
// API - EXPORT
// ============================================================

app.get(
    '/api/export',
    (
        req,
        res
    ) => {

        res.setHeader(
            'Content-Type',
            'application/json'
        );

        res.setHeader(
            'Content-Disposition',
            `attachment; filename="lomy-v43-${Date.now()}.json"`
        );

        res.send(
            JSON.stringify(
                {
                    version:
                        C.version,

                    exportedAt:
                        new Date()
                            .toISOString(),

                    config:
                        C,

                    cash,

                    equity:
                        equity(),

                    positions,

                    history,

                    journal,

                    shadowTrades,

                    stats,

                    dailyPnL,

                    lossStreak,

                    cooldownUntil,

                    cooldownReason
                },
                null,
                2
            )
        );
    }
);

// ============================================================
// API DATA
// ============================================================

app.get(
    '/api/data',
    (
        req,
        res
    ) => {

        const closed =
            stats.wins +
            stats.losses;

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

        let ready =
            0;

        for (
            const symbol
            of subscribed
        ) {

            if (
                candles[
                    symbol
                ]?.length >=
                C.warmup
            ) {

                ready++;
            }
        }

        const btc =
            getBTCContext();

        const shadowWins =
            shadowTrades.filter(
                x =>
                    x.result ===
                    'SHADOW_WIN'
            ).length;

        const shadowLosses =
            shadowTrades.filter(
                x =>
                    x.result ===
                    'SHADOW_LOSS'
            ).length;

        res.json({
            version:
                C.version,

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

            symbols:
                subscribed.size,

            ready,

            poolSize:
                candidatePool.size,

            openPositions:
                Object.keys(
                    positions
                ).length,

            journalCount:
                journal.length,

            historyCount:
                history.length,

            shadowCount:
                shadowTrades.length,

            shadowWins,

            shadowLosses,

            dailyPnL:
                dailyPnL.toFixed(
                    2
                ),

            manualPause,

            dailyPause,

            cooldownActive:
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

            btc,

            stats: {
                ...stats,

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

            positions:
                Object.values(
                    positions
                ),

            latest
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
                miniConnected &&
                klineConnected
                    ? 'OK'
                    : 'DEGRADED',

            version:
                C.version,

            execution:
                'PAPER ONLY',

            market:
                'BINANCE WEBSOCKET',

            restRequests:
                0,

            symbols:
                subscribed.size,

            positions:
                Object.keys(
                    positions
                ).length,

            equity:
                equity().toFixed(
                    2
                ),

            btc:
                getBTCContext()
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

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1">

<title>LOMY V4.3</title>

<style>

*{
box-sizing:border-box
}

body{
margin:0;
padding:16px;
background:#0b0e11;
color:#eaecef;
font-family:Arial,sans-serif;
text-align:center
}

h1{
color:#f3ba2f
}

.badge{
max-width:1000px;
margin:auto;
padding:12px;
background:#f3ba2f;
color:#111;
border-radius:10px;
font-weight:bold
}

.status{
font-weight:bold;
margin:14px
}

.grid{
display:grid;
grid-template-columns:
repeat(auto-fit,minmax(140px,1fr));
gap:10px;
max-width:1250px;
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
font-size:22px;
font-weight:bold;
margin-top:7px
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

button{
padding:12px 17px;
margin:5px;
border:0;
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

.export{
background:#3772ff;
color:#fff
}

table{
width:100%;
max-width:1400px;
margin:20px auto;
border-collapse:collapse;
background:#1e2329
}

th{
background:#2b3139;
color:#848e9c
}

th,td{
font-size:11px;
padding:8px;
border-bottom:1px solid #2b3139
}

</style>

</head>

<body>

<h1>
🤖 LOMY V4.3 PRECISION+
</h1>

<div class="badge">
DYNAMIC SCORE • BTC CONTEXT • ANTI-CHASE • SHADOW TEST • PAPER ONLY
</div>

<div
class="status"
id="connection">
Connecting...
</div>

<div
class="status"
id="btc">
BTC Context...
</div>

<div
class="status"
id="cooldown">
Smart Cooldown...
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
<div class="label">POOL</div>
<div class="value" id="pool">0</div>
</div>

<div class="card">
<div class="label">WS SYMBOLS</div>
<div class="value" id="symbols">0</div>
</div>

<div class="card">
<div class="label">READY</div>
<div class="value" id="ready">0</div>
</div>

<div class="card">
<div class="label">JOURNAL</div>
<div class="value" id="journal">0</div>
</div>

<div class="card">
<div class="label">TODAY PNL</div>
<div class="value" id="daily">$0</div>
</div>

<div class="card">
<div class="label">BATCH</div>
<div class="value" id="batch">0/10</div>
</div>

<div class="card">
<div class="label">LOSS STREAK</div>
<div class="value" id="loss">0/3</div>
</div>

<div class="card">
<div class="label">SHADOW WIN</div>
<div class="value" id="shadowWin">0</div>
</div>

<div class="card">
<div class="label">SHADOW LOSS</div>
<div class="value" id="shadowLoss">0</div>
</div>

<div class="card">
<div class="label">EARLY EXITS</div>
<div class="value" id="early">0</div>
</div>

<div class="card">
<div class="label">MAX DD</div>
<div class="value" id="dd">0%</div>
</div>

</div>

<button
class="pause"
onclick="pauseEntries()">
⏸ PAUSE
</button>

<button
class="resume"
onclick="resumeEntries()">
▶ RESUME
</button>

<button
class="close"
onclick="closeAll()">
🚨 CLOSE ALL
</button>

<button
class="export"
onclick="location='/api/export'">
⬇ EXPORT JSON
</button>

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
<th>Ext5%</th>
<th>BTC</th>
<th>Warnings</th>

</tr>

</thead>

<tbody id="table">

<tr>
<td colspan="11">
Collecting market data...
</td>
</tr>

</tbody>

</table>

</div>

<script>

async function load(){

try{

const response =
await fetch('/api/data');

const d =
await response.json();

document.getElementById('cash').innerText =
'$' + d.cash;

document.getElementById('equity').innerText =
'$' + d.equity;

document.getElementById('closed').innerText =
d.stats.totalTrades;

document.getElementById('win').innerText =
d.stats.winRate + '%';

document.getElementById('profit').innerText =
'$' + Number(d.stats.netProfit).toFixed(2);

document.getElementById('pf').innerText =
d.stats.profitFactor;

document.getElementById('open').innerText =
d.openPositions;

document.getElementById('pool').innerText =
d.poolSize;

document.getElementById('symbols').innerText =
d.symbols;

document.getElementById('ready').innerText =
d.ready;

document.getElementById('journal').innerText =
d.journalCount;

document.getElementById('daily').innerText =
'$' + d.dailyPnL;

document.getElementById('batch').innerText =
d.entriesSinceCooldown + '/10';

document.getElementById('loss').innerText =
d.lossStreak + '/3';

document.getElementById('shadowWin').innerText =
d.shadowWins;

document.getElementById('shadowLoss').innerText =
d.shadowLosses;

document.getElementById('early').innerText =
d.stats.earlyFailureExits;

document.getElementById('dd').innerText =
Number(d.stats.maxDrawdown).toFixed(2) + '%';

const connection =
document.getElementById('connection');

if(
d.miniConnected &&
d.klineConnected
){

connection.innerText =
'🟢 MARKET WEBSOCKETS LIVE • REST = 0';

connection.className =
'status green';

}else{

connection.innerText =
'🔴 WEBSOCKET CONNECTING...';

connection.className =
'status red';

}

const btc =
document.getElementById('btc');

if(
d.btc &&
d.btc.ready
){

btc.innerText =
d.btc.bullish
?'₿ BTC CONTEXT: BULLISH / ENTRY ENABLED'
:'₿ BTC CONTEXT: WEAK / ALT ENTRIES BLOCKED';

btc.className =
d.btc.bullish
?'status green'
:'status red';

}else{

btc.innerText =
'₿ BTC CONTEXT WARMING UP';

btc.className =
'status yellow';

}

const cooldown =
document.getElementById('cooldown');

if(
d.cooldownActive
){

cooldown.innerText =
'🧠 COOLDOWN ' +
d.cooldownReason +
' • ' +
d.cooldownMinutes +
' MIN';

cooldown.className =
'status yellow';

}else{

cooldown.innerText =
'✅ SMART COOLDOWN READY';

cooldown.className =
'status green';

}

const table =
document.getElementById('table');

table.innerHTML = '';

if(
!d.latest.length
){

table.innerHTML =
'<tr><td colspan="11">Warm-up: ' +
d.ready +
' / ' +
d.symbols +
'</td></tr>';

return;

}

d.latest.forEach(x=>{

table.innerHTML +=

'<tr>' +

'<td><b>' +
x.symbol +
'</b></td>' +

'<td>' +
x.grade +
'</td>' +

'<td>' +
x.score +
'</td>' +

'<td>' +
x.decision +
'</td>' +

'<td>' +
x.cmo +
'</td>' +

'<td>' +
x.volume +
'x</td>' +

'<td>' +
x.ema +
'%</td>' +

'<td>' +
x.breakout +
'%</td>' +

'<td>' +
x.ext5 +
'%</td>' +

'<td>' +
x.btc +
'</td>' +

'<td>' +
x.warnings +
'</td>' +

'</tr>';

});

}catch(e){

console.error(e);

}

}

async function pauseEntries(){

await fetch(
'/api/pause',
{method:'POST'}
);

load();

}

async function resumeEntries(){

await fetch(
'/api/resume',
{method:'POST'}
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

await fetch(
'/api/emergency-close',
{method:'POST'}
);

load();

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
// SHUTDOWN
// ============================================================

function shutdown(
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
        `${signal}: saving V4.3 state`
    );

    saveState();

    try {

        miniWs?.close();

    } catch {}

    try {

        klineWs?.close();

    } catch {}

    setTimeout(
        () =>
            process.exit(
                0
            ),
        1000
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
    error => {

        console.error(
            'UNHANDLED REJECTION:',
            error
        );
    }
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

// ============================================================
// START
// ============================================================

app.listen(
    PORT,
    () => {

        console.log('');
        console.log('==============================================');
        console.log('LOMY V4.3 PRECISION+');
        console.log('==============================================');
        console.log('Mode: ADVANCED PRECISION TEST');
        console.log('Market Data: BINANCE WEBSOCKET ONLY');
        console.log('REST Scanner: OFF');
        console.log('Execution: PAPER ONLY');
        console.log(`Universe: TOP ${C.universeSize}`);
        console.log(`Max Positions: ${C.maxPositions}`);
        console.log(`Max New Entries/Cycle: ${C.maxEntriesPerCycle}`);
        console.log('Dynamic Score: ON');
        console.log('Grade A Only: ON');
        console.log('BTC Context: ON');
        console.log('Volume Sweet Spot: ON');
        console.log('False Breakout Protection: ON');
        console.log('Anti-Chase: ON');
        console.log('MFE / MAE Tracking: ON');
        console.log('Early Failure Exit: ON');
        console.log('Shadow Trades: ON');
        console.log('Smart Cooldown: ON');
        console.log('==============================================');

        loadState();

        connectMini();

        connectKline();

        tg(
            `🚀 <b>LOMY V4.3 PRECISION+ STARTED</b>\n\n` +

            `Execution: <b>PAPER ONLY</b>\n` +

            `Dynamic Score: <b>ON</b>\n` +

            `Grade A Only: <b>ON</b>\n` +

            `BTC Context: <b>ON</b>\n` +

            `Anti-Chase: <b>ON</b>\n` +

            `Shadow Testing: <b>ON</b>\n` +

            `MFE/MAE: <b>ON</b>\n` +

            `Early Failure: <b>ON</b>\n\n` +

            `Balance: $${equity().toFixed(2)}\n` +

            `Universe: Top ${C.universeSize}\n` +

            `Max Open: ${C.maxPositions}`
        );
    }
);
