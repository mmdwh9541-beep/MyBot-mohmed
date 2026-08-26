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
// LOMY PRECISION ENGINE V4.1
// ADVANCED DATA COLLECTOR
// WEBSOCKET MARKET DATA ONLY
// PAPER TRADING ONLY
// ============================================================

const CONFIG = {
    version: '4.1-DATA-COLLECTOR',

    paperTrading: true,

    startingBalance: 10000,
    maxPositions: 10,
    minimumScore: 80,

    stopLossPct: 0.01,
    takeProfitPct: 0.02,
    dailyLossLimitPct: 0.10,

    feePct: 0.001,
    slippagePct: 0.0005,

    candleInterval: '5m',

    minWarmupCandles: 55,
    maxStoredCandles: 70,

    universeSize: 300,
    minQuoteVolume: 250000,

    universeRefreshMs: 30 * 60 * 1000,
    controlMessageGapMs: 1000,

    journalMinimumScore: 60,
    journalMaxRecords: 15000,
    tradeHistoryMaxRecords: 3000,

    cooldownTrackingMs: 6 * 60 * 60 * 1000,

    // V4.1 only records cooldown information.
    // It does NOT block trades because of cooldown.
    enforceLossCooldown: false,

    stateFile: path.join(__dirname, 'paper-state.json')
};

// ============================================================
// GLOBAL STATE
// ============================================================

let paperBalance = CONFIG.startingBalance;

let activePositions = {};
let tradeHistory = [];
let opportunityJournal = [];
let lastStopLossBySymbol = {};

let stats = {
    totalTrades: 0,
    winningTrades: 0,
    losingTrades: 0,

    grossProfit: 0,
    grossLoss: 0,

    netProfit: 0,
    totalFees: 0,

    bestTrade: 0,
    worstTrade: 0,

    maxDrawdown: 0
};

let journalStats = {
    analyzedCandles: 0,
    journaledCandidates: 0,
    paperEntries: 0,
    rejectedCandidates: 0
};

let peakEquity = CONFIG.startingBalance;

let dailyPnL = 0;
let dailyStartingEquity = CONFIG.startingBalance;

let currentDay = utcDay();

let tradingPaused = false;

// Manual pause stops NEW entries only.
// WebSockets and data collection continue.
let manualPause = false;

// ============================================================
// MARKET CACHE
// ============================================================

const marketTickers = new Map();

const candleBuffers = {};

const lastAnalyzedCloseTime = {};

let subscribedSymbols = new Set();

let latestResults = [];

// ============================================================
// WEBSOCKET STATE
// ============================================================

let miniWs = null;
let klineWs = null;

let miniConnected = false;
let klineConnected = false;

let lastMiniMessage = 0;
let lastKlineMessage = 0;

let miniReconnectTimer = null;
let klineReconnectTimer = null;

let universeReady = false;
let shuttingDown = false;

// ============================================================
// HELPERS
// ============================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function safeNumber(value, fallback = 0) {
    const n = Number(value);
    return Number.isFinite(n) ? n : fallback;
}

function utcDay() {
    return new Date().toISOString().slice(0, 10);
}

function pct(numerator, denominator) {
    if (!denominator) return 0;

    return (numerator / denominator) * 100;
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function ignoredSymbol(symbol) {
    const ignored = new Set([
        'USDCUSDT',
        'FDUSDUSDT',
        'TUSDUSDT',
        'USDPUSDT',
        'BUSDUSDT',
        'DAIUSDT',
        'USDEUSDT',
        'USD1USDT'
    ]);

    return ignored.has(symbol);
}

function getSessionUTC(timestamp = Date.now()) {
    const hour = new Date(timestamp).getUTCHours();

    if (hour >= 0 && hour < 7) {
        return 'ASIA';
    }

    if (hour >= 7 && hour < 13) {
        return 'LONDON';
    }

    if (hour >= 13 && hour < 16) {
        return 'LONDON_NY_OVERLAP';
    }

    if (hour >= 16 && hour < 21) {
        return 'NEW_YORK';
    }

    return 'LATE_US';
}

function recentStop(symbol) {
    const last = safeNumber(lastStopLossBySymbol[symbol]);

    if (!last) {
        return {
            recentStopLoss: false,
            minutesSinceStopLoss: null
        };
    }

    const elapsed = Date.now() - last;

    return {
        recentStopLoss: elapsed < CONFIG.cooldownTrackingMs,
        minutesSinceStopLoss: elapsed / 60000
    };
}

// ============================================================
// STATE PERSISTENCE
// ============================================================

let saveTimer = null;

function scheduleSave() {
    if (saveTimer) return;

    saveTimer = setTimeout(() => {
        saveTimer = null;
        saveState();
    }, 30000);
}

function saveState() {
    try {
        const buffers = {};

        for (const [symbol, candles] of Object.entries(candleBuffers)) {
            if (Array.isArray(candles)) {
                buffers[symbol] = candles.slice(
                    -CONFIG.maxStoredCandles
                );
            }
        }

        const state = {
            version: CONFIG.version,
            savedAt: new Date().toISOString(),

            paperBalance,
            activePositions,

            tradeHistory: tradeHistory.slice(
                -CONFIG.tradeHistoryMaxRecords
            ),

            opportunityJournal: opportunityJournal.slice(
                -CONFIG.journalMaxRecords
            ),

            lastStopLossBySymbol,

            stats,
            journalStats,

            peakEquity,

            dailyPnL,
            dailyStartingEquity,
            currentDay,

            tradingPaused,
            manualPause,

            candleBuffers: buffers,

            lastAnalyzedCloseTime
        };

        fs.writeFileSync(
            CONFIG.stateFile,
            JSON.stringify(state)
        );

    } catch (error) {
        console.error(
            'STATE SAVE ERROR:',
            error.message
        );
    }
}

function loadState() {
    try {
        if (!fs.existsSync(CONFIG.stateFile)) {
            console.log(
                'No previous paper state. Starting fresh.'
            );
            return;
        }

        const state = JSON.parse(
            fs.readFileSync(
                CONFIG.stateFile,
                'utf8'
            )
        );

        paperBalance = safeNumber(
            state.paperBalance,
            CONFIG.startingBalance
        );

        activePositions =
            state.activePositions || {};

        tradeHistory =
            state.tradeHistory || [];

        opportunityJournal =
            state.opportunityJournal || [];

        lastStopLossBySymbol =
            state.lastStopLossBySymbol || {};

        stats = {
            ...stats,
            ...(state.stats || {})
        };

        journalStats = {
            ...journalStats,
            ...(state.journalStats || {})
        };

        peakEquity = safeNumber(
            state.peakEquity,
            CONFIG.startingBalance
        );

        dailyPnL = safeNumber(
            state.dailyPnL
        );

        dailyStartingEquity = safeNumber(
            state.dailyStartingEquity,
            CONFIG.startingBalance
        );

        currentDay =
            state.currentDay ||
            utcDay();

        tradingPaused =
            Boolean(state.tradingPaused);

        manualPause =
            Boolean(state.manualPause);

        if (state.candleBuffers) {
            for (
                const [symbol, candles]
                of Object.entries(state.candleBuffers)
            ) {
                if (Array.isArray(candles)) {
                    candleBuffers[symbol] =
                        candles.slice(
                            -CONFIG.maxStoredCandles
                        );
                }
            }
        }

        if (state.lastAnalyzedCloseTime) {
            Object.assign(
                lastAnalyzedCloseTime,
                state.lastAnalyzedCloseTime
            );
        }

        console.log(
            `State restored | Cash $${paperBalance.toFixed(2)} | Open ${Object.keys(activePositions).length} | Journal ${opportunityJournal.length}`
        );

    } catch (error) {
        console.error(
            'STATE RESTORE ERROR:',
            error.message
        );
    }
}

// ============================================================
// TELEGRAM
// ============================================================

const telegramQueue = [];

let telegramSending = false;

function sendTelegramMessage(text) {
    if (!TELEGRAM_TOKEN || !CHAT_ID) {
        return;
    }

    telegramQueue.push(text);
}

async function processTelegramQueue() {
    if (
        telegramSending ||
        !telegramQueue.length
    ) {
        return;
    }

    telegramSending = true;

    const text = telegramQueue.shift();

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
        if (error.response?.status === 429) {
            telegramQueue.unshift(text);

            await sleep(5000);

        } else {
            console.error(
                'Telegram:',
                error.response?.data ||
                error.message
            );
        }

    } finally {
        telegramSending = false;
    }
}

setInterval(
    processTelegramQueue,
    1200
);

// ============================================================
// EQUITY / DRAWDOWN
// ============================================================

function currentEquity() {
    let equity = paperBalance;

    for (
        const position
        of Object.values(activePositions)
    ) {
        const price =
            marketTickers.get(position.symbol)?.price ||
            position.lastPrice ||
            position.entryPrice;

        equity +=
            position.qty *
            price;
    }

    return equity;
}

function updateDrawdown() {
    const equity = currentEquity();

    if (equity > peakEquity) {
        peakEquity = equity;
    }

    const drawdown =
        peakEquity > 0
            ? pct(
                peakEquity - equity,
                peakEquity
            )
            : 0;

    stats.maxDrawdown =
        Math.max(
            stats.maxDrawdown,
            drawdown
        );
}

// ============================================================
// DAILY RISK
// ============================================================

function checkDailyReset() {
    const today = utcDay();

    if (today === currentDay) {
        return;
    }

    currentDay = today;

    dailyPnL = 0;

    tradingPaused = false;

    dailyStartingEquity =
        currentEquity();

    scheduleSave();

    sendTelegramMessage(
        `🌅 <b>NEW PAPER DAY</b>\n` +
        `Starting Equity: $${dailyStartingEquity.toFixed(2)}`
    );
}

function checkDailyLossLimit() {
    const limit =
        dailyStartingEquity *
        CONFIG.dailyLossLimitPct;

    if (
        limit > 0 &&
        dailyPnL <= -limit &&
        !tradingPaused
    ) {
        tradingPaused = true;

        scheduleSave();

        sendTelegramMessage(
            `🛑 <b>DAILY LOSS LIMIT</b>\n` +
            `Daily PnL: $${dailyPnL.toFixed(2)}\n` +
            `New entries stopped.`
        );
    }
}

setInterval(
    () => {
        checkDailyReset();
        updateDrawdown();
    },
    10000
);

// ============================================================
// INDICATORS
// ============================================================

function sma(data, period, key) {
    if (data.length < period) {
        return null;
    }

    return data
        .slice(-period)
        .reduce(
            (sum, item) =>
                sum + item[key],
            0
        ) / period;
}

function ema(data, period, key) {
    if (data.length < period) {
        return null;
    }

    let result =
        data
            .slice(0, period)
            .reduce(
                (sum, item) =>
                    sum + item[key],
                0
            ) /
        period;

    const multiplier =
        2 / (period + 1);

    for (
        let i = period;
        i < data.length;
        i++
    ) {
        result =
            (
                data[i][key] -
                result
            ) *
            multiplier +
            result;
    }

    return result;
}

function cmo(data, period) {
    if (
        data.length <
        period + 1
    ) {
        return null;
    }

    let up = 0;
    let down = 0;

    for (
        let i =
            data.length - period;
        i < data.length;
        i++
    ) {
        const diff =
            data[i].close -
            data[i - 1].close;

        if (diff > 0) {
            up += diff;
        } else {
            down +=
                Math.abs(diff);
        }
    }

    const total =
        up + down;

    if (total === 0) {
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

function atr(data, period) {
    if (
        data.length <
        period + 1
    ) {
        return null;
    }

    const ranges = [];

    for (
        let i = 1;
        i < data.length;
        i++
    ) {
        ranges.push(
            Math.max(
                data[i].high -
                    data[i].low,

                Math.abs(
                    data[i].high -
                    data[i - 1].close
                ),

                Math.abs(
                    data[i].low -
                    data[i - 1].close
                )
            )
        );
    }

    return ranges
        .slice(-period)
        .reduce(
            (a, b) => a + b,
            0
        ) / period;
}

// ============================================================
// MARKET STRUCTURE
// ============================================================

function structure(candles) {
    if (candles.length < 4) {
        return 'NEUTRAL';
    }

    const last =
        candles[
            candles.length - 1
        ];

    const previous =
        candles[
            candles.length - 2
        ];

    if (
        last.high > previous.high &&
        last.low > previous.low
    ) {
        return 'BULLISH';
    }

    if (
        last.high < previous.high &&
        last.low < previous.low
    ) {
        return 'BEARISH';
    }

    return 'NEUTRAL';
}

// ============================================================
// SUPPORT / RESISTANCE
// ============================================================

function supportResistance(
    candles,
    period = 20
) {
    const recent =
        candles.slice(-period);

    if (!recent.length) {
        return null;
    }

    return {
        support:
            Math.min(
                ...recent.map(
                    x => x.low
                )
            ),

        resistance:
            Math.max(
                ...recent.map(
                    x => x.high
                )
            )
    };
}

// ============================================================
// FIBONACCI POSITION
// ============================================================

function fibPosition(candles) {
    const recent =
        candles.slice(-30);

    if (recent.length < 20) {
        return null;
    }

    const high =
        Math.max(
            ...recent.map(
                c => c.high
            )
        );

    const low =
        Math.min(
            ...recent.map(
                c => c.low
            )
        );

    if (high <= low) {
        return null;
    }

    return (
        (
            candles[
                candles.length - 1
            ].close -
            low
        ) /
        (
            high -
            low
        )
    );
}

// ============================================================
// SCORE ENGINE
// ============================================================

function calculateScore(candles) {
    if (
        candles.length <
        CONFIG.minWarmupCandles
    ) {
        return null;
    }

    const candle =
        candles[
            candles.length - 1
        ];

    const ema20 =
        ema(
            candles,
            20,
            'close'
        );

    const ema50 =
        ema(
            candles,
            50,
            'close'
        );

    const volumeSMA =
        sma(
            candles,
            20,
            'volume'
        );

    const CMO =
        cmo(
            candles,
            9
        );

    const ATR =
        atr(
            candles,
            14
        );

    if (
        [
            ema20,
            ema50,
            volumeSMA,
            CMO,
            ATR
        ].some(
            value =>
                value === null
        )
    ) {
        return null;
    }

    let score = 0;

    const reasons = [];

    if (candle.close > ema20) {
        score += 10;
        reasons.push('EMA20');
    }

    if (
        ema20 > ema50 &&
        candle.close > ema50
    ) {
        score += 15;
        reasons.push('UPTREND');
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
        range > 0
            ? body / range
            : 0;

    const bullishCandle =
        candle.close >
        candle.open;

    if (
        bullishCandle &&
        bodyRatio >= 0.55
    ) {
        score += 15;
        reasons.push(
            'STRONG_CANDLE'
        );
    }

    const volumeRatio =
        volumeSMA > 0
            ? candle.volume /
              volumeSMA
            : 0;

    if (volumeRatio >= 1.4) {
        score += 15;
        reasons.push(
            'VOLUME_SPIKE'
        );
    }

    if (volumeRatio >= 1.2) {
        score += 10;
        reasons.push(
            'LIQUIDITY'
        );
    }

    if (CMO >= 50) {
        score += 15;
        reasons.push('CMO');
    }

    const marketStructure =
        structure(candles);

    if (
        marketStructure ===
        'BULLISH'
    ) {
        score += 10;
        reasons.push(
            'STRUCTURE'
        );
    }

    const fib =
        fibPosition(candles);

    let fibScore = 0;

    if (fib !== null) {
        if (
            fib >= 0.382 &&
            fib <= 0.618
        ) {
            fibScore = 5;

        } else if (
            fib >= 0.30 &&
            fib <= 0.70
        ) {
            fibScore = 3;
        }
    }

    score += fibScore;

    if (fibScore > 0) {
        reasons.push('FIB');
    }

    if (
        range >=
        ATR * 0.8
    ) {
        score += 5;
        reasons.push('ATR');
    }

    const previous =
        candles.slice(
            -21,
            -1
        );

    const previousResistance =
        previous.length
            ? Math.max(
                ...previous.map(
                    c => c.high
                )
            )
            : 0;

    const breakout =
        previousResistance > 0 &&
        candle.close >
            previousResistance;

    if (breakout) {
        score += 5;
        reasons.push(
            'BREAKOUT'
        );
    }

    const sr =
        supportResistance(
            candles,
            20
        );

    return {
        score:
            Math.min(
                score,
                100
            ),

        reasons,

        ema20,
        ema50,

        cmo: CMO,
        atr: ATR,

        volumeRatio,
        bodyRatio,

        bullishCandle,

        structure:
            marketStructure,

        breakout,

        support:
            sr?.support || 0,

        resistance:
            sr?.resistance || 0,

        fibPosition:
            fib,

        range
    };
}

// ============================================================
// ADVANCED MARKET CONTEXT
// ============================================================

function buildMarketContext(
    candles,
    analysis,
    marketPrice,
    closeTime
) {
    const candle =
        candles[
            candles.length - 1
        ];

    const previous =
        candles[
            candles.length - 2
        ];

    const range =
        candle.high -
        candle.low;

    const upperWick =
        range > 0
            ? (
                candle.high -
                Math.max(
                    candle.open,
                    candle.close
                )
              ) / range
            : 0;

    const lowerWick =
        range > 0
            ? (
                Math.min(
                    candle.open,
                    candle.close
                ) -
                candle.low
              ) / range
            : 0;

    const closeLocation =
        range > 0
            ? (
                candle.close -
                candle.low
              ) / range
            : 0;

    const extension5 =
        candles.length >= 6
            ? pct(
                candle.close -
                    candles[
                        candles.length - 6
                    ].close,

                candles[
                    candles.length - 6
                ].close
            )
            : 0;

    const extension10 =
        candles.length >= 11
            ? pct(
                candle.close -
                    candles[
                        candles.length - 11
                    ].close,

                candles[
                    candles.length - 11
                ].close
            )
            : 0;

    const ema20DistancePct =
        pct(
            marketPrice -
                analysis.ema20,
            analysis.ema20
        );

    const ema50DistancePct =
        pct(
            marketPrice -
                analysis.ema50,
            analysis.ema50
        );

    const atrPct =
        pct(
            analysis.atr,
            marketPrice
        );

    const distanceFromSupportPct =
        analysis.support > 0
            ? pct(
                marketPrice -
                    analysis.support,
                analysis.support
            )
            : null;

    const distanceToResistancePct =
        analysis.resistance > 0
            ? pct(
                analysis.resistance -
                    marketPrice,
                marketPrice
            )
            : null;

    const previousResistance =
        Math.max(
            ...candles
                .slice(
                    -21,
                    -1
                )
                .map(
                    c => c.high
                )
        );

    const breakoutDistancePct =
        previousResistance > 0
            ? pct(
                candle.close -
                    previousResistance,
                previousResistance
            )
            : 0;

    return {
        timestamp:
            Date.now(),

        isoTime:
            new Date()
                .toISOString(),

        candleCloseTime:
            closeTime,

        sessionUTC:
            getSessionUTC(),

        marketPrice,

        open:
            candle.open,

        high:
            candle.high,

        low:
            candle.low,

        close:
            candle.close,

        previousClose:
            previous?.close ||
            null,

        bodyRatio:
            analysis.bodyRatio,

        upperWickRatio:
            upperWick,

        lowerWickRatio:
            lowerWick,

        closeLocationRatio:
            clamp(
                closeLocation,
                0,
                1
            ),

        ema20:
            analysis.ema20,

        ema50:
            analysis.ema50,

        ema20DistancePct,

        ema50DistancePct,

        atr:
            analysis.atr,

        atrPct,

        cmo:
            analysis.cmo,

        volumeRatio:
            analysis.volumeRatio,

        structure:
            analysis.structure,

        breakout:
            analysis.breakout,

        breakoutDistancePct,

        support:
            analysis.support,

        resistance:
            analysis.resistance,

        distanceFromSupportPct,

        distanceToResistancePct,

        fibPosition:
            analysis.fibPosition,

        extension5Pct:
            extension5,

        extension10Pct:
            extension10,

        score:
            analysis.score,

        reasons:
            [
                ...analysis.reasons
            ]
    };
}

// ============================================================
// ENTRY GATE
// ============================================================

function evaluateEntryGate(
    symbol,
    analysis
) {
    const blockers = [];

    const stopInfo =
        recentStop(symbol);

    const confirmations =
        analysis.bullishCandle &&
        analysis.ema20 >
            analysis.ema50 &&
        analysis.cmo >= 50 &&
        analysis.volumeRatio >=
            1.4 &&
        analysis.bodyRatio >=
            0.55 &&
        analysis.structure ===
            'BULLISH';

    if (!CONFIG.paperTrading) {
        blockers.push(
            'PAPER_DISABLED'
        );
    }

    if (manualPause) {
        blockers.push(
            'MANUAL_PAUSE'
        );
    }

    if (tradingPaused) {
        blockers.push(
            'DAILY_RISK_PAUSE'
        );
    }

    if (
        activePositions[
            symbol
        ]
    ) {
        blockers.push(
            'ALREADY_OPEN'
        );
    }

    if (
        Object.keys(
            activePositions
        ).length >=
        CONFIG.maxPositions
    ) {
        blockers.push(
            'MAX_POSITIONS'
        );
    }

    if (
        analysis.score <
        CONFIG.minimumScore
    ) {
        blockers.push(
            'SCORE_LT_MIN'
        );
    }

    if (!confirmations) {
        blockers.push(
            'HARD_CONFIRMATIONS_FAILED'
        );
    }

    if (
        CONFIG.enforceLossCooldown &&
        stopInfo.recentStopLoss
    ) {
        blockers.push(
            'LOSS_COOLDOWN'
        );
    }

    return {
        eligible:
            blockers.length === 0,

        blockers,

        ...stopInfo
    };
}

// ============================================================
// JOURNAL
// ============================================================

function recordOpportunity(
    record
) {
    if (
        record.score <
        CONFIG.journalMinimumScore
    ) {
        return;
    }

    opportunityJournal.push(
        record
    );

    journalStats
        .journaledCandidates++;

    if (
        record.decision ===
        'PAPER_BOUGHT'
    ) {
        journalStats
            .paperEntries++;

    } else if (
        !record.gateEligible
    ) {
        journalStats
            .rejectedCandidates++;
    }

    if (
        opportunityJournal.length >
        CONFIG.journalMaxRecords
    ) {
        opportunityJournal =
            opportunityJournal.slice(
                -CONFIG.journalMaxRecords
            );
    }

    scheduleSave();
}

// ============================================================
// PAPER BUY
// ============================================================

function paperBuy(
    symbol,
    marketPrice,
    analysis,
    context,
    gate
) {
    if (!gate.eligible) {
        if (
            gate.blockers.includes(
                'MANUAL_PAUSE'
            ) ||
            gate.blockers.includes(
                'DAILY_RISK_PAUSE'
            )
        ) {
            return 'PAUSED';
        }

        if (
            gate.blockers.includes(
                'MAX_POSITIONS'
            )
        ) {
            return 'MAX_POSITIONS';
        }

        if (
            gate.blockers.includes(
                'ALREADY_OPEN'
            )
        ) {
            return 'HOLDING';
        }

        return 'WAIT';
    }

    let allocation =
        Math.min(
            currentEquity() /
                CONFIG.maxPositions,

            paperBalance
        );

    if (allocation < 10) {
        return 'NO_BALANCE';
    }

    const entryPrice =
        marketPrice *
        (
            1 +
            CONFIG.slippagePct
        );

    const buyFee =
        allocation *
        CONFIG.feePct;

    const qty =
        (
            allocation -
            buyFee
        ) /
        entryPrice;

    paperBalance -=
        allocation;

    stats.totalFees +=
        buyFee;

    activePositions[
        symbol
    ] = {
        symbol,

        entryPrice,

        qty,

        investedUSDT:
            allocation,

        stopLoss:
            entryPrice *
            (
                1 -
                CONFIG.stopLossPct
            ),

        takeProfit:
            entryPrice *
            (
                1 +
                CONFIG.takeProfitPct
            ),

        lastPrice:
            marketPrice,

        score:
            analysis.score,

        reasons:
            [
                ...analysis.reasons
            ],

        cmo:
            analysis.cmo,

        volumeRatio:
            analysis.volumeRatio,

        structure:
            analysis.structure,

        entryContext:
            context,

        entryTime:
            Date.now()
    };

    scheduleSave();

    console.log(
        `PAPER BUY ${symbol} | Score ${analysis.score}`
    );

    sendTelegramMessage(
        `🟢 <b>PAPER BUY V4.1</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Score: ${analysis.score}/100\n` +
        `Amount: $${allocation.toFixed(2)}\n` +
        `Entry: ${entryPrice.toFixed(8)}\n` +
        `SL: ${activePositions[symbol].stopLoss.toFixed(8)}\n` +
        `TP: ${activePositions[symbol].takeProfit.toFixed(8)}\n` +
        `CMO: ${analysis.cmo.toFixed(2)}\n` +
        `Volume: ${analysis.volumeRatio.toFixed(2)}x\n` +
        `EMA20 dist: ${context.ema20DistancePct.toFixed(2)}%\n` +
        `ATR: ${context.atrPct.toFixed(2)}%`
    );

    return 'PAPER_BOUGHT';
}

// ============================================================
// END OF PART 1
// PART 2 MUST BE PASTED DIRECTLY BELOW THIS LINE
// ============================================================
// ============================================================
// PAPER CLOSE
// ============================================================

function closePaperPosition(
    symbol,
    marketPrice,
    reason
) {
    const trade =
        activePositions[symbol];

    if (!trade) {
        return null;
    }

    const exitPrice =
        marketPrice *
        (1 - CONFIG.slippagePct);

    const grossExit =
        trade.qty *
        exitPrice;

    const sellFee =
        grossExit *
        CONFIG.feePct;

    const netExit =
        grossExit -
        sellFee;

    const profit =
        netExit -
        trade.investedUSDT;

    paperBalance +=
        netExit;

    stats.totalTrades++;

    stats.totalFees +=
        sellFee;

    stats.netProfit +=
        profit;

    if (profit > 0) {
        stats.winningTrades++;

        stats.grossProfit +=
            profit;

        stats.bestTrade =
            Math.max(
                stats.bestTrade,
                profit
            );

    } else {
        stats.losingTrades++;

        stats.grossLoss +=
            Math.abs(profit);

        stats.worstTrade =
            Math.min(
                stats.worstTrade,
                profit
            );
    }

    dailyPnL +=
        profit;

    if (
        reason ===
        'STOP_LOSS'
    ) {
        lastStopLossBySymbol[
            symbol
        ] = Date.now();
    }

    const holdingMs =
        Date.now() -
        trade.entryTime;

    const buyFee =
        trade.investedUSDT *
        CONFIG.feePct;

    tradeHistory.push({
        symbol,

        score:
            trade.score,

        entryPrice:
            trade.entryPrice,

        exitPrice,

        qty:
            trade.qty,

        investedUSDT:
            trade.investedUSDT,

        profit,

        profitPct:
            pct(
                profit,
                trade.investedUSDT
            ),

        reason,

        cmo:
            trade.cmo,

        volumeRatio:
            trade.volumeRatio,

        structure:
            trade.structure,

        reasons:
            trade.reasons,

        buyFee,

        sellFee,

        estimatedTotalFees:
            buyFee +
            sellFee,

        simulatedSlippagePct:
            CONFIG.slippagePct,

        holdingMs,

        holdingMinutes:
            holdingMs /
            60000,

        entryContext:
            trade.entryContext ||
            null,

        exitContext: {
            marketPrice,

            exitPrice,

            timestamp:
                Date.now(),

            isoTime:
                new Date()
                    .toISOString(),

            sessionUTC:
                getSessionUTC(),

            reason
        },

        entryTime:
            trade.entryTime,

        exitTime:
            Date.now()
    });

    if (
        tradeHistory.length >
        CONFIG.tradeHistoryMaxRecords
    ) {
        tradeHistory =
            tradeHistory.slice(
                -CONFIG.tradeHistoryMaxRecords
            );
    }

    delete activePositions[
        symbol
    ];

    updateDrawdown();

    checkDailyLossLimit();

    scheduleSave();

    console.log(
        `PAPER CLOSE ${symbol} | ${reason} | ${profit.toFixed(2)}`
    );

    sendTelegramMessage(
        `${profit >= 0 ? '✅' : '❌'} <b>PAPER CLOSE V4.1</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Reason: ${reason}\n` +
        `PnL: $${profit.toFixed(2)}\n` +
        `Holding: ${(holdingMs / 60000).toFixed(1)} min\n` +
        `Cash: $${paperBalance.toFixed(2)}`
    );

    return profit;
}

// ============================================================
// LIVE POSITION MONITOR
// ============================================================

function manageOpenPosition(
    symbol,
    price
) {
    const position =
        activePositions[
            symbol
        ];

    if (!position) {
        return;
    }

    position.lastPrice =
        price;

    if (
        price <=
        position.stopLoss
    ) {
        closePaperPosition(
            symbol,
            price,
            'STOP_LOSS'
        );

        return;
    }

    if (
        price >=
        position.takeProfit
    ) {
        closePaperPosition(
            symbol,
            price,
            'TAKE_PROFIT'
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
    const candles =
        candleBuffers[
            symbol
        ];

    if (
        !Array.isArray(candles) ||
        candles.length <
            CONFIG.minWarmupCandles
    ) {
        return;
    }

    if (
        lastAnalyzedCloseTime[
            symbol
        ] === closeTime
    ) {
        return;
    }

    lastAnalyzedCloseTime[
        symbol
    ] = closeTime;

    const analysis =
        calculateScore(
            candles
        );

    if (!analysis) {
        return;
    }

    journalStats
        .analyzedCandles++;

    const ticker =
        marketTickers.get(
            symbol
        );

    const marketPrice =
        ticker?.price ||
        candles[
            candles.length - 1
        ].close;

    const context =
        buildMarketContext(
            candles,
            analysis,
            marketPrice,
            closeTime
        );

    const gate =
        evaluateEntryGate(
            symbol,
            analysis
        );

    let decision =
        'WAIT';

    if (
        activePositions[
            symbol
        ]
    ) {
        decision =
            'HOLDING';

    } else {
        decision =
            paperBuy(
                symbol,
                marketPrice,
                analysis,
                context,
                gate
            );
    }

    recordOpportunity({
        symbol,

        score:
            analysis.score,

        decision,

        gateEligible:
            gate.eligible,

        blockers:
            [
                ...gate.blockers
            ],

        price:
            marketPrice,

        cmo:
            analysis.cmo,

        volumeRatio:
            analysis.volumeRatio,

        structure:
            analysis.structure,

        reasons:
            [
                ...analysis.reasons
            ],

        recentStopLoss:
            gate.recentStopLoss,

        minutesSinceStopLoss:
            gate.minutesSinceStopLoss,

        context,

        candleTime:
            closeTime,

        recordedAt:
            Date.now()
    });

    latestResults.push({
        symbol,

        score:
            analysis.score,

        decision,

        price:
            marketPrice,

        cmo:
            analysis.cmo
                .toFixed(2),

        volume:
            analysis.volumeRatio
                .toFixed(2),

        structure:
            analysis.structure,

        ema20DistancePct:
            context
                .ema20DistancePct
                .toFixed(2),

        atrPct:
            context
                .atrPct
                .toFixed(2),

        extension5Pct:
            context
                .extension5Pct
                .toFixed(2),

        blockers:
            gate.blockers
                .join(', '),

        reasons:
            analysis.reasons
                .join(', '),

        candleTime:
            closeTime
    });

    latestResults =
        latestResults
            .sort(
                (a, b) =>
                    b.score -
                    a.score
            )
            .slice(
                0,
                50
            );

    scheduleSave();
}

// ============================================================
// WEBSOCKET CONTROL QUEUE
// ============================================================

const wsControlQueue = [];

let wsControlSending = false;

function queueControlMessage(
    message
) {
    wsControlQueue.push(
        message
    );
}

async function processControlQueue() {
    if (
        wsControlSending ||
        wsControlQueue.length === 0
    ) {
        return;
    }

    if (
        !klineWs ||
        klineWs.readyState !==
            WebSocket.OPEN
    ) {
        return;
    }

    wsControlSending = true;

    const message =
        wsControlQueue.shift();

    try {
        klineWs.send(
            JSON.stringify(
                message
            )
        );

    } catch (error) {
        console.error(
            'WS CONTROL:',
            error.message
        );

    } finally {
        await sleep(
            CONFIG.controlMessageGapMs
        );

        wsControlSending = false;
    }
}

setInterval(
    processControlQueue,
    250
);

// ============================================================
// MINI TICKER WEBSOCKET
// ============================================================

function connectMiniTicker() {
    if (shuttingDown) {
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
        'Connecting MINI TICKER WebSocket...'
    );

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
                'MINI TICKER WebSocket connected.'
            );

            sendTelegramMessage(
                '🟢 <b>Market Price Stream Connected</b>'
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

            } catch (_) {
                return;
            }

            if (
                !Array.isArray(data)
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
                    ignoredSymbol(
                        symbol
                    )
                ) {
                    continue;
                }

                const price =
                    safeNumber(
                        item.c
                    );

                const quoteVolume =
                    safeNumber(
                        item.q
                    );

                if (
                    price <= 0
                ) {
                    continue;
                }

                marketTickers.set(
                    symbol,
                    {
                        price,

                        quoteVolume,

                        updatedAt:
                            Date.now()
                    }
                );

                if (
                    activePositions[
                        symbol
                    ]
                ) {
                    manageOpenPosition(
                        symbol,
                        price
                    );
                }
            }

            if (
                !universeReady &&
                marketTickers.size >
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
                'MINI WebSocket disconnected.'
            );

            scheduleMiniReconnect();
        }
    );

    miniWs.on(
        'error',
        error => {
            console.error(
                'MINI WS:',
                error.message
            );
        }
    );
}

function scheduleMiniReconnect() {
    if (
        shuttingDown ||
        miniReconnectTimer
    ) {
        return;
    }

    miniReconnectTimer =
        setTimeout(
            () => {
                miniReconnectTimer =
                    null;

                connectMiniTicker();
            },
            5000
        );
}

// ============================================================
// KLINE WEBSOCKET
// ============================================================

function connectKlineSocket() {
    if (shuttingDown) {
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
        'Connecting KLINE WebSocket...'
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
                'KLINE WebSocket connected.'
            );

            if (
                subscribedSymbols.size >
                0
            ) {
                queueControlMessage({
                    method:
                        'SUBSCRIBE',

                    params:
                        Array.from(
                            subscribedSymbols
                        ).map(
                            symbol =>
                                `${symbol.toLowerCase()}@kline_${CONFIG.candleInterval}`
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

            } catch (_) {
                return;
            }

            // Subscription ACK
            if (
                Object.prototype
                    .hasOwnProperty.call(
                        event,
                        'result'
                    )
            ) {
                return;
            }

            if (
                event.e !==
                    'kline' ||
                !event.k
            ) {
                return;
            }

            const kline =
                event.k;

            // Closed candles only
            if (
                kline.x !==
                true
            ) {
                return;
            }

            const symbol =
                event.s;

            if (!symbol) {
                return;
            }

            const candle = {
                open:
                    safeNumber(
                        kline.o
                    ),

                high:
                    safeNumber(
                        kline.h
                    ),

                low:
                    safeNumber(
                        kline.l
                    ),

                close:
                    safeNumber(
                        kline.c
                    ),

                volume:
                    safeNumber(
                        kline.v
                    ),

                closeTime:
                    kline.T
            };

            if (
                !candleBuffers[
                    symbol
                ]
            ) {
                candleBuffers[
                    symbol
                ] = [];
            }

            const buffer =
                candleBuffers[
                    symbol
                ];

            const existingIndex =
                buffer.findIndex(
                    c =>
                        c.closeTime ===
                        candle.closeTime
                );

            if (
                existingIndex >= 0
            ) {
                buffer[
                    existingIndex
                ] = candle;

            } else {
                buffer.push(
                    candle
                );
            }

            candleBuffers[
                symbol
            ] =
                buffer
                    .sort(
                        (a, b) =>
                            a.closeTime -
                            b.closeTime
                    )
                    .slice(
                        -CONFIG.maxStoredCandles
                    );

            analyzeClosedCandle(
                symbol,
                candle.closeTime
            );

            scheduleSave();
        }
    );

    klineWs.on(
        'close',
        () => {
            klineConnected =
                false;

            console.log(
                'KLINE WebSocket disconnected.'
            );

            scheduleKlineReconnect();
        }
    );

    klineWs.on(
        'error',
        error => {
            console.error(
                'KLINE WS:',
                error.message
            );
        }
    );
}

function scheduleKlineReconnect() {
    if (
        shuttingDown ||
        klineReconnectTimer
    ) {
        return;
    }

    klineReconnectTimer =
        setTimeout(
            () => {
                klineReconnectTimer =
                    null;

                connectKlineSocket();
            },
            5000
        );
}

// ============================================================
// TOP MARKET UNIVERSE
// ============================================================

function getTopSymbols() {
    const rows = [];

    for (
        const [
            symbol,
            ticker
        ]
        of marketTickers
    ) {
        if (
            !symbol.endsWith(
                'USDT'
            ) ||
            ignoredSymbol(
                symbol
            ) ||
            ticker.quoteVolume <
                CONFIG.minQuoteVolume
        ) {
            continue;
        }

        rows.push({
            symbol,

            quoteVolume:
                ticker.quoteVolume
        });
    }

    return rows
        .sort(
            (a, b) =>
                b.quoteVolume -
                a.quoteVolume
        )
        .slice(
            0,
            CONFIG.universeSize
        )
        .map(
            item =>
                item.symbol
        );
}

// ============================================================
// SUBSCRIPTION REBALANCE
// ============================================================

function rebalanceUniverse() {
    if (
        marketTickers.size ===
        0
    ) {
        return;
    }

    const desired =
        new Set(
            getTopSymbols()
        );

    if (
        desired.size ===
        0
    ) {
        return;
    }

    const toSubscribe = [];
    const toUnsubscribe = [];

    for (
        const symbol
        of desired
    ) {
        if (
            !subscribedSymbols.has(
                symbol
            )
        ) {
            toSubscribe.push(
                symbol
            );
        }
    }

    for (
        const symbol
        of subscribedSymbols
    ) {
        if (
            !desired.has(
                symbol
            )
        ) {
            // Keep symbols with open positions
            if (
                activePositions[
                    symbol
                ]
            ) {
                desired.add(
                    symbol
                );

                continue;
            }

            toUnsubscribe.push(
                symbol
            );
        }
    }

    if (
        toUnsubscribe.length
    ) {
        queueControlMessage({
            method:
                'UNSUBSCRIBE',

            params:
                toUnsubscribe.map(
                    symbol =>
                        `${symbol.toLowerCase()}@kline_${CONFIG.candleInterval}`
                ),

            id:
                Date.now()
        });
    }

    if (
        toSubscribe.length
    ) {
        queueControlMessage({
            method:
                'SUBSCRIBE',

            params:
                toSubscribe.map(
                    symbol =>
                        `${symbol.toLowerCase()}@kline_${CONFIG.candleInterval}`
                ),

            id:
                Date.now() + 1
        });
    }

    subscribedSymbols =
        desired;

    console.log(
        `Universe: ${subscribedSymbols.size} | Added ${toSubscribe.length} | Removed ${toUnsubscribe.length}`
    );

    scheduleSave();
}

setInterval(
    rebalanceUniverse,
    CONFIG.universeRefreshMs
);

// ============================================================
// WEBSOCKET WATCHDOG
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
            miniConnected &&
            lastMiniMessage &&
            now -
                lastMiniMessage >
                90000
        ) {
            console.log(
                'MINI stream stale. Reconnecting.'
            );

            try {
                miniWs.terminate();
            } catch (_) {}
        }

        if (
            klineConnected &&
            lastKlineMessage &&
            now -
                lastKlineMessage >
                10 * 60 * 1000
        ) {
            console.log(
                'KLINE stream stale. Reconnecting.'
            );

            try {
                klineWs.terminate();
            } catch (_) {}
        }

        if (
            !miniConnected
        ) {
            connectMiniTicker();
        }

        if (
            !klineConnected
        ) {
            connectKlineSocket();
        }
    },
    30000
);

// ============================================================
// MANUAL PAUSE / RESUME
// ============================================================

app.post(
    '/api/pause',
    (req, res) => {
        manualPause = true;

        scheduleSave();

        sendTelegramMessage(
            `⏸ <b>NEW ENTRIES PAUSED</b>\n\n` +
            `Market monitoring and data collection continue.`
        );

        res.json({
            success: true,

            msg:
                'New entries paused. Data collection continues.'
        });
    }
);

app.post(
    '/api/resume',
    (req, res) => {
        manualPause = false;

        scheduleSave();

        sendTelegramMessage(
            `▶️ <b>NEW ENTRIES RESUMED</b>`
        );

        res.json({
            success: true,

            msg:
                'Paper entries resumed.'
        });
    }
);

// ============================================================
// EMERGENCY PAPER CLOSE
// ============================================================

app.post(
    '/api/emergency-close',
    (req, res) => {
        const symbols =
            Object.keys(
                activePositions
            );

        if (
            symbols.length ===
            0
        ) {
            return res.json({
                success: false,

                msg:
                    'No open paper positions.'
            });
        }

        let closed = 0;

        for (
            const symbol
            of symbols
        ) {
            const trade =
                activePositions[
                    symbol
                ];

            if (!trade) {
                continue;
            }

            const ticker =
                marketTickers.get(
                    symbol
                );

            const price =
                ticker?.price ||
                trade.lastPrice;

            if (
                !price ||
                price <= 0
            ) {
                continue;
            }

            closePaperPosition(
                symbol,
                price,
                'EMERGENCY_CLOSE'
            );

            closed++;
        }

        res.json({
            success: true,

            msg:
                `Closed ${closed} paper positions.`
        });
    }
);

// ============================================================
// RESET PAPER ACCOUNT
// ============================================================

app.post(
    '/api/reset-paper',
    (req, res) => {
        if (
            Object.keys(
                activePositions
            ).length >
            0
        ) {
            return res
                .status(400)
                .json({
                    success: false,

                    msg:
                        'Close all paper positions before reset.'
                });
        }

        paperBalance =
            CONFIG.startingBalance;

        tradeHistory = [];

        opportunityJournal = [];

        lastStopLossBySymbol = {};

        stats = {
            totalTrades: 0,
            winningTrades: 0,
            losingTrades: 0,

            grossProfit: 0,
            grossLoss: 0,

            netProfit: 0,
            totalFees: 0,

            bestTrade: 0,
            worstTrade: 0,

            maxDrawdown: 0
        };

        journalStats = {
            analyzedCandles: 0,
            journaledCandidates: 0,
            paperEntries: 0,
            rejectedCandidates: 0
        };

        peakEquity =
            CONFIG.startingBalance;

        dailyPnL = 0;

        dailyStartingEquity =
            CONFIG.startingBalance;

        currentDay =
            utcDay();

        tradingPaused = false;

        manualPause = false;

        saveState();

        res.json({
            success: true,

            msg:
                'Paper account and journal reset.'
        });
    }
);

// ============================================================
// JOURNAL EXPORT
// ============================================================

app.get(
    '/api/export',
    (req, res) => {
        res.setHeader(
            'Content-Type',
            'application/json'
        );

        res.setHeader(
            'Content-Disposition',
            `attachment; filename="lomy-journal-${Date.now()}.json"`
        );

        res.send(
            JSON.stringify(
                {
                    version:
                        CONFIG.version,

                    exportedAt:
                        new Date()
                            .toISOString(),

                    configSnapshot: {
                        startingBalance:
                            CONFIG.startingBalance,

                        maxPositions:
                            CONFIG.maxPositions,

                        minimumScore:
                            CONFIG.minimumScore,

                        stopLossPct:
                            CONFIG.stopLossPct,

                        takeProfitPct:
                            CONFIG.takeProfitPct,

                        feePct:
                            CONFIG.feePct,

                        slippagePct:
                            CONFIG.slippagePct,

                        candleInterval:
                            CONFIG.candleInterval,

                        universeSize:
                            CONFIG.universeSize,

                        journalMinimumScore:
                            CONFIG.journalMinimumScore
                    },

                    stats,

                    journalStats,

                    paperBalance,

                    equity:
                        currentEquity(),

                    activePositions,

                    tradeHistory,

                    opportunityJournal,

                    lastStopLossBySymbol
                },
                null,
                2
            )
        );
    }
);

app.get(
    '/api/journal',
    (req, res) => {
        res.json({
            count:
                opportunityJournal.length,

            journal:
                opportunityJournal.slice(
                    -1000
                )
        });
    }
);

// ============================================================
// MAIN DATA API
// ============================================================

app.get(
    '/api/data',
    (req, res) => {
        const closedTrades =
            stats.winningTrades +
            stats.losingTrades;

        const winRate =
            closedTrades > 0
                ? (
                    stats.winningTrades /
                    closedTrades
                  ) * 100
                : 0;

        const profitFactor =
            stats.grossLoss > 0
                ? stats.grossProfit /
                  stats.grossLoss

                : stats.grossProfit > 0
                    ? 999
                    : 0;

        let readySymbols = 0;

        for (
            const symbol
            of subscribedSymbols
        ) {
            if (
                candleBuffers[
                    symbol
                ]?.length >=
                CONFIG.minWarmupCandles
            ) {
                readySymbols++;
            }
        }

        res.json({
            version:
                CONFIG.version,

            mode:
                'WEBSOCKET MARKET / PAPER EXECUTION / DATA COLLECTOR',

            miniConnected,

            klineConnected,

            tickerCount:
                marketTickers.size,

            subscribedSymbols:
                subscribedSymbols.size,

            readySymbols,

            warmupNeeded:
                CONFIG.minWarmupCandles,

            startingBalance:
                CONFIG.startingBalance,

            cashBalance:
                paperBalance.toFixed(2),

            equity:
                currentEquity()
                    .toFixed(2),

            activePositions:
                Object.keys(
                    activePositions
                ).length,

            positions:
                Object.values(
                    activePositions
                ),

            dailyPnL:
                dailyPnL.toFixed(2),

            tradingPaused,

            manualPause,

            journalStats,

            journalRecords:
                opportunityJournal.length,

            stats: {
                ...stats,

                winRate:
                    Number(
                        winRate.toFixed(2)
                    ),

                profitFactor:
                    Number(
                        profitFactor.toFixed(2)
                    )
            },

            live:
                latestResults,

            history:
                tradeHistory.slice(
                    -100
                )
        });
    }
);

// ============================================================
// HEALTH
// ============================================================

app.get(
    '/health',
    (req, res) => {
        res.json({
            status:
                miniConnected &&
                klineConnected
                    ? 'OK'
                    : 'DEGRADED',

            version:
                CONFIG.version,

            execution:
                'PAPER ONLY',

            restRequests:
                0,

            miniWebSocket:
                miniConnected,

            klineWebSocket:
                klineConnected,

            marketSymbols:
                marketTickers.size,

            subscribedSymbols:
                subscribedSymbols.size,

            openPositions:
                Object.keys(
                    activePositions
                ).length,

            manualPause,

            journalRecords:
                opportunityJournal.length,

            equity:
                currentEquity()
                    .toFixed(2)
        });
    }
);

// ============================================================
// DASHBOARD
// ============================================================

app.get(
    '/',
    (req, res) => {
        res.send(`
<!DOCTYPE html>

<html>

<head>

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1">

<title>LOMY V4.1</title>

<style>

*{
box-sizing:border-box
}

body{
margin:0;
background:#0b0e11;
color:#eaecef;
font-family:Arial,sans-serif;
text-align:center;
padding:18px
}

h1{
color:#f3ba2f
}

.badge{
display:inline-block;
background:#f3ba2f;
color:#000;
font-weight:bold;
padding:9px 16px;
border-radius:8px
}

.status{
margin:14px;
font-weight:bold
}

.grid{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
gap:10px;
max-width:1350px;
margin:18px auto
}

.card{
background:#1e2329;
border:1px solid #2b3139;
padding:15px;
border-radius:9px
}

.label{
color:#848e9c;
font-size:11px
}

.value{
font-size:21px;
font-weight:bold;
margin-top:6px
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
color:#fff;
border:0;
padding:12px 18px;
border-radius:7px;
font-weight:bold;
cursor:pointer;
margin:7px
}

.danger{
background:#f6465d
}

.pause{
background:#f0b90b;
color:#000
}

.resume{
background:#0ecb81
}

.export{
background:#3772ff
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
padding:9px;
border-bottom:1px solid #2b3139;
font-size:12px
}

</style>

</head>

<body>

<h1>
🤖 LOMY PRECISION V4.1
</h1>

<div class="badge">
WEBSOCKET • PAPER ONLY • ADVANCED DATA COLLECTOR
</div>

<div
class="status"
id="connection">
Connecting...
</div>

<div
class="status"
id="tradeState">
Checking entry state...
</div>

<div class="grid">

<div class="card">
<div class="label">START</div>
<div class="value">$10,000</div>
</div>

<div class="card">
<div class="label">CASH</div>
<div class="value" id="cash">$0</div>
</div>

<div class="card">
<div class="label">EQUITY</div>
<div class="value" id="equity">$0</div>
</div>

<div class="card">
<div class="label">CLOSED TRADES</div>
<div class="value" id="trades">0</div>
</div>

<div class="card">
<div class="label">WIN RATE</div>
<div class="value" id="winrate">0%</div>
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
<div class="label">MAX DRAWDOWN</div>
<div class="value" id="dd">0%</div>
</div>

<div class="card">
<div class="label">OPEN POSITIONS</div>
<div class="value" id="positions">0</div>
</div>

<div class="card">
<div class="label">WS SYMBOLS</div>
<div class="value" id="symbols">0</div>
</div>

<div class="card">
<div class="label">READY SYMBOLS</div>
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

</div>

<button
class="pause"
onclick="pauseTrading()">
⏸ PAUSE NEW ENTRIES
</button>

<button
class="resume"
onclick="resumeTrading()">
▶ RESUME ENTRIES
</button>

<button
class="danger"
onclick="emergencyClose()">
🚨 CLOSE ALL PAPER POSITIONS
</button>

<button
class="export"
onclick="window.location='/api/export'">
⬇ EXPORT JOURNAL JSON
</button>

<div style="overflow-x:auto">

<table>

<thead>

<tr>

<th>Symbol</th>
<th>Score</th>
<th>Status</th>
<th>CMO</th>
<th>Volume</th>
<th>Structure</th>
<th>EMA20 Dist</th>
<th>ATR%</th>
<th>5C Ext%</th>
<th>Blockers</th>
<th>Price</th>

</tr>

</thead>

<tbody id="table">

<tr>
<td colspan="11">
Collecting WebSocket candles...
</td>
</tr>

</tbody>

</table>

</div>

<script>

async function loadData(){

try{

const response =
await fetch('/api/data');

const data =
await response.json();

document.getElementById(
'cash'
).innerText =
'$' + data.cashBalance;

document.getElementById(
'equity'
).innerText =
'$' + data.equity;

document.getElementById(
'trades'
).innerText =
data.stats.totalTrades;

document.getElementById(
'winrate'
).innerText =
data.stats.winRate + '%';

document.getElementById(
'profit'
).innerText =
'$' +
Number(
data.stats.netProfit
).toFixed(2);

document.getElementById(
'pf'
).innerText =
data.stats.profitFactor;

document.getElementById(
'dd'
).innerText =
Number(
data.stats.maxDrawdown
).toFixed(2) +
'%';

document.getElementById(
'positions'
).innerText =
data.activePositions;

document.getElementById(
'symbols'
).innerText =
data.subscribedSymbols;

document.getElementById(
'ready'
).innerText =
data.readySymbols;

document.getElementById(
'journal'
).innerText =
data.journalRecords;

document.getElementById(
'daily'
).innerText =
'$' + data.dailyPnL;

const connection =
document.getElementById(
'connection'
);

if(
data.miniConnected &&
data.klineConnected
){

connection.innerText =
'🟢 MARKET WEBSOCKETS LIVE • REST REQUESTS = 0';

connection.className =
'status green';

}else{

connection.innerText =
'🔴 WebSocket reconnecting...';

connection.className =
'status red';

}

const tradeState =
document.getElementById(
'tradeState'
);

if(
data.manualPause
){

tradeState.innerText =
'⏸ NEW ENTRIES PAUSED • DATA COLLECTION CONTINUES';

tradeState.className =
'status yellow';

}else if(
data.tradingPaused
){

tradeState.innerText =
'🛑 DAILY RISK PAUSE';

tradeState.className =
'status red';

}else{

tradeState.innerText =
'▶ PAPER ENTRIES ENABLED';

tradeState.className =
'status green';

}

const tbody =
document.getElementById(
'table'
);

tbody.innerHTML = '';

if(
!data.live.length
){

tbody.innerHTML =
'<tr><td colspan="11">Warm-up / collecting candidates. Ready: ' +
data.readySymbols +
' / ' +
data.subscribedSymbols +
'</td></tr>';

return;

}

data.live.forEach(
item => {

tbody.innerHTML +=

'<tr>' +

'<td><b>' +
item.symbol +
'</b></td>' +

'<td class="yellow">' +
item.score +
'</td>' +

'<td>' +
item.decision +
'</td>' +

'<td>' +
item.cmo +
'</td>' +

'<td>' +
item.volume +
'x</td>' +

'<td>' +
item.structure +
'</td>' +

'<td>' +
item.ema20DistancePct +
'%</td>' +

'<td>' +
item.atrPct +
'%</td>' +

'<td>' +
item.extension5Pct +
'%</td>' +

'<td>' +
item.blockers +
'</td>' +

'<td>' +
item.price +
'</td>' +

'</tr>';

});

}catch(error){

console.error(
error
);

}

}

async function pauseTrading(){

const response =
await fetch(
'/api/pause',
{
method:'POST'
}
);

const data =
await response.json();

alert(
data.msg
);

loadData();

}

async function resumeTrading(){

const response =
await fetch(
'/api/resume',
{
method:'POST'
}
);

const data =
await response.json();

alert(
data.msg
);

loadData();

}

async function emergencyClose(){

if(
!confirm(
'Close all PAPER positions?'
)
){
return;
}

const response =
await fetch(
'/api/emergency-close',
{
method:'POST'
}
);

const data =
await response.json();

alert(
data.msg
);

loadData();

}

setInterval(
loadData,
3000
);

loadData();

</script>

</body>

</html>
        `);
    }
);

// ============================================================
// GRACEFUL SHUTDOWN
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
        `${signal}: saving state`
    );

    saveState();

    try {
        miniWs?.close();
    } catch (_) {}

    try {
        klineWs?.close();
    } catch (_) {}

    setTimeout(
        () =>
            process.exit(0),
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

        console.log(
            '=========================================='
        );

        console.log(
            'LOMY PRECISION ENGINE V4.1'
        );

        console.log(
            'Mode: ADVANCED DATA COLLECTOR'
        );

        console.log(
            'Market Data: BINANCE WEBSOCKET ONLY'
        );

        console.log(
            'REST Scanner: DISABLED'
        );

        console.log(
            'Execution: PAPER ONLY'
        );

        console.log(
            `Universe: TOP ${CONFIG.universeSize}`
        );

        console.log(
            `Entry Score: ${CONFIG.minimumScore}+`
        );

        console.log(
            `Journal Candidates: Score ${CONFIG.journalMinimumScore}+`
        );

        console.log(
            `Warmup: ${CONFIG.minWarmupCandles} CLOSED 5m candles`
        );

        console.log(
            '=========================================='
        );

        loadState();

        connectMiniTicker();

        connectKlineSocket();

        sendTelegramMessage(
            `🚀 <b>LOMY V4.1 STARTED</b>\n\n` +
            `Mode: <b>ADVANCED DATA COLLECTOR</b>\n` +
            `Market Data: <b>WEBSOCKET ONLY</b>\n` +
            `REST Scanner: <b>OFF</b>\n` +
            `Execution: <b>PAPER ONLY</b>\n` +
            `Balance: $${currentEquity().toFixed(2)}\n` +
            `Universe: Top ${CONFIG.universeSize}\n` +
            `Entry Score: ${CONFIG.minimumScore}+`
        );
    }
);
