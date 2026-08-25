require('dotenv').config();

const express = require('express');
const axios = require('axios');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

// ============================================================
// LOMY PRECISION ENGINE V4
// REAL BINANCE MARKET DATA
// WEBSOCKET ONLY
// PAPER EXECUTION ONLY
// ============================================================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// Binance market-data-only websocket endpoint
const WS_BASE = 'wss://data-stream.binance.vision';

// ============================================================
// CONFIG
// ============================================================

const CONFIG = {
    paperTrading: true,

    startingBalance: 10000,

    maxPositions: 10,

    minimumScore: 80,

    // Risk
    stopLossPct: 0.01,
    takeProfitPct: 0.02,
    dailyLossLimitPct: 0.10,

    // Simulated execution costs
    feePct: 0.001,
    slippagePct: 0.0005,

    // Market
    candleInterval: '5m',

    // We need >= 50 closed candles for EMA50.
    minWarmupCandles: 55,
    maxStoredCandles: 70,

    // Keep well below Binance's 1024-stream limit.
    universeSize: 300,

    minQuoteVolume: 250000,

    // Re-evaluate top market universe every 30 minutes.
    universeRefreshMs: 30 * 60 * 1000,

    // One websocket control message per second.
    // This is deliberately conservative.
    controlMessageGapMs: 1000,

    stateFile: path.join(__dirname, 'paper-state.json')
};

// ============================================================
// GLOBAL STATE
// ============================================================

let paperBalance = CONFIG.startingBalance;

let activePositions = {};

let tradeHistory = [];

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

let peakEquity = CONFIG.startingBalance;

let dailyPnL = 0;

let dailyStartingEquity = CONFIG.startingBalance;

let currentDay = getUtcDay();

let tradingPaused = false;

// ------------------------------------------------------------
// MARKET CACHE
// ------------------------------------------------------------

const marketTickers = new Map();

const candleBuffers = {};

const lastAnalyzedCloseTime = {};

let subscribedSymbols = new Set();

let latestResults = [];

// ------------------------------------------------------------
// WEBSOCKET STATE
// ------------------------------------------------------------

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

function getUtcDay() {
    return new Date().toISOString().slice(0, 10);
}

function stableIgnored(symbol) {
    const ignored = new Set([
        'USDCUSDT',
        'FDUSDUSDT',
        'TUSDUSDT',
        'USDPUSDT',
        'BUSDUSDT',
        'DAIUSDT',
        'USDEUSDT'
    ]);

    return ignored.has(symbol);
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
        const persistedBuffers = {};

        for (const symbol of Object.keys(candleBuffers)) {
            if (!Array.isArray(candleBuffers[symbol])) continue;

            persistedBuffers[symbol] =
                candleBuffers[symbol].slice(-CONFIG.maxStoredCandles);
        }

        const state = {
            paperBalance,

            activePositions,

            tradeHistory:
                tradeHistory.slice(-2000),

            stats,

            peakEquity,

            dailyPnL,

            dailyStartingEquity,

            currentDay,

            tradingPaused,

            candleBuffers:
                persistedBuffers,

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

        const raw =
            fs.readFileSync(
                CONFIG.stateFile,
                'utf8'
            );

        const state =
            JSON.parse(raw);

        paperBalance =
            safeNumber(
                state.paperBalance,
                CONFIG.startingBalance
            );

        activePositions =
            state.activePositions || {};

        tradeHistory =
            state.tradeHistory || [];

        stats = {
            ...stats,
            ...(state.stats || {})
        };

        peakEquity =
            safeNumber(
                state.peakEquity,
                CONFIG.startingBalance
            );

        dailyPnL =
            safeNumber(
                state.dailyPnL
            );

        dailyStartingEquity =
            safeNumber(
                state.dailyStartingEquity,
                CONFIG.startingBalance
            );

        currentDay =
            state.currentDay ||
            getUtcDay();

        tradingPaused =
            Boolean(
                state.tradingPaused
            );

        if (state.candleBuffers) {
            for (
                const [symbol, candles]
                of Object.entries(
                    state.candleBuffers
                )
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
            `State restored | Cash $${paperBalance.toFixed(2)} | Open ${Object.keys(activePositions).length}`
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
    if (!TELEGRAM_TOKEN || !CHAT_ID) return;

    telegramQueue.push(text);
}

async function processTelegramQueue() {
    if (
        telegramSending ||
        telegramQueue.length === 0
    ) {
        return;
    }

    telegramSending = true;

    const text =
        telegramQueue.shift();

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
        const ticker =
            marketTickers.get(
                position.symbol
            );

        const price =
            ticker?.price ||
            position.lastPrice ||
            position.entryPrice;

        equity +=
            position.qty * price;
    }

    return equity;
}

function updateDrawdown() {
    const equity =
        currentEquity();

    if (equity > peakEquity) {
        peakEquity = equity;
    }

    const drawdown =
        peakEquity > 0
            ? (
                (
                    peakEquity -
                    equity
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
// DAILY RISK
// ============================================================

function checkDailyReset() {
    const today =
        getUtcDay();

    if (today === currentDay) return;

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
            `🛑 <b>DAILY LOSS LIMIT</b>\n\n` +
            `Daily PnL: $${dailyPnL.toFixed(2)}\n` +
            `Maximum: -$${limit.toFixed(2)}\n\n` +
            `New entries have been stopped.`
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

function calculateSMA(
    data,
    period,
    key
) {
    if (data.length < period) {
        return null;
    }

    let sum = 0;

    for (
        let i =
            data.length - period;
        i < data.length;
        i++
    ) {
        sum += data[i][key];
    }

    return sum / period;
}

function calculateEMA(
    data,
    period,
    key
) {
    if (data.length < period) {
        return null;
    }

    const multiplier =
        2 / (period + 1);

    let ema = 0;

    for (let i = 0; i < period; i++) {
        ema += data[i][key];
    }

    ema /= period;

    for (
        let i = period;
        i < data.length;
        i++
    ) {
        ema =
            (
                data[i][key] -
                ema
            ) *
            multiplier +
            ema;
    }

    return ema;
}

function calculateCMO(
    data,
    period
) {
    if (
        data.length <
        period + 1
    ) {
        return null;
    }

    let up = 0;
    let down = 0;

    const start =
        data.length -
        period;

    for (
        let i = start;
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

    return total === 0
        ? 0
        : 100 *
          (
              (
                  up -
                  down
              ) /
              total
          );
}

function calculateATR(
    data,
    period
) {
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
        const high =
            data[i].high;

        const low =
            data[i].low;

        const previousClose =
            data[i - 1].close;

        ranges.push(
            Math.max(
                high - low,
                Math.abs(
                    high -
                    previousClose
                ),
                Math.abs(
                    low -
                    previousClose
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
            (a, b) =>
                a + b,
            0
        ) /
        recent.length
    );
}

// ============================================================
// MARKET STRUCTURE
// ============================================================

function getStructure(candles) {
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
        last.high >
            previous.high &&
        last.low >
            previous.low
    ) {
        return 'BULLISH';
    }

    if (
        last.high <
            previous.high &&
        last.low <
            previous.low
    ) {
        return 'BEARISH';
    }

    return 'NEUTRAL';
}

// ============================================================
// SUPPORT / RESISTANCE
// ============================================================

function getSupportResistance(
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
// FIBONACCI
// ============================================================

function getFibScore(candles) {
    const recent =
        candles.slice(-30);

    if (recent.length < 20) {
        return 0;
    }

    const high =
        Math.max(
            ...recent.map(
                x => x.high
            )
        );

    const low =
        Math.min(
            ...recent.map(
                x => x.low
            )
        );

    const range =
        high - low;

    if (range <= 0) return 0;

    const close =
        candles[
            candles.length - 1
        ].close;

    const position =
        (
            close -
            low
        ) /
        range;

    if (
        position >= 0.382 &&
        position <= 0.618
    ) {
        return 5;
    }

    if (
        position >= 0.30 &&
        position <= 0.70
    ) {
        return 3;
    }

    return 0;
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
        calculateEMA(
            candles,
            20,
            'close'
        );

    const ema50 =
        calculateEMA(
            candles,
            50,
            'close'
        );

    const volumeSMA =
        calculateSMA(
            candles,
            20,
            'volume'
        );

    const cmo =
        calculateCMO(
            candles,
            9
        );

    const atr =
        calculateATR(
            candles,
            14
        );

    if (
        ema20 === null ||
        ema50 === null ||
        volumeSMA === null ||
        cmo === null ||
        atr === null
    ) {
        return null;
    }

    let score = 0;

    const reasons = [];

    // --------------------------------------------------------
    // Trend
    // --------------------------------------------------------

    if (
        candle.close >
        ema20
    ) {
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

    // --------------------------------------------------------
    // Candle strength
    // --------------------------------------------------------

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

    // --------------------------------------------------------
    // Volume + liquidity
    // --------------------------------------------------------

    const volumeRatio =
        volumeSMA > 0
            ? candle.volume /
              volumeSMA
            : 0;

    if (
        volumeRatio >= 1.4
    ) {
        score += 15;
        reasons.push(
            'VOLUME_SPIKE'
        );
    }

    if (
        volumeRatio >= 1.2
    ) {
        score += 10;
        reasons.push(
            'LIQUIDITY'
        );
    }

    // --------------------------------------------------------
    // Momentum
    // --------------------------------------------------------

    if (cmo >= 50) {
        score += 15;
        reasons.push('CMO');
    }

    // --------------------------------------------------------
    // Structure
    // --------------------------------------------------------

    const structure =
        getStructure(
            candles
        );

    if (
        structure ===
        'BULLISH'
    ) {
        score += 10;
        reasons.push(
            'STRUCTURE'
        );
    }

    // --------------------------------------------------------
    // Fibonacci
    // --------------------------------------------------------

    const fibScore =
        getFibScore(
            candles
        );

    score += fibScore;

    if (fibScore > 0) {
        reasons.push('FIB');
    }

    // --------------------------------------------------------
    // ATR
    // --------------------------------------------------------

    if (
        range >=
        atr * 0.8
    ) {
        score += 5;
        reasons.push('ATR');
    }

    // --------------------------------------------------------
    // Support / Resistance
    // --------------------------------------------------------

    const sr =
        getSupportResistance(
            candles,
            20
        );

    let breakout = false;

    if (sr) {
        const previousCandles =
            candles.slice(
                -21,
                -1
            );

        const previousResistance =
            Math.max(
                ...previousCandles.map(
                    c => c.high
                )
            );

        breakout =
            candle.close >
            previousResistance;

        if (breakout) {
            score += 5;
            reasons.push(
                'BREAKOUT'
            );
        }
    }

    return {
        score:
            Math.min(
                score,
                100
            ),

        reasons,

        ema20,
        ema50,

        cmo,

        atr,

        volumeRatio,

        bodyRatio,

        bullishCandle,

        structure,

        breakout,

        support:
            sr?.support || 0,

        resistance:
            sr?.resistance || 0
    };
}

// ============================================================
// PAPER BUY
// ============================================================

function paperBuy(
    symbol,
    marketPrice,
    analysis
) {
    if (
        !CONFIG.paperTrading
    ) {
        return 'DISABLED';
    }

    if (
        tradingPaused
    ) {
        return 'PAUSED';
    }

    if (
        activePositions[
            symbol
        ]
    ) {
        return 'HOLDING';
    }

    if (
        Object.keys(
            activePositions
        ).length >=
        CONFIG.maxPositions
    ) {
        return 'MAX_POSITIONS';
    }

    // --------------------------------------------------------
    // Hard confirmations
    // Score alone is NOT enough
    // --------------------------------------------------------

    const requiredConditions =
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

    if (
        analysis.score <
            CONFIG.minimumScore ||
        !requiredConditions
    ) {
        return 'WAIT';
    }

    const equity =
        currentEquity();

    let allocation =
        equity /
        CONFIG.maxPositions;

    allocation =
        Math.min(
            allocation,
            paperBalance
        );

    if (
        allocation < 10
    ) {
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

    const usable =
        allocation -
        buyFee;

    const qty =
        usable /
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
            analysis.reasons,

        cmo:
            analysis.cmo,

        volumeRatio:
            analysis.volumeRatio,

        structure:
            analysis.structure,

        entryTime:
            Date.now()
    };

    scheduleSave();

    console.log(
        `PAPER BUY ${symbol} | Score ${analysis.score}`
    );

    sendTelegramMessage(
        `🟢 <b>PAPER BUY</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Score: ${analysis.score}/100\n` +
        `Amount: $${allocation.toFixed(2)}\n` +
        `Entry: ${entryPrice.toFixed(8)}\n` +
        `SL: ${activePositions[symbol].stopLoss.toFixed(8)}\n` +
        `TP: ${activePositions[symbol].takeProfit.toFixed(8)}\n` +
        `CMO: ${analysis.cmo.toFixed(2)}\n` +
        `Volume: ${analysis.volumeRatio.toFixed(2)}x`
    );

    return 'PAPER_BOUGHT';
}

// ============================================================
// PAPER CLOSE
// ============================================================

function closePaperPosition(
    symbol,
    marketPrice,
    reason
) {
    const trade =
        activePositions[
            symbol
        ];

    if (!trade) return null;

    const exitPrice =
        marketPrice *
        (
            1 -
            CONFIG.slippagePct
        );

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

    tradeHistory.push({
        symbol,

        score:
            trade.score,

        entryPrice:
            trade.entryPrice,

        exitPrice,

        qty:
            trade.qty,

        profit,

        reason,

        cmo:
            trade.cmo,

        volumeRatio:
            trade.volumeRatio,

        structure:
            trade.structure,

        reasons:
            trade.reasons,

        entryTime:
            trade.entryTime,

        exitTime:
            Date.now()
    });

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
        `${profit >= 0 ? '✅' : '❌'} <b>PAPER CLOSE</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Reason: ${reason}\n` +
        `PnL: $${profit.toFixed(2)}\n` +
        `Cash: $${paperBalance.toFixed(2)}`
    );

    return profit;
}

// ============================================================
// LIVE POSITION MONITOR
// Runs from miniTicker WebSocket
// ============================================================

function manageOpenPosition(
    symbol,
    price
) {
    const position =
        activePositions[
            symbol
        ];

    if (!position) return;

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

    if (!analysis) return;

    const ticker =
        marketTickers.get(
            symbol
        );

    const currentPrice =
        ticker?.price ||
        candles[
            candles.length - 1
        ].close;

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
                currentPrice,
                analysis
            );
    }

    latestResults.push({
        symbol,

        score:
            analysis.score,

        decision,

        price:
            currentPrice,

        cmo:
            analysis.cmo.toFixed(2),

        volume:
            analysis.volumeRatio.toFixed(2),

        structure:
            analysis.structure,

        reasons:
            analysis.reasons.join(
                ', '
            ),

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
// CONTROL MESSAGE QUEUE
// Prevent websocket-control flooding
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
    if (
        shuttingDown
    ) return;

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
            miniConnected = true;
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
                const item of data
            ) {
                const symbol =
                    item.s;

                if (
                    !symbol ||
                    !symbol.endsWith(
                        'USDT'
                    ) ||
                    stableIgnored(
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

                if (price <= 0) {
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
    if (
        shuttingDown
    ) return;

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

            // Resubscribe after reconnect.
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

            // Only use fully closed candles.
            if (
                kline.x !== true
            ) {
                return;
            }

            const symbol =
                event.s;

            if (!symbol) return;

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
                buffer.push(candle);
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
            stableIgnored(
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
        desired.size === 0
    ) {
        return;
    }

    const toSubscribe =
        [];

    const toUnsubscribe =
        [];

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
            // Keep open-position symbols subscribed.
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
        ) return;

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
            symbols.length === 0
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

            if (!trade) continue;

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
            return res.status(400).json({
                success: false,
                msg:
                    'Close all paper positions before reset.'
            });
        }

        paperBalance =
            CONFIG.startingBalance;

        tradeHistory = [];

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

        peakEquity =
            CONFIG.startingBalance;

        dailyPnL = 0;

        dailyStartingEquity =
            CONFIG.startingBalance;

        currentDay =
            getUtcDay();

        tradingPaused =
            false;

        saveState();

        res.json({
            success: true,
            msg:
                'Paper account reset to $10,000.'
        });
    }
);

// ============================================================
// API
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
                : stats.grossProfit >
                  0
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
            mode:
                'WEBSOCKET MARKET / PAPER EXECUTION',

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
                currentEquity().toFixed(2),

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

<title>LOMY V4</title>

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
grid-template-columns:
repeat(auto-fit,minmax(150px,1fr));
gap:10px;
max-width:1250px;
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
background:#f6465d;
color:#fff;
border:0;
padding:12px 20px;
border-radius:7px;
font-weight:bold;
cursor:pointer;
margin:10px
}

table{
width:100%;
max-width:1250px;
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
🤖 LOMY PRECISION V4
</h1>

<div class="badge">
100% WEBSOCKET MARKET DATA • PAPER ONLY
</div>

<div
class="status"
id="connection">
Connecting...
</div>

<div class="grid">

<div class="card">
<div class="label">
START
</div>
<div class="value">
$10,000
</div>
</div>

<div class="card">
<div class="label">
CASH
</div>
<div
class="value"
id="cash">
$0
</div>
</div>

<div class="card">
<div class="label">
EQUITY
</div>
<div
class="value"
id="equity">
$0
</div>
</div>

<div class="card">
<div class="label">
CLOSED TRADES
</div>
<div
class="value"
id="trades">
0
</div>
</div>

<div class="card">
<div class="label">
WIN RATE
</div>
<div
class="value"
id="winrate">
0%
</div>
</div>

<div class="card">
<div class="label">
NET PROFIT
</div>
<div
class="value"
id="profit">
$0
</div>
</div>

<div class="card">
<div class="label">
PROFIT FACTOR
</div>
<div
class="value"
id="pf">
0
</div>
</div>

<div class="card">
<div class="label">
MAX DRAWDOWN
</div>
<div
class="value"
id="dd">
0%
</div>
</div>

<div class="card">
<div class="label">
OPEN POSITIONS
</div>
<div
class="value"
id="positions">
0
</div>
</div>

<div class="card">
<div class="label">
WS SYMBOLS
</div>
<div
class="value"
id="symbols">
0
</div>
</div>

<div class="card">
<div class="label">
READY SYMBOLS
</div>
<div
class="value"
id="ready">
0
</div>
</div>

<div class="card">
<div class="label">
TODAY PNL
</div>
<div
class="value"
id="daily">
$0
</div>
</div>

</div>

<button
onclick="emergencyClose()">
🚨 CLOSE ALL PAPER POSITIONS
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
<th>Price</th>
</tr>
</thead>

<tbody id="table">

<tr>
<td colspan="7">
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
data.stats.netProfit.toFixed(2);

document.getElementById(
'pf'
).innerText =
data.stats.profitFactor;

document.getElementById(
'dd'
).innerText =
data.stats.maxDrawdown.toFixed(2) +
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

const tbody =
document.getElementById(
'table'
);

tbody.innerHTML='';

if(!data.live.length){

tbody.innerHTML =
'<tr><td colspan="7">Warm-up in progress. Ready: ' +
data.readySymbols +
' / ' +
data.subscribedSymbols +
'</td></tr>';

return;

}

data.live.forEach(item=>{

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
item.price +
'</td>' +

'</tr>';

});

}catch(error){

console.error(error);

}

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

alert(data.msg);

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

function shutdown(signal) {
    if (shuttingDown) return;

    shuttingDown = true;

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
        () => process.exit(0),
        1000
    );
}

process.on(
    'SIGTERM',
    () =>
        shutdown('SIGTERM')
);

process.on(
    'SIGINT',
    () =>
        shutdown('SIGINT')
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
    async () => {
        console.log('');
        console.log(
            '=========================================='
        );
        console.log(
            'LOMY PRECISION ENGINE V4'
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
            `Warmup: ${CONFIG.minWarmupCandles} CLOSED 5m candles`
        );
        console.log(
            '=========================================='
        );

        loadState();

        connectMiniTicker();

        connectKlineSocket();

        sendTelegramMessage(
            `🚀 <b>LOMY V4 STARTED</b>\n\n` +
            `Market Data: <b>WEBSOCKET ONLY</b>\n` +
            `REST Scanner: <b>OFF</b>\n` +
            `Execution: <b>PAPER ONLY</b>\n` +
            `Balance: $${currentEquity().toFixed(2)}\n` +
            `Universe: Top ${CONFIG.universeSize}\n` +
            `Entry Score: ${CONFIG.minimumScore}+`
        );
    }
);
