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
// 🌍 MARKET
// ============================================================

const BINANCE_REST = 'https://api.binance.com';

const BINANCE_WS =
    'wss://stream.binance.com:9443/ws/!miniTicker@arr';

// ============================================================
// 📲 TELEGRAM
// ============================================================

const TELEGRAM_TOKEN =
    process.env.TELEGRAM_TOKEN;

const CHAT_ID =
    process.env.TELEGRAM_CHAT_ID;

// ============================================================
// ⚙️ CONFIG
// ============================================================

const CONFIG = {

    paperTrading: true,

    startingBalance: 10000,

    maxPositions: 10,

    minimumScore: 80,

    // ------------------------------------
    // Risk
    // ------------------------------------

    stopLossPct: 0.01,

    takeProfitPct: 0.02,

    dailyLossLimitPct: 0.10,

    // ------------------------------------
    // Simulation costs
    // ------------------------------------

    feePct: 0.001,

    slippagePct: 0.0005,

    // ------------------------------------
    // Candle analysis
    // ------------------------------------

    candleInterval: '5m',

    candleLimit: 100,

    // ------------------------------------
    // Scanner
    // ------------------------------------

    universeSize: 300,

    batchSize: 20,

    scannerIntervalMs: 30000,

    minQuoteVolume: 250000,

    // ------------------------------------
    // REST protection
    // ------------------------------------

    restMinimumGapMs: 400,

    // ------------------------------------
    // State
    // ------------------------------------

    stateFile:
        path.join(
            __dirname,
            'paper-state.json'
        )
};

// ============================================================
// 🧠 STATE
// ============================================================

let validSymbols =
    new Set();

let marketTickers =
    new Map();

let paperBalance =
    CONFIG.startingBalance;

let activePositions = {};

let tradeHistory = [];

let latestResults = [];

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

let peakEquity =
    CONFIG.startingBalance;

let dailyPnL = 0;

let dailyStartingEquity =
    CONFIG.startingBalance;

let currentDay =
    new Date()
        .toISOString()
        .slice(0, 10);

let tradingPaused = false;

let scannerRunning = false;

let currentCoinIndex = 0;

let lastScanTime = null;

let websocketConnected = false;

let lastWebsocketMessage = null;

let websocketReconnectTimer = null;

let ws = null;

// ============================================================
// 🛡️ REST RATE LIMIT STATE
// ============================================================

let lastRestRequestTime = 0;

let restBlockedUntil = 0;

let consecutiveRestErrors = 0;

// ============================================================
// 🛠️ HELPERS
// ============================================================

function sleep(ms) {

    return new Promise(
        resolve =>
            setTimeout(resolve, ms)
    );
}

function safeNumber(
    value,
    fallback = 0
) {

    const number =
        Number(value);

    return Number.isFinite(number)
        ? number
        : fallback;
}

function utcDay() {

    return new Date()
        .toISOString()
        .slice(0, 10);
}

// ============================================================
// 💾 SAVE / LOAD STATE
// ============================================================

function saveState() {

    try {

        const data = {

            paperBalance,

            activePositions,

            tradeHistory:
                tradeHistory.slice(-2000),

            stats,

            peakEquity,

            dailyPnL,

            dailyStartingEquity,

            currentDay,

            tradingPaused
        };

        fs.writeFileSync(
            CONFIG.stateFile,
            JSON.stringify(
                data,
                null,
                2
            )
        );

    } catch (error) {

        console.error(
            '❌ State save:',
            error.message
        );
    }
}

function loadState() {

    try {

        if (
            !fs.existsSync(
                CONFIG.stateFile
            )
        ) {

            console.log(
                'ℹ️ Starting fresh paper account.'
            );

            return;
        }

        const data =
            JSON.parse(
                fs.readFileSync(
                    CONFIG.stateFile,
                    'utf8'
                )
            );

        paperBalance =
            safeNumber(
                data.paperBalance,
                CONFIG.startingBalance
            );

        activePositions =
            data.activePositions || {};

        tradeHistory =
            data.tradeHistory || [];

        stats = {
            ...stats,
            ...(data.stats || {})
        };

        peakEquity =
            safeNumber(
                data.peakEquity,
                CONFIG.startingBalance
            );

        dailyPnL =
            safeNumber(
                data.dailyPnL
            );

        dailyStartingEquity =
            safeNumber(
                data.dailyStartingEquity,
                CONFIG.startingBalance
            );

        currentDay =
            data.currentDay ||
            currentDay;

        tradingPaused =
            Boolean(
                data.tradingPaused
            );

        console.log(
            `🔄 Paper state restored | Cash $${paperBalance.toFixed(2)} | Positions ${Object.keys(activePositions).length}`
        );

    } catch (error) {

        console.error(
            '❌ State restore:',
            error.message
        );
    }
}

// ============================================================
// 📲 TELEGRAM QUEUE
// ============================================================

const telegramQueue = [];

let telegramSending = false;

function sendTelegramMessage(text) {

    if (
        !TELEGRAM_TOKEN ||
        !CHAT_ID
    ) {
        return;
    }

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

        if (
            error.response?.status === 429
        ) {

            telegramQueue.unshift(
                text
            );

            await sleep(4000);
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
// 🛡️ REST REQUEST PROTECTION
// ============================================================

function parseBinanceBanTime(error) {

    const message =
        String(
            error.response?.data?.msg ||
            ''
        );

    const match =
        message.match(
            /banned until\s+(\d+)/i
        );

    if (!match) {

        return null;
    }

    const timestamp =
        Number(match[1]);

    if (
        Number.isFinite(timestamp) &&
        timestamp >
            Date.now()
    ) {

        return timestamp;
    }

    return null;
}

async function publicRequest(
    endpoint,
    params = {}
) {

    const now =
        Date.now();

    // ----------------------------------------
    // Binance temporary ban protection
    // ----------------------------------------

    if (
        now <
        restBlockedUntil
    ) {

        return null;
    }

    // ----------------------------------------
    // Global request spacing
    // ----------------------------------------

    const elapsed =
        now -
        lastRestRequestTime;

    if (
        elapsed <
        CONFIG.restMinimumGapMs
    ) {

        await sleep(
            CONFIG.restMinimumGapMs -
            elapsed
        );
    }

    lastRestRequestTime =
        Date.now();

    try {

        const response =
            await axios.get(

                `${BINANCE_REST}${endpoint}`,

                {
                    params,
                    timeout: 15000
                }
            );

        consecutiveRestErrors = 0;

        return response.data;

    } catch (error) {

        consecutiveRestErrors++;

        const status =
            error.response?.status;

        const code =
            error.response?.data?.code;

        // ----------------------------------------
        // Binance rate-limit protection
        // ----------------------------------------

        if (
            status === 429 ||
            status === 418 ||
            code === -1003
        ) {

            const actualBanTime =
                parseBinanceBanTime(
                    error
                );

            if (
                actualBanTime
            ) {

                restBlockedUntil =
                    actualBanTime +
                    5000;

            } else {

                const backoff =
                    Math.min(
                        15 * 60 * 1000,
                        30000 *
                        Math.pow(
                            2,
                            Math.min(
                                consecutiveRestErrors,
                                5
                            )
                        )
                    );

                restBlockedUntil =
                    Date.now() +
                    backoff;
            }

            const waitSeconds =
                Math.max(
                    0,
                    Math.ceil(
                        (
                            restBlockedUntil -
                            Date.now()
                        ) /
                        1000
                    )
                );

            console.error(
                `🛑 Binance REST paused for ${waitSeconds}s`
            );

            sendTelegramMessage(
                `🛑 <b>BINANCE RATE LIMIT</b>\n\n` +
                `REST scanner paused for approximately ${waitSeconds} seconds.\n` +
                `WebSocket price monitoring remains active.`
            );

            return null;
        }

        console.error(
            `❌ REST ${endpoint}:`,
            error.response?.data ||
            error.message
        );

        return null;
    }
}

// ============================================================
// 🌐 WEBSOCKET - LIVE MARKET
// ============================================================

function connectWebSocket() {

    if (
        ws &&
        (
            ws.readyState ===
                WebSocket.OPEN ||
            ws.readyState ===
                WebSocket.CONNECTING
        )
    ) {

        return;
    }

    console.log(
        '🔌 Connecting Binance WebSocket...'
    );

    ws =
        new WebSocket(
            BINANCE_WS
        );

    ws.on(
        'open',
        () => {

            websocketConnected =
                true;

            lastWebsocketMessage =
                Date.now();

            console.log(
                '✅ Binance WebSocket connected.'
            );
        }
    );

    ws.on(
        'message',
        raw => {

            lastWebsocketMessage =
                Date.now();

            try {

                const data =
                    JSON.parse(
                        raw.toString()
                    );

                if (
                    !Array.isArray(data)
                ) {
                    return;
                }

                for (
                    const ticker
                    of data
                ) {

                    const symbol =
                        ticker.s;

                    if (
                        !symbol ||
                        !symbol.endsWith(
                            'USDT'
                        )
                    ) {

                        continue;
                    }

                    const price =
                        safeNumber(
                            ticker.c
                        );

                    const quoteVolume =
                        safeNumber(
                            ticker.q
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

                    // --------------------------------
                    // Position management LIVE
                    // --------------------------------

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

            } catch (error) {

                console.error(
                    'WS parse error:',
                    error.message
                );
            }
        }
    );

    ws.on(
        'close',
        () => {

            websocketConnected =
                false;

            console.log(
                '⚠️ Binance WebSocket disconnected.'
            );

            scheduleWebSocketReconnect();
        }
    );

    ws.on(
        'error',
        error => {

            websocketConnected =
                false;

            console.error(
                '❌ WebSocket:',
                error.message
            );
        }
    );
}

function scheduleWebSocketReconnect() {

    if (
        websocketReconnectTimer
    ) {
        return;
    }

    websocketReconnectTimer =
        setTimeout(
            () => {

                websocketReconnectTimer =
                    null;

                connectWebSocket();

            },
            5000
        );
}

// ============================================================
// ❤️ WS WATCHDOG
// ============================================================

setInterval(
    () => {

        if (
            !lastWebsocketMessage
        ) {
            return;
        }

        const age =
            Date.now() -
            lastWebsocketMessage;

        if (
            age >
            90000
        ) {

            console.log(
                '⚠️ WebSocket stale. Reconnecting...'
            );

            try {

                ws?.terminate();

            } catch (_) {
            }
        }

    },
    30000
);

// ============================================================
// ✅ SYMBOL LIST
// ============================================================

async function loadValidSymbols() {

    const data =
        await publicRequest(
            '/api/v3/exchangeInfo'
        );

    if (
        !data?.symbols
    ) {

        console.log(
            '⚠️ exchangeInfo unavailable. Will retry later.'
        );

        return false;
    }

    validSymbols =
        new Set();

    for (
        const info
        of data.symbols
    ) {

        if (
            info.status !==
            'TRADING'
        ) {
            continue;
        }

        if (
            info.quoteAsset !==
            'USDT'
        ) {
            continue;
        }

        if (
            info.isSpotTradingAllowed ===
            false
        ) {
            continue;
        }

        validSymbols.add(
            info.symbol
        );
    }

    console.log(
        `✅ Valid Binance USDT symbols: ${validSymbols.size}`
    );

    return true;
}

// ============================================================
// 💰 EQUITY
// ============================================================

function currentEquity() {

    let equity =
        paperBalance;

    for (
        const position
        of Object.values(
            activePositions
        )
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
            position.qty *
            price;
    }

    return equity;
}

function updateDrawdown() {

    const equity =
        currentEquity();

    if (
        equity >
        peakEquity
    ) {

        peakEquity =
            equity;
    }

    const drawdown =
        peakEquity > 0
            ? (
                (
                    peakEquity -
                    equity
                ) /
                peakEquity
              ) *
              100
            : 0;

    stats.maxDrawdown =
        Math.max(
            stats.maxDrawdown,
            drawdown
        );
}

// ============================================================
// 📅 DAILY PROTECTION
// ============================================================

function checkDailyReset() {

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

    tradingPaused =
        false;

    dailyStartingEquity =
        currentEquity();

    saveState();

    sendTelegramMessage(
        `🌅 <b>NEW PAPER DAY</b>\n` +
        `Starting Equity: $${dailyStartingEquity.toFixed(2)}`
    );
}

function checkDailyLoss() {

    const limit =
        dailyStartingEquity *
        CONFIG.dailyLossLimitPct;

    if (
        dailyPnL <=
            -limit &&
        !tradingPaused
    ) {

        tradingPaused =
            true;

        saveState();

        sendTelegramMessage(
            `🛑 <b>DAILY LOSS LIMIT</b>\n\n` +
            `PnL: $${dailyPnL.toFixed(2)}\n` +
            `Limit: -$${limit.toFixed(2)}`
        );
    }
}

// ============================================================
// 📊 INDICATORS
// ============================================================

function calculateSMA(
    data,
    period,
    key
) {

    const result = [];

    for (
        let i = 0;
        i < data.length;
        i++
    ) {

        if (
            i <
            period - 1
        ) {

            result.push(null);
            continue;
        }

        let sum = 0;

        for (
            let j = 0;
            j < period;
            j++
        ) {

            sum +=
                data[i - j][key];
        }

        result.push(
            sum / period
        );
    }

    return result;
}

function calculateEMA(
    data,
    period,
    key
) {

    const result = [];

    const multiplier =
        2 /
        (period + 1);

    let previous = null;

    for (
        let i = 0;
        i < data.length;
        i++
    ) {

        const value =
            data[i][key];

        if (
            i <
            period - 1
        ) {

            result.push(null);
            continue;
        }

        if (
            previous === null
        ) {

            let sum = 0;

            for (
                let j = 0;
                j < period;
                j++
            ) {

                sum +=
                    data[i - j][key];
            }

            previous =
                sum /
                period;

        } else {

            previous =
                (
                    value -
                    previous
                ) *
                multiplier +
                previous;
        }

        result.push(
            previous
        );
    }

    return result;
}

function calculateCMO(
    data,
    period
) {

    const result = [];

    for (
        let i = 0;
        i < data.length;
        i++
    ) {

        if (
            i < period
        ) {

            result.push(null);
            continue;
        }

        let up = 0;
        let down = 0;

        for (
            let j = 0;
            j < period;
            j++
        ) {

            const diff =
                data[i - j].close -
                data[i - j - 1].close;

            if (
                diff > 0
            ) {

                up += diff;

            } else {

                down +=
                    Math.abs(diff);
            }
        }

        const total =
            up + down;

        result.push(
            total === 0
                ? 0
                : 100 *
                  (
                      (
                          up -
                          down
                      ) /
                      total
                  )
        );
    }

    return result;
}

function calculateATR(
    candles,
    period = 14
) {

    if (
        candles.length <
        period + 1
    ) {

        return null;
    }

    const ranges = [];

    for (
        let i = 1;
        i < candles.length;
        i++
    ) {

        const high =
            candles[i].high;

        const low =
            candles[i].low;

        const previousClose =
            candles[i - 1].close;

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
// 🏗️ STRUCTURE
// ============================================================

function getStructure(
    candles
) {

    const last =
        candles[
            candles.length - 2
        ];

    const previous =
        candles[
            candles.length - 3
        ];

    if (
        !last ||
        !previous
    ) {

        return 'NEUTRAL';
    }

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
// 📐 FIBONACCI
// ============================================================

function fibonacciScore(
    candles
) {

    const recent =
        candles.slice(-30);

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

    const range =
        high - low;

    if (
        range <= 0
    ) {

        return 0;
    }

    const close =
        candles[
            candles.length - 2
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
// ⭐ OPPORTUNITY SCORE
// ============================================================

function calculateScore(
    candles
) {

    const index =
        candles.length - 2;

    const candle =
        candles[index];

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
        ema20[index] === null ||
        ema50[index] === null ||
        volumeSMA[index] === null ||
        cmo[index] === null ||
        !atr
    ) {

        return null;
    }

    let score = 0;

    const reasons = [];

    // --------------------------------------
    // Trend
    // --------------------------------------

    if (
        candle.close >
        ema20[index]
    ) {

        score += 10;

        reasons.push(
            'EMA20'
        );
    }

    if (
        ema20[index] >
            ema50[index] &&
        candle.close >
            ema50[index]
    ) {

        score += 15;

        reasons.push(
            'UPTREND'
        );
    }

    // --------------------------------------
    // Candle strength
    // --------------------------------------

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

    if (
        candle.close >
            candle.open &&
        bodyRatio >= 0.55
    ) {

        score += 15;

        reasons.push(
            'STRONG_CANDLE'
        );
    }

    // --------------------------------------
    // Volume
    // --------------------------------------

    const volumeRatio =
        volumeSMA[index] > 0
            ? candle.volume /
              volumeSMA[index]
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

    // --------------------------------------
    // Momentum
    // --------------------------------------

    if (
        cmo[index] >= 50
    ) {

        score += 15;

        reasons.push(
            'CMO'
        );
    }

    // --------------------------------------
    // Structure
    // --------------------------------------

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

    // --------------------------------------
    // Fibonacci
    // --------------------------------------

    const fib =
        fibonacciScore(
            candles
        );

    score += fib;

    if (
        fib > 0
    ) {

        reasons.push(
            'FIB'
        );
    }

    // --------------------------------------
    // ATR
    // --------------------------------------

    if (
        range >=
        atr * 0.8
    ) {

        score += 5;

        reasons.push(
            'ATR'
        );
    }

    return {

        score:
            Math.min(
                score,
                100
            ),

        reasons,

        cmo:
            cmo[index],

        volumeRatio,

        structure,

        atr,

        bodyRatio
    };
}

// ============================================================
// 🕯️ GET CANDLES
// ============================================================

async function getCandles(
    symbol
) {

    if (
        !validSymbols.has(
            symbol
        )
    ) {

        return null;
    }

    const data =
        await publicRequest(
            '/api/v3/klines',
            {
                symbol,
                interval:
                    CONFIG.candleInterval,
                limit:
                    CONFIG.candleLimit
            }
        );

    if (
        !Array.isArray(data) ||
        data.length < 60
    ) {

        return null;
    }

    return data.map(
        candle => ({

            open:
                safeNumber(
                    candle[1]
                ),

            high:
                safeNumber(
                    candle[2]
                ),

            low:
                safeNumber(
                    candle[3]
                ),

            close:
                safeNumber(
                    candle[4]
                ),

            volume:
                safeNumber(
                    candle[5]
                )
        })
    );
}

// ============================================================
// 🟢 PAPER ENTRY
// ============================================================

function paperBuy(
    symbol,
    marketPrice,
    analysis
) {

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

        cmo:
            analysis.cmo,

        volumeRatio:
            analysis.volumeRatio,

        structure:
            analysis.structure,

        reasons:
            analysis.reasons,

        entryTime:
            Date.now()
    };

    saveState();

    sendTelegramMessage(
        `🟢 <b>PAPER BUY</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Score: ${analysis.score}/100\n` +
        `Amount: $${allocation.toFixed(2)}\n` +
        `Entry: ${entryPrice.toFixed(8)}\n` +
        `SL: ${activePositions[symbol].stopLoss.toFixed(8)}\n` +
        `TP: ${activePositions[symbol].takeProfit.toFixed(8)}`
    );

    console.log(
        `🟢 PAPER BUY ${symbol} | Score ${analysis.score}`
    );

    return 'PAPER_BOUGHT';
}

// ============================================================
// 🔴 PAPER CLOSE
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

    if (!trade) {

        return null;
    }

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

    if (
        profit > 0
    ) {

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
            Math.abs(
                profit
            );

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

    checkDailyLoss();

    saveState();

    sendTelegramMessage(
        `${profit >= 0 ? '✅' : '❌'} <b>PAPER CLOSE</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Reason: ${reason}\n` +
        `PnL: $${profit.toFixed(2)}\n` +
        `Cash: $${paperBalance.toFixed(2)}`
    );

    console.log(
        `🔴 CLOSE ${symbol} | ${reason} | $${profit.toFixed(2)}`
    );

    return profit;
}

// ============================================================
// 👀 POSITION MONITOR
// Called directly by WebSocket price events
// ============================================================

function manageOpenPosition(
    symbol,
    price
) {

    const trade =
        activePositions[
            symbol
        ];

    if (!trade) {

        return;
    }

    trade.lastPrice =
        price;

    if (
        price <=
        trade.stopLoss
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
        trade.takeProfit
    ) {

        closePaperPosition(
            symbol,
            price,
            'TAKE_PROFIT'
        );
    }
}

// ============================================================
// 🧠 ANALYZE
// ============================================================

async function analyzeMarket(
    symbol
) {

    const candles =
        await getCandles(
            symbol
        );

    if (!candles) {

        return null;
    }

    const analysis =
        calculateScore(
            candles
        );

    if (!analysis) {

        return null;
    }

    const ticker =
        marketTickers.get(
            symbol
        );

    const marketPrice =
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

    } else if (
        analysis.score >=
        CONFIG.minimumScore
    ) {

        decision =
            paperBuy(
                symbol,
                marketPrice,
                analysis
            );
    }

    return {

        symbol,

        score:
            analysis.score,

        decision,

        price:
            marketPrice,

        cmo:
            analysis.cmo.toFixed(
                2
            ),

        volume:
            analysis.volumeRatio.toFixed(
                2
            ),

        structure:
            analysis.structure,

        reasons:
            analysis.reasons.join(
                ', '
            )
    };
}

// ============================================================
// 🏆 TOP COINS
// No REST request — taken from WebSocket data
// ============================================================

function getTopCoins() {

    if (
        marketTickers.size === 0 ||
        validSymbols.size === 0
    ) {

        return [];
    }

    const ignored =
        new Set([
            'USDCUSDT',
            'FDUSDUSDT',
            'TUSDUSDT',
            'USDPUSDT',
            'BUSDUSDT'
        ]);

    const rows = [];

    for (
        const [
            symbol,
            ticker
        ]
        of marketTickers
    ) {

        if (
            !validSymbols.has(
                symbol
            )
        ) {
            continue;
        }

        if (
            ignored.has(
                symbol
            )
        ) {
            continue;
        }

        if (
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
// 📡 CONSERVATIVE SCANNER
// ============================================================

async function runScanner() {

    if (
        scannerRunning
    ) {

        return;
    }

    scannerRunning =
        true;

    try {

        checkDailyReset();

        // --------------------------------------------
        // Do nothing while Binance REST is blocked
        // --------------------------------------------

        if (
            Date.now() <
            restBlockedUntil
        ) {

            return;
        }

        const coins =
            getTopCoins();

        if (
            coins.length === 0
        ) {

            console.log(
                '⏳ Waiting for market data...'
            );

            return;
        }

        if (
            currentCoinIndex >=
            coins.length
        ) {

            currentCoinIndex =
                0;
        }

        const batch =
            coins.slice(
                currentCoinIndex,
                currentCoinIndex +
                CONFIG.batchSize
            );

        currentCoinIndex +=
            CONFIG.batchSize;

        console.log(
            `🔎 Scanner batch: ${batch.length} | Index ${currentCoinIndex}`
        );

        const results = [];

        // --------------------------------------------
        // Deliberately sequential
        // Binance protection > scanner speed
        // --------------------------------------------

        for (
            const symbol
            of batch
        ) {

            if (
                Date.now() <
                restBlockedUntil
            ) {

                break;
            }

            const result =
                await analyzeMarket(
                    symbol
                );

            if (
                result &&
                (
                    result.score >= 70 ||
                    result.decision !==
                        'WAIT'
                )
            ) {

                results.push(
                    result
                );
            }
        }

        latestResults =
            results

                .sort(
                    (a, b) =>
                        b.score -
                        a.score
                )

                .slice(
                    0,
                    50
                );

        lastScanTime =
            new Date()
                .toISOString();

        updateDrawdown();

        saveState();

        console.log(
            `✅ Scan finished | Candidates ${latestResults.length} | Open ${Object.keys(activePositions).length}`
        );

    } catch (error) {

        console.error(
            '❌ Scanner:',
            error.message
        );

    } finally {

        scannerRunning =
            false;
    }
}

setInterval(
    runScanner,
    CONFIG.scannerIntervalMs
);

// ============================================================
// 🔄 EXCHANGE INFO REFRESH
// Only every 6 hours
// ============================================================

setInterval(
    async () => {

        if (
            Date.now() >=
            restBlockedUntil
        ) {

            await loadValidSymbols();
        }

    },
    6 * 60 * 60 * 1000
);

// ============================================================
// 🚨 PAPER EMERGENCY CLOSE
// Uses WebSocket cached prices
// ============================================================

app.post(
    '/api/emergency-close',
    (req, res) => {

        const symbols =
            Object.keys(
                activePositions
            );

        let closed = 0;

        for (
            const symbol
            of symbols
        ) {

            const ticker =
                marketTickers.get(
                    symbol
                );

            const trade =
                activePositions[
                    symbol
                ];

            const price =
                ticker?.price ||
                trade?.lastPrice;

            if (
                !price ||
                !trade
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
// 📊 DATA API
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
                  ) *
                  100
                : 0;

        const profitFactor =
            stats.grossLoss > 0
                ? stats.grossProfit /
                  stats.grossLoss
                : stats.grossProfit > 0
                    ? 999
                    : 0;

        res.json({

            mode:
                'REAL MARKET / PAPER',

            websocket:
                websocketConnected,

            restBlocked:
                Date.now() <
                restBlockedUntil,

            restBlockedUntil:
                restBlockedUntil > 0
                    ? new Date(
                        restBlockedUntil
                      ).toISOString()
                    : null,

            startingBalance:
                CONFIG.startingBalance,

            cashBalance:
                paperBalance.toFixed(
                    2
                ),

            equity:
                currentEquity().toFixed(
                    2
                ),

            dailyPnL:
                dailyPnL.toFixed(
                    2
                ),

            tradingPaused,

            activePositions:
                Object.keys(
                    activePositions
                ).length,

            positions:
                Object.values(
                    activePositions
                ),

            lastScan:
                lastScanTime,

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
// ❤️ HEALTH
// ============================================================

app.get(
    '/health',
    (req, res) => {

        res.json({

            status: 'OK',

            market:
                'BINANCE REAL SPOT',

            execution:
                'PAPER ONLY',

            websocket:
                websocketConnected,

            websocketLastMessage:
                lastWebsocketMessage
                    ? new Date(
                        lastWebsocketMessage
                      ).toISOString()
                    : null,

            tickerCount:
                marketTickers.size,

            validSymbols:
                validSymbols.size,

            restBlocked:
                Date.now() <
                restBlockedUntil,

            openPositions:
                Object.keys(
                    activePositions
                ).length,

            equity:
                currentEquity().toFixed(
                    2
                )
        });
    }
);

// ============================================================
// 🌐 DASHBOARD
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
content="width=device-width,initial-scale=1.0">

<title>LOMY Precision Engine</title>

<style>

*{
box-sizing:border-box;
}

body{
margin:0;
background:#0b0e11;
color:#eaecef;
font-family:Arial,sans-serif;
padding:20px;
text-align:center;
}

h1{
color:#f3ba2f;
}

.badge{
display:inline-block;
background:#f3ba2f;
color:#000;
font-weight:bold;
padding:10px 18px;
border-radius:9px;
}

.connection{
margin:15px;
font-weight:bold;
}

.grid{
display:grid;
grid-template-columns:
repeat(
auto-fit,
minmax(160px,1fr)
);
gap:12px;
max-width:1200px;
margin:20px auto;
}

.card{
background:#1e2329;
border:1px solid #2b3139;
border-radius:10px;
padding:17px;
}

.label{
color:#848e9c;
font-size:12px;
}

.value{
font-size:23px;
font-weight:bold;
margin-top:8px;
}

.green{
color:#0ecb81;
}

.red{
color:#f6465d;
}

.yellow{
color:#f3ba2f;
}

button{
background:#f6465d;
color:#fff;
border:0;
padding:13px 22px;
font-weight:bold;
border-radius:7px;
cursor:pointer;
}

table{
width:100%;
max-width:1200px;
margin:20px auto;
border-collapse:collapse;
background:#1e2329;
}

th{
background:#2b3139;
color:#848e9c;
}

th,td{
padding:10px;
border-bottom:
1px solid #2b3139;
font-size:13px;
}

</style>

</head>

<body>

<h1>
🤖 LOMY Precision Engine
</h1>

<div class="badge">
REAL MARKET • PAPER TRADING
</div>

<div
id="connection"
class="connection">
Connecting...
</div>

<div class="grid">

<div class="card">
<div class="label">
START BALANCE
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
🚨 Close All Paper Positions
</button>

<div
style="overflow-x:auto">

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
Waiting for market...
</td>
</tr>

</tbody>

</table>

</div>

<script>

async function loadData(){

try{

const response =
await fetch(
'/api/data'
);

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
'daily'
).innerText =
'$' + data.dailyPnL;

const connection =
document.getElementById(
'connection'
);

if(data.websocket){

connection.innerText =
'🟢 WebSocket LIVE';

connection.className =
'connection green';

}else{

connection.innerText =
'🔴 WebSocket disconnected';

connection.className =
'connection red';

}

if(data.restBlocked){

connection.innerText +=
' | 🛑 REST rate limited';

}

const tbody =
document.getElementById(
'table'
);

tbody.innerHTML='';

if(
!data.live.length
){

tbody.innerHTML =
'<tr><td colspan="7">Scanning market...</td></tr>';

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
'Close ALL paper positions?'
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
// 🛑 SHUTDOWN
// ============================================================

function shutdown(
    signal
) {

    console.log(
        `🛑 ${signal}: saving state`
    );

    saveState();

    try {

        ws?.close();

    } catch (_) {
    }

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

// ============================================================
// 🚀 START
// ============================================================

app.listen(
    PORT,
    async () => {

        console.log('');
        console.log(
            '=========================================='
        );
        console.log(
            '🚀 LOMY PRECISION ENGINE V3'
        );
        console.log(
            'Market: REAL BINANCE SPOT'
        );
        console.log(
            'Execution: PAPER ONLY'
        );
        console.log(
            'Prices: WEBSOCKET'
        );
        console.log(
            'Scanner: RATE LIMITED REST'
        );
        console.log(
            'Starting Balance: $10,000'
        );
        console.log(
            `Minimum Score: ${CONFIG.minimumScore}`
        );
        console.log(
            '=========================================='
        );

        loadState();

        // WebSocket does not consume REST weight
        connectWebSocket();

        // Try exchangeInfo
        // If the existing Render IP is still banned,
        // publicRequest will pause automatically.
        const loaded =
            await loadValidSymbols();

        if (
            !loaded
        ) {

            console.log(
                '⏳ Binance REST currently unavailable. Bot will keep WebSocket alive and retry symbol loading later.'
            );
        }

        sendTelegramMessage(
            `🚀 <b>LOMY V3 STARTED</b>\n\n` +
            `Market: REAL BINANCE SPOT\n` +
            `Execution: PAPER ONLY\n` +
            `Prices: WEBSOCKET\n` +
            `Balance: $${currentEquity().toFixed(2)}\n` +
            `Entry Score: ${CONFIG.minimumScore}+`
        );

        // Retry initialization periodically if first call
        // happened during Binance IP ban.
        setInterval(
            async () => {

                if (
                    validSymbols.size === 0 &&
                    Date.now() >=
                        restBlockedUntil
                ) {

                    await loadValidSymbols();
                }

            },
            60000
        );

        // Give websocket time to populate prices
        setTimeout(
            runScanner,
            15000
        );
    }
);
