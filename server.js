require('dotenv').config();

const express = require('express');
const axios = require('axios');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

// ============================================================
// 🚀 REAL MARKET DATA + PAPER EXECUTION
// ============================================================

// Real Binance Spot public market
const BINANCE_URL = 'https://api.binance.com';

// Telegram
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

// ============================================================
// ⚙️ CONFIG
// ============================================================

const CONFIG = {
    paperTrading: true,

    startingBalance: 10000,

    maxPositions: 10,

    minimumScore: 80,

    stopLossPct: 0.01,
    takeProfitPct: 0.02,

    dailyLossLimitPct: 0.10,

    // Simulate realistic execution
    tradingFeePct: 0.001,     // 0.10%
    slippagePct: 0.0005,      // 0.05%

    candleInterval: '5m',
    candleLimit: 100,

    universeSize: 1000,
    batchSize: 100,

    minQuoteVolume: 100000,

    scannerDelayMs: 5000,

    // Independent open-position monitoring
    positionMonitorMs: 5000,

    // Scanner concurrency
    concurrency: 8,

    stateFile: path.join(__dirname, 'paper-state.json')
};

// ============================================================
// 🧠 STATE
// ============================================================

let validSymbols = new Set();

let paperBalance = CONFIG.startingBalance;

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

let dailyPnL = 0;

let currentDay =
    new Date().toISOString().slice(0, 10);

let dailyStartingEquity =
    CONFIG.startingBalance;

let tradingPaused = false;

let peakEquity =
    CONFIG.startingBalance;

let scannerRunning = false;

let positionMonitorRunning = false;

let currentCoinIndex = 0;

let lastScanTime = null;

let lastPositionCheck = null;

// ============================================================
// 🛠 HELPERS
// ============================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function safeNumber(value, fallback = 0) {

    const number = Number(value);

    return Number.isFinite(number)
        ? number
        : fallback;
}

// ============================================================
// 💾 STATE PERSISTENCE
// ============================================================

function saveState() {

    try {

        const data = {

            paperBalance,

            activePositions,

            tradeHistory:
                tradeHistory.slice(-1000),

            stats,

            dailyPnL,

            currentDay,

            dailyStartingEquity,

            tradingPaused,

            peakEquity
        };

        fs.writeFileSync(
            CONFIG.stateFile,
            JSON.stringify(data, null, 2)
        );

    } catch (error) {

        console.error(
            '❌ State save error:',
            error.message
        );
    }
}

function loadState() {

    try {

        if (
            !fs.existsSync(CONFIG.stateFile)
        ) {

            console.log(
                'ℹ️ No previous paper state found.'
            );

            return;
        }

        const raw =
            fs.readFileSync(
                CONFIG.stateFile,
                'utf8'
            );

        const data =
            JSON.parse(raw);

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

        dailyPnL =
            safeNumber(
                data.dailyPnL
            );

        currentDay =
            data.currentDay ||
            currentDay;

        dailyStartingEquity =
            safeNumber(
                data.dailyStartingEquity,
                CONFIG.startingBalance
            );

        tradingPaused =
            Boolean(
                data.tradingPaused
            );

        peakEquity =
            safeNumber(
                data.peakEquity,
                CONFIG.startingBalance
            );

        console.log(
            `🔄 State restored | Cash: $${paperBalance.toFixed(2)} | Positions: ${Object.keys(activePositions).length}`
        );

    } catch (error) {

        console.error(
            '❌ State restore error:',
            error.message
        );
    }
}

// ============================================================
// 📲 TELEGRAM
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

        const url =
            `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;

        await axios.post(
            url,
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

            telegramQueue.unshift(text);

            await sleep(4000);

        } else {

            console.error(
                'Telegram error:',
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
// 🌐 BINANCE PUBLIC REQUEST
// ============================================================

async function publicRequest(
    endpoint,
    params = {}
) {

    try {

        const response =
            await axios.get(
                `${BINANCE_URL}${endpoint}`,
                {
                    params,
                    timeout: 15000
                }
            );

        return response.data;

    } catch (error) {

        console.error(
            `❌ Binance ${endpoint}:`,
            error.response?.data ||
            error.message
        );

        return null;
    }
}

// ============================================================
// ✅ VALID REAL BINANCE SYMBOLS
// ============================================================

async function loadValidSymbols() {

    console.log(
        '🔄 Loading REAL Binance Spot symbols...'
    );

    const data =
        await publicRequest(
            '/api/v3/exchangeInfo'
        );

    if (!data?.symbols) {

        console.error(
            '❌ exchangeInfo unavailable.'
        );

        return false;
    }

    validSymbols =
        new Set();

    for (
        const info of data.symbols
    ) {

        if (
            info.status !== 'TRADING'
        ) {
            continue;
        }

        if (
            info.quoteAsset !== 'USDT'
        ) {
            continue;
        }

        if (
            info.isSpotTradingAllowed === false
        ) {
            continue;
        }

        validSymbols.add(
            info.symbol
        );
    }

    console.log(
        `✅ Real Spot USDT symbols: ${validSymbols.size}`
    );

    return validSymbols.size > 0;
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

        equity +=
            position.qty *
            position.lastPrice;
    }

    return equity;
}

function updateDrawdown() {

    const equity =
        currentEquity();

    if (
        equity > peakEquity
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
              ) * 100
            : 0;

    stats.maxDrawdown =
        Math.max(
            stats.maxDrawdown,
            drawdown
        );
}

// ============================================================
// 📅 DAILY RISK
// ============================================================

function checkDailyReset() {

    const today =
        new Date()
            .toISOString()
            .slice(0, 10);

    if (
        today !== currentDay
    ) {

        currentDay =
            today;

        dailyPnL = 0;

        tradingPaused = false;

        dailyStartingEquity =
            currentEquity();

        saveState();

        sendTelegramMessage(
            `🌅 <b>NEW PAPER DAY</b>\n` +
            `Starting Equity: $${dailyStartingEquity.toFixed(2)}`
        );
    }
}

function checkDailyLoss() {

    const lossLimit =
        dailyStartingEquity *
        CONFIG.dailyLossLimitPct;

    if (
        lossLimit > 0 &&
        dailyPnL <= -lossLimit &&
        !tradingPaused
    ) {

        tradingPaused =
            true;

        saveState();

        sendTelegramMessage(
            `🛑 <b>PAPER TRADING PAUSED</b>\n\n` +
            `Daily PnL: $${dailyPnL.toFixed(2)}\n` +
            `Loss Limit: -$${lossLimit.toFixed(2)}`
        );
    }
}

// ============================================================
// 📈 INDICATORS
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
            i < period - 1
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
        2 / (period + 1);

    let previous = null;

    for (
        let i = 0;
        i < data.length;
        i++
    ) {

        const value =
            data[i][key];

        if (
            i < period - 1
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
                sum / period;

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
// 📊 STRUCTURE
// ============================================================

function calculateStructure(
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
// ⭐ SCORE ENGINE
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

    // Trend
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

    // Candle strength
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
            ? body /
              range
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

    // Volume / liquidity
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

    // Momentum
    if (
        cmo[index] > 50
    ) {

        score += 15;

        reasons.push(
            'CMO'
        );
    }

    // Structure
    const structure =
        calculateStructure(
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

    // Fibonacci
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

    // Volatility
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

        atr,

        volumeRatio,

        structure,

        bodyRatio
    };
}

// ============================================================
// 🕯 CANDLES
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
                ),

            closeTime:
                candle[6]
        })
    );
}

// ============================================================
// 🟢 PAPER BUY
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

        return 'MAX_TRADES';
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

    // Simulated entry slippage
    const entryPrice =
        marketPrice *
        (
            1 +
            CONFIG.slippagePct
        );

    // Simulated buy fee
    const buyFee =
        allocation *
        CONFIG.tradingFeePct;

    const usableAmount =
        allocation -
        buyFee;

    const quantity =
        usableAmount /
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

        qty:
            quantity,

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

        entryTime:
            Date.now(),

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

        lastPrice:
            marketPrice
    };

    saveState();

    sendTelegramMessage(
        `🟢 <b>PAPER BUY</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Score: ${analysis.score}/100\n` +
        `Amount: $${allocation.toFixed(2)}\n` +
        `Entry: $${entryPrice.toFixed(8)}\n` +
        `TP: $${activePositions[symbol].takeProfit.toFixed(8)}\n` +
        `SL: $${activePositions[symbol].stopLoss.toFixed(8)}`
    );

    return 'PAPER_BOUGHT';
}

// ============================================================
// 🔴 CLOSE PAPER POSITION
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

    // Sell slippage
    const exitPrice =
        marketPrice *
        (
            1 -
            CONFIG.slippagePct
        );

    const grossExitValue =
        trade.qty *
        exitPrice;

    const sellFee =
        grossExitValue *
        CONFIG.tradingFeePct;

    const netExitValue =
        grossExitValue -
        sellFee;

    const profit =
        netExitValue -
        trade.investedUSDT;

    paperBalance +=
        netExitValue;

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

    checkDailyLoss();

    saveState();

    sendTelegramMessage(
        `${profit >= 0 ? '✅' : '❌'} <b>PAPER CLOSE</b>\n\n` +
        `<b>${symbol}</b>\n` +
        `Reason: ${reason}\n` +
        `PnL: $${profit.toFixed(2)}\n` +
        `Paper Cash: $${paperBalance.toFixed(2)}`
    );

    return profit;
}

// ============================================================
// 👀 INDEPENDENT POSITION MONITOR
// ============================================================

async function monitorPositions() {

    if (
        positionMonitorRunning
    ) {

        return;
    }

    positionMonitorRunning =
        true;

    try {

        const symbols =
            Object.keys(
                activePositions
            );

        if (
            symbols.length === 0
        ) {

            return;
        }

        // One request gets all Binance prices
        const prices =
            await publicRequest(
                '/api/v3/ticker/price'
            );

        if (
            !Array.isArray(
                prices
            )
        ) {

            return;
        }

        const priceMap =
            new Map();

        for (
            const item of prices
        ) {

            priceMap.set(
                item.symbol,
                safeNumber(
                    item.price
                )
            );
        }

        for (
            const symbol of symbols
        ) {

            const position =
                activePositions[
                    symbol
                ];

            if (!position) {
                continue;
            }

            const price =
                priceMap.get(
                    symbol
                );

            if (
                !price ||
                price <= 0
            ) {

                continue;
            }

            position.lastPrice =
                price;

            const hitStop =
                price <=
                position.stopLoss;

            const hitTarget =
                price >=
                position.takeProfit;

            if (
                hitStop
            ) {

                closePaperPosition(
                    symbol,
                    price,
                    'STOP_LOSS'
                );

                continue;
            }

            if (
                hitTarget
            ) {

                closePaperPosition(
                    symbol,
                    price,
                    'TAKE_PROFIT'
                );
            }
        }

        updateDrawdown();

        lastPositionCheck =
            new Date()
                .toISOString();

    } catch (error) {

        console.error(
            '❌ Position monitor:',
            error.message
        );

    } finally {

        positionMonitorRunning =
            false;
    }
}

setInterval(
    monitorPositions,
    CONFIG.positionMonitorMs
);

// ============================================================
// 🧠 ANALYZE SYMBOL
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

    const marketPrice =
        candles[
            candles.length - 1
        ].close;

    const analysis =
        calculateScore(
            candles
        );

    if (!analysis) {

        return null;
    }

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

        price:
            marketPrice,

        reasons:
            analysis.reasons.join(
                ', '
            )
    };
}

// ============================================================
// 🏆 REAL MARKET UNIVERSE
// ============================================================

async function getTopCoins() {

    const tickers =
        await publicRequest(
            '/api/v3/ticker/24hr'
        );

    if (
        !Array.isArray(
            tickers
        )
    ) {

        return [];
    }

    const stableAssets =
        [
            'USDCUSDT',
            'FDUSDUSDT',
            'TUSDUSDT',
            'USDPUSDT',
            'BUSDUSDT'
        ];

    return tickers

        .filter(
            ticker =>

                validSymbols.has(
                    ticker.symbol
                ) &&

                !stableAssets.includes(
                    ticker.symbol
                ) &&

                safeNumber(
                    ticker.quoteVolume
                ) >=
                CONFIG.minQuoteVolume
        )

        .sort(
            (a, b) =>
                safeNumber(
                    b.quoteVolume
                ) -
                safeNumber(
                    a.quoteVolume
                )
        )

        .slice(
            0,
            CONFIG.universeSize
        )

        .map(
            ticker =>
                ticker.symbol
        );
}

// ============================================================
// ⚡ CONCURRENT SCAN
// ============================================================

async function scanBatch(
    symbols
) {

    const results = [];

    let index = 0;

    async function worker() {

        while (true) {

            const myIndex =
                index++;

            if (
                myIndex >=
                symbols.length
            ) {

                return;
            }

            const symbol =
                symbols[
                    myIndex
                ];

            try {

                const result =
                    await analyzeMarket(
                        symbol
                    );

                if (
                    result &&
                    (
                        result.score >= 70 ||
                        result.decision !== 'WAIT'
                    )
                ) {

                    results.push(
                        result
                    );
                }

            } catch (error) {

                console.error(
                    `Analysis ${symbol}:`,
                    error.message
                );
            }
        }
    }

    const workers = [];

    const workerCount =
        Math.min(
            CONFIG.concurrency,
            symbols.length
        );

    for (
        let i = 0;
        i < workerCount;
        i++
    ) {

        workers.push(
            worker()
        );
    }

    await Promise.all(
        workers
    );

    return results;
}

// ============================================================
// 📡 SCANNER
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

        const coins =
            await getTopCoins();

        if (
            coins.length === 0
        ) {

            console.log(
                '⚠️ No market symbols found.'
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
            `🔎 Real Market Scan: ${batch.length} symbols | Index ${currentCoinIndex}`
        );

        const results =
            await scanBatch(
                batch
            );

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
            `✅ Scan complete | Opportunities: ${latestResults.length} | Positions: ${Object.keys(activePositions).length}`
        );

    } catch (error) {

        console.error(
            '❌ Scanner:',
            error.message
        );

    } finally {

        scannerRunning =
            false;

        setTimeout(
            runScanner,
            CONFIG.scannerDelayMs
        );
    }
}

// ============================================================
// 🚨 FORCE CLOSE
// ============================================================

app.post(
    '/api/emergency-close',
    async (req, res) => {

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

        const prices =
            await publicRequest(
                '/api/v3/ticker/price'
            );

        if (
            !Array.isArray(
                prices
            )
        ) {

            return res.json({
                success: false,
                msg:
                    'Price feed unavailable.'
            });
        }

        const priceMap =
            new Map(
                prices.map(
                    item => [
                        item.symbol,
                        safeNumber(
                            item.price
                        )
                    ]
                )
            );

        let closed = 0;

        for (
            const symbol
            of symbols
        ) {

            const price =
                priceMap.get(
                    symbol
                );

            if (
                !price ||
                !activePositions[
                    symbol
                ]
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
// 📊 API
// ============================================================

app.get(
    '/api/data',
    (req, res) => {

        const equity =
            currentEquity();

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

        res.json({

            mode:
                'REAL MARKET + PAPER TRADING',

            startingBalance:
                CONFIG.startingBalance,

            cashBalance:
                paperBalance.toFixed(
                    2
                ),

            equity:
                equity.toFixed(
                    2
                ),

            activePositions:
                Object.keys(
                    activePositions
                ).length,

            positions:
                Object.values(
                    activePositions
                ),

            dailyPnL:
                dailyPnL.toFixed(
                    2
                ),

            dailyStartingEquity:
                dailyStartingEquity.toFixed(
                    2
                ),

            tradingPaused,

            lastScan:
                lastScanTime,

            lastPositionCheck,

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
                'BINANCE REAL SPOT DATA',

            execution:
                'PAPER ONLY',

            validSymbols:
                validSymbols.size,

            scannerRunning,

            positionMonitorRunning,

            positions:
                Object.keys(
                    activePositions
                ).length,

            equity:
                currentEquity()
                    .toFixed(2),

            lastScan:
                lastScanTime,

            lastPositionCheck
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
content="width=device-width,initial-scale=1">

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
font-size:34px;
}

.badge{
display:inline-block;
background:#f3ba2f;
color:#000;
font-weight:bold;
padding:10px 18px;
border-radius:10px;
margin-bottom:20px;
}

.sub{
color:#848e9c;
margin-bottom:20px;
}

.grid{
display:grid;
grid-template-columns:
repeat(
auto-fit,
minmax(160px,1fr)
);
gap:14px;
max-width:1200px;
margin:20px auto;
}

.card{
background:#1e2329;
border:1px solid #2b3139;
padding:18px;
border-radius:12px;
}

.label{
color:#848e9c;
font-size:13px;
}

.value{
font-weight:bold;
font-size:24px;
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
border-radius:8px;
font-weight:bold;
font-size:15px;
cursor:pointer;
margin:15px;
}

table{
width:100%;
max-width:1250px;
margin:20px auto;
border-collapse:collapse;
background:#1e2329;
}

th{
background:#2b3139;
color:#848e9c;
}

td,
th{
padding:10px;
border-bottom:
1px solid #2b3139;
font-size:13px;
}

@media(max-width:600px){

body{
padding:10px;
}

h1{
font-size:30px;
}

td,
th{
font-size:11px;
padding:8px 5px;
}

}

</style>

</head>

<body>

<h1>
🤖 LOMY Precision Engine
</h1>

<div class="badge">
REAL MARKET • PAPER EXECUTION
</div>

<div class="sub">
Real Binance Spot prices — No real orders
</div>

<div class="grid">

<div class="card">
<div class="label">
STARTING BALANCE
</div>
<div class="value">
$10,000
</div>
</div>

<div class="card">
<div class="label">
CASH
</div>
<div class="value"
id="cash">
$0
</div>
</div>

<div class="card">
<div class="label">
EQUITY
</div>
<div class="value"
id="equity">
$0
</div>
</div>

<div class="card">
<div class="label">
CLOSED TRADES
</div>
<div class="value"
id="trades">
0
</div>
</div>

<div class="card">
<div class="label">
WIN RATE
</div>
<div class="value"
id="winrate">
0%
</div>
</div>

<div class="card">
<div class="label">
NET PROFIT
</div>
<div class="value"
id="profit">
$0
</div>
</div>

<div class="card">
<div class="label">
PROFIT FACTOR
</div>
<div class="value"
id="pf">
0
</div>
</div>

<div class="card">
<div class="label">
MAX DRAWDOWN
</div>
<div class="value"
id="dd">
0%
</div>
</div>

<div class="card">
<div class="label">
OPEN POSITIONS
</div>
<div class="value"
id="positions">
0
</div>
</div>

<div class="card">
<div class="label">
TODAY PNL
</div>
<div class="value"
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

<tbody
id="table">

<tr>

<td colspan="7">
Starting real-market scanner...
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
'$' +
data.cashBalance;

document.getElementById(
'equity'
).innerText =
'$' +
data.equity;

document.getElementById(
'trades'
).innerText =
data.stats.totalTrades;

document.getElementById(
'winrate'
).innerText =
data.stats.winRate +
'%';

document.getElementById(
'profit'
).innerText =
'$' +
data.stats.netProfit.toFixed(
2
);

document.getElementById(
'pf'
).innerText =
data.stats.profitFactor;

document.getElementById(
'dd'
).innerText =
data.stats.maxDrawdown.toFixed(
2
) +
'%';

document.getElementById(
'positions'
).innerText =
data.activePositions;

document.getElementById(
'daily'
).innerText =
'$' +
data.dailyPnL;

const tbody =
document.getElementById(
'table'
);

tbody.innerHTML='';

if(
!data.live.length
){

tbody.innerHTML=
'<tr><td colspan="7">Scanning real Binance market...</td></tr>';

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
item.price +
'</td>' +

'</tr>';

}
);

}catch(error){

console.error(
error
);

}

}

async function emergencyClose(){

if(
!confirm(
'Close all open PAPER positions?'
)
){

return;

}

try{

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

}catch(error){

alert(
'Connection error'
);

}

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
// 🛑 GRACEFUL SHUTDOWN
// ============================================================

function shutdown(signal) {

    console.log(
        `🛑 ${signal} received. Saving paper state.`
    );

    saveState();

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
            '🚀 LOMY PRECISION ENGINE'
        );

        console.log(
            'Market: REAL BINANCE SPOT'
        );

        console.log(
            'Execution: PAPER ONLY'
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

        const loaded =
            await loadValidSymbols();

        if (!loaded) {

            console.error(
                '🛑 Could not load real Binance symbols.'
            );

            return;
        }

        checkDailyReset();

        await monitorPositions();

        sendTelegramMessage(
            `🚀 <b>LOMY PRECISION STARTED</b>\n\n` +
            `Market: <b>REAL BINANCE SPOT</b>\n` +
            `Execution: <b>PAPER ONLY</b>\n` +
            `Equity: $${currentEquity().toFixed(2)}\n` +
            `Minimum Score: ${CONFIG.minimumScore}\n` +
            `SL: ${CONFIG.stopLossPct * 100}%\n` +
            `TP: ${CONFIG.takeProfitPct * 100}%`
        );

        setTimeout(
            runScanner,
            2000
        );

        // Refresh Binance symbol list every 30 minutes
        setInterval(
            loadValidSymbols,
            30 * 60 * 1000
        );
    }
);
