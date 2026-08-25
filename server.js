require('dotenv').config();

const express = require('express');
const axios = require('axios');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const BINANCE_URL = 'https://testnet.binance.vision';

// =====================================================
// ⚙️ CONFIG
// =====================================================

const CONFIG = {
    paperTrading: true,

    startingBalance: 10000,
    maxPositions: 10,

    // Entry quality
    minimumScore: 80,

    // Risk
    stopLossPct: 0.01,
    takeProfitPct: 0.02,
    dailyLossLimitPct: 0.10,

    // Trading friction
    tradingFeePct: 0.001,      // 0.1%
    slippagePct: 0.0005,       // 0.05%

    // Scanner
    interval: '5m',
    candleLimit: 100,
    scanLimit: 1000,
    batchSize: 100,

    // Filters
    minQuoteVolume: 100000,
    volumeSpikeMultiplier: 1.4
};

// =====================================================
// 🧠 STATE
// =====================================================

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
let tradingPaused = false;
let currentDay = new Date().toISOString().slice(0, 10);

let peakEquity = CONFIG.startingBalance;

let currentCoinIndex = 0;
let scannerRunning = false;
let lastScanTime = null;

// =====================================================
// 📲 TELEGRAM
// =====================================================

const telegramQueue = [];
let telegramSending = false;

function sendTelegramMessage(text) {
    if (!TELEGRAM_TOKEN || !CHAT_ID) return;
    telegramQueue.push(text);
}

async function processTelegramQueue() {
    if (telegramSending || telegramQueue.length === 0) return;

    telegramSending = true;

    const text = telegramQueue.shift();

    try {
        const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;

        await axios.post(url, {
            chat_id: CHAT_ID,
            text,
            parse_mode: 'HTML'
        }, {
            timeout: 10000
        });

    } catch (error) {
        console.error('Telegram error:', error.message);
    } finally {
        telegramSending = false;
    }
}

setInterval(processTelegramQueue, 1200);

// =====================================================
// 🛠️ HELPERS
// =====================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function currentEquity() {

    let equity = paperBalance;

    for (const symbol in activePositions) {

        const p = activePositions[symbol];

        equity += p.qty * p.lastPrice;
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
            ? ((peakEquity - equity) / peakEquity) * 100
            : 0;

    if (drawdown > stats.maxDrawdown) {
        stats.maxDrawdown = drawdown;
    }
}

function checkDailyReset() {

    const today = new Date().toISOString().slice(0, 10);

    if (today !== currentDay) {

        currentDay = today;
        dailyPnL = 0;
        tradingPaused = false;

        sendTelegramMessage(
            `🌅 <b>New Paper Trading Day</b>\nDaily PnL reset.`
        );
    }
}

function checkDailyLossLimit() {

    const limit =
        CONFIG.startingBalance *
        CONFIG.dailyLossLimitPct;

    if (
        dailyPnL <= -limit &&
        !tradingPaused
    ) {

        tradingPaused = true;

        sendTelegramMessage(
            `🛑 <b>PAPER TRADING PAUSED</b>\n` +
            `Daily loss: $${dailyPnL.toFixed(2)}\n` +
            `Limit: -$${limit.toFixed(2)}`
        );
    }
}

// =====================================================
// 🌐 BINANCE PUBLIC DATA
// =====================================================

async function publicRequest(endpoint, params = {}) {

    try {

        const response = await axios.get(
            `${BINANCE_URL}${endpoint}`,
            {
                params,
                timeout: 15000
            }
        );

        return response.data;

    } catch (error) {

        console.error(
            `Binance public error ${endpoint}:`,
            error.response?.data || error.message
        );

        return null;
    }
}

// =====================================================
// 🔍 LOAD VALID SYMBOLS
// =====================================================

async function loadValidSymbols() {

    const data =
        await publicRequest('/api/v3/exchangeInfo');

    if (!data?.symbols) {
        return false;
    }

    validSymbols = new Set();

    for (const s of data.symbols) {

        if (
            s.status === 'TRADING' &&
            s.quoteAsset === 'USDT' &&
            s.isSpotTradingAllowed !== false
        ) {

            validSymbols.add(s.symbol);
        }
    }

    console.log(
        `✅ Valid symbols loaded: ${validSymbols.size}`
    );

    return true;
}

// =====================================================
// 📊 INDICATORS
// =====================================================

function calculateSMA(data, period, key) {

    const result = [];

    for (let i = 0; i < data.length; i++) {

        if (i < period - 1) {
            result.push(null);
            continue;
        }

        let sum = 0;

        for (let j = 0; j < period; j++) {
            sum += data[i - j][key];
        }

        result.push(sum / period);
    }

    return result;
}

function calculateEMA(data, period, key) {

    const result = [];

    const multiplier =
        2 / (period + 1);

    let previous = null;

    for (let i = 0; i < data.length; i++) {

        const value = data[i][key];

        if (i < period - 1) {

            result.push(null);

            continue;
        }

        if (previous === null) {

            let sum = 0;

            for (let j = 0; j < period; j++) {
                sum += data[i - j][key];
            }

            previous = sum / period;

        } else {

            previous =
                (value - previous) *
                multiplier +
                previous;
        }

        result.push(previous);
    }

    return result;
}

function calculateCMO(data, period) {

    const result = [];

    for (let i = 0; i < data.length; i++) {

        if (i < period) {

            result.push(null);
            continue;
        }

        let up = 0;
        let down = 0;

        for (let j = 0; j < period; j++) {

            const diff =
                data[i - j].close -
                data[i - j - 1].close;

            if (diff > 0) {
                up += diff;
            } else {
                down += Math.abs(diff);
            }
        }

        const total = up + down;

        result.push(
            total === 0
                ? 0
                : 100 * ((up - down) / total)
        );
    }

    return result;
}

function calculateATR(data, period = 14) {

    if (data.length < period + 1) {
        return null;
    }

    const ranges = [];

    for (let i = 1; i < data.length; i++) {

        const high = data[i].high;
        const low = data[i].low;
        const prevClose = data[i - 1].close;

        ranges.push(
            Math.max(
                high - low,
                Math.abs(high - prevClose),
                Math.abs(low - prevClose)
            )
        );
    }

    const recent =
        ranges.slice(-period);

    return recent.reduce(
        (a, b) => a + b,
        0
    ) / recent.length;
}

// =====================================================
// 🧠 STRUCTURE / FIBONACCI
// =====================================================

function getStructure(candles) {

    const last =
        candles[candles.length - 2];

    const prev =
        candles[candles.length - 3];

    if (!last || !prev) return 'NEUTRAL';

    if (
        last.high > prev.high &&
        last.low > prev.low
    ) {
        return 'BULLISH';
    }

    if (
        last.high < prev.high &&
        last.low < prev.low
    ) {
        return 'BEARISH';
    }

    return 'NEUTRAL';
}

function getFibScore(candles) {

    const recent =
        candles.slice(-30);

    const high =
        Math.max(...recent.map(c => c.high));

    const low =
        Math.min(...recent.map(c => c.low));

    const range = high - low;

    if (range <= 0) return 0;

    const close =
        candles[candles.length - 2].close;

    const position =
        (close - low) / range;

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

// =====================================================
// ⭐ SCORE ENGINE
// =====================================================

function calculateScore(candles) {

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
    if (candle.close > ema20[index]) {

        score += 10;
        reasons.push('EMA20');
    }

    if (
        ema20[index] > ema50[index] &&
        candle.close > ema50[index]
    ) {

        score += 15;
        reasons.push('UPTREND');
    }

    // Candle
    const range =
        candle.high - candle.low;

    const body =
        Math.abs(
            candle.close - candle.open
        );

    const bodyRatio =
        range > 0
            ? body / range
            : 0;

    if (
        candle.close > candle.open &&
        bodyRatio >= 0.55
    ) {

        score += 15;
        reasons.push('STRONG_CANDLE');
    }

    // Volume
    const volumeRatio =
        candle.volume /
        volumeSMA[index];

    if (
        volumeRatio >=
        CONFIG.volumeSpikeMultiplier
    ) {

        score += 15;
        reasons.push('VOLUME_SPIKE');
    }

    if (volumeRatio >= 1.2) {

        score += 10;
        reasons.push('LIQUIDITY');
    }

    // Momentum
    if (cmo[index] > 50) {

        score += 15;
        reasons.push('CMO');
    }

    // Structure
    const structure =
        getStructure(candles);

    if (structure === 'BULLISH') {

        score += 10;
        reasons.push('STRUCTURE');
    }

    // Fibonacci
    const fibScore =
        getFibScore(candles);

    score += fibScore;

    if (fibScore > 0) {
        reasons.push('FIB');
    }

    // ATR
    if (
        range >= atr * 0.8
    ) {

        score += 5;
        reasons.push('ATR');
    }

    return {
        score: Math.min(score, 100),
        reasons,
        cmo: cmo[index],
        volumeRatio,
        atr,
        structure
    };
}

// =====================================================
// 🟢 PAPER BUY
// =====================================================

function paperBuy(
    symbol,
    marketPrice,
    analysis
) {

    if (tradingPaused) {
        return 'PAUSED';
    }

    if (
        activePositions[symbol]
    ) {
        return 'HOLDING';
    }

    if (
        Object.keys(activePositions).length >=
        CONFIG.maxPositions
    ) {
        return 'MAX_TRADES';
    }

    const equity =
        currentEquity();

    const allocation =
        equity /
        CONFIG.maxPositions;

    const usable =
        Math.min(
            allocation,
            paperBalance
        );

    if (usable < 10) {
        return 'NO_BALANCE';
    }

    // Slippage on entry
    const entryPrice =
        marketPrice *
        (1 + CONFIG.slippagePct);

    const buyFee =
        usable *
        CONFIG.tradingFeePct;

    const netAmount =
        usable - buyFee;

    const qty =
        netAmount /
        entryPrice;

    paperBalance -= usable;

    stats.totalFees += buyFee;

    activePositions[symbol] = {

        symbol,

        entryPrice,

        qty,

        investedUSDT:
            usable,

        stopLoss:
            entryPrice *
            (1 - CONFIG.stopLossPct),

        takeProfit:
            entryPrice *
            (1 + CONFIG.takeProfitPct),

        score:
            analysis.score,

        reasons:
            analysis.reasons,

        entryTime:
            Date.now(),

        lastPrice:
            marketPrice
    };

    sendTelegramMessage(
        `🟢 <b>PAPER BUY</b>\n` +
        `<b>${symbol}</b>\n` +
        `Score: ${analysis.score}/100\n` +
        `Entry: $${entryPrice.toFixed(6)}\n` +
        `SL: $${activePositions[symbol].stopLoss.toFixed(6)}\n` +
        `TP: $${activePositions[symbol].takeProfit.toFixed(6)}`
    );

    return 'PAPER_BOUGHT';
}

// =====================================================
// 🔴 PAPER POSITION MANAGEMENT
// =====================================================

function managePaperPosition(
    symbol,
    marketPrice
) {

    const trade =
        activePositions[symbol];

    if (!trade) return 'WAIT';

    trade.lastPrice =
        marketPrice;

    const hitSL =
        marketPrice <=
        trade.stopLoss;

    const hitTP =
        marketPrice >=
        trade.takeProfit;

    if (!hitSL && !hitTP) {

        return (
            `HOLDING | SL ${trade.stopLoss.toFixed(6)} | TP ${trade.takeProfit.toFixed(6)}`
        );
    }

    // Slippage on sell
    const exitPrice =
        marketPrice *
        (1 - CONFIG.slippagePct);

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

    if (profit > 0) {

        stats.winningTrades++;
        stats.grossProfit += profit;

        if (profit > stats.bestTrade) {
            stats.bestTrade = profit;
        }

    } else {

        stats.losingTrades++;
        stats.grossLoss +=
            Math.abs(profit);

        if (profit < stats.worstTrade) {
            stats.worstTrade = profit;
        }
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

        reason:
            hitTP
                ? 'TAKE_PROFIT'
                : 'STOP_LOSS',

        entryTime:
            trade.entryTime,

        exitTime:
            Date.now()
    });

    delete activePositions[symbol];

    updateDrawdown();
    checkDailyLossLimit();

    sendTelegramMessage(
        `${profit >= 0 ? '✅' : '❌'} <b>PAPER CLOSE</b>\n` +
        `<b>${symbol}</b>\n` +
        `Result: ${profit >= 0 ? 'PROFIT' : 'LOSS'}\n` +
        `PnL: $${profit.toFixed(2)}\n` +
        `Balance: $${paperBalance.toFixed(2)}`
    );

    return (
        `CLOSED ${profit >= 0 ? 'PROFIT' : 'LOSS'} $${profit.toFixed(2)}`
    );
}

// =====================================================
// 📊 ANALYZE SYMBOL
// =====================================================

async function analyzeMarket(symbol) {

    if (!validSymbols.has(symbol)) {
        return null;
    }

    const data =
        await publicRequest(
            '/api/v3/klines',
            {
                symbol,
                interval: CONFIG.interval,
                limit: CONFIG.candleLimit
            }
        );

    if (
        !Array.isArray(data) ||
        data.length < 60
    ) {
        return null;
    }

    const candles =
        data.map(c => ({
            open: Number(c[1]),
            high: Number(c[2]),
            low: Number(c[3]),
            close: Number(c[4]),
            volume: Number(c[5])
        }));

    const marketPrice =
        candles[candles.length - 1]
            .close;

    const analysis =
        calculateScore(candles);

    if (!analysis) {
        return null;
    }

    let decision = 'WAIT';

    if (
        activePositions[symbol]
    ) {

        decision =
            managePaperPosition(
                symbol,
                marketPrice
            );

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

        decision,

        score:
            analysis.score,

        cmo:
            analysis.cmo.toFixed(2),

        volume:
            analysis.volumeRatio.toFixed(2),

        structure:
            analysis.structure,

        reasons:
            analysis.reasons.join(', '),

        price:
            marketPrice
    };
}

// =====================================================
// 📡 MARKET UNIVERSE
// =====================================================

async function getTopCoins() {

    const tickers =
        await publicRequest(
            '/api/v3/ticker/24hr'
        );

    if (!Array.isArray(tickers)) {
        return [];
    }

    return tickers
        .filter(t =>
            validSymbols.has(t.symbol) &&
            t.symbol.endsWith('USDT') &&
            Number(t.quoteVolume) >=
            CONFIG.minQuoteVolume
        )
        .sort(
            (a, b) =>
                Number(b.quoteVolume) -
                Number(a.quoteVolume)
        )
        .slice(
            0,
            CONFIG.scanLimit
        )
        .map(
            t => t.symbol
        );
}

// =====================================================
// 🚀 SCANNER
// =====================================================

async function runFullScan() {

    if (scannerRunning) {
        return;
    }

    scannerRunning = true;

    try {

        checkDailyReset();

        const coins =
            await getTopCoins();

        if (!coins.length) {

            console.log(
                '⚠️ No valid coins found.'
            );

            return;
        }

        if (
            currentCoinIndex >=
            coins.length
        ) {
            currentCoinIndex = 0;
        }

        const batch =
            coins.slice(
                currentCoinIndex,
                currentCoinIndex +
                CONFIG.batchSize
            );

        currentCoinIndex +=
            CONFIG.batchSize;

        const results = [];

        console.log(
            `🔎 Paper scanning ${batch.length} coins...`
        );

        for (const symbol of batch) {

            const result =
                await analyzeMarket(symbol);

            if (
                result &&
                (
                    result.score >= 70 ||
                    result.decision !== 'WAIT' ||
                    activePositions[symbol]
                )
            ) {
                results.push(result);
            }

            await sleep(80);
        }

        latestResults =
            results
                .sort(
                    (a, b) =>
                        b.score - a.score
                )
                .slice(0, 50);

        lastScanTime =
            new Date().toISOString();

        updateDrawdown();

        console.log(
            `✅ Scan complete | Opportunities: ${latestResults.length}`
        );

    } catch (error) {

        console.error(
            'Scanner error:',
            error.message
        );

    } finally {

        scannerRunning = false;

        setTimeout(
            runFullScan,
            5000
        );
    }
}

// =====================================================
// 🚨 PAPER EMERGENCY CLOSE
// =====================================================

app.post(
    '/api/emergency-close',
    async (req, res) => {

        const symbols =
            Object.keys(
                activePositions
            );

        let closed = 0;

        for (const symbol of symbols) {

            const priceData =
                await publicRequest(
                    '/api/v3/ticker/price',
                    { symbol }
                );

            if (!priceData?.price) {
                continue;
            }

            managePaperPosition(
                symbol,
                Number(priceData.price)
            );

            // Force close if still open
            if (activePositions[symbol]) {

                const trade =
                    activePositions[symbol];

                const marketPrice =
                    Number(priceData.price);

                const exitPrice =
                    marketPrice *
                    (1 - CONFIG.slippagePct);

                const gross =
                    trade.qty *
                    exitPrice;

                const fee =
                    gross *
                    CONFIG.tradingFeePct;

                const net =
                    gross - fee;

                const profit =
                    net -
                    trade.investedUSDT;

                paperBalance +=
                    net;

                stats.totalTrades++;
                stats.totalFees += fee;
                stats.netProfit += profit;

                if (profit > 0) {

                    stats.winningTrades++;
                    stats.grossProfit += profit;

                } else {

                    stats.losingTrades++;
                    stats.grossLoss +=
                        Math.abs(profit);
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

                    reason:
                        'EMERGENCY_CLOSE',

                    exitTime:
                        Date.now()
                });

                delete activePositions[symbol];

                closed++;
            }
        }

        updateDrawdown();

        res.json({
            success: true,
            msg:
                `Closed ${closed} paper positions.`
        });
    }
);

// =====================================================
// 📊 API
// =====================================================

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
                'PAPER TRADING',

            startingBalance:
                CONFIG.startingBalance,

            cashBalance:
                paperBalance.toFixed(2),

            equity:
                equity.toFixed(2),

            activePositions:
                Object.keys(
                    activePositions
                ).length,

            dailyPnL:
                dailyPnL.toFixed(2),

            tradingPaused,

            lastScan:
                lastScanTime,

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
                tradeHistory.slice(-50)
        });
    }
);

// =====================================================
// ❤️ HEALTH
// =====================================================

app.get(
    '/health',
    (req, res) => {

        res.json({

            status: 'OK',

            mode:
                'PAPER TRADING',

            scannerRunning,

            validSymbols:
                validSymbols.size,

            equity:
                currentEquity().toFixed(2)
        });
    }
);

// =====================================================
// 🌐 DASHBOARD
// =====================================================

app.get(
    '/',
    (req, res) => {

        res.send(`
<!DOCTYPE html>
<html>
<head>

<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">

<title>LOMY Paper Engine</title>

<style>

body{
background:#0b0e11;
color:#eaecef;
font-family:Arial;
padding:20px;
text-align:center;
}

h1{
color:#f3ba2f;
}

.badge{
display:inline-block;
padding:8px 14px;
background:#f3ba2f;
color:#000;
font-weight:bold;
border-radius:8px;
margin-bottom:20px;
}

.grid{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(160px,1fr));
gap:12px;
max-width:1200px;
margin:20px auto;
}

.card{
background:#1e2329;
border:1px solid #2b3139;
padding:15px;
border-radius:10px;
}

.label{
color:#848e9c;
font-size:12px;
}

.value{
font-size:21px;
font-weight:bold;
margin-top:7px;
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
color:white;
border:0;
padding:12px 20px;
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

th,td{
padding:10px;
border-bottom:1px solid #2b3139;
font-size:13px;
}

th{
background:#2b3139;
color:#848e9c;
}

</style>

</head>

<body>

<h1>🤖 LOMY Precision Engine</h1>

<div class="badge">
PAPER TRADING MODE
</div>

<div class="grid">

<div class="card">
<div class="label">STARTING BALANCE</div>
<div class="value">$10,000</div>
</div>

<div class="card">
<div class="label">CASH BALANCE</div>
<div class="value" id="cash">$0</div>
</div>

<div class="card">
<div class="label">EQUITY</div>
<div class="value" id="equity">$0</div>
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

</div>

<button onclick="emergencyClose()">
🚨 Close All Paper Positions
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
Starting scanner...
</td>
</tr>

</tbody>

</table>

</div>

<script>

async function loadData(){

try{

const res =
await fetch('/api/data');

const data =
await res.json();

document.getElementById('cash').innerText =
'$' + data.cashBalance;

document.getElementById('equity').innerText =
'$' + data.equity;

document.getElementById('winrate').innerText =
data.stats.winRate + '%';

document.getElementById('profit').innerText =
'$' + data.stats.netProfit.toFixed(2);

document.getElementById('pf').innerText =
data.stats.profitFactor;

document.getElementById('dd').innerText =
data.stats.maxDrawdown.toFixed(2) + '%';

document.getElementById('positions').innerText =
data.activePositions;

const tbody =
document.getElementById('table');

tbody.innerHTML='';

if(!data.live.length){

tbody.innerHTML =
'<tr><td colspan="7">Scanning market...</td></tr>';

return;

}

data.live.forEach(item=>{

tbody.innerHTML +=
'<tr>' +
'<td>' + item.symbol + '</td>' +
'<td class="yellow">' + item.score + '</td>' +
'<td>' + item.decision + '</td>' +
'<td>' + item.cmo + '</td>' +
'<td>' + item.volume + 'x</td>' +
'<td>' + item.structure + '</td>' +
'<td>' + item.price + '</td>' +
'</tr>';

});

}catch(e){

console.error(e);

}

}

async function emergencyClose(){

if(!confirm('Close all paper positions?')){
return;
}

const res =
await fetch(
'/api/emergency-close',
{method:'POST'}
);

const data =
await res.json();

alert(data.msg);

loadData();

}

setInterval(loadData,3000);

loadData();

</script>

</body>
</html>
        `);
    }
);

// =====================================================
// 🚀 START
// =====================================================

app.listen(
    PORT,
    async () => {

        console.log('');
        console.log('🚀 LOMY PAPER BOT STARTED');
        console.log('Mode: PAPER TRADING');
        console.log('Starting Balance: $10,000');
        console.log('Minimum Score: 80');
        console.log('');

        const loaded =
            await loadValidSymbols();

        if (!loaded) {

            console.error(
                '❌ Could not load Binance symbols.'
            );

            return;
        }

        sendTelegramMessage(
            `🚀 <b>LOMY PAPER BOT STARTED</b>\n` +
            `Balance: $10,000\n` +
            `Minimum Score: 80\n` +
            `Fee: 0.1%\n` +
            `Slippage: 0.05%`
        );

        runFullScan();
    }
);
