require('dotenv').config();

const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

// ============================================================
// 🔐 ENVIRONMENT
// ============================================================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const API_KEY = process.env.BINANCE_API_KEY;
const API_SECRET = process.env.BINANCE_API_SECRET;

// 🚨 TESTNET ONLY
const BINANCE_URL = 'https://testnet.binance.vision';

// ============================================================
// ⚙️ BOT CONFIGURATION
// ============================================================

const CONFIG = {
    maxPositions: 10,

    // Risk per position
    positionAllocationPct: 0.10,

    // Hard protection
    stopLossPct: 0.01,
    takeProfitPct: 0.02,

    // Daily protection
    dailyLossLimitPct: 0.10,

    // Scanner
    scanIntervalMs: 5000,
    candleInterval: '5m',
    candleLimit: 100,

    // Top market universe
    universeSize: 1000,
    batchSize: 100,

    // Minimum score required to enter
    minimumEntryScore: 80,

    // Volume
    volumeLookback: 20,
    volumeSpikeMultiplier: 1.5,

    // Trend
    emaFast: 20,
    emaSlow: 50,

    // Momentum
    cmoLength: 9,

    // ATR
    atrLength: 14,
    atrStopMultiplier: 1.5,
    atrTargetMultiplier: 3.0,

    // S/R
    structureLookback: 30,

    // API concurrency
    maxConcurrentRequests: 8,

    // Minimum USDT reserve
    minimumBalance: 10,

    // State file
    stateFile: path.join(__dirname, 'bot-state.json')
};

// ============================================================
// 🧠 GLOBAL STATE
// ============================================================

let exchangeRules = {};

let latestResults = [];

let activePositions = {};

let tradeHistory = [];

let stats = {
    totalTrades: 0,
    winningTrades: 0,
    losingTrades: 0,
    totalProfitUSDT: 0,
    bestTrade: 0,
    worstTrade: 0
};

let liveWalletBalance = 0;

let totalWalletEquity = 0;

let dailyPnL = 0;

let dailyStartingEquity = 0;

let currentDayKey = '';

let tradingPaused = false;

let currentCoinIndex = 0;

let scannerRunning = false;

let lastScanTime = null;

let lastSyncTime = null;

let serverStartedAt = Date.now();


// ============================================================
// 📁 STATE MANAGEMENT
// ============================================================

function getDayKey() {
    const now = new Date();

    return [
        now.getUTCFullYear(),
        String(now.getUTCMonth() + 1).padStart(2, '0'),
        String(now.getUTCDate()).padStart(2, '0')
    ].join('-');
}

function saveState() {
    try {
        const state = {
            activePositions,
            tradeHistory: tradeHistory.slice(-500),
            stats,
            dailyPnL,
            dailyStartingEquity,
            currentDayKey,
            tradingPaused
        };

        fs.writeFileSync(
            CONFIG.stateFile,
            JSON.stringify(state, null, 2)
        );
    } catch (error) {
        console.error('❌ State save error:', error.message);
    }
}

function loadState() {
    try {
        if (!fs.existsSync(CONFIG.stateFile)) {
            console.log('ℹ️ No previous state found.');
            return;
        }

        const state = JSON.parse(
            fs.readFileSync(CONFIG.stateFile, 'utf8')
        );

        activePositions = state.activePositions || {};
        tradeHistory = state.tradeHistory || [];

        stats = {
            totalTrades: state.stats?.totalTrades || 0,
            winningTrades: state.stats?.winningTrades || 0,
            losingTrades: state.stats?.losingTrades || 0,
            totalProfitUSDT: state.stats?.totalProfitUSDT || 0,
            bestTrade: state.stats?.bestTrade || 0,
            worstTrade: state.stats?.worstTrade || 0
        };

        dailyPnL = state.dailyPnL || 0;
        dailyStartingEquity = state.dailyStartingEquity || 0;
        currentDayKey = state.currentDayKey || '';
        tradingPaused = state.tradingPaused || false;

        console.log(
            `🔄 State restored. Active positions: ${Object.keys(activePositions).length}`
        );

    } catch (error) {
        console.error('❌ State load error:', error.message);
    }
}


// ============================================================
// 🔔 TELEGRAM QUEUE
// ============================================================

const telegramQueue = [];

let telegramSending = false;

async function processTelegramQueue() {

    if (telegramSending || telegramQueue.length === 0) {
        return;
    }

    telegramSending = true;

    const message = telegramQueue.shift();

    if (!TELEGRAM_TOKEN || !CHAT_ID) {
        telegramSending = false;
        return;
    }

    const url =
        `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;

    try {

        await axios.post(
            url,
            {
                chat_id: CHAT_ID,
                text: message,
                parse_mode: 'HTML'
            },
            {
                timeout: 10000
            }
        );

    } catch (error) {

        if (error.response?.status === 429) {

            telegramQueue.unshift(message);

            await sleep(5000);

        } else {

            console.error(
                'Telegram error:',
                error.response?.data || error.message
            );
        }
    }

    telegramSending = false;
}

setInterval(processTelegramQueue, 1000);

function sendTelegramMessage(text) {

    if (!TELEGRAM_TOKEN || !CHAT_ID) {
        return;
    }

    telegramQueue.push(text);
}


// ============================================================
// 🛠️ UTILITIES
// ============================================================

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
}

function safeNumber(value, fallback = 0) {
    const n = Number(value);

    return Number.isFinite(n) ? n : fallback;
}


// ============================================================
// 🔐 BINANCE SIGNED REQUEST
// ============================================================

async function binancePrivateRequest(
    endpoint,
    method = 'GET',
    params = {}
) {

    if (!API_KEY || !API_SECRET) {

        console.error(
            '❌ Binance API credentials are missing.'
        );

        return null;
    }

    const requestParams = {
        ...params,
        timestamp: Date.now(),
        recvWindow: 60000
    };

    const queryString = Object.keys(requestParams)
        .map(key =>
            `${key}=${encodeURIComponent(requestParams[key])}`
        )
        .join('&');

    const signature = crypto
        .createHmac('sha256', API_SECRET)
        .update(queryString)
        .digest('hex');

    const url =
        `${BINANCE_URL}${endpoint}?${queryString}&signature=${signature}`;

    try {

        const response = await axios({
            method,
            url,
            headers: {
                'X-MBX-APIKEY': API_KEY
            },
            timeout: 15000
        });

        return response.data;

    } catch (error) {

        console.error(
            `❌ Binance ${method} ${endpoint}:`,
            error.response?.data || error.message
        );

        return null;
    }
}


// ============================================================
// 📡 BINANCE PUBLIC REQUEST
// ============================================================

async function binancePublicRequest(endpoint, params = {}) {

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
            `❌ Binance public ${endpoint}:`,
            error.response?.data || error.message
        );

        return null;
    }
}


// ============================================================
// 🛡️ EXCHANGE RULES
// ============================================================

async function loadExchangeRules() {

    console.log('📥 Loading Binance exchange rules...');

    const data =
        await binancePublicRequest('/api/v3/exchangeInfo');

    if (!data || !data.symbols) {

        console.error(
            '❌ Could not load exchange rules.'
        );

        return;
    }

    for (const symbol of data.symbols) {

        const lotSize =
            symbol.filters?.find(
                f => f.filterType === 'LOT_SIZE'
            );

        const priceFilter =
            symbol.filters?.find(
                f => f.filterType === 'PRICE_FILTER'
            );

        const minNotional =
            symbol.filters?.find(
                f =>
                    f.filterType === 'MIN_NOTIONAL' ||
                    f.filterType === 'NOTIONAL'
            );

        exchangeRules[symbol.symbol] = {

            stepSize:
                safeNumber(lotSize?.stepSize, 1),

            minQty:
                safeNumber(lotSize?.minQty, 0),

            tickSize:
                safeNumber(priceFilter?.tickSize, 0.00000001),

            minNotional:
                safeNumber(
                    minNotional?.minNotional,
                    0
                )
        };
    }

    console.log(
        `✅ Exchange rules loaded: ${Object.keys(exchangeRules).length}`
    );
}

function decimalsFromStep(step) {

    const value = String(step);

    if (!value.includes('.')) {
        return 0;
    }

    return value
        .split('.')[1]
        .replace(/0+$/, '')
        .length;
}

function formatQuantity(symbol, qty) {

    const rule = exchangeRules[symbol];

    if (!rule) {
        return String(qty);
    }

    const step = rule.stepSize;

    if (!step || step <= 0) {
        return String(qty);
    }

    const decimals = decimalsFromStep(step);

    const normalized =
        Math.floor(qty / step) * step;

    return normalized.toFixed(decimals);
}

function formatPrice(symbol, price) {

    const rule = exchangeRules[symbol];

    if (!rule || !rule.tickSize) {
        return String(price);
    }

    const tick = rule.tickSize;

    const decimals = decimalsFromStep(tick);

    const normalized =
        Math.round(price / tick) * tick;

    return normalized.toFixed(decimals);
}


// ============================================================
// 💰 WALLET
// ============================================================

async function getAccountInfo() {

    return await binancePrivateRequest(
        '/api/v3/account',
        'GET'
    );
}

async function updateWalletBalance() {

    const data = await getAccountInfo();

    if (!data || !data.balances) {
        return false;
    }

    const usdt =
        data.balances.find(
            b => b.asset === 'USDT'
        );

    if (usdt) {
        liveWalletBalance =
            safeNumber(usdt.free);
    }

    return true;
}


// ============================================================
// 💎 REAL EQUITY CALCULATION
// ============================================================

async function calculateRealEquity() {

    const account = await getAccountInfo();

    if (!account || !account.balances) {
        return totalWalletEquity;
    }

    let equity = 0;

    for (const balance of account.balances) {

        const free =
            safeNumber(balance.free);

        const locked =
            safeNumber(balance.locked);

        const amount =
            free + locked;

        if (amount <= 0) {
            continue;
        }

        if (balance.asset === 'USDT') {

            equity += amount;
            continue;
        }

        const symbol =
            `${balance.asset}USDT`;

        const ticker =
            await binancePublicRequest(
                '/api/v3/ticker/price',
                { symbol }
            );

        if (ticker?.price) {

            equity +=
                amount *
                safeNumber(ticker.price);
        }
    }

    totalWalletEquity = equity;

    return equity;
}


// ============================================================
// 📅 DAILY RISK
// ============================================================

function initializeDailyRisk() {

    const today = getDayKey();

    if (currentDayKey !== today) {

        currentDayKey = today;

        dailyPnL = 0;

        tradingPaused = false;

        dailyStartingEquity =
            totalWalletEquity ||
            liveWalletBalance;

        saveState();

        sendTelegramMessage(
            `🌅 <b>New Trading Day</b>\n` +
            `Starting Equity: $${dailyStartingEquity.toFixed(2)}`
        );
    }

    if (!dailyStartingEquity) {

        dailyStartingEquity =
            totalWalletEquity ||
            liveWalletBalance;
    }
}

function getDailyLossLimit() {

    return (
        dailyStartingEquity *
        CONFIG.dailyLossLimitPct
    );
}

function checkDailyLossProtection() {

    const limit =
        getDailyLossLimit();

    if (
        limit > 0 &&
        dailyPnL <= -limit &&
        !tradingPaused
    ) {

        tradingPaused = true;

        saveState();

        sendTelegramMessage(
            `🛑 <b>TRADING PAUSED</b>\n\n` +
            `Daily Loss: $${dailyPnL.toFixed(2)}\n` +
            `Limit: -$${limit.toFixed(2)}`
        );

        return true;
    }

    return false;
}


// ============================================================
// 📊 INDICATORS
// ============================================================

function calculateSMA(data, period, key) {

    const result = [];

    for (let i = 0; i < data.length; i++) {

        if (i < period - 1) {

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
                safeNumber(data[i - j][key]);
        }

        result.push(
            sum / period
        );
    }

    return result;
}

function calculateEMA(data, period, key) {

    const result = [];

    const multiplier =
        2 / (period + 1);

    let ema = null;

    for (let i = 0; i < data.length; i++) {

        const value =
            safeNumber(data[i][key]);

        if (i < period - 1) {

            result.push(null);
            continue;
        }

        if (ema === null) {

            let sum = 0;

            for (
                let j = 0;
                j < period;
                j++
            ) {

                sum +=
                    safeNumber(data[i - j][key]);
            }

            ema =
                sum / period;

        } else {

            ema =
                (value - ema) *
                multiplier +
                ema;
        }

        result.push(ema);
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

        for (
            let j = 0;
            j < period;
            j++
        ) {

            const diff =
                data[i - j].close -
                data[i - j - 1].close;

            if (diff > 0) {
                up += diff;
            } else {
                down += Math.abs(diff);
            }
        }

        const total =
            up + down;

        result.push(
            total === 0
                ? 0
                : 100 * ((up - down) / total)
        );
    }

    return result;
}

function calculateATR(data, period) {

    const tr = [];

    for (let i = 0; i < data.length; i++) {

        if (i === 0) {

            tr.push(
                data[i].high -
                data[i].low
            );

            continue;
        }

        const high =
            data[i].high;

        const low =
            data[i].low;

        const prevClose =
            data[i - 1].close;

        const trueRange =
            Math.max(
                high - low,
                Math.abs(high - prevClose),
                Math.abs(low - prevClose)
            );

        tr.push(trueRange);
    }

    const result = [];

    for (let i = 0; i < tr.length; i++) {

        if (i < period - 1) {

            result.push(null);
            continue;
        }

        let sum = 0;

        for (
            let j = 0;
            j < period;
            j++
        ) {

            sum += tr[i - j];
        }

        result.push(
            sum / period
        );
    }

    return result;
}


// ============================================================
// 📈 MARKET STRUCTURE
// ============================================================

function calculateStructure(candles) {

    const lookback =
        Math.min(
            CONFIG.structureLookback,
            candles.length - 1
        );

    const recent =
        candles.slice(
            candles.length - lookback
        );

    let highest =
        -Infinity;

    let lowest =
        Infinity;

    for (const candle of recent) {

        if (candle.high > highest) {
            highest = candle.high;
        }

        if (candle.low < lowest) {
            lowest = candle.low;
        }
    }

    const last =
        candles[candles.length - 1];

    const range =
        highest - lowest;

    let position = 0;

    if (range > 0) {

        position =
            (last.close - lowest) /
            range;
    }

    return {
        high: highest,
        low: lowest,
        range,
        position
    };
}


// ============================================================
// 🧮 FIBONACCI
// ============================================================

function calculateFibonacci(candles) {

    const structure =
        calculateStructure(candles);

    const high =
        structure.high;

    const low =
        structure.low;

    const range =
        high - low;

    if (range <= 0) {

        return {
            level382: null,
            level500: null,
            level618: null
        };
    }

    return {

        level382:
            high - range * 0.382,

        level500:
            high - range * 0.500,

        level618:
            high - range * 0.618
    };
}


// ============================================================
// 💧 LIQUIDITY ANALYSIS
// ============================================================

function calculateLiquidity(
    candle,
    averageVolume
) {

    if (!averageVolume) {
        return 0;
    }

    const volumeRatio =
        candle.volume /
        averageVolume;

    let score = 0;

    if (volumeRatio >= 3) {
        score = 25;
    } else if (volumeRatio >= 2) {
        score = 20;
    } else if (volumeRatio >= 1.5) {
        score = 15;
    } else if (volumeRatio >= 1.2) {
        score = 8;
    }

    return score;
}


// ============================================================
// 🧠 OPPORTUNITY SCORING
// ============================================================

function calculateOpportunityScore(
    candles
) {

    const last =
        candles[candles.length - 2];

    const ema20 =
        calculateEMA(
            candles,
            CONFIG.emaFast,
            'close'
        );

    const ema50 =
        calculateEMA(
            candles,
            CONFIG.emaSlow,
            'close'
        );

    const cmo =
        calculateCMO(
            candles,
            CONFIG.cmoLength
        );

    const atr =
        calculateATR(
            candles,
            CONFIG.atrLength
        );

    const volumes =
        calculateSMA(
            candles,
            CONFIG.volumeLookback,
            'volume'
        );

    const index =
        candles.length - 2;

    const e20 =
        ema20[index];

    const e50 =
        ema50[index];

    const momentum =
        cmo[index];

    const atrValue =
        atr[index];

    const averageVolume =
        volumes[index];

    if (
        e20 === null ||
        e50 === null ||
        momentum === null ||
        atrValue === null ||
        averageVolume === null
    ) {

        return null;
    }

    let score = 0;

    const reasons = [];

    // --------------------------------------------------------
    // TREND
    // --------------------------------------------------------

    if (last.close > e20) {

        score += 10;
        reasons.push('EMA20');
    }

    if (
        e20 > e50 &&
        last.close > e50
    ) {

        score += 15;
        reasons.push('UPTREND');
    }

    // --------------------------------------------------------
    // CANDLE STRENGTH
    // --------------------------------------------------------

    const candleRange =
        last.high - last.low;

    const body =
        Math.abs(
            last.close -
            last.open
        );

    const bodyRatio =
        candleRange > 0
            ? body / candleRange
            : 0;

    if (
        last.close > last.open &&
        bodyRatio >= 0.55
    ) {

        score += 15;
        reasons.push('STRONG_CANDLE');
    }

    // --------------------------------------------------------
    // VOLUME
    // --------------------------------------------------------

    const volumeRatio =
        averageVolume > 0
            ? last.volume /
              averageVolume
            : 0;

    const liquidityScore =
        calculateLiquidity(
            last,
            averageVolume
        );

    score += liquidityScore;

    if (
        volumeRatio >=
        CONFIG.volumeSpikeMultiplier
    ) {

        reasons.push('VOLUME_SPIKE');
    }

    // --------------------------------------------------------
    // CMO
    // --------------------------------------------------------

    if (momentum >= 50) {

        score += 15;
        reasons.push('MOMENTUM');
    } else if (momentum >= 30) {

        score += 8;
    }

    // --------------------------------------------------------
    // STRUCTURE
    // --------------------------------------------------------

    const structure =
        calculateStructure(candles);

    if (
        structure.position >= 0.60 &&
        structure.position <= 0.90
    ) {

        score += 10;
        reasons.push('STRUCTURE');
    }

    // --------------------------------------------------------
    // FIBONACCI
    // --------------------------------------------------------

    const fib =
        calculateFibonacci(candles);

    if (
        fib.level618 &&
        last.close >= fib.level618
    ) {

        score += 5;
        reasons.push('FIBONACCI');
    }

    // --------------------------------------------------------
    // BREAKOUT
    // --------------------------------------------------------

    const previousHigh =
        Math.max(
            ...candles
                .slice(
                    Math.max(
                        0,
                        candles.length - 22
                    ),
                    candles.length - 2
                )
                .map(c => c.high)
        );

    if (last.close > previousHigh) {

        score += 10;
        reasons.push('BREAKOUT');
    }

    score =
        clamp(score, 0, 100);

    let decision = 'WAIT';

    if (
        score >=
        CONFIG.minimumEntryScore
    ) {

        decision = 'BUY';
    }

    return {

        score,

        decision,

        reasons,

        cmo: momentum,

        atr: atrValue,

        volumeRatio,

        bodyRatio,

        ema20: e20,

        ema50: e50,

        structure,

        fibonacci: fib
    };
}


// ============================================================
// 🕯️ GET CANDLES
// ============================================================

async function getCandles(symbol) {

    const data =
        await binancePublicRequest(
            '/api/v3/klines',
            {
                symbol,
                interval: CONFIG.candleInterval,
                limit: CONFIG.candleLimit
            }
        );

    if (
        !Array.isArray(data) ||
        data.length <
        CONFIG.emaSlow + 5
    ) {

        return null;
    }

    return data.map(c => ({

        open: safeNumber(c[1]),

        high: safeNumber(c[2]),

        low: safeNumber(c[3]),

        close: safeNumber(c[4]),

        volume: safeNumber(c[5]),

        closeTime: c[6]
    }));
}


// ============================================================
// 📊 MARKET ANALYSIS
// ============================================================

async function analyzeMarket(symbol) {

    try {

        const candles =
            await getCandles(symbol);

        if (!candles) {
            return null;
        }

        const currentPrice =
            candles[candles.length - 1].close;

        if (
            !currentPrice ||
            currentPrice <= 0
        ) {

            return null;
        }

        const analysis =
            calculateOpportunityScore(
                candles
            );

        if (!analysis) {
            return null;
        }

        const position =
            activePositions[symbol];

        let status =
            analysis.decision;

        if (position) {

            status =
                await managePosition(
                    symbol,
                    currentPrice
                );
        } else if (
            analysis.decision === 'BUY'
        ) {

            status =
                await processBuy(
                    symbol,
                    currentPrice,
                    analysis
                );
        }

        return {

            symbol,

            decision: status,

            score: analysis.score,

            cmo: analysis.cmo.toFixed(2),

            atr:
                analysis.atr.toFixed(6),

            volume:
                analysis.volumeRatio.toFixed(2),

            spike:
                analysis.volumeRatio >=
                CONFIG.volumeSpikeMultiplier
                    ? 'YES'
                    : 'NO',

            reasons:
                analysis.reasons.join(', '),

            price: currentPrice
        };

    } catch (error) {

        console.error(
            `❌ Analysis error ${symbol}:`,
            error.message
        );

        return null;
    }
}


// ============================================================
// 💰 ORDER EXECUTION
// ============================================================

async function executeMarketBuy(
    symbol,
    usdtAmount
) {

    const amount =
        Number(usdtAmount.toFixed(2));

    return await binancePrivateRequest(
        '/api/v3/order',
        'POST',
        {
            symbol,
            side: 'BUY',
            type: 'MARKET',
            quoteOrderQty: amount.toFixed(2)
        }
    );
}

async function executeMarketSell(
    symbol,
    quantity
) {

    const formatted =
        formatQuantity(
            symbol,
            quantity
        );

    if (
        !formatted ||
        Number(formatted) <= 0
    ) {

        return null;
    }

    return await binancePrivateRequest(
        '/api/v3/order',
        'POST',
        {
            symbol,
            side: 'SELL',
            type: 'MARKET',
            quantity: formatted
        }
    );
}


// ============================================================
// 🟢 BUY ENGINE
// ============================================================

async function processBuy(
    symbol,
    currentPrice,
    analysis
) {

    if (tradingPaused) {
        return 'PAUSED';
    }

    if (
        Object.keys(activePositions).length >=
        CONFIG.maxPositions
    ) {

        return 'MAX_TRADES';
    }

    if (
        analysis.score <
        CONFIG.minimumEntryScore
    ) {

        return 'WAIT';
    }

    await updateWalletBalance();

    if (
        liveWalletBalance <
        CONFIG.minimumBalance
    ) {

        return 'NO_BALANCE';
    }

    const equity =
        totalWalletEquity ||
        liveWalletBalance;

    let tradeAmount =
        equity *
        CONFIG.positionAllocationPct;

    tradeAmount =
        Math.min(
            tradeAmount,
            liveWalletBalance * 0.95
        );

    if (
        tradeAmount <
        CONFIG.minimumBalance
    ) {

        return 'NO_BALANCE';
    }

    console.log(
        `🟢 BUY candidate ${symbol} | Score: ${analysis.score}`
    );

    const order =
        await executeMarketBuy(
            symbol,
            tradeAmount
        );

    if (
        !order ||
        order.status !== 'FILLED'
    ) {

        console.log(
            `❌ BUY not filled: ${symbol}`
        );

        return 'BUY_FAILED';
    }

    let totalCost = 0;
    let totalQty = 0;

    if (
        Array.isArray(order.fills) &&
        order.fills.length
    ) {

        for (const fill of order.fills) {

            const qty =
                safeNumber(fill.qty);

            const price =
                safeNumber(fill.price);

            totalQty += qty;

            totalCost +=
                qty * price;
        }
    }

    const executedQty =
        safeNumber(
            order.executedQty,
            totalQty
        );

    const entryPrice =
        executedQty > 0
            ? totalCost / executedQty
            : currentPrice;

    if (
        !executedQty ||
        !entryPrice
    ) {

        return 'BUY_DATA_ERROR';
    }

    const atr =
        safeNumber(
            analysis.atr,
            entryPrice * 0.01
        );

    // --------------------------------------------------------
    // ATR PROTECTION
    // --------------------------------------------------------

    const atrStop =
        entryPrice -
        atr *
        CONFIG.atrStopMultiplier;

    const percentageStop =
        entryPrice *
        (1 - CONFIG.stopLossPct);

    const stopLoss =
        Math.max(
            atrStop,
            percentageStop
        );

    const atrTarget =
        entryPrice +
        atr *
        CONFIG.atrTargetMultiplier;

    const percentageTarget =
        entryPrice *
        (1 + CONFIG.takeProfitPct);

    const takeProfit =
        Math.min(
            atrTarget,
            percentageTarget
        );

    activePositions[symbol] = {

        symbol,

        entryPrice,

        qty: executedQty,

        investedUSDT:
            entryPrice *
            executedQty,

        stopLoss,

        takeProfit,

        entryTime:
            Date.now(),

        score:
            analysis.score,

        reasons:
            analysis.reasons,

        highestPrice:
            entryPrice
    };

    stats.totalTrades++;

    tradeHistory.push({

        type: 'BUY',

        symbol,

        price: entryPrice,

        qty: executedQty,

        score: analysis.score,

        time: Date.now()
    });

    saveState();

    await updateWalletBalance();

    sendTelegramMessage(
        `🟢 <b>BUY EXECUTED</b>\n\n` +
        `<b>Symbol:</b> ${symbol}\n` +
        `<b>Score:</b> ${analysis.score}/100\n` +
        `<b>Entry:</b> $${entryPrice.toFixed(6)}\n` +
        `<b>Qty:</b> ${executedQty}\n` +
        `<b>SL:</b> $${stopLoss.toFixed(6)}\n` +
        `<b>TP:</b> $${takeProfit.toFixed(6)}\n` +
        `<b>Reason:</b> ${analysis.reasons.join(', ')}`
    );

    return 'BOUGHT';
}


// ============================================================
// 🔴 POSITION MANAGER
// ============================================================

async function managePosition(
    symbol,
    currentPrice
) {

    const position =
        activePositions[symbol];

    if (!position) {
        return 'WAIT';
    }

    if (
        currentPrice >
        position.highestPrice
    ) {

        position.highestPrice =
            currentPrice;
    }

    const hitSL =
        currentPrice <=
        position.stopLoss;

    const hitTP =
        currentPrice >=
        position.takeProfit;

    if (!hitSL && !hitTP) {

        return (
            `HOLDING | SL ${position.stopLoss.toFixed(6)} | TP ${position.takeProfit.toFixed(6)}`
        );
    }

    const reason =
        hitSL
            ? 'STOP LOSS'
            : 'TAKE PROFIT';

    console.log(
        `🔴 Closing ${symbol}: ${reason}`
    );

    const order =
        await executeMarketSell(
            symbol,
            position.qty
        );

    // --------------------------------------------------------
    // 🚨 IMPORTANT
    // Only delete the local position when Binance confirms
    // that the order is actually FILLED.
    // --------------------------------------------------------

    if (
        !order ||
        order.status !== 'FILLED'
    ) {

        console.error(
            `❌ SELL not confirmed for ${symbol}`
        );

        sendTelegramMessage(
            `🚨 <b>SELL ERROR</b>\n\n` +
            `${symbol}\n` +
            `Reason: ${reason}\n` +
            `Binance did not confirm FILLED.`
        );

        return 'SELL_PENDING';
    }

    const executedQty =
        safeNumber(
            order.executedQty,
            position.qty
        );

    let sellValue = 0;

    if (
        Array.isArray(order.fills) &&
        order.fills.length
    ) {

        for (const fill of order.fills) {

            sellValue +=
                safeNumber(fill.qty) *
                safeNumber(fill.price);
        }
    }

    const averageExit =
        executedQty > 0 &&
        sellValue > 0
            ? sellValue / executedQty
            : currentPrice;

    const profitUSDT =
        (
            averageExit -
            position.entryPrice
        ) *
        executedQty;

    dailyPnL +=
        profitUSDT;

    stats.totalProfitUSDT +=
        profitUSDT;

    if (profitUSDT > 0) {

        stats.winningTrades++;

        stats.bestTrade =
            Math.max(
                stats.bestTrade,
                profitUSDT
            );

    } else {

        stats.losingTrades++;

        stats.worstTrade =
            Math.min(
                stats.worstTrade,
                profitUSDT
            );
    }

    tradeHistory.push({

        type: 'SELL',

        symbol,

        entryPrice:
            position.entryPrice,

        exitPrice:
            averageExit,

        qty:
            executedQty,

        profitUSDT,

        reason,

        time:
            Date.now()
    });

    delete activePositions[symbol];

    checkDailyLossProtection();

    saveState();

    await updateWalletBalance();

    const emoji =
        profitUSDT >= 0
            ? '✅'
            : '❌';

    sendTelegramMessage(
        `${emoji} <b>POSITION CLOSED</b>\n\n` +
        `<b>Symbol:</b> ${symbol}\n` +
        `<b>Reason:</b> ${reason}\n` +
        `<b>Entry:</b> $${position.entryPrice.toFixed(6)}\n` +
        `<b>Exit:</b> $${averageExit.toFixed(6)}\n` +
        `<b>PnL:</b> $${profitUSDT.toFixed(4)}\n` +
        `<b>Daily PnL:</b> $${dailyPnL.toFixed(4)}`
    );

    return `CLOSED ${profitUSDT >= 0 ? 'PROFIT' : 'LOSS'} $${profitUSDT.toFixed(2)}`;
}


// ============================================================
// 🔄 POSITION RECONCILIATION
// ============================================================

async function reconcilePositions() {

    console.log('🔄 Synchronizing positions with Binance...');

    const account =
        await getAccountInfo();

    if (!account?.balances) {
        return;
    }

    for (
        const symbol of Object.keys(activePositions)
    ) {

        const position =
            activePositions[symbol];

        const asset =
            symbol.replace('USDT', '');

        const balance =
            account.balances.find(
                b => b.asset === asset
            );

        const actualQty =
            safeNumber(balance?.free) +
            safeNumber(balance?.locked);

        if (
            actualQty <= 0
        ) {

            console.warn(
                `⚠️ Position ${symbol} exists locally but not on Binance. Removing local record.`
            );

            delete activePositions[symbol];
        }
    }

    saveState();

    lastSyncTime = Date.now();
}


// ============================================================
// 📡 TOP MARKET UNIVERSE
// ============================================================

async function getTopCoins() {

    const data =
        await binancePublicRequest(
            '/api/v3/ticker/24hr'
        );

    if (!Array.isArray(data)) {
        return [];
    }

    const ignoredCoins = [
        'USDC',
        'FDUSD',
        'TUSD',
        'USDP',
        'BUSD',
        'EUR',
        'USD1',
        'USDE'
    ];

    return data

        .filter(t =>
            t.symbol.endsWith('USDT') &&
            !ignoredCoins.some(
                stable =>
                    t.symbol.includes(stable)
            )
        )

        .filter(t =>
            exchangeRules[t.symbol]
        )

        .sort(
            (a, b) =>
                safeNumber(b.quoteVolume) -
                safeNumber(a.quoteVolume)
        )

        .slice(
            0,
            CONFIG.universeSize
        )

        .map(
            t => t.symbol
        );
}


// ============================================================
// ⚡ CONCURRENT SCANNER
// ============================================================

async function scanBatch(symbols) {

    const results = [];

    let index = 0;

    async function worker() {

        while (true) {

            const current =
                index++;

            if (
                current >=
                symbols.length
            ) {
                return;
            }

            const symbol =
                symbols[current];

            const result =
                await analyzeMarket(symbol);

            if (result) {

                if (
                    result.decision !== 'WAIT' ||
                    result.score >= 60 ||
                    activePositions[symbol]
                ) {

                    results.push(result);
                }
            }
        }
    }

    const workers = [];

    const workerCount =
        Math.min(
            CONFIG.maxConcurrentRequests,
            symbols.length
        );

    for (
        let i = 0;
        i < workerCount;
        i++
    ) {

        workers.push(worker());
    }

    await Promise.all(workers);

    return results;
}


// ============================================================
// 🚀 FULL SCAN
// ============================================================

async function runFullScan() {

    if (scannerRunning) {

        console.log(
            '⚠️ Scanner already running.'
        );

        return;
    }

    scannerRunning = true;

    try {

        checkDailyLossProtection();

        initializeDailyRisk();

        const topCoins =
            await getTopCoins();

        if (!topCoins.length) {

            console.log(
                '⚠️ No coins available.'
            );

            return;
        }

        if (
            currentCoinIndex >=
            topCoins.length
        ) {

            currentCoinIndex = 0;
        }

        const batch =
            topCoins.slice(
                currentCoinIndex,
                currentCoinIndex +
                CONFIG.batchSize
            );

        currentCoinIndex +=
            CONFIG.batchSize;

        console.log(
            `📡 Scanning ${batch.length} symbols...`
        );

        const results =
            await scanBatch(batch);

        // ----------------------------------------------------
        // SORT BY SCORE
        // ----------------------------------------------------

        results.sort(
            (a, b) =>
                b.score -
                a.score
        );

        latestResults =
            results.slice(0, 50);

        lastScanTime =
            Date.now();

        console.log(
            `✅ Scan complete | Opportunities: ${results.length}`
        );

    } catch (error) {

        console.error(
            '❌ Scanner error:',
            error.message
        );

    } finally {

        scannerRunning = false;

        setTimeout(
            runFullScan,
            CONFIG.scanIntervalMs
        );
    }
}


// ============================================================
// 🚨 EMERGENCY CLOSE
// ============================================================

app.post(
    '/api/emergency-close',
    async (req, res) => {

        const symbols =
            Object.keys(
                activePositions
            );

        if (!symbols.length) {

            return res.json({
                success: false,
                msg: 'No open positions.'
            });
        }

        sendTelegramMessage(
            `🚨 <b>EMERGENCY CLOSE</b>\n` +
            `${symbols.length} positions`
        );

        let closed = 0;

        for (
            const symbol of symbols
        ) {

            const position =
                activePositions[symbol];

            if (!position) {
                continue;
            }

            try {

                const order =
                    await executeMarketSell(
                        symbol,
                        position.qty
                    );

                if (
                    order &&
                    order.status === 'FILLED'
                ) {

                    let value = 0;

                    let qty =
                        safeNumber(
                            order.executedQty,
                            position.qty
                        );

                    if (
                        Array.isArray(order.fills)
                    ) {

                        for (
                            const fill of order.fills
                        ) {

                            value +=
                                safeNumber(fill.qty) *
                                safeNumber(fill.price);
                        }
                    }

                    const exitPrice =
                        qty > 0 &&
                        value > 0
                            ? value / qty
                            : position.entryPrice;

                    const pnl =
                        (
                            exitPrice -
                            position.entryPrice
                        ) *
                        qty;

                    dailyPnL += pnl;

                    stats.totalProfitUSDT += pnl;

                    if (pnl >= 0) {
                        stats.winningTrades++;
                    } else {
                        stats.losingTrades++;
                    }

                    tradeHistory.push({

                        type: 'EMERGENCY_SELL',

                        symbol,

                        entryPrice:
                            position.entryPrice,

                        exitPrice,

                        qty,

                        profitUSDT:
                            pnl,

                        time:
                            Date.now()
                    });

                    delete activePositions[symbol];

                    closed++;
                }

            } catch (error) {

                console.error(
                    `Emergency close ${symbol}:`,
                    error.message
                );
            }

            await sleep(250);
        }

        saveState();

        await updateWalletBalance();

        res.json({

            success: true,

            msg:
                `Emergency close completed. Closed ${closed}/${symbols.length} positions.`
        });
    }
);


// ============================================================
// 📊 API DATA
// ============================================================

app.get(
    '/api/data',
    async (req, res) => {

        const limit =
            getDailyLossLimit();

        res.json({

            status:
                tradingPaused
                    ? 'PAUSED'
                    : 'RUNNING',

            balance:
                liveWalletBalance.toFixed(2),

            equity:
                totalWalletEquity.toFixed(2),

            dailyPnL:
                dailyPnL.toFixed(2),

            dailyLossLimit:
                limit.toFixed(2),

            isPaused:
                tradingPaused,

            activePositions:
                Object.keys(
                    activePositions
                ).length,

            positions:
                Object.values(
                    activePositions
                ),

            stats,

            lastScan:
                lastScanTime
                    ? new Date(lastScanTime).toISOString()
                    : null,

            lastSync:
                lastSyncTime
                    ? new Date(lastSyncTime).toISOString()
                    : null,

            live:
                latestResults
        });
    }
);


// ============================================================
// ❤️ HEALTH CHECK
// ============================================================

app.get(
    '/health',
    (req, res) => {

        res.json({

            status: 'ok',

            uptime:
                Math.floor(
                    (Date.now() -
                        serverStartedAt) /
                    1000
                ),

            scannerRunning,

            activePositions:
                Object.keys(
                    activePositions
                ).length,

            tradingPaused,

            environment:
                'BINANCE TESTNET'
        });
    }
);


// ============================================================
// 🌐 DASHBOARD
// ============================================================

app.get(
    '/',
    (req, res) => {

        res.send(`<!DOCTYPE html>
<html lang="en">

<head>

<meta charset="UTF-8">

<meta
name="viewport"
content="width=device-width,initial-scale=1.0">

<title>LOMY Ultra Engine</title>

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
}

h1{
color:#f3ba2f;
margin-bottom:5px;
}

.subtitle{
color:#848e9c;
margin-bottom:20px;
}

.container{
max-width:1300px;
margin:auto;
}

.wallet{
background:#1e2329;
border:1px solid #2b3139;
border-radius:12px;
padding:18px;
font-size:20px;
font-weight:bold;
margin-bottom:20px;
}

.grid{
display:grid;
grid-template-columns:
repeat(auto-fit,minmax(180px,1fr));
gap:12px;
margin-bottom:20px;
}

.card{
background:#1e2329;
border:1px solid #2b3139;
border-radius:10px;
padding:16px;
}

.label{
color:#848e9c;
font-size:13px;
}

.value{
font-size:22px;
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

.gray{
color:#848e9c;
}

button{
background:#f6465d;
border:0;
color:white;
padding:12px 20px;
border-radius:7px;
font-weight:bold;
cursor:pointer;
margin-bottom:20px;
}

button:hover{
opacity:.85;
}

.status{
display:inline-block;
padding:7px 12px;
border-radius:6px;
font-weight:bold;
}

.running{
background:#0ecb81;
color:#000;
}

.paused{
background:#f6465d;
color:#fff;
}

table{
width:100%;
border-collapse:collapse;
background:#1e2329;
border-radius:10px;
overflow:hidden;
}

th,td{
padding:11px;
border-bottom:1px solid #2b3139;
text-align:center;
font-size:13px;
}

th{
background:#2b3139;
color:#848e9c;
}

.score{
font-weight:bold;
}

@media(max-width:700px){

body{
padding:10px;
}

.wallet{
font-size:15px;
}

th,td{
padding:8px 4px;
font-size:11px;
}

}

</style>

</head>

<body>

<div class="container">

<h1>🤖 LOMY Ultra-Fast Engine</h1>

<div class="subtitle">
Binance Testnet • Opportunity Scoring Engine
</div>

<div id="status"
class="status running">
RUNNING
</div>

<br><br>

<div class="wallet">

💰 Equity:
$<span id="equity">0.00</span>

&nbsp;&nbsp;|&nbsp;&nbsp;

Free USDT:
$<span id="balance">0.00</span>

</div>

<div class="grid">

<div class="card">

<div class="label">
TOTAL TRADES
</div>

<div
class="value"
id="trades">
0
</div>

</div>

<div class="card">

<div class="label">
NET PROFIT
</div>

<div
class="value"
id="profit">
$0.00
</div>

</div>

<div class="card">

<div class="label">
TODAY PNL
</div>

<div
class="value"
id="daily">
$0.00
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
WIN RATE
</div>

<div
class="value"
id="winrate">
0%
</div>

</div>

</div>

<button
onclick="emergencyClose()">
🚨 EMERGENCY CLOSE ALL
</button>

<div
style="overflow-x:auto;">

<table>

<thead>

<tr>

<th>Symbol</th>
<th>Decision</th>
<th>Score</th>
<th>CMO</th>
<th>Volume</th>
<th>Price</th>
<th>Reasons</th>

</tr>

</thead>

<tbody
id="table">

<tr>

<td colspan="7">
Scanning...
</td>

</tr>

</tbody>

</table>

</div>

</div>

<script>

async function loadData(){

try{

const response =
await fetch('/api/data');

const data =
await response.json();

document.getElementById(
'equity'
).innerText =
data.equity;

document.getElementById(
'balance'
).innerText =
data.balance;

document.getElementById(
'trades'
).innerText =
data.stats.totalTrades;

document.getElementById(
'profit'
).innerText =
'$' +
data.stats.totalProfitUSDT.toFixed(2);

document.getElementById(
'daily'
).innerText =
'$' +
data.dailyPnL;

document.getElementById(
'positions'
).innerText =
data.activePositions;

const total =
data.stats.winningTrades +
data.stats.losingTrades;

const winrate =
total > 0
? (
data.stats.winningTrades /
total *
100
).toFixed(1)
: 0;

document.getElementById(
'winrate'
).innerText =
winrate + '%';

const status =
document.getElementById(
'status'
);

if(data.isPaused){

status.innerText =
'PAUSED';

status.className =
'status paused';

}else{

status.innerText =
'RUNNING';

status.className =
'status running';

}

const tbody =
document.getElementById(
'table'
);

if(!data.live.length){

tbody.innerHTML =
'<tr><td colspan="7">Scanning market...</td></tr>';

return;

}

tbody.innerHTML = '';

data.live.forEach(item => {

let color =
'gray';

if(
item.decision.includes('BOUGHT') ||
item.decision.includes('HOLDING')
){

color='green';

}

if(
item.decision.includes('CLOSED') ||
item.decision.includes('FAILED') ||
item.decision==='PAUSED'
){

color='red';

}

if(
item.score >= 80
){

color='green';

}

tbody.innerHTML += \`
<tr>

<td><b>\${item.symbol}</b></td>

<td class="\${color}">
\${item.decision}
</td>

<td class="score">
\${item.score}/100
</td>

<td>
\${item.cmo}
</td>

<td>
\${item.volume}x
</td>

<td>
\${item.price}
</td>

<td>
\${item.reasons || '-'}
</td>

</tr>
\`;

});

}catch(error){

console.error(error);

}

}

async function emergencyClose(){

const confirmed =
confirm(
'Close ALL open positions immediately?'
);

if(!confirmed){

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

alert(data.msg);

loadData();

}catch(error){

alert(
'Server connection error'
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

</html>`);
});


// ============================================================
// 🔄 PERIODIC SYNC
// ============================================================

async function periodicSync() {

    try {

        await updateWalletBalance();

        totalWalletEquity =
            await calculateRealEquity();

        initializeDailyRisk();

        checkDailyLossProtection();

        await reconcilePositions();

    } catch (error) {

        console.error(
            '❌ Periodic sync error:',
            error.message
        );
    }
}


// ============================================================
// 🚀 GRACEFUL SHUTDOWN
// ============================================================

let shuttingDown = false;

async function gracefulShutdown(signal) {

    if (shuttingDown) {
        return;
    }

    shuttingDown = true;

    console.log(
        `🛑 ${signal} received. Saving state...`
    );

    saveState();

    console.log(
        '✅ State saved.'
    );

    process.exit(0);
}

process.on(
    'SIGTERM',
    () => gracefulShutdown('SIGTERM')
);

process.on(
    'SIGINT',
    () => gracefulShutdown('SIGINT')
);


// ============================================================
// 🚀 START SERVER
// ============================================================

app.listen(
    PORT,
    async () => {

        console.log('');
        console.log(
            '======================================'
        );

        console.log(
            '🤖 LOMY ULTRA ENGINE'
        );

        console.log(
            '======================================'
        );

        console.log(
            `🌐 Port: ${PORT}`
        );

        console.log(
            `🔗 Binance: TESTNET`
        );

        console.log(
            `📊 Max Positions: ${CONFIG.maxPositions}`
        );

        console.log(
            `🎯 Minimum Score: ${CONFIG.minimumEntryScore}`
        );

        console.log(
            `🛡️ Daily Loss: ${CONFIG.dailyLossLimitPct * 100}%`
        );

        console.log(
            '======================================'
        );

        // ----------------------------------------------------
        // Load local state first
        // ----------------------------------------------------

        loadState();

        // ----------------------------------------------------
        // Binance setup
        // ----------------------------------------------------

        await loadExchangeRules();

        await updateWalletBalance();

        totalWalletEquity =
            await calculateRealEquity();

        initializeDailyRisk();

        await reconcilePositions();

        lastSyncTime =
            Date.now();

        console.log(
            `💰 Free USDT: $${liveWalletBalance.toFixed(2)}`
        );

        console.log(
            `💎 Equity: $${totalWalletEquity.toFixed(2)}`
        );

        // ----------------------------------------------------
        // Periodic account synchronization
        // ----------------------------------------------------

        setInterval(
            periodicSync,
            60000
        );

        // ----------------------------------------------------
        // Start scanner
        // ----------------------------------------------------

        setTimeout(
            runFullScan,
            3000
        );

        sendTelegramMessage(
            `🚀 <b>LOMY BOT STARTED</b>\n\n` +
            `Environment: <b>BINANCE TESTNET</b>\n` +
            `Equity: $${totalWalletEquity.toFixed(2)}\n` +
            `Max Positions: ${CONFIG.maxPositions}\n` +
            `Minimum Score: ${CONFIG.minimumEntryScore}`
        );
    }
);
