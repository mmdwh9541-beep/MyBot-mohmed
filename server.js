require('dotenv').config();

const express = require('express');
const axios = require('axios');
const crypto = require('crypto');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;

// =====================================================
// 🔐 ENVIRONMENT
// =====================================================

const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

const API_KEY = process.env.BINANCE_API_KEY;
const API_SECRET = process.env.BINANCE_API_SECRET;

const BINANCE_URL =
    process.env.BINANCE_BASE_URL || 'https://testnet.binance.vision';

// =====================================================
// ⚙️ BOT CONFIGURATION
// =====================================================

const CONFIG = {

    // Scanner
    scanLimit: 1000,
    batchSize: 100,

    // Timeframe
    interval: '5m',
    candleLimit: 100,

    // Trading
    maxPositions: 10,
    minimumScore: 80,

    // Risk
    stopLossPct: 0.01,
    takeProfitPct: 0.02,
    dailyLossLimitPct: 0.10,

    // Filters
    minPrice: 0.000001,
    minQuoteVolume: 100000,

    // Scanner speed
    requestDelay: 100,

    // Safety
    tradingEnabled: true
};

// =====================================================
// 🧠 GLOBAL STATE
// =====================================================

let exchangeRules = {};
let validSymbols = new Set();

let latestResults = [];

let activePositions = {};

let tradeHistory = [];

let stats = {
    totalTrades: 0,
    winningTrades: 0,
    losingTrades: 0,
    totalProfitUSDT: 0
};

let liveWalletBalance = 0;

let dailyPnL = 0;
let currentDay = new Date().toISOString().slice(0, 10);

let tradingPaused = false;

let currentCoinIndex = 0;

let lastScanTime = null;

let scannerRunning = false;

// =====================================================
// 📲 TELEGRAM QUEUE
// =====================================================

const telegramQueue = [];
let telegramSending = false;

async function processTelegramQueue() {

    if (telegramSending || telegramQueue.length === 0) {
        return;
    }

    telegramSending = true;

    const text = telegramQueue.shift();

    try {

        if (!TELEGRAM_TOKEN || !CHAT_ID) {
            telegramSending = false;
            return;
        }

        const url =
            `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;

        await axios.post(url, {
            chat_id: CHAT_ID,
            text,
            parse_mode: 'HTML'
        });

    } catch (error) {

        if (error.response?.status === 429) {
            telegramQueue.unshift(text);
            await new Promise(r => setTimeout(r, 5000));
        } else {
            console.error(
                '❌ Telegram error:',
                error.response?.data || error.message
            );
        }

    } finally {
        telegramSending = false;
    }
}

setInterval(processTelegramQueue, 1200);

function sendTelegramMessage(text) {

    if (!TELEGRAM_TOKEN || !CHAT_ID) {
        return;
    }

    telegramQueue.push(text);
}

// =====================================================
// 🛡️ DAILY RESET
// =====================================================

function checkDailyReset() {

    const today = new Date().toISOString().slice(0, 10);

    if (today !== currentDay) {

        currentDay = today;
        dailyPnL = 0;
        tradingPaused = false;

        sendTelegramMessage(
            `🌅 <b>New Trading Day</b>\n` +
            `Daily risk counter has been reset.`
        );

        console.log('🌅 Daily PnL reset.');
    }
}

// =====================================================
// 💰 EQUITY
// =====================================================

function getTotalEquity() {

    let equity = Number(liveWalletBalance) || 0;

    for (const symbol of Object.keys(activePositions)) {

        const position = activePositions[symbol];

        equity +=
            Number(position.qty || 0) *
            Number(position.entryPrice || 0);
    }

    return equity;
}

// =====================================================
// 🧮 PRECISION
// =====================================================

function decimalPlaces(step) {

    const str = String(step);

    if (!str.includes('.')) {
        return 0;
    }

    return str
        .split('.')[1]
        .replace(/0+$/, '')
        .length;
}

function floorToStep(value, step) {

    const precision = decimalPlaces(step);

    const factor = Math.pow(10, precision);

    const result =
        Math.floor(Number(value) * factor) / factor;

    return result.toFixed(precision);
}

function formatQuantity(symbol, quantity) {

    const rule = exchangeRules[symbol];

    if (!rule) {
        return String(quantity);
    }

    return floorToStep(
        quantity,
        rule.stepSize
    );
}

// =====================================================
// 🔍 LOAD BINANCE SYMBOLS
// =====================================================

async function loadExchangeRules() {

    try {

        console.log('🔄 Loading Binance exchange information...');

        const response = await axios.get(
            `${BINANCE_URL}/api/v3/exchangeInfo`,
            {
                timeout: 15000
            }
        );

        if (!response.data?.symbols) {
            throw new Error('exchangeInfo returned no symbols');
        }

        exchangeRules = {};
        validSymbols = new Set();

        let usdtCount = 0;

        for (const symbolInfo of response.data.symbols) {

            const symbol = symbolInfo.symbol;

            // Only real trading symbols
            if (symbolInfo.status !== 'TRADING') {
                continue;
            }

            // Only USDT spot pairs
            if (symbolInfo.quoteAsset !== 'USDT') {
                continue;
            }

            if (
                symbolInfo.isSpotTradingAllowed === false
            ) {
                continue;
            }

            const lotSize =
                symbolInfo.filters?.find(
                    f => f.filterType === 'LOT_SIZE'
                );

            const minNotional =
                symbolInfo.filters?.find(
                    f =>
                        f.filterType === 'MIN_NOTIONAL' ||
                        f.filterType === 'NOTIONAL'
                );

            exchangeRules[symbol] = {

                stepSize:
                    lotSize
                        ? Number(lotSize.stepSize)
                        : 0.000001,

                minQty:
                    lotSize
                        ? Number(lotSize.minQty)
                        : 0,

                minNotional:
                    minNotional
                        ? Number(
                            minNotional.minNotional ||
                            minNotional.notional ||
                            0
                        )
                        : 0
            };

            validSymbols.add(symbol);

            usdtCount++;
        }

        console.log(
            `✅ Valid USDT symbols loaded: ${usdtCount}`
        );

        if (usdtCount === 0) {
            throw new Error(
                'No valid USDT symbols available on Binance Testnet'
            );
        }

        return true;

    } catch (error) {

        console.error(
            '❌ Failed to load exchangeInfo:',
            error.response?.data || error.message
        );

        return false;
    }
}

// =====================================================
// 🚦 SYMBOL VALIDATION
// =====================================================

function isValidSymbol(symbol) {

    if (!symbol) {
        return false;
    }

    return validSymbols.has(symbol);
}

// =====================================================
// 🌐 PUBLIC BINANCE REQUEST
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
            `❌ Binance public ${endpoint}:`,
            error.response?.data || error.message
        );

        return null;
    }
}

// =====================================================
// 🔐 SIGNED BINANCE REQUEST
// =====================================================

async function binancePrivateRequest(
    endpoint,
    method = 'GET',
    params = {}
) {

    if (!API_KEY || !API_SECRET) {

        console.error(
            '❌ Binance API key/secret missing.'
        );

        return null;
    }

    const requestParams = {
        ...params,
        timestamp: Date.now(),
        recvWindow: 60000
    };

    const queryString = Object.keys(requestParams)
        .map(
            key =>
                `${key}=${encodeURIComponent(requestParams[key])}`
        )
        .join('&');

    const signature =
        crypto
            .createHmac(
                'sha256',
                API_SECRET
            )
            .update(queryString)
            .digest('hex');

    const url =
        `${BINANCE_URL}${endpoint}?` +
        `${queryString}&signature=${signature}`;

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
            `❌ Binance private ${endpoint}:`,
            error.response?.data || error.message
        );

        return null;
    }
}

// =====================================================
// 💰 WALLET
// =====================================================

async function updateWalletBalance() {

    const data =
        await binancePrivateRequest(
            '/api/v3/account'
        );

    if (!data?.balances) {
        return false;
    }

    const usdt =
        data.balances.find(
            b => b.asset === 'USDT'
        );

    if (usdt) {

        liveWalletBalance =
            Number(usdt.free) || 0;

        return true;
    }

    return false;
}

// =====================================================
// 📊 MARKET DATA
// =====================================================

async function getTicker24hr() {

    const data =
        await publicRequest(
            '/api/v3/ticker/24hr'
        );

    if (!Array.isArray(data)) {
        return [];
    }

    return data;
}

// =====================================================
// 🧮 SMA
// =====================================================

function calculateSMA(
    data,
    period,
    key
) {

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

            sum += Number(
                data[i - j][key]
            );
        }

        result.push(
            sum / period
        );
    }

    return result;
}

// =====================================================
// 📈 EMA
// =====================================================

function calculateEMA(
    data,
    period,
    key
) {

    const result = [];

    const multiplier =
        2 / (period + 1);

    let previous = null;

    for (let i = 0; i < data.length; i++) {

        const value =
            Number(data[i][key]);

        if (i < period - 1) {

            result.push(null);

            continue;
        }

        if (previous === null) {

            let sum = 0;

            for (
                let j = 0;
                j < period;
                j++
            ) {

                sum += Number(
                    data[i - j][key]
                );
            }

            previous =
                sum / period;

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

// =====================================================
// 📊 CMO
// =====================================================

function calculateCMO(
    data,
    period
) {

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

        const total = up + down;

        const cmo =
            total === 0
                ? 0
                : 100 *
                  ((up - down) / total);

        result.push(cmo);
    }

    return result;
}

// =====================================================
// 📐 ATR
// =====================================================

function calculateATR(
    data,
    period = 14
) {

    if (data.length < period + 1) {
        return null;
    }

    const trs = [];

    for (let i = 1; i < data.length; i++) {

        const high =
            data[i].high;

        const low =
            data[i].low;

        const previousClose =
            data[i - 1].close;

        const tr =
            Math.max(
                high - low,
                Math.abs(
                    high - previousClose
                ),
                Math.abs(
                    low - previousClose
                )
            );

        trs.push(tr);
    }

    const recent =
        trs.slice(-period);

    return (
        recent.reduce(
            (a, b) => a + b,
            0
        ) / recent.length
    );
}

// =====================================================
// 🧠 MARKET STRUCTURE
// =====================================================

function calculateStructure(
    candles
) {

    const last =
        candles[candles.length - 2];

    const previous =
        candles[candles.length - 3];

    if (!last || !previous) {
        return 'NEUTRAL';
    }

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

// =====================================================
// 🧮 FIBONACCI POSITION
// =====================================================

function fibonacciScore(
    candles
) {

    const recent =
        candles.slice(-30);

    const high =
        Math.max(
            ...recent.map(c => c.high)
        );

    const low =
        Math.min(
            ...recent.map(c => c.low)
        );

    const range =
        high - low;

    if (range <= 0) {
        return 0;
    }

    const close =
        candles[candles.length - 2].close;

    const level =
        (close - low) / range;

    // Favor bullish pullbacks
    if (
        level >= 0.382 &&
        level <= 0.618
    ) {
        return 5;
    }

    if (
        level >= 0.30 &&
        level <= 0.70
    ) {
        return 3;
    }

    return 0;
}

// =====================================================
// ⭐ SCORE ENGINE
// =====================================================

function calculateScore(
    candles,
    volumeSMA,
    ema50,
    cmo
) {

    const index =
        candles.length - 2;

    const candle =
        candles[index];

    const previous =
        candles[index - 1];

    let score = 0;

    const reasons = [];

    // -----------------------------------------------
    // TREND - 15
    // -----------------------------------------------

    if (
        ema50[index] &&
        candle.close > ema50[index]
    ) {

        score += 15;

        reasons.push(
            'Bullish EMA50'
        );
    }

    // -----------------------------------------------
    // CANDLE STRENGTH - 10
    // -----------------------------------------------

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
        candle.close > candle.open &&
        bodyRatio >= 0.55
    ) {

        score += 10;

        reasons.push(
            'Strong bullish candle'
        );
    }

    // -----------------------------------------------
    // VOLUME - 15
    // -----------------------------------------------

    if (
        volumeSMA[index] &&
        candle.volume >
        volumeSMA[index] * 1.4
    ) {

        score += 15;

        reasons.push(
            'Volume spike'
        );
    }

    // -----------------------------------------------
    // MOMENTUM - 10
    // -----------------------------------------------

    if (
        cmo[index] !== null &&
        cmo[index] > 50
    ) {

        score += 10;

        reasons.push(
            'Strong momentum'
        );
    }

    // -----------------------------------------------
    // MARKET STRUCTURE - 10
    // -----------------------------------------------

    const structure =
        calculateStructure(
            candles
        );

    if (structure === 'BULLISH') {

        score += 10;

        reasons.push(
            'Bullish structure'
        );
    }

    // -----------------------------------------------
    // PREVIOUS CANDLE SUPPORT - 5
    // -----------------------------------------------

    if (
        previous &&
        candle.low >= previous.low
    ) {

        score += 5;

        reasons.push(
            'Higher low'
        );
    }

    // -----------------------------------------------
    // FIBONACCI - 5
    // -----------------------------------------------

    const fib =
        fibonacciScore(
            candles
        );

    score += fib;

    if (fib > 0) {
        reasons.push(
            'Fibonacci zone'
        );
    }

    // -----------------------------------------------
    // VOLATILITY / ATR - 5
    // -----------------------------------------------

    const atr =
        calculateATR(
            candles,
            14
        );

    if (
        atr &&
        atr > 0 &&
        range > atr * 0.8
    ) {

        score += 5;

        reasons.push(
            'Healthy volatility'
        );
    }

    // -----------------------------------------------
    // LIQUIDITY - 10
    // -----------------------------------------------

    const volumeNow =
        candle.volume;

    const avgVolume =
        volumeSMA[index];

    if (
        avgVolume &&
        volumeNow >
        avgVolume * 1.2
    ) {

        score += 10;

        reasons.push(
            'Liquidity confirmed'
        );
    }

    // -----------------------------------------------
    // FINAL
    // -----------------------------------------------

    return {
        score,
        structure,
        reasons
    };
}

// =====================================================
// 🧠 ANALYZE MARKET
// =====================================================

async function analyzeMarket(
    symbol
) {

    // CRITICAL FIX
    if (!isValidSymbol(symbol)) {

        return null;
    }

    try {

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

        const currentPrice =
            candles[candles.length - 1]
                .close;

        if (
            !Number.isFinite(
                currentPrice
            ) ||
            currentPrice <
            CONFIG.minPrice
        ) {

            return null;
        }

        const volumeSMA =
            calculateSMA(
                candles,
                10,
                'volume'
            );

        const ema50 =
            calculateEMA(
                candles,
                50,
                'close'
            );

        const cmo =
            calculateCMO(
                candles,
                9
            );

        const closedIndex =
            candles.length - 2;

        const closedCandle =
            candles[closedIndex];

        const scoreData =
            calculateScore(
                candles,
                volumeSMA,
                ema50,
                cmo
            );

        let decision = 'WAIT';

        // Strong primary conditions
        const bullishTrend =
            ema50[closedIndex] &&
            closedCandle.close >
            ema50[closedIndex];

        const bullishCandle =
            closedCandle.close >
            closedCandle.open;

        const momentumStrong =
            cmo[closedIndex] !== null &&
            cmo[closedIndex] > 50;

        const volumeStrong =
            volumeSMA[closedIndex] &&
            closedCandle.volume >
            volumeSMA[closedIndex] * 1.4;

        const strongBody =
            (
                closedCandle.high -
                closedCandle.low
            ) > 0 &&
            (
                Math.abs(
                    closedCandle.close -
                    closedCandle.open
                ) /
                (
                    closedCandle.high -
                    closedCandle.low
                )
            ) >= 0.55;

        // ENTRY
        if (
            scoreData.score >=
                CONFIG.minimumScore &&
            bullishTrend &&
            bullishCandle &&
            momentumStrong &&
            volumeStrong &&
            strongBody
        ) {

            decision = 'BUY';
        }

        const tradeStatus =
            await processTradeAction(
                symbol,
                currentPrice,
                decision
            );

        return {

            symbol,

            decision: tradeStatus,

            score: scoreData.score,

            cmo:
                Number(
                    cmo[closedIndex] || 0
                ).toFixed(2),

            structure:
                scoreData.structure,

            volume:
                volumeStrong
                    ? 'YES'
                    : 'NO',

            spike:
                volumeStrong
                    ? 'YES'
                    : 'NO',

            reasons:
                scoreData.reasons.join(
                    ', '
                )
        };

    } catch (error) {

        return null;
    }
}

// =====================================================
// 💹 EXECUTE TRADE
// =====================================================

async function executeTrade(
    symbol,
    side,
    amountOrQty
) {

    // CRITICAL SAFETY CHECK
    if (!isValidSymbol(symbol)) {

        console.error(
            `⚠️ Blocked invalid symbol: ${symbol}`
        );

        return null;
    }

    const params = {
        symbol,
        side,
        type: 'MARKET'
    };

    if (side === 'BUY') {

        params.quoteOrderQty =
            Number(amountOrQty)
                .toFixed(2);

    } else {

        params.quantity =
            formatQuantity(
                symbol,
                amountOrQty
            );
    }

    return await binancePrivateRequest(
        '/api/v3/order',
        'POST',
        params
    );
}

// =====================================================
// 💰 PROCESS TRADE
// =====================================================

async function processTradeAction(
    symbol,
    currentPrice,
    decision
) {

    if (
        decision === 'BUY' &&
        !activePositions[symbol]
    ) {

        if (!CONFIG.tradingEnabled) {
            return 'TRADING_DISABLED';
        }

        if (tradingPaused) {
            return 'PAUSED';
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
            getTotalEquity();

        let tradeAmount =
            equity /
            CONFIG.maxPositions;

        const balance =
            Number(
                liveWalletBalance
            );

        if (
            balance <
            tradeAmount
        ) {

            if (balance > 10) {

                tradeAmount =
                    balance * 0.98;

            } else {

                return 'NO_BALANCE';
            }
        }

        if (tradeAmount < 10) {
            return 'TRADE_TOO_SMALL';
        }

        const order =
            await executeTrade(
                symbol,
                'BUY',
                tradeAmount
            );

        if (
            order &&
            order.status === 'FILLED'
        ) {

            const fills =
                order.fills || [];

            let entryPrice;

            if (fills.length > 0) {

                const totalQty =
                    fills.reduce(
                        (sum, f) =>
                            sum +
                            Number(f.qty),
                        0
                    );

                const totalValue =
                    fills.reduce(
                        (sum, f) =>
                            sum +
                            (
                                Number(f.price) *
                                Number(f.qty)
                            ),
                        0
                    );

                entryPrice =
                    totalQty > 0
                        ? totalValue /
                          totalQty
                        : currentPrice;

            } else {

                entryPrice =
                    currentPrice;
            }

            const qty =
                Number(
                    order.executedQty
                );

            activePositions[symbol] = {

                symbol,

                entryPrice,

                qty,

                stopLoss:
                    entryPrice *
                    (1 -
                        CONFIG.stopLossPct),

                takeProfit:
                    entryPrice *
                    (1 +
                        CONFIG.takeProfitPct),

                openedAt:
                    Date.now()
            };

            sendTelegramMessage(
                `🟢 <b>BUY EXECUTED</b>\n` +
                `<b>Symbol:</b> ${symbol}\n` +
                `<b>Amount:</b> $${tradeAmount.toFixed(2)}\n` +
                `<b>Entry:</b> $${entryPrice.toFixed(6)}\n` +
                `<b>TP:</b> $${activePositions[symbol].takeProfit.toFixed(6)}\n` +
                `<b>SL:</b> $${activePositions[symbol].stopLoss.toFixed(6)}`
            );

            await updateWalletBalance();

            return 'BOUGHT';
        }

        return 'ORDER_FAILED';
    }

    // =================================================
    // POSITION MANAGEMENT
    // =================================================

    if (activePositions[symbol]) {

        const trade =
            activePositions[symbol];

        const hitSL =
            currentPrice <=
            trade.stopLoss;

        const hitTP =
            currentPrice >=
            trade.takeProfit;

        if (hitSL || hitTP) {

            const order =
                await executeTrade(
                    symbol,
                    'SELL',
                    trade.qty
                );

            if (
                order &&
                (
                    order.status === 'FILLED' ||
                    order.status === 'NEW'
                )
            ) {

                const profit =
                    (
                        currentPrice -
                        trade.entryPrice
                    ) *
                    trade.qty;

                stats.totalTrades++;

                if (profit > 0) {
                    stats.winningTrades++;
                } else {
                    stats.losingTrades++;
                }

                stats.totalProfitUSDT +=
                    profit;

                dailyPnL += profit;

                tradeHistory.push({

                    symbol,

                    entry:
                        trade.entryPrice,

                    exit:
                        currentPrice,

                    profit,

                    reason:
                        hitTP
                            ? 'TAKE_PROFIT'
                            : 'STOP_LOSS',

                    time:
                        new Date().toISOString()
                });

                const result =
                    profit >= 0
                        ? '✅ PROFIT'
                        : '❌ LOSS';

                sendTelegramMessage(
                    `🔴 <b>POSITION CLOSED</b>\n` +
                    `<b>${symbol}</b>\n` +
                    `<b>Result:</b> ${result}\n` +
                    `<b>PnL:</b> $${profit.toFixed(2)}`
                );

                delete activePositions[
                    symbol
                ];

                await updateWalletBalance();

                // Daily protection
                const currentEquity =
                    getTotalEquity();

                const maxDailyLoss =
                    currentEquity *
                    CONFIG.dailyLossLimitPct;

                if (
                    dailyPnL <=
                    -maxDailyLoss
                ) {

                    tradingPaused = true;

                    sendTelegramMessage(
                        `🛑 <b>TRADING PAUSED</b>\n` +
                        `Daily loss limit reached.`
                    );
                }

                return (
                    `CLOSED ($${profit.toFixed(2)})`
                );
            }
        }

        return (
            `HOLDING ` +
            `(SL: $${trade.stopLoss.toFixed(6)} ` +
            `| TP: $${trade.takeProfit.toFixed(6)})`
        );
    }

    return decision;
}

// =====================================================
// 🚀 FULL SCANNER
// =====================================================

async function runFullScan() {

    if (scannerRunning) {
        return;
    }

    scannerRunning = true;

    try {

        checkDailyReset();

        // Refresh symbols periodically
        if (
            validSymbols.size === 0
        ) {

            await loadExchangeRules();
        }

        const tickers =
            await getTicker24hr();

        if (!tickers.length) {

            console.log(
                '⚠️ No ticker data.'
            );

            return;
        }

        const ignoredAssets = new Set([
            'USDC',
            'FDUSD',
            'TUSD',
            'USDP',
            'BUSD',
            'USD1'
        ]);

        const coins =
            tickers
                .filter(t => {

                    const symbol =
                        t.symbol;

                    if (
                        !isValidSymbol(
                            symbol
                        )
                    ) {
                        return false;
                    }

                    if (
                        !symbol.endsWith(
                            'USDT'
                        )
                    ) {
                        return false;
                    }

                    for (
                        const asset
                        of ignoredAssets
                    ) {

                        if (
                            symbol.includes(
                                asset
                            )
                        ) {
                            return false;
                        }
                    }

                    const quoteVolume =
                        Number(
                            t.quoteVolume
                        );

                    return (
                        Number.isFinite(
                            quoteVolume
                        ) &&
                        quoteVolume >=
                        CONFIG.minQuoteVolume
                    );
                })
                .sort(
                    (a, b) =>
                        Number(
                            b.quoteVolume
                        ) -
                        Number(
                            a.quoteVolume
                        )
                )
                .slice(
                    0,
                    CONFIG.scanLimit
                )
                .map(
                    t => t.symbol
                );

        if (!coins.length) {

            console.log(
                '⚠️ No valid USDT markets found.'
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
            `🔎 Scanning ${batch.length} symbols...`
        );

        for (
            const symbol
            of batch
        ) {

            const result =
                await analyzeMarket(
                    symbol
                );

            if (
                result &&
                (
                    result.score >= 70 ||
                    result.decision !== 'WAIT' ||
                    activePositions[
                        symbol
                    ]
                )
            ) {

                results.push(result);
            }

            await new Promise(
                resolve =>
                    setTimeout(
                        resolve,
                        CONFIG.requestDelay
                    )
            );
        }

        latestResults =
            results
                .sort(
                    (a, b) =>
                        b.score -
                        a.score
                )
                .slice(0, 50);

        lastScanTime =
            new Date().toISOString();

        console.log(
            `✅ Scan complete | ` +
            `Results: ${latestResults.length} | ` +
            `Next index: ${currentCoinIndex}`
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
            5000
        );
    }
}

// =====================================================
// 🚨 EMERGENCY CLOSE
// =====================================================

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
                msg:
                    'No open positions.'
            });
        }

        let closed = 0;

        sendTelegramMessage(
            `🚨 <b>EMERGENCY CLOSE</b>\n` +
            `${symbols.length} positions requested.`
        );

        for (
            const symbol
            of symbols
        ) {

            if (!isValidSymbol(symbol)) {
                continue;
            }

            try {

                const priceData =
                    await publicRequest(
                        '/api/v3/ticker/price',
                        { symbol }
                    );

                if (!priceData?.price) {
                    continue;
                }

                const price =
                    Number(
                        priceData.price
                    );

                const trade =
                    activePositions[
                        symbol
                    ];

                const order =
                    await executeTrade(
                        symbol,
                        'SELL',
                        trade.qty
                    );

                if (
                    order &&
                    (
                        order.status ===
                        'FILLED' ||
                        order.status ===
                        'NEW'
                    )
                ) {

                    const profit =
                        (
                            price -
                            trade.entryPrice
                        ) *
                        trade.qty;

                    stats.totalTrades++;

                    if (profit >= 0) {
                        stats.winningTrades++;
                    } else {
                        stats.losingTrades++;
                    }

                    stats.totalProfitUSDT +=
                        profit;

                    dailyPnL += profit;

                    delete activePositions[
                        symbol
                    ];

                    closed++;
                }

            } catch (error) {

                console.error(
                    `Emergency close ${symbol}:`,
                    error.message
                );
            }

            await new Promise(
                resolve =>
                    setTimeout(
                        resolve,
                        200
                    )
            );
        }

        await updateWalletBalance();

        res.json({

            success: true,

            msg:
                `Closed ${closed} positions.`

        });
    }
);

// =====================================================
// 📡 API DATA
// =====================================================

app.get(
    '/api/data',
    (req, res) => {

        const equity =
            getTotalEquity();

        const limit =
            equity *
            CONFIG.dailyLossLimitPct;

        const winRate =
            stats.totalTrades > 0
                ? (
                    stats.winningTrades /
                    stats.totalTrades
                ) * 100
                : 0;

        res.json({

            environment:
                'BINANCE TESTNET',

            tradingEnabled:
                CONFIG.tradingEnabled,

            minimumScore:
                CONFIG.minimumScore,

            validSymbols:
                validSymbols.size,

            live:
                latestResults,

            stats: {

                ...stats,

                winRate:
                    Number(
                        winRate.toFixed(2)
                    )
            },

            balance:
                liveWalletBalance.toFixed(2),

            equity:
                equity.toFixed(2),

            dailyPnL:
                dailyPnL.toFixed(2),

            limit:
                limit.toFixed(2),

            isPaused:
                tradingPaused,

            activePositions:
                Object.keys(
                    activePositions
                ).length,

            lastScan:
                lastScanTime
        });
    }
);

// =====================================================
// 🏠 DASHBOARD
// =====================================================

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

body{
background:#0b0e11;
color:#eaecef;
font-family:Arial;
text-align:center;
padding:20px;
}

h1{
color:#f3ba2f;
}

.wallet{
font-size:20px;
color:#0ecb81;
font-weight:bold;
border:2px dashed #2b3139;
padding:12px;
display:inline-block;
border-radius:10px;
}

.stats{
display:flex;
justify-content:center;
gap:12px;
flex-wrap:wrap;
margin:20px 0;
}

.box{
background:#1e2329;
padding:15px 25px;
border-radius:8px;
border:1px solid #2b3139;
}

table{
width:100%;
max-width:1200px;
margin:auto;
border-collapse:collapse;
background:#1e2329;
}

th,td{
padding:10px;
border-bottom:1px solid #2b3139;
}

th{
color:#848e9c;
}

.buy{
color:#0ecb81;
font-weight:bold;
}

.sell{
color:#f6465d;
font-weight:bold;
}

.score{
color:#f3ba2f;
font-weight:bold;
}

button{
background:#f6465d;
color:white;
border:none;
padding:12px 25px;
border-radius:6px;
cursor:pointer;
font-weight:bold;
}

</style>

</head>

<body>

<h1>🤖 LOMY Precision Engine</h1>

<div class="wallet">

💰 Equity:
$<span id="equity">...</span>

|

Free USDT:
$<span id="balance">...</span>

</div>

<div class="stats">

<div class="box">
Trades:
<span id="trades">0</span>
</div>

<div class="box">
Win Rate:
<span id="winrate">0%</span>
</div>

<div class="box">
Profit:
<span id="profit">$0</span>
</div>

<div class="box">
Daily PnL:
<span id="daily">$0</span>
</div>

<div class="box">
Valid Symbols:
<span id="symbols">0</span>
</div>

</div>

<button onclick="emergencyClose()">
🚨 Emergency Close All
</button>

<br><br>

<div style="overflow-x:auto">

<table>

<thead>

<tr>

<th>Symbol</th>
<th>Score</th>
<th>Status</th>
<th>CMO</th>
<th>Structure</th>
<th>Volume</th>

</tr>

</thead>

<tbody id="table">

<tr>
<td colspan="6">
Starting scanner...
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
'winrate'
).innerText =
data.stats.winRate + '%';

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
'symbols'
).innerText =
data.validSymbols;

const tbody =
document.getElementById(
'table'
);

tbody.innerHTML='';

if(!data.live.length){

tbody.innerHTML=
'<tr><td colspan="6">Scanning...</td></tr>';

return;

}

data.live.forEach(item=>{

let statusClass =
item.decision.includes('BUY') ||
item.decision.includes('BOUGHT') ||
item.decision.includes('HOLDING')
? 'buy'
: item.decision.includes('CLOSED') ||
item.decision.includes('PAUSED')
? 'sell'
: '';

tbody.innerHTML +=

'<tr>' +

'<td>' +
item.symbol +
'</td>' +

'<td class="score">' +
item.score +
'</td>' +

'<td class="' +
statusClass +
'">' +
item.decision +
'</td>' +

'<td>' +
item.cmo +
'</td>' +

'<td>' +
item.structure +
'</td>' +

'<td>' +
item.volume +
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
'Close all open positions?'
)
)return;

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
4000
);

loadData();

</script>

</body>

</html>
`);
    }
);

// =====================================================
// ❤️ HEALTH CHECK
// =====================================================

app.get(
    '/health',
    (req, res) => {

        res.json({

            status: 'OK',

            environment:
                'BINANCE TESTNET',

            uptime:
                process.uptime(),

            validSymbols:
                validSymbols.size,

            scannerRunning,

            tradingPaused

        });
    }
);

// =====================================================
// 🚀 START SERVER
// =====================================================

app.listen(
    PORT,
    async () => {

        console.log('');
        console.log(
            '🚀 LOMY BOT STARTED'
        );

        console.log(
            'Environment: BINANCE TESTNET'
        );

        console.log(
            `Max Positions: ${CONFIG.maxPositions}`
        );

        console.log(
            `Minimum Score: ${CONFIG.minimumScore}`
        );

        console.log(
            `Port: ${PORT}`
        );

        console.log('');

        const loaded =
            await loadExchangeRules();

        if (!loaded) {

            console.error(
                '🛑 Could not load Binance symbols.'
            );

            return;
        }

        const wallet =
            await updateWalletBalance();

        console.log(
            wallet
                ? `💰 Equity: $${getTotalEquity().toFixed(2)}`
                : '⚠️ Wallet balance unavailable'
        );

        console.log(
            `✅ Valid symbols: ${validSymbols.size}`
        );

        console.log(
            '🔎 Starting scanner...'
        );

        runFullScan();

        setInterval(
            async () => {
                await updateWalletBalance();
            },
            60000
        );

        // Refresh exchange rules every 30 minutes
        setInterval(
            async () => {
                await loadExchangeRules();
            },
            30 * 60 * 1000
        );
    }
);

// =====================================================
// 🛑 GLOBAL ERROR HANDLING
// =====================================================

process.on(
    'unhandledRejection',
    error => {

        console.error(
            '❌ Unhandled rejection:',
            error
        );
    }
);

process.on(
    'uncaughtException',
    error => {

        console.error(
            '❌ Uncaught exception:',
            error
        );
    }
);
