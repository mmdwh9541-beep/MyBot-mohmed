require('dotenv').config();
const express = require('express');
const axios = require('axios');
const crypto = require('crypto');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 5000;

// ==========================================
// 📱 1. Telegram Settings (From Environment)
// ==========================================
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;

async function sendTelegramMessage(text) {
    if (!TELEGRAM_TOKEN || !CHAT_ID) return;
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    try {
        await axios.post(url, { chat_id: CHAT_ID, text: text, parse_mode: 'HTML' });
    } catch (error) {
        console.error('❌ Telegram Error:', error.message);
    }
}

// ==========================================
// 🔑 2. Binance API Settings (From Environment)
// ==========================================
const API_KEY = process.env.BINANCE_API_KEY;
const API_SECRET = process.env.BINANCE_API_SECRET;
const TESTNET_URL = process.env.USE_TESTNET === 'false' ? 'https://api.binance.com' : 'https://testnet.binance.vision';

// ==========================================
// ⚙️ 3. Risk Management & Global Variables
// ==========================================
const RISK_RULES = {
    tradeAmountUSDT: 100,        
    stopLossPct: 0.03,           
    trailingActivationPct: 0.05, 
    trailingDistancePct: 0.025   
};

let latestResults = [];
let activePositions = {}; 
let tradeHistory = []; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitPct: 0 };
let liveWalletBalance = "0.00"; 

// ==========================================
// 🔐 4. Binance API Request Functions
// ==========================================
async function binancePrivateRequest(endpoint, method = 'GET', params = {}) {
    if (!API_KEY || !API_SECRET) return null;
    params.timestamp = Date.now();
    params.recvWindow = 60000; 

    const queryString = Object.keys(params).map(key => `${key}=${encodeURIComponent(params[key])}`).join('&');
    const signature = crypto.createHmac('sha256', API_SECRET).update(queryString).digest('hex');
    const url = `${TESTNET_URL}${endpoint}?${queryString}&signature=${signature}`;

    try {
        const response = await axios({ method: method, url: url, headers: { 'X-MBX-APIKEY': API_KEY } });
        return response.data;
    } catch (error) {
        console.error(`❌ API Error (${endpoint}):`, error.response ? error.response.data.msg : error.message);
        return null;
    }
}

async function updateWalletBalance() {
    const data = await binancePrivateRequest('/api/v3/account', 'GET');
    if (data && data.balances) {
        const usdt = data.balances.find(b => b.asset === 'USDT');
        if (usdt) liveWalletBalance = parseFloat(usdt.free).toFixed(2);
    }
}

async function executeTrade(symbol, side, quantity = null) {
    let params = { symbol: symbol, side: side, type: 'MARKET' };
    if (side === 'BUY') params.quoteOrderQty = RISK_RULES.tradeAmountUSDT; 
    else params.quantity = quantity; 
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

// ==========================================
// 🔄 5. Recover Lost Positions (Memory Rescue)
// ==========================================
async function recoverActivePositions() {
    console.log('🔄 Checking wallet to recover lost positions...');
    const data = await binancePrivateRequest('/api/v3/account', 'GET');
    
    if (data && data.balances) {
        for (let b of data.balances) {
            const qty = parseFloat(b.free);
            if (b.asset !== 'USDT' && qty > 0.001) {
                const symbol = b.asset + 'USDT';
                try {
                    const priceRes = await axios.get(`${TESTNET_URL}/api/v3/ticker/price?symbol=${symbol}`);
                    const currentPrice = parseFloat(priceRes.data.price);
                    
                    activePositions[symbol] = {
                        entryPrice: currentPrice,
                        qty: qty,
                        highestPrice: currentPrice,
                        stopLoss: currentPrice * (1 - RISK_RULES.stopLossPct),
                        trailingActive: false,
                        time: new Date().toLocaleString()
                    };
                    console.log(`✅ Recovered: ${symbol} | Qty: ${qty} | Price: $${currentPrice}`);
                    
                    const msg = `🔄 <b>Position Recovered</b>\n<b>Symbol:</b> ${symbol}\n<b>Quantity:</b> ${qty}\n<b>Tracking Price:</b> $${currentPrice}`;
                    sendTelegramMessage(msg);
                } catch (e) { console.error(`⚠️ Could not recover ${symbol}:`, e.message); }
            }
        }
    }
}

// ==========================================
// 🧠 6. Auto-Trading Engine (Analysis & Execution)
// ==========================================
function calculateSMA(data, period, key = 'volume') {
    let smaArray = [];
    for (let i = 0; i < data.length; i++) {
        if (i < period - 1) { smaArray.push(null); continue; }
        let sum = 0;
        for (let j = 0; j < period; j++) { sum += data[i - j][key]; }
        smaArray.push(sum / period);
    }
    return smaArray;
}

function calculateCMO(data, period) {
    let cmoArray = [];
    for (let i = 0; i < data.length; i++) {
        if (i < period) { cmoArray.push(null); continue; }
        let sumUp = 0, sumDown = 0;
        for (let j = 0; j < period; j++) {
            let diff = data[i - j].close - data[i - j - 1].close;
            if (diff > 0) sumUp += diff; else sumDown += Math.abs(diff);
        }
        let cmo = (sumUp + sumDown === 0) ? 0 : 100 * ((sumUp - sumDown) / (sumUp + sumDown));
        cmoArray.push(cmo);
    }
    return cmoArray;
}

async function getTopActiveCoins(limit = 15) {
    try {
        const response = await axios.get(`${TESTNET_URL}/api/v3/ticker/24hr`);
        const usdtPairs = response.data.filter(t => t.symbol.endsWith('USDT') && !t.symbol.includes('USDC'));
        usdtPairs.sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume));
        return usdtPairs.slice(0, limit).map(t => t.symbol);
    } catch (e) { return ['BTCUSDT']; }
}

async function managePosition(symbol, currentPrice, decision) {
    if (decision === 'BUY' && !activePositions[symbol]) {
        console.log(`\n⏳ [ATTEMPTING BUY] ${symbol}...`);
        const orderResult = await executeTrade(symbol, 'BUY');
        if (orderResult && orderResult.status === 'FILLED') {
            const entryPrice = parseFloat(orderResult.fills[0] ? orderResult.fills[0].price : currentPrice);
            const qtyBought = parseFloat(orderResult.executedQty);
            
            activePositions[symbol] = {
                entryPrice, qty: qtyBought, highestPrice: entryPrice,
                stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct),
                trailingActive: false, time: new Date().toLocaleString()
            };
            
            sendTelegramMessage(`🟢 <b>BUY EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Price:</b> $${entryPrice}\n<b>Quantity:</b> ${qtyBought}`);
            updateWalletBalance(); 
            return 'BOUGHT';
        }
    }

    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        if (currentPrice > trade.highestPrice) trade.highestPrice = currentPrice;

        const profitPct = (currentPrice - trade.entryPrice) / trade.entryPrice;
        if (!trade.trailingActive && profitPct >= RISK_RULES.trailingActivationPct) {
            trade.trailingActive = true;
        }
        if (trade.trailingActive) {
            const newSL = trade.highestPrice * (1 - RISK_RULES.trailingDistancePct);
            if (newSL > trade.stopLoss) trade.stopLoss = newSL;
        }

        if (currentPrice <= trade.stopLoss || decision === 'SELL') {
            console.log(`\n⏳ [ATTEMPTING SELL] ${symbol}...`);
            const safeQty = Math.floor(trade.qty * 1000) / 1000; 
            const orderResult = await executeTrade(symbol, 'SELL', safeQty);
            
            if (orderResult && orderResult.status === 'FILLED') {
                const finalProfitPct = parseFloat(((currentPrice - trade.entryPrice) / trade.entryPrice * 100).toFixed(2));
                const exitReason = currentPrice <= trade.stopLoss ? 'Stop Loss Hit' : 'SELL Signal';
                
                testStats.totalTrades++;
                if (finalProfitPct > 0) testStats.winningTrades++;
                testStats.totalProfitPct += finalProfitPct;

                tradeHistory.unshift({ time: new Date().toLocaleTimeString(), symbol, reason: exitReason, profitPct: finalProfitPct });
                if (tradeHistory.length > 50) tradeHistory.pop();
                
                const emoji = finalProfitPct >= 0 ? '✅ PROFIT' : '❌ LOSS';
                sendTelegramMessage(`🔴 <b>SELL EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Reason:</b> ${exitReason}\n<b>Result:</b> ${emoji} (${finalProfitPct}%)`);

                delete activePositions[symbol]; 
                updateWalletBalance(); 
                return `CLOSED (${finalProfitPct}%)`;
            }
        }
        return `HOLDING (SL: $${trade.stopLoss.toFixed(4)})`;
    }
    return decision; 
}

async function analyzeMarket(symbol, interval) {
    try {
        const url = `${TESTNET_URL}/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=30`;
        const response = await axios.get(url);
        const candles = response.data.map(c => ({ open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]), close: parseFloat(c[4]), volume: parseFloat(c[5]) }));

        const currentPrice = candles[candles.length - 1].close; 
        const volSMA = calculateSMA(candles, 10, 'volume');
        const cmo = calculateCMO(candles, 9);
        const candle = candles[candles.length - 2];

        const highVolume = candle.volume > (volSMA[volSMA.length - 2] * 1.3);
        const bodyRatio = (candle.high - candle.low) > 0 ? (Math.abs(candle.close - candle.open) / (candle.high - candle.low)) : 0;
        const bullish = candle.close > candle.open && bodyRatio > 0.5 && highVolume;
        const bearish = candle.close < candle.open && bodyRatio > 0.5 && highVolume;

        let decision = 'WAIT';
        if (bullish && cmo[cmo.length - 2] > 30) decision = 'BUY';
        if (bearish && cmo[cmo.length - 2] < -30) decision = 'SELL';

        const tradeStatus = await managePosition(symbol, currentPrice, decision);
        return { symbol, decision: tradeStatus, cmo: cmo[cmo.length - 2].toFixed(2), spike: highVolume ? 'YES' : 'NO' };
    } catch (e) { return null; }
}

async function runFullScan() {
    const topCoins = await getTopActiveCoins(15);
    let currentScan = [];
    for (const coin of topCoins) {
        const result = await analyzeMarket(coin, '15m');
        if (result) currentScan.push(result);
        await new Promise(resolve => setTimeout(resolve, 200));
    }
    latestResults = currentScan; 
}

// 7. Core Loops & Timers
setTimeout(recoverActivePositions, 3000); 
setTimeout(updateWalletBalance, 2000); 
setInterval(updateWalletBalance, 60000);
setInterval(runFullScan, 15000);
runFullScan();

// ==========================================
// 🌐 8. Web Routes (Webhook + Dashboard)
// ==========================================

app.post('/webhook', (req, res) => {
    const alertData = req.body;
    sendTelegramMessage(`🚨 <b>TradingView Alert</b> 🚨\n<b>Symbol:</b> ${alertData.symbol || 'N/A'}\n<b>Action:</b> ${alertData.action || 'N/A'}\n<b>Price:</b> ${alertData.price || 'N/A'}`);
    res.status(200).send('Alert Received');
});

app.get('/api/data', (req, res) => {
    res.json({ live: latestResults, stats: testStats, history: tradeHistory, balance: liveWalletBalance });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Binance Testnet Bot</title><style>body{background-color:#0b0e11;color:#eaecef;font-family:Arial;text-align:center;padding:20px;}h1{color:#f3ba2f;}.wallet{font-size:24px;color:#0ecb81;margin-bottom:20px;font-weight:bold;border:2px dashed #2b3139;padding:10px;display:inline-block;border-radius:10px;}.stats-container{display:flex;justify-content:center;gap:20px;margin-bottom:20px;}.stat-box{background-color:#1e2329;padding:15px 30px;border-radius:8px;font-weight:bold;border:1px solid #2b3139;}table{width:90%;max-width:1000px;margin:10px auto;border-collapse:collapse;background-color:#1e2329;border-radius:8px;}th,td{padding:12px;border-bottom:1px solid #2b3139;}th{background-color:#2b3139;color:#848e9c;}.buy{color:#0ecb81;font-weight:bold;}.sell{color:#f6465d;font-weight:bold;}.wait{color:#848e9c;}.spike{color:#f3ba2f;font-weight:bold;}</style></head><body><h1>🤖 LOMY Ultra-Fast Engine</h1><div class="wallet">💰 Balance: $<span id="wallet-balance">Loading...</span> USDT</div><div class="stats-container"><div class="stat-box">Trades: <span id="tot-trades">0</span></div><div class="stat-box">Profit: <span id="net-profit">0.00%</span></div></div><table><thead><tr><th>Symbol</th><th>Status</th><th>CMO</th><th>Whale</th></tr></thead><tbody id="live-table"><tr><td colspan="4">Scanning...</td></tr></tbody></table><script>async function loadData(){try{const res=await fetch('/api/data');const data=await res.json();document.getElementById('wallet-balance').innerText=data.balance;document.getElementById('tot-trades').innerText=data.stats.totalTrades;let profitEl=document.getElementById('net-profit');profitEl.innerText=data.stats.totalProfitPct.toFixed(2)+'%';profitEl.className=data.stats.totalProfitPct>=0?'buy':'sell';if(data.live.length>0){let liveTbody=document.getElementById('live-table');liveTbody.innerHTML='';data.live.forEach(item=>{let decClass=item.decision.includes('BOUGHT')||item.decision.includes('HOLDING')?'buy':item.decision.includes('SELL')||item.decision.includes('CLOSED')?'sell':'wait';liveTbody.innerHTML+=\`<tr><td>\${item.symbol}</td><td class="\${decClass}">\${item.decision}</td><td>\${item.cmo}</td><td class="\${item.spike.includes('YES')?'spike':''}">\${item.spike}</td></tr>\`;});}}catch(e){}}setInterval(loadData,4000);loadData();</script></body></html>`);
});

app.listen(PORT, () => { console.log('🚀 LOMY Server running on port ' + PORT); });
