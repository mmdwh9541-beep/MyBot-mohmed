require('dotenv').config();
const express = require('express');
const axios = require('axios');
const crypto = require('crypto');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 5000;

// ==========================================
// 📱 1. Environment Variables (Secure)
// ==========================================
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const API_KEY = process.env.BINANCE_API_KEY;
const API_SECRET = process.env.BINANCE_API_SECRET;
const TESTNET_URL = 'https://testnet.binance.vision'; 

// ==========================================
// ⚙️ 2. Risk Management & Global Variables
// ==========================================
const RISK_RULES = {
    tradeAmountUSDT: 100,        
    stopLossPct: 0.02,    // وقف خسارة ثابت 2%
    takeProfitPct: 0.04   // جني أرباح ثابت 4%
};

let exchangeRules = {}; 
let latestResults = [];
let activePositions = {}; 
let tradeHistory = []; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitUSDT: 0 };
let liveWalletBalance = "0.00"; 

// ==========================================
// 🔔 3. Utilities (Telegram & Precision)
// ==========================================
async function sendTelegramMessage(text) {
    if (!TELEGRAM_TOKEN || !CHAT_ID) return;
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    try {
        await axios.post(url, { chat_id: CHAT_ID, text: text, parse_mode: 'HTML' });
    } catch (error) {}
}

async function loadExchangeRules() {
    try {
        console.log('🔄 Loading Binance Exchange Rules (Lot Sizes)...');
        const res = await axios.get(`${TESTNET_URL}/api/v3/exchangeInfo`);
        res.data.symbols.forEach(s => {
            const lotSize = s.filters.find(f => f.filterType === 'LOT_SIZE');
            exchangeRules[s.symbol] = {
                stepSize: lotSize ? parseFloat(lotSize.stepSize) : 1
            };
        });
        console.log('✅ Exchange Rules Loaded Successfully.');
    } catch (e) { console.error('❌ Error loading exchange rules'); }
}

function formatQuantity(symbol, qty) {
    if (!exchangeRules[symbol]) return qty.toString();
    const stepSize = exchangeRules[symbol].stepSize;
    const precision = stepSize.toString().includes('.') ? stepSize.toString().split('.')[1].length : 0;
    const factor = Math.pow(10, precision);
    return (Math.floor(qty * factor) / factor).toFixed(precision);
}

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
    } catch (error) { return null; }
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
    if (side === 'BUY') {
        params.quoteOrderQty = RISK_RULES.tradeAmountUSDT; 
    } else {
        params.quantity = formatQuantity(symbol, quantity);
    }
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

// ==========================================
// 🔄 5. Bulletproof Recovery System
// ==========================================
async function recoverActivePositions() {
    const data = await binancePrivateRequest('/api/v3/account', 'GET');
    if (data && data.balances) {
        for (let b of data.balances) {
            const qty = parseFloat(b.free);
            if (b.asset !== 'USDT' && qty > 0.001) {
                const symbol = b.asset + 'USDT';
                try {
                    const myTrades = await binancePrivateRequest('/api/v3/myTrades', 'GET', { symbol: symbol, limit: 1 });
                    if (myTrades && myTrades.length > 0) {
                        const realEntryPrice = parseFloat(myTrades[0].price);
                        activePositions[symbol] = {
                            entryPrice: realEntryPrice,
                            qty: qty,
                            stopLoss: realEntryPrice * (1 - RISK_RULES.stopLossPct),
                            takeProfit: realEntryPrice * (1 + RISK_RULES.takeProfitPct),
                            time: new Date().toLocaleString()
                        };
                    }
                } catch (e) {}
            }
        }
    }
}

// ==========================================
// 🧠 6. Auto-Trading Engine (Execution)
// ==========================================
async function processTradeAction(symbol, currentPrice, decision) {
    // 1. الشراء عند ظهور الإشارة من المؤشر الداخلي
    if (decision === 'BUY' && !activePositions[symbol]) {
        console.log(`\n⏳ [MARKET BUY - Scanner Signal] ${symbol}...`);
        const orderResult = await executeTrade(symbol, 'BUY');
        if (orderResult && orderResult.status === 'FILLED') {
            const entryPrice = parseFloat(orderResult.fills[0] ? orderResult.fills[0].price : currentPrice);
            const qtyBought = parseFloat(orderResult.executedQty);
            
            activePositions[symbol] = {
                entryPrice, qty: qtyBought,
                stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct),
                takeProfit: entryPrice * (1 + RISK_RULES.takeProfitPct),
                time: new Date().toLocaleString()
            };
            
            sendTelegramMessage(`🟢 <b>شراء جديد (المؤشر الداخلي)</b>\n<b>العملة:</b> ${symbol}\n<b>الدخول:</b> $${entryPrice}\n<b>الهدف:</b> $${activePositions[symbol].takeProfit.toFixed(4)}\n<b>الوقف:</b> $${activePositions[symbol].stopLoss.toFixed(4)}`);
            updateWalletBalance(); 
            return 'BOUGHT';
        }
    }

    // 2. إدارة الصفقات والبيع
    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        
        const hitSL = currentPrice <= trade.stopLoss;
        const hitTP = currentPrice >= trade.takeProfit;
        const forceSell = decision === 'SELL';

        if (hitSL || hitTP || forceSell) {
            console.log(`\n⏳ [MARKET SELL] ${symbol}...`);
            const orderResult = await executeTrade(symbol, 'SELL', trade.qty);
            
            if (orderResult && orderResult.status === 'FILLED') {
                const profitUSDT = (currentPrice - trade.entryPrice) * trade.qty; 
                let exitReason = hitSL ? 'ضرب وقف الخسارة (2%)' : (hitTP ? 'ضرب هدف الربح (4%)' : 'إشارة بيع عكسية من المؤشر');
                
                testStats.totalTrades++;
                if (profitUSDT > 0) testStats.winningTrades++;
                testStats.totalProfitUSDT += profitUSDT;

                tradeHistory.unshift({ time: new Date().toLocaleTimeString(), symbol, reason: exitReason, profitUSDT: profitUSDT.toFixed(2) });
                if (tradeHistory.length > 50) tradeHistory.pop();
                
                const emoji = profitUSDT >= 0 ? '✅ ربح' : '❌ خسارة';
                sendTelegramMessage(`🔴 <b>تم البيع</b>\n<b>العملة:</b> ${symbol}\n<b>السبب:</b> ${exitReason}\n<b>النتيجة:</b> ${emoji} ($${profitUSDT.toFixed(2)} USDT)`);

                delete activePositions[symbol]; 
                updateWalletBalance(); 
                return `CLOSED ($${profitUSDT.toFixed(2)})`;
            }
        }
        return `HOLDING (SL: $${trade.stopLoss.toFixed(4)} | TP: $${trade.takeProfit.toFixed(4)})`;
    }
    return decision; 
}

// ==========================================
// 📊 7. Internal Scanner Logic (The Brain)
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
async function analyzeMarket(symbol) {
    try {
        const res = await axios.get(`${TESTNET_URL}/api/v3/klines?symbol=${symbol}&interval=15m&limit=30`);
        const candles = res.data.map(c => ({ open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]), close: parseFloat(c[4]), volume: parseFloat(c[5]) }));
        const currentPrice = candles[candles.length - 1].close; 
        
        const volSMA = calculateSMA(candles, 10, 'volume');
        const cmo = calculateCMO(candles, 9);
        const candle = candles[candles.length - 2];
        const highVolume = candle.volume > (volSMA[volSMA.length - 2] * 1.3);
        const bodyRatio = (candle.high - candle.low) > 0 ? (Math.abs(candle.close - candle.open) / (candle.high - candle.low)) : 0;
        
        let decision = 'WAIT';
        
        // شروط الدخول بناءً على المؤشر المدمج
        if (candle.close > candle.open && bodyRatio > 0.5 && highVolume && cmo[cmo.length - 2] > 30) decision = 'BUY';
        if (candle.close < candle.open && bodyRatio > 0.5 && highVolume && cmo[cmo.length - 2] < -30) decision = 'SELL';

        const tradeStatus = await processTradeAction(symbol, currentPrice, decision);
        return { symbol, decision: tradeStatus, cmo: cmo[cmo.length - 2].toFixed(2), spike: highVolume ? 'YES' : 'NO' };
    } catch (e) { return null; }
}

async function runFullScan() {
    try {
        const response = await axios.get(`${TESTNET_URL}/api/v3/ticker/24hr`);
        let topCoins = response.data.filter(t => t.symbol.endsWith('USDT') && !t.symbol.includes('USDC'))
            .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume)).slice(0, 10).map(t => t.symbol);
        
        let currentScan = [];
        for (const coin of topCoins) {
            const result = await analyzeMarket(coin);
            if (result) currentScan.push(result);
            await new Promise(resolve => setTimeout(resolve, 200));
        }
        latestResults = currentScan; 
    } catch(e) {}
}

// ==========================================
// 🌐 8. Web Server (Dashboard Only)
// ==========================================
app.get('/api/data', (req, res) => {
    res.json({ live: latestResults, stats: testStats, history: tradeHistory, balance: liveWalletBalance });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Pro Binance Bot</title><style>body{background-color:#0b0e11;color:#eaecef;font-family:Arial;text-align:center;padding:20px;}h1{color:#f3ba2f;}.wallet{font-size:24px;color:#0ecb81;margin-bottom:20px;font-weight:bold;border:2px dashed #2b3139;padding:10px;display:inline-block;border-radius:10px;}.stats-container{display:flex;justify-content:center;gap:20px;margin-bottom:20px;}.stat-box{background-color:#1e2329;padding:15px 30px;border-radius:8px;font-weight:bold;border:1px solid #2b3139;}table{width:90%;max-width:1000px;margin:10px auto;border-collapse:collapse;background-color:#1e2329;border-radius:8px;}th,td{padding:12px;border-bottom:1px solid #2b3139;}th{background-color:#2b3139;color:#848e9c;}.buy{color:#0ecb81;font-weight:bold;}.sell{color:#f6465d;font-weight:bold;}.wait{color:#848e9c;}.spike{color:#f3ba2f;font-weight:bold;}</style></head><body><h1>🤖 LOMY Ultra-Fast Engine</h1><div class="wallet">💰 Balance: $<span id="wallet-balance">Loading...</span> USDT</div><div class="stats-container"><div class="stat-box">Trades: <span id="tot-trades">0</span></div><div class="stat-box">Profit: <span id="net-profit">$0.00</span></div></div><table><thead><tr><th>Symbol</th><th>Status</th><th>CMO</th><th>Whale</th></tr></thead><tbody id="live-table"><tr><td colspan="4">Scanning...</td></tr></tbody></table><script>async function loadData(){try{const res=await fetch('/api/data');const data=await res.json();document.getElementById('wallet-balance').innerText=data.balance;document.getElementById('tot-trades').innerText=data.stats.totalTrades;let profitEl=document.getElementById('net-profit');profitEl.innerText='$' + data.stats.totalProfitUSDT.toFixed(2);profitEl.className=data.stats.totalProfitUSDT>=0?'buy':'sell';if(data.live.length>0){let liveTbody=document.getElementById('live-table');liveTbody.innerHTML='';data.live.forEach(item=>{let decClass=item.decision.includes('BOUGHT')||item.decision.includes('HOLDING')?'buy':item.decision.includes('SELL')||item.decision.includes('CLOSED')?'sell':'wait';liveTbody.innerHTML+=\`<tr><td>\${item.symbol}</td><td class="\${decClass}">\${item.decision}</td><td>\${item.cmo}</td><td class="\${item.spike.includes('YES')?'spike':''}">\${item.spike}</td></tr>\`;});}}catch(e){}}setInterval(loadData,4000);loadData();</script></body></html>`);
});

// ==========================================
// 🚀 9. Initialization
// ==========================================
app.listen(PORT, async () => { 
    console.log('🚀 Server is running on port ' + PORT); 
    await loadExchangeRules();
    setTimeout(recoverActivePositions, 3000); 
    setTimeout(updateWalletBalance, 2000); 
    setInterval(updateWalletBalance, 60000);
    setInterval(runFullScan, 15000);
    runFullScan();
});
