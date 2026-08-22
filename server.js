const express = require('express');
const axios = require('axios');
const crypto = require('crypto');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 5000;

// ==========================================
// 📱 1. Telegram Settings
// ==========================================
const TELEGRAM_TOKEN = '8956340113:AAGyr_IZdKMniNLYPeTnl-NoUzZzbI5hAiI';
const CHAT_ID = '8708481752';

async function sendTelegramMessage(text) {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    try {
        await axios.post(url, { chat_id: CHAT_ID, text: text, parse_mode: 'HTML' });
    } catch (error) {
        console.error('❌ Telegram Error:', error.message);
    }
}

// ==========================================
// ⚙️ 2. Dynamic API & Mode Configuration
// ==========================================
let CONFIG = {
    API_KEY: 'Vwsc1ALWhJlxKZbgq9cCi9UiFOqDya9kleWof1FzZHxSLJ6uytpnybyV5zwY4Yj2',
    API_SECRET: 'eJhpQdGiTl1B8rKLFrnt84C4FSfwFCWeODcE9M5nHQZMjLJurFSKqILoh4r0vgCM',
    isTestnet: true,
    isBotActive: true // 🟢 Bot Status (Active/Paused)
};

function getBaseUrl() {
    return CONFIG.isTestnet ? 'https://testnet.binance.vision' : 'https://api.binance.com';
}

// ==========================================
// 📊 3. Risk Management & Global Variables
// ==========================================
const RISK_RULES = {
    tradeAmountUSDT: 100,        
    stopLossPct: 0.015,          // 1.5% Stop Loss
    trailingActivationPct: 0.03, // 3% Trailing Activation
    trailingDistancePct: 0.01    // 1% Trailing Distance to secure profit
};

let latestResults = [];
let activePositions = {}; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitPct: 0 };
let liveWalletBalance = "0.00"; 

// ==========================================
// 🔐 4. Binance API Request Functions
// ==========================================
async function binancePrivateRequest(endpoint, method = 'GET', params = {}) {
    if (!CONFIG.API_KEY || !CONFIG.API_SECRET) return null;
    params.timestamp = Date.now();
    params.recvWindow = 60000; 

    const queryString = Object.keys(params).map(key => `${key}=${encodeURIComponent(params[key])}`).join('&');
    const signature = crypto.createHmac('sha256', CONFIG.API_SECRET).update(queryString).digest('hex');
    const url = `${getBaseUrl()}${endpoint}?${queryString}&signature=${signature}`;

    try {
        const response = await axios({ method: method, url: url, headers: { 'X-MBX-APIKEY': CONFIG.API_KEY } });
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
    if (!CONFIG.isBotActive && side === 'BUY') return null; // Prevent buy if paused
    let params = { symbol: symbol, side: side, type: 'MARKET' };
    if (side === 'BUY') params.quoteOrderQty = RISK_RULES.tradeAmountUSDT; 
    else params.quantity = quantity; 
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

// ==========================================
// 🔄 5. Recover Lost Positions
// ==========================================
async function recoverActivePositions() {
    const data = await binancePrivateRequest('/api/v3/account', 'GET');
    if (data && data.balances) {
        for (let b of data.balances) {
            const qty = parseFloat(b.free);
            if (b.asset !== 'USDT' && qty > 0.001) {
                const symbol = b.asset + 'USDT';
                try {
                    const priceRes = await axios.get(`${getBaseUrl()}/api/v3/ticker/price?symbol=${symbol}`);
                    const currentPrice = parseFloat(priceRes.data.price);
                    
                    activePositions[symbol] = {
                        entryPrice: currentPrice, qty: qty, highestPrice: currentPrice,
                        stopLoss: currentPrice * (1 - RISK_RULES.stopLossPct), trailingActive: false, time: new Date().toLocaleString()
                    };
                } catch (e) {}
            }
        }
    }
}

// ==========================================
// 🧠 6. Auto-Trading Engine
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

async function managePosition(symbol, currentPrice, decision) {
    // New buys only if bot is active
    if (decision === 'BUY' && !activePositions[symbol] && CONFIG.isBotActive) {
        const orderResult = await executeTrade(symbol, 'BUY');
        if (orderResult && orderResult.status === 'FILLED') {
            const entryPrice = parseFloat(orderResult.fills[0] ? orderResult.fills[0].price : currentPrice);
            const qtyBought = parseFloat(orderResult.executedQty);
            activePositions[symbol] = { entryPrice, qty: qtyBought, highestPrice: entryPrice, stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct), trailingActive: false, time: new Date().toLocaleString() };
            sendTelegramMessage(`🟢 <b>BUY EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Price:</b> $${entryPrice}\n<b>Mode:</b> ${CONFIG.isTestnet ? 'TESTNET' : 'REAL'}`);
            updateWalletBalance(); return 'BOUGHT';
        }
    }

    // Manage open positions (runs even if bot is paused to avoid stuck funds)
    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        if (currentPrice > trade.highestPrice) trade.highestPrice = currentPrice;

        const profitPct = (currentPrice - trade.entryPrice) / trade.entryPrice;
        if (!trade.trailingActive && profitPct >= RISK_RULES.trailingActivationPct) trade.trailingActive = true;
        if (trade.trailingActive) {
            const newSL = trade.highestPrice * (1 - RISK_RULES.trailingDistancePct);
            if (newSL > trade.stopLoss) trade.stopLoss = newSL;
        }

        if (currentPrice <= trade.stopLoss || decision === 'SELL') {
            const safeQty = Math.floor(trade.qty * 1000) / 1000; 
            const orderResult = await executeTrade(symbol, 'SELL', safeQty);
            if (orderResult && orderResult.status === 'FILLED') {
                const finalProfitPct = parseFloat(((currentPrice - trade.entryPrice) / trade.entryPrice * 100).toFixed(2));
                testStats.totalTrades++;
                if (finalProfitPct > 0) testStats.winningTrades++;
                testStats.totalProfitPct += finalProfitPct;
                const emoji = finalProfitPct >= 0 ? '✅' : '❌';
                sendTelegramMessage(`🔴 <b>SELL EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Result:</b> ${emoji} (${finalProfitPct}%)`);
                delete activePositions[symbol]; updateWalletBalance(); return `CLOSED (${finalProfitPct}%)`;
            }
        }
        return `HOLDING (SL: $${trade.stopLoss.toFixed(4)})`;
    }
    return CONFIG.isBotActive ? decision : 'BOT PAUSED'; 
}

async function analyzeMarket(symbol, interval) {
    try {
        const url = `${getBaseUrl()}/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=30`;
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

async function getTopActiveCoins(limit = 15) {
    try {
        const response = await axios.get(`${getBaseUrl()}/api/v3/ticker/24hr`);
        const usdtPairs = response.data.filter(t => t.symbol.endsWith('USDT') && !t.symbol.includes('USDC'));
        usdtPairs.sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume));
        return usdtPairs.slice(0, limit).map(t => t.symbol);
    } catch (e) { return ['BTCUSDT']; }
}

async function runFullScan() {
    if (!CONFIG.isBotActive) {
        // If bot is paused, only monitor and sell open positions if targets are hit
        for (const symbol in activePositions) {
             try {
                const priceRes = await axios.get(`${getBaseUrl()}/api/v3/ticker/price?symbol=${symbol}`);
                await managePosition(symbol, parseFloat(priceRes.data.price), 'WAIT');
             } catch(e) {}
        }
        latestResults = [{symbol: 'SYSTEM', decision: 'BOT PAUSED 🔴', cmo: '-', spike: '-'}];
        return;
    }

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
// 🌐 8. Web Routes (Settings + Dashboard)
// ==========================================

app.post('/api/config', (req, res) => {
    const { apiKey, apiSecret, isTestnet, isBotActive } = req.body;
    
    if (isBotActive !== undefined && CONFIG.isBotActive !== isBotActive) {
        CONFIG.isBotActive = isBotActive;
        sendTelegramMessage(`⚙️ <b>Bot Status:</b> ${isBotActive ? '🟢 STARTED' : '🔴 PAUSED'}`);
        return res.json({ success: true, message: 'Bot status updated' });
    }

    if(apiKey) CONFIG.API_KEY = apiKey;
    if(apiSecret) CONFIG.API_SECRET = apiSecret;
    if(isTestnet !== undefined) CONFIG.isTestnet = isTestnet;
    
    activePositions = {};
    liveWalletBalance = "0.00";
    latestResults = [];
    
    updateWalletBalance();
    recoverActivePositions();
    
    sendTelegramMessage(`⚙️ <b>Settings Updated</b>\n<b>Mode:</b> ${CONFIG.isTestnet ? 'TESTNET' : 'REAL ACCOUNT'}`);
    res.json({ success: true, message: 'Settings saved and engine restarted' });
});

app.get('/api/data', (req, res) => {
    res.json({ live: latestResults, stats: testStats, balance: liveWalletBalance, mode: CONFIG.isTestnet ? 'TESTNET' : 'REAL', isActive: CONFIG.isBotActive });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>LOMY Bot Control</title><style>body{background-color:#0b0e11;color:#eaecef;font-family:Arial;text-align:center;padding:20px;}h1{color:#f3ba2f;}.wallet{font-size:20px;color:#0ecb81;margin:15px;font-weight:bold;border:2px dashed #2b3139;padding:10px;display:inline-block;border-radius:10px;}.panel{background-color:#1e2329;padding:20px;border-radius:8px;max-width:500px;margin:20px auto;border:1px solid #2b3139;}input,select{width:90%;padding:10px;margin:10px 0;background:#0b0e11;color:white;border:1px solid #2b3139;border-radius:4px;}.btn-save{padding:12px 20px;background:#f3ba2f;color:#000;border:none;font-weight:bold;cursor:pointer;border-radius:4px;width:95%;margin-top:10px;}.btn-on{padding:12px;background:#0ecb81;color:#fff;border:none;font-weight:bold;cursor:pointer;border-radius:4px;width:45%;}.btn-off{padding:12px;background:#f6465d;color:#fff;border:none;font-weight:bold;cursor:pointer;border-radius:4px;width:45%;}table{width:100%;max-width:800px;margin:20px auto;border-collapse:collapse;background-color:#1e2329;border-radius:8px;}th,td{padding:12px;border-bottom:1px solid #2b3139;}th{background-color:#2b3139;color:#848e9c;}.buy{color:#0ecb81;font-weight:bold;}.sell{color:#f6465d;font-weight:bold;}.wait{color:#848e9c;}</style></head><body>
    
    <h1>🤖 LOMY Ultra-Fast Engine</h1>
    
    <!-- Power Controls -->
    <div class="panel" style="padding: 15px;">
        <h3 style="margin-top:0;">🛑 Power Switch: <span id="status-indicator" style="color:#0ecb81;">RUNNING</span></h3>
        <div style="display:flex; justify-content:space-around;">
            <button class="btn-on" onclick="toggleBot(true)">🟢 START BOT</button>
            <button class="btn-off" onclick="toggleBot(false)">🔴 STOP BOT</button>
        </div>
    </div>

    <!-- Settings Panel -->
    <div class="panel">
        <h3 style="margin-top:0;">⚙️ API Settings</h3>
        <input type="text" id="api-key" placeholder="Enter Binance API Key">
        <input type="password" id="api-secret" placeholder="Enter Binance API Secret">
        <select id="mode">
            <option value="true">🧪 Testnet Mode (Demo)</option>
            <option value="false">💰 Real Account Mode</option>
        </select>
        <button class="btn-save" onclick="saveSettings()">Save & Restart Engine</button>
        <p id="msg" style="color:#0ecb81;display:none;margin-top:10px;">✅ Updated Successfully!</p>
    </div>

    <div class="wallet">💰 Balance: $<span id="wallet-balance">...</span> | <span id="current-mode" style="color:#f3ba2f;">...</span></div>
    
    <div style="display:flex;justify-content:center;gap:15px;margin-bottom:20px;">
        <div class="panel" style="margin:0;padding:10px 20px;">Trades: <span id="tot-trades">0</span></div>
        <div class="panel" style="margin:0;padding:10px 20px;">Profit: <span id="net-profit">0.00%</span></div>
    </div>

    <table><thead><tr><th>Symbol</th><th>Status</th><th>CMO</th></tr></thead><tbody id="live-table"><tr><td colspan="3">Scanning Market...</td></tr></tbody></table>
    
    <script>
    async function toggleBot(isActive) {
        await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ isBotActive: isActive })
        });
        loadData();
    }

    async function saveSettings(){
        const apiKey = document.getElementById('api-key').value;
        const apiSecret = document.getElementById('api-secret').value;
        const isTestnet = document.getElementById('mode').value === 'true';
        
        await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ apiKey, apiSecret, isTestnet })
        });
        
        let msg = document.getElementById('msg');
        msg.style.display = 'block';
        setTimeout(() => msg.style.display = 'none', 3000);
        document.getElementById('api-key').value = '';
        document.getElementById('api-secret').value = '';
        loadData();
    }
    
    async function loadData(){
        try{
            const res = await fetch('/api/data');
            const data = await res.json();
            document.getElementById('wallet-balance').innerText = data.balance;
            document.getElementById('current-mode').innerText = data.mode;
            document.getElementById('tot-trades').innerText = data.stats.totalTrades;
            document.getElementById('net-profit').innerText = data.stats.totalProfitPct.toFixed(2)+'%';
            
            let statusInd = document.getElementById('status-indicator');
            if(data.isActive) {
                statusInd.innerText = 'RUNNING'; statusInd.style.color = '#0ecb81';
            } else {
                statusInd.innerText = 'PAUSED'; statusInd.style.color = '#f6465d';
            }

            if(data.live.length > 0){
                let liveTbody = document.getElementById('live-table');
                liveTbody.innerHTML = '';
                data.live.forEach(item => {
                    let decClass = item.decision.includes('BOUGHT')||item.decision.includes('HOLDING')||item.decision.includes('RUNNING')?'buy':item.decision.includes('SELL')||item.decision.includes('CLOSED')||item.decision.includes('PAUSED')?'sell':'wait';
                    liveTbody.innerHTML += \`<tr><td>\${item.symbol}</td><td class="\${decClass}">\${item.decision}</td><td>\${item.cmo}</td></tr>\`;
                });
            }
        }catch(e){}
    }
    setInterval(loadData, 4000); loadData();
    </script>
    </body></html>

