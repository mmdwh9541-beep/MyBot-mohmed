const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs'); 

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 5000;

// ==========================================
// 1. Telegram Settings
// ==========================================
const TELEGRAM_TOKEN = '8956340113:AAGyr_IZdKMniNLYPeTnl-NoUzZzbI5hAiI';
const CHAT_ID = '8708481752';

async function sendTelegramMessage(text) {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    try {
        await axios.post(url, { chat_id: CHAT_ID, text: text, parse_mode: 'HTML' });
    } catch (error) { console.error('Telegram Error:', error.message); }
}

// ==========================================
// 2. Configuration
// ==========================================
let CONFIG = {
    API_KEY: 'Vwsc1ALWhJlxKZbgq9cCi9UiFOqDya9kleWof1FzZHxSLJ6uytpnybyV5zwY4Yj2',
    API_SECRET: 'eJhpQdGiTl1B8rKLFrnt84C4FSfwFCWeODcE9M5nHQZMjLJurFSKqILoh4r0vgCM',
    isTestnet: true,
    isBotActive: true
};

function getBaseUrl() {
    return CONFIG.isTestnet ? 'https://testnet.binance.vision' : 'https://api.binance.com';
}

// ==========================================
// 3. Risk Management & MEMORY SYSTEM
// ==========================================
const RISK_RULES = {
    stopLossPct: 0.015,          
    trailingActivationPct: 0.03, 
    trailingDistancePct: 0.01,   
    allocationNormalPct: 0.5,    
    allocationGemPct: 0.5,       
    maxNormalTrades: 10,
    maxGemTrades: 5
};

let latestResults = [];
let activePositions = {}; 
let liveWalletBalance = "0.00"; 
const POSITIONS_FILE = './positions.json'; 

function loadPositions() {
    if (fs.existsSync(POSITIONS_FILE)) {
        try {
            const data = fs.readFileSync(POSITIONS_FILE, 'utf8');
            activePositions = JSON.parse(data);
        } catch (e) { }
    }
}

function savePositions() {
    try {
        fs.writeFileSync(POSITIONS_FILE, JSON.stringify(activePositions));
    } catch (e) { }
}

// ==========================================
// 4. API Functions
// ==========================================
async function binancePrivateRequest(endpoint, method = 'GET', params = {}) {
    if (!CONFIG.API_KEY || !CONFIG.API_SECRET) return null;
    params.timestamp = Date.now();
    const queryString = Object.keys(params).map(key => `${key}=${encodeURIComponent(params[key])}`).join('&');
    const signature = crypto.createHmac('sha256', CONFIG.API_SECRET).update(queryString).digest('hex');
    const url = `${getBaseUrl()}${endpoint}?${queryString}&signature=${signature}`;
    try {
        const response = await axios({ method, url, headers: { 'X-MBX-APIKEY': CONFIG.API_KEY } });
        return response.data;
    } catch (e) { return null; }
}

async function updateWalletBalance() {
    const data = await binancePrivateRequest('/api/v3/account', 'GET');
    if (data && data.balances) {
        const usdt = data.balances.find(b => b.asset === 'USDT');
        if (usdt) liveWalletBalance = parseFloat(usdt.free).toFixed(2);
    }
}

async function executeTrade(symbol, side, quoteQty = null, coinQty = null) {
    if (!CONFIG.isBotActive && side === 'BUY') return null;
    let params = { symbol, side, type: 'MARKET' };
    if (side === 'BUY') params.quoteOrderQty = quoteQty; 
    else params.quantity = coinQty; 
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

// ==========================================
// 5. Position Management
// ==========================================
async function managePosition(symbol, currentPrice, decision, tradeType = 'NORMAL') {
    const activeNormal = Object.values(activePositions).filter(p => p.type === 'NORMAL').length;
    const activeGems = Object.values(activePositions).filter(p => p.type === 'GEM').length;
    
    if (decision === 'BUY' && !activePositions[symbol] && CONFIG.isBotActive) {
        if (tradeType === 'NORMAL' && activeNormal >= RISK_RULES.maxNormalTrades) return 'MAX NORMAL';
        if (tradeType === 'GEM' && activeGems >= RISK_RULES.maxGemTrades) return 'MAX GEMS';

        let totalInvested = Object.values(activePositions).reduce((sum, p) => sum + (p.qty * p.entryPrice), 0);
        let totalCapital = parseFloat(liveWalletBalance) + totalInvested;
        let allocation = (tradeType === 'GEM') ? RISK_RULES.allocationGemPct : RISK_RULES.allocationNormalPct;
        let maxTrades = (tradeType === 'GEM') ? RISK_RULES.maxGemTrades : RISK_RULES.maxNormalTrades;
        let tradeAmountUSDT = (totalCapital * allocation) / maxTrades;

        if (parseFloat(liveWalletBalance) < tradeAmountUSDT) return 'NO BALANCE';

        const order = await executeTrade(symbol, 'BUY', tradeAmountUSDT.toFixed(2));
        if (order && order.status === 'FILLED') {
            const entryPrice = parseFloat(order.fills[0].price);
            activePositions[symbol] = { entryPrice, qty: parseFloat(order.executedQty), highestPrice: entryPrice, stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct), trailingActive: false, type: tradeType };
            
            savePositions(); 

            const icon = tradeType === 'GEM' ? '💎' : '📊';
            const tradeName = tradeType === 'GEM' ? 'شراء جوهرة' : 'شراء عادي';
            sendTelegramMessage(`${icon} <b>تم ${tradeName}</b>\n<b>العملة:</b> ${symbol}\n<b>المبلغ:</b> $${tradeAmountUSDT.toFixed(2)}`);
            
            updateWalletBalance(); return 'BOUGHT';
        }
    }

    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        let memoryNeedsUpdate = false;

        if (currentPrice > trade.highestPrice) { trade.highestPrice = currentPrice; memoryNeedsUpdate = true; }
        if (!trade.trailingActive && (currentPrice - trade.entryPrice) / trade.entryPrice >= RISK_RULES.trailingActivationPct) { trade.trailingActive = true; memoryNeedsUpdate = true; }
        if (trade.trailingActive) {
            const newSL = trade.highestPrice * (1 - RISK_RULES.trailingDistancePct);
            if (newSL > trade.stopLoss) { trade.stopLoss = newSL; memoryNeedsUpdate = true; }
        }

        if (memoryNeedsUpdate) savePositions(); 

        if (currentPrice <= trade.stopLoss || decision === 'SELL') {
            const order = await executeTrade(symbol, 'SELL', null, trade.qty);
            if (order && order.status === 'FILLED') {
                const profit = (((currentPrice - trade.entryPrice) / trade.entryPrice) * 100).toFixed(2);
                const icon = trade.type === 'GEM' ? '💎' : '📊';
                const resultStatus = profit > 0 ? '✅ <b>ربح</b>' : '❌ <b>خسارة</b>';
                
                sendTelegramMessage(`🔴 <b>تم البيع</b> ${icon}\n<b>العملة:</b> ${symbol}\n<b>الحالة:</b> ${resultStatus}\n<b>النتيجة:</b> ${profit}%`);
                
                delete activePositions[symbol]; 
                savePositions(); 
                
                updateWalletBalance(); return 'CLOSED';
            }
        }
        return `HOLDING (SL: $${trade.stopLoss.toFixed(4)})`;
    }
    return CONFIG.isBotActive ? decision : 'PAUSED';
}

// ==========================================
// 6. Market Analysis & Scanner
// ==========================================
async function runFullScan() {
    try {
        const tickers = await axios.get(`${getBaseUrl()}/api/v3/ticker/24hr`);
        const pairs = tickers.data.filter(t => t.symbol.endsWith('USDT')).sort((a,b) => b.quoteVolume - a.quoteVolume).slice(0, 20);
        let scan = [];

        for (let pair of pairs) {
            const klines = (await axios.get(`${getBaseUrl()}/api/v3/klines?symbol=${pair.symbol}&interval=1m&limit=15`)).data;
            const currentVol = parseFloat(klines[klines.length-1][5]);
            const avgVol = klines.slice(0,10).reduce((s,c) => s + parseFloat(c[5]), 0)/10;

            let decision = 'WAIT';
            let type = 'NORMAL';

            if (currentVol > (avgVol * 5)) { decision = 'BUY'; type = 'GEM'; }

            const status = await managePosition(pair.symbol, parseFloat(pair.lastPrice), decision, type);
            scan.push({ symbol: pair.symbol, decision: status, type: type });
        }
        latestResults = scan;
    } catch (e) {}
}

loadPositions();
setInterval(runFullScan, 30000);
setInterval(updateWalletBalance, 60000);

// ==========================================
// 7. Dashboard API & Web UI 
// ==========================================
app.post('/api/config', (req, res) => {
    const { isBotActive } = req.body;
    CONFIG.isBotActive = isBotActive;
    res.json({ success: true });
});

app.get('/api/data', (req, res) => {
    res.json({ live: latestResults, balance: liveWalletBalance, isActive: CONFIG.isBotActive });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><title>LOMY Dashboard</title><style>body{background:#0b0e11;color:white;font-family:Arial;text-align:center;padding:20px;} .panel{background:#1e2329;padding:20px;border-radius:8px;display:inline-block;margin:10px;} table{width:100%;max-width:600px;margin:20px auto;border-collapse:collapse;} td,th{padding:10px;border:1px solid #2b3139;} </style></head><body>
    <h1>🤖 LOMY Ultra-Fast Engine</h1>
    <div class="panel">Balance: $<span id="bal">${liveWalletBalance}</span></div>
    <button onclick="fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({isBotActive:true})})">START</button>
    <button onclick="fetch('/api/config',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({isBotActive:false})})">STOP</button>
    <table><thead><tr><th>Symbol</th><th>Status</th></tr></thead><tbody id="tbl"></tbody></table>
    <script>
    async function load(){
        try {
            const res = await (await fetch('/api/data')).json();
            document.getElementById('bal').innerText = res.balance;
            let html = '';
            res.live.forEach(item => {
                let symbolText = item.symbol;
                if(item.type === 'GEM') symbolText = '💎 ' + symbolText;
                html += \`<tr><td>\${symbolText}</td><td>\${item.decision}</td></tr>\`;
            });
            document.getElementById('tbl').innerHTML = html;
        } catch(e) {}
    }
    setInterval(load, 5000); load();
    </script>
    </body></html>`);
});

app.listen(PORT, () => console.log('Bot is running...'));
