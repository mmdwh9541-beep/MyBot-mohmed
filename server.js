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
    try { await axios.post(url, { chat_id: CHAT_ID, text: text, parse_mode: 'HTML' }); } catch (e) {}
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

function getBaseUrl() { return CONFIG.isTestnet ? 'https://testnet.binance.vision' : 'https://api.binance.com'; }

// ==========================================
// 3. Memory & Risk Rules
// ==========================================
const RISK_RULES = {
    stopLossPct: 0.015,          
    trailingActivationPct: 0.03, 
    trailingDistancePct: 0.015,   
    allocationNormalPct: 0.5,    
    allocationGemPct: 0.5,       
    maxNormalTrades: 10,
    maxGemTrades: 5
};

let activePositions = {}; 
let cooldowns = {}; 
let liveWalletBalance = "0.00"; 
let latestMarketPrices = {}; 
const POSITIONS_FILE = './positions.json'; 

function loadPositions() {
    if (fs.existsSync(POSITIONS_FILE)) {
        try { activePositions = JSON.parse(fs.readFileSync(POSITIONS_FILE, 'utf8')); } catch (e) {}
    }
}
function savePositions() {
    try { fs.writeFileSync(POSITIONS_FILE, JSON.stringify(activePositions)); } catch (e) {}
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
        let tradeAmountUSDT = (totalCapital * (tradeType === 'GEM' ? RISK_RULES.allocationGemPct : RISK_RULES.allocationNormalPct)) / (tradeType === 'GEM' ? RISK_RULES.maxGemTrades : RISK_RULES.maxNormalTrades);

        if (parseFloat(liveWalletBalance) < tradeAmountUSDT) return 'NO BALANCE';

        const order = await executeTrade(symbol, 'BUY', tradeAmountUSDT.toFixed(2));
        if (order && order.status === 'FILLED') {
            const entryPrice = parseFloat(order.fills[0].price);
            activePositions[symbol] = { entryPrice, qty: parseFloat(order.executedQty), highestPrice: entryPrice, stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct), trailingActive: false, type: tradeType };
            savePositions(); 
            
            const icon = tradeType === 'GEM' ? '💎' : '📊';
            sendTelegramMessage(`${icon} <b>BUY EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Type:</b> ${tradeType}\n<b>Amount:</b> $${tradeAmountUSDT.toFixed(2)}`);
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
                const isWin = profit > 0;
                
                cooldowns[symbol] = Date.now() + (15 * 60 * 1000); 

                const icon = trade.type === 'GEM' ? '💎' : '📊';
                const statusText = isWin ? '✅ PROFIT' : '❌ LOSS';
                sendTelegramMessage(`🔴 <b>SELL EXECUTED</b> ${icon}\n<b>Symbol:</b> ${symbol}\n<b>Status:</b> ${statusText}\n<b>Result:</b> ${profit}%`);
                
                delete activePositions[symbol]; 
                savePositions(); 
                updateWalletBalance(); return 'CLOSED';
            }
        }
    }
}

// ==========================================
// 6. Market Scanner
// ==========================================
async function runFullScan() {
    try {
        const tickers = await axios.get(`${getBaseUrl()}/api/v3/ticker/24hr`);
        const pairs = tickers.data.filter(t => t.symbol.endsWith('USDT')).sort((a,b) => b.quoteVolume - a.quoteVolume).slice(0, 40);
        
        pairs.forEach(p => { latestMarketPrices[p.symbol] = parseFloat(p.lastPrice); });

        for (let pair of pairs) {
            if (cooldowns[pair.symbol] && Date.now() < cooldowns[pair.symbol]) continue; 

            const klines = (await axios.get(`${getBaseUrl()}/api/v3/klines?symbol=${pair.symbol}&interval=1m&limit=15`)).data;
            const currentCandle = klines[klines.length-1];
            
            const currentOpen = parseFloat(currentCandle[1]);
            const currentClose = parseFloat(currentCandle[4]);
            const currentVol = parseFloat(currentCandle[5]);
            
            const isGreenCandle = currentClose > currentOpen;
            const candleBodyPct = ((currentClose - currentOpen) / currentOpen) * 100;
            const avgVol = klines.slice(0,10).reduce((s,c) => s + parseFloat(c[5]), 0)/10;

            let decision = 'WAIT';
            let type = 'NORMAL';

            if (currentVol > (avgVol * 5) && isGreenCandle && candleBodyPct > 0.3) { 
                decision = 'BUY'; type = 'GEM'; 
            }

            await managePosition(pair.symbol, parseFloat(pair.lastPrice), decision, type);
        }
        
        for (let symbol in activePositions) {
            if (!latestMarketPrices[symbol]) {
                const tick = await axios.get(`${getBaseUrl()}/api/v3/ticker/price?symbol=${symbol}`);
                latestMarketPrices[symbol] = parseFloat(tick.data.price);
            }
            await managePosition(symbol, latestMarketPrices[symbol], 'WAIT');
        }

    } catch (e) {}
}

loadPositions();
setInterval(runFullScan, 15000);
setInterval(updateWalletBalance, 60000);

// ==========================================
// 7. Dashboard API & Web UI
// ==========================================
app.get('/api/data', (req, res) => {
    let positionsData = Object.keys(activePositions).map(sym => {
        const entry = activePositions[sym].entryPrice;
        const current = latestMarketPrices[sym] || entry;
        const pnl = (((current - entry) / entry) * 100).toFixed(2);
        return { symbol: sym, type: activePositions[sym].type, pnl: parseFloat(pnl) };
    });
    res.json({ positions: positionsData, balance: liveWalletBalance });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><title>LOMY Dashboard</title><style>
        body{background:#0b0e11;color:white;font-family:Arial,sans-serif;text-align:center;padding:20px;} 
        .panel{background:#1e2329;padding:20px;border-radius:8px;display:inline-block;margin-bottom:20px;font-size:20px;font-weight:bold;} 
        table{width:100%;max-width:600px;margin:0 auto;border-collapse:collapse;background:#1e2329;border-radius:8px;overflow:hidden;} 
        th,td{padding:15px;border-bottom:1px solid #2b3139;} 
        th{background:#2b3139;}
        .profit{color:#2ecc71;font-weight:bold;} 
        .loss{color:#e74c3c;font-weight:bold;}
        .empty{color:#888;padding:20px;}
    </style></head><body>
    <h1>🤖 LOMY Ultra-Fast Engine</h1>
    <div class="panel">Balance: $<span id="bal">${liveWalletBalance}</span></div>
    <h3>Active Positions (Live P&L)</h3>
    <table><thead><tr><th>Symbol</th><th>Type</th><th>Live P&L</th></tr></thead><tbody id="tbl"></tbody></table>
    <script>
    async function load(){
        try {
            const res = await (await fetch('/api/data')).json();
            document.getElementById('bal').innerText = res.balance;
            let html = '';
            if(res.positions.length === 0){
                html = '<tr><td colspan="3" class="empty">Scanning for new gems... 📡</td></tr>';
            } else {
                res.positions.forEach(p => {
                    let icon = p.type === 'GEM' ? '💎' : '📊';
                    let pnlClass = p.pnl >= 0 ? 'profit' : 'loss';
                    let pnlSign = p.pnl > 0 ? '+' : '';
                    html += \`<tr><td>\${icon} \${p.symbol}</td><td>\${p.type}</td><td class="\${pnlClass}">\${pnlSign}\${p.pnl}%</td></tr>\`;
                });
            }
            document.getElementById('tbl').innerHTML = html;
        } catch(e) {}
    }
    setInterval(load, 3000); load();
    </script>
    </body></html>`);
});

app.listen(PORT, () => console.log('Bot is running...'));
