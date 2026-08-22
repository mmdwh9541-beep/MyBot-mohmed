const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 5000;

// ==========================================
// 1. Telegram & Config
// ==========================================
const TELEGRAM_TOKEN = '8956340113:AAGyr_IZdKMniNLYPeTnl-NoUzZzbI5hAiI';
const CHAT_ID = '8708481752';

async function sendTelegramMessage(text) {
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    try { await axios.post(url, { chat_id: CHAT_ID, text: text, parse_mode: 'HTML' }); } catch (e) {}
}

let CONFIG = {
    API_KEY: 'Vwsc1ALWhJlxKZbgq9cCi9UiFOqDya9kleWof1FzZHxSLJ6uytpnybyV5zwY4Yj2',
    API_SECRET: 'eJhpQdGiTl1B8rKLFrnt84C4FSfwFCWeODcE9M5nHQZMjLJurFSKqILoh4r0vgCM',
    isTestnet: true,
    isBotActive: true
};

function getBaseUrl() { return CONFIG.isTestnet ? 'https://testnet.binance.vision' : 'https://api.binance.com'; }

// ==========================================
// 2. Memory & Risk
// ==========================================
const RISK_RULES = { stopLossPct: 0.015, trailingActivationPct: 0.03, trailingDistancePct: 0.01, allocationGemPct: 0.5 };
let activePositions = {}; 
let latestMarketData = {};
let liveWalletBalance = "0.00"; 
const POSITIONS_FILE = './positions.json';

function loadPositions() { if (fs.existsSync(POSITIONS_FILE)) activePositions = JSON.parse(fs.readFileSync(POSITIONS_FILE, 'utf8')); }
function savePositions() { fs.writeFileSync(POSITIONS_FILE, JSON.stringify(activePositions)); }

// ==========================================
// 3. Trade Logic
// ==========================================
async function managePosition(symbol, currentPrice, decision, tradeType = 'NORMAL') {
    if (decision === 'BUY' && !activePositions[symbol] && CONFIG.isBotActive) {
        const order = await executeTrade(symbol, 'BUY', '20'); // تجربة بـ 20 دولار للتدقيق
        if (order && order.status === 'FILLED') {
            activePositions[symbol] = { entryPrice: parseFloat(order.fills[0].price), qty: parseFloat(order.executedQty), highestPrice: parseFloat(order.fills[0].price), type: tradeType };
            savePositions();
            sendTelegramMessage(`💎 <b>شراء:</b> ${symbol}`);
            return 'BOUGHT';
        }
    }
    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        let pnl = ((currentPrice - trade.entryPrice) / trade.entryPrice) * 100;
        if (pnl <= -1.5 || decision === 'SELL') {
            const order = await executeTrade(symbol, 'SELL', null, trade.qty);
            if (order && order.status === 'FILLED') {
                sendTelegramMessage(`🔴 <b>بيع ${symbol}</b>\nالنتيجة: ${pnl.toFixed(2)}%`);
                delete activePositions[symbol]; savePositions();
                return 'CLOSED';
            }
        }
        return `HOLDING (${pnl.toFixed(2)}%)`;
    }
    return decision;
}

async function executeTrade(symbol, side, quoteQty, coinQty) {
    let params = { symbol, side, type: 'MARKET', timestamp: Date.now() };
    if (side === 'BUY') params.quoteOrderQty = quoteQty; else params.quantity = coinQty;
    const queryString = new URLSearchParams(params).toString();
    const sig = crypto.createHmac('sha256', CONFIG.API_SECRET).update(queryString).digest('hex');
    try {
        const res = await axios.post(`${getBaseUrl()}/api/v3/order?${queryString}&signature=${sig}`, {}, { headers: { 'X-MBX-APIKEY': CONFIG.API_KEY } });
        return res.data;
    } catch (e) { return null; }
}

// ==========================================
// 4. Scanner
// ==========================================
async function runFullScan() {
    const tickers = (await axios.get(`${getBaseUrl()}/api/v3/ticker/24hr`)).data;
    tickers.forEach(t => latestMarketData[t.symbol] = parseFloat(t.lastPrice));
    
    for (let symbol in activePositions) {
        await managePosition(symbol, latestMarketData[symbol] || 0, 'HOLD');
    }
}
setInterval(runFullScan, 5000);
loadPositions();

// ==========================================
// 5. Dashboard (المتطورة)
// ==========================================
app.get('/api/data', (req, res) => {
    let data = Object.keys(activePositions).map(s => ({
        symbol: s,
        pnl: (((latestMarketData[s] || 0) - activePositions[s].entryPrice) / activePositions[s].entryPrice) * 100
    }));
    res.json({ positions: data, balance: liveWalletBalance });
});

app.get('/', (req, res) => {
    res.send(`<html><head><style>
        body{background:#000;color:#fff;font-family:sans-serif;}
        .profit{color:#2ecc71;} .loss{color:#e74c3c;}
        table{width:100%;}
    </style></head><body>
    <h1>LOMY Ultra Engine</h1>
    <table><thead><tr><th>Symbol</th><th>P&L</th></tr></thead><tbody id="tbl"></tbody></table>
    <script>
    setInterval(async()=>{
        const res = await (await fetch('/api/data')).json();
        let h = '';
        res.positions.forEach(p => {
            h += \`<tr><td>\${p.symbol}</td><td class="\${p.pnl>=0?'profit':'loss'}">\${p.pnl.toFixed(2)}%</td></tr>\`;
        });
        document.getElementById('tbl').innerHTML = h;
    }, 2000);
    </script></body></html>`);
});

app.listen(PORT);
