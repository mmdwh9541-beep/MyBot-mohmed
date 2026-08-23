require('dotenv').config();
const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs').promises;
const path = require('path');

const app = express();
app.use(express.json());
const PORT = process.env.PORT || 5000;

// ==========================================
// 1. Configuration & Global Variables
// ==========================================
const CONFIG = {
    API_KEY: process.env.BINANCE_API_KEY,
    API_SECRET: process.env.BINANCE_API_SECRET,
    isTestnet: process.env.USE_TESTNET === 'false' ? false : true,
};
const TESTNET_URL = CONFIG.isTestnet ? 'https://testnet.binance.vision' : 'https://api.binance.com';

const TELEGRAM = {
    TOKEN: process.env.TELEGRAM_TOKEN,
    CHAT_ID: process.env.TELEGRAM_CHAT_ID
};

const RISK_RULES = {
    tradeAmountUSDT: 100,        
    fallbackStopLossPct: 0.025,   // 2.5% fallback SL
    trailingActivationPct: 0.025, // Activate trailing at 2.5% profit
    trailingDistancePct: 0.01,    // Trailing distance 1% 
    maxTrades: 10                 // الحد الأقصى للصفقات المفتوحة في نفس الوقت
};

let activePositions = {}; 
let cooldowns = {}; 
let liveWalletBalance = "0.00"; 
let latestMarketPrices = {}; 
let symbolRules = {}; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitPct: 0 };

const POSITIONS_FILE = path.join(__dirname, 'positions.json'); 
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ==========================================
// 2. Utility & Technical Indicators (LOMY AI Logic)
// ==========================================
async function sendTelegramMessage(text) {
    if (!TELEGRAM.TOKEN || !TELEGRAM.CHAT_ID) return;
    try { 
        await axios.post(`https://api.telegram.org/bot${TELEGRAM.TOKEN}/sendMessage`, { 
            chat_id: TELEGRAM.CHAT_ID, text: text, parse_mode: 'HTML' 
        }); 
    } catch (e) { console.error('❌ Telegram Error:', e.message); }
}

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

// ==========================================
// 3. Binance API & Precision Limits (The Muscles)
// ==========================================
async function binancePrivateRequest(endpoint, method = 'GET', params = {}) {
    if (!CONFIG.API_KEY || !CONFIG.API_SECRET) return null;
    params.timestamp = Date.now();
    params.recvWindow = 60000;
    const queryString = Object.keys(params).map(key => `${key}=${encodeURIComponent(params[key])}`).join('&');
    const signature = crypto.createHmac('sha256', CONFIG.API_SECRET).update(queryString).digest('hex');
    const url = `${TESTNET_URL}${endpoint}?${queryString}&signature=${signature}`;
    
    try {
        const response = await axios({ method, url, headers: { 'X-MBX-APIKEY': CONFIG.API_KEY } });
        return response.data;
    } catch (e) { return null; }
}

async function loadExchangeRules() {
    try {
        const response = await axios.get(`${TESTNET_URL}/api/v3/exchangeInfo`);
        response.data.symbols.forEach(s => {
            const lotSize = s.filters.find(f => f.filterType === 'LOT_SIZE');
            if (lotSize) symbolRules[s.symbol] = { stepSize: parseFloat(lotSize.stepSize) };
        });
        console.log('✅ Exchange precision rules loaded.');
    } catch (e) { console.error('❌ Error loading exchange rules.'); }
}

function formatQuantity(symbol, qty) {
    if (!symbolRules[symbol]) return qty.toString();
    const stepSize = symbolRules[symbol].stepSize;
    const precision = stepSize.toString().includes('.') ? stepSize.toString().split('.')[1].length : 0;
    const factor = Math.pow(10, precision);
    return (Math.floor(qty * factor) / factor).toFixed(precision);
}

// ==========================================
// 4. File I/O (The Memory)
// ==========================================
async function loadPositions() {
    try {
        const data = await fs.readFile(POSITIONS_FILE, 'utf8');
        activePositions = JSON.parse(data);
        const count = Object.keys(activePositions).length;
        if (count > 0) {
            console.log(`✅ Loaded ${count} active positions from memory.`);
            sendTelegramMessage(`🔄 <b>Bot Restarted!</b>\nRestored ${count} active trades from memory seamlessly.`);
        }
    } catch (e) { activePositions = {}; }
}

async function savePositions() {
    try { await fs.writeFile(POSITIONS_FILE, JSON.stringify(activePositions, null, 2)); } 
    catch (e) { console.error('❌ Error saving positions.'); }
}

// ==========================================
// 5. Order Management
// ==========================================
async function updateWalletBalance() {
    const data = await binancePrivateRequest('/api/v3/account', 'GET');
    if (data && data.balances) {
        const usdt = data.balances.find(b => b.asset === 'USDT');
        if (usdt) liveWalletBalance = parseFloat(usdt.free).toFixed(2);
    }
}

async function executeTrade(symbol, side, quoteQty = null, coinQty = null) {
    let params = { symbol, side, type: 'MARKET' };
    if (side === 'BUY') params.quoteOrderQty = parseFloat(quoteQty).toFixed(2);
    else params.quantity = formatQuantity(symbol, coinQty);
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

async function managePosition(symbol, currentPrice, decision, dynamicSL = null) {
    // EXECUTE BUY
    if (decision === 'BUY' && !activePositions[symbol]) {
        if (Object.keys(activePositions).length >= RISK_RULES.maxTrades) return 'MAX_TRADES_REACHED';
        if (parseFloat(liveWalletBalance) < RISK_RULES.tradeAmountUSDT) return 'INSUFFICIENT_FUNDS';

        console.log(`\n⏳ [ATTEMPTING BUY] ${symbol}...`);
        const order = await executeTrade(symbol, 'BUY', RISK_RULES.tradeAmountUSDT);
        
        if (order && order.status === 'FILLED') {
            const entryPrice = parseFloat(order.fills[0] ? order.fills[0].price : currentPrice);
            const qtyBought = parseFloat(order.executedQty);
            const finalSL = dynamicSL ? dynamicSL : entryPrice * (1 - RISK_RULES.fallbackStopLossPct);

            activePositions[symbol] = { 
                entryPrice, qty: qtyBought, highestPrice: entryPrice, 
                stopLoss: finalSL, trailingActive: false, time: new Date().toLocaleString() 
            };
            await savePositions(); 
            
            sendTelegramMessage(`🟢 <b>LOMY SNIPER: BUY EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Price:</b> $${entryPrice.toFixed(4)}\n<b>Dynamic SL:</b> $${finalSL.toFixed(4)}`);
            await updateWalletBalance(); 
            return 'BOUGHT';
        }
    }

    // MANAGE HOLD/SELL
    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        let memoryNeedsUpdate = false;

        if (currentPrice > trade.highestPrice) { 
            trade.highestPrice = currentPrice; 
            memoryNeedsUpdate = true; 
        }
        
        const profitPct = (currentPrice - trade.entryPrice) / trade.entryPrice;

        // Trailing Activation
        if (!trade.trailingActive && profitPct >= RISK_RULES.trailingActivationPct) { 
            trade.trailingActive = true; 
            memoryNeedsUpdate = true; 
            sendTelegramMessage(`🚀 <b>Target Hit! Trailing Stop Activated</b>\n<b>Symbol:</b> ${symbol}\n<b>Profit:</b> ${(profitPct*100).toFixed(2)}%`);
        }
        
        // Update Trailing SL
        if (trade.trailingActive) {
            const newSL = trade.highestPrice * (1 - RISK_RULES.trailingDistancePct);
            if (newSL > trade.stopLoss) { trade.stopLoss = newSL; memoryNeedsUpdate = true; }
        }
        
        if (memoryNeedsUpdate) await savePositions(); 

        // SELL Logic
        if (currentPrice <= trade.stopLoss || decision === 'SELL') {
            console.log(`\n⏳ [ATTEMPTING SELL] ${symbol}...`);
            const order = await executeTrade(symbol, 'SELL', null, trade.qty);
            
            if (order && (order.status === 'FILLED' || order.status === 'NEW')) {
                const finalProfitPct = parseFloat(((currentPrice - trade.entryPrice) / trade.entryPrice * 100).toFixed(2));
                const exitReason = currentPrice <= trade.stopLoss ? 'Stop Loss / Trailing Hit' : 'SELL Signal';
                
                testStats.totalTrades++;
                if (finalProfitPct > 0) testStats.winningTrades++;
                testStats.totalProfitPct += finalProfitPct;
                
                cooldowns[symbol] = Date.now() + (15 * 60 * 1000); // 15 mins cooldown so it doesn't re-enter immediately
                
                const emoji = finalProfitPct > 0 ? '✅ PROFIT' : '❌ LOSS';
                sendTelegramMessage(`🔴 <b>SELL EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Reason:</b> ${exitReason}\n<b>Result:</b> ${emoji} (${finalProfitPct}%)`);
                
                delete activePositions[symbol]; 
                await savePositions(); 
                await updateWalletBalance();
                return `CLOSED (${finalProfitPct}%)`;
            }
        }
        return `HOLDING (SL: $${trade.stopLoss.toFixed(4)})`;
    }
    return decision;
}

// ==========================================
// 6. Market Scanner (LOMY AI Whale Hunter)
// ==========================================
let isScanning = false;

async function runFullScan() {
    if (isScanning) return; 
    isScanning = true;

    try {
        const response = await axios.get(`${TESTNET_URL}/api/v3/ticker/24hr`);
        const usdtPairs = response.data
            .filter(t => t.symbol.endsWith('USDT') && !t.symbol.includes('USDC'))
            .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
            .slice(0, 20); // Scan top 20 active coins

        for (let pair of usdtPairs) {
            latestMarketPrices[pair.symbol] = parseFloat(pair.lastPrice);
            
            // Skip if on cooldown (recently sold)
            if (cooldowns[pair.symbol] && Date.now() < cooldowns[pair.symbol]) continue; 

            try {
                const klines = await axios.get(`${TESTNET_URL}/api/v3/klines?symbol=${pair.symbol}&interval=15m&limit=30`);
                if (!klines.data || klines.data.length < 30) continue;

                const candles = klines.data.map(c => ({
                    open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]), 
                    close: parseFloat(c[4]), volume: parseFloat(c[5])
                }));

                const currentPrice = candles[candles.length - 1].close; 
                const volSMA = calculateSMA(candles, 10, 'volume');
                const cmo = calculateCMO(candles, 9);
                
                const candle = candles[candles.length - 2];
                const signalLow = candle.low; 

                const highVolume = candle.volume > (volSMA[volSMA.length - 2] * 1.3);
                const bodyRatio = (candle.high - candle.low) > 0 ? (Math.abs(candle.close - candle.open) / (candle.high - candle.low)) : 0;
                
                const bullish = candle.close > candle.open && bodyRatio > 0.5 && highVolume;
                const bearish = candle.close < candle.open && bodyRatio > 0.5 && highVolume;

                let decision = 'WAIT';
                let dynamicSL = null;

                if (bullish && cmo[cmo.length - 2] > 30) {
                    decision = 'BUY';
                    dynamicSL = signalLow * 0.998; // Dynamic SL under signal candle
                }
                
                if (bearish && cmo[cmo.length - 2] < -30) { decision = 'SELL'; }

                await managePosition(pair.symbol, currentPrice, decision, dynamicSL);
                await sleep(150); // Prevent API weight limits

            } catch (e) {} // Ignore single coin error to continue loop
        }

        // Always check active positions even if they dropped out of Top 20
        for (let symbol in activePositions) {
            if (!latestMarketPrices[symbol]) {
                try {
                    const tick = await axios.get(`${TESTNET_URL}/api/v3/ticker/price?symbol=${symbol}`);
                    latestMarketPrices[symbol] = parseFloat(tick.data.price);
                } catch(e) { continue; }
            }
            await managePosition(symbol, latestMarketPrices[symbol], 'WAIT', activePositions[symbol].stopLoss);
        }

    } catch (e) {
    } finally {
        isScanning = false;
        setTimeout(runFullScan, 15000); 
    }
}

// ==========================================
// 7. Initialization & Web Dashboard
// ==========================================
async function startBot() {
    await loadExchangeRules();
    await loadPositions();
    await updateWalletBalance();
    
    runFullScan(); 
    setInterval(updateWalletBalance, 60000); 
}

app.get('/api/data', (req, res) => {
    let positionsData = Object.keys(activePositions).map(sym => {
        const entry = activePositions[sym].entryPrice;
        const current = latestMarketPrices[sym] || entry;
        const pnl = (((current - entry) / entry) * 100).toFixed(2);
        return { symbol: sym, pnl: parseFloat(pnl), sl: activePositions[sym].stopLoss.toFixed(4) };
    });
    res.json({ positions: positionsData, balance: liveWalletBalance, stats: testStats });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><title>LOMY Super Bot</title><style>
        body{background:#0b0e11;color:#eaecef;font-family:Arial,sans-serif;text-align:center;padding:20px;margin:0;} 
        .header h1{color:#f3ba2f;margin-bottom:5px;}
        .panel{background:#1e2329;padding:15px 30px;border-radius:8px;display:inline-block;margin-bottom:20px;font-size:22px;font-weight:bold;border:1px solid #2b3139;} 
        .bal{color:#0ecb81;}
        .stats-container{display:flex;justify-content:center;gap:20px;margin-bottom:20px;}
        .stat-box{background-color:#1e2329;padding:15px 30px;border-radius:8px;font-weight:bold;border:1px solid #2b3139;}
        table{width:100%;max-width:800px;margin:0 auto;border-collapse:collapse;background:#1e2329;border-radius:8px;overflow:hidden;} 
        th,td{padding:15px;border-bottom:1px solid #2b3139;} 
        th{background:#2b3139;color:#848e9c;}
        .profit{color:#0ecb81;font-weight:bold;} 
        .loss{color:#f6465d;font-weight:bold;}
        .empty{color:#848e9c;padding:30px;}
        .badge{background:rgba(243,186,47,0.2);color:#f3ba2f;padding:3px 8px;border-radius:4px;font-size:12px;}
    </style></head><body>
    <div class="header"><h1>🤖 LOMY Super Bot (Whale Hunter Engine)</h1><p>Running on 15m Timeframe</p></div>
    <div class="panel">Wallet: <span class="bal">$<span id="bal">${liveWalletBalance}</span></span></div>
    
    <div class="stats-container">
        <div class="stat-box">Total Trades: <span id="tot-trades">0</span></div>
        <div class="stat-box">Net PnL: <span id="net-profit">0.00%</span></div>
    </div>

    <table><thead><tr><th>Asset</th><th>Status</th><th>Stop Loss</th><th>Live P&L</th></tr></thead><tbody id="tbl"></tbody></table>
    <script>
    async function load(){
        try {
            const res = await (await fetch('/api/data')).json();
            document.getElementById('bal').innerText = res.balance;
            document.getElementById('tot-trades').innerText = res.stats.totalTrades;
            
            let profitEl = document.getElementById('net-profit');
            profitEl.innerText = res.stats.totalProfitPct.toFixed(2) + '%';
            profitEl.className = res.stats.totalProfitPct >= 0 ? 'profit' : 'loss';

            let html = '';
            if(res.positions.length === 0){
                html = '<tr><td colspan="4" class="empty">Scanning markets using LOMY Logic... 📡</td></tr>';
            } else {
                res.positions.forEach(p => {
                    let badge = '<span class="badge">🔥 ACTIVE</span>';
                    let pnlClass = p.pnl >= 0 ? 'profit' : 'loss';
                    let pnlSign = p.pnl > 0 ? '+' : '';
                    html += \`<tr><td><strong>\${p.symbol}</strong></td><td>\${badge}</td><td>$\${p.sl}</td><td class="\${pnlClass}">\${pnlSign}\${p.pnl}%</td></tr>\`;
                });
            }
            document.getElementById('tbl').innerHTML = html;
        } catch(e) {}
    }
    setInterval(load, 3000); load();
    </script>
    </body></html>`);
});

app.listen(PORT, () => {
    console.log(`🚀 LOMY Server is running on port ${PORT}...`);
    startBot();
});
