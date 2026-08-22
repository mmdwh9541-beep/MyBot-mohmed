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
    isTestnet: process.env.USE_TESTNET === 'true',
    isBotActive: process.env.BOT_ACTIVE === 'true'
};

const TELEGRAM = {
    TOKEN: process.env.TELEGRAM_TOKEN,
    CHAT_ID: process.env.TELEGRAM_CHAT_ID
};

const RISK_RULES = {
    stopLossPct: 0.015,          // 1.5% Stop Loss
    trailingActivationPct: 0.03, // Activate trailing at 3% profit
    trailingDistancePct: 0.015,  // Trailing distance 1.5%
    allocationNormalPct: 0.5,    // 50% Capital for Normal Scalping
    allocationGemPct: 0.5,       // 50% Capital for Breakout Gems
    maxNormalTrades: 10,
    maxGemTrades: 5
};

let activePositions = {}; 
let cooldowns = {}; 
let liveWalletBalance = "0.00"; 
let latestMarketPrices = {}; 
let symbolRules = {}; 
const POSITIONS_FILE = path.join(__dirname, 'positions.json'); 

function getBaseUrl() { 
    return CONFIG.isTestnet ? 'https://testnet.binance.vision' : 'https://api.binance.com'; 
}

// ==========================================
// 2. Utility & Technical Indicators
// ==========================================
async function sendTelegramMessage(text) {
    if (!TELEGRAM.TOKEN || !TELEGRAM.CHAT_ID) return;
    try { 
        await axios.post(`https://api.telegram.org/bot${TELEGRAM.TOKEN}/sendMessage`, { 
            chat_id: TELEGRAM.CHAT_ID, text: text, parse_mode: 'HTML' 
        }); 
    } catch (e) { console.error('Telegram Error:', e.message); }
}

function calculateSMA(data, period) {
    if (data.length < period) return null;
    let sum = 0;
    for (let i = data.length - period; i < data.length; i++) sum += data[i].close;
    return sum / period;
}

function calculateRSI(data, period = 14) {
    if (data.length <= period) return 50;
    let gains = 0, losses = 0;
    for (let i = data.length - period; i < data.length; i++) {
        const diff = data[i].close - data[i-1].close;
        if (diff > 0) gains += diff;
        else losses -= diff;
    }
    const avgGain = gains / period;
    const avgLoss = losses / period;
    if (avgLoss === 0) return 100;
    const rs = avgGain / avgLoss;
    return 100 - (100 / (1 + rs));
}

// ==========================================
// 3. Binance API & Precision Limits
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

async function loadExchangeRules() {
    try {
        const response = await axios.get(`${getBaseUrl()}/api/v3/exchangeInfo`);
        response.data.symbols.forEach(s => {
            const lotSize = s.filters.find(f => f.filterType === 'LOT_SIZE');
            if (lotSize) symbolRules[s.symbol] = { stepSize: parseFloat(lotSize.stepSize) };
        });
    } catch (e) { console.error('Error loading exchange rules.'); }
}

function formatQuantity(symbol, qty) {
    if (!symbolRules[symbol]) return qty.toString();
    const stepSize = symbolRules[symbol].stepSize;
    const precision = stepSize.toString().includes('.') ? stepSize.toString().split('.')[1].length : 0;
    const factor = Math.pow(10, precision);
    return (Math.floor(qty * factor) / factor).toFixed(precision);
}

// ==========================================
// 4. File I/O & Memory
// ==========================================
async function loadPositions() {
    try {
        const data = await fs.readFile(POSITIONS_FILE, 'utf8');
        activePositions = JSON.parse(data);
    } catch (e) { activePositions = {}; }
}

async function savePositions() {
    try { await fs.writeFile(POSITIONS_FILE, JSON.stringify(activePositions, null, 2)); } 
    catch (e) { console.error('Error saving positions.'); }
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
    if (!CONFIG.isBotActive && side === 'BUY') return null;
    let params = { symbol, side, type: 'MARKET' };
    if (side === 'BUY') params.quoteOrderQty = parseFloat(quoteQty).toFixed(2);
    else params.quantity = formatQuantity(symbol, coinQty);
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

async function managePosition(symbol, currentPrice, decision, tradeType = 'NORMAL') {
    const activeNormal = Object.values(activePositions).filter(p => p.type === 'NORMAL').length;
    const activeGems = Object.values(activePositions).filter(p => p.type === 'GEM').length;
    
    // EXECUTE BUY
    if (decision === 'BUY' && !activePositions[symbol] && CONFIG.isBotActive) {
        if (tradeType === 'NORMAL' && activeNormal >= RISK_RULES.maxNormalTrades) return;
        if (tradeType === 'GEM' && activeGems >= RISK_RULES.maxGemTrades) return;

        let totalInvested = Object.values(activePositions).reduce((sum, p) => sum + (p.qty * p.entryPrice), 0);
        let totalCapital = parseFloat(liveWalletBalance) + totalInvested;
        let tradeAmountUSDT = (totalCapital * (tradeType === 'GEM' ? RISK_RULES.allocationGemPct : RISK_RULES.allocationNormalPct)) / (tradeType === 'GEM' ? RISK_RULES.maxGemTrades : RISK_RULES.maxNormalTrades);

        if (parseFloat(liveWalletBalance) < tradeAmountUSDT) return;

        const order = await executeTrade(symbol, 'BUY', tradeAmountUSDT);
        if (order && order.status === 'FILLED') {
            const entryPrice = parseFloat(order.fills[0] ? order.fills[0].price : currentPrice);
            activePositions[symbol] = { 
                entryPrice, 
                qty: parseFloat(order.executedQty), 
                highestPrice: entryPrice, 
                stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct), 
                trailingActive: false, 
                type: tradeType 
            };
            await savePositions(); 
            const icon = tradeType === 'GEM' ? '💎' : '📊';
            sendTelegramMessage(`${icon} <b>BUY EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Type:</b> ${tradeType}\n<b>Price:</b> $${entryPrice}\n<b>Amount:</b> $${tradeAmountUSDT.toFixed(2)}`);
            await updateWalletBalance(); 
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
        
        if (!trade.trailingActive && (currentPrice - trade.entryPrice) / trade.entryPrice >= RISK_RULES.trailingActivationPct) { 
            trade.trailingActive = true; 
            memoryNeedsUpdate = true; 
        }
        
        if (trade.trailingActive) {
            const newSL = trade.highestPrice * (1 - RISK_RULES.trailingDistancePct);
            if (newSL > trade.stopLoss) { trade.stopLoss = newSL; memoryNeedsUpdate = true; }
        }
        
        if (memoryNeedsUpdate) await savePositions(); 

        if (currentPrice <= trade.stopLoss || decision === 'SELL') {
            const order = await executeTrade(symbol, 'SELL', null, trade.qty);
            if (order && (order.status === 'FILLED' || order.status === 'NEW')) {
                const profit = (((currentPrice - trade.entryPrice) / trade.entryPrice) * 100).toFixed(2);
                cooldowns[symbol] = Date.now() + (15 * 60 * 1000); // 15 mins cooldown
                const icon = trade.type === 'GEM' ? '💎' : '📊';
                const statusText = profit > 0 ? '✅ PROFIT' : '❌ LOSS';
                sendTelegramMessage(`🔴 <b>SELL EXECUTED</b> ${icon}\n<b>Symbol:</b> ${symbol}\n<b>Status:</b> ${statusText}\n<b>Result:</b> ${profit}%`);
                delete activePositions[symbol]; 
                await savePositions(); 
                await updateWalletBalance();
            }
        }
    }
}

// ==========================================
// 6. Market Scanner (Dual Engine)
// ==========================================
let isScanning = false;

async function runFullScan() {
    if (isScanning) return; 
    isScanning = true;

    try {
        const tickers = await axios.get(`${getBaseUrl()}/api/v3/ticker/24hr`);
        // Filter out dead volume coins, keep top 50
        const pairs = tickers.data
            .filter(t => t.symbol.endsWith('USDT') && parseFloat(t.quoteVolume) > 2000000)
            .sort((a,b) => b.quoteVolume - a.quoteVolume)
            .slice(0, 50);
        
        pairs.forEach(p => { latestMarketPrices[p.symbol] = parseFloat(p.lastPrice); });

        for (let pair of pairs) {
            if (cooldowns[pair.symbol] && Date.now() < cooldowns[pair.symbol]) continue; 

            // Using 3m timeframe for higher accuracy, less noise
            const klines = await axios.get(`${getBaseUrl()}/api/v3/klines?symbol=${pair.symbol}&interval=3m&limit=30`);
            if (!klines.data || klines.data.length < 30) continue;

            const candles = klines.data.map(c => ({
                open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]),
                close: parseFloat(c[4]), volume: parseFloat(c[5])
            }));

            const current = candles[candles.length - 1];
            const previous = candles[candles.length - 2];
            
            const rsi = calculateRSI(candles, 14);
            const sma9 = calculateSMA(candles, 9);
            const sma21 = calculateSMA(candles, 21);
            const avgVol = candles.slice(-10, -1).reduce((s,c) => s + c.volume, 0) / 9;

            let decision = 'WAIT';
            let type = 'NORMAL';

            // --- STRATEGY 1: BREAKOUT GEMS (50%) ---
            // Catching volume buildup before the massive pump
            const volumeBuildup = current.volume > (avgVol * 2.5) && current.volume < (avgVol * 6);
            const priceBreakout = current.close > previous.high && current.close > sma21;
            
            if (volumeBuildup && priceBreakout && rsi > 55 && rsi < 75) { 
                decision = 'BUY'; 
                type = 'GEM'; 
            }
            // --- STRATEGY 2: NORMAL SCALPING (50%) ---
            // Catching strong coins on temporary dips
            else {
                const uptrend = sma9 > sma21;
                const oversoldDip = rsi < 40 && rsi > 25; // Prevent catching falling knives
                const greenReversal = current.close > current.open && current.close > (current.open + (current.high - current.low) * 0.5);

                if (uptrend && oversoldDip && greenReversal) {
                    decision = 'BUY';
                    type = 'NORMAL';
                }
            }

            if (decision === 'BUY') {
                await managePosition(pair.symbol, parseFloat(pair.lastPrice), decision, type);
            }
        }
        
        // Check active positions for stop loss/take profit updates
        for (let symbol in activePositions) {
            if (!latestMarketPrices[symbol]) {
                try {
                    const tick = await axios.get(`${getBaseUrl()}/api/v3/ticker/price?symbol=${symbol}`);
                    latestMarketPrices[symbol] = parseFloat(tick.data.price);
                } catch(e) { continue; }
            }
            await managePosition(symbol, latestMarketPrices[symbol], 'WAIT', activePositions[symbol].type);
        }

    } catch (e) {
        // Silent catch to prevent crashing loop
    } finally {
        isScanning = false;
        setTimeout(runFullScan, 15000); // 15 seconds delay after scan completes
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
        return { symbol: sym, type: activePositions[sym].type, pnl: parseFloat(pnl), sl: activePositions[sym].stopLoss.toFixed(4) };
    });
    res.json({ positions: positionsData, balance: liveWalletBalance });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><title>LOMY Dual Engine</title><style>
        body{background:#0b0e11;color:#eaecef;font-family:Arial,sans-serif;text-align:center;padding:20px;margin:0;} 
        .header h1{color:#f3ba2f;margin-bottom:5px;}
        .panel{background:#1e2329;padding:15px 30px;border-radius:8px;display:inline-block;margin-bottom:20px;font-size:22px;font-weight:bold;border:1px solid #2b3139;} 
        .bal{color:#0ecb81;}
        table{width:100%;max-width:700px;margin:0 auto;border-collapse:collapse;background:#1e2329;border-radius:8px;overflow:hidden;} 
        th,td{padding:15px;border-bottom:1px solid #2b3139;} 
        th{background:#2b3139;color:#848e9c;}
        .profit{color:#0ecb81;font-weight:bold;} 
        .loss{color:#f6465d;font-weight:bold;}
        .empty{color:#848e9c;padding:30px;}
        .badge-gem{background:rgba(243,186,47,0.2);color:#f3ba2f;padding:3px 8px;border-radius:4px;font-size:12px;}
        .badge-norm{background:rgba(14,203,129,0.2);color:#0ecb81;padding:3px 8px;border-radius:4px;font-size:12px;}
    </style></head><body>
    <div class="header"><h1>🤖 LOMY Dual Engine</h1><p>Algorithmic Sniper Bot</p></div>
    <div class="panel">Wallet: <span class="bal">$<span id="bal">${liveWalletBalance}</span></span></div>
    <table><thead><tr><th>Asset</th><th>Strategy</th><th>Stop Loss</th><th>Live P&L</th></tr></thead><tbody id="tbl"></tbody></table>
    <script>
    async function load(){
        try {
            const res = await (await fetch('/api/data')).json();
            document.getElementById('bal').innerText = res.balance;
            let html = '';
            if(res.positions.length === 0){
                html = '<tr><td colspan="4" class="empty">Scanning markets for precise entry... 📡</td></tr>';
            } else {
                res.positions.forEach(p => {
                    let badge = p.type === 'GEM' ? '<span class="badge-gem">💎 GEM</span>' : '<span class="badge-norm">📊 NORMAL</span>';
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
