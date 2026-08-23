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

// --- STRATEGY SETTINGS ---
const RISK_RULES = {
    tradeAmountUSDT: 100,        
    fallbackStopLossPct: 0.02,    // 2% Stop Loss
    trailingActivationPct: 0.04,  // 4% Take Profit (Activation)
    trailingDistancePct: 0.01,    // 1% Trailing Distance
    pullbackEntryPct: 0.005,      // 0.5% Pullback entry (Limit Order)
    maxTrades: 10,
    orderExpiryMinutes: 30        // Cancel Limit Order if not filled in 30 mins
};

let activePositions = {}; 
let cooldowns = {}; 
let liveWalletBalance = "0.00"; 
let latestMarketPrices = {}; 
let symbolRules = {}; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitUSDT: 0 };

const POSITIONS_FILE = path.join(__dirname, 'positions.json'); 
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// ==========================================
// 2. Utility & Technical Indicators (LOMY AI)
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
// 3. Binance API & Precision Limits
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
    } catch (e) { 
        console.error(`❌ API Error (${endpoint}):`, e.response ? e.response.data.msg : e.message);
        return null; 
    }
}

async function loadExchangeRules() {
    try {
        const response = await axios.get(`${TESTNET_URL}/api/v3/exchangeInfo`);
        response.data.symbols.forEach(s => {
            const lotSize = s.filters.find(f => f.filterType === 'LOT_SIZE');
            const priceFilter = s.filters.find(f => f.filterType === 'PRICE_FILTER');
            symbolRules[s.symbol] = { 
                stepSize: lotSize ? parseFloat(lotSize.stepSize) : 1,
                tickSize: priceFilter ? parseFloat(priceFilter.tickSize) : 0.0001
            };
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

function formatPrice(symbol, price) {
    if (!symbolRules[symbol]) return price.toFixed(4);
    const tickSize = symbolRules[symbol].tickSize;
    let precision = tickSize.toString().includes('.') ? tickSize.toString().split('.')[1].length : 0;
    
    // Fallback to ensure we don't truncate extremely small coins to zero
    if (precision < 4) precision = 4;
    return Number.parseFloat(price).toFixed(precision);
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
            console.log(`✅ Loaded ${count} active/pending positions from memory.`);
        }
    } catch (e) { activePositions = {}; }
}

async function savePositions() {
    try { await fs.writeFile(POSITIONS_FILE, JSON.stringify(activePositions, null, 2)); } 
    catch (e) { console.error('❌ Error saving positions.'); }
}

// ==========================================
// 5. Order Execution
// ==========================================
async function updateWalletBalance() {
    const data = await binancePrivateRequest('/api/v3/account', 'GET');
    if (data && data.balances) {
        const usdt = data.balances.find(b => b.asset === 'USDT');
        if (usdt) liveWalletBalance = parseFloat(usdt.free).toFixed(2);
    }
}

async function executeTrade(symbol, side, quoteQty = null, coinQty = null, limitPrice = null) {
    let params = { symbol, side };
    
    if (limitPrice) {
        params.type = 'LIMIT';
        params.timeInForce = 'GTC';
        params.price = formatPrice(symbol, limitPrice);
        params.quantity = formatQuantity(symbol, quoteQty / limitPrice);
    } else {
        params.type = 'MARKET';
        if (side === 'BUY') params.quoteOrderQty = parseFloat(quoteQty).toFixed(2);
        else params.quantity = formatQuantity(symbol, coinQty);
    }
    
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

async function cancelOrder(symbol, orderId) {
    return await binancePrivateRequest('/api/v3/order', 'DELETE', { symbol, orderId });
}

// ==========================================
// 6. Advanced Position Management (Limit Orders)
// ==========================================
async function managePosition(symbol, currentPrice, decision, dynamicSL = null) {
    
    // 1. EXECUTE PENDING BUY (Limit Order at Support)
    if (decision === 'BUY' && !activePositions[symbol]) {
        if (Object.keys(activePositions).length >= RISK_RULES.maxTrades) return;
        if (parseFloat(liveWalletBalance) < RISK_RULES.tradeAmountUSDT) return;

        // Calculate support level for entry
        const entrySupportPrice = currentPrice * (1 - RISK_RULES.pullbackEntryPct);
        console.log(`\n⏳ [PLACING LIMIT BUY] ${symbol} at $${formatPrice(symbol, entrySupportPrice)}...`);
        
        const order = await executeTrade(symbol, 'BUY', RISK_RULES.tradeAmountUSDT, null, entrySupportPrice);
        
        if (order && order.status === 'NEW') {
            // Ensure dynamic SL is not higher than or equal to the actual buying price
            let finalSL = dynamicSL ? dynamicSL : entrySupportPrice * (1 - RISK_RULES.fallbackStopLossPct);
            if (finalSL >= entrySupportPrice) {
                finalSL = entrySupportPrice * (1 - RISK_RULES.fallbackStopLossPct);
            }

            activePositions[symbol] = { 
                status: 'PENDING',
                orderId: order.orderId,
                targetEntryPrice: entrySupportPrice,
                highestPrice: entrySupportPrice, 
                stopLoss: finalSL, 
                trailingActive: false, 
                time: Date.now() 
            };
            await savePositions(); 
            
            sendTelegramMessage(`🎣 <b>LIMIT ORDER PLACED</b>\n<b>Symbol:</b> ${symbol}\n<b>Waiting to Buy at:</b> $${formatPrice(symbol, entrySupportPrice)}\n<b>SL:</b> $${formatPrice(symbol, finalSL)}`);
            return;
        }
    }

    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        let memoryNeedsUpdate = false;

        // 2. CHECK PENDING ORDERS
        if (trade.status === 'PENDING') {
            const checkOrder = await binancePrivateRequest('/api/v3/order', 'GET', { symbol, orderId: trade.orderId });
            
            if (checkOrder && checkOrder.status === 'FILLED') {
                trade.status = 'ACTIVE';
                trade.entryPrice = parseFloat(checkOrder.price || trade.targetEntryPrice);
                trade.qty = parseFloat(checkOrder.executedQty);
                memoryNeedsUpdate = true;
                sendTelegramMessage(`🟢 <b>ORDER FILLED!</b>\n<b>Symbol:</b> ${symbol} bought at $${formatPrice(symbol, trade.entryPrice)}`);
                await updateWalletBalance();
            } else {
                const orderAgeMinutes = (Date.now() - trade.time) / (1000 * 60);
                if (orderAgeMinutes >= RISK_RULES.orderExpiryMinutes) {
                    await cancelOrder(symbol, trade.orderId);
                    delete activePositions[symbol];
                    await savePositions();
                    sendTelegramMessage(`⌛ <b>ORDER CANCELLED</b>\n<b>Symbol:</b> ${symbol}\nReason: Price did not reach support in ${RISK_RULES.orderExpiryMinutes} mins.`);
                    return;
                }
            }
        }

        // 3. MANAGE ACTIVE POSITIONS
        if (trade.status === 'ACTIVE') {
            if (currentPrice > trade.highestPrice) { 
                trade.highestPrice = currentPrice; 
                memoryNeedsUpdate = true; 
            }
            
            const profitPct = (currentPrice - trade.entryPrice) / trade.entryPrice;
            const profitUSDT = (currentPrice - trade.entryPrice) * trade.qty;

            // Take Profit Activation
            if (!trade.trailingActive && profitPct >= RISK_RULES.trailingActivationPct) { 
                trade.trailingActive = true; 
                memoryNeedsUpdate = true; 
                sendTelegramMessage(`🚀 <b>TARGET HIT (4%)</b>\n<b>Symbol:</b> ${symbol}\n<b>Current Profit:</b> +$${profitUSDT.toFixed(2)} USDT\nTrailing Stop Activated!`);
            }
            
            // Trailing Stop Logic
            if (trade.trailingActive) {
                const newSL = trade.highestPrice * (1 - RISK_RULES.trailingDistancePct);
                if (newSL > trade.stopLoss) { trade.stopLoss = newSL; memoryNeedsUpdate = true; }
            }
            
            if (memoryNeedsUpdate) await savePositions(); 

            // Execute Sell
            if (currentPrice <= trade.stopLoss || decision === 'SELL') {
                const order = await executeTrade(symbol, 'SELL', null, trade.qty);
                
                if (order && (order.status === 'FILLED' || order.status === 'NEW')) {
                    const finalProfitPct = (profitPct * 100).toFixed(2);
                    const finalProfitUSDT = profitUSDT.toFixed(2);
                    const exitReason = currentPrice <= trade.stopLoss ? 'Stop Loss / Trailing Hit' : 'SELL Signal';
                    
                    testStats.totalTrades++;
                    if (profitUSDT > 0) testStats.winningTrades++;
                    testStats.totalProfitUSDT += parseFloat(finalProfitUSDT);
                    
                    cooldowns[symbol] = Date.now() + (15 * 60 * 1000); 
                    
                    const emoji = profitUSDT >= 0 ? '✅ PROFIT' : '❌ LOSS';
                    sendTelegramMessage(`🔴 <b>SELL EXECUTED</b>\n<b>Symbol:</b> ${symbol}\n<b>Reason:</b> ${exitReason}\n<b>Result:</b> ${emoji}\n<b>PnL:</b> ${finalProfitUSDT} USDT (${finalProfitPct}%)`);
                    
                    delete activePositions[symbol]; 
                    await savePositions(); 
                    await updateWalletBalance();
                }
            }
        }
    }
}

// ==========================================
// 7. Market Scanner (LOMY AI Whale Hunter)
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
            .slice(0, 20); 

        for (let pair of usdtPairs) {
            latestMarketPrices[pair.symbol] = parseFloat(pair.lastPrice);
            
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
                    dynamicSL = signalLow * 0.998; 
                }
                
                if (bearish && cmo[cmo.length - 2] < -30) { decision = 'SELL'; }

                await managePosition(pair.symbol, currentPrice, decision, dynamicSL);
                await sleep(150); 

            } catch (e) {} 
        }

        // Maintain Active & Pending Orders
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
// 8. Initialization & Web Dashboard
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
        let trade = activePositions[sym];
        if (trade.status === 'PENDING') {
            return { symbol: sym, status: 'PENDING', pnl: 0, pnlUsdt: 0, sl: formatPrice(sym, trade.stopLoss) };
        }
        const current = latestMarketPrices[sym] || trade.entryPrice;
        const pnlPct = (((current - trade.entryPrice) / trade.entryPrice) * 100).toFixed(2);
        const pnlUsdt = ((current - trade.entryPrice) * trade.qty).toFixed(2);
        return { symbol: sym, status: 'ACTIVE', pnl: parseFloat(pnlPct), pnlUsdt: parseFloat(pnlUsdt), sl: formatPrice(sym, trade.stopLoss) };
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
        table{width:100%;max-width:900px;margin:0 auto;border-collapse:collapse;background:#1e2329;border-radius:8px;overflow:hidden;} 
        th,td{padding:15px;border-bottom:1px solid #2b3139;} 
        th{background:#2b3139;color:#848e9c;}
        .profit{color:#0ecb81;font-weight:bold;} 
        .loss{color:#f6465d;font-weight:bold;}
        .empty{color:#848e9c;padding:30px;}
        .badge-act{background:rgba(14,203,129,0.2);color:#0ecb81;padding:3px 8px;border-radius:4px;font-size:12px;}
        .badge-pend{background:rgba(243,186,47,0.2);color:#f3ba2f;padding:3px 8px;border-radius:4px;font-size:12px;}
    </style></head><body>
    <div class="header"><h1>🤖 LOMY Super Bot (Pro Version)</h1><p>Timeframe: 15m | Risk: 2% SL / 4% TP | Pullback Entry</p></div>
    <div class="panel">Wallet: <span class="bal">$<span id="bal">${liveWalletBalance}</span></span></div>
    
    <div class="stats-container">
        <div class="stat-box">Total Trades: <span id="tot-trades">0</span></div>
        <div class="stat-box">Net PnL: $<span id="net-profit">0.00</span> USDT</div>
    </div>

    <table><thead><tr><th>Asset</th><th>Status</th><th>Stop Loss</th><th>PnL (USDT)</th><th>PnL (%)</th></tr></thead><tbody id="tbl"></tbody></table>
    <script>
    async function load(){
        try {
            const res = await (await fetch('/api/data')).json();
            document.getElementById('bal').innerText = res.balance;
            document.getElementById('tot-trades').innerText = res.stats.totalTrades;
            
            let profitEl = document.getElementById('net-profit');
            profitEl.innerText = res.stats.totalProfitUSDT.toFixed(2);
            profitEl.className = res.stats.totalProfitUSDT >= 0 ? 'profit' : 'loss';

            let html = '';
            if(res.positions.length === 0){
                html = '<tr><td colspan="5" class="empty">Scanning markets using LOMY Logic... 📡</td></tr>';
            } else {
                res.positions.forEach(p => {
                    let badge = p.status === 'ACTIVE' ? '<span class="badge-act">🔥 ACTIVE</span>' : '<span class="badge-pend">⏳ PENDING</span>';
                    let pnlClass = p.pnl >= 0 ? 'profit' : 'loss';
                    let pnlSign = p.pnl > 0 ? '+' : '';
                    let pnlText = p.status === 'PENDING' ? 'Waiting' : \`\${pnlSign}\${p.pnlUsdt} USDT\`;
                    let pctText = p.status === 'PENDING' ? '-' : \`\${pnlSign}\${p.pnl}%\`;
                    
                    html += \`<tr><td><strong>\${p.symbol}</strong></td><td>\${badge}</td><td>$\${p.sl}</td><td class="\${pnlClass}">\${pnlText}</td><td class="\${pnlClass}">\${pctText}</td></tr>\`;
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
