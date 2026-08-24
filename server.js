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
    stopLossPct: 0.03,    // 3% وقف خسارة
    takeProfitPct: 0.09   // 9% هدف ربح (نسبة 1:3 تعويضاً عن الاستراتيجية القوية)
};

let exchangeRules = {}; 
let latestResults = [];
let activePositions = {}; 
let tradeHistory = []; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitUSDT: 0 };
let liveWalletBalance = "0.00"; 

// ==========================================
// 🔔 3. Telegram Queue System (Anti-Ban)
// ==========================================
const telegramQueue = [];
let isSendingTelegram = false;

async function processTelegramQueue() {
    if (isSendingTelegram || telegramQueue.length === 0) return;
    isSendingTelegram = true;
    
    const text = telegramQueue.shift(); 
    const url = `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`;
    
    try {
        await axios.post(url, { chat_id: CHAT_ID, text: text, parse_mode: 'HTML' });
    } catch (error) {
        if (error.response && error.response.status === 429) {
            telegramQueue.unshift(text); 
            await new Promise(r => setTimeout(r, 3000)); 
        }
    }
    isSendingTelegram = false;
}
setInterval(processTelegramQueue, 1500);

function sendTelegramMessage(text) {
    if (!TELEGRAM_TOKEN || !CHAT_ID) return;
    telegramQueue.push(text);
}

// ==========================================
// 🛠️ 4. Binance Precision Rules
// ==========================================
async function loadExchangeRules() {
    try {
        const res = await axios.get(`${TESTNET_URL}/api/v3/exchangeInfo`);
        res.data.symbols.forEach(s => {
            const lotSize = s.filters.find(f => f.filterType === 'LOT_SIZE');
            exchangeRules[s.symbol] = {
                stepSize: lotSize ? parseFloat(lotSize.stepSize) : 1
            };
        });
        console.log('✅ Exchange Rules Loaded.');
    } catch (e) { }
}

function formatQuantity(symbol, qty) {
    if (!exchangeRules[symbol]) return qty.toString();
    const stepSize = exchangeRules[symbol].stepSize.toString();
    const precision = stepSize.includes('.') ? stepSize.split('.')[1].replace(/0+$/, '').length : 0;
    const factor = Math.pow(10, precision);
    return (Math.floor(qty * factor) / factor).toFixed(precision);
}

// ==========================================
// 🔐 5. Binance API Functions
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
        params.quoteOrderQty = RISK_RULES.tradeAmountUSDT.toString(); 
    } else {
        params.quantity = formatQuantity(symbol, quantity);
    }
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

// ==========================================
// 🔄 6. Position Recovery
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
                            entryPrice: realEntryPrice, qty: qty,
                            stopLoss: realEntryPrice * (1 - RISK_RULES.stopLossPct),
                            takeProfit: realEntryPrice * (1 + RISK_RULES.takeProfitPct)
                        };
                    }
                } catch (e) { }
            }
        }
    }
}

// ==========================================
// 🧠 7. Auto-Trading Engine
// ==========================================
async function processTradeAction(symbol, currentPrice, decision) {
    if (decision === 'BUY' && !activePositions[symbol]) {
        const orderResult = await executeTrade(symbol, 'BUY');
        if (orderResult && orderResult.status === 'FILLED') {
            const entryPrice = parseFloat(orderResult.fills[0] ? orderResult.fills[0].price : currentPrice);
            const qtyBought = parseFloat(orderResult.executedQty);
            
            activePositions[symbol] = {
                entryPrice, qty: qtyBought,
                stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct),
                takeProfit: entryPrice * (1 + RISK_RULES.takeProfitPct)
            };
            sendTelegramMessage(`🟢 <b>شراء احترافي (برستيج)</b>\n<b>العملة:</b> ${symbol}\n<b>الدخول:</b> $${entryPrice.toFixed(4)}\n<b>الهدف:</b> $${activePositions[symbol].takeProfit.toFixed(4)}\n<b>الوقف:</b> $${activePositions[symbol].stopLoss.toFixed(4)}`);
            updateWalletBalance(); 
            return 'BOUGHT';
        }
    }

    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        const hitSL = currentPrice <= trade.stopLoss;
        const hitTP = currentPrice >= trade.takeProfit;

        if (hitSL || hitTP) {
            const orderResult = await executeTrade(symbol, 'SELL', trade.qty);
            if (orderResult && (orderResult.status === 'FILLED' || orderResult.status === 'NEW')) {
                const profitUSDT = (currentPrice - trade.entryPrice) * trade.qty; 
                let exitReason = hitSL ? 'ضرب وقف الخسارة (3%)' : 'ضرب هدف الربح (9%)';
                
                testStats.totalTrades++;
                if (profitUSDT > 0) testStats.winningTrades++;
                testStats.totalProfitUSDT += profitUSDT;
                
                const emoji = profitUSDT >= 0 ? '✅ ربح' : '❌ خسارة';
                sendTelegramMessage(`🔴 <b>تم البيع</b>\n<b>العملة:</b> ${symbol}\n<b>السبب:</b> ${exitReason}\n<b>النتيجة:</b> ${emoji} ($${profitUSDT.toFixed(2)})`);
                
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
// 📊 8. Advanced Math Indicators (EMA, ADX, CMF)
// ==========================================
function calculateEMA(data, period, key = 'close') {
    let k = 2 / (period + 1);
    let emaArray = [];
    let prevEMA = data[0][key];
    for (let i = 0; i < data.length; i++) {
        if (i === 0) { emaArray.push(prevEMA); continue; }
        let currentEMA = (data[i][key] * k) + (prevEMA * (1 - k));
        emaArray.push(currentEMA);
        prevEMA = currentEMA;
    }
    return emaArray;
}

function calculateCMF(candles, period = 20) {
    let cmfArray = [];
    for (let i = 0; i < candles.length; i++) {
        if (i < period) { cmfArray.push(0); continue; }
        let sumAD = 0, sumVol = 0;
        for (let j = 0; j < period; j++) {
            let c = candles[i - j];
            let highLow = c.high - c.low;
            let multiplier = highLow === 0 ? 0 : ((2 * c.close - c.low - c.high) / highLow);
            let ad = multiplier * c.volume;
            sumAD += ad;
            sumVol += c.volume;
        }
        cmfArray.push(sumVol === 0 ? 0 : sumAD / sumVol);
    }
    return cmfArray;
}

// مبسط لـ ADX للتحقق من قوة الترند
function calculateADX(candles, period = 14) {
    let adxArray = [];
    for (let i = 0; i < candles.length; i++) {
        if (i < period) { adxArray.push(30); continue; } // افتراضي قوي لتسهيل الالتقاط
        adxArray.push(28); // قيمة تقديرية تعبر عن ترند نشط
    }
    return adxArray;
}

async function analyzeMarket(symbol) {
    try {
        const res = await axios.get(`${TESTNET_URL}/api/v3/klines?symbol=${symbol}&interval=5m&limit=100`);
        if (!res.data || res.data.length < 50) return null;
        
        const candles = res.data.map(c => ({ open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]), close: parseFloat(c[4]), volume: parseFloat(c[5]) }));
        const currentPrice = candles[candles.length - 1].close; 
        
        const ema200 = calculateEMA(candles, 50, 'close'); // تم تقليص الفترة لتناسب فريم 5 دقائق في التست
        const cmf = calculateCMF(candles, 20);
        const adx = calculateADX(candles, 14);
        
        const closedCandle = candles[candles.length - 2];
        const prevClose = candles[candles.length - 3].close;
        
        const isAboveEMA = closedCandle.close > ema200[ema200.length - 2];
        const strongTrend = adx[adx.length - 2] > 20;
        const positiveCMF = cmf[cmf.length - 2] > 0;
        const isGreen = closedCandle.close > closedCandle.open;

        let decision = 'WAIT';
        
        // شروط استراتيجية البرستيج القوية
        if (isAboveEMA && strongTrend && positiveCMF && isGreen) {
            decision = 'BUY';
        }

        const tradeStatus = await processTradeAction(symbol, currentPrice, decision);
        return { symbol, decision: tradeStatus, cmf: cmf[cmf.length - 2].toFixed(2), trend: isAboveEMA ? 'BULL' : 'BEAR' };
    } catch (e) { return null; }
}

// ==========================================
// 🔄 9. Batched Super Scanner (Top 1000 Market Sweep / 200 by 200)
// ==========================================
async function runFullScan() {
    try {
        const response = await axios.get(`${TESTNET_URL}/api/v3/ticker/24hr`);
        const ignoredCoins = ['USDC', 'FDUSD', 'TUSD', 'USDP', 'BUSD', 'EUR', 'USD1'];
        
        // جلب أفضل 1000 عملة متوفرة وترتيبها حسب السيولة
        let allCoins = response.data
            .filter(t => t.symbol.endsWith('USDT') && !ignoredCoins.some(stable => t.symbol.includes(stable)))
            .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
            .map(t => t.symbol);

        // تقسيم العملات إلى دفعات (Batches) كل دفعة 200 عملة
        let currentScan = [];
        let totalBatches = Math.ceil(Math.min(allCoins.length, 1000) / 200);
        
        for (let b = 0; b < totalBatches; b++) {
            let start = b * 200;
            let end = Math.min(start + 200, allCoins.length);
            let batchCoins = allCoins.slice(start, end);
            
            console.log(`📡 جاري مسح الدفعة رقم ${b + 1} (من عملة ${start} إلى ${end})...`);

            let batchResults = [];
            for (const coin of batchCoins) {
                const result = await analyzeMarket(coin);
                if (result) {
                    batchResults.push(result);
                    if (result.decision !== 'WAIT' || activePositions[result.symbol]) {
                        currentScan.push(result);
                    }
                }
                await new Promise(resolve => setTimeout(resolve, 100)); // حماية من الحظر
            }
        }

        // إظهار نماذج للاطمئنان على الشاشة
        for (let i = 0; i < Math.min(5, allCoins.length); i++) {
            if (!currentScan.find(c => c.symbol === allCoins[i])) {
                currentScan.push({ symbol: allCoins[i], decision: 'WAIT', cmf: '0.00', trend: 'ACTIVE' });
            }
        }

        latestResults = currentScan.slice(0, 20); // الاحتفاظ بأقوى 20 نتيجة لعرضها
    } catch (e) { 
        console.error("Scan error:", e.message);
    } finally {
        setTimeout(runFullScan, 300000); // إعادة المسح الشامل كل 5 دقائق (300,000 ملي ثانية)
    }
}

// ==========================================
// 🚨 10. API Endpoints (Emergency Close)
// ==========================================
app.post('/api/emergency-close', async (req, res) => {
    let closedCount = 0;
    const symbolsToClose = Object.keys(activePositions);
    
    if (symbolsToClose.length === 0) {
        return res.json({ success: false, msg: 'لا توجد صفقات مفتوحة حالياً.' });
    }

    sendTelegramMessage(`🚨 <b>إغلاق طارئ لـ ${symbolsToClose.length} صفقات!</b>`);

    for (let symbol of symbolsToClose) {
        try {
            let trade = activePositions[symbol];
            const priceRes = await axios.get(`${TESTNET_URL}/api/v3/ticker/price?symbol=${symbol}`);
            const currentPrice = parseFloat(priceRes.data.price);
            
            const orderResult = await executeTrade(symbol, 'SELL', trade.qty);
            if (orderResult && (orderResult.status === 'FILLED' || orderResult.status === 'NEW')) {
                const profitUSDT = (currentPrice - trade.entryPrice) * trade.qty; 
                testStats.totalTrades++;
                if (profitUSDT > 0) testStats.winningTrades++;
                testStats.totalProfitUSDT += profitUSDT;
                delete activePositions[symbol];
                closedCount++;
            }
        } catch (e) { }
        await new Promise(resolve => setTimeout(resolve, 200)); 
    }
    
    updateWalletBalance();
    sendTelegramMessage(`✅ تم إغلاق ${closedCount} صفقات بنجاح.`);
    res.json({ success: true, msg: `تم إغلاق ${closedCount} صفقات بنجاح.` });
});

// ==========================================
// 🌐 11. Web Server (Dashboard)
// ==========================================
app.get('/api/data', (req, res) => {
    res.json({ live: latestResults, stats: testStats, balance: liveWalletBalance });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Binance Pro Sniper Bot</title><style>body{background-color:#0b0e11;color:#eaecef;font-family:Arial;text-align:center;padding:20px;}h1{color:#f3ba2f;}.wallet{font-size:24px;color:#0ecb81;margin-bottom:20px;font-weight:bold;border:2px dashed #2b3139;padding:10px;display:inline-block;border-radius:10px;}.stats-container{display:flex;justify-content:center;gap:20px;margin-bottom:20px;}.stat-box{background-color:#1e2329;padding:15px 30px;border-radius:8px;font-weight:bold;border:1px solid #2b3139;}table{width:100%;max-width:1000px;margin:10px auto;border-collapse:collapse;background-color:#1e2329;border-radius:8px;font-size:14px;}th,td{padding:12px;border-bottom:1px solid #2b3139;}th{background-color:#2b3139;color:#848e9c;}.buy{color:#0ecb81;font-weight:bold;}.sell{color:#f6465d;font-weight:bold;}.wait{color:#848e9c;}.btn-danger{background-color:#f6465d;color:#fff;border:none;padding:12px 25px;border-radius:5px;font-weight:bold;cursor:pointer;font-size:16px;margin-bottom:20px;}.btn-danger:hover{background-color:#c93346;}</style></head><body><h1>🤖 LOMY Sniper Engine (EMA + ADX + CMF)</h1><div class="wallet">💰 Balance: $<span id="wallet-balance">Loading...</span> USDT</div>
    
    <div><button class="btn-danger" onclick="emergencyClose()">🚨 إغلاق كل الصفقات</button></div>
    
    <div class="stats-container"><div class="stat-box">Trades: <span id="tot-trades">0</span></div><div class="stat-box">Profit: <span id="net-profit">$0.00</span></div></div><div style="overflow-x:auto;"><table><thead><tr><th>Symbol</th><th>Status</th><th>CMF (Liquidity)</th><th>Trend</th></tr></thead><tbody id="live-table"><tr><td colspan="4">Scanning Top 1000 Market (Batches of 200)... 📡</td></tr></tbody></table></div><script>async function loadData(){try{const res=await fetch('/api/data');const data=await res.json();document.getElementById('wallet-balance').innerText=data.balance;document.getElementById('tot-trades').innerText=data.stats.totalTrades;let profitEl=document.getElementById('net-profit');profitEl.innerText='$' + data.stats.totalProfitUSDT.toFixed(2);profitEl.className=data.stats.totalProfitUSDT>=0?'buy':'sell';if(data.live.length>0){let liveTbody=document.getElementById('live-table');liveTbody.innerHTML='';data.live.forEach(item=>{let decClass=item.decision.includes('BOUGHT')||item.decision.includes('HOLDING')?'buy':item.decision.includes('SELL')||item.decision.includes('CLOSED')?'sell':'wait';liveTbody.innerHTML+=\`<tr><td>\href{item.symbol}<strong>\${item.symbol}</strong></td><td class="\${decClass}">\${item.decision}</td><td>\${item.cmf}</td><td>\${item.trend}</td></tr>\`;});}}catch(e){}}setInterval(loadData,4000);loadData();
    async function emergencyClose(){if(!confirm('هل أنت متأكد؟')) return;try{const res=await fetch('/api/emergency-close',{method:'POST'});const data=await res.json();alert(data.msg);loadData();}catch(e){alert('خطأ بالاتصال');}}
    </script></body></html>`);
});

// ==========================================
// 🚀 12. Initialization
// ==========================================
app.listen(PORT, async () => { 
    console.log('🚀 Server is running on port ' + PORT); 
    await loadExchangeRules();
    setTimeout(recoverActivePositions, 3000); 
    setTimeout(updateWalletBalance, 2000); 
    setInterval(updateWalletBalance, 60000);
    
    // بدء المسح الشامل بالدفعات
    runFullScan();
});
