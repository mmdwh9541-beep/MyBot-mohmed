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
    maxTrades: 20,           // أقصى عدد للصفقات المفتوحة معاً
    stopLossPct: 0.025,      // 2.5% وقف خسارة
    takeProfitPct: 0.05,     // 5% هدف ربح
    dailyLossLimitPct: 0.10  // 10% حد أقصى للخسارة اليومية من إجمالي المحفظة
};

let exchangeRules = {}; 
let latestResults = [];
let activePositions = {}; 
let tradeHistory = []; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitUSDT: 0 };
let liveWalletBalance = "0.00"; // رصيد الـ USDT المتاح (الحر) فقط

// 🛑 متغيرات حماية الخسارة اليومية
let dailyPnL = 0; 
let currentDay = new Date().getUTCDate();
let tradingPaused = false;

// ==========================================
// 🔔 3. Telegram Queue System
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
// 🛡️ 4. Account Equity & Reset Logic
// ==========================================
// حساب إجمالي قيمة المحفظة (الكاش الحر + قيمة الصفقات المفتوحة)
function getTotalEquity() {
    let total = parseFloat(liveWalletBalance) || 0;
    for (let sym in activePositions) {
        total += (activePositions[sym].qty * activePositions[sym].entryPrice);
    }
    return total;
}

function checkDailyReset() {
    let today = new Date().getUTCDate();
    if (today !== currentDay) {
        currentDay = today;
        dailyPnL = 0;
        if (tradingPaused) {
            tradingPaused = false;
            sendTelegramMessage("🌅 <b>يوم تداول جديد!</b>\nتم تصفير عداد الخسارة اليومية وإعادة تفعيل البوت لاصطياد الفرص.");
        }
    }
}

// ==========================================
// 🛠️ 5. Binance Precision Rules
// ==========================================
async function loadExchangeRules() {
    try {
        const res = await axios.get(`${TESTNET_URL}/api/v3/exchangeInfo`);
        res.data.symbols.forEach(s => {
            const lotSize = s.filters.find(f => f.filterType === 'LOT_SIZE');
            exchangeRules[s.symbol] = { stepSize: lotSize ? parseFloat(lotSize.stepSize) : 1 };
        });
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
// 🔐 6. Binance API Functions
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

async function executeTrade(symbol, side, amountOrQty) {
    let params = { symbol: symbol, side: side, type: 'MARKET' };
    if (side === 'BUY') {
        params.quoteOrderQty = amountOrQty.toFixed(2).toString(); // كمية الـ USDT المطلوبة
    } else {
        params.quantity = formatQuantity(symbol, amountOrQty); // كمية العملة للبيع
    }
    return await binancePrivateRequest('/api/v3/order', 'POST', params);
}

// ==========================================
// 🧠 7. Auto-Trading Engine (Dynamic Sizing)
// ==========================================
async function processTradeAction(symbol, currentPrice, decision) {
    
    // الشراء
    if (decision === 'BUY' && !activePositions[symbol]) {
        if (tradingPaused) return 'PAUSED';
        
        // التحقق من الحد الأقصى للصفقات (20 صفقة)
        if (Object.keys(activePositions).length >= RISK_RULES.maxTrades) return 'MAX_TRADES';

        // حساب الكمية الديناميكية (إجمالي المحفظة / 20)
        const totalEquity = getTotalEquity();
        let dynamicTradeAmount = totalEquity / RISK_RULES.maxTrades;

        // حماية من تجاوز الرصيد الحر المتاح
        if (parseFloat(liveWalletBalance) < dynamicTradeAmount) {
            if (parseFloat(liveWalletBalance) > 10) { 
                dynamicTradeAmount = parseFloat(liveWalletBalance) * 0.98; // الدخول بما تبقى من الرصيد
            } else {
                return 'NO_BALANCE'; // رصيد غير كافٍ (أقل من الحد الأدنى لباينانس)
            }
        }

        const orderResult = await executeTrade(symbol, 'BUY', dynamicTradeAmount);
        if (orderResult && orderResult.status === 'FILLED') {
            const entryPrice = parseFloat(orderResult.fills[0] ? orderResult.fills[0].price : currentPrice);
            const qtyBought = parseFloat(orderResult.executedQty);
            
            activePositions[symbol] = {
                entryPrice, qty: qtyBought,
                stopLoss: entryPrice * (1 - RISK_RULES.stopLossPct),
                takeProfit: entryPrice * (1 + RISK_RULES.takeProfitPct)
            };
            sendTelegramMessage(`🟢 <b>تم الشراء (إدارة ديناميكية)</b>\n<b>العملة:</b> ${symbol}\n<b>المبلغ المستخدم:</b> $${dynamicTradeAmount.toFixed(2)}\n<b>الدخول:</b> $${entryPrice.toFixed(4)}\n<b>الهدف:</b> $${activePositions[symbol].takeProfit.toFixed(4)}\n<b>الوقف:</b> $${activePositions[symbol].stopLoss.toFixed(4)}`);
            updateWalletBalance(); 
            return 'BOUGHT';
        }
    }

    // إدارة البيع
    if (activePositions[symbol]) {
        let trade = activePositions[symbol];
        const hitSL = currentPrice <= trade.stopLoss;
        const hitTP = currentPrice >= trade.takeProfit;

        if (hitSL || hitTP) {
            const orderResult = await executeTrade(symbol, 'SELL', trade.qty);
            if (orderResult && (orderResult.status === 'FILLED' || orderResult.status === 'NEW')) {
                const profitUSDT = (currentPrice - trade.entryPrice) * trade.qty; 
                let exitReason = hitSL ? 'ضرب وقف الخسارة' : 'ضرب هدف الربح';
                
                testStats.totalTrades++;
                if (profitUSDT > 0) testStats.winningTrades++;
                testStats.totalProfitUSDT += profitUSDT;
                
                dailyPnL += profitUSDT;
                
                const emoji = profitUSDT >= 0 ? '✅ ربح' : '❌ خسارة';
                sendTelegramMessage(`🔴 <b>تم البيع</b>\n<b>العملة:</b> ${symbol}\n<b>النتيجة:</b> ${emoji} ($${profitUSDT.toFixed(2)})`);
                
                // 🛑 فحص قاطع التيار (خسارة 10% من المحفظة)
                const totalEquity = getTotalEquity();
                const maxLossAllowed = totalEquity * RISK_RULES.dailyLossLimitPct;

                if (dailyPnL <= -maxLossAllowed && !tradingPaused) {
                    tradingPaused = true;
                    sendTelegramMessage(`🛑 <b>إيقاف طارئ للتداول!</b>\nوصلت الخسارة اليومية إلى (${dailyPnL.toFixed(2)}$) وهو ما يمثل 10% من إجمالي المحفظة.\nلن يتم فتح أي صفقات جديدة اليوم.`);
                }

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
// 📊 8. Super Scanner (5m timeframe)
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
        const res = await axios.get(`${TESTNET_URL}/api/v3/klines?symbol=${symbol}&interval=5m&limit=30`);
        if (!res.data || res.data.length < 30) return null;
        
        const candles = res.data.map(c => ({ open: parseFloat(c[1]), high: parseFloat(c[2]), low: parseFloat(c[3]), close: parseFloat(c[4]), volume: parseFloat(c[5]) }));
        const currentPrice = candles[candles.length - 1].close; 
        const closedCandle = candles[candles.length - 2]; 
        
        const volSMA = calculateSMA(candles, 10, 'volume');
        const cmo = calculateCMO(candles, 9);
        const closedVolSMA = volSMA[volSMA.length - 2];
        const closedCmo = cmo[cmo.length - 2];
        
        const highVolume = closedCandle.volume > (closedVolSMA * 1.3);
        const bodyRatio = (closedCandle.high - closedCandle.low) > 0 ? (Math.abs(closedCandle.close - closedCandle.open) / (closedCandle.high - closedCandle.low)) : 0;
        
        let decision = 'WAIT';
        if (closedCandle.close > closedCandle.open && bodyRatio > 0.5 && highVolume && closedCmo > 30) {
            decision = 'BUY';
        }

        const tradeStatus = await processTradeAction(symbol, currentPrice, decision);
        return { symbol, decision: tradeStatus, cmo: closedCmo.toFixed(2), spike: highVolume ? 'YES' : 'NO' };
    } catch (e) { return null; }
}

async function runFullScan() {
    try {
        checkDailyReset(); 

        const response = await axios.get(`${TESTNET_URL}/api/v3/ticker/24hr`);
        const ignoredCoins = ['USDC', 'FDUSD', 'TUSD', 'USDP', 'BUSD', 'EUR', 'USD1'];
        
        let topCoins = response.data
            .filter(t => t.symbol.endsWith('USDT') && !ignoredCoins.some(stable => t.symbol.includes(stable)))
            .sort((a, b) => parseFloat(b.quoteVolume) - parseFloat(a.quoteVolume))
            .slice(0, 200).map(t => t.symbol);
        
        let currentScan = [];
        
        for (const coin of topCoins) {
            const result = await analyzeMarket(coin);
            if (result && (result.decision !== 'WAIT' || result.spike === 'YES' || activePositions[result.symbol])) {
                currentScan.push(result);
            }
            await new Promise(resolve => setTimeout(resolve, 150));
        }

        for (let i = 0; i < 5; i++) {
            if (topCoins[i] && !currentScan.find(c => c.symbol === topCoins[i])) {
                currentScan.push({ symbol: topCoins[i], decision: (tradingPaused ? 'PAUSED' : 'WAIT'), cmo: '0.00', spike: 'NO' });
            }
        }
        latestResults = currentScan; 
    } catch (e) { 
    } finally {
        setTimeout(runFullScan, 5000); 
    }
}

// ==========================================
// 🚨 9. API Endpoints (Emergency Close)
// ==========================================
app.post('/api/emergency-close', async (req, res) => {
    let closedCount = 0;
    const symbolsToClose = Object.keys(activePositions);
    
    if (symbolsToClose.length === 0) {
        return res.json({ success: false, msg: 'لا توجد صفقات مفتوحة حالياً.' });
    }

    sendTelegramMessage(`🚨 <b>جاري الإغلاق الطارئ لـ ${symbolsToClose.length} صفقات!</b>`);

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
                dailyPnL += profitUSDT; 
                delete activePositions[symbol];
                closedCount++;
            }
        } catch (e) { }
        await new Promise(resolve => setTimeout(resolve, 200)); 
    }
    
    updateWalletBalance();
    sendTelegramMessage(`✅ <b>اكتمل الإغلاق الطارئ!</b>\nتم إغلاق ${closedCount} صفقات.`);
    res.json({ success: true, msg: `تم إغلاق ${closedCount} صفقات بنجاح.` });
});

// ==========================================
// 🌐 10. Web Server (Dashboard)
// ==========================================
app.get('/api/data', (req, res) => {
    const equity = getTotalEquity();
    const limit = equity * RISK_RULES.dailyLossLimitPct;
    res.json({ live: latestResults, stats: testStats, balance: liveWalletBalance, equity: equity.toFixed(2), dailyPnL: dailyPnL.toFixed(2), limit: limit.toFixed(2), isPaused: tradingPaused });
});

app.get('/', (req, res) => {
    res.send(`<!DOCTYPE html><html><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Pro Binance Bot</title><style>body{background-color:#0b0e11;color:#eaecef;font-family:Arial;text-align:center;padding:20px;}h1{color:#f3ba2f;}.wallet{font-size:20px;color:#0ecb81;margin-bottom:20px;font-weight:bold;border:2px dashed #2b3139;padding:10px;display:inline-block;border-radius:10px;}.stats-container{display:flex;justify-content:center;gap:20px;margin-bottom:20px;flex-wrap:wrap;}.stat-box{background-color:#1e2329;padding:15px 30px;border-radius:8px;font-weight:bold;border:1px solid #2b3139;}table{width:100%;max-width:1000px;margin:10px auto;border-collapse:collapse;background-color:#1e2329;border-radius:8px;font-size:14px;}th,td{padding:12px;border-bottom:1px solid #2b3139;}th{background-color:#2b3139;color:#848e9c;}.buy{color:#0ecb81;font-weight:bold;}.sell{color:#f6465d;font-weight:bold;}.wait{color:#848e9c;}.spike{color:#f3ba2f;font-weight:bold;}.btn-danger{background-color:#f6465d;color:#fff;border:none;padding:12px 25px;border-radius:5px;font-weight:bold;cursor:pointer;font-size:16px;margin-bottom:20px;}.btn-danger:hover{background-color:#c93346;}.badge-pause{background:#f6465d;color:#fff;padding:5px 10px;border-radius:5px;font-size:14px;margin-top:10px;display:none;}</style></head><body><h1>🤖 LOMY Ultra-Fast Engine</h1><div class="wallet">💰 Total Equity: $<span id="total-equity">Loading...</span> | Free USDT: $<span id="wallet-balance">...</span></div><br><div id="pause-badge" class="badge-pause">🛑 البوت في وضع الإيقاف (تم تجاوز حد الخسارة 10%)</div>
    
    <div><button class="btn-danger" onclick="emergencyClose()">🚨 إغلاق كل الصفقات</button></div>
    
    <div class="stats-container"><div class="stat-box">Trades: <span id="tot-trades">0</span></div><div class="stat-box">Total Profit: <span id="net-profit">$0.00</span></div><div class="stat-box">Today PnL: <span id="daily-pnl">$0.00</span> (Limit: -$<span id="daily-limit">0.00</span>)</div></div><div style="overflow-x:auto;"><table><thead><tr><th>Symbol</th><th>Status</th><th>CMO</th><th>Whale</th></tr></thead><tbody id="live-table"><tr><td colspan="4">Scanning Market (Top 200)... 📡</td></tr></tbody></table></div><script>async function loadData(){try{const res=await fetch('/api/data');const data=await res.json();document.getElementById('total-equity').innerText=data.equity;document.getElementById('wallet-balance').innerText=data.balance;document.getElementById('tot-trades').innerText=data.stats.totalTrades;let profitEl=document.getElementById('net-profit');profitEl.innerText='$' + data.stats.totalProfitUSDT.toFixed(2);profitEl.className=data.stats.totalProfitUSDT>=0?'buy':'sell';let dailyEl=document.getElementById('daily-pnl');dailyEl.innerText='$' + data.dailyPnL;dailyEl.className=data.dailyPnL>=0?'buy':'sell';document.getElementById('daily-limit').innerText=data.limit;if(data.isPaused){document.getElementById('pause-badge').style.display='inline-block';}else{document.getElementById('pause-badge').style.display='none';}if(data.live.length>0){let liveTbody=document.getElementById('live-table');liveTbody.innerHTML='';data.live.forEach(item=>{let decClass=item.decision.includes('BOUGHT')||item.decision.includes('HOLDING')?'buy':item.decision.includes('SELL')||item.decision.includes('CLOSED')||item.decision==='PAUSED'||item.decision.includes('MAX_TRADES')?'sell':'wait';let displayDecision=item.decision==='MAX_TRADES'?'MAX TRADES REACHED':item.decision;liveTbody.innerHTML+=\`<tr><td>\${item.symbol}</td><td class="\${decClass}">\${displayDecision}</td><td>\${item.cmo}</td><td class="\${item.spike.includes('YES')?'spike':''}">\${item.spike}</td></tr>\`;});}}catch(e){}}setInterval(loadData,4000);loadData();
    async function emergencyClose(){if(!confirm('هل أنت متأكد من إغلاق جميع الصفقات المفتوحة فوراً؟')) return;try{const res=await fetch('/api/emergency-close',{method:'POST'});const data=await res.json();alert(data.msg);loadData();}catch(e){alert('حدث خطأ بالاتصال بالسيرفر');}}
    </script></body></html>`);
});

// ==========================================
// 🚀 11. Initialization
// ==========================================
app.listen(PORT, async () => { 
    console.log('🚀 Server is running on port ' + PORT); 
    await loadExchangeRules();
    setTimeout(updateWalletBalance, 2000); 
    setInterval(updateWalletBalance, 60000);
    
    runFullScan();
});
