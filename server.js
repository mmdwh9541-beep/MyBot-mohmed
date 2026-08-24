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
    maxTrades: 20,           // Maximum concurrent open positions
    stopLossPct: 0.01,       // 1% Stop Loss
    takeProfitPct: 0.02,     // 2% Take Profit
    dailyLossLimitPct: 0.10  // 10% Maximum daily loss limit from total portfolio equity
};

let exchangeRules = {}; 
let latestResults = [];
let activePositions = {}; 
let tradeHistory = []; 
let testStats = { totalTrades: 0, winningTrades: 0, totalProfitUSDT: 0 };
let liveWalletBalance = "0.00"; // Available free USDT balance only

// 🛑 Daily loss protection variables
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
            sendTelegramMessage("🌅 <b>New Trading Day!</b>\nDaily loss counter reset and bot re-enabled.");
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
async function binancePrivateRequest(endpoint, method = 'GET', params = {
