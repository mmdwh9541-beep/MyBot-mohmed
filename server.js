require('dotenv').config();

const express = require('express');
const axios = require('axios');
const WebSocket = require('ws');
const fs = require('fs');
const path = require('path');

const app = express();
app.use(express.json());

const PORT = process.env.PORT || 5000;
const TELEGRAM_TOKEN = process.env.TELEGRAM_TOKEN;
const CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const WS_BASE = 'wss://data-stream.binance.vision';

// ============================================================
// LOMY V4.2 COMPACT
// PAPER ONLY - BINANCE WEBSOCKET ONLY
// ============================================================

const C = {
  version: '4.2-COMPACT',

  startingBalance: 10000,
  maxPositions: 10,
  maxEntriesPerCycle: 2,

  stopLossPct: 0.01,
  takeProfitPct: 0.02,
  feePct: 0.001,
  slippagePct: 0.0005,
  dailyLossLimitPct: 0.10,

  interval: '5m',
  warmup: 55,
  maxCandles: 80,

  universeSize: 300,
  minQuoteVolume: 250000,
  universeRefreshMs: 30 * 60 * 1000,

  // Entry quality
  minScore: 72,
  minCMO: 50,
  maxCMO: 85,
  minVolume: 1.40,
  maxVolume: 4.50,
  maxEma20Distance: 2.25,
  maxBreakoutDistance: 0.75,
  maxExtension5: 3.0,
  maxExtension10: 5.0,
  maxSupportDistance: 6.0,
  minATR: 0.10,
  maxATR: 2.50,
  minBodyRatio: 0.55,

  // Freshness
  candidateExpiryMs: 6 * 60 * 1000,
  maxPriceDriftPct: 0.35,
  rankingDelayMs: 3000,

  // Cooldown
  entriesBeforeCooldown: 10,
  normalCooldownMs: 20 * 60 * 1000,
  lossStreakLimit: 3,
  lossCooldownMs: 60 * 60 * 1000,
  symbolLossCooldownMs: 60 * 60 * 1000,

  // Storage
  journalLimit: 15000,
  historyLimit: 3000,
  stateFile: path.join(__dirname, 'paper-state-v42.json')
};

// ============================================================
// STATE
// ============================================================

let cash = C.startingBalance;
let positions = {};
let history = [];
let journal = [];

let stats = {
  totalTrades: 0,
  wins: 0,
  losses: 0,
  grossProfit: 0,
  grossLoss: 0,
  netProfit: 0,
  fees: 0,
  maxDrawdown: 0
};

let peakEquity = C.startingBalance;
let dailyPnL = 0;
let dailyStartEquity = C.startingBalance;
let currentDay = utcDay();

let manualPause = false;
let dailyPause = false;

let entriesSinceCooldown = 0;
let lossStreak = 0;
let cooldownUntil = 0;
let cooldownReason = null;

const lastStop = {};

const tickers = new Map();
const candles = {};
const lastAnalyzed = {};
const pool = new Map();

let latest = [];
let subscribed = new Set();

let miniWs = null;
let klineWs = null;

let miniConnected = false;
let klineConnected = false;

let lastMiniMessage = 0;
let lastKlineMessage = 0;

let universeReady = false;
let shuttingDown = false;
let rankTimer = null;

// ============================================================
// HELPERS
// ============================================================

const sleep = ms => new Promise(r => setTimeout(r, ms));

function n(v, fallback = 0) {
  const x = Number(v);
  return Number.isFinite(x) ? x : fallback;
}

function utcDay() {
  return new Date().toISOString().slice(0, 10);
}

function pct(diff, base) {
  return base ? (diff / base) * 100 : 0;
}

function clamp(v, a, b) {
  return Math.max(a, Math.min(b, v));
}

function ignored(symbol) {
  return new Set([
    'USDCUSDT',
    'FDUSDUSDT',
    'TUSDUSDT',
    'USDPUSDT',
    'BUSDUSDT',
    'DAIUSDT',
    'USDEUSDT',
    'USD1USDT'
  ]).has(symbol);
}

function sessionUTC() {
  const h = new Date().getUTCHours();

  if (h < 7) return 'ASIA';
  if (h < 13) return 'LONDON';
  if (h < 16) return 'LONDON_NY';
  if (h < 21) return 'NEW_YORK';

  return 'LATE_US';
}

// ============================================================
// TELEGRAM
// ============================================================

const tgQueue = [];
let tgBusy = false;

function tg(text) {
  if (TELEGRAM_TOKEN && CHAT_ID) tgQueue.push(text);
}

setInterval(async () => {
  if (tgBusy || !tgQueue.length) return;

  tgBusy = true;
  const text = tgQueue.shift();

  try {
    await axios.post(
      `https://api.telegram.org/bot${TELEGRAM_TOKEN}/sendMessage`,
      {
        chat_id: CHAT_ID,
        text,
        parse_mode: 'HTML'
      },
      { timeout: 10000 }
    );
  } catch (e) {
    if (e.response?.status === 429) {
      tgQueue.unshift(text);
      await sleep(5000);
    } else {
      console.error('Telegram:', e.message);
    }
  } finally {
    tgBusy = false;
  }
}, 1200);

// ============================================================
// SAVE / LOAD
// ============================================================

let saveTimer = null;

function scheduleSave() {
  if (saveTimer) return;

  saveTimer = setTimeout(() => {
    saveTimer = null;
    saveState();
  }, 20000);
}

function saveState() {
  try {
    const storedCandles = {};

    for (const [symbol, arr] of Object.entries(candles)) {
      if (Array.isArray(arr)) {
        storedCandles[symbol] = arr.slice(-C.maxCandles);
      }
    }

    fs.writeFileSync(
      C.stateFile,
      JSON.stringify({
        cash,
        positions,
        history: history.slice(-C.historyLimit),
        journal: journal.slice(-C.journalLimit),
        stats,
        peakEquity,
        dailyPnL,
        dailyStartEquity,
        currentDay,
        manualPause,
        dailyPause,
        entriesSinceCooldown,
        lossStreak,
        cooldownUntil,
        cooldownReason,
        lastStop,
        candles: storedCandles,
        lastAnalyzed
      })
    );
  } catch (e) {
    console.error('SAVE:', e.message);
  }
}

function loadState() {
  try {
    if (!fs.existsSync(C.stateFile)) {
      console.log('No V4.2 state. Starting fresh.');
      return;
    }

    const s = JSON.parse(
      fs.readFileSync(C.stateFile, 'utf8')
    );

    cash = n(s.cash, C.startingBalance);
    positions = s.positions || {};
    history = s.history || [];
    journal = s.journal || [];
    stats = { ...stats, ...(s.stats || {}) };

    peakEquity = n(s.peakEquity, C.startingBalance);
    dailyPnL = n(s.dailyPnL);
    dailyStartEquity = n(s.dailyStartEquity, C.startingBalance);
    currentDay = s.currentDay || utcDay();

    manualPause = !!s.manualPause;
    dailyPause = !!s.dailyPause;

    entriesSinceCooldown = n(s.entriesSinceCooldown);
    lossStreak = n(s.lossStreak);
    cooldownUntil = n(s.cooldownUntil);
    cooldownReason = s.cooldownReason || null;

    Object.assign(lastStop, s.lastStop || {});
    Object.assign(lastAnalyzed, s.lastAnalyzed || {});

    for (const [symbol, arr] of Object.entries(s.candles || {})) {
      candles[symbol] = Array.isArray(arr)
        ? arr.slice(-C.maxCandles)
        : [];
    }

    console.log(
      `State restored | Cash $${cash.toFixed(2)} | Open ${Object.keys(positions).length}`
    );
  } catch (e) {
    console.error('LOAD:', e.message);
  }
}

// ============================================================
// EQUITY / RISK
// ============================================================

function equity() {
  let total = cash;

  for (const p of Object.values(positions)) {
    const price =
      tickers.get(p.symbol)?.price ||
      p.lastPrice ||
      p.entryPrice;

    total += p.qty * price;
  }

  return total;
}

function updateDrawdown() {
  const e = equity();

  if (e > peakEquity) peakEquity = e;

  const dd = peakEquity
    ? ((peakEquity - e) / peakEquity) * 100
    : 0;

  stats.maxDrawdown = Math.max(stats.maxDrawdown, dd);
}

function checkNewDay() {
  const today = utcDay();

  if (today === currentDay) return;

  currentDay = today;
  dailyPnL = 0;
  dailyPause = false;
  dailyStartEquity = equity();

  scheduleSave();

  tg(
    `🌅 <b>NEW PAPER DAY</b>\nEquity: $${dailyStartEquity.toFixed(2)}`
  );
}

function checkDailyLoss() {
  const limit = dailyStartEquity * C.dailyLossLimitPct;

  if (
    !dailyPause &&
    limit > 0 &&
    dailyPnL <= -limit
  ) {
    dailyPause = true;
    pool.clear();

    tg(
      `🛑 <b>DAILY LOSS LIMIT</b>\nPnL: $${dailyPnL.toFixed(2)}`
    );

    scheduleSave();
  }
}

// ============================================================
// COOLDOWN
// ============================================================

function cooldownActive() {
  if (!cooldownUntil) return false;

  if (Date.now() >= cooldownUntil) {
    cooldownUntil = 0;
    cooldownReason = null;
    entriesSinceCooldown = 0;
    pool.clear();

    tg(
      `▶️ <b>COOLDOWN FINISHED</b>\nFresh signals only.`
    );

    scheduleSave();
    return false;
  }

  return true;
}

function startCooldown(ms, reason) {
  const until = Date.now() + ms;

  if (until <= cooldownUntil) return;

  cooldownUntil = until;
  cooldownReason = reason;
  pool.clear();

  tg(
    `⏸ <b>SMART COOLDOWN</b>\n` +
    `Reason: ${reason}\n` +
    `Duration: ${Math.ceil(ms / 60000)} min`
  );

  scheduleSave();
}

function symbolCooling(symbol) {
  const t = n(lastStop[symbol]);

  return t &&
    Date.now() - t < C.symbolLossCooldownMs;
}

// ============================================================
// INDICATORS
// ============================================================

function sma(arr, period, key) {
  if (arr.length < period) return null;

  return arr
    .slice(-period)
    .reduce((s, x) => s + x[key], 0) / period;
}

function ema(arr, period, key) {
  if (arr.length < period) return null;

  let result =
    arr
      .slice(0, period)
      .reduce((s, x) => s + x[key], 0) / period;

  const k = 2 / (period + 1);

  for (let i = period; i < arr.length; i++) {
    result = (arr[i][key] - result) * k + result;
  }

  return result;
}

function cmo(arr, period = 9) {
  if (arr.length < period + 1) return null;

  let up = 0;
  let down = 0;

  for (let i = arr.length - period; i < arr.length; i++) {
    const d = arr[i].close - arr[i - 1].close;

    if (d > 0) up += d;
    else down += Math.abs(d);
  }

  return up + down
    ? 100 * ((up - down) / (up + down))
    : 0;
}

function atr(arr, period = 14) {
  if (arr.length < period + 1) return null;

  const tr = [];

  for (let i = 1; i < arr.length; i++) {
    tr.push(
      Math.max(
        arr[i].high - arr[i].low,
        Math.abs(arr[i].high - arr[i - 1].close),
        Math.abs(arr[i].low - arr[i - 1].close)
      )
    );
  }

  return tr
    .slice(-period)
    .reduce((a, b) => a + b, 0) / period;
}

function structure(arr) {
  if (arr.length < 2) return 'NEUTRAL';

  const a = arr[arr.length - 1];
  const b = arr[arr.length - 2];

  if (a.high > b.high && a.low > b.low) return 'BULLISH';
  if (a.high < b.high && a.low < b.low) return 'BEARISH';

  return 'NEUTRAL';
}

function supportResistance(arr, period = 20) {
  const r = arr.slice(-period);

  return {
    support: Math.min(...r.map(x => x.low)),
    resistance: Math.max(...r.map(x => x.high))
  };
}

function fibPosition(arr) {
  const r = arr.slice(-30);

  if (r.length < 20) return null;

  const high = Math.max(...r.map(x => x.high));
  const low = Math.min(...r.map(x => x.low));

  if (high <= low) return null;

  return (r[r.length - 1].close - low) / (high - low);
}

// ============================================================
// PRECISION ANALYSIS
// ============================================================

function analyzeSetup(arr) {
  if (arr.length < C.warmup) return null;

  const x = arr[arr.length - 1];

  const e20 = ema(arr, 20, 'close');
  const e50 = ema(arr, 50, 'close');
  const vSma = sma(arr, 20, 'volume');
  const momentum = cmo(arr);
  const a = atr(arr);

  if ([e20, e50, vSma, momentum, a].some(v => v === null)) {
    return null;
  }

  const range = x.high - x.low;
  const body = Math.abs(x.close - x.open);
  const bodyRatio = range ? body / range : 0;

  const bullish = x.close > x.open;
  const struct = structure(arr);

  const volumeRatio = vSma ? x.volume / vSma : 0;
  const atrPct = pct(a, x.close);

  const ema20Distance = pct(x.close - e20, e20);

  const sr = supportResistance(arr);
  const supportDistance = pct(x.close - sr.support, sr.support);

  const previousResistance =
    Math.max(
      ...arr.slice(-21, -1).map(v => v.high)
    );

  const breakout =
    x.close > previousResistance;

  const breakoutDistance =
    pct(
      x.close - previousResistance,
      previousResistance
    );

  const ext5 =
    arr.length >= 6
      ? pct(
          x.close - arr[arr.length - 6].close,
          arr[arr.length - 6].close
        )
      : 0;

  const ext10 =
    arr.length >= 11
      ? pct(
          x.close - arr[arr.length - 11].close,
          arr[arr.length - 11].close
        )
      : 0;

  const fib = fibPosition(arr);

  let trend = 0;
  let entry = 0;
  let risk = 0;
  let penalty = 0;

  const good = [];
  const warnings = [];

  // Trend = 30
  if (x.close > e20) {
    trend += 8;
    good.push('ABOVE_EMA20');
  }

  if (e20 > e50) {
    trend += 12;
    good.push('EMA_TREND');
  }

  if (struct === 'BULLISH') {
    trend += 10;
    good.push('BULLISH_STRUCTURE');
  }

  // Entry = 40
  if (bullish && bodyRatio >= C.minBodyRatio) {
    entry += 10;
    good.push('STRONG_BODY');
  }

  if (
    momentum >= C.minCMO &&
    momentum <= C.maxCMO
  ) {
    entry += 8;
    good.push('CMO_OK');
  }

  if (
    volumeRatio >= C.minVolume &&
    volumeRatio <= C.maxVolume
  ) {
    entry += 10;
    good.push('VOLUME_OK');
  }

  if (
    breakout &&
    breakoutDistance >= 0 &&
    breakoutDistance <= C.maxBreakoutDistance
  ) {
    entry += 12;
    good.push('FRESH_BREAKOUT');
  }

  // Risk = 30
  if (
    ema20Distance >= 0 &&
    ema20Distance <= C.maxEma20Distance
  ) {
    risk += 10;
    good.push('EMA_DISTANCE_OK');
  }

  if (supportDistance <= C.maxSupportDistance) {
    risk += 8;
    good.push('SUPPORT_OK');
  }

  if (
    atrPct >= C.minATR &&
    atrPct <= C.maxATR
  ) {
    risk += 6;
    good.push('ATR_OK');
  }

  if (
    ext5 <= C.maxExtension5 &&
    ext10 <= C.maxExtension10
  ) {
    risk += 6;
    good.push('EXTENSION_OK');
  }

  // Penalties
  if (momentum > C.maxCMO) {
    penalty += 10;
    warnings.push('CMO_HOT');
  }

  if (volumeRatio > C.maxVolume) {
    penalty += 12;
    warnings.push('VOLUME_EXTREME');
  }

  if (ema20Distance > C.maxEma20Distance) {
    penalty += 10;
    warnings.push('EMA20_EXTENDED');
  }

  if (breakoutDistance > C.maxBreakoutDistance) {
    penalty += 10;
    warnings.push('BREAKOUT_EXTENDED');
  }

  if (ext5 > C.maxExtension5) {
    penalty += 8;
    warnings.push('5C_EXTENDED');
  }

  if (ext10 > C.maxExtension10) {
    penalty += 6;
    warnings.push('10C_EXTENDED');
  }

  if (supportDistance > C.maxSupportDistance) {
    penalty += 8;
    warnings.push('FAR_FROM_SUPPORT');
  }

  if (fib !== null && fib > 0.97) {
    penalty += 5;
    warnings.push('RANGE_HIGH');
  }

  const score = clamp(
    trend + entry + risk - penalty,
    0,
    100
  );

  const eligible =
    bullish &&
    struct === 'BULLISH' &&
    e20 > e50 &&
    momentum >= C.minCMO &&
    momentum <= C.maxCMO &&
    volumeRatio >= C.minVolume &&
    volumeRatio <= C.maxVolume &&
    bodyRatio >= C.minBodyRatio &&
    breakout &&
    breakoutDistance >= 0 &&
    breakoutDistance <= C.maxBreakoutDistance &&
    ema20Distance >= 0 &&
    ema20Distance <= C.maxEma20Distance &&
    supportDistance <= C.maxSupportDistance &&
    ext5 <= C.maxExtension5 &&
    ext10 <= C.maxExtension10 &&
    atrPct >= C.minATR &&
    atrPct <= C.maxATR &&
    score >= C.minScore;

  return {
    eligible,
    score,
    trend,
    entry,
    risk,
    penalty,

    cmo: momentum,
    volumeRatio,
    ema20Distance,
    breakoutDistance,
    ext5,
    ext10,
    atrPct,
    bodyRatio,
    supportDistance,

    structure: struct,
    fibPosition: fib,

    good,
    warnings
  };
}

// ============================================================
// JOURNAL
// ============================================================

function logJournal(row) {
  journal.push({
    time: Date.now(),
    ...row
  });

  if (journal.length > C.journalLimit) {
    journal = journal.slice(-C.journalLimit);
  }

  scheduleSave();
}

// ============================================================
// OPPORTUNITY POOL
// ============================================================

function addCandidate(symbol, analysis, closeTime) {
  const price = tickers.get(symbol)?.price;

  if (!price) return;

  const candidate = {
    symbol,
    createdAt: Date.now(),
    expiresAt: Date.now() + C.candidateExpiryMs,
    closeTime,
    signalPrice: price,

    score: analysis.score,
    trend: analysis.trend,
    entry: analysis.entry,
    risk: analysis.risk,
    penalty: analysis.penalty,

    cmo: analysis.cmo,
    volumeRatio: analysis.volumeRatio,
    ema20Distance: analysis.ema20Distance,
    breakoutDistance: analysis.breakoutDistance,
    ext5: analysis.ext5,
    ext10: analysis.ext10,
    atrPct: analysis.atrPct,
    supportDistance: analysis.supportDistance,

    structure: analysis.structure,
    fibPosition: analysis.fibPosition,

    good: analysis.good,
    warnings: analysis.warnings
  };

  pool.set(symbol, candidate);

  logJournal({
    type: 'CANDIDATE',
    decision: 'POOL',
    ...candidate
  });

  scheduleRank();
}

function validateCandidate(candidate) {
  const reasons = [];

  if (Date.now() > candidate.expiresAt) {
    reasons.push('EXPIRED');
  }

  if (positions[candidate.symbol]) {
    reasons.push('ALREADY_OPEN');
  }

  if (symbolCooling(candidate.symbol)) {
    reasons.push('SYMBOL_COOLDOWN');
  }

  const price =
    tickers.get(candidate.symbol)?.price;

  if (!price) {
    reasons.push('NO_PRICE');

    return {
      valid: false,
      price: 0,
      drift: 0,
      reasons
    };
  }

  const drift =
    pct(
      price - candidate.signalPrice,
      candidate.signalPrice
    );

  if (Math.abs(drift) > C.maxPriceDriftPct) {
    reasons.push('PRICE_MOVED');
  }

  return {
    valid: reasons.length === 0,
    price,
    drift,
    reasons
  };
}

function scheduleRank() {
  if (rankTimer) return;

  rankTimer = setTimeout(() => {
    rankTimer = null;
    rankAndExecute();
  }, C.rankingDelayMs);
}

// ============================================================
// END PART 1
// PASTE PART 2 DIRECTLY BELOW
// ============================================================
// ============================================================
// PAPER BUY
// ============================================================

function buy(candidate, price) {
  if (
    manualPause ||
    dailyPause ||
    cooldownActive()
  ) {
    return 'PAUSED';
  }

  if (positions[candidate.symbol]) {
    return 'ALREADY_OPEN';
  }

  if (symbolCooling(candidate.symbol)) {
    return 'SYMBOL_COOLDOWN';
  }

  if (
    Object.keys(positions).length >=
    C.maxPositions
  ) {
    return 'MAX_POSITIONS';
  }

  let allocation =
    Math.min(
      equity() / C.maxPositions,
      cash
    );

  if (allocation < 10) {
    return 'NO_BALANCE';
  }

  const entryPrice =
    price * (1 + C.slippagePct);

  const buyFee =
    allocation * C.feePct;

  const qty =
    (allocation - buyFee) /
    entryPrice;

  cash -= allocation;
  stats.fees += buyFee;

  positions[candidate.symbol] = {
    symbol: candidate.symbol,
    entryPrice,
    qty,
    invested: allocation,

    stopLoss:
      entryPrice *
      (1 - C.stopLossPct),

    takeProfit:
      entryPrice *
      (1 + C.takeProfitPct),

    lastPrice: price,

    score: candidate.score,
    trend: candidate.trend,
    entry: candidate.entry,
    risk: candidate.risk,
    penalty: candidate.penalty,

    cmo: candidate.cmo,
    volumeRatio: candidate.volumeRatio,
    ema20Distance: candidate.ema20Distance,
    breakoutDistance: candidate.breakoutDistance,
    ext5: candidate.ext5,
    ext10: candidate.ext10,
    atrPct: candidate.atrPct,
    supportDistance: candidate.supportDistance,

    structure: candidate.structure,
    fibPosition: candidate.fibPosition,

    good: candidate.good,
    warnings: candidate.warnings,

    signalPrice: candidate.signalPrice,
    candidateCreatedAt: candidate.createdAt,
    session: sessionUTC(),

    entryTime: Date.now()
  };

  entriesSinceCooldown++;

  logJournal({
    type: 'ENTRY',
    decision: 'PAPER_BOUGHT',
    symbol: candidate.symbol,
    score: candidate.score,
    entryPrice,
    signalPrice: candidate.signalPrice,
    volumeRatio: candidate.volumeRatio,
    cmo: candidate.cmo,
    ema20Distance: candidate.ema20Distance,
    breakoutDistance: candidate.breakoutDistance
  });

  tg(
    `🟢 <b>LOMY V4.2 BUY</b>\n\n` +
    `<b>${candidate.symbol}</b>\n` +
    `Score: ${candidate.score}/100\n` +
    `Trend: ${candidate.trend}\n` +
    `Entry: ${candidate.entry}\n` +
    `Risk: ${candidate.risk}\n` +
    `Penalty: -${candidate.penalty}\n\n` +
    `Amount: $${allocation.toFixed(2)}\n` +
    `Entry: ${entryPrice.toFixed(8)}\n` +
    `SL: ${positions[candidate.symbol].stopLoss.toFixed(8)}\n` +
    `TP: ${positions[candidate.symbol].takeProfit.toFixed(8)}`
  );

  scheduleSave();

  if (
    entriesSinceCooldown >=
    C.entriesBeforeCooldown
  ) {
    startCooldown(
      C.normalCooldownMs,
      'BATCH_COMPLETE'
    );
  }

  return 'PAPER_BOUGHT';
}

// ============================================================
// RANKING
// ============================================================

function rankAndExecute() {
  if (
    manualPause ||
    dailyPause ||
    cooldownActive()
  ) {
    return;
  }

  const freeSlots =
    C.maxPositions -
    Object.keys(positions).length;

  if (freeSlots <= 0) return;

  const valid = [];

  for (const [symbol, candidate] of pool) {
    const check =
      validateCandidate(candidate);

    if (!check.valid) {
      logJournal({
        type: 'REJECT_AT_EXECUTION',
        symbol,
        score: candidate.score,
        reasons: check.reasons
      });

      pool.delete(symbol);
      continue;
    }

    valid.push({
      candidate,
      price: check.price,
      drift: check.drift
    });
  }

  valid.sort((a, b) => {
    if (
      b.candidate.score !==
      a.candidate.score
    ) {
      return (
        b.candidate.score -
        a.candidate.score
      );
    }

    if (
      a.candidate.penalty !==
      b.candidate.penalty
    ) {
      return (
        a.candidate.penalty -
        b.candidate.penalty
      );
    }

    return (
      a.candidate.ema20Distance -
      b.candidate.ema20Distance
    );
  });

  const maxNew =
    Math.min(
      C.maxEntriesPerCycle,
      freeSlots
    );

  let opened = 0;

  for (const row of valid) {
    if (opened >= maxNew) break;
    if (cooldownActive()) break;

    const result =
      buy(
        row.candidate,
        row.price
      );

    if (result === 'PAPER_BOUGHT') {
      opened++;
      pool.delete(
        row.candidate.symbol
      );
    }
  }
}

// ============================================================
// CLOSE POSITION
// ============================================================

function closePosition(
  symbol,
  marketPrice,
  reason
) {
  const p = positions[symbol];

  if (!p) return null;

  const exitPrice =
    marketPrice *
    (1 - C.slippagePct);

  const gross =
    p.qty * exitPrice;

  const sellFee =
    gross * C.feePct;

  const net =
    gross - sellFee;

  const profit =
    net - p.invested;

  cash += net;

  stats.totalTrades++;
  stats.fees += sellFee;
  stats.netProfit += profit;

  if (profit > 0) {
    stats.wins++;
    stats.grossProfit += profit;
    lossStreak = 0;
  } else {
    stats.losses++;
    stats.grossLoss += Math.abs(profit);

    if (reason === 'STOP_LOSS') {
      lossStreak++;
      lastStop[symbol] = Date.now();
    } else {
      lossStreak = 0;
    }
  }

  dailyPnL += profit;

  const holdingMs =
    Date.now() - p.entryTime;

  const record = {
    symbol,
    score: p.score,

    entryPrice: p.entryPrice,
    exitPrice,

    invested: p.invested,

    profit,
    profitPct:
      pct(
        profit,
        p.invested
      ),

    reason,

    cmo: p.cmo,
    volumeRatio: p.volumeRatio,
    ema20Distance: p.ema20Distance,
    breakoutDistance: p.breakoutDistance,
    ext5: p.ext5,
    ext10: p.ext10,
    atrPct: p.atrPct,
    supportDistance: p.supportDistance,

    trend: p.trend,
    entry: p.entry,
    risk: p.risk,
    penalty: p.penalty,

    good: p.good,
    warnings: p.warnings,

    buyFee:
      p.invested * C.feePct,

    sellFee,

    holdingMinutes:
      holdingMs / 60000,

    entryTime:
      p.entryTime,

    exitTime:
      Date.now()
  };

  history.push(record);

  if (history.length > C.historyLimit) {
    history =
      history.slice(-C.historyLimit);
  }

  logJournal({
    type: 'TRADE_CLOSE',
    ...record
  });

  delete positions[symbol];

  updateDrawdown();
  checkDailyLoss();

  tg(
    `${profit >= 0 ? '✅' : '❌'} <b>LOMY V4.2 CLOSE</b>\n\n` +
    `<b>${symbol}</b>\n` +
    `Reason: ${reason}\n` +
    `PnL: $${profit.toFixed(2)}\n` +
    `Holding: ${(holdingMs / 60000).toFixed(1)} min\n` +
    `Loss Streak: ${lossStreak}\n` +
    `Cash: $${cash.toFixed(2)}`
  );

  if (
    lossStreak >=
    C.lossStreakLimit
  ) {
    startCooldown(
      C.lossCooldownMs,
      'LOSS_STREAK'
    );

    lossStreak = 0;
  }

  scheduleSave();
  scheduleRank();

  return profit;
}

// ============================================================
// LIVE POSITION MONITOR
// ============================================================

function managePosition(
  symbol,
  price
) {
  const p = positions[symbol];

  if (!p) return;

  p.lastPrice = price;

  if (price <= p.stopLoss) {
    closePosition(
      symbol,
      price,
      'STOP_LOSS'
    );

    return;
  }

  if (price >= p.takeProfit) {
    closePosition(
      symbol,
      price,
      'TAKE_PROFIT'
    );
  }
}

// ============================================================
// ANALYZE CLOSED CANDLE
// ============================================================

function analyzeClosedCandle(
  symbol,
  closeTime
) {
  const arr = candles[symbol];

  if (
    !Array.isArray(arr) ||
    arr.length < C.warmup
  ) {
    return;
  }

  if (
    lastAnalyzed[symbol] ===
    closeTime
  ) {
    return;
  }

  lastAnalyzed[symbol] =
    closeTime;

  const analysis =
    analyzeSetup(arr);

  if (!analysis) return;

  const blockers = [];

  if (manualPause) {
    blockers.push('MANUAL_PAUSE');
  }

  if (dailyPause) {
    blockers.push('DAILY_PAUSE');
  }

  if (cooldownActive()) {
    blockers.push('SMART_COOLDOWN');
  }

  if (positions[symbol]) {
    blockers.push('ALREADY_OPEN');
  }

  if (symbolCooling(symbol)) {
    blockers.push('SYMBOL_COOLDOWN');
  }

  let decision = 'REJECTED';

  if (
    analysis.eligible &&
    !blockers.length
  ) {
    addCandidate(
      symbol,
      analysis,
      closeTime
    );

    decision = 'POOL';
  } else {
    logJournal({
      type: 'ANALYSIS',
      symbol,
      decision: 'REJECTED',
      blockers,

      score: analysis.score,
      trend: analysis.trend,
      entry: analysis.entry,
      risk: analysis.risk,
      penalty: analysis.penalty,

      cmo: analysis.cmo,
      volumeRatio: analysis.volumeRatio,
      ema20Distance: analysis.ema20Distance,
      breakoutDistance: analysis.breakoutDistance,
      ext5: analysis.ext5,
      ext10: analysis.ext10,

      warnings: analysis.warnings
    });
  }

  latest.push({
    symbol,
    decision,

    score: analysis.score,
    trend: analysis.trend,
    entry: analysis.entry,
    risk: analysis.risk,
    penalty: analysis.penalty,

    cmo:
      analysis.cmo.toFixed(2),

    volume:
      analysis.volumeRatio.toFixed(2),

    ema:
      analysis.ema20Distance.toFixed(2),

    breakout:
      analysis.breakoutDistance.toFixed(2),

    ext5:
      analysis.ext5.toFixed(2),

    warnings:
      analysis.warnings.join(', ')
  });

  latest =
    latest
      .sort(
        (a, b) =>
          b.score - a.score
      )
      .slice(0, 50);

  scheduleSave();
}

// ============================================================
// MINI TICKER WEBSOCKET
// ============================================================

function connectMini() {
  if (shuttingDown) return;

  if (
    miniWs &&
    (
      miniWs.readyState ===
        WebSocket.OPEN ||
      miniWs.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  console.log(
    'Connecting MINI ticker...'
  );

  miniWs =
    new WebSocket(
      `${WS_BASE}/ws/!miniTicker@arr`
    );

  miniWs.on('open', () => {
    miniConnected = true;
    lastMiniMessage = Date.now();

    console.log(
      'MINI ticker connected.'
    );

    tg(
      '🟢 <b>LOMY V4.2 Market Stream Connected</b>'
    );
  });

  miniWs.on('message', raw => {
    lastMiniMessage = Date.now();

    let data;

    try {
      data =
        JSON.parse(
          raw.toString()
        );
    } catch {
      return;
    }

    if (!Array.isArray(data)) return;

    for (const item of data) {
      const symbol = item.s;

      if (
        !symbol ||
        !symbol.endsWith('USDT') ||
        ignored(symbol)
      ) {
        continue;
      }

      const price = n(item.c);
      const quoteVolume = n(item.q);

      if (price <= 0) continue;

      tickers.set(
        symbol,
        {
          price,
          quoteVolume,
          updatedAt: Date.now()
        }
      );

      if (positions[symbol]) {
        managePosition(
          symbol,
          price
        );
      }
    }

    if (
      !universeReady &&
      tickers.size > 100
    ) {
      universeReady = true;

      setTimeout(
        rebalanceUniverse,
        3000
      );
    }
  });

  miniWs.on('close', () => {
    miniConnected = false;

    console.log(
      'MINI disconnected.'
    );

    setTimeout(
      connectMini,
      5000
    );
  });

  miniWs.on('error', e => {
    console.error(
      'MINI:',
      e.message
    );
  });
}

// ============================================================
// KLINE WEBSOCKET
// ============================================================

const controlQueue = [];
let controlBusy = false;

function sendControl(msg) {
  controlQueue.push(msg);
}

setInterval(async () => {
  if (
    controlBusy ||
    !controlQueue.length ||
    !klineWs ||
    klineWs.readyState !==
      WebSocket.OPEN
  ) {
    return;
  }

  controlBusy = true;

  const msg =
    controlQueue.shift();

  try {
    klineWs.send(
      JSON.stringify(msg)
    );
  } catch (e) {
    console.error(
      'WS CONTROL:',
      e.message
    );
  }

  await sleep(1000);

  controlBusy = false;
}, 250);

function connectKline() {
  if (shuttingDown) return;

  if (
    klineWs &&
    (
      klineWs.readyState ===
        WebSocket.OPEN ||
      klineWs.readyState ===
        WebSocket.CONNECTING
    )
  ) {
    return;
  }

  console.log(
    'Connecting KLINE...'
  );

  klineWs =
    new WebSocket(
      `${WS_BASE}/ws`
    );

  klineWs.on('open', () => {
    klineConnected = true;
    lastKlineMessage = Date.now();

    console.log(
      'KLINE connected.'
    );

    if (subscribed.size) {
      sendControl({
        method: 'SUBSCRIBE',

        params:
          Array.from(subscribed)
            .map(
              s =>
                `${s.toLowerCase()}@kline_${C.interval}`
            ),

        id: Date.now()
      });
    }
  });

  klineWs.on('message', raw => {
    lastKlineMessage = Date.now();

    let event;

    try {
      event =
        JSON.parse(
          raw.toString()
        );
    } catch {
      return;
    }

    if (
      Object.prototype
        .hasOwnProperty
        .call(
          event,
          'result'
        )
    ) {
      return;
    }

    if (
      event.e !== 'kline' ||
      !event.k ||
      event.k.x !== true
    ) {
      return;
    }

    const symbol = event.s;
    const k = event.k;

    if (!symbol) return;

    const candle = {
      open: n(k.o),
      high: n(k.h),
      low: n(k.l),
      close: n(k.c),
      volume: n(k.v),
      closeTime: k.T
    };

    candles[symbol] =
      candles[symbol] || [];

    const arr =
      candles[symbol];

    const index =
      arr.findIndex(
        x =>
          x.closeTime ===
          candle.closeTime
      );

    if (index >= 0) {
      arr[index] = candle;
    } else {
      arr.push(candle);
    }

    candles[symbol] =
      arr
        .sort(
          (a, b) =>
            a.closeTime -
            b.closeTime
        )
        .slice(
          -C.maxCandles
        );

    analyzeClosedCandle(
      symbol,
      candle.closeTime
    );
  });

  klineWs.on('close', () => {
    klineConnected = false;

    console.log(
      'KLINE disconnected.'
    );

    setTimeout(
      connectKline,
      5000
    );
  });

  klineWs.on('error', e => {
    console.error(
      'KLINE:',
      e.message
    );
  });
}

// ============================================================
// UNIVERSE
// ============================================================

function topSymbols() {
  return Array.from(
    tickers.entries()
  )
    .filter(
      ([symbol, t]) =>
        symbol.endsWith('USDT') &&
        !ignored(symbol) &&
        t.quoteVolume >=
          C.minQuoteVolume
    )
    .sort(
      (a, b) =>
        b[1].quoteVolume -
        a[1].quoteVolume
    )
    .slice(
      0,
      C.universeSize
    )
    .map(
      row =>
        row[0]
    );
}

function rebalanceUniverse() {
  const wanted =
    new Set(
      topSymbols()
    );

  if (!wanted.size) return;

  const add = [];
  const remove = [];

  for (const symbol of wanted) {
    if (!subscribed.has(symbol)) {
      add.push(symbol);
    }
  }

  for (const symbol of subscribed) {
    if (
      !wanted.has(symbol) &&
      !positions[symbol]
    ) {
      remove.push(symbol);
    } else if (positions[symbol]) {
      wanted.add(symbol);
    }
  }

  if (remove.length) {
    sendControl({
      method: 'UNSUBSCRIBE',

      params:
        remove.map(
          s =>
            `${s.toLowerCase()}@kline_${C.interval}`
        ),

      id: Date.now()
    });
  }

  if (add.length) {
    sendControl({
      method: 'SUBSCRIBE',

      params:
        add.map(
          s =>
            `${s.toLowerCase()}@kline_${C.interval}`
        ),

      id: Date.now() + 1
    });
  }

  subscribed = wanted;

  console.log(
    `Universe ${subscribed.size} | +${add.length} | -${remove.length}`
  );
}

setInterval(
  rebalanceUniverse,
  C.universeRefreshMs
);

// ============================================================
// WATCHDOG
// ============================================================

setInterval(() => {
  checkNewDay();
  cooldownActive();
  updateDrawdown();

  const now = Date.now();

  if (
    miniConnected &&
    now - lastMiniMessage >
      90000
  ) {
    try {
      miniWs.terminate();
    } catch {}
  }

  if (
    klineConnected &&
    now - lastKlineMessage >
      10 * 60 * 1000
  ) {
    try {
      klineWs.terminate();
    } catch {}
  }

  if (!miniConnected) {
    connectMini();
  }

  if (!klineConnected) {
    connectKline();
  }
}, 30000);

// ============================================================
// API CONTROLS
// ============================================================

app.post('/api/pause', (req, res) => {
  manualPause = true;
  pool.clear();
  scheduleSave();

  tg(
    '⏸ <b>NEW ENTRIES PAUSED</b>'
  );

  res.json({
    success: true,
    msg: 'New entries paused.'
  });
});

app.post('/api/resume', (req, res) => {
  manualPause = false;
  pool.clear();
  scheduleSave();

  tg(
    '▶️ <b>NEW ENTRIES RESUMED</b>'
  );

  res.json({
    success: true,
    msg: 'Entries resumed with fresh signals only.'
  });
});

app.post(
  '/api/emergency-close',
  (req, res) => {
    let closed = 0;

    for (
      const symbol
      of Object.keys(positions)
    ) {
      const price =
        tickers.get(symbol)?.price ||
        positions[symbol]?.lastPrice;

      if (!price) continue;

      closePosition(
        symbol,
        price,
        'EMERGENCY_CLOSE'
      );

      closed++;
    }

    res.json({
      success: true,
      msg: `Closed ${closed} positions.`
    });
  }
);

// ============================================================
// EXPORT
// ============================================================

app.get('/api/export', (req, res) => {
  res.setHeader(
    'Content-Type',
    'application/json'
  );

  res.setHeader(
    'Content-Disposition',
    `attachment; filename="lomy-v42-${Date.now()}.json"`
  );

  res.send(
    JSON.stringify(
      {
        version: C.version,
        exportedAt:
          new Date().toISOString(),

        config: C,

        cash,
        equity: equity(),

        positions,
        history,
        journal,

        stats,

        entriesSinceCooldown,
        lossStreak,
        cooldownUntil,
        cooldownReason
      },
      null,
      2
    )
  );
});

// ============================================================
// DATA API
// ============================================================

app.get('/api/data', (req, res) => {
  const trades =
    stats.wins +
    stats.losses;

  const winRate =
    trades
      ? (
          stats.wins /
          trades
        ) *
        100
      : 0;

  const pf =
    stats.grossLoss
      ? stats.grossProfit /
        stats.grossLoss
      : stats.grossProfit
        ? 999
        : 0;

  let ready = 0;

  for (const symbol of subscribed) {
    if (
      candles[symbol]?.length >=
      C.warmup
    ) {
      ready++;
    }
  }

  res.json({
    version: C.version,

    miniConnected,
    klineConnected,

    symbols:
      subscribed.size,

    ready,

    cash:
      cash.toFixed(2),

    equity:
      equity().toFixed(2),

    openPositions:
      Object.keys(positions).length,

    poolSize:
      pool.size,

    journal:
      journal.length,

    dailyPnL:
      dailyPnL.toFixed(2),

    manualPause,
    dailyPause,

    cooldownActive:
      cooldownActive(),

    cooldownReason,

    cooldownMinutes:
      Math.ceil(
        Math.max(
          0,
          cooldownUntil -
          Date.now()
        ) /
        60000
      ),

    entriesSinceCooldown,

    lossStreak,

    stats: {
      ...stats,

      winRate:
        Number(
          winRate.toFixed(2)
        ),

      profitFactor:
        Number(
          pf.toFixed(2)
        )
    },

    latest
  });
});

// ============================================================
// HEALTH
// ============================================================

app.get('/health', (req, res) => {
  res.json({
    status:
      miniConnected &&
      klineConnected
        ? 'OK'
        : 'DEGRADED',

    version: C.version,

    execution:
      'PAPER ONLY',

    restRequests:
      0,

    symbols:
      subscribed.size,

    positions:
      Object.keys(positions).length,

    equity:
      equity().toFixed(2)
  });
});

// ============================================================
// DASHBOARD
// ============================================================

app.get('/', (req, res) => {
  res.send(`
<!doctype html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>LOMY V4.2 Compact</title>

<style>
body{
background:#0b0e11;
color:#eaecef;
font-family:Arial;
text-align:center;
margin:0;
padding:16px
}
h1{color:#f3ba2f}
.badge{
background:#f3ba2f;
color:#000;
padding:10px;
border-radius:8px;
font-weight:bold
}
.grid{
display:grid;
grid-template-columns:repeat(auto-fit,minmax(145px,1fr));
gap:9px;
margin:18px auto;
max-width:1200px
}
.card{
background:#1e2329;
padding:15px;
border-radius:9px;
border:1px solid #2b3139
}
.label{
color:#848e9c;
font-size:11px
}
.value{
font-size:22px;
font-weight:bold;
margin-top:7px
}
.green{color:#0ecb81}
.red{color:#f6465d}
.yellow{color:#f3ba2f}
button{
border:0;
padding:12px 18px;
margin:6px;
border-radius:7px;
font-weight:bold;
cursor:pointer
}
.pause{background:#f3ba2f}
.resume{background:#0ecb81}
.close{background:#f6465d;color:white}
.export{background:#3772ff;color:white}
table{
width:100%;
margin-top:20px;
border-collapse:collapse;
background:#1e2329
}
th{
background:#2b3139;
color:#848e9c
}
td,th{
padding:8px;
font-size:11px;
border-bottom:1px solid #2b3139
}
</style>
</head>

<body>

<h1>🤖 LOMY V4.2 COMPACT</h1>

<div class="badge">
ENTRY PRECISION • SMART COOLDOWN • PAPER ONLY
</div>

<h3 id="status">Connecting...</h3>
<h3 id="cooldown"></h3>

<div class="grid">

<div class="card">
<div class="label">CASH</div>
<div class="value" id="cash">$0</div>
</div>

<div class="card">
<div class="label">EQUITY</div>
<div class="value" id="equity">$0</div>
</div>

<div class="card">
<div class="label">CLOSED</div>
<div class="value" id="trades">0</div>
</div>

<div class="card">
<div class="label">WIN RATE</div>
<div class="value" id="win">0%</div>
</div>

<div class="card">
<div class="label">NET PROFIT</div>
<div class="value" id="profit">$0</div>
</div>

<div class="card">
<div class="label">PROFIT FACTOR</div>
<div class="value" id="pf">0</div>
</div>

<div class="card">
<div class="label">OPEN</div>
<div class="value" id="open">0</div>
</div>

<div class="card">
<div class="label">POOL</div>
<div class="value" id="pool">0</div>
</div>

<div class="card">
<div class="label">WS SYMBOLS</div>
<div class="value" id="symbols">0</div>
</div>

<div class="card">
<div class="label">READY</div>
<div class="value" id="ready">0</div>
</div>

<div class="card">
<div class="label">JOURNAL</div>
<div class="value" id="journal">0</div>
</div>

<div class="card">
<div class="label">TODAY PNL</div>
<div class="value" id="daily">$0</div>
</div>

<div class="card">
<div class="label">BATCH</div>
<div class="value" id="batch">0/10</div>
</div>

<div class="card">
<div class="label">LOSS STREAK</div>
<div class="value" id="loss">0/3</div>
</div>

</div>

<button class="pause" onclick="pause()">⏸ PAUSE</button>
<button class="resume" onclick="resume()">▶ RESUME</button>
<button class="close" onclick="closeAll()">🚨 CLOSE ALL</button>
<button class="export" onclick="location='/api/export'">⬇ EXPORT JSON</button>

<div style="overflow-x:auto">

<table>
<thead>
<tr>
<th>Symbol</th>
<th>Score</th>
<th>Trend</th>
<th>Entry</th>
<th>Risk</th>
<th>Penalty</th>
<th>Status</th>
<th>CMO</th>
<th>Volume</th>
<th>EMA%</th>
<th>Breakout%</th>
<th>5C%</th>
<th>Warnings</th>
</tr>
</thead>

<tbody id="table">
<tr><td colspan="13">Collecting candles...</td></tr>
</tbody>
</table>

</div>

<script>

async function load(){
try{

const r=await fetch('/api/data');
const d=await r.json();

cash.innerText='$'+d.cash;
equity.innerText='$'+d.equity;
trades.innerText=d.stats.totalTrades;
win.innerText=d.stats.winRate+'%';
profit.innerText='$'+Number(d.stats.netProfit).toFixed(2);
pf.innerText=d.stats.profitFactor;
open.innerText=d.openPositions;
pool.innerText=d.poolSize;
symbols.innerText=d.symbols;
ready.innerText=d.ready;
journal.innerText=d.journal;
daily.innerText='$'+d.dailyPnL;
batch.innerText=d.entriesSinceCooldown+'/10';
loss.innerText=d.lossStreak+'/3';

status.innerText=
d.miniConnected&&d.klineConnected
?'🟢 MARKET WEBSOCKETS LIVE • REST=0'
:'🔴 CONNECTING...';

status.className=
d.miniConnected&&d.klineConnected
?'green':'red';

cooldown.innerText=
d.cooldownActive
?'🧠 COOLDOWN '+d.cooldownReason+' • '+d.cooldownMinutes+' MIN'
:'✅ SMART COOLDOWN READY';

cooldown.className=
d.cooldownActive?'yellow':'green';

table.innerHTML='';

if(!d.latest.length){
table.innerHTML=
'<tr><td colspan="13">Warm-up: '+d.ready+' / '+d.symbols+'</td></tr>';
return;
}

d.latest.forEach(x=>{
table.innerHTML+=
'<tr>'+
'<td><b>'+x.symbol+'</b></td>'+
'<td>'+x.score+'</td>'+
'<td>'+x.trend+'</td>'+
'<td>'+x.entry+'</td>'+
'<td>'+x.risk+'</td>'+
'<td>'+x.penalty+'</td>'+
'<td>'+x.decision+'</td>'+
'<td>'+x.cmo+'</td>'+
'<td>'+x.volume+'x</td>'+
'<td>'+x.ema+'%</td>'+
'<td>'+x.breakout+'%</td>'+
'<td>'+x.ext5+'%</td>'+
'<td>'+x.warnings+'</td>'+
'</tr>';
});

}catch(e){
console.error(e);
}
}

async function pause(){
await fetch('/api/pause',{method:'POST'});
load();
}

async function resume(){
await fetch('/api/resume',{method:'POST'});
load();
}

async function closeAll(){
if(!confirm('Close all PAPER positions?'))return;
await fetch('/api/emergency-close',{method:'POST'});
load();
}

setInterval(load,3000);
load();

</script>
</body>
</html>
  `);
});

// ============================================================
// SHUTDOWN
// ============================================================

function shutdown(signal) {
  if (shuttingDown) return;

  shuttingDown = true;

  console.log(
    signal,
    'saving state'
  );

  saveState();

  try {
    miniWs?.close();
  } catch {}

  try {
    klineWs?.close();
  } catch {}

  setTimeout(
    () => process.exit(0),
    1000
  );
}

process.on(
  'SIGTERM',
  () => shutdown('SIGTERM')
);

process.on(
  'SIGINT',
  () => shutdown('SIGINT')
);

process.on(
  'unhandledRejection',
  e =>
    console.error(
      'UNHANDLED:',
      e
    )
);

process.on(
  'uncaughtException',
  e =>
    console.error(
      'UNCAUGHT:',
      e
    )
);

// ============================================================
// START
// ============================================================

app.listen(PORT, () => {
  console.log('');
  console.log('==============================');
  console.log('LOMY V4.2 COMPACT');
  console.log('ENTRY PRECISION ENGINE');
  console.log('OPPORTUNITY POOL: ON');
  console.log('SMART COOLDOWN: ON');
  console.log('MARKET DATA: WEBSOCKET ONLY');
  console.log('REST SCANNER: OFF');
  console.log('EXECUTION: PAPER ONLY');
  console.log(`UNIVERSE: TOP ${C.universeSize}`);
  console.log(`MAX POSITIONS: ${C.maxPositions}`);
  console.log(`MAX NEW/CYCLE: ${C.maxEntriesPerCycle}`);
  console.log(`MIN SCORE: ${C.minScore}`);
  console.log('==============================');

  loadState();

  connectMini();
  connectKline();

  tg(
    `🚀 <b>LOMY V4.2 COMPACT STARTED</b>\n\n` +
    `Entry Precision: <b>ON</b>\n` +
    `Opportunity Pool: <b>ON</b>\n` +
    `Smart Cooldown: <b>ON</b>\n` +
    `REST Scanner: <b>OFF</b>\n` +
    `Execution: <b>PAPER ONLY</b>\n\n` +
    `Balance: $${equity().toFixed(2)}\n` +
    `Universe: Top ${C.universeSize}`
  );
});
