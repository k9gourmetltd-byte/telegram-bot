require('dotenv').config();
const TelegramBot = require('node-telegram-bot-api');
const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const axios = require('axios');
const fs = require('fs');
const crypto = require('crypto');

const token = process.env.TELEGRAM_BOT_TOKEN;
if (!token) throw new Error('Missing TELEGRAM_BOT_TOKEN');
const PORT = process.env.PORT || 3000;
const BTC_WALLET = 'bc1q772uueqj2zev3vrc4hmvm8stnc3zksed0p4hmk';
const TWELVE_DATA_KEY = process.env.TWELVE_DATA_API_KEY || 'demo';
const ADMIN_IDS = (process.env.ADMIN_IDS || '').split(',').map(Number).filter(Boolean);
const ALLOWED_IDS = (process.env.ALLOWED_IDS || '').split(',').map(Number).filter(Boolean);
const SIGNAL_CHANNEL = process.env.SIGNAL_CHANNEL || 'none';
const PRIORITY_PAIRS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
const TOP_STOCKS = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'SPY', 'QQQ', 'AMD'];
const METALS = [
  { symbol: 'XAUUSD', name: 'GOLD', yahoo: 'GC=F', volatility: 0.008 },
  { symbol: 'XAGUSD', name: 'SILVER', yahoo: 'SI=F', volatility: 0.015 },
  { symbol: 'XPTUSD', name: 'PLATINUM', yahoo: 'PL=F', volatility: 0.012 },
  { symbol: 'XPDUSD', name: 'PALLADIUM', yahoo: 'PA=F', volatility: 0.015 },
  { symbol: 'XCUUSD', name: 'COPPER', yahoo: 'HG=F', volatility: 0.012 }
];
const METAL_ALIASES = { gold: 'XAUUSD', silver: 'XAGUSD', platinum: 'XPTUSD', palladium: 'XPDUSD', copper: 'XCUUSD' };
if (!fs.existsSync('./state')) fs.mkdirSync('./state');
if (!fs.existsSync('./stats')) fs.mkdirSync('./stats');

const rateLimiters = {
  binance: { tokens: 1200, lastRefill: Date.now(), maxTokens: 1200, refillRate: 20 },
  twelvedata: { tokens: 8, lastRefill: Date.now(), maxTokens: 8, refillRate: 8/60 },
  yahoo: { tokens: 30, lastRefill: Date.now(), maxTokens: 30, refillRate: 0.5 }
};
function checkRateLimit(service) { const limiter = rateLimiters[service]; if (!limiter) return true; const now = Date.now(); const elapsed = (now - limiter.lastRefill) / 1000; limiter.tokens = Math.min(limiter.maxTokens, limiter.tokens + elapsed * limiter.refillRate); limiter.lastRefill = now; if (limiter.tokens >= 1) { limiter.tokens--; return true; } return false; }
function isMarketOpen(type) { const now = new Date(); const utcHour = now.getUTCHours(); const utcDay = now.getUTCDay(); if (utcDay === 0 || utcDay === 6) return type === 'crypto'; if (type === 'metal') return utcHour >= 8 && utcHour <= 22; if (type === 'stock') return utcHour >= 13 && utcHour <= 20; return true; }

function getLeverageInfo(score, warnings, assetType, rr) {
  let maxLev = 500, recLev = 10, riskPct = 1, posSize = '1-2%';
  if (assetType === 'metal') { maxLev = 100; recLev = 5; riskPct = 0.5; posSize = '0.5-1%'; }
  if (assetType === 'stock') { maxLev = 20; recLev = 3; riskPct = 1; posSize = '2-5%'; }
  if (score >= 80 && warnings.length === 0) { recLev = assetType === 'crypto' ? 25 : assetType === 'metal' ? 10 : 5; riskPct = 2; posSize = '2-3%'; }
  else if (score >= 65 && warnings.length <= 1) { recLev = assetType === 'crypto' ? 15 : assetType === 'metal' ? 7 : 4; riskPct = 1.5; posSize = '1-2%'; }
  else if (score >= 45) { recLev = assetType === 'crypto' ? 8 : assetType === 'metal' ? 5 : 3; riskPct = 1; posSize = '1%'; }
  else { recLev = assetType === 'crypto' ? 3 : assetType === 'metal' ? 2 : 1; riskPct = 0.5; posSize = '0.5%'; }
  const leverageRatio = (recLev / maxLev * 100).toFixed(0);
  return { maxLev, recLev, riskPct, posSize, leverageRatio };
}

const bot = new TelegramBot(token, { polling: true });
const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });
app.use(cors()); app.use(express.json());

function isAuth(uid) { if (ADMIN_IDS.length === 0 && ALLOWED_IDS.length === 0) return 'admin'; if (ADMIN_IDS.includes(uid)) return 'admin'; if (ALLOWED_IDS.includes(uid)) return 'user'; return null; }
function userFile(uid) { return './state/u-' + uid + '.json'; }
function loadUser(uid) { try { if (fs.existsSync(userFile(uid))) return JSON.parse(fs.readFileSync(userFile(uid), 'utf8')); } catch (e) {} return { signals: [], positions: [], history: [], stats: { t: 0, w: 0, l: 0, p: 0, s: 0 } }; }
function saveUser(uid, d) { const tmp = userFile(uid) + '.tmp'; fs.writeFileSync(tmp, JSON.stringify(d)); fs.renameSync(tmp, userFile(uid)); }
function gsf() { return './stats/global.json'; }
function loadGS() { try { if (fs.existsSync(gsf())) return JSON.parse(fs.readFileSync(gsf(), 'utf8')); } catch (e) {} return { totalSignals: 0, totalWins: 0, totalLosses: 0, lastUpdated: null }; }
function saveGS(d) { fs.writeFileSync(gsf(), JSON.stringify(d, null, 2)); }
let GS = loadGS();
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36';

async function fetchWithRetry(url, opts, retries) { retries = retries || 3; const service = url.includes('binance') ? 'binance' : url.includes('twelvedata') ? 'twelvedata' : 'yahoo'; for (let i = 0; i < retries; i++) { if (!checkRateLimit(service)) { await new Promise(r => setTimeout(r, 2000)); continue; } try { return await axios.get(url, { ...opts, timeout: (opts?.timeout || 5000) * (i + 1) }); } catch (e) { if (i === retries - 1) throw e; await new Promise(r => setTimeout(r, 1000 * (i + 1))); } } }

async function getPrice(s) { try { const r = await fetchWithRetry('https://api4.binance.com/api/v3/ticker/price?symbol=' + s); return parseFloat(r.data.price); } catch (e) { return null; } }
async function get24hr(s) { try { const r = await fetchWithRetry('https://api4.binance.com/api/v3/ticker/24hr?symbol=' + s); return { high: parseFloat(r.data.highPrice), low: parseFloat(r.data.lowPrice), change: parseFloat(r.data.priceChangePercent), volume: parseFloat(r.data.quoteVolume) }; } catch (e) { return null; } }
async function getKlines(s, i, l) { try { const r = await fetchWithRetry('https://api4.binance.com/api/v3/klines?symbol=' + s + '&interval=' + i + '&limit=' + l); return r.data.map(k => ({ close: parseFloat(k[4]), high: parseFloat(k[2]), low: parseFloat(k[3]), volume: parseFloat(k[5]) })); } catch (e) { return []; } }
async function getDepth(s) { try { const r = await fetchWithRetry('https://api4.binance.com/api/v3/depth?symbol=' + s + '&limit=100'); const bids = r.data.bids.map(b => ({ p: parseFloat(b[0]), q: parseFloat(b[1]) })), asks = r.data.asks.map(a => ({ p: parseFloat(a[0]), q: parseFloat(a[1]) })); const bv = bids.reduce((x, b) => x + b.q * b.p, 0), av = asks.reduce((x, a) => x + a.q * a.p, 0); const bw = findWalls(bids), aw = findWalls(asks); return { imbalance: +(((bv - av) / (bv + av)) * 100).toFixed(1), spread: +((asks[0].p - bids[0].p).toFixed(2)), spreadPct: +(((asks[0].p - bids[0].p) / bids[0].p) * 100).toFixed(3), bestBid: bids[0].p, bestAsk: asks[0].p, bidWalls: bw.slice(0, 3).map(w => ({ price: w.price.toFixed(2), qty: w.totalQty.toFixed(2), orders: w.count })), askWalls: aw.slice(0, 3).map(w => ({ price: w.price.toFixed(2), qty: w.totalQty.toFixed(2), orders: w.count })) }; } catch (e) { return null; } }
function findWalls(o) { const w = []; let c = { price: o[0]?.p || 0, totalQty: 0, count: 0 }; for (let i = 0; i < o.length; i++) { if (i > 0 && Math.abs(o[i].p - o[i - 1].p) / o[i - 1].p > 0.0005) { if (c.count >= 3) w.push(c); c = { price: o[i].p, totalQty: 0, count: 0 }; } c.totalQty += o[i].q; c.count++; } if (c.count >= 3) w.push(c); return w.sort((a, b) => b.totalQty - a.totalQty); }
async function getFunding(s) { try { const r = await fetchWithRetry('https://fapi4.binance.com/fapi/v1/fundingRate?symbol=' + s + '&limit=1'); const rate = parseFloat(r.data[0]?.fundingRate || 0) * 100; return { rate: rate.toFixed(4), sentiment: rate > 0.05 ? 'BEARISH' : rate < -0.05 ? 'BULLISH' : 'NEUTRAL' }; } catch (e) { return { rate: '0', sentiment: 'NEUTRAL' }; } }
async function getLSRatio(s) { try { const r = await fetchWithRetry('https://fapi4.binance.com/fapi/v1/globalLongShortAccountRatio?symbol=' + s + '&period=5m&limit=1'); const ratio = parseFloat(r.data[0]?.longShortRatio || 1); return { long: ratio.toFixed(1), sentiment: ratio > 1.5 ? 'BEARISH (crowded longs)' : ratio < 0.7 ? 'BULLISH (crowded shorts)' : 'NEUTRAL' }; } catch (e) { return { long: '1.0', sentiment: 'NEUTRAL' }; } }
async function getOpenInterest(s) { try { const r = await fetchWithRetry('https://fapi4.binance.com/fapi/v1/openInterest?symbol=' + s); return (parseFloat(r.data.openInterest) / 1e9).toFixed(1) + 'B'; } catch (e) { return 'N/A'; } }

async function getAssetPrice(symbol, yahooSymbol) { if (!checkRateLimit('twelvedata') && !checkRateLimit('yahoo')) return null; try { const r = await fetchWithRetry('https://api.twelvedata.com/price?symbol=' + symbol + '&apikey=' + TWELVE_DATA_KEY, {}, 2); const price = parseFloat(r.data.price); if (price) return price; } catch(e) {} try { const r = await fetchWithRetry('https://query1.finance.yahoo.com/v8/finance/chart/' + (yahooSymbol || symbol), { headers: { 'User-Agent': UA } }, 2); return r.data.chart.result[0].meta.regularMarketPrice; } catch(e) { return null; } }
async function getAssetData(symbol, yahooSymbol) { try { const r = await fetchWithRetry('https://api.twelvedata.com/time_series?symbol=' + symbol + '&interval=5min&outputsize=50&apikey=' + TWELVE_DATA_KEY, {}, 2); const values = r.data.values; if (values && values.length > 10) { return { closes: values.map(v => parseFloat(v.close)).reverse(), highs: values.map(v => parseFloat(v.high)).reverse(), lows: values.map(v => parseFloat(v.low)).reverse() }; } } catch(e) {} try { const r = await fetchWithRetry('https://query1.finance.yahoo.com/v8/finance/chart/' + (yahooSymbol || symbol), { headers: { 'User-Agent': UA } }, 2); const q = r.data.chart.result[0].indicators.quote[0]; return { closes: (q.close || []).filter(c => c !== null), highs: (q.high || []).filter(h => h !== null), lows: (q.low || []).filter(l => l !== null) }; } catch(e) { return { closes: [], highs: [], lows: [] }; } }
async function getStockPrice(s) { return getAssetPrice(s, s); }
async function getStockData(s) { return getAssetData(s, s); }
async function getMetalPrice(s) { const m = METALS.find(x => x.symbol === s); return getAssetPrice(s, m ? m.yahoo : s); }
async function getMetalData(s) { const m = METALS.find(x => x.symbol === s); return getAssetData(s, m ? m.yahoo : s); }

async function fetchForex() { try { const r = await fetchWithRetry('https://nfs.faireconomy.media/ff_calendar_thisweek.json', {}, 2); const events = []; const today = new Date(); if (Array.isArray(r.data)) { r.data.forEach(e => { const d = new Date(e.date || ''); if (Math.abs(d - today) < 86400000) events.push({ title: e.title || 'Event', country: e.country || 'USD', time: e.time || '', impact: e.impact || 'Medium', forecast: e.forecast || '', previous: e.previous || '' }); }); } return events; } catch (e) { return []; } }
async function getPolymarket() { try { const r = await fetchWithRetry('https://gamma-api.polymarket.com/markets?limit=6&order=volume&ascending=false', {}, 2); if (Array.isArray(r.data)) return r.data.slice(0, 6).map(m => ({ title: m.title || '', volume: (parseFloat(m.volumeNum || 0) / 1e6).toFixed(1) + 'M', prices: [m.outcomePrices ? JSON.parse(m.outcomePrices)[0] : '0'].map(p => (parseFloat(p) * 100).toFixed(0) + '%') })); } catch (e) { return null; } }

function calcRSI(p, per) { per = per || 14; if (p.length < per + 1) return 50; let g = 0, l = 0; for (let i = p.length - per; i < p.length; i++) { let d = p[i] - p[i - 1]; if (d >= 0) g += d; else l -= d; } if (l === 0) return 100; return 100 - (100 / (1 + (g / per) / (l / per))); }
function calcMACD(prices) { if (prices.length < 26) return { macd: 0, signal: 0, histogram: 0, trend: 'NEUTRAL' }; const ema = (p, per) => { const k = 2 / (per + 1); let e = p[0]; for (let i = 1; i < p.length; i++) e = p[i] * k + e * (1 - k); return e; }; const e12 = ema(prices, 12), e26 = ema(prices, 26); const mv = e12 - e26, sv = ema([mv], 9); return { macd: mv.toFixed(2), signal: sv.toFixed(2), histogram: (mv - sv).toFixed(4), trend: mv > sv ? 'BULLISH' : 'BEARISH' }; }
function calcSR(prices) { const h = Math.max(...prices), l = Math.min(...prices), c = prices[prices.length - 1]; const pp = (h + l + c) / 3; return { resistance: h.toFixed(2), support: l.toFixed(2), pivot: pp.toFixed(2), r1: (2 * pp - l).toFixed(2), s1: (2 * pp - h).toFixed(2), r2: (pp + (h - l)).toFixed(2), s2: (pp - (h - l)).toFixed(2) }; }
function calcATR(h, l, c, per) { per = per || 14; if (c.length < per + 1) return [0]; const tr = []; for (let i = 0; i < c.length; i++) { if (i === 0) tr.push(h[i] - l[i]); else tr.push(Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1]))); } const atr = []; let sum = tr.slice(0, per).reduce((a, b) => a + b, 0); atr.push(sum / per); for (let i = per; i < tr.length; i++) atr.push((atr[atr.length - 1] * (per - 1) + tr[i]) / per); return atr; }
function calcSupertrend(h, l, c, per, mult) { per = per || 10; mult = mult || 3; if (h.length < per + 1) return { trend: 'NEUTRAL', signal: 'WAIT', value: '0' }; const atr = calcATR(h, l, c, per); const src = c.map((cl, i) => (h[i] + l[i]) / 2); let ub = [], lb = [], st = [], dir = 0; for (let i = 0; i < c.length; i++) { const mid = src[i]; const av = atr[i] || atr[atr.length - 1]; let u = mid + mult * av; let lo = mid - mult * av; if (i > 0) { if (c[i - 1] > ub[i - 1]) u = Math.max(u, ub[i - 1]); if (c[i - 1] < lb[i - 1]) lo = Math.min(lo, lb[i - 1]); } ub.push(u); lb.push(lo); if (i === 0) { st.push(u); dir = -1; } else { if (st[i - 1] === ub[i - 1]) { if (c[i] <= u) { st.push(u); dir = -1; } else { st.push(lo); dir = 1; } } else { if (c[i] >= lo) { st.push(lo); dir = 1; } else { st.push(u); dir = -1; } } } } return { trend: dir === 1 ? 'BULLISH' : 'BEARISH', signal: dir === 1 ? 'BUY' : 'SELL', value: st[st.length - 1].toFixed(2) }; }
function calcEMAVal(cl, per) { if (cl.length < per) return cl[cl.length - 1]; const k = 2 / (per + 1); let e = cl.slice(0, per).reduce((a, b) => a + b, 0) / per; for (let i = per; i < cl.length; i++) e = cl[i] * k + e * (1 - k); return e; }
function calcEMACross(cl) { const e9 = calcEMAVal(cl, 9); const e21 = calcEMAVal(cl, 21); const e50 = calcEMAVal(cl, 50); const e200 = calcEMAVal(cl, 200); const p9 = calcEMAVal(cl.slice(0, -1), 9); const p21 = calcEMAVal(cl.slice(0, -1), 21); return { ema9: e9.toFixed(2), ema21: e21.toFixed(2), ema50: e50.toFixed(2), ema200: e200.toFixed(2), signal: (p9 <= p21 && e9 > e21) ? 'STRONG BUY' : (p9 >= p21 && e9 < e21) ? 'STRONG SELL' : e9 > e21 ? 'BUY' : 'SELL', slowCross: e50 > e200 ? 'BULLISH' : 'BEARISH' }; }
function tfVote(tf) { if (!tf || tf.bias === '--' || tf.bias === 'N/A') return 'neutral'; if (tf.bias.includes('BULLISH')) return 'long'; if (tf.bias.includes('BEARISH')) return 'short'; return 'neutral'; }

function estimateTPTime(s, assetType) { const vol = assetType === 'metal' ? (METALS.find(m => m.symbol === s.symbol)?.volatility || 0.012) : assetType === 'stock' ? 0.015 : s.symbol?.includes('BTC') ? 0.008 : 0.020; const tp1Dist = Math.abs((parseFloat(s.tp1) / parseFloat(s.entry) - 1) * 100); const baseMin = assetType === 'metal' ? 18 : assetType === 'stock' ? 15 : s.symbol?.includes('BTC') ? 20 : 12; const tp1min = Math.round(baseMin * tp1Dist / vol); const tp2min = Math.round(baseMin * 2.5 * (Math.abs((parseFloat(s.tp2) / parseFloat(s.entry) - 1) * 100)) / vol); const tp3min = Math.round(baseMin * 5 * (Math.abs((parseFloat(s.tp3) / parseFloat(s.entry) - 1) * 100)) / vol); const fmt = m => m < 60 ? m + 'm' : Math.round(m/60) + 'h' + (m%60>0 ? ' ' + (m%60) + 'm' : ''); return { tp1: fmt(tp1min), tp2: fmt(tp2min), tp3: fmt(tp3min) }; }

async function genSignal(symbol, type, meta, forceConsensus) {
  if (!forceConsensus && !isMarketOpen(type)) return null;
  let price, closes, highs, lows, depth, funding, lsRatio, oi, stats;
  const minVotes = forceConsensus ? 2 : (type === 'crypto' ? 3 : 2);
  
  if (type === 'crypto') {
    const [p, st, k5m, k15m, k1h, k4h, d, f, lr, o] = await Promise.all([getPrice(symbol), get24hr(symbol), getKlines(symbol, '5m', 50), getKlines(symbol, '15m', 30), getKlines(symbol, '1h', 50), getKlines(symbol, '4h', 50), getDepth(symbol), getFunding(symbol), getLSRatio(symbol), getOpenInterest(symbol)]);
    if (!p || !d) return null;
    price = p; stats = st; depth = d; funding = f; lsRatio = lr; oi = o;
    closes = k1h.map(k => k.close); highs = k1h.map(k => k.high); lows = k1h.map(k => k.low);
    var tf5m = { label: '5M', bias: calcRSI(k5m.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k5m.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k5m.map(k => k.close)) > 55 ? '🟢' : calcRSI(k5m.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k5m.map(k => k.close)).toFixed(1), macd: calcMACD(k5m.map(k => k.close)).trend, st: calcSupertrend(k5m.map(k => k.high), k5m.map(k => k.low), k5m.map(k => k.close)).signal };
    var tf15m = { label: '15M', bias: calcRSI(k15m.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k15m.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k15m.map(k => k.close)) > 55 ? '🟢' : calcRSI(k15m.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k15m.map(k => k.close)).toFixed(1), macd: calcMACD(k15m.map(k => k.close)).trend, st: calcSupertrend(k15m.map(k => k.high), k15m.map(k => k.low), k15m.map(k => k.close)).signal };
    var tf1h = { label: '1H', bias: calcRSI(k1h.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k1h.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k1h.map(k => k.close)) > 55 ? '🟢' : calcRSI(k1h.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k1h.map(k => k.close)).toFixed(1), macd: calcMACD(k1h.map(k => k.close)).trend, st: calcSupertrend(k1h.map(k => k.high), k1h.map(k => k.low), k1h.map(k => k.close)).signal };
    var tf4h = { label: '4H', bias: calcRSI(k4h.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k4h.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k4h.map(k => k.close)) > 55 ? '🟢' : calcRSI(k4h.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k4h.map(k => k.close)).toFixed(1), macd: calcMACD(k4h.map(k => k.close)).trend, st: calcSupertrend(k4h.map(k => k.high), k4h.map(k => k.low), k4h.map(k => k.close)).signal };
  } else {
    const fetchPrice = type === 'metal' ? getMetalPrice : getStockPrice;
    const fetchData = type === 'metal' ? getMetalData : getStockData;
    const [p, data] = await Promise.all([fetchPrice(symbol), fetchData(symbol)]);
    if (!p || !data || !data.closes || data.closes.length < 15) return null;
    price = p; closes = data.closes; highs = data.highs; lows = data.lows;
    depth = { imbalance: 'N/A', spread: 'N/A', spreadPct: 'N/A', bestBid: 'N/A', bestAsk: 'N/A', bidWalls: [], askWalls: [] };
    funding = { rate: 'N/A', sentiment: 'N/A' }; lsRatio = { long: 'N/A', sentiment: 'N/A' }; oi = 'N/A'; stats = null;
    var tf5m = { label: '5M', bias: calcRSI(closes.slice(-50)) > 55 ? 'BULLISH' : calcRSI(closes.slice(-50)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(closes.slice(-50)) > 55 ? '🟢' : calcRSI(closes.slice(-50)) < 45 ? '🔴' : '⚪', rsi: calcRSI(closes.slice(-50)).toFixed(1), macd: calcMACD(closes.slice(-50)).trend, st: calcSupertrend(highs.slice(-50), lows.slice(-50), closes.slice(-50)).signal };
    var tf15m = { label: '15M', bias: calcRSI(closes.slice(-30)) > 55 ? 'BULLISH' : calcRSI(closes.slice(-30)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(closes.slice(-30)) > 55 ? '🟢' : calcRSI(closes.slice(-30)) < 45 ? '🔴' : '⚪', rsi: calcRSI(closes.slice(-30)).toFixed(1), macd: calcMACD(closes.slice(-30)).trend, st: calcSupertrend(highs.slice(-30), lows.slice(-30), closes.slice(-30)).signal };
    var tf1h = { label: '1H', bias: calcRSI(closes) > 55 ? 'BULLISH' : calcRSI(closes) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(closes) > 55 ? '🟢' : calcRSI(closes) < 45 ? '🔴' : '⚪', rsi: calcRSI(closes).toFixed(1), macd: calcMACD(closes).trend, st: calcSupertrend(highs, lows, closes).signal };
    var tf4h = { label: '4H', bias: 'N/A', emoji: '⚪', rsi: '--', macd: '--', st: '--' };
  }

  const tfs = [tf5m, tf15m, tf1h, tf4h].filter(t => t.bias !== 'N/A');
  const votes = tfs.map(tfVote);
  const lv = votes.filter(v => v === 'long').length, sv = votes.filter(v => v === 'short').length;
  let dir = null;
  if (lv >= minVotes) dir = 'LONG'; else if (sv >= minVotes) dir = 'SHORT';
  if (!dir && type === 'crypto' && !forceConsensus) {
    if (lv === 2 && sv <= 1 && depth.imbalance > 10) dir = 'LONG';
    else if (sv === 2 && lv <= 1 && depth.imbalance < -10) dir = 'SHORT';
  }
  if (!dir) return null;

  const rsi1h = calcRSI(closes), macd1h = calcMACD(closes), sr = calcSR(closes);
  const st = calcSupertrend(highs, lows, closes);
  const ema = calcEMACross(closes);
  let warnings = [], reasons = [];
  if (funding.sentiment === 'BEARISH' && dir === 'LONG') warnings.push('Funding opposes');
  if (funding.sentiment === 'BULLISH' && dir === 'SHORT') warnings.push('Funding opposes');
  if (macd1h.trend === 'BEARISH' && dir === 'LONG') warnings.push('MACD opposes');
  if (macd1h.trend === 'BULLISH' && dir === 'SHORT') warnings.push('MACD opposes');
  if (st.trend !== (dir === 'LONG' ? 'BULLISH' : 'BEARISH')) warnings.push('Supertrend opposes');
  if (lv >= minVotes) reasons.push(lv + '/' + tfs.length + ' TFs ' + (dir === 'LONG' ? 'bullish' : 'bearish'));
  if (sv >= minVotes) reasons.push(sv + '/' + tfs.length + ' TFs ' + (dir === 'LONG' ? 'bullish' : 'bearish'));
  if (type === 'crypto' && depth.imbalance > 10 && dir === 'LONG') reasons.push('Order book confirms (' + depth.imbalance + '%)');
  if (type === 'crypto' && depth.imbalance < -10 && dir === 'SHORT') reasons.push('Order book confirms (' + Math.abs(depth.imbalance) + '%)');
  if (st.trend === (dir === 'LONG' ? 'BULLISH' : 'BEARISH')) reasons.push('Supertrend aligned');
  if (ema.signal.includes(dir === 'LONG' ? 'BUY' : 'SELL')) reasons.push('EMA aligned');
  let sc = lv >= minVotes ? 45 + lv * 12 : 45 + sv * 12;
  if (type === 'crypto' && depth.imbalance > 15 && dir === 'LONG') sc += 10;
  if (type === 'crypto' && depth.imbalance < -15 && dir === 'SHORT') sc += 10;
  if (st.trend === (dir === 'LONG' ? 'BULLISH' : 'BEARISH')) sc += 10;
  if (ema.signal.includes(dir === 'LONG' ? 'BUY' : 'SELL')) sc += 5;
  sc -= warnings.length * 5;
  if (forceConsensus) sc = Math.min(sc, 55);
  sc = Math.min(95, Math.max(20, sc));
  
  const vol = type === 'metal' ? (meta?.volatility || 0.012) : type === 'stock' ? 0.015 : symbol.includes('BTC') ? 0.008 : 0.020;
  let e, tp1, tp2, tp3, sl;
  if (dir === 'LONG') { e = price * 0.999; tp1 = +(e * (1 + vol)).toFixed(2); tp2 = +(e * (1 + vol * 2.0)).toFixed(2); tp3 = +(e * (1 + vol * 3.5)).toFixed(2); sl = +(e * (1 - vol * 0.8)).toFixed(2); }
  else { e = price * 1.001; tp1 = +(e * (1 - vol)).toFixed(2); tp2 = +(e * (1 - vol * 2.0)).toFixed(2); tp3 = +(e * (1 - vol * 3.5)).toFixed(2); sl = +(e * (1 + vol * 0.8)).toFixed(2); }
  
  const name = type === 'metal' ? (meta?.name || symbol) : symbol;
  const rr1 = (Math.abs(tp1 - e) / Math.abs(e - sl)).toFixed(1);
  const lev = getLeverageInfo(sc, warnings, type, rr1);
  return { id: crypto.randomBytes(4).toString('hex'), type, symbol, name, direction: dir, price: price.toFixed(2), entry: e.toFixed(2), tp1, tp2, tp3, sl, trailingSL: (vol * 0.6 * 100).toFixed(1) + '%', score: sc, rsi1h: rsi1h.toFixed(1), rsi4h: type === 'crypto' ? calcRSI(closes.slice(-50)).toFixed(1) : 'N/A', rsi15m: type === 'crypto' ? calcRSI(closes.slice(-30)).toFixed(1) : 'N/A', macd: macd1h, sr, depth, rr1, rr2: (Math.abs(tp2 - e) / Math.abs(e - sl)).toFixed(1), rr3: (Math.abs(tp3 - e) / Math.abs(e - sl)).toFixed(1), funding, lsRatio, oi, vs: '1.5', reasons, warnings, st, ema, tf5m, tf15m, tf1h, tf4h, stats, longVotes: lv, shortVotes: sv, lev, forceConsensus: !!forceConsensus, timestamp: new Date().toISOString() };
}

function fullSignal(s) {
  const emoji = s.direction === 'LONG' ? '🟢' : '🔴', stars = s.score >= 65 ? '★★★' : s.score >= 45 ? '★★' : '★';
  const label = s.type === 'stock' ? '📈 STOCK' : s.type === 'metal' ? '🥇 METAL' : '₿ CRYPTO';
  const rsiEmoji = parseFloat(s.rsi1h) > 70 ? '🔥' : parseFloat(s.rsi1h) < 30 ? '❄️' : '➖';
  const db = s.depth && s.depth.imbalance !== 'N/A' ? (parseFloat(s.depth.imbalance) > 10 ? 'BULLISH' : parseFloat(s.depth.imbalance) < -10 ? 'BEARISH' : 'NEUTRAL') : 'N/A';
  const et = estimateTPTime(s, s.type);
  const consensusLabel = s.forceConsensus ? '⚠️ RELAXED CONSENSUS' : 'TIMEFRAME CONSENSUS';
  let tfSection = '⏱ *' + consensusLabel + '* (' + (s.longVotes || 0) + 'L/' + (s.shortVotes || 0) + 'S)\n';
  [s.tf5m, s.tf15m, s.tf1h, s.tf4h].forEach(tf => { if (tf && tf.bias && tf.bias !== '--' && tf.bias !== 'N/A') tfSection += tf.emoji + ' *' + tf.label + ':* ' + tf.bias + ' | RSI:' + tf.rsi + ' | MACD:' + tf.macd + ' | ST:' + tf.st + '\n'; });
  let wallText = '';
  if (s.depth && s.depth.bidWalls && s.depth.bidWalls.length > 0) { wallText += '\n🧱 *Bid Walls:*'; s.depth.bidWalls.forEach(w => { wallText += '\n  $' + w.price + ' — ' + w.qty + ' units (' + w.orders + ' orders)'; }); }
  if (s.depth && s.depth.askWalls && s.depth.askWalls.length > 0) { wallText += '\n🧱 *Ask Walls:*'; s.depth.askWalls.forEach(w => { wallText += '\n  $' + w.price + ' — ' + w.qty + ' units (' + w.orders + ' orders)'; }); }
  let warnText = s.warnings && s.warnings.length > 0 ? '\n⚠️ *Warnings:* ' + s.warnings.join(' • ') : '';
  let stAlign = s.st.trend === (s.direction === 'LONG' ? 'BULLISH' : 'BEARISH') ? ' ✅' : ' ⚠️';
  let emaAlign = s.ema.signal.includes(s.direction === 'LONG' ? 'BUY' : 'SELL') ? ' ✅' : ' ⚠️';
  let crossAlign = s.ema.slowCross === (s.direction === 'LONG' ? 'BULLISH' : 'BEARISH') ? ' ✅' : ' ⚠️';
  let extra = '';
  if (s.type === 'crypto') extra = '\n— *MARKET DATA* —\n💰 *Funding:* ' + s.funding.rate + '% (' + s.funding.sentiment + ')\n👥 *L/S Ratio:* ' + s.lsRatio.long + ':1 — ' + s.lsRatio.sentiment + '\n📊 *OI:* ' + (s.oi === '0.0B' ? 'Fetching...' : s.oi) + ' | *Vol:* ' + s.vs + 'x avg\n' + (s.stats ? '📈 *24h:* ' + s.stats.change.toFixed(1) + '% | H:$' + s.stats.high.toFixed(2) + ' L:$' + s.stats.low.toFixed(2) : '');
  if (s.type === 'metal') extra = '\n🕐 *Best spreads 8AM-5PM ET*';
  
  let leverageSection = '\n— *LEVERAGE & POSITION* —\n';
  leverageSection += '📊 *Max Leverage:* ' + s.lev.maxLev + 'x | *Recommended:* ' + s.lev.recLev + 'x (' + s.lev.leverageRatio + '% of max)\n';
  leverageSection += '💰 *Suggested Size:* ' + s.lev.posSize + ' of portfolio\n';
  leverageSection += '⚠️ *Risk per trade:* ' + s.lev.riskPct + '%\n';
  leverageSection += '📈 *R:R Ratio:* 1:' + s.rr1 + '\n';
  if (s.forceConsensus) leverageSection += '\n⚠️ *RELAXED CONSENSUS — TRADE AT YOUR OWN RISK*\n⚠️ Lower conviction signal — strict 3/4 agreement not met.';
  
  const footer = '\n\n🔐 *K9 SignalBot • Verified Consensus Engine*\n📡 @k9signalalerts • 💎 @K9sigbot';
  
  return label + ' • ' + emoji + ' *' + s.direction + ' ' + (s.name || s.symbol) + '*\n\n' +
    '💰 *Current:* $' + s.price + '  →  🎯 *Entry:* $' + s.entry + '\n\n' +
    '📈 *TP1:* $' + s.tp1 + ' _(+' + ((parseFloat(s.tp1) / parseFloat(s.entry) - 1) * 100).toFixed(1) + '%, R:R 1:' + s.rr1 + ')_ ⏱ ~' + et.tp1 + '\n' +
    '   *TP2:* $' + s.tp2 + ' _(R:R 1:' + s.rr2 + ')_ ⏱ ~' + et.tp2 + '\n' +
    '   *TP3:* $' + s.tp3 + ' _(R:R 1:' + s.rr3 + ')_ ⏱ ~' + et.tp3 + '\n\n' +
    '🛑 *Stop:* $' + s.sl + ' _(' + ((parseFloat(s.sl) / parseFloat(s.entry) - 1) * 100).toFixed(1) + '%)_\n📊 *Trailing SL:* ' + (s.trailingSL || 'N/A') + '\n\n' + tfSection + '\n' +
    leverageSection + '\n' +
    '— *TECHNICALS (1H)* —\n' +
    '📊 *RSI:* 1H ' + s.rsi1h + ' ' + rsiEmoji + ' | 4H ' + (s.rsi4h || 'N/A') + ' | 15m ' + (s.rsi15m || 'N/A') + '\n  ' + (parseFloat(s.rsi1h) > 70 ? '⚠️ Overbought' : parseFloat(s.rsi1h) < 30 ? '💡 Oversold' : '➖ Neutral') + '\n' +
    '📈 *MACD:* ' + s.macd.trend + ' | Signal: ' + s.macd.signal + ' | Hist: ' + s.macd.histogram + '\n\n' +
    '📐 *S/R Levels:*\n  Support: $' + s.sr.support + ' | Resistance: $' + s.sr.resistance + '\n  Pivot: $' + s.sr.pivot + ' | R1: $' + s.sr.r1 + ' | S1: $' + s.sr.s1 + '\n  R2: $' + s.sr.r2 + ' | S2: $' + s.sr.s2 + '\n\n' +
    '— *ORDER BOOK* —\n📊 *Imbalance:* ' + (s.depth ? s.depth.imbalance + '% (' + db + ')' : 'N/A') + '\n📏 *Spread:* $' + (s.depth ? s.depth.spread + ' (' + s.depth.spreadPct + '%)' : 'N/A') + '\n  Bid: $' + (s.depth ? s.depth.bestBid : 'N/A') + ' | Ask: $' + (s.depth ? s.depth.bestAsk : 'N/A') + wallText + '\n\n' +
    '— *TRADINGVIEW* —\n📊 *Supertrend:* ' + s.st.signal + ' (' + s.st.trend + ') | Level: $' + s.st.value + stAlign + '\n📈 *EMA Crossover:* ' + s.ema.signal + emaAlign + '\n  EMA9:$' + s.ema.ema9 + ' | EMA21:$' + s.ema.ema21 + '\n  EMA50:$' + s.ema.ema50 + ' | EMA200:$' + s.ema.ema200 + '\n  ' + (s.ema.slowCross === 'BULLISH' ? '✅ Golden Cross' : '❌ Death Cross') + crossAlign + '\n' +
    extra + warnText + '\n\n💡 *Signals:* ' + s.reasons.slice(0, 6).join(' • ') + '\n\n⚡ *K9 Conviction:* ' + stars + ' ' + s.score + '/95 | Consensus: ' + (s.longVotes || 0) + 'L/' + (s.shortVotes || 0) + 'S' + footer;
}

function freeSignal(s) { const e = s.direction === 'LONG' ? '🟢' : '🔴'; return e + ' *' + s.direction + ' ' + (s.name || s.symbol) + '*\n💰 $' + s.price + ' | ' + (s.longVotes || 0) + 'L/' + (s.shortVotes || 0) + 'S\n⚡ Premium: Full breakdown'; }

async function broadcast() { const sigs = []; for (const sym of PRIORITY_PAIRS) { try { const s = await genSignal(sym, 'crypto'); if (s) sigs.push(s); } catch (e) {} } if (sigs.length === 0) { for (const sym of PRIORITY_PAIRS) { try { const s = await genSignal(sym, 'crypto', null, true); if (s) sigs.push(s); } catch(e) {} } } if (sigs.length > 0 && SIGNAL_CHANNEL !== 'none') { const t = GS.totalWins + GS.totalLosses; try { await bot.sendMessage(SIGNAL_CHANNEL, '🤖 *K9 • ' + new Date().toLocaleString() + '*\n\n' + sigs.map(s => freeSignal(s)).join('\n\n') + '\n\n📊 ' + GS.totalSignals + ' signals | ' + (t > 0 ? ((GS.totalWins / t) * 100).toFixed(1) : '--') + '% WR\n💎 @K9sigbot — $249/mo\n\n🔐 K9 SignalBot • Verified Consensus Engine', { parse_mode: 'Markdown' }); } catch (e) {} } }

// ============ MENUS ============
const CM = { reply_markup: { keyboard: [['📊 BTC Signal', '📊 ETH Signal', '📊 SOL Signal'], ['⚠️ /force BTC', '⚠️ /force ETH', '⚠️ /force SOL'], ['📈 STOCKS ▶️', '🥇 METALS ▶️', '🔍 Search Coin'], ['🔍 /diag', '📋 My Positions', '📈 Performance'], ['📊 Global Stats', '🎲 Polymarket', '📰 Forex News'], ['💎 Plans & Pay', '📊 Dashboard']], resize_keyboard: true, persistent: true } };
const SM = { reply_markup: { keyboard: [['📈 AAPL', '📈 TSLA', '📈 NVDA'], ['📈 MSFT', '📈 GOOGL', '📈 AMZN'], ['📈 META', '📈 SPY', '📈 QQQ'], ['📈 AMD', '◀️ BACK TO MAIN', '📊 Dashboard']], resize_keyboard: true, persistent: true } };
const MM = { reply_markup: { keyboard: [['🥇 GOLD', '🥇 SILVER', '🥇 PLATINUM'], ['🥇 PALLADIUM', '🥇 COPPER', '◀️ BACK TO MAIN'], ["📊 System Status", "📊 Dashboard"]], resize_keyboard: true, persistent: true } };
const COINS = { btc: 'BTCUSDT', eth: 'ETHUSDT', sol: 'SOLUSDT', doge: 'DOGEUSDT', xrp: 'XRPUSDT', ada: 'ADAUSDT', pepe: 'PEPEUSDT', shib: 'SHIBUSDT', bonk: 'BONKUSDT', wif: 'WIFUSDT', aave: 'AAVEUSDT', ltc: 'LTCUSDT', sui: 'SUIUSDT', sei: 'SEIUSDT', inj: 'INJUSDT', tia: 'TIAUSDT', rune: 'RUNEUSDT' };

// ============ TELEGRAM HANDLERS ============
bot.onText(/\/start/, msg => { const uid = msg.from?.id; if (!isAuth(uid)) return bot.sendMessage(msg.chat.id, '⛔ Unauthorized'); bot.sendMessage(msg.chat.id, '🤖 *K9 SignalBot v11*\n\n🎯 Strict 3/4 TF Consensus\n⚠️ /force — Relaxed consensus (2/4)\n🔍 /diag — See why no signal\n📊 Leverage + Position Size\n₿ Crypto • 📈 Stocks • 🥇 Metals\n\n👇 Menu:', { parse_mode: 'Markdown', ...CM }); });

bot.onText(/\/signal(?:\s+(.+))?/, async (msg, match) => { const uid = msg.from?.id; if (!isAuth(uid)) return; const input = match?.[1]; if (!input) return bot.sendMessage(msg.chat.id, '/signal btc | eth | sol | aapl | tsla | gold | silver', { parse_mode: 'Markdown' }); let sym = input.toUpperCase(); if (METAL_ALIASES[input.toLowerCase()]) { const mSym = METAL_ALIASES[input.toLowerCase()]; const meta = METALS.find(m => m.symbol === mSym); const s = await genSignal(mSym, 'metal', meta); if (!s) return bot.sendMessage(msg.chat.id, '⚪ *No Clear Signal*\n\n⚠️ Use /force ' + input.toLowerCase() + ' for relaxed (2/3) signal.', { parse_mode: 'Markdown' }); bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); return; } if (TOP_STOCKS.includes(sym)) { const s = await genSignal(sym, 'stock'); if (!s) return bot.sendMessage(msg.chat.id, '⚪ *No Clear Signal*\n\n⚠️ Use /force ' + sym.toLowerCase() + ' for relaxed signal.', { parse_mode: 'Markdown' }); bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); return; } if (!sym.endsWith('USDT')) sym = COINS[input.toLowerCase()] || sym + 'USDT'; const s = await genSignal(sym, 'crypto'); if (!s) return bot.sendMessage(msg.chat.id, '⚪ *No Clear Signal*\n\nStrict 3/4 consensus not met.\n\n⚠️ Tap ⚠️ /force ' + input.toLowerCase() + ' or use button below.', { parse_mode: 'Markdown' }); bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '✅ Trade 25%', callback_data: 'exec_' + s.id + '_25' }], [{ text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }, { text: '🛑 SL Hit', callback_data: 'sl_' + s.id }]] } }); });

bot.onText(/\/force(?:\s+(.+))?/, async (msg, match) => { const uid = msg.from?.id; if (!isAuth(uid)) return; const input = match?.[1]; if (!input) return bot.sendMessage(msg.chat.id, '/force btc | eth | sol | aapl | gold', { parse_mode: 'Markdown' }); let sym = input.toUpperCase(); if (METAL_ALIASES[input.toLowerCase()]) { sym = METAL_ALIASES[input.toLowerCase()]; const meta = METALS.find(m => m.symbol === sym); const s = await genSignal(sym, 'metal', meta, true); if (!s) return bot.sendMessage(msg.chat.id, '⚪ Still no signal.'); bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); return; } if (TOP_STOCKS.includes(sym)) { const s = await genSignal(sym, 'stock', null, true); if (!s) return bot.sendMessage(msg.chat.id, '⚪ Still no signal.'); bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown' }); return; } if (!sym.endsWith('USDT')) sym = COINS[input.toLowerCase()] || sym + 'USDT'; const s = await genSignal(sym, 'crypto', null, true); if (!s) return bot.sendMessage(msg.chat.id, '⚪ Still no signal.'); bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); });

bot.onText(/\/diag(?:\s+(.+))?/, async (msg, match) => { const uid = msg.from?.id; if (!isAuth(uid)) return; const input = match?.[1] || 'BTC'; let sym = input.toUpperCase(); if (!sym.endsWith('USDT')) sym = COINS[input.toLowerCase()] || sym + 'USDT'; const [price, k5m, k15m, k1h, k4h, depth] = await Promise.all([getPrice(sym), getKlines(sym, '5m', 50), getKlines(sym, '15m', 30), getKlines(sym, '1h', 50), getKlines(sym, '4h', 50), getDepth(sym)]); if (!price) return bot.sendMessage(msg.chat.id, '❌ Cannot fetch ' + sym); const rsi5 = calcRSI(k5m.map(k => k.close)).toFixed(1); const rsi15 = calcRSI(k15m.map(k => k.close)).toFixed(1); const rsi1h = calcRSI(k1h.map(k => k.close)).toFixed(1); const rsi4h = calcRSI(k4h.map(k => k.close)).toFixed(1); const macd1h = calcMACD(k1h.map(k => k.close)); const st1h = calcSupertrend(k1h.map(k => k.high), k1h.map(k => k.low), k1h.map(k => k.close)); const tf5m = rsi5 > 55 ? '🟢 LONG' : rsi5 < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const tf15m = rsi15 > 55 ? '🟢 LONG' : rsi15 < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const tf1h = rsi1h > 55 ? '🟢 LONG' : rsi1h < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const tf4h = rsi4h > 55 ? '🟢 LONG' : rsi4h < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const lv = [tf5m,tf15m,tf1h,tf4h].filter(v => v.includes('LONG')).length; const sv = [tf5m,tf15m,tf1h,tf4h].filter(v => v.includes('SHORT')).length; bot.sendMessage(msg.chat.id, '🔍 *Diagnostic: ' + sym + '*\n💰 Price: $' + price.toFixed(2) + '\n\n⏱ *Timeframe Votes:*\n' + tf5m + ' 5M (RSI:' + rsi5 + ')\n' + tf15m + ' 15M (RSI:' + rsi15 + ')\n' + tf1h + ' 1H (RSI:' + rsi1h + ')\n' + tf4h + ' 4H (RSI:' + rsi4h + ')\n\n📊 *MACD 1H:* ' + macd1h.trend + '\n📊 *Supertrend 1H:* ' + st1h.signal + ' (' + st1h.trend + ')\n📊 *Order Book:* ' + (depth ? depth.imbalance + '%' : 'N/A') + '\n\n🎯 *Votes:* ' + lv + 'L / ' + sv + 'S\n⚡ *Status:* ' + (lv >= 3 ? '✅ STRICT LONG' : sv >= 3 ? '✅ STRICT SHORT' : lv >= 2 ? '⚠️ RELAXED POSSIBLE (2L) — /force ' + input.toLowerCase() : sv >= 2 ? '⚠️ RELAXED POSSIBLE (2S) — /force ' + input.toLowerCase() : '❌ NO SIGNAL — wait'), { parse_mode: 'Markdown' }); });

bot.onText(/\/adduser (\d+)/, (msg, match) => { const uid = msg.from?.id; if (!ADMIN_IDS.includes(uid)) return; const nid = parseInt(match[1]); const cur = process.env.ALLOWED_IDS || ''; const up = cur ? cur + ',' + nid : String(nid); let env = fs.readFileSync('.env', 'utf8'); env = env.includes('ALLOWED_IDS=') ? env.replace(/ALLOWED_IDS=.*/, 'ALLOWED_IDS=' + up) : env + '\nALLOWED_IDS=' + up; fs.writeFileSync('.env', env); ALLOWED_IDS.push(nid); bot.sendMessage(msg.chat.id, '✅ User ' + nid + ' added!'); });

bot.on('message', async msg => { const uid = msg.from?.id; if (!isAuth(uid)) return; const t = msg.text || '', cid = msg.chat.id; if (t.startsWith('/')) return;
  if (t.includes('/force BTC') || t.includes('/force ETH') || t.includes('/force SOL')) { let sym = 'BTCUSDT'; if (t.includes('ETH')) sym = 'ETHUSDT'; else if (t.includes('SOL')) sym = 'SOLUSDT'; const s = await genSignal(sym, 'crypto', null, true); if (s) bot.sendMessage(cid, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); else bot.sendMessage(cid, '⚪ Still no signal even with relaxed consensus.', { parse_mode: 'Markdown' }); }
  else if (t.includes('/diag') && !t.startsWith('/diag')) { let sym = 'BTCUSDT'; if (t.toUpperCase().includes('ETH')) sym = 'ETHUSDT'; else if (t.toUpperCase().includes('SOL')) sym = 'SOLUSDT'; const input = sym === 'ETHUSDT' ? 'eth' : sym === 'SOLUSDT' ? 'sol' : 'btc'; const [price, k5m, k15m, k1h, k4h, depth] = await Promise.all([getPrice(sym), getKlines(sym, '5m', 50), getKlines(sym, '15m', 30), getKlines(sym, '1h', 50), getKlines(sym, '4h', 50), getDepth(sym)]); if (!price) return; const rsi5 = calcRSI(k5m.map(k => k.close)).toFixed(1); const rsi15 = calcRSI(k15m.map(k => k.close)).toFixed(1); const rsi1h = calcRSI(k1h.map(k => k.close)).toFixed(1); const rsi4h = calcRSI(k4h.map(k => k.close)).toFixed(1); const macd1h = calcMACD(k1h.map(k => k.close)); const st1h = calcSupertrend(k1h.map(k => k.high), k1h.map(k => k.low), k1h.map(k => k.close)); const tf5m = rsi5 > 55 ? '🟢 LONG' : rsi5 < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const tf15m = rsi15 > 55 ? '🟢 LONG' : rsi15 < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const tf1h = rsi1h > 55 ? '🟢 LONG' : rsi1h < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const tf4h = rsi4h > 55 ? '🟢 LONG' : rsi4h < 45 ? '🔴 SHORT' : '⚪ NEUTRAL'; const lv = [tf5m,tf15m,tf1h,tf4h].filter(v => v.includes('LONG')).length; const sv = [tf5m,tf15m,tf1h,tf4h].filter(v => v.includes('SHORT')).length; bot.sendMessage(cid, '🔍 *Diag: ' + sym + '*\n💰 $' + price.toFixed(2) + '\n\n' + tf5m + ' 5M | ' + tf15m + ' 15M | ' + tf1h + ' 1H | ' + tf4h + ' 4H\n📊 MACD:' + macd1h.trend + ' ST:' + st1h.signal + '\n🎯 ' + lv + 'L/' + sv + 'S | ' + (lv >= 3 ? '✅ STRICT' : lv >= 2 ? '⚠️ RELAXED' : '❌ NONE'), { parse_mode: 'Markdown' }); }
  else if (t.includes('GOLD') || t.includes('SILVER') || t.includes('PLATINUM') || t.includes('PALLADIUM') || t.includes('COPPER')) { const mm = { GOLD: 'XAUUSD', SILVER: 'XAGUSD', PLATINUM: 'XPTUSD', PALLADIUM: 'XPDUSD', COPPER: 'XCUUSD' }; const mSym = Object.keys(mm).find(k => t.toUpperCase().includes(k)); if (mSym && mm[mSym]) { const meta = METALS.find(m => m.symbol === mm[mSym]); const s = await genSignal(mm[mSym], 'metal', meta); if (!s) return bot.sendMessage(cid, '⚪ No signal. /force ' + mSym.toLowerCase(), { parse_mode: 'Markdown' }); bot.sendMessage(cid, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); } }
  else if (TOP_STOCKS.some(s => t.includes(s) && t.includes('📈'))) { const sym = TOP_STOCKS.find(s => t.includes(s)); if (sym) { const s = await genSignal(sym, 'stock'); if (!s) return bot.sendMessage(cid, '⚪ No signal. /force ' + sym.toLowerCase(), { parse_mode: 'Markdown' }); bot.sendMessage(cid, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); } }
  else if (t.includes('BTC Signal') || t.includes('ETH Signal') || t.includes('SOL Signal')) { let sym = 'BTCUSDT'; if (t.includes('ETH')) sym = 'ETHUSDT'; else if (t.includes('SOL')) sym = 'SOLUSDT'; const s = await genSignal(sym, 'crypto'); if (!s) return bot.sendMessage(cid, '⚪ *No Clear Signal*\n\n⚠️ Tap ⚠️ /force button below.', { parse_mode: 'Markdown' }); bot.sendMessage(cid, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '✅ Trade 25%', callback_data: 'exec_' + s.id + '_25' }], [{ text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }, { text: '🛑 SL Hit', callback_data: 'sl_' + s.id }]] } }); }
  else if (t.includes('STOCKS ▶️')) { bot.sendMessage(cid, '📈 *Stocks*\n\nSelect:', { parse_mode: 'Markdown', ...SM }); }
  else if (t.includes('METALS ▶️')) { bot.sendMessage(cid, '🥇 *Metals*\n\nSelect:', { parse_mode: 'Markdown', ...MM }); }
  else if (t.includes('BACK TO MAIN')) { bot.sendMessage(cid, '🤖 *Main Menu*', { parse_mode: 'Markdown', ...CM }); }
  else if (t.includes('Search Coin')) { bot.sendMessage(cid, '🔍 /signal btc | eth | sol | aapl | gold\n⚠️ /force btc — relaxed\n🔍 /diag btc — diagnostic', { parse_mode: 'Markdown' }); }
  else if (t.includes('My Positions')) { const st = loadUser(uid); bot.sendMessage(cid, st.positions && st.positions.length > 0 ? '📊 *Positions*\n\n' + st.positions.map((x, i) => (i + 1) + '. ' + x.direction + ' ' + x.symbol + ' @ $' + x.entry).join('\n') : '📊 No open positions.', { parse_mode: 'Markdown' }); }
  else if (t.includes('Performance')) { const s = loadUser(uid).stats, tot = s.w + s.l; bot.sendMessage(cid, '📈 *Stats*\nTotal:' + s.t + ' | Wins:' + s.w + ' | Losses:' + s.l + ' | WR:' + (tot > 0 ? ((s.w / tot) * 100).toFixed(1) : '0') + '% | Streak:' + s.s, { parse_mode: 'Markdown' }); }
  else if (t.includes('Global Stats')) { const g = GS; const tot = g.totalWins + g.totalLosses; bot.sendMessage(cid, '📊 *Global*\nTotal:' + g.totalSignals + ' | Wins:' + g.totalWins + ' | Losses:' + g.totalLosses + ' | WR:' + (tot > 0 ? ((g.totalWins / tot) * 100).toFixed(1) : '--') + '%', { parse_mode: 'Markdown' }); }
  else if (t.includes('Polymarket')) { const pm = await getPolymarket(); if (pm) { let m = '🎲 *Polymarket*\n\n'; pm.forEach((x, i) => { m += (i + 1) + '. *' + x.title.substring(0, 45) + '*\n   Vol: ' + x.volume + ' | ' + (x.prices || []).join(' / ') + '\n\n'; }); bot.sendMessage(cid, m, { parse_mode: 'Markdown' }); } else bot.sendMessage(cid, '🎲 Polymarket unavailable.', { parse_mode: 'Markdown' }); }
  else if (t.includes('Forex News')) { const fe = await fetchForex(); bot.sendMessage(cid, '📅 *Forex Today*\n\n' + (fe.length > 0 ? fe.slice(0, 8).map(x => (x.impact === 'High' ? '🔴' : '🟡') + ' ' + x.title + ' (' + x.country + ' ' + x.time + ')\n  F:' + (x.forecast || '--') + ' P:' + (x.previous || '--')).join('\n\n') : 'No major events.\n\n📅 Forex Factory'), { parse_mode: 'Markdown' }); }
  else if (t.includes('Plans & Pay')) { bot.sendMessage(cid, '💎 *Plans — BTC Only*\n\n📅 Monthly — $249\n🗓 Yearly — $1,499\n\n`' + BTC_WALLET + '`\n⚠️ BTC ONLY\n\n📡 Free: @k9signalalerts', { parse_mode: 'Markdown' }); }
  else if (t.includes('Dashboard')) { bot.sendMessage(cid, '📊 *Dashboard*\n\nTap below:', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🚀 Open Dashboard', web_app: { url: 'https://telegram-bot-2d4i.onrender.com/dashboard/' + uid } }]] } }); } });

bot.on('callback_query', async q => { const uid = q.from.id; if (!isAuth(uid)) return bot.answerCallbackQuery(q.id); const cid = q.message.chat.id, d = q.data, state = loadUser(uid);
  if (d.startsWith('exec_')) { state.stats.t++; saveUser(uid, state); bot.answerCallbackQuery(q.id, { text: '✅ Trade opened!' }); }
  else if (d.startsWith('tp_')) { state.stats.w++; state.stats.s++; state.stats.p += 1.5; saveUser(uid, state); GS.totalWins++; GS.totalSignals++; saveGS(GS); bot.answerCallbackQuery(q.id, { text: '🎯 TP1 +1.5%' }); bot.sendMessage(cid, '🎯 *TP1 HIT!* +1.5%\nStreak: ' + state.stats.s + ' 🔥', { parse_mode: 'Markdown' }); }
  else if (d.startsWith('sl_')) { state.stats.l++; state.stats.s = 0; state.stats.p -= 1.2; saveUser(uid, state); GS.totalLosses++; GS.totalSignals++; saveGS(GS); bot.answerCallbackQuery(q.id, { text: '🛑 SL -1.2%' }); bot.sendMessage(cid, '🛑 *SL Hit* -1.2%', { parse_mode: 'Markdown' }); }
  else { bot.answerCallbackQuery(q.id); } });

app.get('/api/globalstats', (req, res) => res.json(GS));
app.get('/dashboard/:uid', (req, res) => { if (!isAuth(parseInt(req.params.uid))) return res.status(403).send('Unauthorized'); const html = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>K9 SignalBot</title><script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script><link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap" rel="stylesheet"><style>:root{--bg:#060b10;--bg2:#0d1520;--bg3:#131d2a;--border:rgba(255,255,255,0.06);--text:#e8edf3;--text2:#8899b4;--accent:#F0B90B;--green:#00d4aa;--red:#ff4757;--radius:12px;--shadow:0 4px 24px rgba(0,0,0,0.3)}*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:Inter,sans-serif}.app{max-width:1400px;margin:0 auto;padding:20px}.header{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:20px 28px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center;box-shadow:var(--shadow)}.header h1{font-size:20px;font-weight:700}.badge-live{font-size:11px;padding:6px 14px;border-radius:20px;background:rgba(0,212,170,0.1);color:var(--green);border:1px solid rgba(0,212,170,0.2)}.stats-row{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}.stat-card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:20px;box-shadow:var(--shadow);text-align:center}.stat-value{font-size:32px;font-weight:700}.stat-label{font-size:11px;color:var(--text2);text-transform:uppercase;letter-spacing:1px;margin-top:4px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.panel{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:20px;box-shadow:var(--shadow)}.panel-title{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--text2);margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border)}.signal-card{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:14px;margin-bottom:10px}.sig-details{font-size:11px;color:var(--text2);line-height:1.8;margin-top:6px}.pos-row,.price-row{display:flex;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border);font-size:13px}.green{color:var(--green)}.red{color:var(--red)}.accent{color:var(--accent)}.empty{text-align:center;padding:40px;color:var(--text3)}.pulse{animation:pulse 2s infinite}@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}@media(max-width:768px){.stats-row{grid-template-columns:1fr 1fr}.grid{grid-template-columns:1fr}}</style></head><body><div class="app"><div class="header"><div><h1>K9 SignalBot</h1><div style="font-size:11px;color:var(--text2);">Professional Trading Dashboard</div></div><span class="badge-live"><span class="pulse" style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);margin-right:6px;"></span>Live</span></div><div class="stats-row"><div class="stat-card"><div class="stat-value green" id="tT">0</div><div class="stat-label">Total Trades</div></div><div class="stat-card"><div class="stat-value green" id="tW">0</div><div class="stat-label">Wins</div></div><div class="stat-card"><div class="stat-value red" id="tL">0</div><div class="stat-label">Losses</div></div><div class="stat-card"><div class="stat-value accent" id="tWR">0%</div><div class="stat-label">Win Rate</div></div></div><div class="grid"><div class="panel"><div class="panel-title">Active Signals</div><div id="signals"><div class="empty">No signals yet</div></div></div><div class="panel"><div class="panel-title">Open Positions</div><div id="positions"><div class="empty">No positions</div></div></div><div class="panel"><div class="panel-title">Global Stats</div><div id="global"><div class="empty">Loading...</div></div></div><div class="panel"><div class="panel-title">Live Prices</div><div id="prices"><div class="empty">Loading...</div></div></div></div></div><script>const s=io();s.on("stateUpdate-' + req.params.uid + '",st=>{if(st.stats){const x=st.stats,t=x.w+x.l;document.getElementById("tT").textContent=x.t||0;document.getElementById("tW").textContent=x.w||0;document.getElementById("tL").textContent=x.l||0;document.getElementById("tWR").textContent=(t>0?((x.w/t)*100).toFixed(1):"0")+"%"}if(st.signals){document.getElementById("signals").innerHTML=st.signals.slice(0,8).map(function(x){var stars=x.score>=65?"★★★":x.score>=45?"★★":"★";return "<div class=signal-card><div style=display:flex;justify-content:space-between;align-items:center;margin-bottom:6px><span style=font-weight:600;font-size:14px>"+(x.direction=="LONG"?"🟢":"🔴")+" "+x.symbol+" <span style=color:"+(x.direction=="LONG"?"var(--green)":"var(--red)")+">"+x.direction+"</span></span><span style=font-weight:700;font-size:16px;color:"+(x.score>=70?"var(--green)":x.score>=45?"var(--accent)":"var(--red)")+">"+stars+" "+x.score+"/95</span></div><div class=sig-details>$"+x.price+" → Entry: $"+x.entry+"<br>TP1: $"+x.tp1+" | TP2: $"+x.tp2+" | TP3: $"+x.tp3+"<br>SL: $"+x.sl+" | R:R 1:"+x.rr1+"<br>RSI: "+x.rsi1h+" | MACD: "+(x.macd?x.macd.trend:"--")+" | S/R: $"+(x.sr?x.sr.support:"--")+"/$"+(x.sr?x.sr.resistance:"--")+"<br>"+(x.reasons||[]).slice(0,4).join(" • ")+"</div></div>"}).join("")||"<div class=empty>No signals yet</div>"}if(st.positions){document.getElementById("positions").innerHTML=st.positions.length?st.positions.map(function(p){return "<div class=pos-row><span>"+p.direction+" "+p.symbol+" @ $"+p.entry+" ("+p.size+"%)</span><span class="+((p.pnlPercent||0)>=0?"green":"red")+">"+((p.pnlPercent||0)>=0?"+":"")+(p.pnlPercent||0).toFixed(2)+"%</span></div>"}).join(""):"<div class=empty>No positions</div>"}});fetch("/api/globalstats").then(function(r){return r.json()}).then(function(g){var t=(g.totalWins||0)+(g.totalLosses||0);document.getElementById("global").innerHTML="<div style=text-align:center><div style=font-size:32px;font-weight:700;color:var(--accent);>"+(g.totalSignals||0)+"</div><div style=font-size:11px;color:var(--text2);>Total Signals</div><div style=margin-top:12px;><span class=green>"+(g.totalWins||0)+" Wins</span> • <span class=red>"+(g.totalLosses||0)+" Losses</span></div><div style=margin-top:4px;font-size:18px;font-weight:600;color:var(--accent);>"+(t>0?((g.totalWins/t)*100).toFixed(1):"--")+"% WR</div></div>"});s.on("priceUpdate",function(p){var h="";for(var k in p){if(p.hasOwnProperty(k))h+="<div class=price-row><span>"+k+"</span><span style=color:var(--accent)>$"+(p[k].price?p[k].price.toFixed(2):"--")+"</span></div>"}document.getElementById("prices").innerHTML=h||"<div class=empty>Loading...</div>"});</script></body></html>'; res.send(html); });

async function updatePrices() { for (const s of PRIORITY_PAIRS) { const p = await getPrice(s); if (p) { if (!GS.marketData) GS.marketData = {}; GS.marketData[s] = { price: p }; } } io.emit('priceUpdate', GS.marketData || {}); }

server.listen(PORT, '0.0.0.0', () => { console.log('K9 SignalBot v11 Final running on port ' + PORT); broadcast(); setInterval(broadcast, 15 * 60 * 1000); setInterval(updatePrices, 5000); updatePrices(); });
