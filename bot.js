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
const ADMIN_IDS = (process.env.ADMIN_IDS || '').split(',').map(Number).filter(Boolean);
const ALLOWED_IDS = (process.env.ALLOWED_IDS || '').split(',').map(Number).filter(Boolean);
const SIGNAL_CHANNEL = process.env.SIGNAL_CHANNEL || 'none';
const PRIORITY_PAIRS = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT'];
const TOP_STOCKS = ['AAPL', 'TSLA', 'NVDA', 'MSFT', 'GOOGL', 'AMZN', 'META', 'SPY', 'QQQ', 'AMD'];
const AV_KEY = process.env.ALPHA_VANTAGE_KEY || 'demo';
if (!fs.existsSync('./state')) fs.mkdirSync('./state');
if (!fs.existsSync('./stats')) fs.mkdirSync('./stats');

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

const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36';
async function fetchWithRetry(url, opts, retries) { retries = retries || 3; for (let i = 0; i < retries; i++) { try { return await axios.get(url, { ...opts, timeout: (opts?.timeout || 5000) * (i + 1) }); } catch (e) { if (i === retries - 1) throw e; await new Promise(r => setTimeout(r, 1000 * (i + 1))); } } }

// BINANCE
async function getPrice(s) { try { const r = await fetchWithRetry('https://api.binance.com/api/v3/ticker/price?symbol=' + s); return parseFloat(r.data.price); } catch (e) { return null; } }
async function get24hr(s) { try { const r = await fetchWithRetry('https://api.binance.com/api/v3/ticker/24hr?symbol=' + s); return { high: parseFloat(r.data.highPrice), low: parseFloat(r.data.lowPrice), change: parseFloat(r.data.priceChangePercent), volume: parseFloat(r.data.quoteVolume) }; } catch (e) { return null; } }
async function getKlines(s, i, l) { try { const r = await fetchWithRetry('https://api.binance.com/api/v3/klines?symbol=' + s + '&interval=' + i + '&limit=' + l); return r.data.map(k => ({ close: parseFloat(k[4]), high: parseFloat(k[2]), low: parseFloat(k[3]), volume: parseFloat(k[5]) })); } catch (e) { return []; } }
async function getDepth(s) { try { const r = await fetchWithRetry('https://api.binance.com/api/v3/depth?symbol=' + s + '&limit=100'); const bids = r.data.bids.map(b => ({ p: parseFloat(b[0]), q: parseFloat(b[1]) })), asks = r.data.asks.map(a => ({ p: parseFloat(a[0]), q: parseFloat(a[1]) })); const bv = bids.reduce((x, b) => x + b.q * b.p, 0), av = asks.reduce((x, a) => x + a.q * a.p, 0); const bw = findWalls(bids), aw = findWalls(asks); return { imbalance: +(((bv - av) / (bv + av)) * 100).toFixed(1), spread: +((asks[0].p - bids[0].p).toFixed(2)), spreadPct: +(((asks[0].p - bids[0].p) / bids[0].p) * 100).toFixed(3), bestBid: bids[0].p, bestAsk: asks[0].p, bidWalls: bw.slice(0, 3).map(w => ({ price: w.price.toFixed(2), qty: w.totalQty.toFixed(2), orders: w.count })), askWalls: aw.slice(0, 3).map(w => ({ price: w.price.toFixed(2), qty: w.totalQty.toFixed(2), orders: w.count })) }; } catch (e) { return null; } }
function findWalls(o) { const w = []; let c = { price: o[0]?.p || 0, totalQty: 0, count: 0 }; for (let i = 0; i < o.length; i++) { if (i > 0 && Math.abs(o[i].p - o[i - 1].p) / o[i - 1].p > 0.0005) { if (c.count >= 3) w.push(c); c = { price: o[i].p, totalQty: 0, count: 0 }; } c.totalQty += o[i].q; c.count++; } if (c.count >= 3) w.push(c); return w.sort((a, b) => b.totalQty - a.totalQty); }
async function getFunding(s) { try { const r = await fetchWithRetry('https://fapi.binance.com/fapi/v1/fundingRate?symbol=' + s + '&limit=1'); const rate = parseFloat(r.data[0]?.fundingRate || 0) * 100; return { rate: rate.toFixed(4), sentiment: rate > 0.05 ? 'BEARISH' : rate < -0.05 ? 'BULLISH' : 'NEUTRAL' }; } catch (e) { return { rate: '0', sentiment: 'NEUTRAL' }; } }
async function getLSRatio(s) { try { const r = await fetchWithRetry('https://fapi.binance.com/fapi/v1/globalLongShortAccountRatio?symbol=' + s + '&period=5m&limit=1'); const ratio = parseFloat(r.data[0]?.longShortRatio || 1); return { long: ratio.toFixed(1), sentiment: ratio > 1.5 ? 'BEARISH (crowded longs)' : ratio < 0.7 ? 'BULLISH (crowded shorts)' : 'NEUTRAL' }; } catch (e) { return { long: '1.0', sentiment: 'NEUTRAL' }; } }
async function getOpenInterest(s) { try { const r = await fetchWithRetry('https://fapi.binance.com/fapi/v1/openInterest?symbol=' + s); return (parseFloat(r.data.openInterest) / 1e9).toFixed(1) + 'B'; } catch (e) { return 'N/A'; } }

// STOCKS
async function getStockPriceAV(s) { try { const r = await fetchWithRetry('https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=' + s + '&apikey=' + AV_KEY, {}, 2); const q = r.data['Global Quote']; return parseFloat(q['05. price']) || null; } catch (e) { return null; } }
async function getStockDataAV(s) { try { const r = await fetchWithRetry('https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&symbol=' + s + '&interval=5min&outputsize=compact&apikey=' + AV_KEY, {}, 2); const ts = r.data['Time Series (5min)']; if (!ts) return null; const entries = Object.entries(ts).slice(0, 50); return { closes: entries.map(e => parseFloat(e[1]['4. close'])).reverse(), highs: entries.map(e => parseFloat(e[1]['2. high'])).reverse(), lows: entries.map(e => parseFloat(e[1]['3. low'])).reverse() }; } catch (e) { return null; } }
async function getStockPrice(s) { let p = await getStockPriceAV(s); if (p) return p; try { const r = await fetchWithRetry('https://query1.finance.yahoo.com/v8/finance/chart/' + s, { headers: { 'User-Agent': UA } }, 2); return r.data.chart.result[0].meta.regularMarketPrice; } catch (e) { return null; } }
async function getStockData(s) { let d = await getStockDataAV(s); if (d && d.closes.length > 10) return d; try { const r = await fetchWithRetry('https://query1.finance.yahoo.com/v8/finance/chart/' + s, { headers: { 'User-Agent': UA } }, 2); const q = r.data.chart.result[0].indicators.quote[0]; return { closes: (q.close || []).filter(c => c !== null), highs: (q.high || []).filter(h => h !== null), lows: (q.low || []).filter(l => l !== null) }; } catch (e) { return { closes: [], highs: [], lows: [] }; } }

// FOREX & POLYMARKET
async function fetchForex() { try { const r = await fetchWithRetry('https://nfs.faireconomy.media/ff_calendar_thisweek.json', {}, 2); const events = []; const today = new Date(); if (Array.isArray(r.data)) { r.data.forEach(e => { const d = new Date(e.date || ''); if (Math.abs(d - today) < 86400000) events.push({ title: e.title || 'Event', country: e.country || 'USD', time: e.time || '', impact: e.impact || 'Medium', forecast: e.forecast || '', previous: e.previous || '' }); }); } return events; } catch (e) { return []; } }
async function getPolymarket() { try { const r = await fetchWithRetry('https://gamma-api.polymarket.com/markets?limit=6&order=volume&ascending=false', {}, 2); if (Array.isArray(r.data)) return r.data.slice(0, 6).map(m => ({ title: m.title || m.question || '', volume: (parseFloat(m.volumeNum || m.volume || 0) / 1e6).toFixed(1) + 'M', prices: [m.outcomePrices ? JSON.parse(m.outcomePrices)[0] : '0'].map(p => (parseFloat(p) * 100).toFixed(0) + '%') })); } catch (e) { return null; } }

// TECHNICALS
function calcRSI(p, per) { per = per || 14; if (p.length < per + 1) return 50; let g = 0, l = 0; for (let i = p.length - per; i < p.length; i++) { let d = p[i] - p[i - 1]; if (d >= 0) g += d; else l -= d; } if (l === 0) return 100; return 100 - (100 / (1 + (g / per) / (l / per))); }
function calcMACD(prices) { if (prices.length < 26) return { macd: 0, signal: 0, histogram: 0, trend: 'NEUTRAL' }; const ema = (p, per) => { const k = 2 / (per + 1); let e = p[0]; for (let i = 1; i < p.length; i++) e = p[i] * k + e * (1 - k); return e; }; const e12 = ema(prices, 12), e26 = ema(prices, 26); const mv = e12 - e26, sv = ema([mv], 9); return { macd: mv.toFixed(2), signal: sv.toFixed(2), histogram: (mv - sv).toFixed(4), trend: mv > sv ? 'BULLISH' : 'BEARISH' }; }
function calcSR(prices) { const h = Math.max(...prices), l = Math.min(...prices), c = prices[prices.length - 1]; const pp = (h + l + c) / 3; return { resistance: h.toFixed(2), support: l.toFixed(2), pivot: pp.toFixed(2), r1: (2 * pp - l).toFixed(2), s1: (2 * pp - h).toFixed(2), r2: (pp + (h - l)).toFixed(2), s2: (pp - (h - l)).toFixed(2) }; }
function calcATR(h, l, c, per) { per = per || 14; if (c.length < per + 1) return [0]; const tr = []; for (let i = 0; i < c.length; i++) { if (i === 0) tr.push(h[i] - l[i]); else tr.push(Math.max(h[i] - l[i], Math.abs(h[i] - c[i - 1]), Math.abs(l[i] - c[i - 1]))); } const atr = []; let sum = tr.slice(0, per).reduce((a, b) => a + b, 0); atr.push(sum / per); for (let i = per; i < tr.length; i++) atr.push((atr[atr.length - 1] * (per - 1) + tr[i]) / per); return atr; }
function calcSupertrend(h, l, c, per, mult) { per = per || 10; mult = mult || 3; if (h.length < per + 1) return { trend: 'NEUTRAL', signal: 'WAIT', value: '0' }; const atr = calcATR(h, l, c, per); const src = c.map((cl, i) => (h[i] + l[i]) / 2); let ub = [], lb = [], st = [], dir = 0; for (let i = 0; i < c.length; i++) { const mid = src[i]; const av = atr[i] || atr[atr.length - 1]; let u = mid + mult * av; let lo = mid - mult * av; if (i > 0) { if (c[i - 1] > ub[i - 1]) u = Math.max(u, ub[i - 1]); if (c[i - 1] < lb[i - 1]) lo = Math.min(lo, lb[i - 1]); } ub.push(u); lb.push(lo); if (i === 0) { st.push(u); dir = -1; } else { if (st[i - 1] === ub[i - 1]) { if (c[i] <= u) { st.push(u); dir = -1; } else { st.push(lo); dir = 1; } } else { if (c[i] >= lo) { st.push(lo); dir = 1; } else { st.push(u); dir = -1; } } } } return { trend: dir === 1 ? 'BULLISH' : 'BEARISH', signal: dir === 1 ? 'BUY' : 'SELL', value: st[st.length - 1].toFixed(2) }; }
function calcEMAVal(cl, per) { if (cl.length < per) return cl[cl.length - 1]; const k = 2 / (per + 1); let e = cl.slice(0, per).reduce((a, b) => a + b, 0) / per; for (let i = per; i < cl.length; i++) e = cl[i] * k + e * (1 - k); return e; }
function calcEMACross(cl) { const e9 = calcEMAVal(cl, 9); const e21 = calcEMAVal(cl, 21); const e50 = calcEMAVal(cl, 50); const e200 = calcEMAVal(cl, 200); const p9 = calcEMAVal(cl.slice(0, -1), 9); const p21 = calcEMAVal(cl.slice(0, -1), 21); return { ema9: e9.toFixed(2), ema21: e21.toFixed(2), ema50: e50.toFixed(2), ema200: e200.toFixed(2), signal: (p9 <= p21 && e9 > e21) ? 'STRONG BUY' : (p9 >= p21 && e9 < e21) ? 'STRONG SELL' : e9 > e21 ? 'BUY' : 'SELL', slowCross: e50 > e200 ? 'BULLISH' : 'BEARISH' }; }
function tfVote(tf) { if (!tf || tf.bias === '--' || tf.bias === 'N/A') return 'neutral'; if (tf.bias.includes('BULLISH')) return 'long'; if (tf.bias.includes('BEARISH')) return 'short'; return 'neutral'; }

async function genCryptoSignal(symbol) {
  const [price, stats, k5m, k15m, k1h, k4h, depth, funding, lsRatio, oi] = await Promise.all([getPrice(symbol), get24hr(symbol), getKlines(symbol, '5m', 50), getKlines(symbol, '15m', 30), getKlines(symbol, '1h', 50), getKlines(symbol, '4h', 50), getDepth(symbol), getFunding(symbol), getLSRatio(symbol), getOpenInterest(symbol)]);
  if (!price || !depth) return null;
  const tf5m = { label: '5M', bias: calcRSI(k5m.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k5m.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k5m.map(k => k.close)) > 55 ? '🟢' : calcRSI(k5m.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k5m.map(k => k.close)).toFixed(1), macd: calcMACD(k5m.map(k => k.close)).trend, st: calcSupertrend(k5m.map(k => k.high), k5m.map(k => k.low), k5m.map(k => k.close)).signal };
  const tf15m = { label: '15M', bias: calcRSI(k15m.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k15m.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k15m.map(k => k.close)) > 55 ? '🟢' : calcRSI(k15m.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k15m.map(k => k.close)).toFixed(1), macd: calcMACD(k15m.map(k => k.close)).trend, st: calcSupertrend(k15m.map(k => k.high), k15m.map(k => k.low), k15m.map(k => k.close)).signal };
  const tf1h = { label: '1H', bias: calcRSI(k1h.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k1h.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k1h.map(k => k.close)) > 55 ? '🟢' : calcRSI(k1h.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k1h.map(k => k.close)).toFixed(1), macd: calcMACD(k1h.map(k => k.close)).trend, st: calcSupertrend(k1h.map(k => k.high), k1h.map(k => k.low), k1h.map(k => k.close)).signal };
  const tf4h = { label: '4H', bias: calcRSI(k4h.map(k => k.close)) > 55 ? 'BULLISH' : calcRSI(k4h.map(k => k.close)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(k4h.map(k => k.close)) > 55 ? '🟢' : calcRSI(k4h.map(k => k.close)) < 45 ? '🔴' : '⚪', rsi: calcRSI(k4h.map(k => k.close)).toFixed(1), macd: calcMACD(k4h.map(k => k.close)).trend, st: calcSupertrend(k4h.map(k => k.high), k4h.map(k => k.low), k4h.map(k => k.close)).signal };
  const votes = [tfVote(tf5m), tfVote(tf15m), tfVote(tf1h), tfVote(tf4h)];
  const lv = votes.filter(v => v === 'long').length, sv = votes.filter(v => v === 'short').length;
  let dir = null;
  if (lv >= 3) dir = 'LONG'; else if (sv >= 3) dir = 'SHORT';
  else if (lv === 2 && sv <= 1 && depth.imbalance > 10) dir = 'LONG';
  else if (sv === 2 && lv <= 1 && depth.imbalance < -10) dir = 'SHORT';
  if (!dir) return null;
  const c1h = k1h.map(k => k.close);
  const rsi1h = calcRSI(c1h), rsi4h = calcRSI(k4h.map(k => k.close)), rsi15m = calcRSI(k15m.map(k => k.close));
  const macd1h = calcMACD(c1h), sr = calcSR(c1h);
  const st = calcSupertrend(k1h.map(k => k.high), k1h.map(k => k.low), c1h);
  const ema = calcEMACross(c1h);
  const rv = k15m.slice(-5).reduce((s, k) => s + k.volume, 0) / 5;
  const av = k15m.slice(0, 25).reduce((s, k) => s + k.volume, 0) / 25;
  const vs = av > 0 ? rv / av : 1;
  let warnings = [], reasons = [];
  if (funding.sentiment === 'BEARISH' && dir === 'LONG') warnings.push('Funding opposes');
  if (funding.sentiment === 'BULLISH' && dir === 'SHORT') warnings.push('Funding opposes');
  if (macd1h.trend === 'BEARISH' && dir === 'LONG') warnings.push('MACD opposes');
  if (macd1h.trend === 'BULLISH' && dir === 'SHORT') warnings.push('MACD opposes');
  if (ema.slowCross !== (dir === 'LONG' ? 'BULLISH' : 'BEARISH')) warnings.push('Cross opposes');
  if (lv >= 3) reasons.push(lv + '/4 timeframes bullish');
  if (sv >= 3) reasons.push(sv + '/4 timeframes bearish');
  if (depth.imbalance > 10 && dir === 'LONG') reasons.push('Order book confirms');
  if (depth.imbalance < -10 && dir === 'SHORT') reasons.push('Order book confirms');
  if (st.trend === (dir === 'LONG' ? 'BULLISH' : 'BEARISH')) reasons.push('Supertrend aligned');
  if (ema.signal.includes(dir === 'LONG' ? 'BUY' : 'SELL')) reasons.push('EMA aligned');
  if (vs > 2) reasons.push('Volume surge ' + vs.toFixed(1) + 'x');
  let sc = lv >= 3 ? 55 + lv * 10 : sv >= 3 ? 55 + sv * 10 : 45;
  if (depth.imbalance > 15 && dir === 'LONG') sc += 10;
  if (depth.imbalance < -15 && dir === 'SHORT') sc += 10;
  if (st.trend === (dir === 'LONG' ? 'BULLISH' : 'BEARISH')) sc += 10;
  if (ema.signal.includes(dir === 'LONG' ? 'BUY' : 'SELL')) sc += 5;
  sc -= warnings.length * 5;
  sc = Math.min(95, Math.max(25, sc));
  const vol = symbol.includes('BTC') ? 0.008 : 0.020;
  let e, tp1, tp2, tp3, sl;
  if (dir === 'LONG') { e = price * 0.999; tp1 = +(e * (1 + vol)).toFixed(2); tp2 = +(e * (1 + vol * 2.0)).toFixed(2); tp3 = +(e * (1 + vol * 3.5)).toFixed(2); sl = +(e * (1 - vol * 0.8)).toFixed(2); }
  else { e = price * 1.001; tp1 = +(e * (1 - vol)).toFixed(2); tp2 = +(e * (1 - vol * 2.0)).toFixed(2); tp3 = +(e * (1 - vol * 3.5)).toFixed(2); sl = +(e * (1 + vol * 0.8)).toFixed(2); }
  return { id: crypto.randomBytes(4).toString('hex'), type: 'crypto', symbol, direction: dir, price: price.toFixed(2), entry: e.toFixed(2), tp1, tp2, tp3, sl, trailingSL: (vol * 0.6 * 100).toFixed(1) + '%', score: sc, rsi1h: rsi1h.toFixed(1), rsi4h: rsi4h.toFixed(1), rsi15m: rsi15m.toFixed(1), macd: macd1h, sr, depth, rr1: (Math.abs(tp1 - e) / Math.abs(e - sl)).toFixed(1), rr2: (Math.abs(tp2 - e) / Math.abs(e - sl)).toFixed(1), rr3: (Math.abs(tp3 - e) / Math.abs(e - sl)).toFixed(1), funding, lsRatio, oi, vs: vs.toFixed(1), reasons, warnings, st, ema, tf5m, tf15m, tf1h, tf4h, stats, longVotes: lv, shortVotes: sv, timestamp: new Date().toISOString() };
}

async function genStockSignal(symbol) {
  const [price, data] = await Promise.all([getStockPrice(symbol), getStockData(symbol)]);
  if (!price || !data.closes || data.closes.length < 15) return null;
  const cl = data.closes, hi = data.highs, lo = data.lows;
  const tf5m = { label: '5M', bias: calcRSI(cl.slice(-50)) > 55 ? 'BULLISH' : calcRSI(cl.slice(-50)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(cl.slice(-50)) > 55 ? '🟢' : calcRSI(cl.slice(-50)) < 45 ? '🔴' : '⚪', rsi: calcRSI(cl.slice(-50)).toFixed(1), macd: calcMACD(cl.slice(-50)).trend, st: calcSupertrend(hi.slice(-50), lo.slice(-50), cl.slice(-50)).signal };
  const tf15m = { label: '15M', bias: calcRSI(cl.slice(-30)) > 55 ? 'BULLISH' : calcRSI(cl.slice(-30)) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(cl.slice(-30)) > 55 ? '🟢' : calcRSI(cl.slice(-30)) < 45 ? '🔴' : '⚪', rsi: calcRSI(cl.slice(-30)).toFixed(1), macd: calcMACD(cl.slice(-30)).trend, st: calcSupertrend(hi.slice(-30), lo.slice(-30), cl.slice(-30)).signal };
  const tf1h = { label: '1H', bias: calcRSI(cl) > 55 ? 'BULLISH' : calcRSI(cl) < 45 ? 'BEARISH' : 'NEUTRAL', emoji: calcRSI(cl) > 55 ? '🟢' : calcRSI(cl) < 45 ? '🔴' : '⚪', rsi: calcRSI(cl).toFixed(1), macd: calcMACD(cl).trend, st: calcSupertrend(hi, lo, cl).signal };
  const votes = [tfVote(tf5m), tfVote(tf15m), tfVote(tf1h)];
  const lv = votes.filter(v => v === 'long').length, sv = votes.filter(v => v === 'short').length;
  let dir = null;
  if (lv >= 2) dir = 'LONG'; else if (sv >= 2) dir = 'SHORT';
  if (!dir) return null;
  const rsi = calcRSI(cl), macd = calcMACD(cl), sr = calcSR(cl), st = calcSupertrend(hi, lo, cl), ema = calcEMACross(cl);
  let sc = lv >= 2 ? 50 + lv * 10 : 50 + sv * 10;
  if (st.trend === (dir === 'LONG' ? 'BULLISH' : 'BEARISH')) sc += 10;
  sc = Math.min(90, Math.max(30, sc));
  const vol = 0.015;
  let e, tp1, tp2, tp3, sl;
  if (dir === 'LONG') { e = price * 0.999; tp1 = +(e * (1 + vol)).toFixed(2); tp2 = +(e * (1 + vol * 2)).toFixed(2); tp3 = +(e * (1 + vol * 3.5)).toFixed(2); sl = +(e * (1 - vol * 0.8)).toFixed(2); }
  else { e = price * 1.001; tp1 = +(e * (1 - vol)).toFixed(2); tp2 = +(e * (1 - vol * 2)).toFixed(2); tp3 = +(e * (1 - vol * 3.5)).toFixed(2); sl = +(e * (1 + vol * 0.8)).toFixed(2); }
  return { id: crypto.randomBytes(4).toString('hex'), type: 'stock', symbol, direction: dir, price: price.toFixed(2), entry: e.toFixed(2), tp1, tp2, tp3, sl, score: sc, rsi1h: rsi.toFixed(1), macd, sr, depth: { imbalance: 'N/A', spread: 'N/A', spreadPct: 'N/A', bestBid: 'N/A', bestAsk: 'N/A', bidWalls: [], askWalls: [] }, rr1: (Math.abs(tp1 - e) / Math.abs(e - sl)).toFixed(1), rr2: (Math.abs(tp2 - e) / Math.abs(e - sl)).toFixed(1), rr3: (Math.abs(tp3 - e) / Math.abs(e - sl)).toFixed(1), funding: { rate: 'N/A', sentiment: 'N/A' }, lsRatio: { long: 'N/A', sentiment: 'N/A' }, oi: 'N/A', vs: '1.0', reasons: [(lv >= 2 ? lv : sv) + '/3 timeframes aligned'], warnings: [], st, ema, tf5m, tf15m, tf1h, tf4h: { label:'4H',bias:'N/A',emoji:'⚪',rsi:'--',macd:'--',st:'--'}, stats: null, rsi4h: 'N/A', rsi15m: 'N/A', trailingSL: 'N/A', longVotes: lv, shortVotes: sv, timestamp: new Date().toISOString() };
}

function fullSignal(s) {
  const emoji = s.direction === 'LONG' ? '🟢' : '🔴', stars = s.score >= 65 ? '★★★' : s.score >= 45 ? '★★' : '★';
  const label = s.type === 'stock' ? '📈 STOCK' : '₿ CRYPTO';
  const rsiEmoji = parseFloat(s.rsi1h) > 70 ? '🔥' : parseFloat(s.rsi1h) < 30 ? '❄️' : '➖';
  const db = s.depth && s.depth.imbalance !== 'N/A' ? (parseFloat(s.depth.imbalance) > 10 ? 'BULLISH' : parseFloat(s.depth.imbalance) < -10 ? 'BEARISH' : 'NEUTRAL') : 'N/A';
  let tfSection = '⏱ *TIMEFRAME CONSENSUS* (' + (s.longVotes || 0) + 'L/' + (s.shortVotes || 0) + 'S)\n';
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
  return label + ' • ' + emoji + ' *' + s.direction + ' ' + s.symbol + '*\n\n' +
    '💰 *Current:* $' + s.price + '  →  🎯 *Entry:* $' + s.entry + '\n\n' +
    '📈 *TP1:* $' + s.tp1 + ' _(+' + ((parseFloat(s.tp1) / parseFloat(s.entry) - 1) * 100).toFixed(1) + '%, R:R 1:' + s.rr1 + ')_\n' +
    '   *TP2:* $' + s.tp2 + ' _(R:R 1:' + s.rr2 + ')_\n' +
    '   *TP3:* $' + s.tp3 + ' _(R:R 1:' + s.rr3 + ')_\n\n' +
    '🛑 *Stop:* $' + s.sl + ' _(' + ((parseFloat(s.sl) / parseFloat(s.entry) - 1) * 100).toFixed(1) + '%)_\n📊 *Trailing SL:* ' + (s.trailingSL || 'N/A') + '\n\n' + tfSection + '\n' +
    '— *TECHNICALS (1H)* —\n' +
    '📊 *RSI:* 1H ' + s.rsi1h + ' ' + rsiEmoji + ' | 4H ' + (s.rsi4h || 'N/A') + ' | 15m ' + (s.rsi15m || 'N/A') + '\n  ' + (parseFloat(s.rsi1h) > 70 ? '⚠️ Overbought' : parseFloat(s.rsi1h) < 30 ? '💡 Oversold' : '➖ Neutral') + '\n' +
    '📈 *MACD:* ' + s.macd.trend + ' | Signal: ' + s.macd.signal + ' | Hist: ' + s.macd.histogram + '\n\n' +
    '📐 *S/R Levels:*\n  Support: $' + s.sr.support + ' | Resistance: $' + s.sr.resistance + '\n  Pivot: $' + s.sr.pivot + ' | R1: $' + s.sr.r1 + ' | S1: $' + s.sr.s1 + '\n  R2: $' + s.sr.r2 + ' | S2: $' + s.sr.s2 + '\n\n' +
    '— *ORDER BOOK* —\n📊 *Imbalance:* ' + (s.depth ? s.depth.imbalance + '% (' + db + ')' : 'N/A') + '\n📏 *Spread:* $' + (s.depth ? s.depth.spread + ' (' + s.depth.spreadPct + '%)' : 'N/A') + '\n  Bid: $' + (s.depth ? s.depth.bestBid : 'N/A') + ' | Ask: $' + (s.depth ? s.depth.bestAsk : 'N/A') + wallText + '\n\n' +
    '— *TRADINGVIEW* —\n📊 *Supertrend:* ' + s.st.signal + ' (' + s.st.trend + ') | Level: $' + s.st.value + stAlign + '\n📈 *EMA Crossover:* ' + s.ema.signal + emaAlign + '\n  EMA9:$' + s.ema.ema9 + ' | EMA21:$' + s.ema.ema21 + '\n  EMA50:$' + s.ema.ema50 + ' | EMA200:$' + s.ema.ema200 + '\n  ' + (s.ema.slowCross === 'BULLISH' ? '✅ Golden Cross' : '❌ Death Cross') + crossAlign + '\n' +
    extra + warnText + '\n\n💡 *Signals:* ' + s.reasons.slice(0, 6).join(' • ') + '\n\n⚡ *K9 Conviction:* ' + stars + ' ' + s.score + '/95 | Consensus: ' + (s.longVotes || 0) + 'L/' + (s.shortVotes || 0) + 'S';
}

function freeSignal(s) { const e = s.direction === 'LONG' ? '🟢' : '🔴'; return e + ' *' + s.direction + ' ' + s.symbol + '*\n💰 $' + s.price + ' | ' + (s.longVotes || 0) + 'L/' + (s.shortVotes || 0) + 'S\n⚡ Premium: Full breakdown'; }

async function broadcast() { const sigs = []; for (const sym of PRIORITY_PAIRS) { try { const s = await genCryptoSignal(sym); if (s) sigs.push(s); } catch (e) {} } if (sigs.length > 0 && SIGNAL_CHANNEL !== 'none') { const t = GS.totalWins + GS.totalLosses; try { await bot.sendMessage(SIGNAL_CHANNEL, '🤖 *K9 • ' + new Date().toLocaleString() + '*\n\n' + sigs.map(s => freeSignal(s)).join('\n\n') + '\n\n📊 ' + GS.totalSignals + ' signals | ' + (t > 0 ? ((GS.totalWins / t) * 100).toFixed(1) : '--') + '% WR\n💎 @K9sigbot — $249/mo', { parse_mode: 'Markdown' }); } catch (e) {} } }

const CM = { reply_markup: { keyboard: [['📊 BTC Signal', '📊 ETH Signal', '📊 SOL Signal'], ['📈 STOCKS ▶️', '🔍 Search Coin', '📋 My Positions'], ['📈 Performance', '📊 Global Stats', '🎲 Polymarket'], ['📰 Forex News', '💎 Plans & Pay', '📊 Dashboard']], resize_keyboard: true, persistent: true } };
const SM = { reply_markup: { keyboard: [['📈 AAPL', '📈 TSLA', '📈 NVDA'], ['📈 MSFT', '📈 GOOGL', '📈 AMZN'], ['📈 META', '📈 SPY', '📈 QQQ'], ['📈 AMD', '◀️ BACK TO CRYPTO', '📊 Dashboard']], resize_keyboard: true, persistent: true } };
const COINS = { btc: 'BTCUSDT', eth: 'ETHUSDT', sol: 'SOLUSDT', doge: 'DOGEUSDT', xrp: 'XRPUSDT', ada: 'ADAUSDT', pepe: 'PEPEUSDT', shib: 'SHIBUSDT', bonk: 'BONKUSDT', wif: 'WIFUSDT', aave: 'AAVEUSDT', ltc: 'LTCUSDT', sui: 'SUIUSDT', sei: 'SEIUSDT', inj: 'INJUSDT', tia: 'TIAUSDT', rune: 'RUNEUSDT' };

bot.onText(/\/start/, msg => { const uid = msg.from?.id; if (!isAuth(uid)) return bot.sendMessage(msg.chat.id, '⛔ Unauthorized'); bot.sendMessage(msg.chat.id, '🤖 *K9 SignalBot v7*\n\n🎯 True Multi-TF Consensus\n⏱ 5M•15M•1H•4H real klines\n📊 Alpha Vantage stocks\n🔁 Retry logic on all APIs\n💾 Persistent database\n📡 Free: @k9signalalerts\n\n👇 Menu:', { parse_mode: 'Markdown', ...CM }); });
bot.onText(/\/signal(?:\s+(.+))?/, async (msg, match) => { const uid = msg.from?.id; if (!isAuth(uid)) return; const input = match?.[1]; if (!input) return bot.sendMessage(msg.chat.id, '/signal btc | eth | sol | aapl | tsla | doge', { parse_mode: 'Markdown' }); let sym = input.toUpperCase(); if (TOP_STOCKS.includes(sym)) { const s = await genStockSignal(sym); if (s) bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); return; } if (!sym.endsWith('USDT')) sym = COINS[input.toLowerCase()] || sym + 'USDT'; const s = await genCryptoSignal(sym); if (!s) return bot.sendMessage(msg.chat.id, '⚪ *No Clear Signal*\n\nTrue multi-TF consensus not met.', { parse_mode: 'Markdown' }); bot.sendMessage(msg.chat.id, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '✅ Trade 25%', callback_data: 'exec_' + s.id + '_25' }], [{ text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }, { text: '🛑 SL Hit', callback_data: 'sl_' + s.id }]] } }); });
bot.onText(/\/adduser (\d+)/, (msg, match) => { const uid = msg.from?.id; if (!ADMIN_IDS.includes(uid)) return; const nid = parseInt(match[1]); const cur = process.env.ALLOWED_IDS || ''; const up = cur ? cur + ',' + nid : String(nid); let env = fs.readFileSync('.env', 'utf8'); env = env.includes('ALLOWED_IDS=') ? env.replace(/ALLOWED_IDS=.*/, 'ALLOWED_IDS=' + up) : env + '\nALLOWED_IDS=' + up; fs.writeFileSync('.env', env); ALLOWED_IDS.push(nid); bot.sendMessage(msg.chat.id, '✅ User ' + nid + ' added!'); });

bot.on('message', async msg => { const uid = msg.from?.id; if (!isAuth(uid)) return; const t = msg.text || '', cid = msg.chat.id; if (t.startsWith('/')) return;
  if (TOP_STOCKS.some(s => t.includes(s) && t.includes('📈'))) { const sym = TOP_STOCKS.find(s => t.includes(s)); if (sym) { const s = await genStockSignal(sym); if (s) bot.sendMessage(cid, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }]] } }); } }
  else if (t.includes('BTC Signal') || t.includes('ETH Signal') || t.includes('SOL Signal')) { let sym = 'BTCUSDT'; if (t.includes('ETH')) sym = 'ETHUSDT'; else if (t.includes('SOL')) sym = 'SOLUSDT'; const s = await genCryptoSignal(sym); if (s) bot.sendMessage(cid, fullSignal(s), { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '✅ Trade 10%', callback_data: 'exec_' + s.id + '_10' }, { text: '✅ Trade 25%', callback_data: 'exec_' + s.id + '_25' }], [{ text: '🎯 TP1 Hit', callback_data: 'tp_' + s.id }, { text: '🛑 SL Hit', callback_data: 'sl_' + s.id }]] } }); else bot.sendMessage(cid, '⚪ *No Clear Signal*\n\nTrue multi-TF consensus not met.', { parse_mode: 'Markdown' }); }
  else if (t.includes('STOCKS ▶️')) { bot.sendMessage(cid, '📈 *Stocks — Alpha Vantage API*\n\n⏱ 5M•15M•1H consensus\n✅ RSI•MACD•S/R•Supertrend•EMA\n\nSelect:', { parse_mode: 'Markdown', ...SM }); }
  else if (t.includes('BACK TO CRYPTO')) { bot.sendMessage(cid, '₿ *Crypto — True Multi-TF*\n\n⏱ Each TF from real klines\n✅ 3/4 consensus required\n\nSelect:', { parse_mode: 'Markdown', ...CM }); }
  else if (t.includes('Search Coin')) { bot.sendMessage(cid, '🔍 /signal btc | eth | sol | doge | pepe | aapl | tsla\n\n50+ crypto + 10 stocks', { parse_mode: 'Markdown' }); }
  else if (t.includes('My Positions')) { const st = loadUser(uid); bot.sendMessage(cid, st.positions && st.positions.length > 0 ? '📊 *Positions*\n\n' + st.positions.map((x, i) => (i + 1) + '. ' + x.direction + ' ' + x.symbol + ' @ $' + x.entry).join('\n') : '📊 No open positions.', { parse_mode: 'Markdown' }); }
  else if (t.includes('Performance')) { const s = loadUser(uid).stats, tot = s.w + s.l; bot.sendMessage(cid, '📈 *Your Stats*\nTotal:' + s.t + ' | Wins:' + s.w + ' | Losses:' + s.l + ' | WR:' + (tot > 0 ? ((s.w / tot) * 100).toFixed(1) : '0') + '% | Streak:' + s.s + ' | PnL:' + (s.p >= 0 ? '+' : '') + (s.p || 0).toFixed(1) + '%', { parse_mode: 'Markdown' }); }
  else if (t.includes('Global Stats')) { const g = GS; const tot = g.totalWins + g.totalLosses; bot.sendMessage(cid, '📊 *Global*\nTotal:' + g.totalSignals + ' | Wins:' + g.totalWins + ' | Losses:' + g.totalLosses + ' | WR:' + (tot > 0 ? ((g.totalWins / tot) * 100).toFixed(1) : '--') + '%', { parse_mode: 'Markdown' }); }
  else if (t.includes('Polymarket')) { const pm = await getPolymarket(); if (pm) { let m = '🎲 *Polymarket — Top Markets*\n\n'; pm.forEach((x, i) => { m += (i + 1) + '. *' + x.title.substring(0, 45) + '*\n   Vol: ' + x.volume + ' | Price: ' + (x.prices || ['N/A']).join(' / ') + '\n\n'; }); bot.sendMessage(cid, m, { parse_mode: 'Markdown' }); } else bot.sendMessage(cid, '🎲 Polymarket unavailable.', { parse_mode: 'Markdown' }); }
  else if (t.includes('Forex News')) { const fe = await fetchForex(); bot.sendMessage(cid, '📅 *Forex Today*\n\n' + (fe.length > 0 ? fe.slice(0, 8).map(x => (x.impact === 'High' ? '🔴' : '🟡') + ' ' + x.title + ' (' + x.country + ' ' + x.time + ')\n  F:' + (x.forecast || '--') + ' P:' + (x.previous || '--')).join('\n\n') : 'No major events.\n\n📅 Forex Factory'), { parse_mode: 'Markdown' }); }
  else if (t.includes('Plans & Pay')) { bot.sendMessage(cid, '💎 *Plans — BTC Only*\n\n📅 Monthly — $249\n🗓 Yearly — $1,499\n\n`' + BTC_WALLET + '`\n⚠️ BTC ONLY\n\n📡 Free: @k9signalalerts', { parse_mode: 'Markdown' }); }
  else if (t.includes('Dashboard')) { bot.sendMessage(cid, '📊 *Dashboard*\n\nTap below:', { parse_mode: 'Markdown', reply_markup: { inline_keyboard: [[{ text: '🚀 Open Dashboard', web_app: { url: 'https://telegram-bot-2d4i.onrender.com/dashboard/' + uid } }]] } }); } });

bot.on('callback_query', async q => { const uid = q.from.id; if (!isAuth(uid)) return bot.answerCallbackQuery(q.id); const cid = q.message.chat.id, d = q.data, state = loadUser(uid);
  if (d.startsWith('exec_')) { state.stats.t++; saveUser(uid, state); bot.answerCallbackQuery(q.id, { text: '✅ Trade opened!' }); bot.sendMessage(cid, '✅ *Trade Opened*\nTrack in My Positions', { parse_mode: 'Markdown' }); }
  else if (d.startsWith('tp_')) { state.stats.w++; state.stats.s++; state.stats.p += 1.5; saveUser(uid, state); GS.totalWins++; GS.totalSignals++; saveGS(GS); bot.answerCallbackQuery(q.id, { text: '🎯 TP1 +1.5%' }); bot.sendMessage(cid, '🎯 *TP1 HIT!* +1.5%\nStreak: ' + state.stats.s + ' 🔥\n📊 Global stats updated', { parse_mode: 'Markdown' }); }
  else if (d.startsWith('sl_')) { state.stats.l++; state.stats.s = 0; state.stats.p -= 1.2; saveUser(uid, state); GS.totalLosses++; GS.totalSignals++; saveGS(GS); bot.answerCallbackQuery(q.id, { text: '🛑 SL -1.2%' }); bot.sendMessage(cid, '🛑 *SL Hit* -1.2%\nStreak reset\n📊 Global stats updated', { parse_mode: 'Markdown' }); }
  else { bot.answerCallbackQuery(q.id); } });

fetch("/api/globalstats").then(r=>r.json()).then(g=>{const t=(g.totalWins||0)+(g.totalLosses||0);document.getElementById("globalStats").innerHTML=`<div style="text-align:center"><div style="font-size:32px;font-weight:700;color:var(--accent);">${g.totalSignals||0}</div><div style="font-size:11px;color:var(--text2);">Total Signals</div><div style="margin-top:12px;font-size:14px;"><span class="green">${g.totalWins||0} Wins</span> • <span class="red">${g.totalLosses||0} Losses</span></div><div style="margin-top:4px;font-size:18px;font-weight:600;color:var(--accent);">${t>0?((g.totalWins/t)*100).toFixed(1):"--"}% WR</div></div>`;});
socket.on("priceUpdate",p=>{let h="";for(const[k,v]of Object.entries(p||{}))h+=`<div class="price-row"><span class="price-symbol">${k}</span><span class="price-value">$${(v.price?.toFixed(2)||"--")}</span></div>`;document.getElementById("pricesList").innerHTML=h||"<div class=\"empty-state\"><div>Loading prices...</div></div>";});
</script>
</body>
</html>`);
});

app.get('/dashboard/:uid', (req, res) => {
  if (!isAuth(parseInt(req.params.uid))) return res.status(403).send('Unauthorized');
  res.send('<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"><title>K9 SignalBot</title><script src="https://cdn.socket.io/4.7.2/socket.io.min.js"></script><link href="https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap" rel="stylesheet"><style>:root{--bg:#060b10;--bg2:#0d1520;--bg3:#131d2a;--bg4:#1a2738;--border:rgba(255,255,255,0.06);--text:#e8edf3;--text2:#8899b4;--accent:#F0B90B;--green:#00d4aa;--red:#ff4757;--amber:#f0a840;--radius:12px;--shadow:0 4px 24px rgba(0,0,0,0.3)}*{box-sizing:border-box;margin:0;padding:0}body{background:var(--bg);color:var(--text);font-family:Inter,sans-serif}.app{max-width:1400px;margin:0 auto;padding:20px}.header{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:20px 28px;margin-bottom:20px;display:flex;justify-content:space-between;align-items:center;box-shadow:var(--shadow)}.header h1{font-size:20px;font-weight:700}.badge-live{font-size:11px;padding:6px 14px;border-radius:20px;background:rgba(0,212,170,0.1);color:var(--green);border:1px solid rgba(0,212,170,0.2)}.stats-row{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin-bottom:20px}.stat-card{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:20px;box-shadow:var(--shadow);text-align:center}.stat-value{font-size:32px;font-weight:700}.stat-label{font-size:11px;color:var(--text2);text-transform:uppercase;letter-spacing:1px;margin-top:4px}.grid{display:grid;grid-template-columns:1fr 1fr;gap:14px}.panel{background:var(--bg2);border:1px solid var(--border);border-radius:var(--radius);padding:20px;box-shadow:var(--shadow)}.panel-title{font-size:12px;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--text2);margin-bottom:14px;padding-bottom:12px;border-bottom:1px solid var(--border)}.signal-card{background:var(--bg3);border:1px solid var(--border);border-radius:8px;padding:14px;margin-bottom:10px}.sig-symbol{font-weight:600;font-size:14px}.dir-long{color:var(--green)}.dir-short{color:var(--red)}.sig-details{font-size:11px;color:var(--text2);line-height:1.8;margin-top:6px}.sig-score{font-weight:700;font-size:16px;margin-top:4px}.pos-row,.price-row{display:flex;justify-content:space-between;padding:10px 0;border-bottom:1px solid var(--border);font-size:13px}.green{color:var(--green)}.red{color:var(--red)}.accent{color:var(--accent)}.empty{text-align:center;padding:40px;color:var(--text3)}.pulse{animation:pulse 2s infinite}@keyframes pulse{0%,100%{opacity:1}50%{opacity:0.5}}@media(max-width:768px){.stats-row{grid-template-columns:1fr 1fr}.grid{grid-template-columns:1fr}}</style></head><body><div class="app"><div class="header"><div><h1>🤖 K9 SignalBot</h1><div style="font-size:11px;color:var(--text2);">Professional Trading Dashboard</div></div><span class="badge-live"><span class="pulse" style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);margin-right:6px;"></span>Live</span></div><div class="stats-row"><div class="stat-card"><div class="stat-value green" id="tT">0</div><div class="stat-label">Total Trades</div></div><div class="stat-card"><div class="stat-value green" id="tW">0</div><div class="stat-label">Wins</div></div><div class="stat-card"><div class="stat-value red" id="tL">0</div><div class="stat-label">Losses</div></div><div class="stat-card"><div class="stat-value accent" id="tWR">0%</div><div class="stat-label">Win Rate</div></div></div><div class="grid"><div class="panel"><div class="panel-title">🧠 Active Signals</div><div id="signals"><div class="empty">No signals yet</div></div></div><div class="panel"><div class="panel-title">📊 Open Positions</div><div id="positions"><div class="empty">No positions</div></div></div><div class="panel"><div class="panel-title">🌐 Global Stats</div><div id="global"><div class="empty">Loading...</div></div></div><div class="panel"><div class="panel-title">💰 Live Prices</div><div id="prices"><div class="empty">Loading...</div></div></div></div></div><script>const s=io();s.on("stateUpdate-' + req.params.uid + '",st=>{if(st.stats){const x=st.stats,t=x.w+x.l;document.getElementById("tT").textContent=x.t||0;document.getElementById("tW").textContent=x.w||0;document.getElementById("tL").textContent=x.l||0;document.getElementById("tWR").textContent=(t>0?((x.w/t)*100).toFixed(1):"0")+"%";}if(st.signals){document.getElementById("signals").innerHTML=st.signals.slice(0,8).map(x=>{const stars=x.score>=65?"★★★":x.score>=45?"★★":"★";return "<div class=signal-card><div style=display:flex;justify-content:space-between;align-items:center;><span class=sig-symbol>"+(x.direction=="LONG"?"🟢":"🔴")+" "+x.symbol+" <span class="+(x.direction=="LONG"?"dir-long":"dir-short")+">"+x.direction+"</span></span><span class=sig-score style=color:"+(x.score>=70?"var(--green)":x.score>=45?"var(--accent)":"var(--red)")+">"+stars+" "+x.score+"/95</span></div><div class=sig-details>💰 $"+x.price+" → 🎯 Entry: $"+x.entry+"<br>📈 TP1: $"+x.tp1+" | TP2: $"+x.tp2+" | TP3: $"+x.tp3+"<br>🛑 SL: $"+x.sl+" | R:R 1:"+x.rr1+"<br>📊 RSI: "+x.rsi1h+" | MACD: "+(x.macd?x.macd.trend:"--")+" | S/R: $"+(x.sr?x.sr.support:"--")+"/$"+(x.sr?x.sr.resistance:"--")+"<br>💡 "+(x.reasons||[]).slice(0,4).join(" • ")+"</div></div>";}).join("")||"<div class=empty>No signals yet</div>";}if(st.positions){document.getElementById("positions").innerHTML=st.positions.length?st.positions.map(p=>"<div class=pos-row><span>"+p.direction+" "+p.symbol+" @ $"+p.entry+" ("+p.size+"%)</span><span class="+((p.pnlPercent||0)>=0?"green":"red")+">"+((p.pnlPercent||0)>=0?"+":"")+(p.pnlPercent||0).toFixed(2)+"%</span></div>").join(""):"<div class=empty>No positions</div>";}});fetch("/api/globalstats").then(r=>r.json()).then(g=>{const t=(g.totalWins||0)+(g.totalLosses||0);document.getElementById("global").innerHTML="<div style=text-align:center><div style=font-size:32px;font-weight:700;color:var(--accent);>"+(g.totalSignals||0)+"</div><div style=font-size:11px;color:var(--text2);>Total Signals</div><div style=margin-top:12px;><span class=green>"+(g.totalWins||0)+" Wins</span> • <span class=red>"+(g.totalLosses||0)+" Losses</span></div><div style=margin-top:4px;font-size:18px;font-weight:600;color:var(--accent);>"+(t>0?((g.totalWins/t)*100).toFixed(1):"--")+"% WR</div></div>";});s.on("priceUpdate",p=>{let h="";for(const[k,v]of Object.entries(p||{}))h+="<div class=price-row><span>"+k+"</span><span style=color:var(--accent)>$"+(v.price?.toFixed(2)||"--")+"</span></div>";document.getElementById("prices").innerHTML=h||"<div class=empty>Loading...</div>";});</script></body></html>');
});
