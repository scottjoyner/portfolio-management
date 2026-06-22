// Paper sweeper: scans all Coinbase markets, runs strategies, generates paper signals

// P&L tracker — ring buffer of sweep results
export const sweepHistory = [];
const MAX_HISTORY = 1000;

async function cli(action, payload = {}) {
  const { spawnSync } = await import('node:child_process');
  const r = spawnSync('node', ['/app/coinbase/src/bridge_cli.mjs', JSON.stringify({ action, payload })], { encoding: 'utf8', timeout: 30000 });
  try { return JSON.parse(r.stdout); } catch { return { ok: false, error: 'parse_failed', raw: r.stdout?.slice(0, 200) }; }
}

export async function fetchAllProducts() {
  const res = await cli('get_products');
  if (!res.ok) return [];
  return res.data.filter(p => p.status === 'online');
}

export async function fetchQuotes() {
  const res = await cli('best_bid_ask');
  if (!res.ok) return {};
  const quotes = {};
  for (const pb of (res.data?.pricebooks || [])) {
    const bid = Number(pb.bids?.[0]?.price || 0);
    const ask = Number(pb.asks?.[0]?.price || 0);
    if (bid > 0 && ask > 0) {
      quotes[pb.product_id] = { bid, ask, mid: (bid + ask) / 2, spreadBps: ((ask - bid) / ask) * 10000 };
    }
  }
  return quotes;
}

export async function fetchCandles(productId, minutesBack = 120) {
  const now = Math.floor(Date.now() / 1000);
  const res = await cli('get_candles', {
    product_id: productId, start_unix: now - minutesBack * 60, end_unix: now, granularity: '5m', limit: Math.ceil(minutesBack / 5),
  });
  if (!res.ok || !res.data?.length) return [];
  return res.data.map(c => ({
    start: Number(c.start), open: Number(c.open), high: Number(c.high),
    low: Number(c.low), close: Number(c.close), volume: Number(c.volume),
  }));
}

export async function fetchProductBook(productId) {
  const res = await cli('get_product_book', { product_id: productId });
  if (!res.ok || !res.data) return null;
  const book = res.data;
  const bids = (book.bids || []).map(b => ({ price: Number(b.price), size: Number(b.size) }));
  const asks = (book.asks || []).map(b => ({ price: Number(b.price), size: Number(b.size) }));
  const bidVol = bids.reduce((s, b) => s + b.size, 0);
  const askVol = asks.reduce((s, b) => s + b.size, 0);
  const imbalance = bidVol + askVol > 0 ? (bidVol - askVol) / (bidVol + askVol) : 0;
  return { productId, bids, asks, bidVol, askVol, imbalance, depthBps: bids.length > 0 && asks.length > 0 ? ((asks[0].price - bids[0].price) / asks[0].price) * 10000 : 0 };
}

// === ANALYSIS FUNCTIONS ===

export function computeVWAP(candles) {
  let volSum = 0, priceVolSum = 0;
  for (const c of candles) {
    const typical = (c.high + c.low + c.close) / 3;
    priceVolSum += typical * c.volume;
    volSum += c.volume;
  }
  return volSum > 0 ? priceVolSum / volSum : 0;
}

function computeBB(candles, period = 10, mult = 2) {
  if (candles.length < period) return null;
  const recent = candles.slice(-period);
  const prices = recent.map(c => c.close);
  const mean = prices.reduce((a, b) => a + b, 0) / prices.length;
  const variance = prices.reduce((sum, p) => sum + (p - mean) ** 2, 0) / prices.length;
  const std = Math.sqrt(variance);
  return { mean, upper: mean + mult * std, lower: mean - mult * std, width: std / mean };
}

export function computeATR(candles, period = 14) {
  if (candles.length < period + 1) return 0;
  let trSum = 0;
  for (let i = candles.length - period; i < candles.length; i++) {
    const c = candles[i];
    const p = candles[i - 1];
    const hl = c.high - c.low;
    const hc = Math.abs(c.high - p.close);
    const lc = Math.abs(c.low - p.close);
    trSum += Math.max(hl, hc, lc);
  }
  return trSum / period;
}

export function detectRegime(candles) {
  if (candles.length < 20) return { regime: 'unknown', strength: 0 };
  const recent = candles.slice(-20);
  const closes = recent.map(c => c.close);
  const returns = [];
  for (let i = 1; i < closes.length; i++) returns.push((closes[i] - closes[i - 1]) / closes[i - 1]);
  const meanRet = returns.reduce((a, b) => a + b, 0) / returns.length;
  const variance = returns.reduce((sum, r) => sum + (r - meanRet) ** 2, 0) / returns.length;
  const vol = Math.sqrt(variance);
  // ADX-like: compare directional movement to noise
  const avgVol = recent.reduce((s, c) => s + c.volume, 0) / recent.length;
  const startP = closes[0], endP = closes[closes.length - 1];
  const totalMove = Math.abs(endP - startP) / startP;
  const trendNoise = totalMove / (vol * Math.sqrt(recent.length) || 0.001);
  // Determine regime
  if (vol > 0.008) return { regime: 'volatile', strength: Math.min(1, vol / 0.02), vol };
  if (trendNoise > 1.5 && meanRet > 0) return { regime: 'trending_up', strength: Math.min(1, trendNoise / 3), vol };
  if (trendNoise > 1.5 && meanRet < 0) return { regime: 'trending_down', strength: Math.min(1, trendNoise / 3), vol };
  if (vol < 0.002) return { regime: 'quiet', strength: Math.min(1, 1 - vol / 0.002), vol };
  return { regime: 'mean_reverting', strength: Math.min(1, 1 / (trendNoise + 0.01) / 2), vol };
}

export function findPivotLevels(candles, window = 5) {
  if (candles.length < window * 2 + 1) return { supports: [], resistances: [] };
  const supports = [];
  const resistances = [];
  for (let i = window; i < candles.length - window; i++) {
    const c = candles[i];
    const leftHi = candles.slice(i - window, i);
    const rightHi = candles.slice(i + 1, i + window + 1);
    if (leftHi.every(l => c.high > l.high) && rightHi.every(r => c.high > r.high)) {
      resistances.push({ price: c.high, strength: Math.min(1, c.volume / 100), index: i });
    }
    if (leftHi.every(l => c.low < l.low) && rightHi.every(r => c.low < r.low)) {
      supports.push({ price: c.low, strength: Math.min(1, c.volume / 100), index: i });
    }
  }
  const recent = candles.slice(-window);
  const nearHi = Math.max(...recent.map(c => c.high));
  const nearLo = Math.min(...recent.map(c => c.low));
  return {
    supports: supports.filter(s => s.price < nearHi * 1.02).sort((a, b) => b.price - a.price).slice(0, 3),
    resistances: resistances.filter(r => r.price > nearLo * 0.98).sort((a, b) => a.price - b.price).slice(0, 3),
  };
}

export function analyzeCandlestickPattern(candles) {
  if (candles.length < 3) return null;
  const c = candles[candles.length - 1];
  const p = candles[candles.length - 2];
  const pp = candles.length >= 3 ? candles[candles.length - 3] : null;

  const body = Math.abs(c.close - c.open);
  const upperWick = c.high - Math.max(c.open, c.close);
  const lowerWick = Math.min(c.open, c.close) - c.low;
  const totalRange = c.high - c.low;
  const pBody = Math.abs(p.close - p.open);
  const pRange = p.high - p.low;

  if (totalRange <= 0) return null;

  const bodyPct = body / totalRange;
  const upperPct = upperWick / totalRange;
  const lowerPct = lowerWick / totalRange;

  // Doji: tiny body
  if (bodyPct < 0.1 && totalRange > 0) {
    const dir = upperWick > lowerWick ? 'sell' : 'buy';
    return { pattern: 'doji', direction: dir, confidence: 0.25, reason: 'doji indecision' };
  }

  // Hammer: small body at top, long lower wick (2x+ body)
  if (lowerPct > 0.6 && bodyPct > 0.05 && bodyPct < 0.4 && upperPct < 0.15) {
    return { pattern: 'hammer', direction: 'buy', confidence: 0.35, reason: 'hammer bottom rejection' };
  }

  // Shooting star: small body at bottom, long upper wick
  if (upperPct > 0.6 && bodyPct > 0.05 && bodyPct < 0.4 && lowerPct < 0.15) {
    return { pattern: 'shooting_star', direction: 'sell', confidence: 0.35, reason: 'shooting star top rejection' };
  }

  // Marubozu: tiny or no wicks
  if (bodyPct > 0.85 && upperPct < 0.08 && lowerPct < 0.08) {
    const dir = c.close > c.open ? 'buy' : 'sell';
    return { pattern: 'marubozu', direction: dir, confidence: 0.3, reason: `marubozu ${dir}` };
  }

  // Bullish engulfing
  if (pp && p.close < p.open && c.close > c.open && c.open < p.close && c.close > p.open) {
    return { pattern: 'bullish_engulfing', direction: 'buy', confidence: 0.4, reason: 'bullish engulfing' };
  }

  // Bearish engulfing
  if (pp && p.close > p.open && c.close < c.open && c.open > p.close && c.close < p.open) {
    return { pattern: 'bearish_engulfing', direction: 'sell', confidence: 0.4, reason: 'bearish engulfing' };
  }

  // Morning star: long bearish, doji/hammer, long bullish
  if (pp && pp.close < pp.open && pBody / pRange < 0.15 && c.close > c.open && c.close > (pp.open + pp.close) / 2) {
    return { pattern: 'morning_star', direction: 'buy', confidence: 0.45, reason: 'morning star reversal' };
  }

  // Evening star: long bullish, doji/hammer, long bearish
  if (pp && pp.close > pp.open && pBody / pRange < 0.15 && c.close < c.open && c.close < (pp.open + pp.close) / 2) {
    return { pattern: 'evening_star', direction: 'sell', confidence: 0.45, reason: 'evening star reversal' };
  }

  // Three white soldiers: 3 consecutive bullish candles with rising closes
  if (pp && p.close > p.open && c.close > c.open && pp.close > pp.open &&
      p.close > pp.close && c.close > p.close &&
      p.open > pp.open && c.open > p.open) {
    return { pattern: 'three_soldiers', direction: 'buy', confidence: 0.4, reason: 'three white soldiers' };
  }

  // Three black crows: 3 consecutive bearish candles with falling closes
  if (pp && p.close < p.open && c.close < c.open && pp.close < pp.open &&
      p.close < pp.close && c.close < p.close &&
      p.open < pp.open && c.open < p.open) {
    return { pattern: 'three_crows', direction: 'sell', confidence: 0.4, reason: 'three black crows' };
  }

  return null;
}

// === SWEEP P&L ===
export function computeSweepPnL() {
  const byStrategy = {};
  for (const entry of sweepHistory) {
    const key = entry.strategy;
    if (!byStrategy[key]) byStrategy[key] = { strategy: key, totalSignals: 0, wins: 0, losses: 0, avgConfidence: 0 };
    byStrategy[key].totalSignals++;
    byStrategy[key].avgConfidence += (entry.confidence - byStrategy[key].avgConfidence) / byStrategy[key].totalSignals;
  }
  return Object.values(byStrategy).sort((a, b) => b.totalSignals - a.totalSignals);
}

// === STRATEGIES ===
function ema(values, period) {
  if (values.length < period) return 0;
  const k = 2 / (period + 1);
  let result = values.slice(0, period).reduce((a, b) => a + b, 0) / period;
  for (let i = period; i < values.length; i++) result = values[i] * k + result * (1 - k);
  return result;
}

function computeRSI(closes, period = 14) {
  if (closes.length < period + 1) return 50;
  let gains = 0, losses = 0;
  for (let i = closes.length - period; i < closes.length; i++) {
    const d = closes[i] - closes[i - 1];
    if (d > 0) gains += d; else losses -= d;
  }
  const avgGain = gains / period, avgLoss = losses / period;
  if (avgLoss === 0) return 100;
  const rs = avgGain / avgLoss;
  return 100 - 100 / (1 + rs);
}

function computeOBV(candles) {
  let obv = 0;
  for (let i = 1; i < candles.length; i++) {
    if (candles[i].close > candles[i - 1].close) obv += candles[i].volume;
    else if (candles[i].close < candles[i - 1].close) obv -= candles[i].volume;
  }
  return obv;
}

function computeMFI(candles, period = 14) {
  if (candles.length < period + 1) return 50;
  let posFlow = 0, negFlow = 0;
  for (let i = candles.length - period; i < candles.length; i++) {
    const typical = (candles[i].high + candles[i].low + candles[i].close) / 3;
    const rawFlow = typical * candles[i].volume;
    if (i > 0 && typical > (candles[i - 1].high + candles[i - 1].low + candles[i - 1].close) / 3) posFlow += rawFlow;
    else negFlow += rawFlow;
  }
  if (negFlow === 0) return 100;
  const mfi = 100 - 100 / (1 + posFlow / negFlow);
  return mfi;
}

function computeHeikinAshi(candles) {
  const ha = [];
  for (let i = 0; i < candles.length; i++) {
    const c = candles[i];
    const prevHa = ha[i - 1];
    const haClose = (c.open + c.high + c.low + c.close) / 4;
    const haOpen = prevHa ? (prevHa.haOpen + prevHa.haClose) / 2 : (c.open + c.close) / 2;
    const haHigh = Math.max(c.high, haOpen, haClose);
    const haLow = Math.min(c.low, haOpen, haClose);
    ha.push({ haOpen, haClose, haHigh, haLow });
  }
  return ha;
}

function computeStochastic(candles, kPeriod = 14, dPeriod = 3) {
  if (candles.length < kPeriod + dPeriod) return { k: 50, d: 50 };
  const ks = [];
  for (let i = candles.length - kPeriod - dPeriod + 1; i < candles.length; i++) {
    const window = candles.slice(i - kPeriod + 1, i + 1);
    const hi = Math.max(...window.map(c => c.high));
    const lo = Math.min(...window.map(c => c.low));
    if (hi === lo) { ks.push(50); continue; }
    ks.push(((candles[i].close - lo) / (hi - lo)) * 100);
  }
  const k = ks[ks.length - 1];
  const d = ks.length >= dPeriod ? ks.slice(-dPeriod).reduce((a, b) => a + b, 0) / dPeriod : 50;
  return { k, d };
}

function detectStreak(candles) {
  if (candles.length < 4) return { direction: 'none', count: 0 };
  const last = candles[candles.length - 1];
  const prev = candles[candles.length - 2];
  const dir = last.close > prev.close ? 'up' : 'down';
  let count = 1;
  for (let i = candles.length - 2; i > Math.max(0, candles.length - 10); i--) {
    const cur = candles[i].close > candles[i - 1].close ? 'up' : 'down';
    if (cur === dir) count++; else break;
  }
  return { direction: count >= 4 ? dir : 'none', count };
}

function computeVolumeProfile(candles, numLevels = 10) {
  if (candles.length < 5) return [];
  const hi = Math.max(...candles.map(c => c.high));
  const lo = Math.min(...candles.map(c => c.low));
  const range = hi - lo;
  if (range <= 0) return [];
  const binSize = range / numLevels;
  const bins = Array.from({ length: numLevels }, (_, i) => ({
    priceLow: lo + i * binSize, priceHigh: lo + (i + 1) * binSize, volume: 0, count: 0,
  }));
  for (const c of candles) {
    const idx = Math.min(Math.floor((c.close - lo) / binSize), numLevels - 1);
    bins[idx].volume += c.volume;
    bins[idx].count++;
  }
  const maxVol = Math.max(...bins.map(b => b.volume));
  return bins.map(b => ({ ...b, volumePct: maxVol > 0 ? b.volume / maxVol : 0, midPrice: (b.priceLow + b.priceHigh) / 2 }));
}

export const STRATEGIES = {
  // === LIQUIDITY / SPREAD STRATEGIES ===
  tightBidAsk: {
    name: 'Tight Bid/Ask',
    run(productId, quote, candles) {
      if (!quote.spreadBps || !quote.mid) return null;
      if (quote.spreadBps < 5) {
        const c = Math.min(0.45, 0.25 + (5 - quote.spreadBps) * 0.04);
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: c, reason: `tight ${quote.spreadBps.toFixed(1)}bps` };
      }
      return null;
    }
  },
  spreadWidening: {
    name: 'Spread Widening',
    run(productId, quote, candles) {
      if (!quote.spreadBps || !quote.mid || candles.length < 8) return null;
      const olderSpread = candles.slice(-3).reduce((s, c) => s + Math.abs(c.high - c.low) / c.close, 0) / 3;
      const curSpread = quote.spreadBps / 10000;
      if (curSpread > olderSpread * 3 && curSpread > 0.005) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.55, 0.2 + curSpread * 50), reason: `spread widen ${(curSpread*10000).toFixed(0)}bps` };
      }
      return null;
    }
  },

  // === MOMENTUM / TREND STRATEGIES ===
  shortMomentum: {
    name: 'Short Momentum',
    run(productId, quote, candles) {
      if (candles.length < 4 || !quote.mid) return null;
      const last = candles.slice(-2);
      const prev = candles.slice(-4, -2);
      if (prev.length < 2) return null;
      const lastAvg = (last[0].close + last[1].close) / 2;
      const prevAvg = (prev[0].close + prev[1].close) / 2;
      if (prevAvg <= 0) return null;
      const chg = (lastAvg - prevAvg) / prevAvg;
      if (chg > 0.003) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.55, 0.15 + chg * 40), reason: `mom up ${(chg*100).toFixed(2)}%` };
      }
      if (chg < -0.003) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.15 + Math.abs(chg) * 40), reason: `mom down ${(Math.abs(chg)*100).toFixed(2)}%` };
      }
      return null;
    }
  },
  vwapOscillator: {
    name: 'VWAP Oscillator',
    run(productId, quote, candles) {
      if (candles.length < 6 || !quote.mid) return null;
      const vwap = computeVWAP(candles);
      if (vwap <= 0) return null;
      const pct = (quote.mid - vwap) / vwap;
      if (Math.abs(pct) < 0.0005) return null;
      const action = pct > 0 ? 'sell' : 'buy';
      return { action, quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.15 + Math.abs(pct) * 80), reason: `vwap ${(pct*100).toFixed(3)}% ${action}` };
    }
  },
  bollingerBounce: {
    name: 'Bollinger Bounce',
    run(productId, quote, candles) {
      if (candles.length < 12 || !quote.mid) return null;
      const bb = computeBB(candles, 10, 2);
      if (!bb || bb.width <= 0) return null;
      if (quote.mid <= bb.lower) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.55, 0.25 + (bb.lower - quote.mid) / quote.mid * 50), reason: `bb lower ${quote.mid.toFixed(6)}` };
      }
      if (quote.mid >= bb.upper) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (quote.mid - bb.upper) / quote.mid * 50), reason: `bb upper ${quote.mid.toFixed(6)}` };
      }
      return null;
    }
  },
  bollingerSqueeze: {
    name: 'Bollinger Squeeze',
    run(productId, quote, candles) {
      if (candles.length < 16 || !quote.mid) return null;
      const recent = computeBB(candles, 10, 2);
      const older = computeBB(candles.slice(0, 10), 10, 2);
      if (!recent || !older || older.width <= 0) return null;
      const squeeze = recent.width / older.width;
      if (squeeze < 0.5) {
        const dir = candles[candles.length - 1].close > candles[candles.length - 3].close ? 'buy' : 'sell';
        return { action: dir, quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (0.5 - squeeze) * 0.6), reason: `bb squeeze ${(squeeze*100).toFixed(0)}%` };
      }
      return null;
    }
  },
  regimeMomentum: {
    name: 'Regime Momentum',
    run(productId, quote, candles, context) {
      if (!context?.regime || candles.length < 4 || !quote.mid) return null;
      const regime = context.regime.regime;
      if (regime !== 'trending_up' && regime !== 'trending_down') return null;
      const chg = (candles[candles.length - 1].close - candles[candles.length - 4].close) / candles[candles.length - 4].close;
      if (regime === 'trending_up' && chg > 0.002) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.6, 0.2 + Math.abs(chg) * 60), reason: `regime ${regime} chg ${(chg*100).toFixed(2)}%` };
      }
      if (regime === 'trending_down' && chg < -0.002) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.6, 0.2 + Math.abs(chg) * 60), reason: `regime ${regime} chg ${(Math.abs(chg)*100).toFixed(2)}%` };
      }
      return null;
    }
  },

  // === VOLUME / ACCUMULATION STRATEGIES ===
  volumeBreakout: {
    name: 'Volume Breakout',
    run(productId, quote, candles) {
      if (candles.length < 8 || !quote.mid) return null;
      const recent3 = candles.slice(-3);
      const prior5 = candles.slice(-8, -3);
      const avgRecent = recent3.reduce((s, c) => s + c.volume, 0) / 3;
      const avgPrior = prior5.reduce((s, c) => s + c.volume, 0) / 5;
      if (avgPrior <= 0 || avgRecent / avgPrior < 1.8) return null;
      const priceRise = recent3[2].close > recent3[0].close;
      return {
        action: priceRise ? 'buy' : 'sell', quantity: 0.001, price: quote.mid,
        confidence: Math.min(0.55, 0.15 + (avgRecent / avgPrior - 1.8) * 0.3),
        reason: `vol ${(avgRecent/avgPrior).toFixed(1)}x ${priceRise ? 'up' : 'down'}`
      };
    }
  },
  accumulationQuiet: {
    name: 'Accumulation Quiet',
    run(productId, quote, candles) {
      if (candles.length < 10 || !quote.mid) return null;
      const recent = candles.slice(-10);
      const prices = recent.map(c => c.close);
      const range = Math.max(...prices) - Math.min(...prices);
      const avgRange = range / Math.max(1, recent.reduce((s, c) => s + c.close, 0) / recent.length);
      if (avgRange > 0.03) return null;
      const vols = recent.slice(-5).map(c => c.volume);
      const volRising = vols[vols.length - 1] > vols[0] && vols[1] > vols[0];
      if (volRising && avgRange < 0.015) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: 0.35, reason: `quiet accum ${(avgRange*100).toFixed(2)}% range` };
      }
      return null;
    }
  },

  // === VOLATILITY / PAIR STRATEGIES ===
  pairRatioMeanRev: {
    name: 'Pair Ratio Mean Rev',
    run(productId, quote, candles, context) {
      if (!productId.endsWith('-BTC')) return null;
      if (candles.length < 12 || !quote.mid || !context?.allQuotes) return null;
      const btcUsd = context.allQuotes['BTC-USD'];
      if (!btcUsd?.mid) return null;
      const prices = candles.map(c => c.close);
      const mean = prices.reduce((a, b) => a + b, 0) / prices.length;
      const std = Math.sqrt(prices.reduce((sum, p) => sum + (p - mean) ** 2, 0) / prices.length);
      if (std <= 0) return null;
      const z = (quote.mid - mean) / std;
      if (z < -1.2) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.6, 0.25 + Math.abs(z) * 0.15), reason: `pair rev z=${z.toFixed(2)} (${productId})` };
      }
      if (z > 1.2) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.55, 0.2 + Math.abs(z) * 0.15), reason: `pair rev z=${z.toFixed(2)} (${productId})` };
      }
      return null;
    }
  },
  btcPairSpread: {
    name: 'BTC Pair Spread',
    run(productId, quote, candles, context) {
      if (!productId.endsWith('-BTC') || !quote.spreadBps) return null;
      if (!context?.allQuotes) return null;
      const btcUsd = context.allQuotes['BTC-USD'];
      if (!btcUsd?.mid) return null;
      if (quote.spreadBps > 20 && quote.spreadBps < 150) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.15 + (quote.spreadBps - 20) * 0.003), reason: `btc pair ${quote.spreadBps.toFixed(0)}bps spread` };
      }
      return null;
    }
  },
  ethBtcRatio: {
    name: 'ETH/BTC Ratio',
    run(productId, quote, candles, context) {
      if (productId !== 'ETH-BTC' || !quote.mid || !context?.allQuotes) return null;
      const btcUsd = context.allQuotes['BTC-USD'];
      const ethUsd = context.allQuotes['ETH-USD'];
      if (!btcUsd?.mid || !ethUsd?.mid) return null;
      const implied = ethUsd.mid / btcUsd.mid;
      const diff = (quote.mid - implied) / implied;
      if (Math.abs(diff) < 0.001) return null;
      const action = diff > 0 ? 'sell' : 'buy';
      return { action, quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + Math.abs(diff) * 50), reason: `ETHBTC ${(diff*10000).toFixed(1)}bp arb` };
    }
  },

  // === CANDLESTICK PATTERNS ===
  candlePatterns: {
    name: 'Candlestick Patterns',
    run(productId, quote, candles) {
      const pattern = analyzeCandlestickPattern(candles);
      if (!pattern) return null;
      return {
        action: pattern.direction, quantity: 0.001, price: quote.mid,
        confidence: pattern.confidence, reason: pattern.reason,
      };
    }
  },

  // === ORDER BOOK IMBALANCE ===
  orderBookImbalance: {
    name: 'Order Book Imbalance',
    run(productId, quote, candles, context) {
      const book = context?.orderBooks?.[productId];
      if (!book || Math.abs(book.imbalance) < 0.3) return null;
      const action = book.imbalance > 0 ? 'buy' : 'sell';
      return { action, quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.15 + Math.abs(book.imbalance) * 0.35), reason: `book ${(book.imbalance*100).toFixed(0)}% ${action} pressure` };
    }
  },

  // === PIVOT POINTS ===
  pivotBounce: {
    name: 'Pivot Bounce',
    run(productId, quote, candles, context) {
      if (!context?.pivots || !quote.mid) return null;
      const { supports, resistances } = context.pivots;
      for (const s of supports) {
        if (quote.mid > 0 && Math.abs(quote.mid - s.price) / quote.mid < 0.005) {
          return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + s.strength * 0.25), reason: `support ${s.price.toFixed(6)}` };
        }
      }
      for (const r of resistances) {
        if (quote.mid > 0 && Math.abs(quote.mid - r.price) / quote.mid < 0.005) {
          return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + r.strength * 0.25), reason: `resistance ${r.price.toFixed(6)}` };
        }
      }
      return null;
    }
  },

  // === STOP-LOSS / POSITION EXIT ===
  stopLoss: {
    name: 'Stop Loss',
    run(productId, quote, candles, context) {
      const positions = context?.positions || [];
      const pos = positions.find(p => p.symbol === productId && p.status === 'open');
      if (!pos || !quote.mid || pos.quantity === 0) return null;
      const entry = Number(pos.averagePrice);
      if (entry <= 0) return null;
      const pnlPct = (quote.mid - entry) / entry * (pos.side === 'sell' ? -1 : 1);
      // Hard stop at -5%
      if (pnlPct < -0.05) {
        return { action: pos.side === 'buy' ? 'sell' : 'buy', quantity: Math.abs(pos.quantity), price: quote.mid, confidence: 0.9, reason: `hard stop ${(pnlPct*100).toFixed(1)}%` };
      }
      // Trailing stop: if was up >3%, stop at -1.5% from peak
      if (pnlPct > 0.03 && context.positionPeaks?.[productId]) {
        const peak = context.positionPeaks[productId];
        const drawdown = (quote.mid - peak) / peak;
        if (drawdown < -0.015) {
          return { action: pos.side === 'buy' ? 'sell' : 'buy', quantity: Math.abs(pos.quantity), price: quote.mid, confidence: 0.7, reason: `trailing stop ${(drawdown*100).toFixed(1)}% from peak` };
        }
      }
      return null;
    }
  },

  // === EXECUTION / PROFIT SWEEP ===
  sweepToBtc: {
    name: 'Sweep to BTC',
    run(productId, quote, candles, context) {
      if (productId.endsWith('-BTC') || productId === 'BTC-USD' || productId === 'BTC-USDC') return null;
      if (candles.length < 4 || !quote.mid || !context?.allQuotes) return null;
      const btcQ = context.allQuotes['BTC-USD'];
      if (!btcQ?.mid) return null;
      const rise = (candles[candles.length - 1].close - candles[candles.length - 4].close) / candles[candles.length - 4].close;
      if (rise > 0.015) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.15 + rise * 10), reason: `profit sweep ${(rise*100).toFixed(2)}% gain → BTC` };
      }
      return null;
    }
  },
  buyBtcDip: {
    name: 'Buy BTC Dip',
    run(productId, quote, candles) {
      if (productId !== 'BTC-USD' && productId !== 'BTC-USDC') return null;
      if (candles.length < 6 || !quote.mid) return null;
      const prices = candles.map(c => c.close);
      const mean = prices.reduce((a, b) => a + b, 0) / prices.length;
      const pct = (quote.mid - mean) / mean;
      if (pct < -0.005) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.55, 0.2 + Math.abs(pct) * 20), reason: `btc dip ${(Math.abs(pct)*100).toFixed(2)}%` };
      }
      return null;
    }
  },
  usdcPark: {
    name: 'USDC Park',
    run(productId, quote) {
      if (productId !== 'USDC-USD') return null;
      if (!quote.bid || !quote.ask) return null;
      const drift = (quote.bid - 1) * 10000;
      if (Math.abs(drift) > 2) {
        return { action: drift > 0 ? 'sell' : 'buy', quantity: 1, price: quote.mid, confidence: Math.min(0.6, 0.3 + Math.abs(drift) * 0.05), reason: `USDC peg ${drift.toFixed(1)}bps` };
      }
      return null;
    }
  },

  // === OSCILLATORS ===
  rsiOversold: {
    name: 'RSI Oversold/Overbought',
    run(productId, quote, candles) {
      if (candles.length < 16 || !quote.mid) return null;
      const closes = candles.map(c => c.close);
      const rsi = computeRSI(closes, 14);
      if (rsi < 30) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.55, 0.25 + (30 - rsi) * 0.015), reason: `RSI ${rsi.toFixed(1)} oversold` };
      }
      if (rsi > 70) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (rsi - 70) * 0.015), reason: `RSI ${rsi.toFixed(1)} overbought` };
      }
      return null;
    }
  },
  macdCross: {
    name: 'MACD Crossover',
    run(productId, quote, candles) {
      if (candles.length < 27 || !quote.mid) return null;
      const closes = candles.map(c => c.close);
      const macdPeriod = 12, signalPeriod = 9;
      const ema12 = ema(closes, macdPeriod);
      const ema26 = ema(closes, 26);
      const macdLine = ema12 - ema26;
      const macdValues = [];
      for (let i = 26; i < closes.length; i++) {
        const e12 = ema(closes.slice(0, i + 1), 12);
        const e26 = ema(closes.slice(0, i + 1), 26);
        macdValues.push(e12 - e26);
      }
      const signalLine = ema(macdValues, signalPeriod);
      const prevMacd = macdValues.length >= 2 ? macdValues[macdValues.length - 2] : macdLine;
      const prevSignal = macdValues.length >= 10 ? ema(macdValues.slice(0, -1), signalPeriod) : signalLine;
      if (prevMacd <= prevSignal && macdLine > signalLine) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: 0.4, reason: `MACD bullish cross ${macdLine.toFixed(4)}/${signalLine.toFixed(4)}` };
      }
      if (prevMacd >= prevSignal && macdLine < signalLine) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: 0.4, reason: `MACD bearish cross ${macdLine.toFixed(4)}/${signalLine.toFixed(4)}` };
      }
      return null;
    }
  },
  moneyFlowIndex: {
    name: 'Money Flow Index',
    run(productId, quote, candles) {
      if (candles.length < 16 || !quote.mid) return null;
      const mfi = computeMFI(candles, 14);
      if (mfi < 20) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (20 - mfi) * 0.01), reason: `MFI ${mfi.toFixed(1)} oversold` };
      }
      if (mfi > 80) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.15 + (mfi - 80) * 0.01), reason: `MFI ${mfi.toFixed(1)} overbought` };
      }
      return null;
    }
  },

  // === CHANNEL / VOLATILITY ===
  keltnerBreakout: {
    name: 'Keltner Breakout',
    run(productId, quote, candles) {
      if (candles.length < 22 || !quote.mid) return null;
      const closes = candles.map(c => c.close);
      const atrVal = computeATR(candles, 20);
      const mean20 = closes.slice(-20).reduce((a, b) => a + b, 0) / 20;
      if (atrVal <= 0 || mean20 <= 0) return null;
      const upper = mean20 + 2 * atrVal;
      const lower = mean20 - 2 * atrVal;
      if (quote.mid >= upper) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + (quote.mid - upper) / quote.mid * 50), reason: `keltner upper ${upper.toFixed(6)}` };
      }
      if (quote.mid <= lower) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + (lower - quote.mid) / quote.mid * 50), reason: `keltner lower ${lower.toFixed(6)}` };
      }
      return null;
    }
  },
  donchianBreakout: {
    name: 'Donchian Breakout',
    run(productId, quote, candles) {
      if (candles.length < 20 || !quote.mid) return null;
      const recent = candles.slice(-20);
      const hi = Math.max(...recent.map(c => c.high));
      const lo = Math.min(...recent.map(c => c.low));
      const prevHi = Math.max(...recent.slice(0, -1).map(c => c.high));
      const prevLo = Math.min(...recent.slice(0, -1).map(c => c.low));
      if (quote.mid > hi && hi > prevHi) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (quote.mid - hi) / hi * 100), reason: `donchian breakout ${hi.toFixed(6)}` };
      }
      if (quote.mid < lo && lo < prevLo) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (lo - quote.mid) / lo * 100), reason: `donchian breakdown ${lo.toFixed(6)}` };
      }
      return null;
    }
  },
  elderBullBear: {
    name: 'Elder Bull/Bear',
    run(productId, quote, candles) {
      if (candles.length < 14 || !quote.mid) return null;
      const closes = candles.map(c => c.close);
      const ema13 = ema(closes, 13);
      if (ema13 <= 0) return null;
      const last = candles[candles.length - 1];
      const bullPower = last.high - ema13;
      const bearPower = last.low - ema13;
      if (bullPower > 0 && bullPower > (candles[candles.length - 2]?.high - ema(closes.slice(0, -1), 13) || 0)) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + bullPower / ema13 * 50), reason: `bull power ${bullPower.toFixed(6)}` };
      }
      if (bearPower < 0 && bearPower < (candles[candles.length - 2]?.low - ema(closes.slice(0, -1), 13) || 0)) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + Math.abs(bearPower) / ema13 * 50), reason: `bear power ${bearPower.toFixed(6)}` };
      }
      return null;
    }
  },

  // === VOLUME / FLOW ===
  obvDivergence: {
    name: 'OBV Divergence',
    run(productId, quote, candles) {
      if (candles.length < 12 || !quote.mid) return null;
      const half = Math.floor(candles.length / 2);
      const first = candles.slice(0, half);
      const second = candles.slice(half);
      const obvFirst = computeOBV(first);
      const obvSecond = computeOBV(second);
      const priceFirst = (first[0].close + first[first.length - 1].close) / 2;
      const priceSecond = (second[0].close + second[second.length - 1].close) / 2;
      const priceRise = priceSecond > priceFirst;
      const obvRise = obvSecond > obvFirst;
      if (priceRise && !obvRise) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: 0.4, reason: 'OBV divergence (price up, volume down)' };
      }
      if (!priceRise && obvRise) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: 0.4, reason: 'OBV divergence (price down, volume up)' };
      }
      return null;
    }
  },
  volumeRsi: {
    name: 'Volume RSI',
    run(productId, quote, candles) {
      if (candles.length < 16 || !quote.mid) return null;
      const closes = candles.map(c => c.close);
      const rsi = computeRSI(closes, 14);
      const recent3 = candles.slice(-3);
      const prior3 = candles.slice(-6, -3);
      const avgRecent = recent3.reduce((s, c) => s + c.volume, 0) / 3;
      const avgPrior = prior3.reduce((s, c) => s + c.volume, 0) / 3;
      if (avgPrior <= 0 || avgRecent / avgPrior < 1.5) return null;
      if (rsi < 35) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.55, 0.25 + (35 - rsi) * 0.01), reason: `vol RSI ${rsi.toFixed(1)} oversold` };
      }
      if (rsi > 65) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (rsi - 65) * 0.01), reason: `vol RSI ${rsi.toFixed(1)} overbought` };
      }
      return null;
    }
  },

  // === CROSS-ASSET ===
  correlationDivergence: {
    name: 'Correlation Divergence',
    run(productId, quote, candles, context) {
      if (productId !== 'ETH-USD' && productId !== 'ETH-USDC') return null;
      if (candles.length < 12 || !quote.mid || !context?.allQuotes) return null;
      const btcQ = context.allQuotes['BTC-USD'];
      if (!btcQ?.mid) return null;
      const ratio = quote.mid / btcQ.mid;
      const closes = candles.map(c => c.close);
      const btcPrices = []; // use ETH candles but compute ETH/BTC ratio over time
      const ratios = candles.map(c => {
        const estBtc = btcQ.mid; // approximate
        return c.close / estBtc;
      });
      const meanRatio = ratios.reduce((a, b) => a + b, 0) / ratios.length;
      const stdRatio = Math.sqrt(ratios.reduce((sum, r) => sum + (r - meanRatio) ** 2, 0) / ratios.length);
      if (stdRatio <= 0) return null;
      const z = (ratio - meanRatio) / stdRatio;
      if (z < -1.5) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + Math.abs(z) * 0.1), reason: `ETH/BTC z=${z.toFixed(2)} cheap` };
      }
      if (z > 1.5) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.15 + Math.abs(z) * 0.1), reason: `ETH/BTC z=${z.toFixed(2)} expensive` };
      }
      return null;
    }
  },
  gapReversion: {
    name: 'Gap Reversion',
    run(productId, quote, candles) {
      if (candles.length < 4 || !quote.mid) return null;
      const lastCandle = candles[candles.length - 1];
      const gap = quote.mid - lastCandle.close;
      const gapPct = Math.abs(gap) / lastCandle.close;
      if (gapPct < 0.001 || gapPct > 0.05) return null;
      const action = gap > 0 ? 'sell' : 'buy';
      const c = Math.min(0.45, 0.15 + gapPct * 20);
      return { action, quantity: 0.001, price: quote.mid, confidence: c, reason: `gap ${(gapPct*100).toFixed(2)}% reversion` };
    }
  },

  // === STOCHASTIC ===
  stochastic: {
    name: 'Stochastic',
    run(productId, quote, candles) {
      if (candles.length < 20 || !quote.mid) return null;
      const { k, d } = computeStochastic(candles, 14, 3);
      if (k < 20 && d < 20 && k > d) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.25 + (20 - k) * 0.01), reason: `stoch ${k.toFixed(1)}/${d.toFixed(1)} oversold cross` };
      }
      if (k > 80 && d > 80 && k < d) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + (k - 80) * 0.01), reason: `stoch ${k.toFixed(1)}/${d.toFixed(1)} overbought cross` };
      }
      return null;
    }
  },

  // === HEIKIN-ASHI TREND ===
  heikinAshiTrend: {
    name: 'Heikin-Ashi Trend',
    run(productId, quote, candles) {
      if (candles.length < 6 || !quote.mid) return null;
      const ha = computeHeikinAshi(candles);
      let upCount = 0, downCount = 0;
      for (let i = ha.length - 1; i >= Math.max(0, ha.length - 5); i--) {
        if (ha[i].haClose > ha[i].haOpen) upCount++; else downCount++;
      }
      if (upCount >= 4) {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + upCount * 0.07), reason: `HA ${upCount} green` };
      }
      if (downCount >= 4) {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + downCount * 0.07), reason: `HA ${downCount} red` };
      }
      return null;
    }
  },

  // === STREAK EXHAUSTION ===
  streakExhaustion: {
    name: 'Streak Exhaustion',
    run(productId, quote, candles) {
      if (!quote.mid) return null;
      const streak = detectStreak(candles);
      if (streak.count < 5) return null;
      const action = streak.direction === 'up' ? 'sell' : 'buy';
      return { action, quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + (streak.count - 4) * 0.08), reason: `${streak.count}-${streak.direction} streak exhaustion` };
    }
  },

  // === VOLUME PROFILE ===
  volumeProfileBounce: {
    name: 'Volume Profile',
    run(productId, quote, candles) {
      if (candles.length < 10 || !quote.mid) return null;
      const profile = computeVolumeProfile(candles, 10);
      const strongLevels = profile.filter(b => b.volumePct > 0.6);
      for (const level of strongLevels) {
        const pct = Math.abs(quote.mid - level.midPrice) / quote.mid;
        if (pct < 0.005) {
          const action = quote.mid > level.midPrice ? 'sell' : 'buy';
          return { action, quantity: 0.001, price: quote.mid, confidence: Math.min(0.4, 0.15 + level.volumePct * 0.25), reason: `vol profile ${level.midPrice.toFixed(6)} ${action}` };
        }
      }
      return null;
    }
  },

  // === VOLATILITY PERCENTILE ===
  volPercentile: {
    name: 'Volatility Percentile',
    run(productId, quote, candles, context) {
      if (candles.length < 20 || !quote.mid || !context?.atrHistory) return null;
      const atr = context.atr;
      const pct = computeVolatilityPercentile(atr, context.atrHistory);
      if (pct > 85 && (context.regime?.regime === 'mean_reverting' || context.regime?.regime === 'unknown')) {
        const recentDir = candles[candles.length - 1].close > candles[candles.length - 3].close ? 'buy' : 'sell';
        const action = recentDir === 'buy' ? 'sell' : 'buy'; // fade the move in high vol
        return { action, quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.15 + (pct - 85) * 0.02), reason: `vol ${pct.toFixed(0)}pct ${action}` };
      }
      if (pct < 15) {
        // Low vol = expect breakout in trend direction
        const dir = context.regime?.regime === 'trending_up' ? 'buy' : context.regime?.regime === 'trending_down' ? 'sell' : null;
        if (dir) {
          return { action: dir, quantity: 0.001, price: quote.mid, confidence: Math.min(0.45, 0.2 + (15 - pct) * 0.01), reason: `vol squeeze ${pct.toFixed(0)}pct ${dir}` };
        }
      }
      return null;
    }
  },

  // === MULTI-TIMEFRAME TREND ===
  multiTimeframeTrend: {
    name: 'Multi-TF Trend',
    run(productId, quote, candles, context) {
      if (!context?.tfTrend || !quote.mid) return null;
      const tf = context.tfTrend;
      if (tf === 'up') {
        return { action: 'buy', quantity: 0.001, price: quote.mid, confidence: 0.35, reason: '1h trend up, 5m confirming' };
      }
      if (tf === 'down') {
        return { action: 'sell', quantity: 0.001, price: quote.mid, confidence: 0.35, reason: '1h trend down, 5m confirming' };
      }
      return null;
    }
  },

  // === CROSS-QUOTE ARB ===
  crossQuoteArb: {
    name: 'Cross-Quote Arb',
    run(productId, quote, candles, context) {
      if (!quote.mid || !context?.allQuotes) return null;
      const base = productId.split('-')[0];
      const quoteCurrency = productId.split('-')[1];
      const otherCurrency = quoteCurrency === 'USD' ? 'USDC' : quoteCurrency === 'USDC' ? 'USD' : null;
      if (!otherCurrency) return null;
      const otherPair = base + '-' + otherCurrency;
      const otherQ = context.allQuotes[otherPair];
      if (!otherQ?.mid) return null;
      const drift = (quote.mid - otherQ.mid) / otherQ.mid;
      if (Math.abs(drift) < 0.0002) return null;
      const action = drift > 0 ? 'sell' : 'buy';
      return { action, quantity: 0.001, price: quote.mid, confidence: Math.min(0.5, 0.2 + Math.abs(drift) * 800), reason: `${(drift*10000).toFixed(1)}bp arb ${otherPair}` };
    }
  },
};

export async function runSweep(options = {}) {
  const strategies = options.strategies || Object.keys(STRATEGIES);
  const maxMarkets = options.maxMarkets || 100;
  const includePairs = options.includePairs || ['USD', 'USDC', 'BTC', 'EUR'];
  const openPositions = options.positions || [];
  const fetchBooks = options.fetchOrderBooks || false;

  const products = await fetchAllProducts();
  const targets = products.filter(p => includePairs.some(q => p.product_id.endsWith('-' + q)));
  const limited = targets.slice(0, maxMarkets);
  const quotes = await fetchQuotes();

  // Pre-compute position peak tracking for trailing stops
  const positionPeaks = {};
  for (const pos of openPositions) {
    if (pos.status === 'open' && pos.symbol) {
      const q = quotes[pos.symbol];
      if (q?.mid > Number(pos.averagePrice || 0)) {
        positionPeaks[pos.symbol] = Math.max(q.mid, positionPeaks[pos.symbol] || 0);
      }
    }
  }

  const results = [];
  const sweepTime = new Date();
  const atrHistory = []; // collected across all products for volatility percentile ranking

  for (const product of limited) {
    const pid = product.product_id;
    const quote = quotes[pid];
    if (!quote) continue;
    const candles = await fetchCandles(pid, 120);
    if (!candles.length) continue;

    // Analysis layer: regime, pivots, ATR
    const regime = detectRegime(candles);
    const pivots = findPivotLevels(candles);
    const atr = computeATR(candles);
    const atrPct = atr > 0 && quote.mid > 0 ? atr / quote.mid : 0;
    if (atr > 0) atrHistory.push(atr);

    // Scale quantity by ATR (inverse: high vol = smaller size)
    const baseSize = 0.001;
    const atrSize = atrPct > 0 ? baseSize * (0.003 / Math.max(atrPct, 0.0005)) : baseSize;
    const quantity = Math.min(baseSize * 5, Math.max(baseSize * 0.1, atrSize));

    // Multi-timeframe: fetch 1h candles for trend confirmation
    let tfTrend = null;
    const hourlyCandles = await fetchCandles(pid, 240);
    if (hourlyCandles.length >= 4) {
      const hourlyCloses = hourlyCandles.map(c => c.close);
      const emaFast = ema(hourlyCloses, 4);
      const emaSlow = ema(hourlyCloses, 8);
      const prevFast = ema(hourlyCloses.slice(0, -1), 4);
      const prevSlow = ema(hourlyCloses.slice(0, -1), 8);
      if (emaFast > emaSlow && prevFast <= prevSlow) tfTrend = 'up';
      else if (emaFast < emaSlow && prevFast >= prevSlow) tfTrend = 'down';
    }

    const context = {
      allQuotes: quotes,
      regime,
      pivots,
      atr,
      atrPct,
      atrHistory,
      tfTrend,
      positions: openPositions,
      positionPeaks,
      orderBooks: {},
    };

    for (const sName of strategies) {
      const strategy = STRATEGIES[sName];
      if (!strategy) continue;
      try {
        const signal = strategy.run(pid, quote, candles, context);
        if (signal) {
          const finalQty = signal.quantity === baseSize || signal.quantity === 1 ? Number(quantity.toFixed(8)) : signal.quantity;
          results.push({
            productId: pid, strategy: sName, strategyName: strategy.name,
            timestamp: sweepTime.toISOString(), ...signal, quantity: finalQty,
            regime: regime.regime, atrPct: Number(atrPct.toFixed(6)),
          });
        }
      } catch { /* skip */ }
    }
  }

  // === Signal stacking: consensus meta-signals ===
  if (!options.strategies || options.strategies.includes('signalConsensus')) {
    const byProduct = {};
    for (const r of results) {
      if (!byProduct[r.productId]) byProduct[r.productId] = { buys: 0, sells: 0, signals: [] };
      if (r.action === 'buy') byProduct[r.productId].buys++;
      else if (r.action === 'sell') byProduct[r.productId].sells++;
      byProduct[r.productId].signals.push(r);
    }
    for (const [pid, data] of Object.entries(byProduct)) {
      const total = data.buys + data.sells;
      if (total >= 3) {
        const dominant = data.buys > data.sells ? 'buy' : 'sell';
        const ratio = data.buys > data.sells ? data.buys / total : data.sells / total;
        if (ratio >= 0.6) {
          const avgConf = data.signals.reduce((s, x) => s + x.confidence, 0) / total;
          results.push({
            productId: pid, strategy: 'signalConsensus', strategyName: 'Signal Consensus',
            timestamp: sweepTime.toISOString(), action: dominant, quantity: 0.001,
            price: data.signals[0].price, confidence: Math.min(0.7, avgConf + ratio * 0.1),
            reason: `${data.buys}B/${data.sells}S consensus ${dominant}`,
            regime: data.signals[0].regime, atrPct: data.signals[0].atrPct,
          });
        }
      }
    }
  }

  // === Cross-quote arb signals (systematic for every pair) ===
  if (!options.strategies || options.strategies.includes('crossQuoteArb')) {
    const bases = {};
    for (const pid of targets.map(p => p.product_id)) {
      const parts = pid.split('-');
      if (parts.length !== 2) continue;
      if (!bases[parts[0]]) bases[parts[0]] = [];
      bases[parts[0]].push(parts[1]);
    }
    for (const [base, currencies] of Object.entries(bases)) {
      if (currencies.includes('USD') && currencies.includes('USDC')) {
        const usdQ = quotes[base + '-USD'];
        const usdcQ = quotes[base + '-USDC'];
        if (usdQ?.mid && usdcQ?.mid) {
          const drift = (usdQ.mid - usdcQ.mid) / usdcQ.mid;
          if (Math.abs(drift) >= 0.0002) {
            const action = drift > 0 ? 'sell' : 'buy';
            const side = drift > 0 ? 'USD' : 'USDC';
            results.push({
              productId: base + '-USD', strategy: 'crossQuoteArb', strategyName: 'Cross-Quote Arb',
              timestamp: sweepTime.toISOString(), action, quantity: 0.001,
              price: usdQ.mid,
              confidence: Math.min(0.5, 0.2 + Math.abs(drift) * 800),
              reason: `${(drift*10000).toFixed(1)}bp arb ${base}-${side}`,
              regime: 'n/a', atrPct: 0,
            });
          }
        }
      }
    }
  }

  // Fetch order books for products that had signals (if enabled)
  if (fetchBooks) {
    const signalProductIds = [...new Set(results.map(r => r.productId))];
    for (const pid of signalProductIds.slice(0, 20)) {
      const book = await fetchProductBook(pid);
      if (book) {
        const q = quotes[pid];
        if (q) q.bookImbalance = book.imbalance;
        // Re-run orderBookImbalance for products that had order book data
        const s = STRATEGIES.orderBookImbalance;
        if (s && strategies.includes('orderBookImbalance')) {
          try {
            const sig = s.run(pid, quotes[pid], null, { orderBooks: { [pid]: book }, allQuotes: quotes });
            if (sig) {
              results.push({
                productId: pid, strategy: 'orderBookImbalance', strategyName: s.name,
                timestamp: sweepTime.toISOString(), ...sig,
                regime: 'n/a', atrPct: 0,
              });
            }
          } catch { /* skip */ }
        }
      }
    }
  }

  results.sort((a, b) => b.confidence - a.confidence);

  // Record to sweep history for P&L tracking
  const snapshot = { timestamp: sweepTime.toISOString(), signalCount: results.length, strategies: {} };
  for (const r of results) {
    snapshot.strategies[r.strategy] = (snapshot.strategies[r.strategy] || 0) + 1;
  }
  sweepHistory.push(snapshot);
  if (sweepHistory.length > MAX_HISTORY) sweepHistory.splice(0, sweepHistory.length - MAX_HISTORY);

  return { ok: true, scanned: limited.length, quoted: Object.keys(quotes).length, signals: results.slice(0, 200), history: { totalSweeps: sweepHistory.length, lastSweep: snapshot } };
}
