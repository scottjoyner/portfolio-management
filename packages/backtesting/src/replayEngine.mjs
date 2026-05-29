export function normalizeBar(bar) {
  const timestamp = bar.timestamp || bar.t || bar.time;
  const open = Number(bar.open ?? bar.o);
  const high = Number(bar.high ?? bar.h);
  const low = Number(bar.low ?? bar.l);
  const close = Number(bar.close ?? bar.c);
  const volume = Number(bar.volume ?? bar.v ?? 0);
  if (!timestamp) throw new Error('bar_timestamp_required');
  for (const [name, value] of Object.entries({ open, high, low, close })) {
    if (!Number.isFinite(value)) throw new Error(`bar_${name}_invalid`);
  }
  return { timestamp, open, high, low, close, volume };
}

export function validateHistoricalBars(bars = []) {
  const normalized = bars.map(normalizeBar).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
  const issues = [];
  for (let i = 1; i < normalized.length; i += 1) {
    if (new Date(normalized[i].timestamp).getTime() === new Date(normalized[i - 1].timestamp).getTime()) issues.push({ index: i, issue: 'duplicate_timestamp' });
    if (new Date(normalized[i].timestamp).getTime() < new Date(normalized[i - 1].timestamp).getTime()) issues.push({ index: i, issue: 'out_of_order' });
  }
  return { ok: issues.length === 0 && normalized.length > 0, bars: normalized, issues };
}

export function movingAverage(values, period) {
  if (values.length < period) return null;
  const slice = values.slice(values.length - period);
  return slice.reduce((sum, value) => sum + value, 0) / period;
}

export function replayMovingAverageCross({ strategy, bars, initialCapitalUsd = 100000, feeBps = 5, slippageBps = 10 }) {
  const validation = validateHistoricalBars(bars);
  if (!validation.ok) return { ok: false, errors: validation.issues.length ? validation.issues.map(issue => issue.issue) : ['bars_required'] };
  const fast = Number(strategy?.parameters?.fastPeriod || 10);
  const slow = Number(strategy?.parameters?.slowPeriod || 30);
  if (fast <= 0 || slow <= 0 || fast >= slow) return { ok: false, errors: ['invalid_ma_periods'] };
  const closes = [];
  const trades = [];
  const equityCurve = [];
  let cash = Number(initialCapitalUsd);
  let quantity = 0;
  let lastSignal = 'flat';

  for (const bar of validation.bars) {
    closes.push(bar.close);
    const fastMa = movingAverage(closes, fast);
    const slowMa = movingAverage(closes, slow);
    if (fastMa && slowMa) {
      const signal = fastMa > slowMa ? 'long' : 'flat';
      if (signal === 'long' && lastSignal !== 'long' && quantity === 0) {
        const price = bar.close * (1 + slippageBps / 10000);
        const notional = cash;
        const fee = notional * feeBps / 10000;
        quantity = (notional - fee) / price;
        cash = 0;
        trades.push({ timestamp: bar.timestamp, side: 'buy', price: Number(price.toFixed(8)), quantity: Number(quantity.toFixed(8)), fee: Number(fee.toFixed(8)) });
      } else if (signal === 'flat' && lastSignal === 'long' && quantity > 0) {
        const price = bar.close * (1 - slippageBps / 10000);
        const gross = quantity * price;
        const fee = gross * feeBps / 10000;
        cash = gross - fee;
        trades.push({ timestamp: bar.timestamp, side: 'sell', price: Number(price.toFixed(8)), quantity: Number(quantity.toFixed(8)), fee: Number(fee.toFixed(8)) });
        quantity = 0;
      }
      lastSignal = signal;
    }
    const equity = cash + quantity * bar.close;
    equityCurve.push({ timestamp: bar.timestamp, equity: Number(equity.toFixed(2)) });
  }

  const finalEquity = equityCurve.at(-1).equity;
  const totalReturnPct = Number((((finalEquity / initialCapitalUsd) - 1) * 100).toFixed(2));
  const peakDrawdowns = equityCurve.reduce((acc, point) => {
    const peak = Math.max(acc.peak, point.equity);
    const drawdown = peak > 0 ? ((peak - point.equity) / peak) * 100 : 0;
    return { peak, maxDrawdownPct: Math.max(acc.maxDrawdownPct, drawdown) };
  }, { peak: initialCapitalUsd, maxDrawdownPct: 0 });

  return {
    ok: true,
    assumptions: { initialCapitalUsd, feeBps, slippageBps, engine: 'moving_average_cross_replay', fastPeriod: fast, slowPeriod: slow, bars: validation.bars.length },
    metrics: { totalReturnPct, finalEquity, maxDrawdownPct: Number(peakDrawdowns.maxDrawdownPct.toFixed(2)), totalTrades: trades.length },
    equityCurve,
    trades
  };
}
