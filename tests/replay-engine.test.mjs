import test from 'node:test';
import assert from 'node:assert/strict';
import { normalizeBar, replayMovingAverageCross, validateHistoricalBars } from '../packages/backtesting/src/replayEngine.mjs';

const bars = Array.from({ length: 12 }, (_, index) => ({
  timestamp: new Date(Date.UTC(2026, 0, index + 1)).toISOString(),
  open: 100 + index,
  high: 101 + index,
  low: 99 + index,
  close: 100 + index,
  volume: 1000 + index
}));

test('normalizes alternate OHLCV bar field names', () => {
  const bar = normalizeBar({ t: '2026-01-01T00:00:00.000Z', o: '1', h: '2', l: '0.5', c: '1.5', v: '10' });
  assert.equal(bar.timestamp, '2026-01-01T00:00:00.000Z');
  assert.equal(bar.close, 1.5);
  assert.equal(bar.volume, 10);
});

test('historical bar validation sorts bars and rejects duplicates', () => {
  const result = validateHistoricalBars([bars[1], bars[0], bars[1]]);
  assert.equal(result.ok, false);
  assert.ok(result.issues.some(issue => issue.issue === 'duplicate_timestamp'));
});

test('moving average replay returns metrics trades and equity curve', () => {
  const result = replayMovingAverageCross({ strategy: { parameters: { fastPeriod: 2, slowPeriod: 4 } }, bars, initialCapitalUsd: 100000, feeBps: 5, slippageBps: 10 });
  assert.equal(result.ok, true);
  assert.equal(result.assumptions.engine, 'moving_average_cross_replay');
  assert.ok(result.equityCurve.length === bars.length);
  assert.ok(Number.isFinite(result.metrics.totalReturnPct));
  assert.ok(result.metrics.totalTrades >= 1);
});

test('moving average replay rejects invalid periods', () => {
  const result = replayMovingAverageCross({ strategy: { parameters: { fastPeriod: 5, slowPeriod: 3 } }, bars });
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('invalid_ma_periods'));
});

test('moving average replay rejects empty bars', () => {
  const result = replayMovingAverageCross({ strategy: { parameters: { fastPeriod: 2, slowPeriod: 4 } }, bars: [] });
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('bars_required'));
});
