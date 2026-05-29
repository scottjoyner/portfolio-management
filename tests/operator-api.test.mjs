import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { handleRequest, createInitialState } from '../apps/api/src/server.mjs';

function req(method, url, body) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = url;
  if (body !== undefined) stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function json(method, url, body, options = {}) {
  const out = await handleRequest(req(method, url, body), options);
  return { ...out, data: JSON.parse(out.body) };
}

test('readiness stays fail-closed and not production ready', async () => {
  const state = createInitialState();
  const out = await json('GET', '/ready', undefined, { state });
  assert.equal(out.status, 503);
  assert.equal(out.data.productionReady, false);
  assert.equal(out.data.liveTradingCertified, false);
  assert.ok(out.data.blockers.includes('real_execution_disabled'));
});

test('strategy creation validates and persists in request state', async () => {
  const state = createInitialState();
  const created = await json('POST', '/api/strategies', { name: 'Test Strategy', riskLevel: 'low', parameters: { symbol: 'BTC-USD' } }, { state });
  assert.equal(created.status, 201);
  assert.equal(created.data.strategy.name, 'Test Strategy');
  const list = await json('GET', '/api/strategies', undefined, { state });
  assert.ok(list.data.strategies.some(strategy => strategy.name === 'Test Strategy'));
});

test('backtest requires a known strategy and produces deterministic metrics', async () => {
  const state = createInitialState();
  const missing = await json('POST', '/api/backtests', { strategyId: 'missing' }, { state });
  assert.equal(missing.status, 404);
  const strategyId = state.strategies[0].id;
  const created = await json('POST', '/api/backtests', { strategyId, initialCapitalUsd: 50000, feeBps: 5, slippageBps: 10 }, { state });
  assert.equal(created.status, 201);
  assert.equal(created.data.backtest.strategyId, strategyId);
  assert.equal(created.data.backtest.status, 'completed');
  assert.ok(created.data.backtest.metrics.totalTrades > 0);
});

test('approval is blocked without strategy and created with backtest evidence', async () => {
  const state = createInitialState();
  const missing = await json('POST', '/api/approvals', { strategyId: 'missing' }, { state });
  assert.equal(missing.status, 404);
  const strategyId = state.strategies[0].id;
  const created = await json('POST', '/api/approvals', { strategyId, tier: 'canary' }, { state });
  assert.equal(created.status, 201);
  assert.equal(created.data.approval.status, 'pending_review');
});

test('live execution route is explicitly forbidden', async () => {
  const state = createInitialState();
  const out = await json('POST', '/api/execution/live/orders', { side: 'buy' }, { state });
  assert.equal(out.status, 403);
  assert.equal(out.data.error, 'live_execution_disabled');
});

test('kill switch toggles readiness blocker and audit event', async () => {
  const state = createInitialState();
  const enabled = await json('POST', '/api/kill-switch', { enabled: true, reason: 'test' }, { state });
  assert.equal(enabled.status, 200);
  assert.equal(enabled.data.killSwitch.enabled, true);
  const ready = await json('GET', '/ready', undefined, { state });
  assert.ok(ready.data.blockers.includes('kill_switch_enabled'));
  assert.ok(state.audit.some(event => event.action === 'kill_switch_enabled'));
});
