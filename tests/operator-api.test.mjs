import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { mkdtempSync, rmSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { handleRequest, createInitialState } from '../apps/api/src/server.mjs';
import { FileOperatorStore, MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

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

function memoryStore() {
  return new MemoryOperatorStore(createInitialState());
}

test('readiness stays fail-closed and not production ready', async () => {
  const store = memoryStore();
  const out = await json('GET', '/ready', undefined, { store });
  assert.equal(out.status, 503);
  assert.equal(out.data.productionReady, false);
  assert.equal(out.data.liveTradingCertified, false);
  assert.ok(out.data.blockers.includes('real_execution_disabled'));
  assert.ok(out.data.blockers.includes('database_persistence_not_enabled'));
});

test('durable file store changes readiness blocker from missing persistence to sql migrations pending', async () => {
  const root = mkdtempSync(join(tmpdir(), 'operator-api-'));
  const store = new FileOperatorStore(join(root, 'operator-state.json'));
  try {
    const out = await json('GET', '/ready', undefined, { store });
    assert.equal(out.status, 503);
    assert.equal(out.data.storage.durable, true);
    assert.ok(out.data.blockers.includes('sql_database_migrations_pending'));
    assert.equal(out.data.blockers.includes('database_persistence_not_enabled'), false);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test('sql store with migrations avoids sql migrations pending blocker but remains fail-closed', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  store.kind = 'postgres';
  store.durable = true;
  store.sql = true;
  store.migrations = { ok: true, checked: true, applied: ['001_operator_state'] };
  const out = await json('GET', '/ready', undefined, { store });
  assert.equal(out.status, 503);
  assert.equal(out.data.storage.sql, true);
  assert.equal(out.data.blockers.includes('sql_database_migrations_pending'), false);
  assert.equal(out.data.blockers.includes('sql_database_migrations_not_ready'), false);
  assert.ok(out.data.blockers.includes('real_execution_disabled'));
});

test('sql store with missing migrations reports migration blocker', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  store.kind = 'postgres';
  store.durable = true;
  store.sql = true;
  store.migrations = { ok: false, checked: true, applied: [], reason: 'operator_tables_missing' };
  const out = await json('GET', '/ready', undefined, { store });
  assert.equal(out.status, 503);
  assert.ok(out.data.blockers.includes('sql_database_migrations_not_ready'));
});

test('store load error returns 503 for health and API routes', async () => {
  const store = {
    getStatus: () => ({ kind: 'postgres', durable: true, sql: true, migrations: { ok: false } }),
    load: async () => { throw new Error('postgres_migrations_not_ready'); }
  };
  const health = await json('GET', '/health', undefined, { store });
  assert.equal(health.status, 503);
  assert.equal(health.data.ok, false);
  const api = await json('GET', '/api/strategies', undefined, { store });
  assert.equal(api.status, 503);
  assert.equal(api.data.error, 'operator_store_unavailable');
});

test('strategy creation validates and persists in request state', async () => {
  const store = memoryStore();
  const created = await json('POST', '/api/strategies', { name: 'Test Strategy', riskLevel: 'low', parameters: { symbol: 'BTC-USD' } }, { store });
  assert.equal(created.status, 201);
  assert.equal(created.data.strategy.name, 'Test Strategy');
  const list = await json('GET', '/api/strategies', undefined, { store });
  assert.ok(list.data.strategies.some(strategy => strategy.name === 'Test Strategy'));
});

test('backtest requires a known strategy and produces deterministic metrics', async () => {
  const store = memoryStore();
  const state = await store.load();
  const missing = await json('POST', '/api/backtests', { strategyId: 'missing' }, { store });
  assert.equal(missing.status, 404);
  const strategyId = state.strategies[0].id;
  const created = await json('POST', '/api/backtests', { strategyId, initialCapitalUsd: 50000, feeBps: 5, slippageBps: 10 }, { store });
  assert.equal(created.status, 201);
  assert.equal(created.data.backtest.strategyId, strategyId);
  assert.equal(created.data.backtest.status, 'completed');
  assert.ok(created.data.backtest.metrics.totalTrades > 0);
});

test('approval is blocked without strategy and created with backtest evidence', async () => {
  const store = memoryStore();
  const state = await store.load();
  const missing = await json('POST', '/api/approvals', { strategyId: 'missing' }, { store });
  assert.equal(missing.status, 404);
  const strategyId = state.strategies[0].id;
  const created = await json('POST', '/api/approvals', { strategyId, tier: 'canary' }, { store });
  assert.equal(created.status, 201);
  assert.equal(created.data.approval.status, 'pending_review');
});

test('live execution route is explicitly forbidden', async () => {
  const store = memoryStore();
  const out = await json('POST', '/api/execution/live/orders', { side: 'buy' }, { store });
  assert.equal(out.status, 403);
  assert.equal(out.data.error, 'live_execution_disabled');
});

test('kill switch toggles readiness blocker and audit event', async () => {
  const store = memoryStore();
  const enabled = await json('POST', '/api/kill-switch', { enabled: true, reason: 'test' }, { store });
  assert.equal(enabled.status, 200);
  assert.equal(enabled.data.killSwitch.enabled, true);
  const ready = await json('GET', '/ready', undefined, { store });
  const state = await store.load();
  assert.ok(ready.data.blockers.includes('kill_switch_enabled'));
  assert.ok(state.audit.some(event => event.action === 'kill_switch_enabled'));
});
