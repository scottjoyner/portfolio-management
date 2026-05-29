import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function req(method, url, body) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = url;
  if (body !== undefined) stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function json(method, url, body, store = new MemoryOperatorStore(createInitialState())) {
  const out = await handleRequest(req(method, url, body), { store });
  return { ...out, data: JSON.parse(out.body), store };
}

test('lists P1 product primitives', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const accounts = await json('GET', '/api/accounts', undefined, store);
  const instruments = await json('GET', '/api/instruments', undefined, store);
  const templates = await json('GET', '/api/strategy-templates', undefined, store);
  assert.equal(accounts.status, 200);
  assert.ok(accounts.data.accounts.length >= 1);
  assert.ok(instruments.data.instruments.some(i => i.symbol === 'BTC-USD'));
  assert.ok(templates.data.templates.some(t => t.id === 'template-ema-crossover'));
});

test('creates strategy from template and clones version', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const created = await json('POST', '/api/strategies/from-template', { templateId: 'template-ema-crossover', name: 'Template Strategy', parameters: { symbol: 'BTC-USD', timeframe: '1h', fastPeriod: 10, slowPeriod: 30 } }, store);
  assert.equal(created.status, 201);
  assert.equal(created.data.strategy.version, 1);
  const cloned = await json('POST', `/api/strategies/${created.data.strategy.id}/clone`, { parameters: { fastPeriod: 12 } }, store);
  assert.equal(cloned.status, 201);
  assert.equal(cloned.data.strategy.version, 2);
  assert.equal(cloned.data.strategy.parentStrategyId, created.data.strategy.id);
});

test('runs backtest, returns report, requests and decides approval', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const strategyId = (await store.load()).strategies[0].id;
  const backtest = await json('POST', '/api/backtests/run', { strategyId, initialCapitalUsd: 75000 }, store);
  assert.equal(backtest.status, 201);
  const report = await json('GET', `/api/backtests/${backtest.data.backtest.id}/report`, undefined, store);
  assert.equal(report.status, 200);
  assert.ok(report.data.metrics.totalTrades > 0);
  const approval = await json('POST', '/api/approvals/request', { strategyId, backtestId: backtest.data.backtest.id, tier: 'canary' }, store);
  assert.equal(approval.status, 201);
  const decision = await json('POST', `/api/approvals/${approval.data.approval.id}/decision`, { status: 'approved', reviewer: 'test' }, store);
  assert.equal(decision.status, 200);
  assert.equal(decision.data.approval.status, 'approved');
});

test('paper execution requires approval and can be stopped', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const state = await store.load();
  const strategyId = state.strategies[0].id;
  const blocked = await json('POST', '/api/paper-executions', { strategyId }, store);
  assert.equal(blocked.status, 400);
  assert.ok(blocked.data.errors.includes('approval_required'));
  state.approvals[0].status = 'approved';
  await store.save(state);
  const started = await json('POST', '/api/paper-executions', { strategyId, accountId: 'acct-paper-primary' }, store);
  assert.equal(started.status, 201);
  assert.equal(started.data.execution.status, 'running');
  const stopped = await json('POST', `/api/paper-executions/${started.data.execution.id}/stop`, { reason: 'test_stop' }, store);
  assert.equal(stopped.status, 200);
  assert.equal(stopped.data.execution.status, 'stopped');
});

test('kill switch stop-paper stops running paper executions', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const state = await store.load();
  state.approvals[0].status = 'approved';
  state.paperExecutions.push({ id: 'paper-running', strategyId: state.strategies[0].id, accountId: 'acct-paper-primary', status: 'running', startedAt: 'now' });
  await store.save(state);
  const stopped = await json('POST', '/api/kill-switch/stop-paper', {}, store);
  assert.equal(stopped.status, 200);
  assert.ok(stopped.data.executions.every(e => e.status !== 'running'));
});
