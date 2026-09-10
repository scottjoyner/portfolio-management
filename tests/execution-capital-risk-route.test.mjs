import test from 'node:test';
import assert from 'node:assert/strict';

import { handleOperatorRoute } from '../apps/api/src/operatorRouter.mjs';
import { createInitialOperatorState, MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function healthyState() {
  const now = new Date().toISOString();
  const state = createInitialOperatorState(now);
  state.killSwitch = { enabled: false, reason: 'route_test_ready', updatedAt: now };
  state.marketDataSnapshots = [{
    id: 'md-route-btc',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    bid: 99990,
    ask: 100010,
    status: 'connected',
    source: 'route-test',
    timestamp: now,
  }];
  return state;
}

function executionBody(overrides = {}) {
  return {
    strategyId: 'strategy-route-risk',
    accountId: 'acct-paper-primary',
    mode: 'paper',
    venue: 'coinbase',
    symbol: 'BTC-USD',
    side: 'buy',
    quantity: 0.01,
    confidenceScore: 0.9,
    entryPrice: 100000,
    takeProfitPrice: 105000,
    stopLossPrice: 97000,
    ...overrides,
  };
}

async function request(store, pathname, body = {}, method = 'POST') {
  return handleOperatorRoute({
    method,
    pathname,
    state: store.state,
    store,
    readJsonBody: async () => body,
  });
}

function resetEngine() {
  handleOperatorRoute._execEngine = null;
}

test('execution plan is evaluated against canonical operator state', async () => {
  resetEngine();
  const store = new MemoryOperatorStore(healthyState());
  const response = await request(store, '/api/execution/plan', executionBody({ riskDecision: { approved: false, reasons: ['caller_claim'] } }));

  assert.equal(response.status, 200);
  assert.equal(response.body.ok, true);
  assert.equal(response.body.approved, true);
  assert.equal(response.body.riskDecision.source, 'canonical_capital_risk_snapshot');
  assert.equal(response.body.capitalRiskSnapshotHash.length, 64);
  assert.equal(store.state.executions.length, 0);
});

test('caller approval cannot bypass a current kill switch', async () => {
  resetEngine();
  const state = healthyState();
  state.killSwitch = { enabled: true, reason: 'operator_stop', updatedAt: new Date().toISOString() };
  const store = new MemoryOperatorStore(state);
  const response = await request(store, '/api/execution/execute', executionBody({
    riskDecision: { approved: true, reasons: [] },
  }));

  assert.equal(response.status, 400);
  assert.equal(response.body.ok, false);
  assert.ok(response.body.errors.includes('kill_switch_on'));
  assert.equal(store.state.executions.length, 0);
});

test('healthy state admits a paper draft and persists capital-risk audit lineage', async () => {
  resetEngine();
  const store = new MemoryOperatorStore(healthyState());
  const response = await request(store, '/api/execution/execute', executionBody());

  assert.equal(response.status, 200);
  assert.equal(response.body.ok, true);
  assert.equal(response.body.execution.status, 'draft');
  assert.equal(response.body.execution.capitalRiskSnapshotHash.length, 64);
  assert.equal(response.body.execution.overseerDecision.capitalRiskSnapshotHash, response.body.execution.capitalRiskSnapshotHash);
  assert.equal(store.state.executions.length, 1);
  const audit = store.state.audit.at(-1);
  assert.equal(audit.payload.tradeIntentHash, response.body.execution.tradeIntentHash);
  assert.equal(audit.payload.capitalRiskSnapshotHash, response.body.execution.capitalRiskSnapshotHash);
  assert.equal(audit.payload.riskDecisionHash, response.body.execution.riskDecisionHash);
});

test('approval refresh rejects when kill switch flips after draft creation', async () => {
  resetEngine();
  const store = new MemoryOperatorStore(healthyState());
  const created = await request(store, '/api/execution/execute', executionBody());
  assert.equal(created.status, 200);
  const executionId = created.body.execution.id;

  store.state.killSwitch = { enabled: true, reason: 'post_draft_stop', updatedAt: new Date().toISOString() };
  const approved = await request(store, `/api/execution/${executionId}/approve`, {});

  assert.equal(approved.status, 400);
  assert.equal(approved.body.ok, false);
  assert.ok(approved.body.errors.includes('kill_switch_on'));
  assert.equal(store.state.executions.find(row => row.id === executionId).status, 'draft');
});

test('approval refresh rejects when account cash drops after draft creation', async () => {
  resetEngine();
  const store = new MemoryOperatorStore(healthyState());
  const created = await request(store, '/api/execution/execute', executionBody());
  assert.equal(created.status, 200);
  const executionId = created.body.execution.id;

  store.state.accounts[0].cash = 1;
  store.state.accounts[0].updatedAt = new Date().toISOString();
  const approved = await request(store, `/api/execution/${executionId}/approve`, {});

  assert.equal(approved.status, 400);
  assert.equal(approved.body.ok, false);
  assert.ok(approved.body.errors.includes('insufficient_balance'));
  assert.equal(store.state.executions.find(row => row.id === executionId).status, 'draft');
});

test('approval refresh rejects when market data ages out after draft creation', async () => {
  resetEngine();
  const store = new MemoryOperatorStore(healthyState());
  const created = await request(store, '/api/execution/execute', executionBody());
  assert.equal(created.status, 200);
  const executionId = created.body.execution.id;

  store.state.marketDataSnapshots[0].timestamp = new Date(Date.now() - 10 * 60_000).toISOString();
  const approved = await request(store, `/api/execution/${executionId}/approve`, {});

  assert.equal(approved.status, 400);
  assert.equal(approved.body.ok, false);
  assert.ok(approved.body.errors.includes('capital_risk_stale:market_data'));
  assert.ok(approved.body.errors.includes('orderbook_stale'));
});

test('missing kill-switch timestamp fails closed through the active route', async () => {
  resetEngine();
  const state = healthyState();
  state.killSwitch = { enabled: false, reason: null, updatedAt: null };
  const store = new MemoryOperatorStore(state);
  const response = await request(store, '/api/execution/execute', executionBody());

  assert.equal(response.status, 400);
  assert.ok(response.body.errors.includes('capital_risk_missing:kill_switch_state'));
});

test('live remains blocked even when authoritative paper-risk state is otherwise healthy', async () => {
  resetEngine();
  const store = new MemoryOperatorStore(healthyState());
  const response = await request(store, '/api/execution/execute', executionBody({ mode: 'live' }));

  assert.equal(response.status, 400);
  assert.ok(response.body.errors.includes('live_execution_not_certified'));
  assert.equal(store.state.executions.length, 0);
});
