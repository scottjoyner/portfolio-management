import test from 'node:test';
import assert from 'node:assert/strict';

import ExecutionEngine from '../packages/execution/src/executionEngine.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-09-11T14:00:00.000Z';
const ACCOUNT_ID = 'acct-paper-primary';

function stateFixture(now = NOW) {
  const state = createInitialOperatorState(now);
  state.accounts[0] = {
    ...state.accounts[0],
    cash: 100000,
    nav: 100000,
    updatedAt: now,
  };
  state.killSwitch = { enabled: false, reason: 'test_ready', updatedAt: now };
  state.portfolioRiskState = {
    accounts: [{
      accountId: ACCOUNT_ID,
      highWaterNavUsd: 100000,
      highWaterAt: now,
      initializedAt: now,
      updatedAt: now,
      source: 'test',
    }],
    correlationMatrix: {},
    volatilityBySymbol: {},
    updatedAt: now,
  };
  state.marketDataSnapshots = [{
    id: 'md-btc',
    symbol: 'BTC-USD',
    venue: 'coinbase-paper',
    bid: 99990,
    ask: 100010,
    volume24h: 1_000_000,
    status: 'connected',
    source: 'test',
    timestamp: now,
  }];
  return state;
}

function order(quantity = 0.05, overrides = {}) {
  return {
    id: 'order-btc',
    marketId: 'BTC-USD',
    symbol: 'BTC-USD',
    venue: 'coinbase-paper',
    side: 'buy',
    quantity,
    price: 100000,
    orderType: 'market',
    timeInForce: 'GTC',
    confidenceScore: 0.9,
    feeBps: 5,
    ...overrides,
  };
}

function request(quantity = 0.05, overrides = {}) {
  return {
    strategyId: 'strategy-target',
    accountId: ACCOUNT_ID,
    mode: 'paper',
    venue: 'coinbase-paper',
    symbol: 'BTC-USD',
    side: 'buy',
    confidenceScore: 0.9,
    entryPrice: 100000,
    orders: [order(quantity)],
    ...overrides,
  };
}

function mutableProvider(ref) {
  return async ({ now }) => ({
    state: ref.state,
    source: 'mutable-test-store',
    revision: ref.revision,
    observedAt: now,
  });
}

test('plan binds portfolio allocation into capital-risk provenance and approved intent', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ riskStateProvider: mutableProvider(ref) });
  const plan = await engine.plan(request(), { now: NOW });

  assert.equal(plan.approved, true);
  assert.equal(plan.portfolioAllocation.approved, true);
  assert.equal(plan.portfolioAllocationHash.length, 64);
  assert.equal(plan.portfolioAllocationDecisionHash.length, 64);
  assert.equal(plan.portfolioAllocation.approvedIntentHash, plan.tradeIntentHash);
  assert.equal(plan.riskDecision.portfolioAllocationHash, plan.portfolioAllocationHash);
  assert.equal(plan.riskDecision.portfolioAllocationPolicyVersion, plan.portfolioAllocation.policyVersion);
  assert.equal(plan.riskDecision.portfolioAllocationDecisionHash, plan.portfolioAllocationDecisionHash);
  assert.equal(plan.capitalRiskSnapshot.tradeIntentHash, plan.tradeIntentHash);
});

test('manual request executes only the allocator-approved reduced quantity', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ requireApproval: false, riskStateProvider: mutableProvider(ref) });
  const result = await engine.execute(request(0.20), { now: NOW });

  assert.equal(result.ok, true);
  assert.equal(result.execution.portfolioAllocation.decision, 'REDUCE');
  assert.equal(result.execution.portfolioAllocation.requestedNotionalUsd, 20000);
  assert.equal(result.execution.portfolioAllocation.approvedNotionalUsd, 10000);
  assert.equal(result.execution.orders[0].quantity, 0.10);
  assert.equal(result.execution.fills[0].quantity, 0.10);
  assert.equal(result.execution.capitalRiskSnapshot.limits.orderNotionalUsd, 10000);

  const created = engine.getEvents(result.execution.id).find(event => event.type === 'created');
  assert.equal(created.portfolioAllocationHash, result.execution.portfolioAllocationHash);
});

test('economic request that needs downsizing is rejected until economics are regenerated', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ requireApproval: false, riskStateProvider: mutableProvider(ref) });
  const result = await engine.execute(request(0.20, {
    sourceAgentId: 'agent-1',
    economicDecisionId: 'decision-1',
    forecastId: 'forecast-1',
    executionCostSnapshotId: 'cost-1',
    netExecutableEdgeUsd: 25,
  }), { now: NOW });

  assert.equal(result.ok, false);
  assert.equal(result.execution.status, 'draft');
  assert.equal(result.execution.fills.length, 0);
  assert.equal(result.execution.portfolioAllocation.approved, false);
  assert.equal(result.execution.portfolioAllocation.suggestedNotionalUsd, 10000);
  assert.ok(result.errors.includes('economic_reprice_required_after_allocation'));
});

test('approval fails closed and requests a replan when portfolio state tightens sizing', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ requireApproval: true, riskStateProvider: mutableProvider(ref) });
  const created = await engine.execute(request(0.05), { now: NOW });
  assert.equal(created.ok, true);
  assert.equal(created.execution.orders[0].quantity, 0.05);

  ref.state.positions = [{
    id: 'position-existing-btc',
    accountId: ACCOUNT_ID,
    strategyId: 'other-strategy',
    symbol: 'BTC-USD',
    quantity: 0.19,
    markPrice: 100000,
    status: 'open',
  }];
  ref.revision = 'r2';

  const approved = await engine.approve(created.execution.id, { now: NOW });
  assert.equal(approved.ok, false);
  assert.equal(approved.execution.status, 'draft');
  assert.equal(approved.execution.fills.length, 0);
  assert.ok(approved.errors.includes('portfolio_allocation_changed'));
  assert.ok(approved.errors.includes('portfolio_allocation_replan_required'));
  assert.equal(approved.replan.approvedNotionalUsd, 1000);
});

test('tampered stored portfolio allocation blocks approval before submission', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ requireApproval: true, riskStateProvider: mutableProvider(ref) });
  const created = await engine.execute(request(), { now: NOW });
  created.execution.portfolioAllocation.budgets.singleTradeUsd = 999999;

  const approved = await engine.approve(created.execution.id, { now: NOW });
  assert.equal(approved.ok, false);
  assert.equal(approved.execution.status, 'draft');
  assert.ok(approved.errors.includes('portfolio_allocation_hash_mismatch'));
  assert.equal(approved.execution.fills.length, 0);
});

test('direct submit rejects an expired portfolio allocation', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ requireApproval: true, riskStateProvider: mutableProvider(ref) });
  const created = await engine.execute(request(), { now: NOW });

  const submitted = await engine.submit(created.execution, { now: '2026-09-11T14:00:31.000Z' });
  assert.equal(submitted.ok, false);
  assert.equal(submitted.execution.status, 'draft');
  assert.ok(submitted.errors.includes('portfolio_allocation_expired'));
  assert.equal(submitted.execution.fills.length, 0);
});

test('approval excludes its own persisted draft from pending portfolio reservations', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ requireApproval: true, riskStateProvider: mutableProvider(ref) });
  const created = await engine.execute(request(0.10), { now: NOW });
  assert.equal(created.ok, true);
  assert.equal(created.execution.portfolioAllocation.approvedNotionalUsd, 10000);

  ref.state.executions = [structuredClone(created.execution)];
  ref.revision = 'r2';
  const approved = await engine.approve(created.execution.id, { now: NOW });

  assert.equal(approved.ok, true);
  assert.equal(approved.execution.status, 'filled');
  assert.equal(approved.execution.fills[0].quantity, 0.10);
});

test('live execution remains blocked after portfolio allocation integration', async () => {
  const ref = { state: stateFixture(), revision: 'r1' };
  const engine = new ExecutionEngine({ requireApproval: false, riskStateProvider: mutableProvider(ref) });
  const result = await engine.execute(request(0.01, {
    mode: 'live',
    venue: 'coinbase',
    orders: [order(0.01, { venue: 'coinbase', executionMode: 'live' })],
  }), { now: NOW });

  assert.equal(result.ok, false);
  assert.equal(result.execution.fills.length, 0);
  assert.ok(result.errors.includes('live_execution_not_certified'));
});
