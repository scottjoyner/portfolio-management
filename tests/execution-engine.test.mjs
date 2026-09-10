import { describe, it, before } from 'node:test';
import assert from 'node:assert/strict';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

let ExecutionEngine;

function healthyRiskState() {
  const now = new Date().toISOString();
  const state = createInitialOperatorState(now);
  state.killSwitch = { enabled: false, reason: 'test_ready', updatedAt: now };
  state.marketDataSnapshots = [{
    id: 'md-test-btc',
    symbol: 'BTC-USD',
    venue: 'paper',
    bid: 68240,
    ask: 68260,
    status: 'connected',
    source: 'test-fixture',
    timestamp: now,
  }];
  return { state, observedAt: now, source: 'test_operator_state', revision: `test:${now}` };
}

async function healthyRiskStateProvider() {
  return healthyRiskState();
}

before(async () => {
  const mod = await import('../packages/execution/src/executionEngine.mjs');
  const BaseExecutionEngine = mod.default;
  ExecutionEngine = class TestExecutionEngine extends BaseExecutionEngine {
    constructor(config = {}) {
      super({ riskStateProvider: healthyRiskStateProvider, ...config });
    }
  };
});

const sampleOrder = (overrides = {}) => ({
  id: 'test-ord-001',
  marketId: 'BTC-USD',
  symbol: 'BTC-USD',
  venue: 'paper',
  side: 'buy',
  quantity: 0.1,
  price: 68250,
  orderType: 'market',
  timeInForce: 'GTC',
  strategyId: 'test-strategy-001',
  executionMode: 'paper',
  confidenceScore: 0.75,
  createdAt: new Date().toISOString(),
  updatedAt: new Date().toISOString(),
  ...overrides,
});

const sampleRequest = (overrides = {}) => ({
  strategyId: 'test-strategy-001',
  accountId: 'acct-paper-primary',
  mode: 'paper',
  orders: [sampleOrder()],
  riskDecision: { approved: true, reasons: [] },
  ...overrides,
});

describe('ExecutionEngine', () => {
  it('creates engine with fail-closed defaults', () => {
    const engine = new ExecutionEngine();
    assert.ok(engine);
    assert.equal(engine.minConfidence, 0.6);
    assert.equal(engine.requireApproval, true);
    assert.equal(engine.requireRiskCheck, true);
    assert.ok(engine.overseerTtlMs > 0);
  });

  it('rejects low confidence orders', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const req = sampleRequest({
      orders: [sampleOrder({ confidenceScore: 0.3 })],
    });
    const result = await engine.execute(req);
    assert.equal(result.ok, false);
    assert.ok(result.errors.includes('confidence_below_threshold'));
  });

  it('fails closed without an authoritative risk state provider', async () => {
    const engine = new ExecutionEngine({ requireApproval: false, riskStateProvider: null });
    const result = await engine.execute(sampleRequest());
    assert.equal(result.ok, false);
    assert.ok(result.errors.includes('capital_risk_missing:authoritative_state'));
    assert.ok(result.errors.includes('capital_risk_missing:authoritative_state_observed_at'));
    assert.equal(result.execution.overseerDecision.approved, false);
  });

  it('does not require caller-supplied risk approval when canonical state is healthy', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest({ riskDecision: undefined }));
    assert.equal(result.ok, true);
    assert.equal(result.execution.status, 'filled');
    assert.equal(result.execution.riskDecision.source, 'canonical_capital_risk_snapshot');
  });

  it('does not let malformed caller risk data replace canonical evaluation', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest({
      riskDecision: { approved: 'yes', reasons: [] },
    }));
    assert.equal(result.ok, true);
    assert.equal(result.execution.riskDecision.approved, true);
    assert.equal(result.execution.riskDecision.source, 'canonical_capital_risk_snapshot');
  });

  it('does not let a caller-supplied risk rejection replace canonical evaluation', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest({
      riskDecision: { approved: false, reasons: ['caller_claim'] },
    }));
    assert.equal(result.ok, true);
    assert.equal(result.execution.riskDecision.approved, true);
    assert.equal(result.execution.riskDecision.reasons.includes('caller_claim'), false);
  });

  it('accepts canonically risk-approved high confidence paper orders', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest());
    assert.equal(result.ok, true);
    assert.equal(result.execution.status, 'filled');
    assert.equal(result.execution.fills.length, 1);
    assert.equal(result.execution.overseerDecision.decision, 'PAPER');
    assert.equal(result.execution.capitalRiskSnapshotHash.length, 64);
    assert.equal(result.execution.overseerDecision.capitalRiskSnapshotHash, result.execution.capitalRiskSnapshotHash);
  });

  it('keeps requireRiskCheck=false as an explicit compatibility escape hatch', async () => {
    const engine = new ExecutionEngine({ requireApproval: false, requireRiskCheck: false, riskStateProvider: null });
    const result = await engine.execute(sampleRequest({ riskDecision: undefined }));
    assert.equal(result.ok, true);
    assert.equal(result.execution.status, 'filled');
    assert.deepEqual(result.execution.riskDecision.reasons, ['risk_check_disabled_by_config']);
    assert.equal(result.execution.capitalRiskSnapshot, null);
  });

  it('creates a hash-bound overseer-authorized draft when approval is required', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const result = await engine.execute(sampleRequest());
    assert.equal(result.ok, true);
    assert.equal(result.execution.status, 'draft');
    assert.ok(result.warnings.includes('awaiting_approval'));
    assert.equal(result.execution.tradeIntentHash.length, 64);
    assert.equal(result.execution.capitalRiskSnapshotHash.length, 64);
    assert.equal(result.execution.riskDecisionHash.length, 64);
    assert.equal(result.execution.overseerDecision.intentHash, result.execution.tradeIntentHash);
    assert.equal(result.execution.overseerDecision.capitalRiskSnapshotHash, result.execution.capitalRiskSnapshotHash);
    assert.equal(result.execution.overseerDecision.riskDecisionHash, result.execution.riskDecisionHash);
    assert.equal(result.execution.overseerDecision.decision, 'PAPER');
    assert.equal(result.execution.overseerDecision.approved, true);
    assert.equal(result.execution.overseerDecision.requiresHumanApproval, true);
    assert.equal(result.execution.overseerDecision.decisionHash.length, 64);
  });

  it('approves and submits the same authorized draft after refreshing canonical state', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const createResult = await engine.execute(sampleRequest());
    const execId = createResult.execution.id;
    const firstSnapshotHash = createResult.execution.capitalRiskSnapshotHash;

    const approveResult = await engine.approve(execId);
    assert.equal(approveResult.ok, true);
    assert.equal(approveResult.execution.status, 'filled');
    assert.equal(approveResult.execution.fills.length, 1);
    assert.equal(approveResult.execution.tradeIntentHash, createResult.execution.tradeIntentHash);
    assert.equal(approveResult.execution.capitalRiskSnapshotHash.length, 64);
    assert.ok(firstSnapshotHash);
  });

  it('rejects approval after a material order mutation', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const createResult = await engine.execute(sampleRequest());
    createResult.execution.orders[0].quantity = 0.2;

    const approveResult = await engine.approve(createResult.execution.id);
    assert.equal(approveResult.ok, false);
    assert.equal(approveResult.execution.status, 'draft');
    assert.ok(approveResult.errors.includes('trade_intent_mutated'));
    assert.equal(approveResult.execution.fills.length, 0);
  });

  it('rejects approval when the overseer decision is corrupted', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const createResult = await engine.execute(sampleRequest());
    createResult.execution.overseerDecision.decisionHash = '0'.repeat(64);

    const approveResult = await engine.approve(createResult.execution.id);
    assert.equal(approveResult.ok, false);
    assert.ok(approveResult.errors.includes('overseer_decision_hash_mismatch'));
    assert.equal(approveResult.execution.status, 'draft');
  });

  it('rejects approval when the capital risk snapshot is corrupted', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const createResult = await engine.execute(sampleRequest());
    createResult.execution.capitalRiskSnapshot.accountState.cashUsd = 1;

    const approveResult = await engine.approve(createResult.execution.id);
    assert.equal(approveResult.ok, false);
    assert.ok(approveResult.errors.includes('capital_risk_snapshot_hash_mismatch'));
    assert.equal(approveResult.execution.status, 'draft');
  });

  it('rejects approval when the overseer authorization has expired', async () => {
    const engine = new ExecutionEngine({ requireApproval: true, overseerTtlMs: 1 });
    const createResult = await engine.execute(sampleRequest());
    await new Promise(resolve => setTimeout(resolve, 5));

    const approveResult = await engine.approve(createResult.execution.id);
    assert.equal(approveResult.ok, false);
    assert.ok(approveResult.errors.includes('overseer_decision_expired'));
    assert.equal(approveResult.execution.status, 'draft');
  });

  it('blocks live execution even with healthy canonical risk state', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest({
      mode: 'live',
      orders: [sampleOrder({ venue: 'coinbase', executionMode: 'live' })],
    }));
    assert.equal(result.ok, false);
    assert.ok(result.errors.includes('live_execution_not_certified'));
    assert.equal(result.execution.overseerDecision.decision, 'LIVE_REQUIRES_HUMAN');
    assert.equal(result.execution.fills.length, 0);
  });

  it('defends submit directly against missing overseer authorization', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const createResult = await engine.execute(sampleRequest());
    delete createResult.execution.overseerDecision;

    const submitResult = await engine.submit(createResult.execution);
    assert.equal(submitResult.ok, false);
    assert.ok(submitResult.errors.includes('overseer_decision_required'));
    assert.equal(submitResult.execution.status, 'draft');
  });

  it('rejects a draft execution', async () => {
    const engine = new ExecutionEngine();
    const createResult = await engine.execute(sampleRequest());
    const execId = createResult.execution.id;

    const rejectResult = await engine.reject(execId, 'test_reason');
    assert.equal(rejectResult.ok, false);
    assert.equal(rejectResult.execution.status, 'rejected');
    assert.ok(rejectResult.errors.includes('test_reason'));
  });

  it('cancels a draft execution', async () => {
    const engine = new ExecutionEngine();
    const createResult = await engine.execute(sampleRequest());
    const execId = createResult.execution.id;

    const cancelResult = await engine.cancel(execId);
    assert.equal(cancelResult.ok, true);
    assert.equal(cancelResult.execution.status, 'cancelled');
  });

  it('returns an overseer-authorized plan without executing', async () => {
    const engine = new ExecutionEngine();
    const plan = await engine.plan(sampleRequest());
    assert.ok(plan.id);
    assert.equal(plan.confidenceScore, 0.75);
    assert.equal(plan.approved, true);
    assert.equal(plan.overseerDecision.decision, 'PAPER');
    assert.equal(plan.overseerDecision.intentHash, plan.tradeIntentHash);
    assert.equal(plan.overseerDecision.capitalRiskSnapshotHash, plan.capitalRiskSnapshotHash);
  });

  it('computes conviction weight from confidence', async () => {
    const engine = new ExecutionEngine();
    const req = sampleRequest({ orders: [sampleOrder({ confidenceScore: 0.8 })] });
    const plan = await engine.plan(req);
    assert.equal(plan.convictionWeight, 0.9); // 0.5 + 0.8 * 0.5
  });

  it('lists all executions', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    await engine.execute(sampleRequest());
    await engine.execute(sampleRequest({ orders: [sampleOrder({ marketId: 'ETH-USD' })] }));

    const list = engine.listExecutions();
    assert.equal(list.length, 2);
  });

  it('filters executions by status', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    await engine.execute(sampleRequest());
    await engine.execute(sampleRequest({ orders: [sampleOrder({ marketId: 'ETH-USD' })] }));

    const drafts = engine.listExecutions({ status: 'draft' });
    assert.equal(drafts.length, 2);

    const filled = engine.listExecutions({ status: 'filled' });
    assert.equal(filled.length, 0);
  });

  it('records overseer and capital-risk lineage in execution events', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest());
    const events = engine.getEvents(result.execution.id);

    assert.ok(events.length >= 2); // created, submitted, filled
    assert.equal(events[0].executionId, result.execution.id);
    assert.equal(events[0].type, 'created');
    assert.equal(events[0].overseerDecision, 'PAPER');
    assert.equal(events[0].tradeIntentHash, result.execution.tradeIntentHash);
    assert.equal(events[0].capitalRiskSnapshotHash, result.execution.capitalRiskSnapshotHash);
  });

  it('getAllEvents returns all events', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    await engine.execute(sampleRequest());
    await engine.execute(sampleRequest({ orders: [sampleOrder({ marketId: 'ETH-USD' })] }));

    const all = engine.getAllEvents();
    assert.ok(all.length >= 4);
  });

  it('getExecution returns specific execution', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest());
    const found = engine.getExecution(result.execution.id);
    assert.ok(found);
    assert.equal(found.id, result.execution.id);
  });

  it('handles not-found approve gracefully', async () => {
    const engine = new ExecutionEngine();
    const result = await engine.approve('nonexistent');
    assert.equal(result.ok, false);
    assert.ok(result.errors.includes('execution_not_found'));
  });

  it('rejects invalid state transitions', async () => {
    const engine = new ExecutionEngine();
    const createResult = await engine.execute(sampleRequest());
    const execId = createResult.execution.id;

    await engine.approve(execId);
    const cancelResult = await engine.cancel(execId);
    assert.equal(cancelResult.ok, false);
    assert.ok(cancelResult.errors[0].includes('cannot_cancel'));
  });

  it('includes fee calculations in fills', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const req = sampleRequest({
      orders: [sampleOrder({ feeBps: 10, price: 50000, quantity: 1 })],
    });
    const result = await engine.execute(req);
    const fill = result.execution.fills[0];
    assert.equal(fill.fee, 50); // 1 * 50000 * 10 / 10000
  });
});

describe('ExecutionEngine config', () => {
  it('respects custom minConfidence', async () => {
    const engine = new ExecutionEngine({ minConfidence: 0.8, requireApproval: false });
    const lowResult = await engine.execute(sampleRequest({ orders: [sampleOrder({ confidenceScore: 0.7 })] }));
    assert.equal(lowResult.ok, false);

    const highResult = await engine.execute(sampleRequest({ orders: [sampleOrder({ confidenceScore: 0.9 })] }));
    assert.equal(highResult.ok, true);
  });

  it('respects maxExecutionRetries', () => {
    const engine = new ExecutionEngine({ maxExecutionRetries: 5 });
    assert.equal(engine.maxRetries, 5);
  });
});
