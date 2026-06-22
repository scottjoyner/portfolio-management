import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';

let ExecutionEngine;

before(async () => {
  const mod = await import('../packages/execution/src/executionEngine.mjs');
  ExecutionEngine = mod.default;
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
  ...overrides,
});

describe('ExecutionEngine', () => {
  it('creates engine with default config', () => {
    const engine = new ExecutionEngine();
    assert.ok(engine);
    assert.equal(engine.minConfidence, 0.6);
    assert.equal(engine.requireApproval, true);
  });

  it('rejects low confidence orders', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const req = sampleRequest({
      orders: [sampleOrder({ confidenceScore: 0.3 })],
    });
    const result = await engine.execute(req);
    assert.equal(result.ok, false);
    assert.ok(result.errors.some(e => e.includes('confidence')));
  });

  it('accepts high confidence orders (auto-submit)', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const req = sampleRequest();
    const result = await engine.execute(req);
    assert.equal(result.ok, true);
    assert.equal(result.execution.status, 'filled');
    assert.equal(result.execution.fills.length, 1);
  });

  it('creates draft execution when approval required', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const req = sampleRequest();
    const result = await engine.execute(req);
    assert.equal(result.ok, true);
    assert.equal(result.execution.status, 'draft');
    assert.ok(result.warnings.includes('awaiting_approval'));
  });

  it('approves a draft execution', async () => {
    const engine = new ExecutionEngine({ requireApproval: true });
    const req = sampleRequest();
    const createResult = await engine.execute(req);
    const execId = createResult.execution.id;

    const approveResult = await engine.approve(execId);
    assert.equal(approveResult.ok, true);
    assert.equal(approveResult.execution.status, 'filled');
    assert.equal(approveResult.execution.fills.length, 1);
  });

  it('rejects a draft execution', async () => {
    const engine = new ExecutionEngine();
    const req = sampleRequest();
    const createResult = await engine.execute(req);
    const execId = createResult.execution.id;

    const rejectResult = await engine.reject(execId, 'test_reason');
    assert.equal(rejectResult.ok, false);
    assert.equal(rejectResult.execution.status, 'rejected');
    assert.ok(rejectResult.errors.includes('test_reason'));
  });

  it('cancels a draft execution', async () => {
    const engine = new ExecutionEngine();
    const req = sampleRequest();
    const createResult = await engine.execute(req);
    const execId = createResult.execution.id;

    const cancelResult = await engine.cancel(execId);
    assert.equal(cancelResult.ok, true);
    assert.equal(cancelResult.execution.status, 'cancelled');
  });

  it('returns plan without executing', async () => {
    const engine = new ExecutionEngine();
    const req = sampleRequest();
    const plan = await engine.plan(req);
    assert.ok(plan.id);
    assert.equal(plan.confidenceScore, 0.75);
    assert.equal(plan.approved, true);
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

  it('records and retrieves events', async () => {
    const engine = new ExecutionEngine({ requireApproval: false });
    const result = await engine.execute(sampleRequest());
    const events = engine.getEvents(result.execution.id);

    assert.ok(events.length >= 2); // created, submitted, filled
    assert.equal(events[0].executionId, result.execution.id);
    assert.equal(events[0].type, 'created');
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
    const req = sampleRequest();
    const createResult = await engine.execute(req);
    const execId = createResult.execution.id;

    // Can't cancel after approve
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
