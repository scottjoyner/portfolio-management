import test from 'node:test';
import assert from 'node:assert/strict';

import ExecutionEngine from '../packages/execution/src/executionEngine.mjs';

function publish(model) {
  globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__ = {
    source: 'postgres-transactional-operator-state',
    publishedAt: '2026-07-30T20:00:00.000Z',
    events: [],
    ...model,
  };
}

function cleanup() {
  delete globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__;
}

test.afterEach(cleanup);

test('execution engine hydrates persisted executions after process restart', () => {
  publish({
    revision: 'revision-1',
    executions: [{
      id: 'exec-persisted',
      strategyId: 'strategy-1',
      symbol: 'BTC-USD',
      venue: 'coinbase',
      mode: 'paper',
      side: 'buy',
      status: 'draft',
      orders: [{ id: 'order-1', symbol: 'BTC-USD', venue: 'coinbase', side: 'buy', quantity: 0.01, price: 100000 }],
      fills: [],
      createdAt: '2026-07-30T19:59:00.000Z',
      updatedAt: '2026-07-30T19:59:30.000Z',
    }],
  });

  const engine = new ExecutionEngine();
  const execution = engine.getExecution('exec-persisted');
  assert.equal(execution.id, 'exec-persisted');
  assert.equal(execution.status, 'draft');
  assert.equal(engine.listExecutions().length, 1);
  assert.equal(engine.lastHydratedRevision, 'revision-1');
});

test('newer in-process execution state is not overwritten by an older durable snapshot', () => {
  publish({
    revision: 'revision-1',
    executions: [{
      id: 'exec-1',
      symbol: 'ETH-USD',
      venue: 'coinbase',
      mode: 'paper',
      side: 'buy',
      status: 'draft',
      orders: [],
      fills: [],
      updatedAt: '2026-07-30T20:00:00.000Z',
    }],
  });

  const engine = new ExecutionEngine();
  assert.equal(engine.getExecution('exec-1').status, 'draft');
  engine.executions.set('exec-1', {
    ...engine.executions.get('exec-1'),
    status: 'filled',
    updatedAt: '2026-07-30T20:05:00.000Z',
  });

  publish({
    revision: 'revision-2',
    executions: [{
      id: 'exec-1',
      symbol: 'ETH-USD',
      venue: 'coinbase',
      mode: 'paper',
      side: 'buy',
      status: 'submitted',
      orders: [],
      fills: [],
      updatedAt: '2026-07-30T20:02:00.000Z',
    }],
  });

  const report = engine.hydrateDurableReadModel();
  assert.equal(report.retainedNewerLocal, 1);
  assert.equal(engine.getExecution('exec-1').status, 'filled');
});

test('newer persisted state replaces stale compatibility state', () => {
  const engine = new ExecutionEngine();
  engine.executions.set('exec-2', {
    id: 'exec-2',
    symbol: 'SOL-USD',
    venue: 'coinbase',
    mode: 'paper',
    side: 'sell',
    status: 'draft',
    orders: [],
    fills: [],
    updatedAt: '2026-07-30T20:00:00.000Z',
  });

  publish({
    revision: 'revision-newer',
    executions: [{
      id: 'exec-2',
      symbol: 'SOL-USD',
      venue: 'coinbase',
      mode: 'paper',
      side: 'sell',
      status: 'cancelled',
      orders: [],
      fills: [],
      updatedAt: '2026-07-30T20:10:00.000Z',
    }],
  });

  const report = engine.hydrateDurableReadModel();
  assert.equal(report.replaced, 1);
  assert.equal(engine.getExecution('exec-2').status, 'cancelled');
});

test('non-PostgreSQL global data is ignored to isolate memory-store tests', () => {
  globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__ = {
    source: 'memory',
    revision: 'ignored',
    executions: [{ id: 'should-not-load' }],
  };
  const engine = new ExecutionEngine();
  assert.equal(engine.listExecutions().length, 0);
  assert.equal(engine.lastHydratedRevision, null);
});
