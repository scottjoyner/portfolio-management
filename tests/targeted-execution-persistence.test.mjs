import test from 'node:test';
import assert from 'node:assert/strict';

import { handleOperatorRoute } from '../apps/api/src/operatorRouter.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { TransactionalPostgresOperatorStore } from '../packages/storage/src/transactionalPostgresOperatorStore.mjs';

function execution(id, status) {
  return {
    id,
    idempotencyKey: `intent-${id}`,
    strategyId: 'strategy-1',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    mode: 'paper',
    side: 'buy',
    status,
    version: 1,
    quantity: 0.01,
    notional: 1000,
    entryPrice: 100000,
    orders: [],
    fills: [],
  };
}

function targetedStore() {
  const calls = [];
  return {
    calls,
    async persistExecutionMutation(input, options) {
      calls.push({ input, options });
      return { persistence: 'targeted-optimistic' };
    },
    async mutate() {
      throw new Error('broad_mutate_must_not_run');
    },
  };
}

test('execution submit uses targeted persistence instead of broad state mutation', async () => {
  const state = createInitialOperatorState('2026-08-02T20:00:00.000Z');
  const store = targetedStore();
  handleOperatorRoute._execEngine = {
    async execute(input) {
      assert.equal(input.tradePlan.entry_price, 100000);
      return { ok: true, execution: execution('execution-submit', 'submitted') };
    },
  };

  const response = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/execution/execute',
    state,
    store,
    readJsonBody: async () => ({
      symbol: 'BTC-USD',
      side: 'buy',
      quantity: 0.01,
      entryPrice: 100000,
      takeProfitPrice: 110000,
      stopLossPrice: 95000,
    }),
  });

  assert.equal(response.status, 200);
  assert.equal(response.body.persistence, 'targeted-optimistic');
  assert.equal(store.calls.length, 1);
  assert.equal(store.calls[0].input.execution.id, 'execution-submit');
  assert.equal(store.calls[0].input.auditEvent.action, 'execution_submitted');
  assert.equal(state.executions.at(-1).id, 'execution-submit');
});

for (const scenario of [
  { pathname: '/api/execution/execution-1/approve', method: 'approve', status: 'approved', action: 'execution_approved' },
  { pathname: '/api/execution/execution-1/reject', method: 'reject', status: 'rejected', action: 'execution_rejected', body: { reason: 'risk' } },
  { pathname: '/api/execution/execution-1/cancel', method: 'cancel', status: 'cancelled', action: 'execution_cancelled' },
]) {
  test(`${scenario.method} uses targeted persistence`, async () => {
    const state = createInitialOperatorState('2026-08-02T20:00:00.000Z');
    const store = targetedStore();
    handleOperatorRoute._execEngine = {
      async [scenario.method](id, reason) {
        assert.equal(id, 'execution-1');
        if (scenario.method === 'reject') assert.equal(reason, 'risk');
        return { ok: true, execution: execution(id, scenario.status) };
      },
    };

    const response = await handleOperatorRoute({
      method: 'POST',
      pathname: scenario.pathname,
      state,
      store,
      readJsonBody: async () => scenario.body || {},
    });

    assert.equal(response.status, 200);
    assert.equal(response.body.persistence, 'targeted-optimistic');
    assert.equal(store.calls.length, 1);
    assert.equal(store.calls[0].input.auditEvent.action, scenario.action);
  });
}

test('memory-compatible stores retain the state mutation fallback', async () => {
  const state = createInitialOperatorState('2026-08-02T20:00:00.000Z');
  let mutateCalls = 0;
  const store = {
    async mutate(mutator) {
      mutateCalls += 1;
      return mutator(state);
    },
  };
  handleOperatorRoute._execEngine = {
    async approve(id) {
      return { ok: true, execution: execution(id, 'approved') };
    },
  };

  const response = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/execution/execution-fallback/approve',
    state,
    store,
    readJsonBody: async () => ({}),
  });

  assert.equal(response.status, 200);
  assert.equal(response.body.persistence, 'compatibility-state');
  assert.equal(mutateCalls, 1);
});

class FakeConnection {
  constructor() {
    this.calls = [];
    this.released = false;
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    return { rows: [] };
  }

  release() {
    this.released = true;
  }
}

class FakePool {
  constructor() {
    this.connections = [];
  }

  async connect() {
    const connection = new FakeConnection();
    this.connections.push(connection);
    return connection;
  }
}

test('targeted PostgreSQL execution persistence never invokes broad state replacement', async () => {
  const pool = new FakePool();
  const store = new TransactionalPostgresOperatorStore({
    client: pool,
    bootstrap: false,
    operatorWriteLockKey: 4321,
  });
  store.state = createInitialOperatorState('2026-08-02T20:00:00.000Z');
  store.synchronizeExecutions = async executions => ({
    ok: true,
    executionCount: executions.length,
    reports: [],
  });
  store.appendAuditEventTargeted = async event => ({
    event: {
      ...event,
      previousHash: null,
      eventHash: 'hash-1',
      sequenceNumber: 1,
    },
    idempotent: false,
  });
  store.loadExecutionEventsForReadModel = async () => [];
  store.publishExecutionReadModel = () => {};
  store.save = async () => {
    throw new Error('broad_save_must_not_run');
  };
  store.mutate = async () => {
    throw new Error('broad_mutate_must_not_run');
  };

  const result = await TransactionalPostgresOperatorStore.prototype.persistExecutionMutation.call(store, {
    execution: execution('execution-targeted', 'submitted'),
    auditEvent: {
      id: 'audit-targeted',
      action: 'execution_submitted',
      actor: 'operator',
      at: '2026-08-02T20:01:00.000Z',
    },
  }, { now: '2026-08-02T20:01:00.000Z' });

  assert.equal(result.persistence, 'targeted-optimistic');
  assert.equal(store.state.executions.at(-1).id, 'execution-targeted');
  const sql = pool.connections[0].calls.map(call => String(call.sql));
  assert.ok(sql.some(statement => statement.startsWith('BEGIN ISOLATION LEVEL')));
  assert.ok(sql.some(statement => statement.includes('pg_advisory_xact_lock')));
  assert.ok(sql.includes('COMMIT'));
  assert.equal(sql.some(statement => /DELETE FROM|INSERT INTO strategies|INSERT INTO operator_flags/.test(statement)), false);
  assert.equal(pool.connections[0].released, true);
});
