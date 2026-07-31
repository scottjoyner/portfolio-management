import test from 'node:test';
import assert from 'node:assert/strict';

import ExecutionEngine from '../../packages/execution/src/executionEngine.mjs';
import { TransactionalPostgresOperatorStore } from '../../packages/storage/src/transactionalPostgresOperatorStore.mjs';

const DATABASE_URL = process.env.DATABASE_URL;
const NOW = '2026-07-31T18:00:00.000Z';
const EXECUTION_ID = `postgres-smoke-execution-${process.pid}`;
const JOB_KEY = `postgres-smoke-job-${process.pid}`;

function requireDatabase() {
  assert.ok(DATABASE_URL, 'DATABASE_URL is required for the PostgreSQL integration smoke test');
}

function executionFixture() {
  return {
    id: EXECUTION_ID,
    idempotencyKey: `postgres-smoke:${EXECUTION_ID}`,
    strategyId: 'postgres-smoke-strategy',
    sourceAgentId: 'postgres-smoke-operator',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    mode: 'paper',
    side: 'buy',
    status: 'draft',
    quantity: 0.001,
    notionalUsd: 100,
    requestedPrice: 100000,
    orders: [{
      id: `${EXECUTION_ID}-order-1`,
      symbol: 'BTC-USD',
      venue: 'coinbase',
      side: 'buy',
      quantity: 0.001,
      price: 100000,
      orderType: 'market',
      timeInForce: 'GTC',
    }],
    fills: [],
    createdAt: NOW,
    updatedAt: NOW,
  };
}

test('migration 006 survives a store restart and hydrates a fresh execution engine', async () => {
  requireDatabase();
  delete globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__;

  const firstStore = new TransactionalPostgresOperatorStore({ databaseUrl: DATABASE_URL });
  try {
    const migrations = await firstStore.checkMigrations();
    assert.equal(migrations.ok, true);
    assert.ok(migrations.applied.includes('006_normalized_execution_runtime'));

    const result = await firstStore.mutate(state => {
      state.executions ||= [];
      state.executions = state.executions.filter(row => row.id !== EXECUTION_ID);
      state.executions.push(executionFixture());
      return { executionId: EXECUTION_ID };
    });
    assert.equal(result.executionId, EXECUTION_ID);

    const bundle = await firstStore.loadExecutionBundle(EXECUTION_ID);
    assert.equal(bundle.execution.id, EXECUTION_ID);
    assert.equal(bundle.execution.status, 'draft');
    assert.equal(bundle.orders.length, 1);
    assert.ok(bundle.events.length >= 1);
  } finally {
    await firstStore.close();
  }

  delete globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__;
  const restartedStore = new TransactionalPostgresOperatorStore({ databaseUrl: DATABASE_URL });
  try {
    const restartedState = await restartedStore.load();
    const persisted = restartedState.executions.find(row => row.id === EXECUTION_ID);
    assert.ok(persisted, 'execution must be present after a fresh store instance loads PostgreSQL');
    assert.equal(persisted.status, 'draft');

    const readModel = globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__;
    assert.equal(readModel.source, 'postgres-transactional-operator-state');
    assert.ok(readModel.executions.some(row => row.id === EXECUTION_ID));
    assert.ok(readModel.events.some(row => row.executionId === EXECUTION_ID));

    const engineAfterRestart = new ExecutionEngine();
    const hydrated = engineAfterRestart.getExecution(EXECUTION_ID);
    assert.ok(hydrated, 'fresh execution engine must hydrate the durable execution read model');
    assert.equal(hydrated.status, 'draft');
    assert.equal(engineAfterRestart.lastHydratedRevision, readModel.revision);

    const firstEvent = await restartedStore.query(
      'SELECT event_id FROM execution_events WHERE execution_id = $1 ORDER BY sequence_number ASC LIMIT 1',
      [EXECUTION_ID],
    );
    assert.ok(firstEvent.rows?.[0]?.event_id);
    await assert.rejects(
      restartedStore.query('UPDATE execution_events SET payload_json = payload_json WHERE event_id = $1', [firstEvent.rows[0].event_id]),
      /append.only|execution_events_are_append_only/i,
    );
  } finally {
    await restartedStore.close();
    delete globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__;
  }
});

test('lease-backed runtime jobs enqueue, claim, heartbeat, and complete on real PostgreSQL', async () => {
  requireDatabase();
  const store = new TransactionalPostgresOperatorStore({ databaseUrl: DATABASE_URL });
  try {
    const queued = await store.runtimeJobs.enqueue({
      jobType: 'postgres-smoke',
      scope: 'release-readiness',
      idempotencyKey: JOB_KEY,
      payload: { executionId: EXECUTION_ID },
      now: NOW,
    });
    assert.ok(queued.job);

    const claimed = await store.runtimeJobs.claim({
      workerId: 'postgres-smoke-worker',
      jobTypes: ['postgres-smoke'],
      leaseSeconds: 60,
      now: NOW,
    });
    assert.equal(claimed.id, queued.job.id);
    assert.equal(claimed.status, 'running');

    const heartbeat = await store.runtimeJobs.heartbeat({
      jobId: claimed.id,
      workerId: 'postgres-smoke-worker',
      leaseSeconds: 120,
      now: '2026-07-31T18:00:10.000Z',
    });
    assert.equal(heartbeat.status, 'running');
    assert.equal(heartbeat.leaseOwner, 'postgres-smoke-worker');

    const completed = await store.runtimeJobs.complete({
      jobId: claimed.id,
      workerId: 'postgres-smoke-worker',
      result: { ok: true },
      now: '2026-07-31T18:00:20.000Z',
    });
    assert.equal(completed.status, 'completed');
    assert.deepEqual(completed.result, { ok: true });
  } finally {
    await store.close();
  }
});
