import test from 'node:test';
import assert from 'node:assert/strict';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { createOperatorStore } from '../packages/storage/src/operatorStoreFactory.mjs';
import { TransactionalPostgresOperatorStore } from '../packages/storage/src/transactionalPostgresOperatorStore.mjs';

class FakeConnection {
  constructor() {
    this.calls = [];
    this.released = false;
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    if (sql.includes("to_regclass('public.schema_migrations')")) {
      return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies', opportunities: 'opportunities' }] };
    }
    if (sql === 'SELECT version FROM schema_migrations ORDER BY version ASC') {
      return { rows: [
        { version: '001_operator_state' },
        { version: '002_operator_product_layer' },
        { version: '004_opportunity_agent_workflow' },
        { version: '005_runtime_job_queue' },
      ] };
    }
    if (sql.startsWith('SELECT * FROM')) return { rows: [] };
    if (sql.startsWith('SELECT key, value_json')) return { rows: [] };
    return { rows: [], rowCount: 0 };
  }

  release() {
    this.released = true;
  }
}

class FakePool {
  constructor() {
    this.poolQueries = [];
    this.connections = [];
  }

  async connect() {
    const connection = new FakeConnection();
    this.connections.push(connection);
    return connection;
  }

  async query(sql, params = []) {
    this.poolQueries.push({ sql, params });
    throw new Error('pool_query_must_not_run_inside_operator_transaction');
  }
}

function commands(connection) {
  return connection.calls.map(call => String(call.sql).trim().split(/\s+/).slice(0, 3).join(' ').toUpperCase());
}

test('factory selects the transactional PostgreSQL store', () => {
  const store = createOperatorStore({ kind: 'postgres', client: new FakePool(), bootstrap: false });
  assert.ok(store instanceof TransactionalPostgresOperatorStore);
  assert.equal(store.getStatus().transactionModel, 'pinned-client-serializable');
  assert.equal(store.getStatus().runtimeJobQueue, 'lease-backed-postgres');
});

test('transactional store rejects PostgreSQL without the runtime queue migration', async () => {
  const connection = new FakeConnection();
  connection.query = async (sql, params = []) => {
    connection.calls.push({ sql, params });
    if (sql.includes("to_regclass('public.schema_migrations')")) {
      return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies', opportunities: 'opportunities' }] };
    }
    if (sql === 'SELECT version FROM schema_migrations ORDER BY version ASC') {
      return { rows: [
        { version: '002_operator_product_layer' },
        { version: '004_opportunity_agent_workflow' },
      ] };
    }
    return { rows: [] };
  };
  const store = new TransactionalPostgresOperatorStore({ client: connection, bootstrap: false });
  const migrations = await store.checkMigrations();
  assert.equal(migrations.ok, false);
  assert.equal(migrations.reason, 'runtime_job_queue_migration_missing');
});

test('layered P0/P1/P2 save commits once on one checked-out client', async () => {
  const pool = new FakePool();
  const store = new TransactionalPostgresOperatorStore({ client: pool, bootstrap: false, operatorWriteLockKey: 1234 });
  const state = createInitialOperatorState('2026-07-30T12:00:00.000Z');
  state.audit.push({ id: 'audit-1', action: 'transaction-test', actor: 'test', at: '2026-07-30T12:00:00.000Z' });

  await store.save(state);

  assert.equal(pool.poolQueries.length, 0);
  assert.equal(pool.connections.length, 1);
  const connection = pool.connections[0];
  const sql = connection.calls.map(call => call.sql);
  assert.equal(sql.filter(statement => statement.startsWith('BEGIN ISOLATION LEVEL')).length, 1);
  assert.equal(sql.filter(statement => statement === 'COMMIT').length, 1);
  assert.equal(sql.filter(statement => statement === 'ROLLBACK').length, 0);
  assert.equal(sql.filter(statement => statement === 'BEGIN').length, 0);
  assert.deepEqual(connection.calls.find(call => call.sql.includes('pg_advisory_xact_lock'))?.params, [1234]);
  assert.ok(sql.some(statement => statement.includes('INSERT INTO audit_events')));
  assert.ok(sql.some(statement => statement.includes('INSERT INTO operator_flags')));
  assert.equal(connection.released, true);
});

test('mutation lock is acquired before state is loaded', async () => {
  const pool = new FakePool();
  const store = new TransactionalPostgresOperatorStore({ client: pool, bootstrap: false, operatorWriteLockKey: 9876 });

  await store.mutate(state => {
    state.killSwitch = { enabled: true, reason: 'test', updatedAt: '2026-07-30T12:00:00.000Z' };
    return state.killSwitch;
  });

  const connection = pool.connections[0];
  const lockIndex = connection.calls.findIndex(call => call.sql.includes('pg_advisory_xact_lock'));
  const loadIndex = connection.calls.findIndex(call => call.sql.startsWith('SELECT * FROM strategies'));
  assert.ok(lockIndex >= 0);
  assert.ok(loadIndex > lockIndex);
  assert.equal(connection.calls.filter(call => call.sql === 'COMMIT').length, 1);
  assert.equal(connection.released, true);
});

test('failed mutations roll back the pinned transaction and release the client', async () => {
  const pool = new FakePool();
  const store = new TransactionalPostgresOperatorStore({ client: pool, bootstrap: false });

  await assert.rejects(
    store.mutate(() => {
      throw new Error('mutation_failed_for_test');
    }),
    /mutation_failed_for_test/,
  );

  const connection = pool.connections[0];
  assert.equal(connection.calls.filter(call => call.sql === 'COMMIT').length, 0);
  assert.equal(connection.calls.filter(call => call.sql === 'ROLLBACK').length, 1);
  assert.equal(connection.released, true);
});

test('nested transactions reuse the same session and suppress inherited controls', async () => {
  const pool = new FakePool();
  const store = new TransactionalPostgresOperatorStore({ client: pool, bootstrap: false });
  let outerClient;
  let innerClient;

  await store.withTransaction(async client => {
    outerClient = client;
    await store.withTransaction(async nested => {
      innerClient = nested;
      const suppressed = await store.query('BEGIN');
      assert.equal(suppressed.nestedTransactionControlSuppressed, true);
    });
  });

  assert.equal(outerClient, innerClient);
  assert.equal(pool.connections.length, 1);
  assert.equal(commands(pool.connections[0]).filter(value => value.startsWith('BEGIN ISOLATION LEVEL')).length, 1);
  assert.equal(pool.connections[0].calls.filter(call => call.sql === 'COMMIT').length, 1);
});
