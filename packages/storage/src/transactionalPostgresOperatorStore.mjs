import { AsyncLocalStorage } from 'node:async_hooks';

import { normalizeOperatorState } from './operatorStore.mjs';
import { PostgresOperatorStoreP2 } from './postgresOperatorStoreP2.mjs';
import { RuntimeJobQueue } from './runtimeJobQueue.mjs';

const DEFAULT_OPERATOR_WRITE_LOCK_KEY = 0x504f5254;

function command(sql) {
  return String(sql || '').trim().replace(/;$/, '').toUpperCase();
}

function isTransactionControl(sql) {
  return ['BEGIN', 'COMMIT', 'ROLLBACK'].includes(command(sql));
}

/**
 * Production PostgreSQL store.
 *
 * The legacy stores issue BEGIN/COMMIT through store.query() and layer P0, P1,
 * and P2 saves independently. This wrapper pins the complete operation to one
 * checked-out client, suppresses nested transaction controls, and serializes
 * whole-state mutations with a transaction-scoped advisory lock.
 *
 * Targeted row repositories and the runtime job queue execute inside the same
 * pinned transaction because store.query() resolves through AsyncLocalStorage.
 */
export class TransactionalPostgresOperatorStore extends PostgresOperatorStoreP2 {
  constructor(options = {}) {
    super(options);
    this.kind = 'postgres-transactional';
    this.transactionContext = new AsyncLocalStorage();
    this.operatorWriteLockKey = Number(options.operatorWriteLockKey ?? process.env.OPERATOR_WRITE_LOCK_KEY ?? DEFAULT_OPERATOR_WRITE_LOCK_KEY);
    this.transactionIsolation = options.transactionIsolation || process.env.OPERATOR_TRANSACTION_ISOLATION || 'SERIALIZABLE';
    this.runtimeJobs = new RuntimeJobQueue(this);
  }

  async checkMigrations() {
    const migrations = await super.checkMigrations();
    if (!migrations.ok) return migrations;
    const hasRuntimeQueue = migrations.applied.includes('005_runtime_job_queue')
      || migrations.applied.includes('005_runtime_job_queue.sql');
    if (!hasRuntimeQueue) {
      this.migrations = { ...migrations, ok: false, reason: 'runtime_job_queue_migration_missing' };
      return this.migrations;
    }
    this.migrations = migrations;
    return this.migrations;
  }

  currentTransaction() {
    return this.transactionContext.getStore() || null;
  }

  async query(sql, params = []) {
    const transaction = this.currentTransaction();
    if (!transaction?.client) return super.query(sql, params);

    // The inherited P0/P1/P2 save layers each contain their own transaction
    // controls. They are intentionally no-ops inside the outer pinned session.
    if (isTransactionControl(sql)) {
      return { rows: [], rowCount: 0, command: command(sql), nestedTransactionControlSuppressed: true };
    }

    try {
      return await transaction.client.query(sql, params);
    } catch (error) {
      this.lastError = error;
      throw error;
    }
  }

  async withTransaction(operation, options = {}) {
    const active = this.currentTransaction();
    if (active?.client) return operation(active.client, active);

    const poolOrClient = await this.getClient();
    const checkedOut = typeof poolOrClient.connect === 'function';
    const client = checkedOut ? await poolOrClient.connect() : poolOrClient;
    const isolation = String(options.isolation || this.transactionIsolation || 'SERIALIZABLE').toUpperCase();
    const lockWrites = options.lockWrites !== false;
    const context = {
      client,
      startedAt: new Date().toISOString(),
      isolation,
      writeLockKey: lockWrites ? this.operatorWriteLockKey : null,
    };

    try {
      await client.query(`BEGIN ISOLATION LEVEL ${isolation}`);
      if (lockWrites) await client.query('SELECT pg_advisory_xact_lock($1)', [this.operatorWriteLockKey]);
      const result = await this.transactionContext.run(context, () => operation(client, context));
      await client.query('COMMIT');
      return result;
    } catch (error) {
      this.lastError = error;
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      if (checkedOut) client.release();
    }
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    return this.withTransaction(async () => {
      // Calling the inherited layered save is safe here: nested BEGIN/COMMIT
      // statements are suppressed and every query uses this pinned client.
      const saved = await super.save(state);
      this.state = saved;
      return saved;
    });
  }

  async mutate(mutator) {
    return this.withTransaction(async () => {
      // The advisory lock is acquired before loading, which prevents two API
      // or worker processes from reading the same revision and overwriting one
      // another with competing whole-state snapshots.
      const state = await super.load();
      const result = await mutator(state);
      await super.save(state);
      this.state = state;
      return result;
    });
  }

  async close() {
    const client = this.client;
    if (client && typeof client.end === 'function') await client.end();
    this.client = null;
  }

  getStatus() {
    return {
      ...super.getStatus(),
      transactionModel: 'pinned-client-serializable',
      mutationSerialization: 'postgres-advisory-xact-lock',
      operatorWriteLockKey: this.operatorWriteLockKey,
      runtimeJobQueue: 'lease-backed-postgres',
      activeTransaction: Boolean(this.currentTransaction()),
    };
  }
}
