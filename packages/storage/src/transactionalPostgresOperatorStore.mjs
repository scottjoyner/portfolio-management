import { AsyncLocalStorage } from 'node:async_hooks';

import { synchronizeCompatibilityExecutions } from './executionCompatibilitySync.mjs';
import { ExecutionRepository } from './executionRepository.mjs';
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

function hasMigration(applied = [], version) {
  return applied.includes(version) || applied.includes(`${version}.sql`);
}

/**
 * Production PostgreSQL store.
 *
 * The legacy stores issue BEGIN/COMMIT through store.query() and layer P0, P1,
 * and P2 saves independently. This wrapper pins the complete operation to one
 * checked-out client, suppresses nested transaction controls, and serializes
 * whole-state mutations with a transaction-scoped advisory lock.
 *
 * Targeted row repositories, normalized executions, and the runtime job queue
 * execute inside the same pinned transaction because store.query() resolves
 * through AsyncLocalStorage.
 */
export class TransactionalPostgresOperatorStore extends PostgresOperatorStoreP2 {
  constructor(options = {}) {
    super(options);
    this.kind = 'postgres';
    this.implementation = 'postgres-transactional-p2';
    this.transactionContext = new AsyncLocalStorage();
    this.operatorWriteLockKey = Number(options.operatorWriteLockKey ?? process.env.OPERATOR_WRITE_LOCK_KEY ?? DEFAULT_OPERATOR_WRITE_LOCK_KEY);
    this.transactionIsolation = options.transactionIsolation || process.env.OPERATOR_TRANSACTION_ISOLATION || 'SERIALIZABLE';
    this.runtimeJobs = new RuntimeJobQueue(this);
    this.executionRepository = new ExecutionRepository(this);
    this.lastExecutionSync = null;
  }

  async checkMigrations() {
    const migrations = await super.checkMigrations();
    if (!migrations.ok) return migrations;
    if (!hasMigration(migrations.applied, '005_runtime_job_queue')) {
      this.migrations = { ...migrations, ok: false, reason: 'runtime_job_queue_migration_missing' };
      return this.migrations;
    }
    if (!hasMigration(migrations.applied, '006_normalized_execution_runtime')) {
      this.migrations = { ...migrations, ok: false, reason: 'normalized_execution_migration_missing' };
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

  async synchronizeExecutions(executions = [], now = new Date().toISOString()) {
    this.lastExecutionSync = await synchronizeCompatibilityExecutions(this.executionRepository, executions, { now });
    return this.lastExecutionSync;
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    return this.withTransaction(async () => {
      const saved = await super.save(state);
      await this.synchronizeExecutions(saved.executions || []);
      this.state = saved;
      return saved;
    });
  }

  async mutate(mutator) {
    return this.withTransaction(async () => {
      const state = await super.load();
      const result = await mutator(state);
      await super.save(state);
      await this.synchronizeExecutions(state.executions || []);
      this.state = state;
      return result;
    });
  }

  async createExecutionRecord(input, options) {
    return this.executionRepository.create(input, options);
  }

  async transitionExecutionRecord(input, options) {
    return this.executionRepository.transition(input, options);
  }

  async appendExecutionEvent(input, options) {
    return this.executionRepository.appendEvent(input, options);
  }

  async putExecutionOrder(input, options) {
    return this.executionRepository.putOrder(input, options);
  }

  async recordExecutionFill(input, options) {
    return this.executionRepository.recordFill(input, options);
  }

  async loadExecutionBundle(executionId) {
    return this.executionRepository.loadBundle(executionId);
  }

  async close() {
    const client = this.client;
    if (client && typeof client.end === 'function') await client.end();
    this.client = null;
  }

  getStatus() {
    return {
      ...super.getStatus(),
      kind: 'postgres',
      implementation: this.implementation,
      transactionModel: 'pinned-client-serializable',
      mutationSerialization: 'postgres-advisory-xact-lock',
      operatorWriteLockKey: this.operatorWriteLockKey,
      runtimeJobQueue: 'lease-backed-postgres',
      executionPersistence: 'normalized-optimistic-postgres',
      executionEvents: 'append-only-postgres',
      executionCompatibilitySync: 'atomic-on-save',
      lastExecutionSync: this.lastExecutionSync,
      activeTransaction: Boolean(this.currentTransaction()),
    };
  }
}
