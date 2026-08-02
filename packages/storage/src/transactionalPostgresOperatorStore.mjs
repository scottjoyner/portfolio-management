import { AsyncLocalStorage } from 'node:async_hooks';

import { buildAuditEvent, completeAuditChain } from './auditChain.mjs';
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

function clone(value) {
  if (typeof structuredClone === 'function') return structuredClone(value);
  return JSON.parse(JSON.stringify(value));
}

function parseJson(value, fallback = {}) {
  if (value == null) return fallback;
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch { return fallback; }
}

function iso(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function requiredText(name, value) {
  const text = String(value || '').trim();
  if (!text) throw new Error(`${name}_required`);
  return text;
}

function hasAuditMetadata(event = {}) {
  return event.eventHash != null || event.sequenceNumber != null || event.previousHash != null;
}

function mapAuditEvent(row) {
  if (!row) return null;
  return {
    id: row.id,
    action: row.action,
    actor: row.actor || 'system',
    at: iso(row.at),
    details: row.details || null,
    payload: parseJson(row.payload_json),
    previousHash: row.previous_hash || null,
    eventHash: row.event_hash || null,
    sequenceNumber: row.sequence_number == null ? null : Number(row.sequence_number),
  };
}

function mapExecutionEvent(row) {
  return {
    id: row.event_id,
    executionId: row.execution_id,
    sequenceNumber: Number(row.sequence_number),
    type: row.event_type,
    fromStatus: row.from_status,
    toStatus: row.to_status,
    executionVersion: Number(row.execution_version),
    actor: row.actor,
    idempotencyKey: row.idempotency_key,
    payload: parseJson(row.payload_json),
    createdAt: row.created_at,
    timestamp: row.created_at,
  };
}

/**
 * Production PostgreSQL store.
 *
 * The compatibility state remains available for read models and non-migrated
 * domains. Normalized execution routes use targeted optimistic rows and direct
 * append-only audit writes; they do not invoke the broad whole-state saver.
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

    const execute = async () => {
      try {
        return await transaction.client.query(sql, params);
      } catch (error) {
        this.lastError = error;
        throw error;
      }
    };
    const pending = (transaction.queryTail || Promise.resolve()).then(execute);
    transaction.queryTail = pending.catch(() => {});
    return pending;
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
      queryTail: Promise.resolve(),
    };

    try {
      await client.query(`BEGIN ISOLATION LEVEL ${isolation}`);
      if (lockWrites) await client.query('SELECT pg_advisory_xact_lock($1)', [this.operatorWriteLockKey]);
      const result = await this.transactionContext.run(context, () => operation(client, context));
      await context.queryTail;
      await client.query('COMMIT');
      return result;
    } catch (error) {
      this.lastError = error;
      await context.queryTail.catch(() => {});
      await client.query('ROLLBACK').catch(() => {});
      throw error;
    } finally {
      if (checkedOut) client.release();
    }
  }

  normalizeForPersistence(input) {
    const state = normalizeOperatorState(input);
    return {
      ...state,
      audit: completeAuditChain(state.audit || []),
    };
  }

  async loadPersistedAuditChain() {
    const result = await this.query(
      `SELECT id, action, actor, at, details, payload_json,
              previous_hash, event_hash, sequence_number
       FROM audit_events
       ORDER BY sequence_number ASC NULLS LAST, at ASC, id ASC`,
    );
    const events = (result.rows || []).map(mapAuditEvent);
    const metadataCount = events.filter(hasAuditMetadata).length;
    if (metadataCount > 0 && metadataCount !== events.length) {
      throw new Error('audit_chain_partial_metadata_missing');
    }
    return completeAuditChain(events);
  }

  async loadStateWithAuditMetadata() {
    const state = await super.load();
    const persistedAudit = await this.loadPersistedAuditChain();
    return normalizeOperatorState({
      ...state,
      audit: persistedAudit.length ? persistedAudit : state.audit,
    });
  }

  assertAuditAppendOnly(persisted = [], next = []) {
    if (next.length < persisted.length) throw new Error('audit_chain_append_only_violation');
    for (let index = 0; index < persisted.length; index += 1) {
      const existing = persisted[index];
      const candidate = next[index];
      if (!candidate || candidate.id !== existing.id || candidate.eventHash !== existing.eventHash) {
        throw new Error(`audit_chain_append_only_violation:${existing.id || index}`);
      }
    }
  }

  async persistAuditChain(events = []) {
    for (const event of events) {
      if (!event?.id || !event.eventHash || !Number.isInteger(Number(event.sequenceNumber))) {
        throw new Error(`audit_chain_persistence_metadata_missing:${event?.id || 'unknown'}`);
      }
      await this.query(
        `UPDATE audit_events
         SET previous_hash = $2, event_hash = $3, sequence_number = $4
         WHERE id = $1`,
        [event.id, event.previousHash || null, event.eventHash, Number(event.sequenceNumber)],
      );
    }
  }

  async appendAuditEventTargeted(input = {}, options = {}) {
    const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
    const id = requiredText('audit_event_id', input.id);
    const existing = await this.query(
      `SELECT id, action, actor, at, details, payload_json,
              previous_hash, event_hash, sequence_number
       FROM audit_events WHERE id = $1`,
      [id],
    );
    if (existing.rows?.[0]) return { event: mapAuditEvent(existing.rows[0]), idempotent: true };

    const previousResult = await this.query(
      `SELECT id, action, actor, at, details, payload_json,
              previous_hash, event_hash, sequence_number
       FROM audit_events
       ORDER BY sequence_number DESC NULLS LAST, at DESC, id DESC
       LIMIT 1
       FOR UPDATE`,
    );
    const previous = mapAuditEvent(previousResult.rows?.[0]);
    if (previous && !previous.eventHash) throw new Error('audit_chain_previous_hash_missing');
    const event = buildAuditEvent({
      ...input,
      id,
      action: requiredText('audit_event_action', input.action),
      at: input.at || now,
      actor: input.actor || 'system',
      payload: input.payload || {},
    }, previous);

    const inserted = await this.query(
      `INSERT INTO audit_events (
         id, action, actor, at, details, payload_json,
         previous_hash, event_hash, sequence_number
       ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9)
       ON CONFLICT (id) DO NOTHING
       RETURNING id, action, actor, at, details, payload_json,
                 previous_hash, event_hash, sequence_number`,
      [
        event.id,
        event.action,
        event.actor,
        event.at,
        event.details || null,
        JSON.stringify(event.payload || {}),
        event.previousHash || null,
        event.eventHash,
        Number(event.sequenceNumber),
      ],
    );
    if (inserted.rows?.[0]) return { event: mapAuditEvent(inserted.rows[0]), idempotent: false };

    const conflicted = await this.query(
      `SELECT id, action, actor, at, details, payload_json,
              previous_hash, event_hash, sequence_number
       FROM audit_events WHERE id = $1`,
      [id],
    );
    if (!conflicted.rows?.[0]) throw new Error(`audit_event_insert_conflict:${id}`);
    return { event: mapAuditEvent(conflicted.rows[0]), idempotent: true };
  }

  async loadExecutionEventsForReadModel(limit = Number(process.env.EXECUTION_READ_MODEL_EVENT_LIMIT || 5000)) {
    const boundedLimit = Math.max(1, Math.min(50000, Math.floor(Number(limit) || 5000)));
    const result = await this.query(
      'SELECT * FROM execution_events ORDER BY sequence_number DESC LIMIT $1',
      [boundedLimit],
    );
    return (result.rows || []).map(mapExecutionEvent).reverse();
  }

  publishExecutionReadModel(state, events = []) {
    const executions = Array.isArray(state?.executions) ? clone(state.executions) : [];
    const durableEvents = Array.isArray(events) ? clone(events) : [];
    const finalEvent = durableEvents.at(-1);
    globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__ = {
      source: 'postgres-transactional-operator-state',
      revision: `${Date.now()}:${executions.length}:${finalEvent?.sequenceNumber || 0}:${executions.map(row => `${row.id}:${row.status}:${row.updatedAt || row.lastHeartbeatAt || ''}`).join('|')}`,
      publishedAt: new Date().toISOString(),
      executions,
      events: durableEvents,
    };
  }

  async load() {
    return this.withTransaction(async () => {
      const state = await this.loadStateWithAuditMetadata();
      await this.persistAuditChain(state.audit || []);
      const events = await this.loadExecutionEventsForReadModel();
      this.state = state;
      this.publishExecutionReadModel(state, events);
      return state;
    });
  }

  async synchronizeExecutions(executions = [], now = new Date().toISOString()) {
    this.lastExecutionSync = await synchronizeCompatibilityExecutions(this.executionRepository, executions, { now });
    return this.lastExecutionSync;
  }

  async persistExecutionMutation({ execution, auditEvent = null } = {}, options = {}) {
    if (!execution?.id) throw new Error('execution_mutation_execution_required');
    const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
    return this.withTransaction(async () => {
      const synchronization = await this.synchronizeExecutions([execution], now);
      if (!synchronization.ok) throw new Error(`execution_targeted_sync_divergence:${execution.id}`);
      const auditResult = auditEvent
        ? await this.appendAuditEventTargeted(auditEvent, { now })
        : null;

      const state = normalizeOperatorState(clone(this.state || {}));
      state.executions = Array.isArray(state.executions) ? state.executions : [];
      const executionIndex = state.executions.findIndex(row => row.id === execution.id);
      if (executionIndex >= 0) state.executions[executionIndex] = clone(execution);
      else state.executions.push(clone(execution));
      if (auditResult?.event && !state.audit.some(row => row.id === auditResult.event.id)) {
        state.audit.push(clone(auditResult.event));
      }

      const events = await this.loadExecutionEventsForReadModel();
      this.state = state;
      this.publishExecutionReadModel(state, events);
      return {
        execution: clone(execution),
        synchronization,
        auditEvent: auditResult?.event || null,
        auditIdempotent: auditResult?.idempotent || false,
        persistence: 'targeted-optimistic',
      };
    });
  }

  async save(nextState) {
    const state = this.normalizeForPersistence(nextState);
    return this.withTransaction(async () => {
      const persistedAudit = await this.loadPersistedAuditChain();
      this.assertAuditAppendOnly(persistedAudit, state.audit || []);
      const saved = await super.save(state);
      await this.persistAuditChain(saved.audit || []);
      await this.synchronizeExecutions(saved.executions || []);
      const events = await this.loadExecutionEventsForReadModel();
      this.state = saved;
      this.publishExecutionReadModel(saved, events);
      return saved;
    });
  }

  async mutate(mutator) {
    return this.withTransaction(async () => {
      const state = await this.loadStateWithAuditMetadata();
      const persistedAudit = clone(state.audit || []);
      const result = await mutator(state);
      const normalized = this.normalizeForPersistence(state);
      this.assertAuditAppendOnly(persistedAudit, normalized.audit || []);
      await super.save(normalized);
      await this.persistAuditChain(normalized.audit || []);
      await this.synchronizeExecutions(normalized.executions || []);
      const events = await this.loadExecutionEventsForReadModel();
      this.state = normalized;
      this.publishExecutionReadModel(normalized, events);
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
      queryScheduling: 'pinned-client-serialized',
      mutationSerialization: 'postgres-advisory-xact-lock',
      operatorWriteLockKey: this.operatorWriteLockKey,
      runtimeJobQueue: 'lease-backed-postgres',
      executionPersistence: 'normalized-optimistic-postgres',
      executionRoutePersistence: 'targeted-optimistic-with-append-only-audit',
      executionEvents: 'append-only-postgres',
      auditEvents: 'hash-chained-append-only-postgres',
      executionCompatibilitySync: 'targeted-routes-and-compatibility-boundaries',
      executionReadModel: 'postgres-published-compatibility-with-events',
      lastExecutionSync: this.lastExecutionSync,
      activeTransaction: Boolean(this.currentTransaction()),
    };
  }
}
