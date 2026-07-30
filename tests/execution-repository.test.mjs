import test from 'node:test';
import assert from 'node:assert/strict';

import {
  ExecutionRepository,
  ExecutionRepositoryError,
  isExecutionTransitionAllowed,
  normalizeExecutionRecord,
} from '../packages/storage/src/executionRepository.mjs';

class FakeExecutionStore {
  constructor() {
    this.executions = new Map();
    this.events = new Map();
    this.sequence = 0;
    this.calls = [];
  }

  async withTransaction(operation) {
    return operation();
  }

  async query(sql, params = []) {
    const normalized = String(sql).replace(/\s+/g, ' ').trim();
    this.calls.push({ sql: normalized, params });

    if (normalized.startsWith('INSERT INTO execution_records')) {
      const idempotencyKey = params[1];
      const existing = [...this.executions.values()].find(row => row.idempotency_key === idempotencyKey);
      if (existing) return { rows: [], rowCount: 0 };
      const row = {
        id: params[0],
        idempotency_key: params[1],
        opportunity_id: params[2],
        strategy_id: params[3],
        source_agent_id: params[4],
        economic_decision_id: params[5],
        model_quote_id: params[6],
        forecast_id: params[7],
        execution_cost_snapshot_id: params[8],
        symbol: params[9],
        venue: params[10],
        mode: params[11],
        side: params[12],
        status: params[13],
        version: params[14],
        quantity: params[15],
        notional_usd: params[16],
        requested_price: params[17],
        net_executable_edge_usd: params[18],
        metadata_json: JSON.parse(params[19]),
        created_at: params[20],
        updated_at: params[21],
        terminal_at: params[22],
      };
      this.executions.set(row.id, row);
      return { rows: [row], rowCount: 1 };
    }

    if (normalized.startsWith('SELECT * FROM execution_records WHERE idempotency_key')) {
      const row = [...this.executions.values()].find(value => value.idempotency_key === params[0]);
      return { rows: row ? [row] : [] };
    }

    if (normalized.startsWith('SELECT * FROM execution_records WHERE id =')) {
      const row = this.executions.get(params[0]);
      return { rows: row ? [row] : [] };
    }

    if (normalized.startsWith('INSERT INTO execution_events')) {
      const idempotencyKey = params[7];
      const existing = this.events.get(idempotencyKey);
      if (existing) return { rows: [], rowCount: 0 };
      const row = {
        event_id: params[0],
        execution_id: params[1],
        sequence_number: ++this.sequence,
        event_type: params[2],
        from_status: params[3],
        to_status: params[4],
        execution_version: params[5],
        actor: params[6],
        idempotency_key: idempotencyKey,
        payload_json: JSON.parse(params[8]),
        created_at: params[9],
      };
      this.events.set(idempotencyKey, row);
      return { rows: [row], rowCount: 1 };
    }

    if (normalized.startsWith('SELECT * FROM execution_events WHERE idempotency_key')) {
      const row = this.events.get(params[0]);
      return { rows: row ? [row] : [] };
    }

    if (normalized.startsWith('UPDATE execution_records')) {
      const row = this.executions.get(params[0]);
      if (!row || Number(row.version) !== Number(params[6])) return { rows: [], rowCount: 0 };
      row.status = params[1];
      row.version = params[2];
      row.metadata_json = { ...(row.metadata_json || {}), ...JSON.parse(params[3]) };
      row.updated_at = params[4];
      row.terminal_at = params[5] || row.terminal_at;
      return { rows: [row], rowCount: 1 };
    }

    throw new Error(`unexpected_sql:${normalized}`);
  }
}

test('execution transition matrix fails closed', () => {
  assert.equal(isExecutionTransitionAllowed('draft', 'approved'), true);
  assert.equal(isExecutionTransitionAllowed('approved', 'submitted'), true);
  assert.equal(isExecutionTransitionAllowed('submitted', 'filled'), true);
  assert.equal(isExecutionTransitionAllowed('filled', 'settled'), true);
  assert.equal(isExecutionTransitionAllowed('draft', 'settled'), false);
  assert.equal(isExecutionTransitionAllowed('settled', 'submitted'), false);
});

test('normalization preserves economic lineage and requires explicit idempotency', () => {
  const normalized = normalizeExecutionRecord({
    id: 'exec-001',
    idempotencyKey: 'intent-001',
    opportunityId: 'opp-001',
    economicDecisionId: 'decision-001',
    modelQuoteId: 'quote-001',
    forecastId: 'forecast-001',
    executionCostSnapshotId: 'cost-001',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    side: 'buy',
    notional: 100,
    price: 100000,
  }, '2026-07-30T20:00:00.000Z');

  assert.equal(normalized.mode, 'paper');
  assert.equal(normalized.status, 'draft');
  assert.equal(normalized.version, 1);
  assert.equal(normalized.economicDecisionId, 'decision-001');
  assert.equal(normalized.modelQuoteId, 'quote-001');
  assert.equal(normalized.notionalUsd, 100);
  assert.throws(
    () => normalizeExecutionRecord({ symbol: 'BTC-USD', venue: 'coinbase', side: 'buy' }),
    error => error instanceof ExecutionRepositoryError && error.code === 'execution_idempotency_key_required',
  );
});

test('create and transition are idempotent and version checked', async () => {
  const store = new FakeExecutionStore();
  const repository = new ExecutionRepository(store);
  const created = await repository.create({
    id: 'exec-001',
    idempotencyKey: 'intent-001',
    symbol: 'ETH-USD',
    venue: 'coinbase',
    side: 'buy',
    quantity: 1,
    notionalUsd: 3000,
    requestedPrice: 3000,
  }, { now: '2026-07-30T20:00:00.000Z', actor: 'operator' });

  assert.equal(created.idempotent, false);
  assert.equal(created.execution.status, 'draft');
  assert.equal(created.execution.version, 1);
  assert.equal(created.event.type, 'execution_created');

  const duplicate = await repository.create({
    id: 'different-id',
    idempotencyKey: 'intent-001',
    symbol: 'ETH-USD',
    venue: 'coinbase',
    side: 'buy',
  });
  assert.equal(duplicate.idempotent, true);
  assert.equal(duplicate.execution.id, 'exec-001');

  const approved = await repository.transition({
    executionId: 'exec-001',
    toStatus: 'approved',
    expectedVersion: 1,
    idempotencyKey: 'transition-approve-001',
    actor: 'risk-manager',
  }, { now: '2026-07-30T20:01:00.000Z' });
  assert.equal(approved.execution.status, 'approved');
  assert.equal(approved.execution.version, 2);
  assert.equal(approved.event.fromStatus, 'draft');
  assert.equal(approved.event.toStatus, 'approved');

  const repeated = await repository.transition({
    executionId: 'exec-001',
    toStatus: 'approved',
    expectedVersion: 1,
    idempotencyKey: 'transition-approve-001',
  });
  assert.equal(repeated.idempotent, true);
  assert.equal(repeated.execution.version, 2);

  await assert.rejects(
    repository.transition({
      executionId: 'exec-001',
      toStatus: 'submitted',
      expectedVersion: 1,
      idempotencyKey: 'transition-submit-stale',
    }),
    error => error instanceof ExecutionRepositoryError && error.code === 'execution_version_conflict',
  );

  await assert.rejects(
    repository.transition({
      executionId: 'exec-001',
      toStatus: 'settled',
      expectedVersion: 2,
      idempotencyKey: 'transition-settle-invalid',
    }),
    error => error instanceof ExecutionRepositoryError && error.code === 'execution_transition_invalid',
  );
});
