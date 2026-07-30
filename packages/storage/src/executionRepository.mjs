import { randomUUID } from 'node:crypto';

export const EXECUTION_STATUSES = Object.freeze([
  'draft',
  'approved',
  'rejected',
  'submitted',
  'partially_filled',
  'filled',
  'settlement_pending',
  'settled',
  'failed',
  'cancelled',
]);

const TERMINAL_STATUSES = new Set(['rejected', 'settled', 'failed', 'cancelled']);
const TRANSITIONS = Object.freeze({
  draft: new Set(['approved', 'rejected', 'cancelled']),
  approved: new Set(['submitted', 'cancelled', 'failed']),
  submitted: new Set(['partially_filled', 'filled', 'failed', 'cancelled']),
  partially_filled: new Set(['filled', 'failed', 'cancelled']),
  filled: new Set(['settlement_pending', 'settled', 'failed']),
  settlement_pending: new Set(['settled', 'failed']),
  rejected: new Set(),
  settled: new Set(),
  failed: new Set(),
  cancelled: new Set(),
});

export class ExecutionRepositoryError extends Error {
  constructor(code, details = {}) {
    super(code);
    this.name = 'ExecutionRepositoryError';
    this.code = code;
    this.details = details;
  }
}

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function json(value, fallback = {}) {
  if (value == null) return fallback;
  if (typeof value === 'object') return value;
  try { return JSON.parse(value); } catch { return fallback; }
}

function requireText(name, value) {
  const text = String(value || '').trim();
  if (!text) throw new ExecutionRepositoryError(`${name}_required`);
  return text;
}

export function isExecutionTransitionAllowed(fromStatus, toStatus) {
  if (!EXECUTION_STATUSES.includes(fromStatus) || !EXECUTION_STATUSES.includes(toStatus)) return false;
  return TRANSITIONS[fromStatus]?.has(toStatus) === true;
}

export function normalizeExecutionRecord(input = {}, now = new Date().toISOString()) {
  const mode = input.mode === 'live' ? 'live' : 'paper';
  const side = String(input.side || '').toLowerCase();
  if (!['buy', 'sell'].includes(side)) throw new ExecutionRepositoryError('execution_side_invalid');
  const status = input.status || 'draft';
  if (!EXECUTION_STATUSES.includes(status)) throw new ExecutionRepositoryError('execution_status_invalid');
  const quantity = finite(input.quantity, null);
  const notionalUsd = finite(input.notionalUsd ?? input.notional, null);
  const requestedPrice = finite(input.requestedPrice ?? input.price ?? input.entryPrice, null);
  return {
    id: requireText('execution_id', input.id || `execution-${randomUUID()}`),
    idempotencyKey: requireText('execution_idempotency_key', input.idempotencyKey || input.id),
    opportunityId: input.opportunityId || null,
    strategyId: input.strategyId || null,
    sourceAgentId: input.sourceAgentId || null,
    economicDecisionId: input.economicDecisionId || null,
    modelQuoteId: input.modelQuoteId || null,
    forecastId: input.forecastId || null,
    executionCostSnapshotId: input.executionCostSnapshotId || null,
    symbol: requireText('execution_symbol', input.symbol),
    venue: requireText('execution_venue', input.venue),
    mode,
    side,
    status,
    version: Math.max(1, Math.floor(finite(input.version, 1))),
    quantity,
    notionalUsd,
    requestedPrice,
    netExecutableEdgeUsd: finite(input.netExecutableEdgeUsd, null),
    metadata: {
      tradePlan: input.tradePlan || null,
      tradeIntent: input.tradeIntent || null,
      executionPurpose: input.executionPurpose || null,
      positionSide: input.positionSide || null,
      confidenceScore: finite(input.confidenceScore, null),
      convictionWeight: finite(input.convictionWeight, null),
      riskDecision: input.riskDecision || null,
      tags: input.tags || {},
      ...(input.metadata || {}),
    },
    createdAt: input.createdAt || now,
    updatedAt: input.updatedAt || now,
    terminalAt: TERMINAL_STATUSES.has(status) ? (input.terminalAt || now) : null,
  };
}

function mapExecution(row) {
  if (!row) return null;
  return {
    id: row.id,
    idempotencyKey: row.idempotency_key,
    opportunityId: row.opportunity_id,
    strategyId: row.strategy_id,
    sourceAgentId: row.source_agent_id,
    economicDecisionId: row.economic_decision_id,
    modelQuoteId: row.model_quote_id,
    forecastId: row.forecast_id,
    executionCostSnapshotId: row.execution_cost_snapshot_id,
    symbol: row.symbol,
    venue: row.venue,
    mode: row.mode,
    side: row.side,
    status: row.status,
    version: Number(row.version),
    quantity: finite(row.quantity, null),
    notionalUsd: finite(row.notional_usd, null),
    requestedPrice: finite(row.requested_price, null),
    netExecutableEdgeUsd: finite(row.net_executable_edge_usd, null),
    metadata: json(row.metadata_json),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
    terminalAt: row.terminal_at,
  };
}

function mapEvent(row) {
  if (!row) return null;
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
    payload: json(row.payload_json),
    createdAt: row.created_at,
  };
}

function mapOrder(row) {
  if (!row) return null;
  return {
    id: row.id,
    executionId: row.execution_id,
    clientOrderId: row.client_order_id,
    venueOrderId: row.venue_order_id,
    idempotencyKey: row.idempotency_key,
    side: row.side,
    orderType: row.order_type,
    timeInForce: row.time_in_force,
    quantity: finite(row.quantity, null),
    limitPrice: finite(row.limit_price, null),
    status: row.status,
    request: json(row.request_json),
    response: json(row.response_json, null),
    createdAt: row.created_at,
    updatedAt: row.updated_at,
  };
}

function mapFill(row) {
  if (!row) return null;
  return {
    id: row.id,
    executionId: row.execution_id,
    orderId: row.order_id,
    venueFillId: row.venue_fill_id,
    idempotencyKey: row.idempotency_key,
    quantity: finite(row.quantity, 0),
    price: finite(row.price, 0),
    feeUsd: finite(row.fee_usd, 0),
    liquidity: row.liquidity,
    settlementStatus: row.settlement_status,
    metadata: json(row.metadata_json),
    filledAt: row.filled_at,
    createdAt: row.created_at,
  };
}

export class ExecutionRepository {
  constructor(store) {
    if (!store || typeof store.query !== 'function') throw new ExecutionRepositoryError('execution_repository_store_required');
    this.store = store;
  }

  async transaction(operation) {
    if (typeof this.store.withTransaction === 'function') return this.store.withTransaction(() => operation());
    return operation();
  }

  async get(executionId, { forUpdate = false } = {}) {
    const result = await this.store.query(
      `SELECT * FROM execution_records WHERE id = $1${forUpdate ? ' FOR UPDATE' : ''}`,
      [executionId],
    );
    return mapExecution(result.rows?.[0]);
  }

  async getByIdempotencyKey(idempotencyKey) {
    const result = await this.store.query('SELECT * FROM execution_records WHERE idempotency_key = $1', [idempotencyKey]);
    return mapExecution(result.rows?.[0]);
  }

  async list({ status, symbol, limit = 100 } = {}) {
    const filters = [];
    const params = [];
    if (status) {
      params.push(status);
      filters.push(`status = $${params.length}`);
    }
    if (symbol) {
      params.push(symbol);
      filters.push(`symbol = $${params.length}`);
    }
    params.push(Math.max(1, Math.min(1000, Math.floor(finite(limit, 100)))));
    const result = await this.store.query(
      `SELECT * FROM execution_records${filters.length ? ` WHERE ${filters.join(' AND ')}` : ''} ORDER BY created_at DESC LIMIT $${params.length}`,
      params,
    );
    return (result.rows || []).map(mapExecution);
  }

  async create(input = {}, options = {}) {
    const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
    const record = normalizeExecutionRecord(input, now);
    return this.transaction(async () => {
      const inserted = await this.store.query(`
        INSERT INTO execution_records (
          id, idempotency_key, opportunity_id, strategy_id, source_agent_id,
          economic_decision_id, model_quote_id, forecast_id, execution_cost_snapshot_id,
          symbol, venue, mode, side, status, version, quantity, notional_usd,
          requested_price, net_executable_edge_usd, metadata_json, created_at, updated_at, terminal_at
        ) VALUES (
          $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20::jsonb,$21,$22,$23
        )
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *
      `, [
        record.id,
        record.idempotencyKey,
        record.opportunityId,
        record.strategyId,
        record.sourceAgentId,
        record.economicDecisionId,
        record.modelQuoteId,
        record.forecastId,
        record.executionCostSnapshotId,
        record.symbol,
        record.venue,
        record.mode,
        record.side,
        record.status,
        record.version,
        record.quantity,
        record.notionalUsd,
        record.requestedPrice,
        record.netExecutableEdgeUsd,
        JSON.stringify(record.metadata),
        record.createdAt,
        record.updatedAt,
        record.terminalAt,
      ]);

      if (!inserted.rows?.[0]) {
        const existing = await this.getByIdempotencyKey(record.idempotencyKey);
        if (!existing) throw new ExecutionRepositoryError('execution_idempotency_conflict');
        return { execution: existing, idempotent: true };
      }

      const execution = mapExecution(inserted.rows[0]);
      const event = await this.insertEvent({
        execution,
        type: options.eventType || 'execution_created',
        fromStatus: null,
        toStatus: execution.status,
        actor: options.actor || 'system',
        idempotencyKey: `${record.idempotencyKey}:created`,
        payload: options.payload || {},
        now,
      });
      return { execution, event, idempotent: false };
    });
  }

  async insertEvent({ execution, type, fromStatus, toStatus, actor, idempotencyKey, payload, now }) {
    const result = await this.store.query(`
      INSERT INTO execution_events (
        event_id, execution_id, event_type, from_status, to_status,
        execution_version, actor, idempotency_key, payload_json, created_at
      ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)
      ON CONFLICT (idempotency_key) DO NOTHING
      RETURNING *
    `, [
      `execution-event-${randomUUID()}`,
      execution.id,
      requireText('execution_event_type', type),
      fromStatus || null,
      toStatus || null,
      execution.version,
      actor || 'system',
      requireText('execution_event_idempotency_key', idempotencyKey),
      JSON.stringify(payload || {}),
      now,
    ]);
    if (result.rows?.[0]) return mapEvent(result.rows[0]);
    const existing = await this.store.query('SELECT * FROM execution_events WHERE idempotency_key = $1', [idempotencyKey]);
    return mapEvent(existing.rows?.[0]);
  }

  async transition(input = {}, options = {}) {
    const executionId = requireText('execution_id', input.executionId || input.id);
    const toStatus = requireText('execution_to_status', input.toStatus || input.status);
    const idempotencyKey = requireText('execution_transition_idempotency_key', input.idempotencyKey);
    const expectedVersion = finite(input.expectedVersion, null);
    const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
    return this.transaction(async () => {
      const priorEvent = await this.store.query('SELECT * FROM execution_events WHERE idempotency_key = $1', [idempotencyKey]);
      if (priorEvent.rows?.[0]) {
        return { execution: await this.get(executionId), event: mapEvent(priorEvent.rows[0]), idempotent: true };
      }

      const current = await this.get(executionId, { forUpdate: true });
      if (!current) throw new ExecutionRepositoryError('execution_not_found', { executionId });
      if (expectedVersion != null && current.version !== expectedVersion) {
        throw new ExecutionRepositoryError('execution_version_conflict', { executionId, expectedVersion, actualVersion: current.version });
      }
      if (!isExecutionTransitionAllowed(current.status, toStatus)) {
        throw new ExecutionRepositoryError('execution_transition_invalid', { executionId, fromStatus: current.status, toStatus });
      }

      const nextVersion = current.version + 1;
      const terminalAt = TERMINAL_STATUSES.has(toStatus) ? now : null;
      const updated = await this.store.query(`
        UPDATE execution_records
        SET status = $2,
            version = $3,
            metadata_json = metadata_json || $4::jsonb,
            updated_at = $5,
            terminal_at = COALESCE($6, terminal_at)
        WHERE id = $1 AND version = $7
        RETURNING *
      `, [executionId, toStatus, nextVersion, JSON.stringify(input.metadata || {}), now, terminalAt, current.version]);
      if (!updated.rows?.[0]) {
        throw new ExecutionRepositoryError('execution_version_conflict', { executionId, expectedVersion: current.version });
      }
      const execution = mapExecution(updated.rows[0]);
      const event = await this.insertEvent({
        execution,
        type: input.eventType || `execution_${toStatus}`,
        fromStatus: current.status,
        toStatus,
        actor: input.actor || options.actor || 'system',
        idempotencyKey,
        payload: input.payload || {},
        now,
      });
      return { execution, event, idempotent: false };
    });
  }

  async appendEvent(input = {}, options = {}) {
    const executionId = requireText('execution_id', input.executionId || input.id);
    const idempotencyKey = requireText('execution_event_idempotency_key', input.idempotencyKey);
    const expectedVersion = finite(input.expectedVersion, null);
    const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
    return this.transaction(async () => {
      const current = await this.get(executionId, { forUpdate: true });
      if (!current) throw new ExecutionRepositoryError('execution_not_found', { executionId });
      if (expectedVersion != null && current.version !== expectedVersion) {
        throw new ExecutionRepositoryError('execution_version_conflict', { executionId, expectedVersion, actualVersion: current.version });
      }
      const event = await this.insertEvent({
        execution: current,
        type: input.eventType || input.type,
        fromStatus: current.status,
        toStatus: current.status,
        actor: input.actor || options.actor || 'system',
        idempotencyKey,
        payload: input.payload || {},
        now,
      });
      return { execution: current, event };
    });
  }

  async putOrder(input = {}, options = {}) {
    const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
    const executionId = requireText('execution_id', input.executionId);
    const idempotencyKey = requireText('execution_order_idempotency_key', input.idempotencyKey || input.clientOrderId || input.id);
    const side = String(input.side || '').toLowerCase();
    if (!['buy', 'sell'].includes(side)) throw new ExecutionRepositoryError('execution_order_side_invalid');
    return this.transaction(async () => {
      const execution = await this.get(executionId, { forUpdate: true });
      if (!execution) throw new ExecutionRepositoryError('execution_not_found', { executionId });
      const inserted = await this.store.query(`
        INSERT INTO execution_orders (
          id, execution_id, client_order_id, venue_order_id, idempotency_key,
          side, order_type, time_in_force, quantity, limit_price, status,
          request_json, response_json, created_at, updated_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13::jsonb,$14,$15)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *
      `, [
        input.id || `execution-order-${randomUUID()}`,
        executionId,
        input.clientOrderId || null,
        input.venueOrderId || null,
        idempotencyKey,
        side,
        input.orderType || 'market',
        input.timeInForce || null,
        finite(input.quantity, null),
        finite(input.limitPrice ?? input.price, null),
        input.status || 'planned',
        JSON.stringify(input.request || {}),
        input.response == null ? null : JSON.stringify(input.response),
        now,
        now,
      ]);
      if (inserted.rows?.[0]) return { order: mapOrder(inserted.rows[0]), idempotent: false };
      const existing = await this.store.query('SELECT * FROM execution_orders WHERE idempotency_key = $1', [idempotencyKey]);
      return { order: mapOrder(existing.rows?.[0]), idempotent: true };
    });
  }

  async recordFill(input = {}, options = {}) {
    const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
    const executionId = requireText('execution_id', input.executionId);
    const idempotencyKey = requireText('execution_fill_idempotency_key', input.idempotencyKey || input.venueFillId || input.id);
    const quantity = finite(input.quantity, null);
    const price = finite(input.price, null);
    if (quantity == null || quantity <= 0) throw new ExecutionRepositoryError('execution_fill_quantity_invalid');
    if (price == null || price <= 0) throw new ExecutionRepositoryError('execution_fill_price_invalid');
    return this.transaction(async () => {
      const execution = await this.get(executionId, { forUpdate: true });
      if (!execution) throw new ExecutionRepositoryError('execution_not_found', { executionId });
      const inserted = await this.store.query(`
        INSERT INTO execution_fills (
          id, execution_id, order_id, venue_fill_id, idempotency_key,
          quantity, price, fee_usd, liquidity, settlement_status,
          metadata_json, filled_at, created_at
        ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb,$12,$13)
        ON CONFLICT (idempotency_key) DO NOTHING
        RETURNING *
      `, [
        input.id || `execution-fill-${randomUUID()}`,
        executionId,
        input.orderId || null,
        input.venueFillId || null,
        idempotencyKey,
        quantity,
        price,
        finite(input.feeUsd ?? input.fee, 0),
        input.liquidity || null,
        input.settlementStatus || 'pending',
        JSON.stringify(input.metadata || {}),
        input.filledAt || now,
        now,
      ]);
      if (inserted.rows?.[0]) return { fill: mapFill(inserted.rows[0]), idempotent: false };
      const existing = await this.store.query('SELECT * FROM execution_fills WHERE idempotency_key = $1', [idempotencyKey]);
      return { fill: mapFill(existing.rows?.[0]), idempotent: true };
    });
  }

  async loadBundle(executionId) {
    const [execution, events, orders, fills] = await Promise.all([
      this.get(executionId),
      this.store.query('SELECT * FROM execution_events WHERE execution_id = $1 ORDER BY sequence_number ASC', [executionId]),
      this.store.query('SELECT * FROM execution_orders WHERE execution_id = $1 ORDER BY created_at ASC', [executionId]),
      this.store.query('SELECT * FROM execution_fills WHERE execution_id = $1 ORDER BY filled_at ASC', [executionId]),
    ]);
    if (!execution) return null;
    return {
      execution,
      events: (events.rows || []).map(mapEvent),
      orders: (orders.rows || []).map(mapOrder),
      fills: (fills.rows || []).map(mapFill),
    };
  }
}
