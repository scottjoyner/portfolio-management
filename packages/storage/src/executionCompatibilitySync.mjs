import { EXECUTION_STATUSES, isExecutionTransitionAllowed } from './executionRepository.mjs';

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function normalizedStatus(value) {
  const status = String(value || 'draft').toLowerCase();
  if (EXECUTION_STATUSES.includes(status)) return status;
  if (status === 'expired') return 'cancelled';
  if (status === 'pending' || status === 'pending_approval' || status === 'planned') return 'draft';
  return null;
}

function revisionFor(execution) {
  return String(
    execution.updatedAt
      || execution.lastHeartbeatAt
      || execution.completedAt
      || execution.settledAt
      || execution.startedAt
      || execution.createdAt
      || `${execution.status || 'unknown'}:${execution.orders?.length || 0}:${execution.fills?.length || 0}`,
  ).replace(/[^a-z0-9_.:-]+/gi, '-');
}

function executionInput(execution) {
  return {
    ...execution,
    status: normalizedStatus(execution.status) || 'draft',
    idempotencyKey: execution.idempotencyKey || `compat-execution:${execution.id}`,
    notionalUsd: finite(execution.notionalUsd ?? execution.notional, null),
    requestedPrice: finite(execution.requestedPrice ?? execution.price ?? execution.entryPrice, null),
    metadata: {
      compatibilitySource: 'operator_state.executions',
      compatibilityRevision: revisionFor(execution),
      compatibilityOriginalStatus: execution.status || null,
      preview: execution.preview || null,
      errors: execution.errors || [],
      settlement: execution.settlement || null,
      ...(execution.metadata || {}),
    },
  };
}

async function snapshotAlreadyRecorded(repository, key) {
  const result = await repository.store.query('SELECT event_id FROM execution_events WHERE idempotency_key = $1', [key]);
  return Boolean(result.rows?.[0]);
}

async function updateSnapshot(repository, execution, current, now) {
  const revision = revisionFor(execution);
  const idempotencyKey = `compat-snapshot:${execution.id}:${revision}`;
  if (await snapshotAlreadyRecorded(repository, idempotencyKey)) {
    return { updated: false, idempotent: true, executionVersion: current.version };
  }
  const nextVersion = current.version + 1;
  const metadata = executionInput(execution).metadata;
  const updated = await repository.store.query(`
    UPDATE execution_records
    SET opportunity_id = COALESCE($2, opportunity_id),
        strategy_id = COALESCE($3, strategy_id),
        source_agent_id = COALESCE($4, source_agent_id),
        economic_decision_id = COALESCE($5, economic_decision_id),
        model_quote_id = COALESCE($6, model_quote_id),
        forecast_id = COALESCE($7, forecast_id),
        execution_cost_snapshot_id = COALESCE($8, execution_cost_snapshot_id),
        quantity = COALESCE($9, quantity),
        notional_usd = COALESCE($10, notional_usd),
        requested_price = COALESCE($11, requested_price),
        net_executable_edge_usd = COALESCE($12, net_executable_edge_usd),
        metadata_json = metadata_json || $13::jsonb,
        version = $14,
        updated_at = $15
    WHERE id = $1 AND version = $16
    RETURNING version
  `, [
    execution.id,
    execution.opportunityId || null,
    execution.strategyId || null,
    execution.sourceAgentId || null,
    execution.economicDecisionId || null,
    execution.modelQuoteId || null,
    execution.forecastId || null,
    execution.executionCostSnapshotId || null,
    finite(execution.quantity, null),
    finite(execution.notionalUsd ?? execution.notional, null),
    finite(execution.requestedPrice ?? execution.price ?? execution.entryPrice, null),
    finite(execution.netExecutableEdgeUsd, null),
    JSON.stringify(metadata),
    nextVersion,
    now,
    current.version,
  ]);
  if (!updated.rows?.[0]) throw new Error(`execution_version_conflict:${execution.id}`);
  await repository.appendEvent({
    executionId: execution.id,
    expectedVersion: nextVersion,
    eventType: 'execution_snapshot_synchronized',
    idempotencyKey,
    actor: 'execution-compatibility-sync',
    payload: {
      compatibilityRevision: revision,
      orderCount: execution.orders?.length || 0,
      fillCount: execution.fills?.length || 0,
    },
  }, { now });
  return { updated: true, idempotent: false, executionVersion: nextVersion };
}

async function synchronizeOne(repository, execution, now) {
  if (!execution?.id || !execution.symbol || !execution.venue || !execution.side) {
    return { executionId: execution?.id || null, skipped: true, reason: 'execution_identity_incomplete' };
  }
  const targetStatus = normalizedStatus(execution.status);
  if (!targetStatus) {
    return { executionId: execution.id, skipped: true, reason: `execution_status_unsupported:${execution.status}` };
  }

  let current = await repository.get(execution.id, { forUpdate: true });
  const created = !current;
  if (!current) {
    const result = await repository.create(executionInput(execution), {
      now,
      actor: 'execution-compatibility-sync',
      eventType: 'execution_imported',
      payload: { compatibilityRevision: revisionFor(execution), originalStatus: execution.status || null },
    });
    current = result.execution;
  }

  let transitioned = null;
  let divergence = null;
  if (current.status !== targetStatus) {
    const transitionKey = `compat-transition:${execution.id}:${current.status}:${targetStatus}:${revisionFor(execution)}`;
    if (isExecutionTransitionAllowed(current.status, targetStatus)) {
      transitioned = await repository.transition({
        executionId: execution.id,
        toStatus: targetStatus,
        expectedVersion: current.version,
        idempotencyKey: transitionKey,
        eventType: `execution_${targetStatus}`,
        actor: 'execution-compatibility-sync',
        payload: { compatibilityRevision: revisionFor(execution), originalStatus: execution.status || null },
      }, { now });
      current = transitioned.execution;
    } else {
      divergence = await repository.appendEvent({
        executionId: execution.id,
        expectedVersion: current.version,
        eventType: 'execution_compatibility_divergence',
        idempotencyKey: transitionKey,
        actor: 'execution-compatibility-sync',
        payload: { durableStatus: current.status, compatibilityStatus: targetStatus, originalStatus: execution.status || null },
      }, { now });
    }
  }

  const snapshot = await updateSnapshot(repository, execution, current, now);
  const orders = [];
  const importedOrderIds = new Set();
  for (let index = 0; index < (execution.orders || []).length; index += 1) {
    const order = execution.orders[index];
    const orderId = order.id || `${execution.id}-order-${index + 1}`;
    const result = await repository.putOrder({
      ...order,
      executionId: execution.id,
      id: orderId,
      idempotencyKey: order.idempotencyKey || `compat-order:${execution.id}:${order.id || index + 1}`,
      side: order.side || execution.side,
      request: order.request || order,
      response: order.response || order.preview || null,
    }, { now });
    if (result.order?.id) importedOrderIds.add(result.order.id);
    orders.push(result);
  }

  const fills = [];
  const skippedFills = [];
  for (let index = 0; index < (execution.fills || []).length; index += 1) {
    const fill = execution.fills[index];
    if (!(finite(fill.quantity, 0) > 0) || !(finite(fill.price, 0) > 0)) {
      skippedFills.push({ index, reason: 'fill_quantity_or_price_invalid' });
      continue;
    }
    const orderId = fill.orderId && importedOrderIds.has(fill.orderId) ? fill.orderId : null;
    if (fill.orderId && !orderId) skippedFills.push({ index, reason: 'orphan_order_reference_removed', orderId: fill.orderId });
    const result = await repository.recordFill({
      ...fill,
      orderId,
      executionId: execution.id,
      id: fill.id || `${execution.id}-fill-${index + 1}`,
      idempotencyKey: fill.idempotencyKey || `compat-fill:${execution.id}:${fill.id || fill.venueFillId || index + 1}`,
      feeUsd: fill.feeUsd ?? fill.fee,
      metadata: { settlement: fill.settlement || null, originalOrderId: fill.orderId || null, ...(fill.metadata || {}) },
    }, { now });
    fills.push(result);
  }

  return {
    executionId: execution.id,
    created,
    transitioned: Boolean(transitioned),
    divergence: Boolean(divergence),
    snapshot,
    ordersImported: orders.length,
    fillsImported: fills.length,
    skippedFills,
  };
}

export async function synchronizeCompatibilityExecutions(repository, executions = [], options = {}) {
  if (!repository) throw new Error('execution_repository_required');
  const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
  const reports = [];
  for (const execution of executions || []) {
    reports.push(await repository.transaction(() => synchronizeOne(repository, execution, now)));
  }
  return {
    ok: reports.every(row => !row.divergence),
    synchronizedAt: now,
    executionCount: reports.length,
    createdCount: reports.filter(row => row.created).length,
    transitionCount: reports.filter(row => row.transitioned).length,
    divergenceCount: reports.filter(row => row.divergence).length,
    skippedCount: reports.filter(row => row.skipped).length,
    reports,
  };
}
