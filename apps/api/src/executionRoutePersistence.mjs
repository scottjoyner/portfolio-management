import { createHash } from 'node:crypto';

function routeMatch(pathname, pattern) {
  const pathParts = pathname.split('/').filter(Boolean);
  const patternParts = pattern.split('/').filter(Boolean);
  if (pathParts.length !== patternParts.length) return null;
  const params = {};
  for (let index = 0; index < patternParts.length; index += 1) {
    const patternPart = patternParts[index];
    if (patternPart.startsWith(':')) params[patternPart.slice(1)] = decodeURIComponent(pathParts[index]);
    else if (patternPart !== pathParts[index]) return null;
  }
  return params;
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    return `{${Object.keys(value).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  return JSON.stringify(value);
}

function normalizeTradePlan(input = {}) {
  const tradePlan = input.tradePlan && typeof input.tradePlan === 'object' ? input.tradePlan : {};
  const entryPrice = Number(input.entryPrice ?? tradePlan.entry_price ?? input.price ?? input.orders?.[0]?.price ?? 0);
  const takeProfitPrice = Number(input.takeProfitPrice ?? tradePlan.take_profit_price ?? input.orders?.[0]?.takeProfitPrice ?? 0);
  const stopLossPrice = Number(input.stopLossPrice ?? tradePlan.stop_loss_price ?? input.orders?.[0]?.stopLossPrice ?? 0);
  const executionPurpose = input.executionPurpose ?? tradePlan.execution_purpose ?? null;
  const tradeIntent = input.tradeIntent ?? tradePlan.plan_type ?? null;
  const positionSide = input.positionSide ?? tradePlan.position_side ?? null;

  if (!Number.isFinite(entryPrice) || entryPrice <= 0) return { errors: ['entry_price_required'] };
  if (!Number.isFinite(takeProfitPrice) || takeProfitPrice <= 0) return { errors: ['take_profit_price_required'] };
  if (!Number.isFinite(stopLossPrice) || stopLossPrice <= 0) return { errors: ['stop_loss_price_required'] };

  const normalizedPlan = {
    ...tradePlan,
    entry_price: entryPrice,
    take_profit_price: takeProfitPrice,
    stop_loss_price: stopLossPrice,
    execution_purpose: executionPurpose,
    plan_type: tradeIntent,
    position_side: positionSide,
  };
  const normalizeOrder = order => ({
    ...order,
    price: Number(order.price ?? entryPrice),
    takeProfitPrice,
    stopLossPrice,
    tradePlan: {
      ...normalizedPlan,
      ...(order.tradePlan && typeof order.tradePlan === 'object' ? order.tradePlan : {}),
    },
  });

  return {
    ...input,
    entryPrice,
    takeProfitPrice,
    stopLossPrice,
    executionPurpose,
    tradeIntent,
    positionSide,
    tradePlan: normalizedPlan,
    orders: Array.isArray(input.orders) && input.orders.length
      ? input.orders.map(normalizeOrder)
      : [normalizeOrder({
        id: `ord-${Date.now()}`,
        side: String(input.side || 'buy').toLowerCase() === 'sell' ? 'sell' : 'buy',
        symbol: input.symbol,
        quantity: Number(input.quantity || 0),
        orderType: 'market',
        timeInForce: 'GTC',
      })],
  };
}

function statusForErrors(errors = []) {
  return errors.some(error => String(error).endsWith('_not_found')) ? 404 : 400;
}

function upsertExecution(state, execution) {
  state.executions = Array.isArray(state.executions) ? state.executions : [];
  const index = state.executions.findIndex(row => row.id === execution.id);
  if (index >= 0) state.executions[index] = execution;
  else state.executions.push(execution);
}

function auditEvent(action, execution, payload = {}, now = new Date().toISOString()) {
  const revision = execution?.version
    ?? execution?.updatedAt
    ?? execution?.lastHeartbeatAt
    ?? execution?.status
    ?? 'unknown';
  const digest = createHash('sha256')
    .update(canonicalJson({
      action,
      executionId: execution?.id || null,
      revision,
      payload,
    }))
    .digest('hex')
    .slice(0, 24);
  return {
    id: `audit-execution-${digest}`,
    action,
    actor: 'operator',
    at: now,
    details: execution?.id || null,
    payload,
  };
}

async function previewExecution(result) {
  if (!result.execution?.orders?.length) return;
  const order = result.execution.orders[0];
  const venue = order.venue || 'coinbase';
  try {
    const { getDefaultRegistry } = await import('../../../packages/adapters/src/adapterRegistry.mjs');
    const adapter = getDefaultRegistry().getAdapterForVenue(venue);
    if (adapter?.previewOrder) {
      const preview = await adapter.previewOrder(order);
      if (preview?.ok) result.execution.preview = preview.preview;
    }
  } catch {
    // Preview availability is advisory; execution persistence remains deterministic.
  }
}

async function persistResult({ store, state, result, action, payload = {}, now }) {
  const errors = result?.errors || [];
  if (errors.length || result?.ok === false) {
    return { status: statusForErrors(errors), body: { ok: false, errors: errors.length ? errors : ['execution_operation_failed'] } };
  }
  if (!result?.execution) return { status: 200, body: { ok: true, ...result } };

  const event = action ? auditEvent(action, result.execution, payload, now) : null;
  if (typeof store?.persistExecutionMutation === 'function') {
    await store.persistExecutionMutation({ execution: result.execution, auditEvent: event }, { now });
    if (state) {
      upsertExecution(state, result.execution);
      if (event) {
        state.audit = Array.isArray(state.audit) ? state.audit : [];
        if (!state.audit.some(row => row.id === event.id)) state.audit.push(event);
      }
    }
    return { status: 200, body: { ok: true, ...result, persistence: 'targeted-optimistic' } };
  }

  const mutation = await store.mutate(async current => {
    upsertExecution(current, result.execution);
    if (event) {
      current.audit = Array.isArray(current.audit) ? current.audit : [];
      if (!current.audit.some(row => row.id === event.id)) current.audit.push(event);
    }
    return result;
  });
  const mutationErrors = mutation?.errors || [];
  if (mutationErrors.length) {
    return { status: statusForErrors(mutationErrors), body: { ok: false, errors: mutationErrors } };
  }
  return { status: 200, body: { ok: true, ...mutation, persistence: 'compatibility-state' } };
}

export async function handleTargetedExecutionRoute({
  method,
  pathname,
  state,
  store,
  readJsonBody,
  getExecutionEngine,
}) {
  const executionApprove = routeMatch(pathname, '/api/execution/:id/approve');
  const executionReject = routeMatch(pathname, '/api/execution/:id/reject');
  const executionCancel = routeMatch(pathname, '/api/execution/:id/cancel');
  const isExecute = method === 'POST' && pathname === '/api/execution/execute';
  const isLifecycleMutation = method === 'POST' && (executionApprove || executionReject || executionCancel);
  if (!isExecute && !isLifecycleMutation) return null;

  const engine = await getExecutionEngine();
  const now = new Date().toISOString();

  if (isExecute) {
    const body = await readJsonBody();
    const normalized = normalizeTradePlan(body);
    if (normalized.errors) return { status: 400, body: { ok: false, errors: normalized.errors } };
    const result = await engine.execute(normalized);
    await previewExecution(result);
    return persistResult({
      store,
      state,
      result,
      action: 'execution_submitted',
      payload: {
        strategyId: result.execution?.strategyId || null,
        status: result.execution?.status || null,
        confidenceScore: result.execution?.confidenceScore ?? null,
      },
      now,
    });
  }

  if (executionApprove) {
    const result = await engine.approve(executionApprove.id);
    return persistResult({ store, state, result, action: 'execution_approved', now });
  }

  if (executionReject) {
    const body = await readJsonBody();
    const result = await engine.reject(executionReject.id, body.reason);
    return persistResult({
      store,
      state,
      result,
      action: 'execution_rejected',
      payload: { reason: body.reason || null },
      now,
    });
  }

  const result = await engine.cancel(executionCancel.id);
  return persistResult({ store, state, result, action: 'execution_cancelled', now });
}
