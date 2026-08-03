// Self-contained compatibility execution engine for runtime use by the API server.
// PostgreSQL remains the durable source of truth; this engine hydrates its
// compatibility map from the read model published by the transactional store.

const VALID_TRANSITIONS = {
  draft: ['approved', 'rejected', 'cancelled'],
  approved: ['submitted', 'rejected'],
  rejected: [],
  submitted: ['partially_filled', 'filled', 'cancelled', 'failed'],
  partially_filled: ['filled', 'cancelled', 'failed'],
  filled: [],
  cancelled: [],
  expired: [],
  failed: [],
};

function validateTransition(from, to) {
  return VALID_TRANSITIONS[from]?.includes(to) ?? false;
}

function clone(value) {
  if (typeof structuredClone === 'function') return structuredClone(value);
  return JSON.parse(JSON.stringify(value));
}

function stateTimestamp(value) {
  const candidate = value?.updatedAt
    || value?.lastHeartbeatAt
    || value?.completedAt
    || value?.settledAt
    || value?.startedAt
    || value?.createdAt;
  const timestamp = candidate ? new Date(candidate).getTime() : 0;
  return Number.isFinite(timestamp) ? timestamp : 0;
}

function normalizeDurableExecution(execution = {}) {
  return {
    ...clone(execution),
    orders: Array.isArray(execution.orders) ? clone(execution.orders) : [],
    fills: Array.isArray(execution.fills) ? clone(execution.fills) : [],
    tags: execution.tags && typeof execution.tags === 'object' ? clone(execution.tags) : {},
  };
}

function durableReadModel() {
  const model = globalThis.__PORTFOLIO_EXECUTION_READ_MODEL__;
  if (!model || model.source !== 'postgres-transactional-operator-state') return null;
  if (!Array.isArray(model.executions)) return null;
  return model;
}

export default class ExecutionEngine {
  constructor(config = {}) {
    this.minConfidence = config.minConfidence ?? 0.6;
    this.requireApproval = config.requireApproval !== false;
    this.requireRiskCheck = config.requireRiskCheck !== false;
    this.maxRetries = config.maxExecutionRetries ?? 3;
    this.executions = new Map();
    this.events = [];
    this.lastHydratedRevision = null;
    this.lastHydratedAt = null;
  }

  hydrateDurableReadModel() {
    const model = durableReadModel();
    if (!model || model.revision === this.lastHydratedRevision) {
      return { hydrated: false, revision: this.lastHydratedRevision, executionCount: this.executions.size };
    }

    let imported = 0;
    let replaced = 0;
    let retainedNewerLocal = 0;
    for (const durableExecution of model.executions) {
      if (!durableExecution?.id) continue;
      const incoming = normalizeDurableExecution(durableExecution);
      const current = this.executions.get(incoming.id);
      if (!current) {
        this.executions.set(incoming.id, incoming);
        imported += 1;
        continue;
      }
      if (stateTimestamp(incoming) > stateTimestamp(current)) {
        this.executions.set(incoming.id, incoming);
        replaced += 1;
      } else {
        retainedNewerLocal += 1;
      }
    }

    if (Array.isArray(model.events) && model.events.length) {
      const known = new Set(this.events.map(event => event?.id).filter(Boolean));
      for (const event of model.events) {
        if (!event?.id || known.has(event.id)) continue;
        this.events.push(clone(event));
        known.add(event.id);
      }
      this.events.sort((a, b) => new Date(a.timestamp || a.createdAt || 0) - new Date(b.timestamp || b.createdAt || 0));
    }

    this.lastHydratedRevision = model.revision;
    this.lastHydratedAt = new Date().toISOString();
    return {
      hydrated: true,
      revision: model.revision,
      publishedAt: model.publishedAt || null,
      imported,
      replaced,
      retainedNewerLocal,
      executionCount: this.executions.size,
      eventCount: this.events.length,
    };
  }

  async plan(request) {
    const overallScore = request.orders[0]?.confidenceScore ?? request.confidenceScore ?? 0.5;
    const riskDecision = request.riskDecision || { approved: true, reasons: [] };
    const convictionWeight = request.convictionWeight ?? (0.5 + overallScore * 0.5);
    const approved = overallScore >= this.minConfidence && riskDecision.approved;
    return {
      id: `plan-${Date.now()}`,
      requests: [request],
      confidenceScore: overallScore,
      convictionWeight,
      riskDecision,
      createdAt: new Date().toISOString(),
      approved,
      tradePlan: request.tradePlan || null,
      entryPrice: request.entryPrice ?? request.orders?.[0]?.price ?? null,
      takeProfitPrice: request.takeProfitPrice ?? request.orders?.[0]?.takeProfitPrice ?? null,
      stopLossPrice: request.stopLossPrice ?? request.orders?.[0]?.stopLossPrice ?? null,
      tradeIntent: request.tradeIntent || null,
      executionPurpose: request.executionPurpose || null,
      positionSide: request.positionSide || null,
      economicDecisionId: request.economicDecisionId || null,
      modelQuoteId: request.modelQuoteId || null,
      forecastId: request.forecastId || null,
      executionCostSnapshotId: request.executionCostSnapshotId || null,
      netExecutableEdgeUsd: request.netExecutableEdgeUsd ?? null,
    };
  }

  async execute(request) {
    this.hydrateDurableReadModel();
    const plan = await this.plan(request);
    if (!plan.approved) {
      const reasons = [];
      if (plan.confidenceScore < this.minConfidence) reasons.push('confidence_below_threshold');
      if (!plan.riskDecision.approved) reasons.push(...plan.riskDecision.reasons);
      const state = this.createState(request, plan);
      return { ok: false, execution: state, errors: reasons };
    }

    const state = this.createState(request, plan);
    this.executions.set(state.id, state);
    this.emit({ executionId: state.id, type: 'created', economicDecisionId: state.economicDecisionId });

    if (this.requireApproval) return { ok: true, execution: state, warnings: ['awaiting_approval'] };
    return this.submit(state);
  }

  async approve(executionId) {
    this.hydrateDurableReadModel();
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    if (state.status !== 'draft') return { ok: false, execution: state, errors: [`invalid_status: ${state.status}`] };
    state.status = 'approved';
    state.updatedAt = new Date().toISOString();
    this.emit({ executionId, type: 'approved', economicDecisionId: state.economicDecisionId });
    return this.submit(state);
  }

  async reject(executionId, reason) {
    this.hydrateDurableReadModel();
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    state.status = 'rejected';
    state.error = reason || 'rejected_by_operator';
    state.completedAt = new Date().toISOString();
    state.updatedAt = state.completedAt;
    this.emit({ executionId, type: 'rejected', economicDecisionId: state.economicDecisionId });
    return { ok: false, execution: state, errors: [state.error] };
  }

  async cancel(executionId) {
    this.hydrateDurableReadModel();
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    if (!validateTransition(state.status, 'cancelled')) return { ok: false, execution: state, errors: [`cannot_cancel: ${state.status}`] };
    state.status = 'cancelled';
    state.completedAt = new Date().toISOString();
    state.updatedAt = state.completedAt;
    this.emit({ executionId, type: 'cancelled', economicDecisionId: state.economicDecisionId });
    return { ok: true, execution: state };
  }

  async submit(state) {
    try {
      state.status = 'submitted';
      state.lastHeartbeatAt = new Date().toISOString();
      state.updatedAt = state.lastHeartbeatAt;
      this.emit({ executionId: state.id, type: 'submitted', economicDecisionId: state.economicDecisionId });

      for (const order of state.orders) {
        await this.delay(100);
        const fill = {
          id: `fill-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          orderId: order.id || state.id,
          executionId: state.id,
          marketId: order.marketId,
          symbol: order.symbol || state.symbol,
          venue: order.venue || state.venue,
          side: order.side || state.side,
          quantity: order.quantity,
          price: order.price || state.entryPrice || 100,
          fee: order.quantity * (order.price || state.entryPrice || 100) * (order.feeBps || 5) / 10000,
          feeCurrency: 'USD',
          liquidity: 'taker',
          filledAt: new Date().toISOString(),
          settlementStatus: 'settled',
          economicDecisionId: state.economicDecisionId,
          modelQuoteId: state.modelQuoteId,
        };
        state.fills.push(fill);
        state.status = 'filled';
        state.completedAt = new Date().toISOString();
        state.lastHeartbeatAt = state.completedAt;
        state.updatedAt = state.completedAt;
        this.emit({ executionId: state.id, type: 'filled', fillId: fill.id, economicDecisionId: state.economicDecisionId });
      }
      return { ok: true, execution: state };
    } catch (error) {
      state.status = 'failed';
      state.error = String(error);
      state.completedAt = new Date().toISOString();
      state.updatedAt = state.completedAt;
      this.emit({ executionId: state.id, type: 'failed', economicDecisionId: state.economicDecisionId });
      return { ok: false, execution: state, errors: [String(error)] };
    }
  }

  getExecution(id) {
    this.hydrateDurableReadModel();
    return this.executions.get(id);
  }

  listExecutions(filter) {
    this.hydrateDurableReadModel();
    let results = Array.from(this.executions.values());
    if (filter?.strategyId) results = results.filter(execution => execution.strategyId === filter.strategyId);
    if (filter?.status) results = results.filter(execution => execution.status === filter.status);
    if (filter?.mode) results = results.filter(execution => execution.mode === filter.mode);
    return results;
  }

  getEvents(executionId) {
    this.hydrateDurableReadModel();
    return this.events.filter(event => event.executionId === executionId);
  }

  createState(request, plan) {
    const id = request.executionId || `exec-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const firstOrder = request.orders?.[0] || {};
    const now = new Date().toISOString();
    return {
      id,
      strategyId: request.strategyId,
      opportunityId: request.opportunityId,
      sourceAgentId: request.sourceAgentId || null,
      accountId: request.accountId,
      mode: request.mode || 'paper',
      status: 'draft',
      version: Number(request.version || 1),
      venue: request.venue || firstOrder.venue || null,
      symbol: request.symbol || firstOrder.symbol || null,
      side: request.side || firstOrder.side || null,
      quantity: request.quantity ?? firstOrder.quantity ?? null,
      notional: request.notional ?? request.notionalUsd ?? null,
      orders: request.orders,
      tradePlan: request.tradePlan || plan.tradePlan || null,
      tradeIntent: request.tradeIntent || plan.tradeIntent || null,
      executionPurpose: request.executionPurpose || plan.executionPurpose || null,
      positionSide: request.positionSide || plan.positionSide || null,
      entryPrice: request.entryPrice ?? plan.entryPrice ?? firstOrder.price ?? null,
      takeProfitPrice: request.takeProfitPrice ?? plan.takeProfitPrice ?? firstOrder.takeProfitPrice ?? null,
      stopLossPrice: request.stopLossPrice ?? plan.stopLossPrice ?? firstOrder.stopLossPrice ?? null,
      fills: [],
      confidenceScore: plan.confidenceScore,
      convictionWeight: plan.convictionWeight,
      riskDecision: plan.riskDecision,
      economicDecisionId: request.economicDecisionId || plan.economicDecisionId || null,
      modelQuoteId: request.modelQuoteId || plan.modelQuoteId || null,
      forecastId: request.forecastId || plan.forecastId || null,
      executionCostSnapshotId: request.executionCostSnapshotId || plan.executionCostSnapshotId || null,
      netExecutableEdgeUsd: request.netExecutableEdgeUsd ?? plan.netExecutableEdgeUsd ?? null,
      counterfactualPnlUsd: request.counterfactualPnlUsd ?? null,
      tags: {
        ...(request.tags || {}),
        economicDecisionId: request.economicDecisionId || plan.economicDecisionId || null,
        modelQuoteId: request.modelQuoteId || plan.modelQuoteId || null,
      },
      createdAt: request.createdAt || now,
      startedAt: now,
      updatedAt: now,
      lastHeartbeatAt: now,
    };
  }

  getAllEvents() {
    this.hydrateDurableReadModel();
    return [...this.events];
  }

  emit(event) {
    const timestamp = new Date().toISOString();
    const emitted = { id: `evt-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`, ...event, timestamp, createdAt: timestamp };
    this.events.push(emitted);
  }

  delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
}
