// Self-contained execution engine for runtime use by the API server

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

export default class ExecutionEngine {
  constructor(config = {}) {
    this.minConfidence = config.minConfidence ?? 0.6;
    this.requireApproval = config.requireApproval !== false;
    this.requireRiskCheck = config.requireRiskCheck !== false;
    this.maxRetries = config.maxExecutionRetries ?? 3;
    this.executions = new Map();
    this.events = [];
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
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    if (state.status !== 'draft') return { ok: false, execution: state, errors: [`invalid_status: ${state.status}`] };
    state.status = 'approved';
    this.emit({ executionId, type: 'approved', economicDecisionId: state.economicDecisionId });
    return this.submit(state);
  }

  async reject(executionId, reason) {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    state.status = 'rejected';
    state.error = reason || 'rejected_by_operator';
    this.emit({ executionId, type: 'rejected', economicDecisionId: state.economicDecisionId });
    return { ok: false, execution: state, errors: [state.error] };
  }

  async cancel(executionId) {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    if (!validateTransition(state.status, 'cancelled')) return { ok: false, execution: state, errors: [`cannot_cancel: ${state.status}`] };
    state.status = 'cancelled';
    state.completedAt = new Date().toISOString();
    this.emit({ executionId, type: 'cancelled', economicDecisionId: state.economicDecisionId });
    return { ok: true, execution: state };
  }

  async submit(state) {
    try {
      state.status = 'submitted';
      state.lastHeartbeatAt = new Date().toISOString();
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
        this.emit({ executionId: state.id, type: 'filled', fillId: fill.id, economicDecisionId: state.economicDecisionId });
      }
      return { ok: true, execution: state };
    } catch (error) {
      state.status = 'failed';
      state.error = String(error);
      state.completedAt = new Date().toISOString();
      this.emit({ executionId: state.id, type: 'failed', economicDecisionId: state.economicDecisionId });
      return { ok: false, execution: state, errors: [String(error)] };
    }
  }

  getExecution(id) { return this.executions.get(id); }
  listExecutions(filter) {
    let results = Array.from(this.executions.values());
    if (filter?.strategyId) results = results.filter(e => e.strategyId === filter.strategyId);
    if (filter?.status) results = results.filter(e => e.status === filter.status);
    if (filter?.mode) results = results.filter(e => e.mode === filter.mode);
    return results;
  }
  getEvents(executionId) { return this.events.filter(e => e.executionId === executionId); }

  createState(request, plan) {
    const id = request.executionId || `exec-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    const firstOrder = request.orders?.[0] || {};
    return {
      id,
      strategyId: request.strategyId,
      opportunityId: request.opportunityId,
      sourceAgentId: request.sourceAgentId || null,
      accountId: request.accountId,
      mode: request.mode || 'paper',
      status: 'draft',
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
      startedAt: new Date().toISOString(),
      lastHeartbeatAt: new Date().toISOString(),
    };
  }

  getAllEvents() { return [...this.events]; }

  emit(event) {
    const e = { id: `evt-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`, ...event, timestamp: new Date().toISOString() };
    this.events.push(e);
  }

  delay(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }
}
