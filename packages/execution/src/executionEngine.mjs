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
    const overallScore = request.orders[0]?.confidenceScore ?? 0.5;
    const riskDecision = { approved: true, reasons: [] };
    const convictionWeight = 0.5 + overallScore * 0.5;
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
    };
  }

  async execute(request) {
    const plan = await this.plan(request);
    if (!plan.approved) {
      const reasons = [];
      if (plan.confidenceScore < this.minConfidence) reasons.push(`confidence_below_threshold`);
      if (!plan.riskDecision.approved) reasons.push(...plan.riskDecision.reasons);
      const state = this.createState(request, plan);
      return { ok: false, execution: state, errors: reasons };
    }

    let state = this.createState(request, plan);
    this.executions.set(state.id, state);
    this.emit({ executionId: state.id, type: 'created' });

    if (this.requireApproval) return { ok: true, execution: state, warnings: ['awaiting_approval'] };

    return this.submit(state);
  }

  async approve(executionId) {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    if (state.status !== 'draft') return { ok: false, execution: state, errors: [`invalid_status: ${state.status}`] };
    state.status = 'approved';
    this.emit({ executionId, type: 'approved' });
    return this.submit(state);
  }

  async reject(executionId, reason) {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    state.status = 'rejected';
    state.error = reason || 'rejected_by_operator';
    this.emit({ executionId, type: 'rejected' });
    return { ok: false, execution: state, errors: [state.error] };
  }

  async cancel(executionId) {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    if (!validateTransition(state.status, 'cancelled')) return { ok: false, execution: state, errors: [`cannot_cancel: ${state.status}`] };
    state.status = 'cancelled';
    state.completedAt = new Date().toISOString();
    this.emit({ executionId, type: 'cancelled' });
    return { ok: true, execution: state };
  }

  async submit(state) {
    try {
      state.status = 'submitted';
      state.lastHeartbeatAt = new Date().toISOString();
      this.emit({ executionId: state.id, type: 'submitted' });

      for (const order of state.orders) {
        await this.delay(100);
        const fill = {
          id: `fill-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          orderId: state.id,
          marketId: order.marketId,
          venue: order.venue,
          side: order.side,
          quantity: order.quantity,
          price: order.price || 100,
          fee: order.quantity * (order.price || 100) * (order.feeBps || 5) / 10000,
          feeCurrency: 'USD',
          liquidity: 'taker',
          filledAt: new Date().toISOString(),
          settlementStatus: 'settled',
        };
        state.fills.push(fill);
        state.status = 'filled';
        state.completedAt = new Date().toISOString();
        this.emit({ executionId: state.id, type: 'filled' });
      }
      return { ok: true, execution: state };
    } catch (error) {
      state.status = 'failed';
      state.error = String(error);
      state.completedAt = new Date().toISOString();
      this.emit({ executionId: state.id, type: 'failed' });
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
    const id = `exec-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`;
    return {
      id, strategyId: request.strategyId, opportunityId: request.opportunityId,
      accountId: request.accountId, mode: request.mode || 'paper', status: 'draft',
      orders: request.orders,
      tradePlan: request.tradePlan || plan.tradePlan || null,
      tradeIntent: request.tradeIntent || plan.tradeIntent || null,
      executionPurpose: request.executionPurpose || plan.executionPurpose || null,
      positionSide: request.positionSide || plan.positionSide || null,
      entryPrice: request.entryPrice ?? plan.entryPrice ?? request.orders?.[0]?.price ?? null,
      takeProfitPrice: request.takeProfitPrice ?? plan.takeProfitPrice ?? request.orders?.[0]?.takeProfitPrice ?? null,
      stopLossPrice: request.stopLossPrice ?? plan.stopLossPrice ?? request.orders?.[0]?.stopLossPrice ?? null,
      fills: [],
      confidenceScore: plan.confidenceScore, convictionWeight: plan.convictionWeight,
      riskDecision: plan.riskDecision,
      startedAt: new Date().toISOString(), lastHeartbeatAt: new Date().toISOString(),
    };
  }

  getAllEvents() { return [...this.events]; }

  emit(event) {
    const e = { id: `evt-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`, ...event, timestamp: new Date().toISOString() };
    this.events.push(e);
  }

  delay(ms) { return new Promise(r => setTimeout(r, ms)); }
}
