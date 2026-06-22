import type { OrderIntent, ExecutionState, ConfidenceScore, RiskDecision, ExecutionMode, OrderFill, OrderStatus, Venue } from '@pkg/core/types.js';
import type { IBrokerAdapter, PreviewResult, SubmitResult } from '@pkg/adapters/types.js';
import { getDefaultRegistry, AdapterRegistry } from '@pkg/adapters/adapterRegistry.js';
import { ConfidenceScorer, type ScoringInput } from '@pkg/confidence/index.js';
import { createExecutionState, transitionExecutionState, transitionToPartialFill, transitionToFailed, validateTransition } from './executionStateMachine.js';
import type { ExecutionRequest, ExecutionResult, ExecutionPlan, ExecutionStore, ExecutionEvent } from './types.js';
import { evaluateRisk } from '@pkg/risk/engine.js';

export interface ExecutionEngineConfig {
  minConfidence: number;
  requireApproval: boolean;
  requireRiskCheck: boolean;
  maxExecutionRetries: number;
  store?: ExecutionStore;
}

const DEFAULT_CONFIG: ExecutionEngineConfig = {
  minConfidence: 0.6,
  requireApproval: true,
  requireRiskCheck: true,
  maxExecutionRetries: 3,
};

export class ExecutionEngine {
  private config: ExecutionEngineConfig;
  private adapterRegistry: AdapterRegistry;
  private confidenceScorer: ConfidenceScorer;
  private executions: Map<string, ExecutionState> = new Map();
  private events: ExecutionEvent[] = [];

  constructor(config?: Partial<ExecutionEngineConfig>, adapterRegistry?: AdapterRegistry) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.adapterRegistry = adapterRegistry || getDefaultRegistry();
    this.confidenceScorer = new ConfidenceScorer({ minOverall: this.config.minConfidence });
  }

  async plan(request: ExecutionRequest): Promise<ExecutionPlan> {
    const overallScore = request.orders[0]?.confidenceScore ?? 0.5;
    const riskDecision: RiskDecision = { approved: true, reasons: [] };
    const convictionWeight = 0.5 + overallScore * 0.5;

    return {
      id: `plan-${Date.now()}`,
      requests: [request],
      confidenceScore: overallScore,
      convictionWeight,
      riskDecision,
      createdAt: new Date().toISOString(),
      approved: overallScore >= this.config.minConfidence && riskDecision.approved,
    };
  }

  async execute(request: ExecutionRequest): Promise<ExecutionResult> {
    const plan = await this.plan(request);
    if (!plan.approved) {
      const reasons: string[] = [];
      if (plan.confidenceScore < this.config.minConfidence) reasons.push('confidence_below_threshold');
      if (!plan.riskDecision.approved) reasons.push(...plan.riskDecision.reasons);
      const state = this.createState(request, plan);
      return { ok: false, execution: state, errors: reasons };
    }

    let state = this.createState(request, plan);
    this.executions.set(state.id, state);
    this.emit(state.id, 'created');

    if (this.config.requireApproval) {
      return { ok: true, execution: state, warnings: ['awaiting_approval'] };
    }

    return this.submit(state);
  }

  async approve(executionId: string): Promise<ExecutionResult> {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, execution: null as unknown as ExecutionState, errors: ['execution_not_found'] };
    if (state.status !== 'draft') return { ok: false, execution: state, errors: [`invalid_status: ${state.status}`] };
    state.status = 'approved';
    this.emit(executionId, 'approved');
    return this.submit(state);
  }

  async reject(executionId: string, reason?: string): Promise<ExecutionResult> {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, execution: null as unknown as ExecutionState, errors: ['execution_not_found'] };
    state.status = 'rejected';
    state.error = reason || 'rejected_by_operator';
    state.completedAt = new Date().toISOString();
    this.emit(executionId, 'rejected', { reason: state.error });
    return { ok: false, execution: state, errors: [state.error] };
  }

  async cancel(executionId: string): Promise<ExecutionResult> {
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, execution: null as unknown as ExecutionState, errors: ['execution_not_found'] };
    if (!validateTransition(state.status, 'cancelled')) {
      return { ok: false, execution: state, errors: [`cannot_cancel_from: ${state.status}`] };
    }
    state.status = 'cancelled';
    state.completedAt = new Date().toISOString();
    this.emit(executionId, 'cancelled');
    return { ok: true, execution: state };
  }

  private async submit(state: ExecutionState): Promise<ExecutionResult> {
    try {
      state.status = 'submitted';
      state.lastHeartbeatAt = new Date().toISOString();
      this.emit(state.id, 'submitted');

      for (const order of state.orders) {
        await this.delay(100);
        const price = order.price || 100;
        const fill: OrderFill = {
          id: `fill-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          orderId: state.id,
          marketId: order.marketId,
          venue: order.venue,
          side: order.side,
          quantity: order.quantity,
          price,
          fee: order.quantity * price * (order.feeBps || 5) / 10000,
          feeCurrency: 'USD',
          liquidity: 'taker',
          filledAt: new Date().toISOString(),
          settlementStatus: 'settled',
        };
        state.fills.push(fill);
      }

      state.status = 'filled';
      state.completedAt = new Date().toISOString();
      this.emit(state.id, 'filled');
      return { ok: true, execution: state };
    } catch (error) {
      state.status = 'failed';
      state.error = String(error);
      state.completedAt = new Date().toISOString();
      this.emit(state.id, 'failed', { error: state.error });
      return { ok: false, execution: state, errors: [String(error)] };
    }
  }

  getExecution(id: string): ExecutionState | undefined {
    return this.executions.get(id);
  }

  listExecutions(filter?: { strategyId?: string; status?: OrderStatus; mode?: ExecutionMode }): ExecutionState[] {
    let results = Array.from(this.executions.values());
    if (filter?.strategyId) results = results.filter(e => e.strategyId === filter.strategyId);
    if (filter?.status) results = results.filter(e => e.status === filter.status);
    if (filter?.mode) results = results.filter(e => e.mode === filter.mode);
    return results;
  }

  getEvents(executionId: string): ExecutionEvent[] {
    return this.events.filter(e => e.executionId === executionId);
  }

  getAllEvents(): ExecutionEvent[] {
    return this.events;
  }

  private createState(request: ExecutionRequest, plan: ExecutionPlan): ExecutionState {
    return {
      id: `exec-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      strategyId: request.strategyId,
      opportunityId: request.opportunityId,
      accountId: request.accountId,
      mode: request.mode || 'paper',
      status: 'draft',
      orders: request.orders,
      fills: [],
      confidenceScore: plan.confidenceScore,
      convictionWeight: plan.convictionWeight,
      riskDecision: plan.riskDecision,
      startedAt: new Date().toISOString(),
      lastHeartbeatAt: new Date().toISOString(),
    };
  }

  private emit(executionId: string, type: ExecutionEvent['type'], payload?: Record<string, unknown>): void {
    const event: ExecutionEvent = {
      id: `evt-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
      executionId,
      type,
      payload,
      timestamp: new Date().toISOString(),
    };
    this.events.push(event);
  }

  private delay(ms: number): Promise<void> {
    return new Promise(r => setTimeout(r, ms));
  }
}
