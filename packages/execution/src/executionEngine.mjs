// Self-contained compatibility execution engine for runtime use by the API server.
// PostgreSQL remains the durable source of truth; this engine hydrates its
// compatibility map from the read model published by the transactional store.

import {
  buildCapitalRiskSnapshot,
  evaluateCapitalRiskSnapshot,
  verifyCapitalRiskSnapshot,
} from './capitalRiskSnapshot.mjs';
import {
  DEFAULT_OVERSEER_TTL_MS,
  buildTradeIntentEnvelope,
  evaluateTradeIntent,
  stableHash,
  verifyStoredExecutionAuthorization,
} from './overseer.mjs';

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
  if (value === undefined) return undefined;
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

function evaluationTime(context = {}) {
  const value = context.now instanceof Date ? context.now : new Date(context.now ?? Date.now());
  if (!Number.isFinite(value.getTime())) throw new TypeError('execution evaluation time invalid');
  return value.toISOString();
}

function providerResultState(result) {
  if (!result || typeof result !== 'object') return { state: null, source: {} };
  if (result.state && typeof result.state === 'object') {
    return {
      state: result.state,
      source: {
        source: result.source || result.type || 'operator_store',
        revision: result.revision ?? null,
        observedAt: result.observedAt ?? null,
      },
    };
  }
  return { state: result, source: {} };
}

function currentEvaluationTime(context = {}) {
  return context.now !== undefined && context.now !== null
    ? evaluationTime(context)
    : new Date().toISOString();
}

export default class ExecutionEngine {
  constructor(config = {}) {
    this.minConfidence = config.minConfidence ?? 0.6;
    this.requireApproval = config.requireApproval !== false;
    this.requireRiskCheck = config.requireRiskCheck !== false;
    this.overseerTtlMs = config.overseerTtlMs ?? DEFAULT_OVERSEER_TTL_MS;
    this.maxRetries = config.maxExecutionRetries ?? 3;
    this.riskStateProvider = config.riskStateProvider || null;
    this.riskPolicy = config.riskPolicy || {};
    this.executions = new Map();
    this.events = [];
    this.lastHydratedRevision = null;
    this.lastHydratedAt = null;
  }

  overseerOptions(now, bindings = {}) {
    return {
      minConfidence: this.minConfidence,
      requireApproval: this.requireApproval,
      requireRiskCheck: this.requireRiskCheck,
      ttlMs: this.overseerTtlMs,
      now,
      ...bindings,
    };
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

  async canonicalRiskFor(input, tradeIntentEnvelope, tradeIntentHash, context = {}, excludeExecutionId = null) {
    if (!this.requireRiskCheck) {
      const evaluatedAt = currentEvaluationTime(context);
      const riskDecision = { approved: true, reasons: ['risk_check_disabled_by_config'] };
      return {
        capitalRiskSnapshot: null,
        capitalRiskSnapshotHash: null,
        riskDecision,
        riskDecisionHash: stableHash(riskDecision),
        capitalRiskPolicyVersion: null,
        evaluatedAt,
      };
    }

    const providerRequestedAt = currentEvaluationTime(context);
    const provider = context.riskStateProvider || this.riskStateProvider;
    let resolved = null;
    if (typeof provider === 'function') {
      try {
        resolved = await provider({
          tradeIntentEnvelope: clone(tradeIntentEnvelope),
          tradeIntentHash,
          excludeExecutionId,
          now: providerRequestedAt,
        });
      } catch (error) {
        resolved = {
          state: null,
          source: 'risk_state_provider_error',
          revision: null,
          observedAt: null,
          error: String(error?.message || error),
        };
      }
    }
    const { state, source } = providerResultState(resolved);
    const evaluatedAt = currentEvaluationTime(context);
    const snapshot = buildCapitalRiskSnapshot({
      state,
      source,
      tradeIntentEnvelope,
      tradeIntentHash,
      now: evaluatedAt,
      excludeExecutionId,
      policy: { ...this.riskPolicy, ...(context.riskPolicy || {}) },
    });
    const riskDecision = evaluateCapitalRiskSnapshot(snapshot, { tradeIntentHash, now: evaluatedAt });
    return {
      capitalRiskSnapshot: snapshot,
      capitalRiskSnapshotHash: snapshot.snapshotHash,
      riskDecision,
      riskDecisionHash: stableHash(riskDecision),
      capitalRiskPolicyVersion: snapshot.policyVersion,
      evaluatedAt,
    };
  }

  async evaluateRequest(request, context = {}, excludeExecutionId = null) {
    const overallScore = request.orders?.[0]?.confidenceScore ?? request.confidenceScore ?? 0.5;
    const convictionWeight = request.convictionWeight ?? (0.5 + overallScore * 0.5);
    const normalized = { ...request, confidenceScore: overallScore, convictionWeight };
    const tradeIntentEnvelope = buildTradeIntentEnvelope(normalized);
    const tradeIntentHash = stableHash(tradeIntentEnvelope);
    const canonicalRisk = await this.canonicalRiskFor(
      normalized,
      tradeIntentEnvelope,
      tradeIntentHash,
      context,
      excludeExecutionId,
    );
    const evaluation = evaluateTradeIntent(
      { ...normalized, riskDecision: canonicalRisk.riskDecision },
      this.overseerOptions(canonicalRisk.evaluatedAt, {
        capitalRiskSnapshotHash: canonicalRisk.capitalRiskSnapshotHash,
        capitalRiskPolicyVersion: canonicalRisk.capitalRiskPolicyVersion,
        riskDecisionHash: canonicalRisk.riskDecisionHash,
      }),
    );
    return {
      ...evaluation,
      confidenceScore: overallScore,
      convictionWeight,
      capitalRiskSnapshot: canonicalRisk.capitalRiskSnapshot,
      capitalRiskSnapshotHash: canonicalRisk.capitalRiskSnapshotHash,
      riskDecisionHash: canonicalRisk.riskDecisionHash,
      evaluatedAt: canonicalRisk.evaluatedAt,
    };
  }

  async plan(request, context = {}) {
    const evaluation = await this.evaluateRequest(request, context);
    return {
      id: `plan-${Date.now()}`,
      requests: [request],
      confidenceScore: evaluation.confidenceScore,
      convictionWeight: evaluation.convictionWeight,
      riskDecision: evaluation.riskDecision,
      riskDecisionHash: evaluation.riskDecisionHash,
      createdAt: new Date().toISOString(),
      approved: evaluation.overseerDecision.approved,
      tradeIntentEnvelope: evaluation.tradeIntentEnvelope,
      tradeIntentHash: evaluation.tradeIntentHash,
      capitalRiskSnapshot: clone(evaluation.capitalRiskSnapshot),
      capitalRiskSnapshotHash: evaluation.capitalRiskSnapshotHash,
      overseerDecision: evaluation.overseerDecision,
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
      netExecutableEdgeUsd: evaluation.capitalRiskSnapshot?.edgeState?.netExecutableEdgeUsd ?? null,
    };
  }

  async execute(request, context = {}) {
    this.hydrateDurableReadModel();
    const plan = await this.plan(request, context);
    if (!plan.approved) {
      const state = this.createState(request, plan);
      const reasons = plan.overseerDecision?.reasons?.length
        ? plan.overseerDecision.reasons
        : ['overseer_rejected'];
      return { ok: false, execution: state, errors: reasons };
    }

    const state = this.createState(request, plan);
    this.executions.set(state.id, state);
    this.emit({
      executionId: state.id,
      type: 'created',
      economicDecisionId: state.economicDecisionId,
      overseerDecision: state.overseerDecision?.decision || null,
      tradeIntentHash: state.tradeIntentHash || null,
      capitalRiskSnapshotHash: state.capitalRiskSnapshotHash || null,
    });

    if (this.requireApproval) return { ok: true, execution: state, warnings: ['awaiting_approval'] };
    return this.submit(state, context);
  }

  async approve(executionId, context = {}) {
    this.hydrateDurableReadModel();
    const state = this.executions.get(executionId);
    if (!state) return { ok: false, errors: ['execution_not_found'] };
    if (state.status !== 'draft') return { ok: false, execution: state, errors: [`invalid_status: ${state.status}`] };

    const storedAuthorization = verifyStoredExecutionAuthorization(state, { now: currentEvaluationTime(context) });
    if (!storedAuthorization.ok) {
      return { ok: false, execution: state, errors: storedAuthorization.reasons };
    }

    const refreshed = await this.evaluateRequest(state, context, executionId);
    if (refreshed.tradeIntentHash !== state.tradeIntentHash) {
      return { ok: false, execution: state, errors: ['trade_intent_mutated'] };
    }
    if (!refreshed.overseerDecision.approved) {
      return {
        ok: false,
        execution: state,
        errors: refreshed.overseerDecision.reasons.length
          ? refreshed.overseerDecision.reasons
          : ['overseer_rejected'],
      };
    }

    state.riskDecision = refreshed.riskDecision;
    state.riskDecisionHash = refreshed.riskDecisionHash;
    state.tradeIntentEnvelope = refreshed.tradeIntentEnvelope;
    state.capitalRiskSnapshot = clone(refreshed.capitalRiskSnapshot);
    state.capitalRiskSnapshotHash = refreshed.capitalRiskSnapshotHash;
    state.overseerDecision = refreshed.overseerDecision;
    state.netExecutableEdgeUsd = refreshed.capitalRiskSnapshot?.edgeState?.netExecutableEdgeUsd ?? null;
    state.status = 'approved';
    state.updatedAt = new Date().toISOString();
    this.emit({
      executionId,
      type: 'approved',
      economicDecisionId: state.economicDecisionId,
      overseerDecision: state.overseerDecision.decision,
      tradeIntentHash: state.tradeIntentHash,
      capitalRiskSnapshotHash: state.capitalRiskSnapshotHash,
    });
    return this.submit(state, context);
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

  verifyCapitalRiskFreshness(state, context = {}) {
    if (!state?.capitalRiskSnapshot) return { ok: true, reasons: [] };
    return verifyCapitalRiskSnapshot(state.capitalRiskSnapshot, {
      tradeIntentHash: state.tradeIntentHash,
      now: currentEvaluationTime(context),
    });
  }

  async submit(state, context = {}) {
    const freshness = this.verifyCapitalRiskFreshness(state, context);
    if (!freshness.ok) {
      return { ok: false, execution: state, errors: freshness.reasons };
    }
    const authorization = verifyStoredExecutionAuthorization(state, { now: currentEvaluationTime(context) });
    if (!authorization.ok) {
      return { ok: false, execution: state, errors: authorization.reasons };
    }

    try {
      state.status = 'submitted';
      state.lastHeartbeatAt = new Date().toISOString();
      state.updatedAt = state.lastHeartbeatAt;
      this.emit({
        executionId: state.id,
        type: 'submitted',
        economicDecisionId: state.economicDecisionId,
        overseerDecision: state.overseerDecision?.decision || null,
        tradeIntentHash: state.tradeIntentHash || null,
        capitalRiskSnapshotHash: state.capitalRiskSnapshotHash || null,
      });

      for (const order of state.orders) {
        const loopFreshness = this.verifyCapitalRiskFreshness(state, context);
        if (!loopFreshness.ok) throw new Error(loopFreshness.reasons[0]);
        await this.delay(100);
        const fillPrice = Number(order.price ?? state.entryPrice ?? state.capitalRiskSnapshot?.marketDataState?.referencePrice);
        if (!Number.isFinite(fillPrice) || fillPrice <= 0) throw new Error('execution_fill_price_unavailable');
        const quantity = Number(order.quantity);
        if (!Number.isFinite(quantity) || quantity <= 0) throw new Error('execution_fill_quantity_invalid');
        const feeBps = Number(order.feeBps ?? 5);
        if (!Number.isFinite(feeBps) || feeBps < 0) throw new Error('execution_fee_bps_invalid');
        const fill = {
          id: `fill-${Date.now()}-${Math.random().toString(36).slice(2, 6)}`,
          orderId: order.id || state.id,
          executionId: state.id,
          marketId: order.marketId,
          symbol: order.symbol || state.symbol,
          venue: order.venue || state.venue,
          side: order.side || state.side,
          quantity,
          price: fillPrice,
          fee: quantity * fillPrice * feeBps / 10000,
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
      notional: request.notional ?? request.notionalUsd ?? firstOrder.notional ?? null,
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
      riskDecision: clone(plan.riskDecision),
      riskDecisionHash: plan.riskDecisionHash,
      tradeIntentEnvelope: clone(plan.tradeIntentEnvelope),
      tradeIntentHash: plan.tradeIntentHash,
      capitalRiskSnapshot: clone(plan.capitalRiskSnapshot),
      capitalRiskSnapshotHash: plan.capitalRiskSnapshotHash,
      overseerDecision: clone(plan.overseerDecision),
      economicDecisionId: request.economicDecisionId || plan.economicDecisionId || null,
      modelQuoteId: request.modelQuoteId || plan.modelQuoteId || null,
      forecastId: request.forecastId || plan.forecastId || null,
      executionCostSnapshotId: request.executionCostSnapshotId || plan.executionCostSnapshotId || null,
      netExecutableEdgeUsd: plan.netExecutableEdgeUsd ?? null,
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
