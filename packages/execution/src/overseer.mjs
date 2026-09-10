import { createHash } from 'node:crypto';

export const OVERSEER_SCHEMA_VERSION = 1;
export const OVERSEER_POLICY_VERSION = 'execution-admission-v1';
export const DEFAULT_OVERSEER_TTL_MS = 15 * 60 * 1000;

function canonicalJson(value) {
  if (value === null) return 'null';
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`;
  if (value && typeof value === 'object') {
    const keys = Object.keys(value).filter(key => value[key] !== undefined).sort();
    return `{${keys.map(key => `${JSON.stringify(key)}:${canonicalJson(value[key])}`).join(',')}}`;
  }
  if (typeof value === 'number' && !Number.isFinite(value)) {
    throw new TypeError('overseer intent contains non-finite number');
  }
  const encoded = JSON.stringify(value);
  if (encoded === undefined) throw new TypeError('overseer intent contains unsupported value');
  return encoded;
}

export function stableHash(value) {
  return createHash('sha256').update(canonicalJson(value)).digest('hex');
}

function finiteOrNull(value) {
  if (value === null || value === undefined || value === '') return null;
  const number = Number(value);
  return Number.isFinite(number) ? number : null;
}

function normalizeOrder(order = {}) {
  return {
    id: order.id ?? null,
    strategyId: order.strategyId ?? null,
    opportunityId: order.opportunityId ?? null,
    marketId: order.marketId ?? null,
    symbol: order.symbol ?? null,
    venue: order.venue ?? null,
    side: order.side ?? null,
    quantity: finiteOrNull(order.quantity),
    notional: finiteOrNull(order.notional),
    price: finiteOrNull(order.price),
    stopPrice: finiteOrNull(order.stopPrice),
    targetPrice: finiteOrNull(order.targetPrice),
    takeProfitPrice: finiteOrNull(order.takeProfitPrice),
    stopLossPrice: finiteOrNull(order.stopLossPrice),
    orderType: order.orderType ?? null,
    timeInForce: order.timeInForce ?? null,
    executionMode: order.executionMode ?? null,
    feeBps: finiteOrNull(order.feeBps),
    slippageBps: finiteOrNull(order.slippageBps),
    confidenceScore: finiteOrNull(order.confidenceScore),
    convictionWeight: finiteOrNull(order.convictionWeight),
    parentOrderId: order.parentOrderId ?? null,
  };
}

export function normalizeRiskDecision(value, { required = true } = {}) {
  if (value === null || value === undefined) {
    if (required) {
      return {
        valid: false,
        decision: { approved: false, reasons: ['risk_decision_required'] },
      };
    }
    return {
      valid: true,
      decision: { approved: true, reasons: ['risk_check_disabled_by_config'] },
    };
  }

  if (
    typeof value !== 'object'
    || typeof value.approved !== 'boolean'
    || !Array.isArray(value.reasons)
    || value.reasons.some(reason => typeof reason !== 'string')
  ) {
    return {
      valid: false,
      decision: { approved: false, reasons: ['risk_decision_invalid'] },
    };
  }

  const reasons = [...new Set(value.reasons.map(reason => reason.trim()).filter(Boolean))];
  if (!value.approved && reasons.length === 0) reasons.push('risk_rejected');
  return {
    valid: true,
    decision: { approved: value.approved, reasons },
  };
}

export function buildTradeIntentEnvelope(input = {}, riskDecision = input.riskDecision) {
  const orders = Array.isArray(input.orders) ? input.orders.map(normalizeOrder) : [];
  const firstOrder = orders[0] || {};
  return {
    schemaVersion: 1,
    strategyId: input.strategyId ?? firstOrder.strategyId ?? null,
    opportunityId: input.opportunityId ?? firstOrder.opportunityId ?? null,
    sourceAgentId: input.sourceAgentId ?? null,
    accountId: input.accountId ?? null,
    mode: input.mode || 'paper',
    venue: input.venue ?? firstOrder.venue ?? null,
    symbol: input.symbol ?? firstOrder.symbol ?? null,
    side: input.side ?? firstOrder.side ?? null,
    quantity: finiteOrNull(input.quantity ?? firstOrder.quantity),
    notional: finiteOrNull(input.notional ?? input.notionalUsd ?? firstOrder.notional),
    confidenceScore: finiteOrNull(input.confidenceScore ?? firstOrder.confidenceScore ?? 0.5),
    convictionWeight: finiteOrNull(input.convictionWeight),
    entryPrice: finiteOrNull(input.entryPrice ?? firstOrder.price),
    takeProfitPrice: finiteOrNull(input.takeProfitPrice ?? firstOrder.takeProfitPrice),
    stopLossPrice: finiteOrNull(input.stopLossPrice ?? firstOrder.stopLossPrice),
    tradeIntent: input.tradeIntent ?? null,
    executionPurpose: input.executionPurpose ?? null,
    positionSide: input.positionSide ?? null,
    economicDecisionId: input.economicDecisionId ?? null,
    modelQuoteId: input.modelQuoteId ?? null,
    forecastId: input.forecastId ?? null,
    executionCostSnapshotId: input.executionCostSnapshotId ?? null,
    netExecutableEdgeUsd: finiteOrNull(input.netExecutableEdgeUsd),
    tradePlan: input.tradePlan && typeof input.tradePlan === 'object' ? input.tradePlan : null,
    orders,
    riskDecision: {
      approved: riskDecision?.approved === true,
      reasons: Array.isArray(riskDecision?.reasons) ? [...riskDecision.reasons] : [],
    },
  };
}

function materialValidationReasons(input, confidenceScore) {
  const reasons = [];
  if (!Number.isFinite(confidenceScore)) reasons.push('confidence_invalid');
  const orders = Array.isArray(input.orders) ? input.orders : [];
  if (orders.length === 0) reasons.push('orders_required');
  orders.forEach((order, index) => {
    const quantity = Number(order?.quantity);
    if (!Number.isFinite(quantity) || quantity <= 0) reasons.push(`order_quantity_invalid:${index}`);
    if (order?.price !== undefined && order?.price !== null) {
      const price = Number(order.price);
      if (!Number.isFinite(price) || price <= 0) reasons.push(`order_price_invalid:${index}`);
    }
    if (!['buy', 'sell', 'yes', 'no'].includes(String(order?.side || '').toLowerCase())) {
      reasons.push(`order_side_invalid:${index}`);
    }
  });
  return reasons;
}

function isoNow(now) {
  const date = now instanceof Date ? now : new Date(now ?? Date.now());
  if (!Number.isFinite(date.getTime())) throw new TypeError('invalid overseer evaluation time');
  return date;
}

export function evaluateTradeIntent(input = {}, options = {}) {
  const minConfidence = Number(options.minConfidence ?? 0.6);
  const requireApproval = options.requireApproval !== false;
  const requireRiskCheck = options.requireRiskCheck !== false;
  const ttlMs = Number(options.ttlMs ?? DEFAULT_OVERSEER_TTL_MS);
  if (!Number.isFinite(minConfidence) || minConfidence < 0 || minConfidence > 1) {
    throw new TypeError('minConfidence must be in [0, 1]');
  }
  if (!Number.isFinite(ttlMs) || ttlMs <= 0) throw new TypeError('overseer ttl must be positive');

  const normalizedRisk = normalizeRiskDecision(input.riskDecision, { required: requireRiskCheck });
  const normalizedInput = { ...input, riskDecision: normalizedRisk.decision };
  const envelope = buildTradeIntentEnvelope(normalizedInput, normalizedRisk.decision);
  const intentHash = stableHash(envelope);
  const confidenceScore = envelope.confidenceScore;
  const mode = envelope.mode;
  const reasons = materialValidationReasons(normalizedInput, confidenceScore);

  if (!normalizedRisk.valid) reasons.push(...normalizedRisk.decision.reasons);
  else if (!normalizedRisk.decision.approved) reasons.push(...normalizedRisk.decision.reasons);
  if (Number.isFinite(confidenceScore) && confidenceScore < minConfidence) reasons.push('confidence_below_threshold');
  if (mode === 'live') reasons.push('live_execution_not_certified');
  else if (mode === 'readonly') reasons.push('readonly_execution_blocked');
  else if (!['paper', 'demo'].includes(mode)) reasons.push('execution_mode_invalid');

  const uniqueReasons = [...new Set(reasons)];
  const approved = uniqueReasons.length === 0;
  let decision = 'REJECT';
  if (mode === 'live') decision = 'LIVE_REQUIRES_HUMAN';
  else if (approved && mode === 'paper') decision = 'PAPER';
  else if (approved && mode === 'demo') decision = 'DEMO';

  const evaluatedAt = isoNow(options.now);
  const core = {
    schemaVersion: OVERSEER_SCHEMA_VERSION,
    policyVersion: OVERSEER_POLICY_VERSION,
    decision,
    approved,
    reasons: uniqueReasons,
    intentHash,
    evaluatedAt: evaluatedAt.toISOString(),
    validUntil: new Date(evaluatedAt.getTime() + ttlMs).toISOString(),
    requiresHumanApproval: requireApproval || mode === 'live',
    mode,
  };
  const overseerDecision = { ...core, decisionHash: stableHash(core) };
  return {
    riskDecision: normalizedRisk.decision,
    tradeIntentEnvelope: envelope,
    tradeIntentHash: intentHash,
    overseerDecision,
  };
}

export function verifyOverseerDecision(decision, { intentHash, now } = {}) {
  const reasons = [];
  if (!decision || typeof decision !== 'object') return { ok: false, reasons: ['overseer_decision_required'] };
  const required = [
    'schemaVersion', 'policyVersion', 'decision', 'approved', 'reasons', 'intentHash',
    'evaluatedAt', 'validUntil', 'requiresHumanApproval', 'mode', 'decisionHash',
  ];
  for (const field of required) {
    if (!(field in decision)) reasons.push(`overseer_decision_missing_field:${field}`);
  }
  if (reasons.length) return { ok: false, reasons };
  if (decision.schemaVersion !== OVERSEER_SCHEMA_VERSION || decision.policyVersion !== OVERSEER_POLICY_VERSION) {
    reasons.push('overseer_policy_version_mismatch');
  }
  if (intentHash && decision.intentHash !== intentHash) reasons.push('overseer_intent_hash_mismatch');
  if (decision.approved !== true) reasons.push(...(Array.isArray(decision.reasons) && decision.reasons.length ? decision.reasons : ['overseer_rejected']));
  if (!Array.isArray(decision.reasons) || decision.reasons.some(reason => typeof reason !== 'string')) {
    reasons.push('overseer_reasons_invalid');
  }

  const core = { ...decision };
  const suppliedHash = core.decisionHash;
  delete core.decisionHash;
  try {
    if (stableHash(core) !== suppliedHash) reasons.push('overseer_decision_hash_mismatch');
  } catch {
    reasons.push('overseer_decision_not_canonicalizable');
  }

  const expiresAt = new Date(decision.validUntil).getTime();
  const current = isoNow(now).getTime();
  if (!Number.isFinite(expiresAt)) reasons.push('overseer_expiry_invalid');
  else if (current > expiresAt) reasons.push('overseer_decision_expired');
  if (decision.mode === 'live') reasons.push('live_execution_not_certified');

  return { ok: reasons.length === 0, reasons: [...new Set(reasons)] };
}

export function verifyStoredExecutionAuthorization(state, { now } = {}) {
  const reasons = [];
  let envelope;
  let currentHash;
  try {
    envelope = buildTradeIntentEnvelope(state, state?.riskDecision);
    currentHash = stableHash(envelope);
  } catch {
    return { ok: false, reasons: ['trade_intent_not_canonicalizable'] };
  }
  if (!state?.tradeIntentHash) reasons.push('trade_intent_hash_required');
  else if (state.tradeIntentHash !== currentHash) reasons.push('trade_intent_mutated');
  if (!state?.tradeIntentEnvelope) reasons.push('trade_intent_envelope_required');
  else {
    try {
      if (stableHash(state.tradeIntentEnvelope) !== state.tradeIntentHash) reasons.push('trade_intent_envelope_hash_mismatch');
    } catch {
      reasons.push('trade_intent_envelope_not_canonicalizable');
    }
  }
  const overseer = verifyOverseerDecision(state?.overseerDecision, { intentHash: currentHash, now });
  reasons.push(...overseer.reasons);
  return {
    ok: reasons.length === 0,
    reasons: [...new Set(reasons)],
    tradeIntentEnvelope: envelope,
    tradeIntentHash: currentHash,
  };
}
