const MODES = new Set(['local_only', 'economic_auto', 'openrouter_allowed']);
const MINIMUM_ATTRIBUTION_OBSERVATIONS = 5;

export const DEFAULT_INTELLIGENCE_ROUTING_POLICY = Object.freeze({
  mode: 'local_only',
  remoteSpendCapUsdPerDay: 5,
  remoteSpendCapUsdPerRequest: 1,
  minimumRemoteValueCoverage: 3,
  fallbackToLocalOnRemoteBlock: true,
});

function finite(value, fallback) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function round(value, digits = 4) {
  if (!Number.isFinite(value)) return value;
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function summary(values = []) {
  if (!values.length) return { observations: 0, meanUsd: null, standardErrorUsd: null, lowerBoundUsd: null, upperBoundUsd: null };
  const mean = values.reduce((sum, value) => sum + value, 0) / values.length;
  const variance = values.length > 1
    ? values.reduce((sum, value) => sum + ((value - mean) ** 2), 0) / (values.length - 1)
    : 0;
  const standardError = Math.sqrt(variance) / Math.sqrt(values.length);
  const margin = 1.28 * standardError;
  return {
    observations: values.length,
    meanUsd: round(mean, 6),
    standardErrorUsd: round(standardError, 6),
    lowerBoundUsd: round(mean - margin, 6),
    upperBoundUsd: round(mean + margin, 6),
  };
}

function quoteIdsForAttribution(state, record) {
  const ids = new Set(Array.isArray(record.modelQuoteIds) ? record.modelQuoteIds.filter(Boolean) : []);
  const decision = record.decisionId ? state.economicDecisions?.find(row => row.id === record.decisionId) : null;
  if (decision?.modelQuoteId) ids.add(decision.modelQuoteId);
  return [...ids];
}

function attributionValues(state, locality, model = null) {
  const quoteById = new Map((state.modelUsageLedger || []).map(row => [row.id, row]));
  return (state.agentAttributionRecords || [])
    .map(record => {
      const quotes = quoteIdsForAttribution(state, record).map(id => quoteById.get(id)).filter(Boolean);
      const matches = quotes.some(quote => quote.localOrRemote === locality && (!model || quote.model === model));
      return matches ? { value: finite(record.incrementalValueUsd, null), observedAt: record.observedAt || '' } : null;
    })
    .filter(row => row?.value != null)
    .sort((a, b) => new Date(b.observedAt || 0) - new Date(a.observedAt || 0))
    .slice(0, 50)
    .map(row => row.value);
}

export function historicalRemoteValueEstimate(state = {}, input = {}) {
  const remote = summary(attributionValues(state, 'remote', input.model || null));
  const local = summary(attributionValues(state, 'local', input.localModel || null));
  const sufficient = remote.observations >= MINIMUM_ATTRIBUTION_OBSERVATIONS
    && local.observations >= MINIMUM_ATTRIBUTION_OBSERVATIONS;
  const expectedDecisionImprovementUsd = sufficient
    ? Math.max(0, Number(remote.lowerBoundUsd || 0) - Number(local.upperBoundUsd || 0))
    : null;
  return {
    available: sufficient,
    minimumObservationsPerSide: MINIMUM_ATTRIBUTION_OBSERVATIONS,
    expectedDecisionImprovementUsd: expectedDecisionImprovementUsd == null ? null : round(expectedDecisionImprovementUsd, 6),
    source: sufficient ? 'historical_remote_lower_bound_minus_local_upper_bound' : 'insufficient_remote_vs_local_attribution',
    remote,
    local,
  };
}

export function normalizeIntelligenceRoutingPolicy(value = {}) {
  const source = value && typeof value === 'object' ? value : {};
  const dailyCap = Math.max(0, finite(source.remoteSpendCapUsdPerDay, DEFAULT_INTELLIGENCE_ROUTING_POLICY.remoteSpendCapUsdPerDay));
  const requestCap = Math.min(dailyCap, Math.max(0, finite(source.remoteSpendCapUsdPerRequest, DEFAULT_INTELLIGENCE_ROUTING_POLICY.remoteSpendCapUsdPerRequest)));
  return {
    mode: MODES.has(source.mode) ? source.mode : DEFAULT_INTELLIGENCE_ROUTING_POLICY.mode,
    remoteSpendCapUsdPerDay: round(dailyCap),
    remoteSpendCapUsdPerRequest: round(requestCap),
    minimumRemoteValueCoverage: round(Math.max(1, finite(source.minimumRemoteValueCoverage, DEFAULT_INTELLIGENCE_ROUTING_POLICY.minimumRemoteValueCoverage))),
    fallbackToLocalOnRemoteBlock: source.fallbackToLocalOnRemoteBlock !== false,
    updatedAt: source.updatedAt || null,
    updatedBy: source.updatedBy || null,
  };
}

export function validateIntelligenceRoutingPolicy(input = {}) {
  const errors = [];
  if (!MODES.has(input.mode)) errors.push('intelligence_routing_mode_invalid');
  const dailyCap = finite(input.remoteSpendCapUsdPerDay, null);
  const requestCap = finite(input.remoteSpendCapUsdPerRequest, null);
  const coverage = finite(input.minimumRemoteValueCoverage, null);
  if (dailyCap == null || dailyCap < 0 || dailyCap > 10000) errors.push('remote_daily_spend_cap_invalid');
  if (requestCap == null || requestCap < 0 || requestCap > 10000) errors.push('remote_request_spend_cap_invalid');
  if (dailyCap != null && requestCap != null && requestCap > dailyCap) errors.push('remote_request_cap_exceeds_daily_cap');
  if (coverage == null || coverage < 1 || coverage > 100) errors.push('minimum_remote_value_coverage_invalid');
  if (input.fallbackToLocalOnRemoteBlock !== undefined && typeof input.fallbackToLocalOnRemoteBlock !== 'boolean') errors.push('fallback_to_local_on_remote_block_invalid');
  return { ok: errors.length === 0, errors };
}

export function intelligenceDeploymentCapabilities(env = process.env) {
  const remoteExecutionEnabled = env.REMOTE_LLM_EXECUTION_ENABLED === 'true';
  const openRouterKeyConfigured = Boolean(String(env.OPENROUTER_API_KEY || '').trim());
  return {
    localConfigured: Boolean(String(env.LOCAL_LLM_NODES_JSON || env.LOCAL_LLM_ENDPOINTS || '').trim()),
    remoteExecutionEnabled,
    openRouterKeyConfigured,
    openRouterAvailable: remoteExecutionEnabled && openRouterKeyConfigured,
  };
}

export function remoteSpendSummary(state = {}, now = new Date(), excludeQuoteId = null) {
  const day = new Date(now).toISOString().slice(0, 10);
  const rows = (state.modelUsageLedger || []).filter(row => row.id !== excludeQuoteId
    && row.localOrRemote === 'remote'
    && String(row.requestedAt || row.createdAt || '').slice(0, 10) === day);
  const committedStatuses = new Set(['quoted', 'running', 'usage_pending', 'reconciled']);
  const cost = row => Number(row.status === 'reconciled' ? row.actualCostUsd : row.authoritativeCostUsd ?? row.estimatedCostUsd) || 0;
  return {
    date: day,
    committedUsd: round(rows.filter(row => committedStatuses.has(row.status)).reduce((sum, row) => sum + cost(row), 0), 8),
    actualUsd: round(rows.filter(row => row.status === 'reconciled').reduce((sum, row) => sum + (Number(row.actualCostUsd) || 0), 0), 8),
    committedRequests: rows.filter(row => committedStatuses.has(row.status)).length,
    reconciledRequests: rows.filter(row => row.status === 'reconciled').length,
  };
}

export function intelligenceRoutingPolicyView(state = {}, env = process.env, now = new Date()) {
  const policy = normalizeIntelligenceRoutingPolicy(state.config?.intelligenceRoutingPolicy);
  const capabilities = intelligenceDeploymentCapabilities(env);
  const spend = remoteSpendSummary(state, now);
  return {
    policy,
    capabilities,
    historicalValue: historicalRemoteValueEstimate(state),
    spend: { ...spend, remainingDailyUsd: round(Math.max(0, policy.remoteSpendCapUsdPerDay - spend.committedUsd), 8) },
    effective: {
      localAllowed: capabilities.localConfigured,
      remoteAllowed: policy.mode !== 'local_only' && capabilities.openRouterAvailable,
      automaticComparisonEnabled: policy.mode === 'economic_auto',
    },
  };
}

function approvedLegacyQuote(state, quoteId) {
  if (!quoteId || state.config?.intelligenceRoutingPolicy) return false;
  const quote = state.modelUsageLedger?.find(row => row.id === quoteId);
  if (!quote || quote.routingPolicyDecision) return false;
  return Boolean(state.economicDecisions?.some(decision => decision.modelQuoteId === quoteId && decision.intelligenceAllowed === true));
}

export function evaluateRemoteIntelligencePolicy(state = {}, input = {}, env = process.env, now = new Date()) {
  const policy = normalizeIntelligenceRoutingPolicy(state.config?.intelligenceRoutingPolicy);
  const capabilities = intelligenceDeploymentCapabilities(env);
  const estimatedCostUsd = Math.max(0, finite(input.estimatedCostUsd, 0));
  const explicitExpectedValue = finite(input.expectedDecisionImprovementUsd, null);
  const historicalValue = explicitExpectedValue == null ? historicalRemoteValueEstimate(state, input) : null;
  const expectedDecisionImprovementUsd = explicitExpectedValue ?? historicalValue?.expectedDecisionImprovementUsd ?? null;
  const expectedValueSource = explicitExpectedValue != null ? 'request_evidence' : historicalValue?.source || 'unavailable';
  const spend = remoteSpendSummary(state, now, input.excludeQuoteId || null);
  const valueCoverage = expectedDecisionImprovementUsd == null
    ? null
    : estimatedCostUsd > 0
      ? expectedDecisionImprovementUsd / estimatedCostUsd
      : Number.POSITIVE_INFINITY;
  const legacyApprovedQuote = approvedLegacyQuote(state, input.excludeQuoteId);
  const blockers = [];

  if (policy.mode === 'local_only' && !legacyApprovedQuote) blockers.push('intelligence_policy_local_only');
  if (!capabilities.remoteExecutionEnabled) blockers.push('remote_llm_execution_disabled');
  if (!capabilities.openRouterKeyConfigured) blockers.push('openrouter_api_key_required');
  if (estimatedCostUsd > policy.remoteSpendCapUsdPerRequest) blockers.push('remote_request_spend_cap_exceeded');
  if (spend.committedUsd + estimatedCostUsd > policy.remoteSpendCapUsdPerDay) blockers.push('remote_daily_spend_cap_exceeded');
  if (policy.mode === 'economic_auto' && !legacyApprovedQuote) {
    if (expectedDecisionImprovementUsd == null || expectedDecisionImprovementUsd < 0) blockers.push('expected_remote_decision_improvement_required');
    if (valueCoverage != null && valueCoverage < policy.minimumRemoteValueCoverage) blockers.push('remote_value_coverage_below_policy');
  }

  return {
    allowed: blockers.length === 0,
    blockers,
    policy,
    capabilities,
    legacyApprovedQuote,
    estimatedCostUsd: round(estimatedCostUsd, 8),
    expectedDecisionImprovementUsd: expectedDecisionImprovementUsd == null ? null : round(expectedDecisionImprovementUsd, 8),
    expectedValueSource,
    historicalValue,
    valueCoverage: Number.isFinite(valueCoverage) ? round(valueCoverage, 6) : valueCoverage,
    committedTodayUsd: spend.committedUsd,
    projectedCommittedTodayUsd: round(spend.committedUsd + estimatedCostUsd, 8),
  };
}

export function updateIntelligenceRoutingPolicy(state = {}, input = {}, now = new Date().toISOString()) {
  const validation = validateIntelligenceRoutingPolicy(input);
  if (!validation.ok) return { errors: validation.errors };
  state.config ||= {};
  const policy = normalizeIntelligenceRoutingPolicy({ ...input, updatedAt: now, updatedBy: input.updatedBy || 'operator' });
  state.config.intelligenceRoutingPolicy = policy;
  state.audit ||= [];
  state.audit.push({
    id: `audit-intelligence-policy-${Date.now()}`,
    action: 'intelligence_routing_policy_updated',
    actor: policy.updatedBy,
    at: now,
    details: policy.mode,
    payload: {
      mode: policy.mode,
      remoteSpendCapUsdPerDay: policy.remoteSpendCapUsdPerDay,
      remoteSpendCapUsdPerRequest: policy.remoteSpendCapUsdPerRequest,
      minimumRemoteValueCoverage: policy.minimumRemoteValueCoverage,
      fallbackToLocalOnRemoteBlock: policy.fallbackToLocalOnRemoteBlock,
    },
  });
  return { policy };
}
