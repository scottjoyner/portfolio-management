const MODES = new Set(['local_only', 'economic_auto', 'openrouter_allowed']);

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
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
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

export function remoteSpendSummary(state = {}, now = new Date()) {
  const day = new Date(now).toISOString().slice(0, 10);
  const rows = (state.modelUsageLedger || []).filter(row => row.localOrRemote === 'remote' && String(row.requestedAt || row.createdAt || '').slice(0, 10) === day);
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
    spend: { ...spend, remainingDailyUsd: round(Math.max(0, policy.remoteSpendCapUsdPerDay - spend.committedUsd), 8) },
    effective: {
      localAllowed: true,
      remoteAllowed: policy.mode !== 'local_only' && capabilities.openRouterAvailable,
      automaticComparisonEnabled: policy.mode === 'economic_auto',
    },
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
