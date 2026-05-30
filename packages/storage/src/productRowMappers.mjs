function parseJson(value, fallback) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

function iso(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function num(value, fallback = 0) {
  if (value === null || value === undefined) return fallback;
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

export function marketSnapshotFromRow(row = {}) {
  return {
    id: row.id,
    symbol: row.symbol,
    venue: row.venue,
    assetClass: row.asset_class,
    bid: num(row.bid, null),
    ask: num(row.ask, null),
    spreadBps: num(row.spread_bps, null),
    volume24h: num(row.volume_24h, null),
    liquidityScore: num(row.liquidity_score, null),
    volatilityScore: num(row.volatility_score, null),
    status: row.status,
    source: row.source,
    timestamp: iso(row.timestamp)
  };
}

export function marketSnapshotParams(snapshot = {}) {
  return [
    snapshot.id,
    snapshot.symbol,
    snapshot.venue,
    snapshot.assetClass || snapshot.asset_class,
    snapshot.bid ?? null,
    snapshot.ask ?? null,
    snapshot.spreadBps ?? snapshot.spread_bps ?? null,
    snapshot.volume24h ?? snapshot.volume_24h ?? null,
    snapshot.liquidityScore ?? snapshot.liquidity_score ?? null,
    snapshot.volatilityScore ?? snapshot.volatility_score ?? null,
    snapshot.status || 'watching',
    snapshot.source || 'unknown',
    snapshot.timestamp || new Date().toISOString()
  ];
}

export function agentBudgetFromRow(row = {}) {
  return {
    agentId: row.agent_id,
    dailyTokenLimit: num(row.daily_token_limit),
    dailyCostLimit: num(row.daily_cost_limit),
    perJobTokenLimit: num(row.per_job_token_limit),
    perMarketCostLimit: num(row.per_market_cost_limit),
    requireApprovalAboveCost: num(row.require_approval_above_cost),
    enabled: Boolean(row.enabled),
    updatedAt: iso(row.updated_at)
  };
}

export function agentBudgetParams(budget = {}) {
  return [
    budget.agentId || budget.agent_id,
    budget.dailyTokenLimit || 0,
    budget.dailyCostLimit || 0,
    budget.perJobTokenLimit || 0,
    budget.perMarketCostLimit || 0,
    budget.requireApprovalAboveCost || 0,
    budget.enabled !== false,
    budget.updatedAt || new Date().toISOString()
  ];
}

export function budgetApprovalFromRow(row = {}) {
  return {
    id: row.id,
    agentId: row.agent_id,
    marketScope: row.market_scope,
    opportunityId: row.opportunity_id,
    requestedBy: row.requested_by,
    reason: row.reason,
    status: row.status,
    projectedCost: num(row.projected_cost),
    projectedTokens: num(row.projected_tokens),
    approvedCostLimit: num(row.approved_cost_limit),
    approvedTokenLimit: num(row.approved_token_limit),
    reviewer: row.reviewer,
    decisionReason: row.decision_reason,
    requestedAt: iso(row.requested_at),
    reviewedAt: iso(row.reviewed_at),
    expiresAt: iso(row.expires_at)
  };
}

export function budgetApprovalParams(approval = {}) {
  return [
    approval.id,
    approval.agentId || approval.agent_id,
    approval.marketScope || approval.market_scope || null,
    approval.opportunityId || approval.opportunity_id || null,
    approval.requestedBy || approval.requested_by || 'operator',
    approval.reason || 'additional research budget requested',
    approval.status || 'pending_review',
    approval.projectedCost ?? approval.projected_cost ?? 0,
    approval.projectedTokens ?? approval.projected_tokens ?? 0,
    approval.approvedCostLimit ?? approval.approved_cost_limit ?? 0,
    approval.approvedTokenLimit ?? approval.approved_token_limit ?? 0,
    approval.reviewer || null,
    approval.decisionReason || approval.decision_reason || null,
    approval.requestedAt || approval.requested_at || new Date().toISOString(),
    approval.reviewedAt || approval.reviewed_at || null,
    approval.expiresAt || approval.expires_at || null
  ];
}

export function researchJobFromRow(row = {}) {
  return {
    id: row.id,
    agentId: row.agent_id,
    triggerType: row.trigger_type,
    marketScope: row.market_scope,
    symbolScope: row.symbol_scope,
    provider: row.provider,
    model: row.model,
    localOrRemote: row.local_or_remote,
    status: row.status,
    startedAt: iso(row.started_at),
    completedAt: iso(row.completed_at),
    promptTokens: num(row.prompt_tokens),
    completionTokens: num(row.completion_tokens),
    totalTokens: num(row.total_tokens),
    estimatedRemoteCost: num(row.estimated_remote_cost),
    estimatedLocalCost: num(row.estimated_local_cost),
    budgetApprovalId: row.budget_approval_id,
    opportunityIdsCreated: parseJson(row.opportunity_ids_json, []),
    failureReason: row.failure_reason
  };
}

export function researchJobParams(job = {}) {
  return [
    job.id,
    job.agentId || job.agent_id,
    job.triggerType || job.trigger_type || 'operator_request',
    job.marketScope || job.market_scope || 'general',
    job.symbolScope || job.symbol_scope || null,
    job.provider || 'unknown',
    job.model || 'unknown',
    job.localOrRemote || job.local_or_remote || 'remote',
    job.status || 'completed',
    job.startedAt || job.started_at || new Date().toISOString(),
    job.completedAt || job.completed_at || null,
    job.promptTokens || job.prompt_tokens || 0,
    job.completionTokens || job.completion_tokens || 0,
    job.totalTokens || job.total_tokens || 0,
    job.estimatedRemoteCost || job.estimated_remote_cost || 0,
    job.estimatedLocalCost || job.estimated_local_cost || 0,
    job.budgetApprovalId || job.budget_approval_id || null,
    JSON.stringify(job.opportunityIdsCreated || job.opportunity_ids_json || []),
    job.failureReason || job.failure_reason || null
  ];
}

export function opportunityFromRow(row = {}) {
  return {
    id: row.id,
    sourceAgentId: row.source_agent_id,
    researchJobId: row.research_job_id,
    strategyId: row.strategy_id,
    marketType: row.market_type,
    venue: row.venue,
    symbol: row.symbol,
    marketSlug: row.market_slug,
    title: row.title,
    recommendation: row.recommendation,
    confidenceScore: num(row.confidence_score),
    winProbability: num(row.win_probability),
    lossProbability: num(row.loss_probability),
    expectedValue: num(row.expected_value),
    grossExpectedValue: num(row.gross_expected_value),
    totalMoneyRisked: num(row.total_money_risked),
    maxLoss: num(row.max_loss),
    potentialUpside: num(row.potential_upside),
    rewardRiskRatio: num(row.reward_risk_ratio),
    liquidityScore: num(row.liquidity_score),
    dataFreshnessScore: num(row.data_freshness_score),
    backtestId: row.backtest_id,
    backtestStatus: row.backtest_status,
    riskBreakdownId: row.risk_breakdown_id,
    status: row.status,
    approvalStatus: row.approval_status,
    estimatedFees: num(row.estimated_fees),
    estimatedSlippage: num(row.estimated_slippage),
    estimatedGas: num(row.estimated_gas),
    agentResearchCost: num(row.agent_research_cost),
    modelInferenceCost: num(row.model_inference_cost),
    netExpectedValue: num(row.net_expected_value),
    notes: row.notes,
    evidence: parseJson(row.evidence_json, []),
    expiresAt: iso(row.expires_at),
    reviewedAt: iso(row.reviewed_at),
    reviewer: row.reviewer,
    decisionReason: row.decision_reason,
    createdAt: iso(row.created_at),
    updatedAt: iso(row.updated_at)
  };
}

export function opportunityParams(opportunity = {}) {
  return [
    opportunity.id,
    opportunity.sourceAgentId || opportunity.source_agent_id,
    opportunity.researchJobId || opportunity.research_job_id || null,
    opportunity.strategyId || opportunity.strategy_id || null,
    opportunity.marketType || opportunity.market_type,
    opportunity.venue,
    opportunity.symbol || null,
    opportunity.marketSlug || opportunity.market_slug || null,
    opportunity.title,
    opportunity.recommendation || 'review',
    opportunity.confidenceScore ?? opportunity.confidence_score ?? 0.5,
    opportunity.winProbability ?? opportunity.win_probability ?? 0.5,
    opportunity.lossProbability ?? opportunity.loss_probability ?? 0.5,
    opportunity.expectedValue ?? opportunity.expected_value ?? 0,
    opportunity.grossExpectedValue ?? opportunity.gross_expected_value ?? 0,
    opportunity.totalMoneyRisked ?? opportunity.total_money_risked ?? 0,
    opportunity.maxLoss ?? opportunity.max_loss ?? 0,
    opportunity.potentialUpside ?? opportunity.potential_upside ?? 0,
    opportunity.rewardRiskRatio ?? opportunity.reward_risk_ratio ?? 0,
    opportunity.liquidityScore ?? opportunity.liquidity_score ?? 50,
    opportunity.dataFreshnessScore ?? opportunity.data_freshness_score ?? 70,
    opportunity.backtestId || opportunity.backtest_id || null,
    opportunity.backtestStatus || opportunity.backtest_status || 'backtest_missing',
    opportunity.riskBreakdownId || opportunity.risk_breakdown_id || null,
    opportunity.status || 'needs_review',
    opportunity.approvalStatus || opportunity.approval_status || opportunity.status || 'needs_review',
    opportunity.estimatedFees ?? opportunity.estimated_fees ?? 0,
    opportunity.estimatedSlippage ?? opportunity.estimated_slippage ?? 0,
    opportunity.estimatedGas ?? opportunity.estimated_gas ?? 0,
    opportunity.agentResearchCost ?? opportunity.agent_research_cost ?? 0,
    opportunity.modelInferenceCost ?? opportunity.model_inference_cost ?? 0,
    opportunity.netExpectedValue ?? opportunity.net_expected_value ?? 0,
    opportunity.notes || null,
    JSON.stringify(opportunity.evidence || opportunity.evidence_json || []),
    opportunity.expiresAt || opportunity.expires_at || null,
    opportunity.reviewedAt || opportunity.reviewed_at || null,
    opportunity.reviewer || null,
    opportunity.decisionReason || opportunity.decision_reason || null,
    opportunity.createdAt || opportunity.created_at || new Date().toISOString(),
    opportunity.updatedAt || opportunity.updated_at || new Date().toISOString()
  ];
}

export function riskBreakdownFromRow(row = {}) {
  return {
    id: row.id,
    scope: row.scope,
    scopeId: row.scope_id,
    aggregateScore: num(row.aggregate_score),
    capitalAtRiskScore: num(row.capital_at_risk_score),
    liquidityScore: num(row.liquidity_score),
    slippageScore: num(row.slippage_score),
    drawdownScore: num(row.drawdown_score),
    volatilityScore: num(row.volatility_score),
    correlationScore: num(row.correlation_score),
    modelConfidenceScore: num(row.model_confidence_score),
    dataFreshnessScore: num(row.data_freshness_score),
    agentCostScore: num(row.agent_cost_score),
    explanation: row.explanation,
    generatedAt: iso(row.generated_at)
  };
}

export function riskBreakdownParams(risk = {}) {
  return [
    risk.id,
    risk.scope || 'opportunity',
    risk.scopeId || risk.scope_id,
    risk.aggregateScore ?? risk.aggregate_score ?? 0,
    risk.capitalAtRiskScore ?? risk.capital_at_risk_score ?? 0,
    risk.liquidityScore ?? risk.liquidity_score ?? 0,
    risk.slippageScore ?? risk.slippage_score ?? 0,
    risk.drawdownScore ?? risk.drawdown_score ?? 0,
    risk.volatilityScore ?? risk.volatility_score ?? 0,
    risk.correlationScore ?? risk.correlation_score ?? 0,
    risk.modelConfidenceScore ?? risk.model_confidence_score ?? 0,
    risk.dataFreshnessScore ?? risk.data_freshness_score ?? 0,
    risk.agentCostScore ?? risk.agent_cost_score ?? 0,
    risk.explanation || '',
    risk.generatedAt || risk.generated_at || new Date().toISOString()
  ];
}

export function agentCostFromRow(row = {}) {
  return {
    id: row.id,
    agentId: row.agent_id,
    jobId: row.job_id,
    model: row.model,
    provider: row.provider,
    localOrRemote: row.local_or_remote,
    promptTokens: num(row.prompt_tokens),
    completionTokens: num(row.completion_tokens),
    totalTokens: num(row.total_tokens),
    remoteApiCost: num(row.remote_api_cost),
    localComputeCost: num(row.local_compute_cost),
    allocatedOpportunityId: row.allocated_opportunity_id,
    createdAt: iso(row.created_at)
  };
}

export function agentCostParams(cost = {}) {
  return [
    cost.id,
    cost.agentId || cost.agent_id,
    cost.jobId || cost.job_id,
    cost.model || 'unknown',
    cost.provider || 'unknown',
    cost.localOrRemote || cost.local_or_remote || 'remote',
    cost.promptTokens || cost.prompt_tokens || 0,
    cost.completionTokens || cost.completion_tokens || 0,
    cost.totalTokens || cost.total_tokens || 0,
    cost.remoteApiCost || cost.remote_api_cost || 0,
    cost.localComputeCost || cost.local_compute_cost || 0,
    cost.allocatedOpportunityId || cost.allocated_opportunity_id || null,
    cost.createdAt || cost.created_at || new Date().toISOString()
  ];
}
