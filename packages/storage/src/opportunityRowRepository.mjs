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

function rowJson(value, fallback) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

export function mapBudgetApprovalRow(row = {}) {
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

export function mapResearchJobRow(row = {}) {
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
    opportunityIdsCreated: rowJson(row.opportunity_ids_json, []),
    failureReason: row.failure_reason
  };
}

export function mapAgentCostRow(row = {}) {
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

export function mapOpportunityRow(row = {}) {
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
    notes: row.notes || '',
    evidence: rowJson(row.evidence_json, []),
    expiresAt: iso(row.expires_at),
    reviewedAt: iso(row.reviewed_at),
    reviewer: row.reviewer,
    decisionReason: row.decision_reason,
    createdAt: iso(row.created_at),
    updatedAt: iso(row.updated_at)
  };
}

export function mapRiskBreakdownRow(row = {}) {
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

export class OpportunityRowRepository {
  constructor(store) {
    this.store = store;
  }

  async query(sql, params = []) {
    return this.store.query(sql, params);
  }

  async listBudgetApprovals() {
    const result = await this.query('SELECT * FROM budget_approvals ORDER BY requested_at ASC, id ASC');
    return result.rows.map(mapBudgetApprovalRow);
  }

  async upsertBudgetApproval(approval) {
    await this.query(
      `INSERT INTO budget_approvals (id, agent_id, market_scope, opportunity_id, requested_by, reason, status, projected_cost, projected_tokens, approved_cost_limit, approved_token_limit, reviewer, decision_reason, requested_at, reviewed_at, expires_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
       ON CONFLICT (id) DO UPDATE SET
         agent_id = EXCLUDED.agent_id,
         market_scope = EXCLUDED.market_scope,
         opportunity_id = EXCLUDED.opportunity_id,
         requested_by = EXCLUDED.requested_by,
         reason = EXCLUDED.reason,
         status = EXCLUDED.status,
         projected_cost = EXCLUDED.projected_cost,
         projected_tokens = EXCLUDED.projected_tokens,
         approved_cost_limit = EXCLUDED.approved_cost_limit,
         approved_token_limit = EXCLUDED.approved_token_limit,
         reviewer = EXCLUDED.reviewer,
         decision_reason = EXCLUDED.decision_reason,
         reviewed_at = EXCLUDED.reviewed_at,
         expires_at = EXCLUDED.expires_at`,
      [approval.id, approval.agentId || approval.agent_id, approval.marketScope || approval.market_scope || null, approval.opportunityId || approval.opportunity_id || null, approval.requestedBy || approval.requested_by || 'operator', approval.reason || 'additional research budget requested', approval.status || 'pending_review', approval.projectedCost ?? approval.projected_cost ?? 0, approval.projectedTokens ?? approval.projected_tokens ?? 0, approval.approvedCostLimit ?? approval.approved_cost_limit ?? 0, approval.approvedTokenLimit ?? approval.approved_token_limit ?? 0, approval.reviewer || null, approval.decisionReason || approval.decision_reason || null, approval.requestedAt || approval.requested_at || new Date().toISOString(), approval.reviewedAt || approval.reviewed_at || null, approval.expiresAt || approval.expires_at || null]
    );
    return approval;
  }

  async listResearchJobs() {
    const result = await this.query('SELECT * FROM research_jobs ORDER BY started_at ASC, id ASC');
    return result.rows.map(mapResearchJobRow);
  }

  async upsertResearchJob(job) {
    await this.query(
      `INSERT INTO research_jobs (id, agent_id, trigger_type, market_scope, symbol_scope, provider, model, local_or_remote, status, started_at, completed_at, prompt_tokens, completion_tokens, total_tokens, estimated_remote_cost, estimated_local_cost, budget_approval_id, opportunity_ids_json, failure_reason)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
       ON CONFLICT (id) DO UPDATE SET
         status = EXCLUDED.status,
         completed_at = EXCLUDED.completed_at,
         prompt_tokens = EXCLUDED.prompt_tokens,
         completion_tokens = EXCLUDED.completion_tokens,
         total_tokens = EXCLUDED.total_tokens,
         estimated_remote_cost = EXCLUDED.estimated_remote_cost,
         estimated_local_cost = EXCLUDED.estimated_local_cost,
         budget_approval_id = EXCLUDED.budget_approval_id,
         opportunity_ids_json = EXCLUDED.opportunity_ids_json,
         failure_reason = EXCLUDED.failure_reason`,
      [job.id, job.agentId || job.agent_id, job.triggerType || job.trigger_type || 'operator_request', job.marketScope || job.market_scope || 'general', job.symbolScope || job.symbol_scope || null, job.provider || 'unknown', job.model || 'unknown', job.localOrRemote || job.local_or_remote || 'remote', job.status || 'completed', job.startedAt || job.started_at || new Date().toISOString(), job.completedAt || job.completed_at || null, job.promptTokens || job.prompt_tokens || 0, job.completionTokens || job.completion_tokens || 0, job.totalTokens || job.total_tokens || 0, job.estimatedRemoteCost || job.estimated_remote_cost || 0, job.estimatedLocalCost || job.estimated_local_cost || 0, job.budgetApprovalId || job.budget_approval_id || null, JSON.stringify(job.opportunityIdsCreated || job.opportunity_ids_json || []), job.failureReason || job.failure_reason || null]
    );
    return job;
  }

  async listAgentCosts() {
    const result = await this.query('SELECT * FROM agent_cost_ledger ORDER BY created_at ASC, id ASC');
    return result.rows.map(mapAgentCostRow);
  }

  async upsertAgentCost(cost) {
    await this.query(
      `INSERT INTO agent_cost_ledger (id, agent_id, job_id, model, provider, local_or_remote, prompt_tokens, completion_tokens, total_tokens, remote_api_cost, local_compute_cost, allocated_opportunity_id, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       ON CONFLICT (id) DO UPDATE SET
         allocated_opportunity_id = EXCLUDED.allocated_opportunity_id`,
      [cost.id, cost.agentId || cost.agent_id, cost.jobId || cost.job_id, cost.model || 'unknown', cost.provider || 'unknown', cost.localOrRemote || cost.local_or_remote || 'remote', cost.promptTokens || cost.prompt_tokens || 0, cost.completionTokens || cost.completion_tokens || 0, cost.totalTokens || cost.total_tokens || 0, cost.remoteApiCost || cost.remote_api_cost || 0, cost.localComputeCost || cost.local_compute_cost || 0, cost.allocatedOpportunityId || cost.allocated_opportunity_id || null, cost.createdAt || cost.created_at || new Date().toISOString()]
    );
    return cost;
  }

  async listOpportunities() {
    const result = await this.query('SELECT * FROM opportunities ORDER BY created_at ASC, id ASC');
    return result.rows.map(mapOpportunityRow);
  }

  async upsertOpportunity(opportunity) {
    await this.query(
      `INSERT INTO opportunities (id, source_agent_id, research_job_id, strategy_id, market_type, venue, symbol, market_slug, title, recommendation, confidence_score, win_probability, loss_probability, expected_value, gross_expected_value, total_money_risked, max_loss, potential_upside, reward_risk_ratio, liquidity_score, data_freshness_score, backtest_id, backtest_status, risk_breakdown_id, status, approval_status, estimated_fees, estimated_slippage, estimated_gas, agent_research_cost, model_inference_cost, net_expected_value, notes, evidence_json, expires_at, reviewed_at, reviewer, decision_reason, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40)
       ON CONFLICT (id) DO UPDATE SET
         risk_breakdown_id = EXCLUDED.risk_breakdown_id,
         status = EXCLUDED.status,
         approval_status = EXCLUDED.approval_status,
         reviewed_at = EXCLUDED.reviewed_at,
         reviewer = EXCLUDED.reviewer,
         decision_reason = EXCLUDED.decision_reason,
         updated_at = EXCLUDED.updated_at`,
      [opportunity.id, opportunity.sourceAgentId || opportunity.source_agent_id || 'market-research-agent', opportunity.researchJobId || opportunity.research_job_id || null, opportunity.strategyId || opportunity.strategy_id || null, opportunity.marketType || opportunity.market_type, opportunity.venue, opportunity.symbol || null, opportunity.marketSlug || opportunity.market_slug || null, opportunity.title, opportunity.recommendation || 'review', opportunity.confidenceScore ?? opportunity.confidence_score ?? 0.5, opportunity.winProbability ?? opportunity.win_probability ?? 0.5, opportunity.lossProbability ?? opportunity.loss_probability ?? 0.5, opportunity.expectedValue ?? opportunity.expected_value ?? 0, opportunity.grossExpectedValue ?? opportunity.gross_expected_value ?? 0, opportunity.totalMoneyRisked ?? opportunity.total_money_risked ?? 0, opportunity.maxLoss ?? opportunity.max_loss ?? 0, opportunity.potentialUpside ?? opportunity.potential_upside ?? 0, opportunity.rewardRiskRatio ?? opportunity.reward_risk_ratio ?? 0, opportunity.liquidityScore ?? opportunity.liquidity_score ?? 50, opportunity.dataFreshnessScore ?? opportunity.data_freshness_score ?? 70, opportunity.backtestId || opportunity.backtest_id || null, opportunity.backtestStatus || opportunity.backtest_status || 'backtest_missing', opportunity.riskBreakdownId || opportunity.risk_breakdown_id || null, opportunity.status || 'needs_review', opportunity.approvalStatus || opportunity.approval_status || 'needs_review', opportunity.estimatedFees ?? opportunity.estimated_fees ?? 0, opportunity.estimatedSlippage ?? opportunity.estimated_slippage ?? 0, opportunity.estimatedGas ?? opportunity.estimated_gas ?? 0, opportunity.agentResearchCost ?? opportunity.agent_research_cost ?? 0, opportunity.modelInferenceCost ?? opportunity.model_inference_cost ?? 0, opportunity.netExpectedValue ?? opportunity.net_expected_value ?? 0, opportunity.notes || '', JSON.stringify(opportunity.evidence || opportunity.evidence_json || []), opportunity.expiresAt || opportunity.expires_at || null, opportunity.reviewedAt || opportunity.reviewed_at || null, opportunity.reviewer || null, opportunity.decisionReason || opportunity.decision_reason || null, opportunity.createdAt || opportunity.created_at || new Date().toISOString(), opportunity.updatedAt || opportunity.updated_at || new Date().toISOString()]
    );
    return opportunity;
  }

  async listRiskBreakdowns() {
    const result = await this.query('SELECT * FROM risk_breakdowns ORDER BY generated_at ASC, id ASC');
    return result.rows.map(mapRiskBreakdownRow);
  }

  async upsertRiskBreakdown(risk) {
    await this.query(
      `INSERT INTO risk_breakdowns (id, scope, scope_id, aggregate_score, capital_at_risk_score, liquidity_score, slippage_score, drawdown_score, volatility_score, correlation_score, model_confidence_score, data_freshness_score, agent_cost_score, explanation, generated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
       ON CONFLICT (id) DO UPDATE SET
         aggregate_score = EXCLUDED.aggregate_score,
         capital_at_risk_score = EXCLUDED.capital_at_risk_score,
         liquidity_score = EXCLUDED.liquidity_score,
         slippage_score = EXCLUDED.slippage_score,
         drawdown_score = EXCLUDED.drawdown_score,
         volatility_score = EXCLUDED.volatility_score,
         correlation_score = EXCLUDED.correlation_score,
         model_confidence_score = EXCLUDED.model_confidence_score,
         data_freshness_score = EXCLUDED.data_freshness_score,
         agent_cost_score = EXCLUDED.agent_cost_score,
         explanation = EXCLUDED.explanation,
         generated_at = EXCLUDED.generated_at`,
      [risk.id, risk.scope || 'opportunity', risk.scopeId || risk.scope_id, risk.aggregateScore ?? risk.aggregate_score ?? 0, risk.capitalAtRiskScore ?? risk.capital_at_risk_score ?? 0, risk.liquidityScore ?? risk.liquidity_score ?? 0, risk.slippageScore ?? risk.slippage_score ?? 0, risk.drawdownScore ?? risk.drawdown_score ?? 0, risk.volatilityScore ?? risk.volatility_score ?? 0, risk.correlationScore ?? risk.correlation_score ?? 0, risk.modelConfidenceScore ?? risk.model_confidence_score ?? 0, risk.dataFreshnessScore ?? risk.data_freshness_score ?? 0, risk.agentCostScore ?? risk.agent_cost_score ?? 0, risk.explanation || '', risk.generatedAt || risk.generated_at || new Date().toISOString()]
    );
    return risk;
  }

  async replaceOpportunityWorkflow({ opportunities = [], riskBreakdowns = [], budgetApprovals = [], researchJobs = [], agentCostLedger = [] }) {
    await this.query('DELETE FROM agent_cost_ledger');
    await this.query('DELETE FROM risk_breakdowns');
    await this.query('DELETE FROM opportunities');
    await this.query('DELETE FROM research_jobs');
    await this.query('DELETE FROM budget_approvals');
    for (const approval of budgetApprovals) await this.upsertBudgetApproval(approval);
    for (const job of researchJobs) await this.upsertResearchJob(job);
    for (const opportunity of opportunities) await this.upsertOpportunity(opportunity);
    for (const risk of riskBreakdowns) await this.upsertRiskBreakdown(risk);
    for (const cost of agentCostLedger) await this.upsertAgentCost(cost);
  }
}
