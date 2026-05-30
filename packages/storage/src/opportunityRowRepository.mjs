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

  async replaceOpportunityWorkflow({ budgetApprovals = [], researchJobs = [], agentCostLedger = [] }) {
    await this.query('DELETE FROM agent_cost_ledger');
    await this.query('DELETE FROM research_jobs');
    await this.query('DELETE FROM budget_approvals');
    for (const approval of budgetApprovals) await this.upsertBudgetApproval(approval);
    for (const job of researchJobs) await this.upsertResearchJob(job);
    for (const cost of agentCostLedger) await this.upsertAgentCost(cost);
  }
}
