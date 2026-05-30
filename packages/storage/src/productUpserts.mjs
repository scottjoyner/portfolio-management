import {
  agentBudgetParams,
  agentCostParams,
  budgetApprovalParams,
  marketSnapshotParams,
  opportunityParams,
  researchJobParams,
  riskBreakdownParams
} from './productRowMappers.mjs';

export const PRODUCT_UPSERT_SQL = {
  marketDataSnapshot: `INSERT INTO market_data_snapshots (id, symbol, venue, asset_class, bid, ask, spread_bps, volume_24h, liquidity_score, volatility_score, status, source, timestamp)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (id) DO UPDATE SET symbol = EXCLUDED.symbol, venue = EXCLUDED.venue, asset_class = EXCLUDED.asset_class, bid = EXCLUDED.bid, ask = EXCLUDED.ask, spread_bps = EXCLUDED.spread_bps, volume_24h = EXCLUDED.volume_24h, liquidity_score = EXCLUDED.liquidity_score, volatility_score = EXCLUDED.volatility_score, status = EXCLUDED.status, source = EXCLUDED.source, timestamp = EXCLUDED.timestamp`,

  agentBudget: `INSERT INTO agent_budgets (agent_id, daily_token_limit, daily_cost_limit, per_job_token_limit, per_market_cost_limit, require_approval_above_cost, enabled, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
ON CONFLICT (agent_id) DO UPDATE SET daily_token_limit = EXCLUDED.daily_token_limit, daily_cost_limit = EXCLUDED.daily_cost_limit, per_job_token_limit = EXCLUDED.per_job_token_limit, per_market_cost_limit = EXCLUDED.per_market_cost_limit, require_approval_above_cost = EXCLUDED.require_approval_above_cost, enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at`,

  budgetApproval: `INSERT INTO budget_approvals (id, agent_id, market_scope, opportunity_id, requested_by, reason, status, projected_cost, projected_tokens, approved_cost_limit, approved_token_limit, reviewer, decision_reason, requested_at, reviewed_at, expires_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
ON CONFLICT (id) DO UPDATE SET agent_id = EXCLUDED.agent_id, market_scope = EXCLUDED.market_scope, opportunity_id = EXCLUDED.opportunity_id, requested_by = EXCLUDED.requested_by, reason = EXCLUDED.reason, status = EXCLUDED.status, projected_cost = EXCLUDED.projected_cost, projected_tokens = EXCLUDED.projected_tokens, approved_cost_limit = EXCLUDED.approved_cost_limit, approved_token_limit = EXCLUDED.approved_token_limit, reviewer = EXCLUDED.reviewer, decision_reason = EXCLUDED.decision_reason, requested_at = EXCLUDED.requested_at, reviewed_at = EXCLUDED.reviewed_at, expires_at = EXCLUDED.expires_at`,

  researchJob: `INSERT INTO research_jobs (id, agent_id, trigger_type, market_scope, symbol_scope, provider, model, local_or_remote, status, started_at, completed_at, prompt_tokens, completion_tokens, total_tokens, estimated_remote_cost, estimated_local_cost, budget_approval_id, opportunity_ids_json, failure_reason)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19)
ON CONFLICT (id) DO UPDATE SET agent_id = EXCLUDED.agent_id, trigger_type = EXCLUDED.trigger_type, market_scope = EXCLUDED.market_scope, symbol_scope = EXCLUDED.symbol_scope, provider = EXCLUDED.provider, model = EXCLUDED.model, local_or_remote = EXCLUDED.local_or_remote, status = EXCLUDED.status, started_at = EXCLUDED.started_at, completed_at = EXCLUDED.completed_at, prompt_tokens = EXCLUDED.prompt_tokens, completion_tokens = EXCLUDED.completion_tokens, total_tokens = EXCLUDED.total_tokens, estimated_remote_cost = EXCLUDED.estimated_remote_cost, estimated_local_cost = EXCLUDED.estimated_local_cost, budget_approval_id = EXCLUDED.budget_approval_id, opportunity_ids_json = EXCLUDED.opportunity_ids_json, failure_reason = EXCLUDED.failure_reason`,

  opportunity: `INSERT INTO opportunities (id, source_agent_id, research_job_id, strategy_id, market_type, venue, symbol, market_slug, title, recommendation, confidence_score, win_probability, loss_probability, expected_value, gross_expected_value, total_money_risked, max_loss, potential_upside, reward_risk_ratio, liquidity_score, data_freshness_score, backtest_id, backtest_status, risk_breakdown_id, status, approval_status, estimated_fees, estimated_slippage, estimated_gas, agent_research_cost, model_inference_cost, net_expected_value, notes, evidence_json, expires_at, reviewed_at, reviewer, decision_reason, created_at, updated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40)
ON CONFLICT (id) DO UPDATE SET source_agent_id = EXCLUDED.source_agent_id, research_job_id = EXCLUDED.research_job_id, strategy_id = EXCLUDED.strategy_id, market_type = EXCLUDED.market_type, venue = EXCLUDED.venue, symbol = EXCLUDED.symbol, market_slug = EXCLUDED.market_slug, title = EXCLUDED.title, recommendation = EXCLUDED.recommendation, confidence_score = EXCLUDED.confidence_score, win_probability = EXCLUDED.win_probability, loss_probability = EXCLUDED.loss_probability, expected_value = EXCLUDED.expected_value, gross_expected_value = EXCLUDED.gross_expected_value, total_money_risked = EXCLUDED.total_money_risked, max_loss = EXCLUDED.max_loss, potential_upside = EXCLUDED.potential_upside, reward_risk_ratio = EXCLUDED.reward_risk_ratio, liquidity_score = EXCLUDED.liquidity_score, data_freshness_score = EXCLUDED.data_freshness_score, backtest_id = EXCLUDED.backtest_id, backtest_status = EXCLUDED.backtest_status, risk_breakdown_id = EXCLUDED.risk_breakdown_id, status = EXCLUDED.status, approval_status = EXCLUDED.approval_status, estimated_fees = EXCLUDED.estimated_fees, estimated_slippage = EXCLUDED.estimated_slippage, estimated_gas = EXCLUDED.estimated_gas, agent_research_cost = EXCLUDED.agent_research_cost, model_inference_cost = EXCLUDED.model_inference_cost, net_expected_value = EXCLUDED.net_expected_value, notes = EXCLUDED.notes, evidence_json = EXCLUDED.evidence_json, expires_at = EXCLUDED.expires_at, reviewed_at = EXCLUDED.reviewed_at, reviewer = EXCLUDED.reviewer, decision_reason = EXCLUDED.decision_reason, created_at = EXCLUDED.created_at, updated_at = EXCLUDED.updated_at`,

  riskBreakdown: `INSERT INTO risk_breakdowns (id, scope, scope_id, aggregate_score, capital_at_risk_score, liquidity_score, slippage_score, drawdown_score, volatility_score, correlation_score, model_confidence_score, data_freshness_score, agent_cost_score, explanation, generated_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
ON CONFLICT (id) DO UPDATE SET scope = EXCLUDED.scope, scope_id = EXCLUDED.scope_id, aggregate_score = EXCLUDED.aggregate_score, capital_at_risk_score = EXCLUDED.capital_at_risk_score, liquidity_score = EXCLUDED.liquidity_score, slippage_score = EXCLUDED.slippage_score, drawdown_score = EXCLUDED.drawdown_score, volatility_score = EXCLUDED.volatility_score, correlation_score = EXCLUDED.correlation_score, model_confidence_score = EXCLUDED.model_confidence_score, data_freshness_score = EXCLUDED.data_freshness_score, agent_cost_score = EXCLUDED.agent_cost_score, explanation = EXCLUDED.explanation, generated_at = EXCLUDED.generated_at`,

  agentCost: `INSERT INTO agent_cost_ledger (id, agent_id, job_id, model, provider, local_or_remote, prompt_tokens, completion_tokens, total_tokens, remote_api_cost, local_compute_cost, allocated_opportunity_id, created_at)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
ON CONFLICT (id) DO UPDATE SET agent_id = EXCLUDED.agent_id, job_id = EXCLUDED.job_id, model = EXCLUDED.model, provider = EXCLUDED.provider, local_or_remote = EXCLUDED.local_or_remote, prompt_tokens = EXCLUDED.prompt_tokens, completion_tokens = EXCLUDED.completion_tokens, total_tokens = EXCLUDED.total_tokens, remote_api_cost = EXCLUDED.remote_api_cost, local_compute_cost = EXCLUDED.local_compute_cost, allocated_opportunity_id = EXCLUDED.allocated_opportunity_id, created_at = EXCLUDED.created_at`
};

export const PRODUCT_PARAM_BUILDERS = {
  marketDataSnapshot: marketSnapshotParams,
  agentBudget: agentBudgetParams,
  budgetApproval: budgetApprovalParams,
  researchJob: researchJobParams,
  opportunity: opportunityParams,
  riskBreakdown: riskBreakdownParams,
  agentCost: agentCostParams
};

export async function upsertProductRecord(query, type, record) {
  const sql = PRODUCT_UPSERT_SQL[type];
  const params = PRODUCT_PARAM_BUILDERS[type]?.(record);
  if (!sql || !params) throw new Error(`unknown_product_record_type:${type}`);
  return query(sql, params);
}

export async function upsertProductRecords(query, type, records = []) {
  const results = [];
  for (const record of records) results.push(await upsertProductRecord(query, type, record));
  return results;
}
