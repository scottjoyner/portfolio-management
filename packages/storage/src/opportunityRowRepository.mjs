import {
  agentCostFromRow,
  budgetApprovalFromRow,
  marketSnapshotFromRow,
  opportunityFromRow,
  researchJobFromRow,
  riskBreakdownFromRow
} from './productRowMappers.mjs';
import { upsertProductRecord, upsertProductRecords } from './productUpserts.mjs';

export const mapBudgetApprovalRow = budgetApprovalFromRow;
export const mapResearchJobRow = researchJobFromRow;
export const mapAgentCostRow = agentCostFromRow;
export const mapMarketSnapshotRow = marketSnapshotFromRow;
export const mapOpportunityRow = opportunityFromRow;
export const mapRiskBreakdownRow = riskBreakdownFromRow;

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

  async listMarketDataSnapshots() {
    const result = await this.query('SELECT * FROM market_data_snapshots ORDER BY timestamp DESC, id ASC');
    return result.rows.map(mapMarketSnapshotRow);
  }

  async upsertMarketDataSnapshot(snapshot) {
    await upsertProductRecord(this.query.bind(this), 'marketDataSnapshot', snapshot);
    return snapshot;
  }

  async upsertMarketDataSnapshots(snapshots = []) {
    await upsertProductRecords(this.query.bind(this), 'marketDataSnapshot', snapshots);
    return snapshots;
  }

  async listBudgetApprovals() {
    const result = await this.query('SELECT * FROM budget_approvals ORDER BY requested_at ASC, id ASC');
    return result.rows.map(mapBudgetApprovalRow);
  }

  async upsertBudgetApproval(approval) {
    await upsertProductRecord(this.query.bind(this), 'budgetApproval', approval);
    return approval;
  }

  async listResearchJobs() {
    const result = await this.query('SELECT * FROM research_jobs ORDER BY started_at ASC, id ASC');
    return result.rows.map(mapResearchJobRow);
  }

  async upsertResearchJob(job) {
    await upsertProductRecord(this.query.bind(this), 'researchJob', job);
    return job;
  }

  async listOpportunities() {
    const result = await this.query('SELECT * FROM opportunities ORDER BY created_at ASC, id ASC');
    return result.rows.map(mapOpportunityRow);
  }

  async upsertOpportunity(opportunity) {
    await upsertProductRecord(this.query.bind(this), 'opportunity', opportunity);
    return opportunity;
  }

  async listRiskBreakdowns() {
    const result = await this.query('SELECT * FROM risk_breakdowns ORDER BY generated_at ASC, id ASC');
    return result.rows.map(mapRiskBreakdownRow);
  }

  async upsertRiskBreakdown(riskBreakdown) {
    await upsertProductRecord(this.query.bind(this), 'riskBreakdown', riskBreakdown);
    return riskBreakdown;
  }

  async listAgentCosts() {
    const result = await this.query('SELECT * FROM agent_cost_ledger ORDER BY created_at ASC, id ASC');
    return result.rows.map(mapAgentCostRow);
  }

  async upsertAgentCost(cost) {
    await upsertProductRecord(this.query.bind(this), 'agentCost', cost);
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
  async upsertOpportunityBundle({ marketDataSnapshots = [], budgetApprovals = [], researchJobs = [], opportunities = [], riskBreakdowns = [], agentCostLedger = [] }) {
    await this.upsertMarketDataSnapshots(marketDataSnapshots);
    await upsertProductRecords(this.query.bind(this), 'budgetApproval', budgetApprovals);
    await upsertProductRecords(this.query.bind(this), 'researchJob', researchJobs);
    await upsertProductRecords(this.query.bind(this), 'opportunity', opportunities);
    await upsertProductRecords(this.query.bind(this), 'riskBreakdown', riskBreakdowns);
    await upsertProductRecords(this.query.bind(this), 'agentCost', agentCostLedger);
    return { marketDataSnapshots, budgetApprovals, researchJobs, opportunities, riskBreakdowns, agentCostLedger };
  }

  async replaceOpportunityWorkflow({ marketDataSnapshots = [], budgetApprovals = [], researchJobs = [], opportunities = [], riskBreakdowns = [], agentCostLedger = [] }) {
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
    await this.query('DELETE FROM market_data_snapshots');
    await this.upsertOpportunityBundle({ marketDataSnapshots, budgetApprovals, researchJobs, opportunities, riskBreakdowns, agentCostLedger });
  }
}
