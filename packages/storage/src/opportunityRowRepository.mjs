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
    await this.query('DELETE FROM market_data_snapshots');
    await this.upsertOpportunityBundle({ marketDataSnapshots, budgetApprovals, researchJobs, opportunities, riskBreakdowns, agentCostLedger });
  }
}
