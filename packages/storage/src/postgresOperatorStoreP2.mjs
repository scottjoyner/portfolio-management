import { PostgresOperatorStoreP1 } from './postgresOperatorStoreP1.mjs';
import { normalizeOperatorState } from './operatorStore.mjs';
import { OpportunityRowRepository } from './opportunityRowRepository.mjs';

export class PostgresOperatorStoreP2 extends PostgresOperatorStoreP1 {
  constructor(options = {}) {
    super(options);
    this.kind = 'postgres-p2';
    this.opportunityRows = new OpportunityRowRepository(this);
  }

  async checkMigrations() {
    const migrations = await super.checkMigrations();
    if (!migrations.ok) return migrations;
    const hasOpportunityWorkflow = migrations.applied.includes('004_opportunity_agent_workflow') || migrations.reason === 'schema_migrations_table_missing_legacy_ok';
    if (!hasOpportunityWorkflow) {
      this.migrations = { ...migrations, ok: false, reason: 'opportunity_agent_workflow_migration_missing' };
      return this.migrations;
    }
    this.migrations = migrations;
    return this.migrations;
  }

  async load() {
    const base = await super.load();
    const [opportunities, riskBreakdowns, budgetApprovals, researchJobs, agentCostLedger] = await Promise.all([
      this.opportunityRows.listOpportunities(),
      this.opportunityRows.listRiskBreakdowns(),
      this.opportunityRows.listBudgetApprovals(),
      this.opportunityRows.listResearchJobs(),
      this.opportunityRows.listAgentCosts()
    ]);
    this.state = normalizeOperatorState({
      ...base,
      opportunities,
      riskBreakdowns,
      budgetApprovals,
      researchJobs,
      agentCostLedger
    });
    return this.state;
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    await super.save(state);
    await this.query('BEGIN');
    try {
      await this.opportunityRows.replaceOpportunityWorkflow(state);
      await this.query('COMMIT');
      this.state = state;
      return this.state;
    } catch (error) {
      await this.query('ROLLBACK').catch(() => {});
      this.lastError = error;
      throw error;
    }
  }

  async upsertBudgetApproval(approval) {
    await this.opportunityRows.upsertBudgetApproval(approval);
    return approval;
  }

  async upsertResearchJob(job) {
    await this.opportunityRows.upsertResearchJob(job);
    return job;
  }

  async upsertAgentCost(cost) {
    await this.opportunityRows.upsertAgentCost(cost);
    return cost;
  }

  async upsertOpportunity(opportunity) {
    await this.opportunityRows.upsertOpportunity(opportunity);
    return opportunity;
  }

  async upsertRiskBreakdown(riskBreakdown) {
    await this.opportunityRows.upsertRiskBreakdown(riskBreakdown);
    return riskBreakdown;
  }

  getStatus() {
    return { ...super.getStatus(), opportunityWorkflowLayer: 'p2-row-tables' };
  }
}
