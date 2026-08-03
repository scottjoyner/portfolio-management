import { PostgresOperatorStoreP1 } from './postgresOperatorStoreP1.mjs';
import { normalizeOperatorState } from './operatorStore.mjs';
import { OpportunityRowRepository } from './opportunityRowRepository.mjs';

function rowJson(value, fallback) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

function mergeRows(baseRows = [], overlays = []) {
  const byId = new Map(overlays.filter(row => row?.id).map(row => [row.id, row]));
  return baseRows.map(row => ({ ...(byId.get(row.id) || {}), ...row }));
}

function economicDocument(state) {
  return {
    schemaVersion: Number(state.schemaVersion || 5),
    savedAt: new Date().toISOString(),
    config: state.config || {},
    modelPricingSnapshots: state.modelPricingSnapshots || [],
    modelUsageLedger: state.modelUsageLedger || [],
    priceForecasts: state.priceForecasts || [],
    forecastOutcomes: state.forecastOutcomes || [],
    executionCostSnapshots: state.executionCostSnapshots || [],
    economicDecisions: state.economicDecisions || [],
    agentAttributionRecords: state.agentAttributionRecords || [],
    economicAttributionQueue: state.economicAttributionQueue || [],
    economicMaintenance: state.economicMaintenance || null,
    researchJobs: state.researchJobs || [],
    opportunities: state.opportunities || [],
    agentCostLedger: state.agentCostLedger || [],
  };
}

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

  async loadOperatorDocuments() {
    const result = await this.query("SELECT key, value_json, updated_at FROM operator_flags WHERE key IN ('executions', 'economic_state')");
    const values = Object.fromEntries((result.rows || []).map(row => [row.key, rowJson(row.value_json, null)]));
    return {
      executions: Array.isArray(values.executions) ? values.executions : [],
      economic: values.economic_state && typeof values.economic_state === 'object' ? values.economic_state : {},
    };
  }

  async load() {
    const base = await super.load();
    const [marketDataSnapshots, budgetApprovals, researchJobs, opportunities, riskBreakdowns, agentCostLedger, documents] = await Promise.all([
      this.opportunityRows.listMarketDataSnapshots(),
      this.opportunityRows.listBudgetApprovals(),
      this.opportunityRows.listResearchJobs(),
      this.opportunityRows.listOpportunities(),
      this.opportunityRows.listRiskBreakdowns(),
      this.opportunityRows.listAgentCosts(),
      this.loadOperatorDocuments(),
    ]);
    const economic = documents.economic || {};
    this.state = normalizeOperatorState({
      ...base,
      schemaVersion: Math.max(Number(base.schemaVersion || 0), Number(economic.schemaVersion || 0)),
      config: { ...(base.config || {}), ...(economic.config || {}) },
      marketDataSnapshots,
      budgetApprovals,
      researchJobs: mergeRows(researchJobs, economic.researchJobs),
      opportunities: mergeRows(opportunities, economic.opportunities),
      riskBreakdowns,
      agentCostLedger: mergeRows(agentCostLedger, economic.agentCostLedger),
      executions: documents.executions.length ? documents.executions : base.executions,
      modelPricingSnapshots: economic.modelPricingSnapshots || [],
      modelUsageLedger: economic.modelUsageLedger || [],
      priceForecasts: economic.priceForecasts || [],
      forecastOutcomes: economic.forecastOutcomes || [],
      executionCostSnapshots: economic.executionCostSnapshots || [],
      economicDecisions: economic.economicDecisions || [],
      agentAttributionRecords: economic.agentAttributionRecords || [],
      economicAttributionQueue: economic.economicAttributionQueue || [],
      economicMaintenance: economic.economicMaintenance || base.economicMaintenance,
    });
    return this.state;
  }

  async saveEconomicDocument(state) {
    await this.query(
      `INSERT INTO operator_flags (key, value_json, updated_at)
       VALUES ($1,$2,$3)
       ON CONFLICT (key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at`,
      ['economic_state', JSON.stringify(economicDocument(state)), new Date().toISOString()],
    );
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    await super.save(state);
    await this.query('BEGIN');
    try {
      await this.opportunityRows.replaceOpportunityWorkflow(state);
      await this.saveEconomicDocument(state);
      await this.query('COMMIT');
      this.state = state;
      return this.state;
    } catch (error) {
      await this.query('ROLLBACK').catch(() => {});
      this.lastError = error;
      throw error;
    }
  }

  async upsertMarketDataSnapshot(snapshot) {
    await this.opportunityRows.upsertMarketDataSnapshot(snapshot);
    return snapshot;
  }

  async upsertMarketDataSnapshots(snapshots = []) {
    await this.opportunityRows.upsertMarketDataSnapshots(snapshots);
    return snapshots;
  }

  async upsertBudgetApproval(approval) {
    await this.opportunityRows.upsertBudgetApproval(approval);
    return approval;
  }

  async upsertResearchJob(job) {
    await this.opportunityRows.upsertResearchJob(job);
    return job;
  }

  async upsertOpportunity(opportunity) {
    await this.opportunityRows.upsertOpportunity(opportunity);
    return opportunity;
  }

  async upsertRiskBreakdown(riskBreakdown) {
    await this.opportunityRows.upsertRiskBreakdown(riskBreakdown);
    return riskBreakdown;
  }

  async upsertAgentCost(cost) {
    await this.opportunityRows.upsertAgentCost(cost);
    return cost;
  }

  async upsertOpportunityBundle(bundle) {
    return this.opportunityRows.upsertOpportunityBundle(bundle);
  }

  getStatus() {
    return {
      ...super.getStatus(),
      opportunityWorkflowLayer: 'p2-row-tables',
      targetedProductMutations: true,
      economicStatePersistence: 'operator_flags_json_v1',
    };
  }
}
