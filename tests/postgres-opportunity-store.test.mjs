import test from 'node:test';
import assert from 'node:assert/strict';
import { PostgresOperatorStore } from '../packages/storage/src/postgresOperatorStore.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { createResearchJob, createOpportunity, decideOpportunity, ensureOpportunityState } from '../apps/api/src/opportunityFlows.mjs';

class FakePgClient {
  constructor() {
    this.calls = [];
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    if (sql.includes("to_regclass('public.schema_migrations')")) {
      return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies', opportunities: 'opportunities' }] };
    }
    if (sql === 'SELECT version FROM schema_migrations ORDER BY version ASC') {
      return { rows: [{ version: '001_operator_state.sql' }, { version: '004_opportunity_agent_workflow.sql' }] };
    }
    if (sql.startsWith('SELECT * FROM strategies')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM backtest_runs')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM approvals')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM positions')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM audit_events')) return { rows: [] };
    if (sql.startsWith('SELECT key, value_json')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM market_data_snapshots')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM agent_budgets')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM research_jobs')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM opportunities')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM risk_breakdowns')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM agent_cost_ledger')) return { rows: [] };
    return { rows: [] };
  }

  findSql(fragment) {
    return this.calls.find(call => call.sql.includes(fragment));
  }
}

test('Postgres store rejects unmigrated opportunity workflow tables', async () => {
  const client = new FakePgClient();
  client.query = async sql => {
    if (sql.includes("to_regclass('public.schema_migrations')")) {
      return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies', opportunities: null }] };
    }
    return { rows: [] };
  };
  const store = new PostgresOperatorStore({ client });
  const migrations = await store.checkMigrations();
  assert.equal(migrations.ok, false);
  assert.equal(migrations.reason, 'opportunity_agent_tables_missing');
});

test('Postgres store saves opportunity agent workflow tables', async () => {
  const client = new FakePgClient();
  const state = createInitialOperatorState();
  ensureOpportunityState(state);
  state.marketDataSnapshots.push({
    id: 'md-store-test-eth',
    symbol: 'ETH-USD',
    venue: 'coinbase-paper',
    assetClass: 'crypto',
    bid: 2999,
    ask: 3001,
    spreadBps: 6.67,
    volume24h: 1000000,
    liquidityScore: 80,
    volatilityScore: 40,
    status: 'paper_only',
    source: 'postgres-opportunity-store-test',
    timestamp: '2026-05-30T00:00:00.000Z',
  });
  const job = createResearchJob(state, {
    agentId: 'store-test-agent',
    model: 'local-test-model',
    localOrRemote: 'local',
    promptTokens: 1000,
    completionTokens: 500,
    totalTokens: 1500,
    runtimeSeconds: 90,
    approvedBudgetOverride: true,
    marketScope: 'ETH-USD',
  });
  assert.ok(job.job.id);
  const created = createOpportunity(state, {
    researchJobId: job.job.id,
    marketType: 'crypto_spot',
    venue: 'coinbase-paper',
    symbol: 'ETH-USD',
    title: 'Postgres store opportunity',
    recommendation: 'paper_review',
    confidenceScore: 0.6,
    winProbability: 0.55,
    lossProbability: 0.45,
    grossExpectedValue: 50,
    totalMoneyRisked: 500,
    maxLoss: 100,
    potentialUpside: 220,
    estimatedFees: 2,
    estimatedSlippage: 3,
    agentResearchCost: 1,
    modelInferenceCost: 1,
  });
  assert.ok(created.opportunity.id);
  decideOpportunity(state, created.opportunity.id, { status: 'approved', reviewer: 'store-test' });

  const store = new PostgresOperatorStore({ client });
  await store.save(state);

  assert.ok(client.findSql('INSERT INTO market_data_snapshots'));
  assert.ok(client.findSql('INSERT INTO agent_budgets'));
  assert.ok(client.findSql('INSERT INTO research_jobs'));
  assert.ok(client.findSql('INSERT INTO opportunities'));
  assert.ok(client.findSql('INSERT INTO risk_breakdowns'));
  assert.ok(client.findSql('INSERT INTO agent_cost_ledger'));

  const opportunityInsert = client.findSql('INSERT INTO opportunities');
  assert.ok(opportunityInsert.params.includes('Postgres store opportunity'));
  assert.ok(opportunityInsert.params.includes('approved'));

  const costInsert = client.findSql('INSERT INTO agent_cost_ledger');
  assert.ok(costInsert.params.includes(job.job.id));
});

test('Postgres store loads opportunity agent workflow tables', async () => {
  const client = new FakePgClient();
  client.query = async (sql, params = []) => {
    client.calls.push({ sql, params });
    if (sql.includes("to_regclass('public.schema_migrations')")) return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies', opportunities: 'opportunities' }] };
    if (sql === 'SELECT version FROM schema_migrations ORDER BY version ASC') return { rows: [{ version: '004_opportunity_agent_workflow.sql' }] };
    if (sql.startsWith('SELECT * FROM strategies')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM backtest_runs')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM approvals')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM positions')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM audit_events')) return { rows: [] };
    if (sql.startsWith('SELECT key, value_json')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM market_data_snapshots')) return { rows: [{ id: 'md-1', symbol: 'ETH-USD', venue: 'coinbase-paper', asset_class: 'crypto', bid: '100', ask: '101', spread_bps: '100', volume_24h: '1000', liquidity_score: '80', volatility_score: '40', status: 'eligible', source: 'test', timestamp: '2026-05-30T00:00:00.000Z' }] };
    if (sql.startsWith('SELECT * FROM agent_budgets')) return { rows: [{ agent_id: 'agent-1', daily_token_limit: '1000', daily_cost_limit: '10', per_job_token_limit: '500', per_market_cost_limit: '5', require_approval_above_cost: '4', enabled: true, updated_at: '2026-05-30T00:00:00.000Z' }] };
    if (sql.startsWith('SELECT * FROM research_jobs')) return { rows: [{ id: 'job-1', agent_id: 'agent-1', trigger_type: 'operator_request', market_scope: 'ETH-USD', symbol_scope: 'ETH-USD', provider: 'local', model: 'local-test', local_or_remote: 'local', status: 'completed', started_at: '2026-05-30T00:00:00.000Z', completed_at: '2026-05-30T00:01:00.000Z', prompt_tokens: '100', completion_tokens: '50', total_tokens: '150', estimated_remote_cost: '0', estimated_local_cost: '0.01', opportunity_ids_json: ['opp-1'], failure_reason: null }] };
    if (sql.startsWith('SELECT * FROM opportunities')) return { rows: [{ id: 'opp-1', source_agent_id: 'agent-1', research_job_id: 'job-1', strategy_id: null, market_type: 'crypto_spot', venue: 'coinbase-paper', symbol: 'ETH-USD', market_slug: null, title: 'Loaded opportunity', recommendation: 'paper_review', confidence_score: '0.6', win_probability: '0.55', loss_probability: '0.45', expected_value: '50', gross_expected_value: '50', total_money_risked: '500', max_loss: '100', potential_upside: '220', reward_risk_ratio: '2.2', liquidity_score: '80', data_freshness_score: '90', backtest_id: null, backtest_status: 'backtest_missing', risk_breakdown_id: 'risk-1', status: 'needs_review', approval_status: 'needs_review', estimated_fees: '2', estimated_slippage: '3', estimated_gas: '0', agent_research_cost: '1', model_inference_cost: '1', net_expected_value: '43', notes: 'loaded', evidence_json: [], expires_at: null, reviewed_at: null, reviewer: null, decision_reason: null, created_at: '2026-05-30T00:00:00.000Z', updated_at: '2026-05-30T00:00:00.000Z' }] };
    if (sql.startsWith('SELECT * FROM risk_breakdowns')) return { rows: [{ id: 'risk-1', scope: 'opportunity', scope_id: 'opp-1', aggregate_score: '42', capital_at_risk_score: '20', liquidity_score: '80', slippage_score: '10', drawdown_score: '10', volatility_score: '40', correlation_score: '35', model_confidence_score: '60', data_freshness_score: '90', agent_cost_score: '6', explanation: 'loaded risk', generated_at: '2026-05-30T00:00:00.000Z' }] };
    if (sql.startsWith('SELECT * FROM agent_cost_ledger')) return { rows: [{ id: 'cost-1', agent_id: 'agent-1', job_id: 'job-1', model: 'local-test', provider: 'local', local_or_remote: 'local', prompt_tokens: '100', completion_tokens: '50', total_tokens: '150', remote_api_cost: '0', local_compute_cost: '0.01', allocated_opportunity_id: 'opp-1', created_at: '2026-05-30T00:00:00.000Z' }] };
    return { rows: [] };
  };

  const store = new PostgresOperatorStore({ client, bootstrap: false });
  const state = await store.load();
  assert.equal(state.marketDataSnapshots[0].symbol, 'ETH-USD');
  assert.equal(state.agentBudgets[0].agentId, 'agent-1');
  assert.equal(state.researchJobs[0].id, 'job-1');
  assert.equal(state.opportunities[0].title, 'Loaded opportunity');
  assert.equal(state.riskBreakdowns[0].aggregateScore, 42);
  assert.equal(state.agentCostLedger[0].allocatedOpportunityId, 'opp-1');
});
