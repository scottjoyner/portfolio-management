import test from 'node:test';
import assert from 'node:assert/strict';
import { OpportunityRowRepository } from '../packages/storage/src/opportunityRowRepository.mjs';

class FakeStore {
  constructor() {
    this.calls = [];
    this.rowsByTable = {
      budget_approvals: [],
      research_jobs: [],
      agent_cost_ledger: [],
      opportunities: [],
      risk_breakdowns: []
    };
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    if (/SELECT \* FROM budget_approvals/.test(sql)) return { rows: this.rowsByTable.budget_approvals };
    if (/SELECT \* FROM research_jobs/.test(sql)) return { rows: this.rowsByTable.research_jobs };
    if (/SELECT \* FROM agent_cost_ledger/.test(sql)) return { rows: this.rowsByTable.agent_cost_ledger };
    if (/SELECT \* FROM opportunities/.test(sql)) return { rows: this.rowsByTable.opportunities };
    if (/SELECT \* FROM risk_breakdowns/.test(sql)) return { rows: this.rowsByTable.risk_breakdowns };
    return { rows: [] };
  }
}

test('budget approval rows map to domain shape', async () => {
  const store = new FakeStore();
  store.rowsByTable.budget_approvals.push({
    id: 'budget-approval-001', agent_id: 'market-research-agent', market_scope: 'PREDICTION:DEMO', opportunity_id: 'opp-001', requested_by: 'operator', reason: 'deeper research', status: 'approved', projected_cost: '12.5', projected_tokens: '60000', approved_cost_limit: '15', approved_token_limit: '70000', reviewer: 'risk-manager', decision_reason: 'ok', requested_at: new Date('2026-05-30T00:00:00Z'), reviewed_at: '2026-05-30T01:00:00.000Z', expires_at: null
  });
  const repo = new OpportunityRowRepository(store);
  const rows = await repo.listBudgetApprovals();
  assert.equal(rows[0].id, 'budget-approval-001');
  assert.equal(rows[0].agentId, 'market-research-agent');
  assert.equal(rows[0].marketScope, 'PREDICTION:DEMO');
  assert.equal(rows[0].projectedCost, 12.5);
  assert.equal(rows[0].approvedTokenLimit, 70000);
  assert.equal(rows[0].requestedAt, '2026-05-30T00:00:00.000Z');
});

test('research job rows include budgetApprovalId', async () => {
  const store = new FakeStore();
  store.rowsByTable.research_jobs.push({
    id: 'job-001', agent_id: 'market-research-agent', trigger_type: 'operator_request', market_scope: 'PREDICTION:DEMO', symbol_scope: 'PREDICTION:DEMO', provider: 'remote', model: 'expensive-model', local_or_remote: 'remote', status: 'completed', started_at: '2026-05-30T00:00:00.000Z', completed_at: '2026-05-30T00:02:00.000Z', prompt_tokens: '1000', completion_tokens: '500', total_tokens: '1500', estimated_remote_cost: '12', estimated_local_cost: '0', budget_approval_id: 'budget-approval-001', opportunity_ids_json: '["opp-001"]', failure_reason: null
  });
  const repo = new OpportunityRowRepository(store);
  const rows = await repo.listResearchJobs();
  assert.equal(rows[0].budgetApprovalId, 'budget-approval-001');
  assert.deepEqual(rows[0].opportunityIdsCreated, ['opp-001']);
  assert.equal(rows[0].estimatedRemoteCost, 12);
});

test('opportunity and risk rows map to domain shape', async () => {
  const store = new FakeStore();
  store.rowsByTable.opportunities.push({
    id: 'opp-001', source_agent_id: 'market-research-agent', research_job_id: 'job-001', strategy_id: null, market_type: 'prediction_market', venue: 'polymarket-watch', symbol: 'PREDICTION:DEMO', market_slug: 'prediction-demo', title: 'Prediction demo', recommendation: 'review_yes', confidence_score: '0.68', win_probability: '0.57', loss_probability: '0.43', expected_value: '68.4', gross_expected_value: '68.4', total_money_risked: '500', max_loss: '500', potential_upside: '420', reward_risk_ratio: '0', liquidity_score: '71', data_freshness_score: '86', backtest_id: null, backtest_status: 'backtest_missing', risk_breakdown_id: 'risk-001', status: 'needs_review', approval_status: 'needs_review', estimated_fees: '5', estimated_slippage: '10', estimated_gas: '0', agent_research_cost: '9.35', model_inference_cost: '2.9', net_expected_value: '41.15', notes: 'mapped row', evidence_json: '[{"type":"market_snapshot"}]', expires_at: null, reviewed_at: null, reviewer: null, decision_reason: null, created_at: '2026-05-30T00:00:00.000Z', updated_at: '2026-05-30T00:01:00.000Z'
  });
  store.rowsByTable.risk_breakdowns.push({
    id: 'risk-001', scope: 'opportunity', scope_id: 'opp-001', aggregate_score: '44', capital_at_risk_score: '20', liquidity_score: '71', slippage_score: '50', drawdown_score: '50', volatility_score: '50', correlation_score: '35', model_confidence_score: '68', data_freshness_score: '86', agent_cost_score: '37', explanation: 'risk explanation', generated_at: '2026-05-30T00:01:00.000Z'
  });
  const repo = new OpportunityRowRepository(store);
  const opportunities = await repo.listOpportunities();
  const risks = await repo.listRiskBreakdowns();
  assert.equal(opportunities[0].riskBreakdownId, 'risk-001');
  assert.equal(opportunities[0].netExpectedValue, 41.15);
  assert.deepEqual(opportunities[0].evidence, [{ type: 'market_snapshot' }]);
  assert.equal(risks[0].scopeId, 'opp-001');
  assert.equal(risks[0].aggregateScore, 44);
});

test('upsert methods issue targeted SQL without deleting unrelated tables', async () => {
  const store = new FakeStore();
  const repo = new OpportunityRowRepository(store);
  await repo.upsertBudgetApproval({ id: 'budget-approval-001', agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000, status: 'pending_review' });
  await repo.upsertResearchJob({ id: 'job-001', agentId: 'market-research-agent', marketScope: 'PREDICTION:DEMO', budgetApprovalId: 'budget-approval-001' });
  await repo.upsertOpportunity({ id: 'opp-001', sourceAgentId: 'market-research-agent', marketType: 'prediction_market', venue: 'polymarket-watch', title: 'Demo', riskBreakdownId: 'risk-001' });
  await repo.upsertRiskBreakdown({ id: 'risk-001', scope: 'opportunity', scopeId: 'opp-001' });
  await repo.upsertAgentCost({ id: 'cost-001', agentId: 'market-research-agent', jobId: 'job-001', model: 'm', provider: 'p' });
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO budget_approvals')));
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO research_jobs')));
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO opportunities')));
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO risk_breakdowns')));
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO agent_cost_ledger')));
  assert.ok(!store.calls.some(call => /DELETE FROM strategies|DELETE FROM accounts/.test(call.sql)));
});

test('replaceOpportunityWorkflow only clears opportunity workflow tables', async () => {
  const store = new FakeStore();
  const repo = new OpportunityRowRepository(store);
  await repo.replaceOpportunityWorkflow({
    budgetApprovals: [{ id: 'budget-approval-001', agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000 }],
    researchJobs: [{ id: 'job-001', agentId: 'market-research-agent', marketScope: 'PREDICTION:DEMO', budgetApprovalId: 'budget-approval-001' }],
    opportunities: [{ id: 'opp-001', sourceAgentId: 'market-research-agent', marketType: 'prediction_market', venue: 'polymarket-watch', title: 'Demo' }],
    riskBreakdowns: [{ id: 'risk-001', scope: 'opportunity', scopeId: 'opp-001' }],
    agentCostLedger: [{ id: 'cost-001', agentId: 'market-research-agent', jobId: 'job-001', model: 'm', provider: 'p' }]
  });
  assert.deepEqual(store.calls.slice(0, 5).map(call => call.sql), ['DELETE FROM agent_cost_ledger', 'DELETE FROM risk_breakdowns', 'DELETE FROM opportunities', 'DELETE FROM research_jobs', 'DELETE FROM budget_approvals']);
  assert.ok(!store.calls.some(call => /DELETE FROM strategies|DELETE FROM accounts/.test(call.sql)));
});
