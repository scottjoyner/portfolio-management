import test from 'node:test';
import assert from 'node:assert/strict';
import {
  agentBudgetFromRow,
  agentBudgetParams,
  agentCostFromRow,
  agentCostParams,
  budgetApprovalFromRow,
  budgetApprovalParams,
  marketSnapshotFromRow,
  marketSnapshotParams,
  opportunityFromRow,
  opportunityParams,
  researchJobFromRow,
  researchJobParams,
  riskBreakdownFromRow,
  riskBreakdownParams
} from '../packages/storage/src/productRowMappers.mjs';

test('maps market snapshots between app objects and SQL rows', () => {
  const snapshot = {
    id: 'md-1', symbol: 'BTC-USD', venue: 'coinbase-paper', assetClass: 'crypto', bid: 10, ask: 11, spreadBps: 90.9, volume24h: 1000,
    liquidityScore: 80, volatilityScore: 55, status: 'watching', source: 'test', timestamp: '2026-05-30T00:00:00.000Z'
  };
  assert.deepEqual(marketSnapshotParams(snapshot).slice(0, 4), ['md-1', 'BTC-USD', 'coinbase-paper', 'crypto']);
  assert.deepEqual(marketSnapshotFromRow({ id: 'md-1', symbol: 'BTC-USD', venue: 'coinbase-paper', asset_class: 'crypto', bid: '10', ask: '11', spread_bps: '90.9', volume_24h: '1000', liquidity_score: '80', volatility_score: '55', status: 'watching', source: 'test', timestamp: '2026-05-30T00:00:00.000Z' }), snapshot);
});

test('maps agent budgets and budget approvals', () => {
  const budget = { agentId: 'agent-a', dailyTokenLimit: 100, dailyCostLimit: 10, perJobTokenLimit: 50, perMarketCostLimit: 5, requireApprovalAboveCost: 2, enabled: true, updatedAt: '2026-05-30T00:00:00.000Z' };
  assert.deepEqual(agentBudgetParams(budget), ['agent-a', 100, 10, 50, 5, 2, true, '2026-05-30T00:00:00.000Z']);
  assert.deepEqual(agentBudgetFromRow({ agent_id: 'agent-a', daily_token_limit: '100', daily_cost_limit: '10', per_job_token_limit: '50', per_market_cost_limit: '5', require_approval_above_cost: '2', enabled: true, updated_at: '2026-05-30T00:00:00.000Z' }), budget);

  const approval = { id: 'budget-001', agentId: 'agent-a', marketScope: 'ETH-USD', opportunityId: 'opp-1', requestedBy: 'operator', reason: 'deep research', status: 'approved', projectedCost: 12, projectedTokens: 60000, approvedCostLimit: 12, approvedTokenLimit: 60000, reviewer: 'risk', decisionReason: 'ok', requestedAt: '2026-05-30T00:00:00.000Z', reviewedAt: '2026-05-30T00:05:00.000Z', expiresAt: null };
  assert.equal(budgetApprovalParams(approval)[0], 'budget-001');
  assert.equal(budgetApprovalParams(approval)[14], '2026-05-30T00:05:00.000Z');
  assert.deepEqual(budgetApprovalFromRow({ id: 'budget-001', agent_id: 'agent-a', market_scope: 'ETH-USD', opportunity_id: 'opp-1', requested_by: 'operator', reason: 'deep research', status: 'approved', projected_cost: '12', projected_tokens: '60000', approved_cost_limit: '12', approved_token_limit: '60000', reviewer: 'risk', decision_reason: 'ok', requested_at: '2026-05-30T00:00:00.000Z', reviewed_at: '2026-05-30T00:05:00.000Z', expires_at: null }), approval);
});

test('maps research jobs with budget approval linkage', () => {
  const job = { id: 'job-1', agentId: 'agent-a', triggerType: 'operator_request', marketScope: 'ETH-USD', symbolScope: 'ETH-USD', provider: 'local', model: 'qwen', localOrRemote: 'local', status: 'completed', startedAt: '2026-05-30T00:00:00.000Z', completedAt: '2026-05-30T00:01:00.000Z', promptTokens: 100, completionTokens: 50, totalTokens: 150, estimatedRemoteCost: 0, estimatedLocalCost: 0.2, budgetApprovalId: 'budget-001', opportunityIdsCreated: ['opp-1'], failureReason: null };
  assert.equal(researchJobParams(job)[16], 'budget-001');
  assert.deepEqual(researchJobFromRow({ id: 'job-1', agent_id: 'agent-a', trigger_type: 'operator_request', market_scope: 'ETH-USD', symbol_scope: 'ETH-USD', provider: 'local', model: 'qwen', local_or_remote: 'local', status: 'completed', started_at: '2026-05-30T00:00:00.000Z', completed_at: '2026-05-30T00:01:00.000Z', prompt_tokens: '100', completion_tokens: '50', total_tokens: '150', estimated_remote_cost: '0', estimated_local_cost: '0.2', budget_approval_id: 'budget-001', opportunity_ids_json: '["opp-1"]', failure_reason: null }), job);
});

test('maps opportunities with cost and risk fields', () => {
  const opportunity = { id: 'opp-1', sourceAgentId: 'agent-a', researchJobId: 'job-1', strategyId: null, marketType: 'crypto_spot', venue: 'coinbase-paper', symbol: 'ETH-USD', marketSlug: null, title: 'ETH setup', recommendation: 'paper_review', confidenceScore: 0.6, winProbability: 0.55, lossProbability: 0.45, expectedValue: 90, grossExpectedValue: 100, totalMoneyRisked: 1000, maxLoss: 200, potentialUpside: 400, rewardRiskRatio: 2, liquidityScore: 80, dataFreshnessScore: 90, backtestId: null, backtestStatus: 'backtest_missing', riskBreakdownId: 'risk-1', status: 'needs_review', approvalStatus: 'needs_review', estimatedFees: 2, estimatedSlippage: 3, estimatedGas: 0, agentResearchCost: 4, modelInferenceCost: 1, netExpectedValue: 90, notes: 'note', evidence: [{ type: 'test' }], expiresAt: null, reviewedAt: null, reviewer: null, decisionReason: null, createdAt: '2026-05-30T00:00:00.000Z', updatedAt: '2026-05-30T00:01:00.000Z' };
  assert.equal(opportunityParams(opportunity)[0], 'opp-1');
  assert.equal(opportunityParams(opportunity)[32], 90);
  assert.deepEqual(opportunityFromRow({ id: 'opp-1', source_agent_id: 'agent-a', research_job_id: 'job-1', strategy_id: null, market_type: 'crypto_spot', venue: 'coinbase-paper', symbol: 'ETH-USD', market_slug: null, title: 'ETH setup', recommendation: 'paper_review', confidence_score: '0.6', win_probability: '0.55', loss_probability: '0.45', expected_value: '90', gross_expected_value: '100', total_money_risked: '1000', max_loss: '200', potential_upside: '400', reward_risk_ratio: '2', liquidity_score: '80', data_freshness_score: '90', backtest_id: null, backtest_status: 'backtest_missing', risk_breakdown_id: 'risk-1', status: 'needs_review', approval_status: 'needs_review', estimated_fees: '2', estimated_slippage: '3', estimated_gas: '0', agent_research_cost: '4', model_inference_cost: '1', net_expected_value: '90', notes: 'note', evidence_json: '[{"type":"test"}]', expires_at: null, reviewed_at: null, reviewer: null, decision_reason: null, created_at: '2026-05-30T00:00:00.000Z', updated_at: '2026-05-30T00:01:00.000Z' }), opportunity);
});

test('maps risk breakdowns and agent cost rows', () => {
  const risk = { id: 'risk-1', scope: 'opportunity', scopeId: 'opp-1', aggregateScore: 25, capitalAtRiskScore: 40, liquidityScore: 80, slippageScore: 10, drawdownScore: 20, volatilityScore: 55, correlationScore: 35, modelConfidenceScore: 60, dataFreshnessScore: 90, agentCostScore: 5, explanation: 'ok', generatedAt: '2026-05-30T00:00:00.000Z' };
  assert.equal(riskBreakdownParams(risk)[2], 'opp-1');
  assert.deepEqual(riskBreakdownFromRow({ id: 'risk-1', scope: 'opportunity', scope_id: 'opp-1', aggregate_score: '25', capital_at_risk_score: '40', liquidity_score: '80', slippage_score: '10', drawdown_score: '20', volatility_score: '55', correlation_score: '35', model_confidence_score: '60', data_freshness_score: '90', agent_cost_score: '5', explanation: 'ok', generated_at: '2026-05-30T00:00:00.000Z' }), risk);

  const cost = { id: 'cost-1', agentId: 'agent-a', jobId: 'job-1', model: 'qwen', provider: 'local', localOrRemote: 'local', promptTokens: 100, completionTokens: 50, totalTokens: 150, remoteApiCost: 0, localComputeCost: 0.2, allocatedOpportunityId: 'opp-1', createdAt: '2026-05-30T00:00:00.000Z' };
  assert.equal(agentCostParams(cost)[11], 'opp-1');
  assert.deepEqual(agentCostFromRow({ id: 'cost-1', agent_id: 'agent-a', job_id: 'job-1', model: 'qwen', provider: 'local', local_or_remote: 'local', prompt_tokens: '100', completion_tokens: '50', total_tokens: '150', remote_api_cost: '0', local_compute_cost: '0.2', allocated_opportunity_id: 'opp-1', created_at: '2026-05-30T00:00:00.000Z' }), cost);
});
