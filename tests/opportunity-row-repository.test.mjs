import test from 'node:test';
import assert from 'node:assert/strict';
import { OpportunityRowRepository } from '../packages/storage/src/opportunityRowRepository.mjs';

class FakeStore {
  constructor() {
    this.calls = [];
    this.rowsByTable = {
      budget_approvals: [],
      research_jobs: [],
      agent_cost_ledger: []
    };
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    if (/SELECT \* FROM budget_approvals/.test(sql)) return { rows: this.rowsByTable.budget_approvals };
    if (/SELECT \* FROM research_jobs/.test(sql)) return { rows: this.rowsByTable.research_jobs };
    if (/SELECT \* FROM agent_cost_ledger/.test(sql)) return { rows: this.rowsByTable.agent_cost_ledger };
    return { rows: [] };
  }
}

test('budget approval rows map to domain shape', async () => {
  const store = new FakeStore();
  store.rowsByTable.budget_approvals.push({
    id: 'budget-approval-001',
    agent_id: 'market-research-agent',
    market_scope: 'PREDICTION:DEMO',
    opportunity_id: 'opp-001',
    requested_by: 'operator',
    reason: 'deeper research',
    status: 'approved',
    projected_cost: '12.5',
    projected_tokens: '60000',
    approved_cost_limit: '15',
    approved_token_limit: '70000',
    reviewer: 'risk-manager',
    decision_reason: 'ok',
    requested_at: new Date('2026-05-30T00:00:00Z'),
    reviewed_at: '2026-05-30T01:00:00.000Z',
    expires_at: null
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
    id: 'job-001',
    agent_id: 'market-research-agent',
    trigger_type: 'operator_request',
    market_scope: 'PREDICTION:DEMO',
    symbol_scope: 'PREDICTION:DEMO',
    provider: 'remote',
    model: 'expensive-model',
    local_or_remote: 'remote',
    status: 'completed',
    started_at: '2026-05-30T00:00:00.000Z',
    completed_at: '2026-05-30T00:02:00.000Z',
    prompt_tokens: '1000',
    completion_tokens: '500',
    total_tokens: '1500',
    estimated_remote_cost: '12',
    estimated_local_cost: '0',
    budget_approval_id: 'budget-approval-001',
    opportunity_ids_json: '["opp-001"]',
    failure_reason: null
  });
  const repo = new OpportunityRowRepository(store);
  const rows = await repo.listResearchJobs();
  assert.equal(rows[0].budgetApprovalId, 'budget-approval-001');
  assert.deepEqual(rows[0].opportunityIdsCreated, ['opp-001']);
  assert.equal(rows[0].estimatedRemoteCost, 12);
});

test('upsert methods issue targeted SQL without deleting unrelated tables', async () => {
  const store = new FakeStore();
  const repo = new OpportunityRowRepository(store);
  await repo.upsertBudgetApproval({ id: 'budget-approval-001', agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000, status: 'pending_review' });
  await repo.upsertResearchJob({ id: 'job-001', agentId: 'market-research-agent', marketScope: 'PREDICTION:DEMO', budgetApprovalId: 'budget-approval-001' });
  await repo.upsertAgentCost({ id: 'cost-001', agentId: 'market-research-agent', jobId: 'job-001', model: 'm', provider: 'p' });
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO budget_approvals')));
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO research_jobs')));
  assert.ok(store.calls.some(call => call.sql.includes('INSERT INTO agent_cost_ledger')));
  assert.ok(!store.calls.some(call => /DELETE FROM strategies|DELETE FROM opportunities/.test(call.sql)));
});

test('replaceOpportunityWorkflow only clears opportunity workflow cost tables', async () => {
  const store = new FakeStore();
  const repo = new OpportunityRowRepository(store);
  await repo.replaceOpportunityWorkflow({
    budgetApprovals: [{ id: 'budget-approval-001', agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000 }],
    researchJobs: [{ id: 'job-001', agentId: 'market-research-agent', marketScope: 'PREDICTION:DEMO', budgetApprovalId: 'budget-approval-001' }],
    agentCostLedger: [{ id: 'cost-001', agentId: 'market-research-agent', jobId: 'job-001', model: 'm', provider: 'p' }]
  });
  assert.deepEqual(store.calls.slice(0, 3).map(call => call.sql), ['DELETE FROM agent_cost_ledger', 'DELETE FROM research_jobs', 'DELETE FROM budget_approvals']);
  assert.ok(!store.calls.some(call => /DELETE FROM strategies|DELETE FROM accounts|DELETE FROM opportunities/.test(call.sql)));
});
