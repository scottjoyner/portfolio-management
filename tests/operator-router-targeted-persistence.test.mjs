import test from 'node:test';
import assert from 'node:assert/strict';
import { createInitialOperatorState, normalizeOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { handleOperatorRoute } from '../apps/api/src/operatorRouter.mjs';

function jsonBody(body) {
  return async () => body;
}

class TargetedStore {
  constructor() {
    this.state = normalizeOperatorState(createInitialOperatorState());
    this.persisted = { budgetApprovals: [], researchJobs: [], costs: [] };
  }

  async mutate(mutator) {
    return mutator(this.state);
  }

  async upsertBudgetApproval(approval) {
    this.persisted.budgetApprovals.push(approval);
  }

  async upsertResearchJob(job) {
    this.persisted.researchJobs.push(job);
  }

  async upsertAgentCost(cost) {
    this.persisted.costs.push(cost);
  }
}

test('budget approval route persists through targeted row helper when available', async () => {
  const store = new TargetedStore();
  const route = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/agents/budget-approvals',
    state: store.state,
    store,
    readJsonBody: jsonBody({
      agentId: 'market-research-agent',
      marketScope: 'PREDICTION:DEMO',
      projectedCost: 12,
      projectedTokens: 60000,
      requestedBy: 'test',
      reason: 'targeted persistence test'
    })
  });
  assert.equal(route.status, 201);
  assert.equal(store.persisted.budgetApprovals.length, 1);
  assert.equal(store.persisted.budgetApprovals[0].id, route.body.budgetApproval.id);
  assert.equal(store.persisted.budgetApprovals[0].marketScope, 'PREDICTION:DEMO');
});

test('research job route persists job and cost ledger through targeted row helpers', async () => {
  const store = new TargetedStore();
  const route = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/agents/jobs',
    state: store.state,
    store,
    readJsonBody: jsonBody({
      agentId: 'market-research-agent',
      model: 'local-review',
      localOrRemote: 'local',
      totalTokens: 1500,
      runtimeSeconds: 30,
      systemBudgetOverride: true,
      marketScope: 'PREDICTION:DEMO'
    })
  });
  assert.equal(route.status, 201);
  assert.equal(store.persisted.researchJobs.length, 1);
  assert.equal(store.persisted.costs.length, 1);
  assert.equal(store.persisted.researchJobs[0].id, route.body.job.id);
  assert.equal(store.persisted.costs[0].jobId, route.body.job.id);
});

test('budget approval decision persists the updated approval row', async () => {
  const store = new TargetedStore();
  const requested = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/agents/budget-approvals',
    state: store.state,
    store,
    readJsonBody: jsonBody({ agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000 })
  });
  const decided = await handleOperatorRoute({
    method: 'POST',
    pathname: `/api/agents/budget-approvals/${requested.body.budgetApproval.id}/decision`,
    state: store.state,
    store,
    readJsonBody: jsonBody({ status: 'approved', reviewer: 'test', approvedCostLimit: 12, approvedTokenLimit: 60000 })
  });
  assert.equal(decided.status, 200);
  assert.equal(store.persisted.budgetApprovals.at(-1).status, 'approved');
  assert.equal(store.persisted.budgetApprovals.at(-1).approvedTokenLimit, 60000);
});

test('targeted persistence hook is optional for memory/file-style stores', async () => {
  const state = normalizeOperatorState(createInitialOperatorState());
  const store = { async mutate(mutator) { return mutator(state); } };
  const route = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/agents/jobs',
    state,
    store,
    readJsonBody: jsonBody({ agentId: 'market-research-agent', localOrRemote: 'local', totalTokens: 1500, systemBudgetOverride: true })
  });
  assert.equal(route.status, 201);
  assert.equal(route.body.ok, true);
  assert.ok(route.body.job.id);
});
