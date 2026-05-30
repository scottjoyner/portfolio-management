import test from 'node:test';
import assert from 'node:assert/strict';
import { persistRouteArtifacts } from '../apps/api/src/operatorRouter.mjs';

function fakeStore(methods = {}) {
  const calls = [];
  const store = { calls };
  for (const [name, impl] of Object.entries(methods)) {
    store[name] = async value => {
      calls.push({ method: name, value });
      if (impl) return impl(value);
      return value;
    };
  }
  return store;
}

test('persistRouteArtifacts uses bundle upsert when available', async () => {
  const store = fakeStore({ upsertOpportunityBundle: null });
  await persistRouteArtifacts(store, {
    snapshots: [{ id: 'md-1' }],
    budgetApproval: { id: 'budget-1' },
    job: { id: 'job-1' },
    ledger: { id: 'cost-1' },
    opportunity: { id: 'opp-1' },
    riskBreakdown: { id: 'risk-1' }
  });

  assert.equal(store.calls.length, 1);
  assert.equal(store.calls[0].method, 'upsertOpportunityBundle');
  assert.deepEqual(Object.keys(store.calls[0].value), ['marketDataSnapshots', 'budgetApprovals', 'researchJobs', 'opportunities', 'riskBreakdowns', 'agentCostLedger']);
  assert.deepEqual(store.calls[0].value.opportunities.map(row => row.id), ['opp-1']);
});

test('persistRouteArtifacts falls back to individual targeted methods', async () => {
  const store = fakeStore({
    upsertMarketDataSnapshots: null,
    upsertBudgetApproval: null,
    upsertResearchJob: null,
    upsertAgentCost: null,
    upsertOpportunity: null,
    upsertRiskBreakdown: null
  });
  await persistRouteArtifacts(store, {
    snapshots: [{ id: 'md-1' }],
    budgetApproval: { id: 'budget-1' },
    job: { id: 'job-1' },
    ledger: { id: 'cost-1' },
    opportunity: { id: 'opp-1' },
    riskBreakdown: { id: 'risk-1' },
    jobs: [{ id: 'job-2' }],
    ledgers: [{ id: 'cost-2' }],
    opportunities: [{ id: 'opp-2' }],
    riskBreakdowns: [{ id: 'risk-2' }]
  });

  assert.deepEqual(store.calls.map(call => call.method), [
    'upsertMarketDataSnapshots',
    'upsertBudgetApproval',
    'upsertResearchJob',
    'upsertAgentCost',
    'upsertOpportunity',
    'upsertRiskBreakdown',
    'upsertResearchJob',
    'upsertAgentCost',
    'upsertOpportunity',
    'upsertRiskBreakdown'
  ]);
});

test('persistRouteArtifacts is a no-op for validation errors and unsupported stores', async () => {
  const store = fakeStore({ upsertOpportunityBundle: null });
  await persistRouteArtifacts(store, { errors: ['bad_input'], opportunity: { id: 'opp-1' } });
  assert.equal(store.calls.length, 0);

  const memoryLikeStore = { calls: [] };
  await persistRouteArtifacts(memoryLikeStore, { opportunity: { id: 'opp-1' } });
  assert.deepEqual(memoryLikeStore.calls, []);
});

test('persistRouteArtifacts does not call bundle upsert for empty results', async () => {
  const store = fakeStore({ upsertOpportunityBundle: null });
  await persistRouteArtifacts(store, { ok: true });
  assert.equal(store.calls.length, 0);
});
