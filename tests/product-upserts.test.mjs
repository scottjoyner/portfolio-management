import test from 'node:test';
import assert from 'node:assert/strict';
import { PRODUCT_PARAM_BUILDERS, PRODUCT_UPSERT_SQL, upsertProductRecord, upsertProductRecords } from '../packages/storage/src/productUpserts.mjs';

const sampleRecords = {
  marketDataSnapshot: { id: 'md-1', symbol: 'BTC-USD', venue: 'coinbase-paper', assetClass: 'crypto' },
  agentBudget: { agentId: 'agent-a' },
  budgetApproval: { id: 'budget-1', agentId: 'agent-a', projectedCost: 12, projectedTokens: 60000 },
  researchJob: { id: 'job-1', agentId: 'agent-a', budgetApprovalId: 'budget-1' },
  opportunity: { id: 'opp-1', sourceAgentId: 'agent-a', marketType: 'crypto_spot', venue: 'coinbase-paper', title: 'ETH setup' },
  riskBreakdown: { id: 'risk-1', scopeId: 'opp-1' },
  agentCost: { id: 'cost-1', agentId: 'agent-a', jobId: 'job-1' }
};

const expectedParamCounts = {
  marketDataSnapshot: 13,
  agentBudget: 8,
  budgetApproval: 16,
  researchJob: 19,
  opportunity: 40,
  riskBreakdown: 15,
  agentCost: 13
};

test('all product upsert helpers have SQL and parameter builders', () => {
  for (const [type, record] of Object.entries(sampleRecords)) {
    assert.match(PRODUCT_UPSERT_SQL[type], /ON CONFLICT/i);
    assert.equal(PRODUCT_PARAM_BUILDERS[type](record).length, expectedParamCounts[type]);
  }
});

test('upsertProductRecord dispatches SQL and params for one record', async () => {
  const calls = [];
  await upsertProductRecord((sql, params) => {
    calls.push({ sql, params });
    return { rowCount: 1 };
  }, 'budgetApproval', sampleRecords.budgetApproval);
  assert.equal(calls.length, 1);
  assert.match(calls[0].sql, /INSERT INTO budget_approvals/);
  assert.equal(calls[0].params.length, expectedParamCounts.budgetApproval);
});

test('upsertProductRecords dispatches all records in order', async () => {
  const calls = [];
  await upsertProductRecords((sql, params) => {
    calls.push({ sql, params });
    return { rowCount: 1 };
  }, 'agentBudget', [{ agentId: 'a' }, { agentId: 'b' }]);
  assert.deepEqual(calls.map(call => call.params[0]), ['a', 'b']);
});

test('unknown product upsert type is rejected', async () => {
  await assert.rejects(() => upsertProductRecord(() => {}, 'unknown', {}), /unknown_product_record_type/);
});
