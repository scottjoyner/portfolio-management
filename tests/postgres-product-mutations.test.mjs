import test from 'node:test';
import assert from 'node:assert/strict';
import {
  attachProductMutations,
  upsertAgentCost,
  upsertBudgetApproval,
  upsertMarketDataSnapshots,
  upsertProductBundle,
  upsertProductRecord,
  upsertProducts
} from '../packages/storage/src/postgresProductMutations.mjs';

function fakeStore({ failOnSql = null, migrations = { ok: true } } = {}) {
  const calls = [];
  return {
    calls,
    migrations,
    async checkMigrations() {
      calls.push({ sql: 'CHECK_MIGRATIONS', params: [] });
      return migrations;
    },
    async query(sql, params = []) {
      calls.push({ sql, params });
      if (failOnSql && String(sql).includes(failOnSql)) throw new Error(`forced failure on ${failOnSql}`);
      return { rowCount: 1, rows: [] };
    }
  };
}

function sqlCalls(store) {
  return store.calls.map(call => call.sql);
}

test('upsertProductRecord wraps a single targeted write in a transaction', async () => {
  const store = fakeStore();
  await upsertProductRecord(store, 'budgetApproval', {
    id: 'budget-001',
    agentId: 'market-research-agent',
    projectedCost: 12,
    projectedTokens: 60000
  });

  const sql = sqlCalls(store);
  assert.equal(sql[0], 'CHECK_MIGRATIONS');
  assert.equal(sql[1], 'BEGIN');
  assert.match(sql[2], /INSERT INTO budget_approvals/);
  assert.match(sql[2], /ON CONFLICT/);
  assert.equal(sql.at(-1), 'COMMIT');
  assert.ok(!sql.some(statement => /DELETE FROM/i.test(statement)), 'targeted upsert must not delete product tables');
});

test('upsertProducts writes each record in one transaction', async () => {
  const store = fakeStore();
  await upsertProducts(store, 'marketDataSnapshot', [
    { id: 'md-1', symbol: 'BTC-USD', venue: 'coinbase-paper', assetClass: 'crypto' },
    { id: 'md-2', symbol: 'ETH-USD', venue: 'coinbase-paper', assetClass: 'crypto' }
  ]);

  const inserts = store.calls.filter(call => /INSERT INTO market_data_snapshots/.test(call.sql));
  assert.equal(inserts.length, 2);
  assert.equal(sqlCalls(store).filter(sql => sql === 'BEGIN').length, 1);
  assert.equal(sqlCalls(store).filter(sql => sql === 'COMMIT').length, 1);
});

test('upsertProductBundle writes multiple product tables in one transaction', async () => {
  const store = fakeStore();
  await upsertProductBundle(store, {
    marketDataSnapshot: [{ id: 'md-1', symbol: 'BTC-USD', venue: 'coinbase-paper', assetClass: 'crypto' }],
    budgetApproval: [{ id: 'budget-001', agentId: 'market-research-agent', projectedCost: 12, projectedTokens: 60000 }],
    researchJob: [{ id: 'job-001', agentId: 'market-research-agent', budgetApprovalId: 'budget-001' }],
    agentCost: [{ id: 'cost-001', agentId: 'market-research-agent', jobId: 'job-001' }]
  });

  const sql = sqlCalls(store).join('\n');
  assert.match(sql, /INSERT INTO market_data_snapshots/);
  assert.match(sql, /INSERT INTO budget_approvals/);
  assert.match(sql, /INSERT INTO research_jobs/);
  assert.match(sql, /INSERT INTO agent_cost_ledger/);
  assert.equal(sqlCalls(store).filter(statement => statement === 'BEGIN').length, 1);
  assert.equal(sqlCalls(store).filter(statement => statement === 'COMMIT').length, 1);
});

test('targeted mutations roll back on failure', async () => {
  const store = fakeStore({ failOnSql: 'agent_cost_ledger' });
  await assert.rejects(() => upsertAgentCost(store, { id: 'cost-001', agentId: 'agent-a', jobId: 'job-001' }), /forced failure/);
  const sql = sqlCalls(store);
  assert.equal(sql.at(-1), 'ROLLBACK');
  assert.ok(!sql.includes('COMMIT'));
});

test('targeted mutations reject missing migrations before opening transaction', async () => {
  const store = fakeStore({ migrations: { ok: false, reason: 'opportunity_agent_tables_missing' } });
  await assert.rejects(() => upsertBudgetApproval(store, { id: 'budget-001', agentId: 'agent-a', projectedCost: 1, projectedTokens: 10 }), /postgres_migrations_not_ready/);
  assert.ok(!sqlCalls(store).includes('BEGIN'));
});

test('attachProductMutations installs ergonomic methods on a store', async () => {
  const store = attachProductMutations(fakeStore());
  assert.equal(typeof store.upsertBudgetApproval, 'function');
  assert.equal(typeof store.upsertProductBundle, 'function');
  await store.upsertBudgetApproval({ id: 'budget-001', agentId: 'agent-a', projectedCost: 1, projectedTokens: 10 });
  assert.match(sqlCalls(store).join('\n'), /INSERT INTO budget_approvals/);
});

test('specific helper aliases dispatch to the correct tables', async () => {
  const store = fakeStore();
  await upsertMarketDataSnapshots(store, [{ id: 'md-1', symbol: 'SOL-USD', venue: 'coinbase-paper', assetClass: 'crypto' }]);
  assert.match(sqlCalls(store).join('\n'), /INSERT INTO market_data_snapshots/);
});
