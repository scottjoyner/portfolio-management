import test from 'node:test';
import assert from 'node:assert/strict';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { PostgresOperatorStoreP2 } from '../packages/storage/src/postgresOperatorStoreP2.mjs';

class EconomicFlagClient {
  constructor(rows = []) {
    this.rows = rows;
    this.calls = [];
  }

  async query(sql, params = []) {
    this.calls.push({ sql, params });
    if (sql.includes("WHERE key IN ('executions', 'economic_state')")) return { rows: this.rows };
    return { rows: [] };
  }
}

test('Postgres P2 reads execution and economic operator documents', async () => {
  const client = new EconomicFlagClient([
    { key: 'executions', value_json: [{ id: 'exec-1', status: 'filled' }] },
    {
      key: 'economic_state',
      value_json: {
        schemaVersion: 5,
        modelUsageLedger: [{ id: 'quote-1', status: 'reconciled', actualCostUsd: 0.02 }],
        economicDecisions: [{ id: 'decision-1', executionAllowed: true }],
      },
    },
  ]);
  const store = new PostgresOperatorStoreP2({ client, bootstrap: false });
  const documents = await store.loadOperatorDocuments();
  assert.equal(documents.executions[0].id, 'exec-1');
  assert.equal(documents.economic.schemaVersion, 5);
  assert.equal(documents.economic.modelUsageLedger[0].actualCostUsd, 0.02);
});

test('Postgres P2 writes a versioned economic document with linkage overlays', async () => {
  const client = new EconomicFlagClient();
  const store = new PostgresOperatorStoreP2({ client, bootstrap: false });
  const state = createInitialOperatorState('2026-07-29T22:00:00.000Z');
  state.modelPricingSnapshots.push({ id: 'pricing-1', provider: 'openrouter' });
  state.modelUsageLedger.push({ id: 'quote-1', status: 'reconciled', actualCostUsd: 0.02 });
  state.priceForecasts.push({ id: 'forecast-1', symbol: 'BTC-USD' });
  state.executionCostSnapshots.push({ id: 'cost-snapshot-1', symbol: 'BTC-USD' });
  state.economicDecisions.push({ id: 'decision-1', executionAllowed: true });
  state.economicAttributionQueue.push({ id: 'pending-1', executionId: 'exec-1' });
  state.researchJobs.push({ id: 'job-1', modelQuoteId: 'quote-1', economicDecisionId: 'decision-1' });
  state.opportunities.push({ id: 'opp-1', economicDecisionId: 'decision-1' });
  state.agentCostLedger.push({ id: 'ledger-1', jobId: 'job-1', modelQuoteId: 'quote-1' });

  await store.saveEconomicDocument(state);
  const insert = client.calls.find(call => call.sql.includes("INSERT INTO operator_flags"));
  assert.ok(insert);
  assert.equal(insert.params[0], 'economic_state');
  const payload = JSON.parse(insert.params[1]);
  assert.equal(payload.schemaVersion, 5);
  assert.equal(payload.modelUsageLedger[0].id, 'quote-1');
  assert.equal(payload.economicDecisions[0].id, 'decision-1');
  assert.equal(payload.researchJobs[0].modelQuoteId, 'quote-1');
  assert.equal(payload.economicAttributionQueue[0].executionId, 'exec-1');
});
