import test from 'node:test';
import assert from 'node:assert/strict';
import { OperatorRowRepository } from '../packages/storage/src/operatorRowRepository.mjs';

class RecordingStore {
  constructor() { this.calls = []; }
  async query(sql, params = []) {
    this.calls.push({ sql, params });
    return { rows: [] };
  }
}

test('row repository upserts strategy with parameter JSON', async () => {
  const store = new RecordingStore();
  const repo = new OperatorRowRepository(store);
  await repo.upsertStrategy({ id: 's1', name: 'Strategy', version: 2, status: 'draft', riskLevel: 'low', parameters: { symbol: 'BTC-USD' }, createdAt: '2026-05-29T00:00:00.000Z', updatedAt: '2026-05-29T00:00:00.000Z' });
  assert.equal(store.calls.length, 1);
  assert.ok(store.calls[0].sql.includes('ON CONFLICT (id) DO UPDATE'));
  assert.equal(store.calls[0].params[0], 's1');
  assert.equal(store.calls[0].params[5], JSON.stringify({ symbol: 'BTC-USD' }));
});

test('row repository upserts paper execution with fills JSON', async () => {
  const store = new RecordingStore();
  const repo = new OperatorRowRepository(store);
  await repo.upsertPaperExecution({ id: 'paper1', strategyId: 's1', accountId: 'acct', status: 'running', mode: 'paper', startedAt: '2026-05-29T00:00:00.000Z', fills: [{ id: 'fill1' }] });
  assert.equal(store.calls.length, 1);
  assert.ok(store.calls[0].sql.includes('paper_executions'));
  assert.equal(store.calls[0].params[0], 'paper1');
  assert.equal(store.calls[0].params[9], JSON.stringify([{ id: 'fill1' }]));
});

test('row repository inserts audit events idempotently', async () => {
  const store = new RecordingStore();
  const repo = new OperatorRowRepository(store);
  await repo.insertAudit({ id: 'audit1', action: 'test', actor: 'operator', at: '2026-05-29T00:00:00.000Z', details: 'details', payload: { ok: true } });
  assert.ok(store.calls[0].sql.includes('ON CONFLICT (id) DO NOTHING'));
  assert.equal(store.calls[0].params[5], JSON.stringify({ ok: true }));
});
