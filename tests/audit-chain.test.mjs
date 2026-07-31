import test from 'node:test';
import assert from 'node:assert/strict';
import {
  buildAuditEvent,
  completeAuditChain,
  verifyAuditChain,
} from '../packages/storage/src/auditChain.mjs';
import { OperatorRowRepository } from '../packages/storage/src/operatorRowRepository.mjs';
import {
  createInitialOperatorState,
  MemoryOperatorStore,
} from '../packages/storage/src/operatorStore.mjs';

class AuditStore {
  constructor() { this.rows = []; }
  async query(sql, params = []) {
    if (sql.startsWith('SELECT * FROM audit_events')) return { rows: this.rows.slice(-1) };
    if (sql.startsWith('INSERT INTO audit_events')) {
      const [id, action, actor, at, details, payload, previousHash, eventHash, sequenceNumber] = params;
      this.rows.push({ id, action, actor, at, details, payload_json: JSON.parse(payload), previous_hash: previousHash, event_hash: eventHash, sequence_number: sequenceNumber });
      return { rows: [] };
    }
    return { rows: [] };
  }
}

test('audit chain builds sequential hashes', () => {
  const first = buildAuditEvent({ id: 'a1', action: 'created', actor: 'test', at: '2026-05-29T00:00:00.000Z', payload: { x: 1 } });
  const second = buildAuditEvent({ id: 'a2', action: 'updated', actor: 'test', at: '2026-05-29T00:01:00.000Z', payload: { x: 2 } }, first);
  assert.equal(first.sequenceNumber, 1);
  assert.equal(second.sequenceNumber, 2);
  assert.equal(second.previousHash, first.eventHash);
  assert.equal(verifyAuditChain([first, second]).ok, true);
});

test('audit chain detects tampering', () => {
  const first = buildAuditEvent({ id: 'a1', action: 'created', actor: 'test', at: '2026-05-29T00:00:00.000Z', payload: { x: 1 } });
  const second = buildAuditEvent({ id: 'a2', action: 'updated', actor: 'test', at: '2026-05-29T00:01:00.000Z', payload: { x: 2 } }, first);
  const tampered = { ...second, payload: { x: 99 } };
  const result = verifyAuditChain([first, tampered]);
  assert.equal(result.ok, false);
  assert.ok(result.issues.some(issue => issue.issue === 'event_hash_mismatch'));
});

test('legacy plain audit rows are completed deterministically and idempotently', () => {
  const plain = [
    { id: 'legacy-1', action: 'created', actor: 'test', at: '2026-05-29T00:00:00.000Z', payload: { x: 1 } },
    { id: 'legacy-2', action: 'updated', actor: 'test', at: '2026-05-29T00:01:00.000Z', payload: { x: 2 } },
  ];
  const completed = completeAuditChain(plain);
  const repeated = completeAuditChain(completed);
  assert.equal(completed[0].sequenceNumber, 1);
  assert.equal(completed[1].sequenceNumber, 2);
  assert.equal(completed[1].previousHash, completed[0].eventHash);
  assert.deepEqual(repeated, completed);
  assert.equal(verifyAuditChain(completed).ok, true);
});

test('audit completion rejects conflicting persisted metadata', () => {
  const first = buildAuditEvent({ id: 'a1', action: 'created', actor: 'test', at: '2026-05-29T00:00:00.000Z' });
  const second = buildAuditEvent({ id: 'a2', action: 'updated', actor: 'test', at: '2026-05-29T00:01:00.000Z' }, first);
  assert.throws(
    () => completeAuditChain([first, { ...second, eventHash: 'tampered' }]),
    /audit_chain_event_hash_conflict:a2/,
  );
  assert.throws(
    () => completeAuditChain([first, { ...second, previousHash: 'wrong' }]),
    /audit_chain_previous_hash_conflict:a2/,
  );
});

test('operator-store normalization hashes newly appended audit events', async () => {
  const store = new MemoryOperatorStore(createInitialOperatorState());
  await store.mutate(state => {
    state.audit.push({
      id: 'audit-memory-1',
      action: 'memory_mutation',
      actor: 'test',
      at: '2026-05-29T00:00:00.000Z',
      payload: { ok: true },
    });
  });
  const state = await store.load();
  assert.equal(state.audit[0].sequenceNumber, 1);
  assert.match(state.audit[0].eventHash, /^[a-f0-9]{64}$/);
  assert.equal(verifyAuditChain(state.audit).ok, true);
});

test('row repository insertAudit appends hash chain fields', async () => {
  const store = new AuditStore();
  const repo = new OperatorRowRepository(store);
  const first = await repo.insertAudit({ id: 'audit1', action: 'first', actor: 'operator', at: '2026-05-29T00:00:00.000Z' });
  const second = await repo.insertAudit({ id: 'audit2', action: 'second', actor: 'operator', at: '2026-05-29T00:01:00.000Z' });
  assert.equal(first.sequenceNumber, 1);
  assert.equal(second.sequenceNumber, 2);
  assert.equal(second.previousHash, first.eventHash);
  assert.equal(store.rows[1].previous_hash, first.eventHash);
});
