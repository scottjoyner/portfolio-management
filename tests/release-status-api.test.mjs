import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';
import { buildAuditEvent } from '../packages/storage/src/auditChain.mjs';

function req(method, url, body, headers = {}) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = url;
  stream.headers = headers;
  if (body !== undefined) stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function call(method, url, store, env = {}) {
  const out = await handleRequest(req(method, url), { store, env });
  return { ...out, data: JSON.parse(out.body) };
}

test('audit verify endpoint reports clean empty chain', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const out = await call('GET', '/api/audit/verify', store);
  assert.equal(out.status, 200);
  assert.equal(out.data.audit.ok, true);
  assert.equal(out.data.audit.mode, 'no_chained_events_yet');
});

test('audit verify endpoint detects tampered chain', async () => {
  const state = createInitialState();
  const first = buildAuditEvent({ id: 'audit1', action: 'first', actor: 'operator', at: '2026-05-29T00:00:00.000Z', payload: { ok: true } });
  const second = buildAuditEvent({ id: 'audit2', action: 'second', actor: 'operator', at: '2026-05-29T00:01:00.000Z', payload: { ok: true } }, first);
  state.audit = [first, { ...second, payload: { ok: false } }];
  const store = new MemoryOperatorStore(state);
  const out = await call('GET', '/api/audit/verify', store);
  assert.equal(out.status, 200);
  assert.equal(out.data.audit.ok, false);
  assert.ok(out.data.audit.issues.some(issue => issue.issue === 'event_hash_mismatch'));
});

test('release status endpoint summarizes paper-only certification posture', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const out = await call('GET', '/api/release/status', store, { LIVE_TRADING: 'false' });
  assert.equal(out.status, 200);
  assert.equal(out.data.status.release, 'first-prod-paper-only');
  assert.equal(out.data.status.liveTradingCertified, false);
  assert.equal(out.data.status.capabilities.paperTrading, true);
  assert.equal(out.data.status.capabilities.liveOrderSubmission, false);
});

test('release status endpoint reports blocker when live flag is enabled', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const out = await call('GET', '/api/release/status', store, { LIVE_TRADING: 'true' });
  assert.equal(out.status, 200);
  assert.equal(out.data.status.ok, false);
  assert.ok(out.data.status.blockers.includes('live_trading_flag_enabled'));
});
