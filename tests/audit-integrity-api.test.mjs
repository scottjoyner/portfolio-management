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
  return { ...out, data: out.body ? JSON.parse(out.body) : null };
}

function chain(events) {
  let previous = null;
  return events.map(event => {
    const built = buildAuditEvent(event, previous);
    previous = built;
    return built;
  });
}

test('audit verify endpoint returns valid chain result', async () => {
  const state = createInitialState();
  state.audit = chain(state.audit);
  const store = new MemoryOperatorStore(state);
  const out = await call('GET', '/api/audit/verify', store);
  assert.equal(out.status, 200);
  assert.equal(out.data.ok, true);
  assert.equal(out.data.reason, 'audit_chain_valid');
});

test('audit verify endpoint detects missing hashes', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  // Seed normalization intentionally completes legacy hashes. Tamper the loaded
  // store afterward so this test verifies an actually broken runtime chain.
  store.state.audit = [{
    id: 'audit-unhashed-001',
    action: 'test_unhashed_event',
    actor: 'test',
    at: '2026-05-29T00:00:00.000Z',
    details: 'explicit unhashed evidence',
  }];
  const out = await call('GET', '/api/audit/verify', store);
  assert.equal(out.status, 409);
  assert.equal(out.data.ok, false);
  assert.equal(out.data.reason, 'audit_hashes_missing');
  assert.deepEqual(out.data.missing, ['audit-unhashed-001']);
});

test('production-paper readiness fails closed for non-postgres store', async () => {
  const state = createInitialState();
  state.audit = chain(state.audit);
  const store = new MemoryOperatorStore(state);
  const out = await call('GET', '/ready/production-paper', store, {
    DEPLOYMENT_ENV: 'production',
    OPERATOR_STORE: 'postgres',
    DATABASE_URL: 'postgresql://portfolio:strong-hosted-password@db.example.internal:5432/portfolio',
    OPERATOR_AUTH_REQUIRED: 'true',
    OPERATOR_ADMIN_TOKEN: 'admin-token',
    CSRF_REQUIRED: 'true',
    OPERATOR_CSRF_TOKEN: 'csrf-token',
    CORS_ORIGINS: 'https://operator.example.com',
    LIVE_TRADING: 'false',
    ALLOW_POLYMARKET_ORDER_SUBMISSION: 'false',
    ALLOW_LIVE_SETTLEMENT_REDEMPTION: 'false'
  });
  assert.equal(out.status, 503);
  assert.equal(out.data.productionPaperReady, false);
  assert.ok(out.data.blockers.includes('postgres_storage_required'));
});
