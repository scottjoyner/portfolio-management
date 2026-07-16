import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function req(method, url, body) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = url;
  stream.headers = {};
  if (body !== undefined) stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function json(method, url, body, store) {
  const out = await handleRequest(req(method, url, body), { store, env: { OPERATOR_AUTH_REQUIRED: 'false', MODE: 'mock' } });
  return { ...out, data: JSON.parse(out.body) };
}

test('GET /api/secrets masks values and reports freshness', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const state = await store.load();
  state.config.coinbaseApiKey = 'org-supersecrettoken';
  state.config.secretMeta = { coinbaseApiKey: { updatedAt: new Date(Date.now() - 40 * 86_400_000).toISOString() } };
  state.config.secretRotationDays = 30;
  await store.save(state);

  const out = await json('GET', '/api/secrets', undefined, store);
  assert.equal(out.status, 200);
  const secret = out.data.secrets.secrets.find(s => s.key === 'coinbaseApiKey');
  assert.equal(secret.set, true);
  assert.ok(secret.masked.startsWith('org-') && !secret.masked.includes('supersecret'));
  assert.equal(secret.freshness.state, 'expired');
});

test('POST /api/secrets/rotate/:provider rotates and refreshes freshness', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const state = await store.load();
  state.config.coinbaseApiKey = 'org-OLDKEY123';
  state.config.secretMeta = { coinbaseApiKey: { updatedAt: new Date(Date.now() - 40 * 86_400_000).toISOString() } };
  await store.save(state);

  const out = await json('POST', '/api/secrets/rotate/coinbase', undefined, store);
  assert.equal(out.status, 200);
  assert.deepEqual(out.data.rotatedFields.sort(), ['coinbaseApiKey', 'coinbaseApiSecret']);

  const next = await store.load();
  assert.notEqual(next.config.coinbaseApiKey, 'org-OLDKEY123');
  assert.ok(next.config.secretMeta.coinbaseApiKey.updatedAt);
  const view = await json('GET', '/api/secrets', undefined, store);
  const secret = view.data.secrets.secrets.find(s => s.key === 'coinbaseApiKey');
  assert.equal(secret.freshness.state, 'fresh');
  assert.ok(next.audit.some(e => e.action === 'secret_rotated'));
});

test('PUT /api/secrets validates and updates credentials', async () => {
  const store = new MemoryOperatorStore(createInitialState());

  const bad = await json('PUT', '/api/secrets', { coinbaseApiKey: 'short' }, store);
  assert.equal(bad.status, 400);
  assert.ok(bad.data.errors.includes('coinbaseApiKey_too_short'));

  const good = await json('PUT', '/api/secrets', { kalshiPassword: 'BrandNewPass123!' }, store);
  assert.equal(good.status, 200);
  const next = await store.load();
  assert.equal(next.config.kalshiPassword, 'BrandNewPass123!');
  assert.ok(next.config.secretMeta.kalshiPassword.updatedAt);
  assert.ok(next.audit.some(e => e.action === 'secrets_updated'));
});

test('auto-rotate config + run rotates only due providers', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const state = await store.load();
  state.config.coinbaseApiKey = 'org-OLD';
  state.config.secretMeta = { coinbaseApiKey: { updatedAt: new Date(Date.now() - 40 * 86_400_000).toISOString() } };
  state.config.secretRotationDays = 30;
  await store.save(state);

  const cfg = await json('POST', '/api/secrets/auto-rotate/config', { enabled: true, rotationDays: 30, intervalDays: 7 }, store);
  assert.equal(cfg.status, 200);
  assert.equal(cfg.data.config.autoRotateSecrets, true);

  const run = await json('POST', '/api/secrets/auto-rotate/run', undefined, store);
  assert.equal(run.status, 200);
  assert.ok(run.data.rotated.some(r => r.provider === 'coinbase'));
});
