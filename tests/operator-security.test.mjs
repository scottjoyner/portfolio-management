import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';
import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function req(method, url, body, headers = {}) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = url;
  stream.headers = headers;
  if (body !== undefined) stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function call(method, url, body, env = {}, headers = {}) {
  const store = new MemoryOperatorStore(createInitialState());
  const out = await handleRequest(req(method, url, body, headers), { store, env });
  return { ...out, data: out.body ? JSON.parse(out.body) : null };
}

test('security headers are attached to operator responses', async () => {
  const out = await call('GET', '/api/accounts');
  assert.equal(out.status, 200);
  assert.equal(out.headers['x-content-type-options'], 'nosniff');
  assert.equal(out.headers['x-frame-options'], 'DENY');
  assert.ok(out.headers['content-security-policy'].includes("default-src 'self'"));
});

test('cors only allows configured origins', async () => {
  const allowed = await call('GET', '/api/accounts', undefined, { CORS_ORIGINS: 'https://operator.example.com' }, { origin: 'https://operator.example.com' });
  assert.equal(allowed.headers['access-control-allow-origin'], 'https://operator.example.com');
  const blocked = await call('GET', '/api/accounts', undefined, { CORS_ORIGINS: 'https://operator.example.com' }, { origin: 'https://evil.example.com' });
  assert.equal(blocked.headers['access-control-allow-origin'], undefined);
});

test('options preflight returns cors headers for allowed origin', async () => {
  const out = await call('OPTIONS', '/api/accounts', undefined, { CORS_ORIGINS: 'https://operator.example.com' }, { origin: 'https://operator.example.com' });
  assert.equal(out.status, 204);
  assert.equal(out.headers['access-control-allow-origin'], 'https://operator.example.com');
});

test('csrf blocks mutating requests when required', async () => {
  const out = await call('POST', '/api/strategies/from-template', { templateId: 'template-ema-crossover' }, { CSRF_REQUIRED: 'true', OPERATOR_CSRF_TOKEN: 'csrf' });
  assert.equal(out.status, 403);
  assert.equal(out.data.error, 'csrf_required');
});

test('csrf allows mutating requests with valid token', async () => {
  const out = await call('POST', '/api/strategies/from-template', { templateId: 'template-ema-crossover' }, { CSRF_REQUIRED: 'true', OPERATOR_CSRF_TOKEN: 'csrf' }, { 'x-csrf-token': 'csrf' });
  assert.equal(out.status, 201);
  assert.equal(out.data.ok, true);
});
