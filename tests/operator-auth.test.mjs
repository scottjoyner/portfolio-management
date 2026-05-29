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
  return { ...out, data: JSON.parse(out.body) };
}

test('operator auth allows dev mock mode without token', async () => {
  const out = await call('GET', '/api/accounts');
  assert.equal(out.status, 200);
  assert.equal(out.data.actor, 'dev-operator');
});

test('operator auth blocks protected routes when required and token missing', async () => {
  const out = await call('GET', '/api/accounts', undefined, { OPERATOR_AUTH_REQUIRED: 'true', OPERATOR_AUTH_TOKEN: 'secret' }, { 'x-request-id': 'req-test-auth' });
  assert.equal(out.status, 401);
  assert.equal(out.data.error, 'operator_auth_required');
  assert.equal(out.data.requestId, 'req-test-auth');
});

test('operator auth accepts bearer token when required', async () => {
  const out = await call('GET', '/api/accounts', undefined, { OPERATOR_AUTH_REQUIRED: 'true', OPERATOR_AUTH_TOKEN: 'secret' }, { authorization: 'Bearer secret', 'x-request-id': 'req-ok' });
  assert.equal(out.status, 200);
  assert.equal(out.data.actor, 'operator');
  assert.equal(out.data.requestId, 'req-ok');
});

test('health remains available when auth is required', async () => {
  const out = await call('GET', '/health', undefined, { OPERATOR_AUTH_REQUIRED: 'true', OPERATOR_AUTH_TOKEN: 'secret' });
  assert.equal(out.status, 200);
});
