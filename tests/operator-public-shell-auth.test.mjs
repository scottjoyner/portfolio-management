import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';

import { createInitialState, handleRequest } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function req(method, url, headers = {}) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = url;
  stream.headers = headers;
  stream.push(null);
  return stream;
}

const env = {
  OPERATOR_AUTH_REQUIRED: 'true',
  OPERATOR_AUTH_TOKEN: 'operator-secret',
};

test('production auth permits only the static operator shell before login', async () => {
  const store = new MemoryOperatorStore(createInitialState());

  const page = await handleRequest(req('GET', '/'), { store, env });
  assert.equal(page.status, 200);
  assert.match(page.headers['content-type'], /text\/html/);
  assert.match(page.body, /\/ui\/operator-session\.js/);

  const sessionAsset = await handleRequest(req('GET', '/ui/operator-session.js'), { store, env });
  assert.equal(sessionAsset.status, 200);
  assert.match(sessionAsset.body, /sessionStorage/);

  const protectedApi = await handleRequest(req('GET', '/api/accounts'), { store, env });
  assert.equal(protectedApi.status, 401);
  assert.equal(JSON.parse(protectedApi.body).error, 'operator_auth_required');

  const authenticatedApi = await handleRequest(req('GET', '/api/accounts', {
    authorization: 'Bearer operator-secret',
  }), { store, env });
  assert.equal(authenticatedApi.status, 200);
});
