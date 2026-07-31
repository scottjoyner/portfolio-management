import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';

import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function get(path) {
  const request = new Readable({ read() {} });
  request.method = 'GET';
  request.url = path;
  request.headers = {};
  request.push(null);
  return request;
}

async function fetchAsset(path) {
  return handleRequest(get(path), {
    store: new MemoryOperatorStore(createInitialState()),
    env: { OPERATOR_AUTH_REQUIRED: 'false', MODE: 'mock' },
  });
}

test('operator session wrapper loads before the console starts API requests', async () => {
  const page = await fetchAsset('/');
  assert.equal(page.status, 200);
  const sessionIndex = page.body.indexOf('/ui/operator-session.js');
  const appIndex = page.body.indexOf('/ui/app.js');
  const policyIndex = page.body.indexOf('/ui/intelligence-policy.js');
  assert.ok(sessionIndex >= 0);
  assert.ok(appIndex > sessionIndex);
  assert.ok(policyIndex > appIndex);
});

test('operator session asset restricts credentials to same-origin API requests', async () => {
  const asset = await fetchAsset('/ui/operator-session.js');
  assert.equal(asset.status, 200);
  assert.match(asset.body, /url\.origin === window\.location\.origin/);
  assert.match(asset.body, /pathname\.startsWith\('\/api\/'\)/);
  assert.match(asset.body, /authorization/);
  assert.match(asset.body, /x-csrf-token/);
  assert.match(asset.body, /sessionStorage/);
  assert.match(asset.body, /never written into portfolio state/);
});
