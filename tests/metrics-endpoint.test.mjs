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

test('metrics endpoint returns prometheus text and records requests', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  await handleRequest(req('GET', '/api/accounts'), { store });
  const out = await handleRequest(req('GET', '/metrics'), { store });
  assert.equal(out.status, 200);
  assert.equal(out.headers['content-type'], 'text/plain; version=0.0.4; charset=utf-8');
  assert.ok(out.body.includes('portfolio_requests_total'));
  assert.ok(out.body.includes('portfolio_responses_total'));
  assert.ok(out.body.includes('portfolio_uptime_seconds'));
});
