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

async function call(method, url, body) {
  const store = new MemoryOperatorStore(createInitialState());
  const out = await handleRequest(req(method, url, body), { store });
  return { ...out, data: out.headers['content-type']?.includes('json') ? JSON.parse(out.body) : out.body };
}

test('json metrics route remains available for existing API consumers', async () => {
  const out = await call('GET', '/metrics');
  assert.equal(out.status, 200);
  assert.equal(typeof out.data.strategies_total, 'number');
  assert.equal(out.headers['content-type'].includes('application/json'), true);
});

test('prometheus metrics are exposed on metrics.prom', async () => {
  const out = await call('GET', '/metrics.prom');
  assert.equal(out.status, 200);
  assert.equal(out.headers['content-type'].includes('text/plain'), true);
  assert.match(out.body, /portfolio_requests_total/);
  assert.match(out.body, /portfolio_uptime_seconds/);
});
