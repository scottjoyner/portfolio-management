import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';

import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function request(path, method = 'GET', body = null) {
  const stream = new Readable({ read() {} });
  stream.method = method;
  stream.url = path;
  stream.headers = body ? { 'content-type': 'application/json' } : {};
  if (body) stream.push(JSON.stringify(body));
  stream.push(null);
  return stream;
}

async function call(store, path, method, body, now = new Date('2026-07-30T20:00:00.000Z')) {
  const response = await handleRequest(request(path, method, body), {
    store,
    now,
    env: { OPERATOR_AUTH_REQUIRED: 'false', MODE: 'mock' },
  });
  return { ...response, data: JSON.parse(response.body) };
}

test('POST /api/agents/jobs defaults omitted locality and status to local queued', async () => {
  const store = new MemoryOperatorStore(createInitialState('2026-07-30T20:00:00.000Z'));
  const response = await call(store, '/api/agents/jobs', 'POST', {
    model: 'qwen-local',
    promptTokens: 1000,
    completionTokens: 250,
    totalTokens: 1250,
    runtimeSeconds: 30,
    estimatedWatts: 110,
    marketScope: 'BTC-USD',
  });

  assert.equal(response.status, 201);
  assert.equal(response.data.job.localOrRemote, 'local');
  assert.equal(response.data.job.status, 'queued');
  assert.equal(response.data.job.queuedAt, '2026-07-30T20:00:00.000Z');
  assert.equal(response.data.job.startedAt, null);
  assert.equal(response.data.job.completedAt, null);
  assert.equal(response.data.ledger.remoteApiCost, 0);
  assert.ok(response.data.ledger.localComputeCost > 0);
});

test('explicit remote research remains blocked without economic evidence', async () => {
  const store = new MemoryOperatorStore(createInitialState('2026-07-30T20:00:00.000Z'));
  const response = await call(store, '/api/agents/jobs', 'POST', {
    localOrRemote: 'remote',
    model: 'paid-model',
    totalTokens: 1000,
  });

  assert.equal(response.status, 409);
  assert.equal(response.data.error, 'remote_intelligence_purchase_blocked');
  assert.ok(response.data.errors.includes('model_quote_required'));
  assert.ok(response.data.errors.includes('economic_decision_required'));
});
