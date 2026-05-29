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

async function call(method, url, body, token, extraEnv = {}) {
  const store = new MemoryOperatorStore(createInitialState());
  const headers = token ? { authorization: `Bearer ${token}` } : {};
  const out = await handleRequest(req(method, url, body, headers), {
    store,
    env: {
      OPERATOR_AUTH_REQUIRED: 'true',
      OPERATOR_ADMIN_TOKEN: 'admin-token',
      OPERATOR_PAPER_TOKEN: 'paper-token',
      OPERATOR_READONLY_TOKEN: 'readonly-token',
      ...extraEnv
    }
  });
  return { ...out, data: out.body ? JSON.parse(out.body) : null, store };
}

test('readonly role can read operator resources', async () => {
  const out = await call('GET', '/api/accounts', undefined, 'readonly-token');
  assert.equal(out.status, 200);
  assert.equal(out.data.role, 'readonly');
});

test('readonly role cannot mutate strategies', async () => {
  const out = await call('POST', '/api/strategies/from-template', { templateId: 'template-ema-crossover' }, 'readonly-token');
  assert.equal(out.status, 403);
  assert.equal(out.data.error, 'operator_role_forbidden');
});

test('paper role can run paper-scoped actions', async () => {
  const out = await call('POST', '/api/backtests/run', { strategyId: 'strategy-ema-cross-v1' }, 'paper-token');
  assert.equal(out.status, 201);
  assert.equal(out.data.role, 'paper');
  assert.equal(out.data.backtest.status, 'completed');
});

test('paper role cannot change strategy lifecycle', async () => {
  const out = await call('POST', '/api/strategies/strategy-ema-cross-v1/status', { status: 'archived' }, 'paper-token');
  assert.equal(out.status, 403);
  assert.equal(out.data.error, 'operator_role_forbidden');
});

test('admin role can mutate strategy lifecycle', async () => {
  const out = await call('POST', '/api/strategies/strategy-ema-cross-v1/status', { status: 'archived' }, 'admin-token');
  assert.equal(out.status, 200);
  assert.equal(out.data.role, 'admin');
  assert.equal(out.data.strategy.status, 'archived');
});
