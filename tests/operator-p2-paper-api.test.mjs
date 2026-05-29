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
  const out = await handleRequest(req(method, url, body), { store });
  return { ...out, data: JSON.parse(out.body) };
}

test('paper signal route previews, fills, updates position, and reconciles', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const state = await store.load();
  state.approvals[0].status = 'approved';
  state.paperExecutions.push({ id: 'paper-api', strategyId: state.strategies[0].id, accountId: state.accounts[0].id, status: 'running', mode: 'paper', fills: [] });
  await store.save(state);

  const out = await json('POST', '/api/paper-executions/paper-api/signal', {
    signal: { symbol: 'BTC-USD', side: 'buy', quantity: 0.1, price: 50000, feeBps: 5, slippageBps: 10 }
  }, store);

  assert.equal(out.status, 200);
  assert.equal(out.data.fill.status, 'filled');
  assert.equal(out.data.reconciliation.status, 'ok');
  const next = await store.load();
  assert.equal(next.paperExecutions[0].fills.length, 1);
  assert.ok(next.positions.some(position => position.symbol === 'BTC-USD' && position.quantity > 0));
  assert.ok(next.audit.some(event => event.action === 'paper_signal_filled'));
});

test('paper signal route blocks stopped sessions', async () => {
  const store = new MemoryOperatorStore(createInitialState());
  const state = await store.load();
  state.paperExecutions.push({ id: 'paper-stopped-api', strategyId: state.strategies[0].id, accountId: state.accounts[0].id, status: 'stopped', mode: 'paper', fills: [] });
  await store.save(state);
  const out = await json('POST', '/api/paper-executions/paper-stopped-api/signal', { signal: { symbol: 'BTC-USD', side: 'buy', quantity: 1, price: 100 } }, store);
  assert.equal(out.status, 400);
  assert.ok(out.data.errors.includes('paper_execution_not_running'));
});
