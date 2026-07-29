import test from 'node:test';
import assert from 'node:assert/strict';
import { Readable } from 'node:stream';

import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function req(url) {
  const stream = new Readable({ read() {} });
  stream.method = 'GET';
  stream.url = url;
  stream.headers = {};
  stream.push(null);
  return stream;
}

test('positions endpoint returns source-labelled operator positions without inventing marks', async () => {
  const state = createInitialState();
  state.positions = [{
    symbol: 'BTC-USD',
    venue: 'coinbase',
    quantity: 0.25,
    averagePrice: 0,
    markPrice: 0,
    unrealizedPnl: 0,
    status: 'open',
  }];
  state.capitalInPlayUsd = 2500;

  const out = await handleRequest(req('/api/positions'), {
    store: new MemoryOperatorStore(state),
    env: { OPERATOR_AUTH_REQUIRED: 'false', MODE: 'mock' },
  });
  const body = JSON.parse(out.body);

  assert.equal(out.status, 200);
  assert.equal(body.ok, true);
  assert.equal(body.source, 'operator_store');
  assert.equal(body.positions.length, 1);
  assert.equal(body.positions[0].symbol, 'BTC-USD');
  assert.equal(body.positions[0].markPrice, 0);
  assert.equal(body.capitalInPlayUsd, 2500);
});
