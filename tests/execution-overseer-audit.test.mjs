import test from 'node:test';
import assert from 'node:assert/strict';

import { handleOperatorRoute } from '../apps/api/src/operatorRouter.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

const INTENT_HASH = 'a'.repeat(64);

function authorizedExecution(id, status) {
  return {
    id,
    strategyId: 'strategy-overseer-test',
    accountId: 'acct-paper-primary',
    symbol: 'BTC-USD',
    venue: 'paper',
    mode: 'paper',
    side: 'buy',
    status,
    version: 1,
    quantity: 0.01,
    notional: 1000,
    entryPrice: 100000,
    orders: [{
      id: 'order-overseer-test',
      marketId: 'BTC-USD',
      symbol: 'BTC-USD',
      venue: 'paper',
      side: 'buy',
      quantity: 0.01,
      price: 100000,
    }],
    fills: [],
    tradeIntentHash: INTENT_HASH,
    overseerDecision: { decision: 'PAPER', approved: true },
  };
}

function targetedStore() {
  const calls = [];
  return {
    calls,
    async persistExecutionMutation(input) {
      calls.push(input);
      return { auditEvent: input.auditEvent, auditIdempotent: false };
    },
    async mutate() {
      throw new Error('broad_mutate_must_not_run');
    },
  };
}

test('targeted execution submit persists overseer decision and intent hash in audit payload', async () => {
  const state = createInitialOperatorState('2026-09-10T19:00:00.000Z');
  const store = targetedStore();
  handleOperatorRoute._execEngine = {
    async execute() {
      return { ok: true, execution: authorizedExecution('exec-overseer-submit', 'draft') };
    },
  };

  const response = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/execution/execute',
    state,
    store,
    readJsonBody: async () => ({
      symbol: 'BTC-USD',
      side: 'buy',
      quantity: 0.01,
      entryPrice: 100000,
      takeProfitPrice: 105000,
      stopLossPrice: 97000,
      riskDecision: { approved: true, reasons: [] },
    }),
  });

  assert.equal(response.status, 200);
  assert.equal(store.calls.length, 1);
  const payload = store.calls[0].auditEvent.payload;
  assert.equal(payload.overseerDecision, 'PAPER');
  assert.equal(payload.tradeIntentHash, INTENT_HASH);
});

test('targeted operator approval persists the refreshed overseer lineage', async () => {
  const state = createInitialOperatorState('2026-09-10T19:00:00.000Z');
  const store = targetedStore();
  handleOperatorRoute._execEngine = {
    async approve(id) {
      return { ok: true, execution: authorizedExecution(id, 'filled') };
    },
  };

  const response = await handleOperatorRoute({
    method: 'POST',
    pathname: '/api/execution/exec-overseer-approve/approve',
    state,
    store,
    readJsonBody: async () => ({}),
  });

  assert.equal(response.status, 200);
  assert.equal(store.calls.length, 1);
  const payload = store.calls[0].auditEvent.payload;
  assert.equal(payload.overseerDecision, 'PAPER');
  assert.equal(payload.tradeIntentHash, INTENT_HASH);
});
