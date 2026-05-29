import test from 'node:test';
import assert from 'node:assert/strict';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import { applyPaperFill, executePaperSignal, fillPaperOrder, previewPaperOrder } from '../packages/execution/src/paperEngine.mjs';

test('paper order preview rejects insufficient cash', () => {
  const state = createInitialOperatorState();
  const strategy = state.strategies[0];
  const account = { ...state.accounts[0], cash: 1 };
  const result = previewPaperOrder({ strategy, account, signal: { symbol: 'BTC-USD', side: 'buy', quantity: 1, price: 100000 } });
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('insufficient_paper_cash'));
});

test('paper fill updates account and opens position', () => {
  const state = createInitialOperatorState();
  const execution = { id: 'paper-test', strategyId: state.strategies[0].id, accountId: state.accounts[0].id, status: 'running', fills: [] };
  state.paperExecutions.push(execution);
  const preview = previewPaperOrder({ strategy: state.strategies[0], account: state.accounts[0], signal: { symbol: 'BTC-USD', side: 'buy', quantity: 2, price: 100 } }).preview;
  const fill = fillPaperOrder(preview, '2026-05-29T00:00:00.000Z');
  const applied = applyPaperFill(state, execution, fill);
  assert.equal(applied.ok, true);
  assert.equal(state.positions.length, 1);
  assert.equal(state.positions[0].quantity, 2);
  assert.equal(execution.reconciliation.status, 'ok');
  assert.ok(state.accounts[0].cash < 100000);
});

test('execute paper signal requires running execution and records fill', () => {
  const state = createInitialOperatorState();
  state.approvals[0].status = 'approved';
  state.paperExecutions.push({ id: 'paper-running', strategyId: state.strategies[0].id, accountId: state.accounts[0].id, status: 'running', fills: [] });
  const result = executePaperSignal(state, 'paper-running', { symbol: 'ETH-USD', side: 'buy', quantity: 1, price: 2500 });
  assert.equal(result.ok, true);
  assert.equal(result.fill.status, 'filled');
  assert.equal(state.paperExecutions[0].fills.length, 1);
  assert.equal(result.reconciliation.status, 'ok');
});

test('execute paper signal blocks stopped execution', () => {
  const state = createInitialOperatorState();
  state.paperExecutions.push({ id: 'paper-stopped', strategyId: state.strategies[0].id, accountId: state.accounts[0].id, status: 'stopped', fills: [] });
  const result = executePaperSignal(state, 'paper-stopped', { symbol: 'ETH-USD', side: 'buy', quantity: 1, price: 2500 });
  assert.equal(result.ok, false);
  assert.ok(result.errors.includes('paper_execution_not_running'));
});
