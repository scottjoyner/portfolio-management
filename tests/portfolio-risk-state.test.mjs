import test from 'node:test';
import assert from 'node:assert/strict';

import {
  accountHasOpenRisk,
  applyPortfolioHighWaterUpdates,
  hydratePortfolioRiskState,
  planPortfolioHighWaterUpdates,
} from '../packages/execution/src/portfolioRiskState.mjs';
import { createInitialOperatorState, normalizeOperatorState } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-09-11T05:10:00.000Z';
const ACCOUNT_ID = 'acct-paper-primary';

test('fresh operator state starts with an explicit high-water mark', () => {
  const state = createInitialOperatorState(NOW);
  assert.equal(state.portfolioRiskState.accounts[0].accountId, ACCOUNT_ID);
  assert.equal(state.portfolioRiskState.accounts[0].highWaterNavUsd, 100000);
  assert.equal(state.portfolioRiskState.accounts[0].highWaterAt, NOW);
});

test('normalizing legacy state does not silently invent a high-water mark', () => {
  const legacy = createInitialOperatorState(NOW);
  delete legacy.portfolioRiskState;
  const normalized = normalizeOperatorState(legacy);
  assert.deepEqual(normalized.portfolioRiskState.accounts, []);
});

test('missing high-water can initialize only when account has no open or pending risk', () => {
  const state = createInitialOperatorState(NOW);
  state.portfolioRiskState.accounts = [];
  state.audit = [];

  const planned = planPortfolioHighWaterUpdates(state, NOW);
  assert.equal(planned.actions.length, 1);
  assert.equal(planned.actions[0].action, 'portfolio_high_water_initialized');
  assert.equal(planned.actions[0].highWaterNavUsd, 100000);

  const applied = applyPortfolioHighWaterUpdates(state, { now: NOW });
  assert.equal(applied.changed, true);
  assert.equal(state.portfolioRiskState.accounts[0].highWaterNavUsd, 100000);
  assert.equal(state.audit.at(-1).action, 'portfolio_high_water_initialized');
});

test('missing high-water stays unavailable when open position risk already exists', () => {
  const state = createInitialOperatorState(NOW);
  state.portfolioRiskState.accounts = [];
  state.audit = [];
  state.positions = [{
    id: 'position-existing',
    accountId: ACCOUNT_ID,
    symbol: 'BTC-USD',
    quantity: 0.1,
    averagePrice: 100000,
    status: 'open',
  }];

  assert.equal(accountHasOpenRisk(state, ACCOUNT_ID), true);
  const planned = planPortfolioHighWaterUpdates(state, NOW);
  assert.deepEqual(planned.actions, []);
  assert.deepEqual(planned.blockedAccounts, [ACCOUNT_ID]);
});

test('unscoped pending risk also prevents unsafe high-water bootstrap', () => {
  const state = createInitialOperatorState(NOW);
  state.portfolioRiskState.accounts = [];
  state.audit = [];
  state.executions = [{ id: 'legacy-unscoped', status: 'draft', orders: [] }];

  const planned = planPortfolioHighWaterUpdates(state, NOW);
  assert.deepEqual(planned.actions, []);
  assert.deepEqual(planned.blockedAccounts, [ACCOUNT_ID]);
});

test('new account NAV peak advances high-water monotonically and appends audit evidence', () => {
  const state = createInitialOperatorState(NOW);
  state.accounts[0].nav = 112000;
  const later = '2026-09-11T05:11:00.000Z';

  const result = applyPortfolioHighWaterUpdates(state, { now: later });
  assert.equal(result.changed, true);
  assert.equal(state.portfolioRiskState.accounts[0].highWaterNavUsd, 112000);
  assert.equal(state.portfolioRiskState.accounts[0].highWaterAt, later);
  assert.equal(state.audit.at(-1).action, 'portfolio_high_water_advanced');
  assert.equal(state.audit.at(-1).payload.previousHighWaterNavUsd, 100000);
  assert.equal(state.audit.at(-1).payload.highWaterNavUsd, 112000);

  state.accounts[0].nav = 105000;
  const noRegression = applyPortfolioHighWaterUpdates(state, { now: '2026-09-11T05:12:00.000Z' });
  assert.equal(noRegression.changed, false);
  assert.equal(state.portfolioRiskState.accounts[0].highWaterNavUsd, 112000);
});

test('high-water can be reconstructed after restart from append-only audit alone', () => {
  const state = createInitialOperatorState(NOW);
  state.accounts[0].nav = 120000;
  applyPortfolioHighWaterUpdates(state, { now: '2026-09-11T05:11:00.000Z' });
  const persistedAudit = structuredClone(state.audit);

  const restarted = normalizeOperatorState({
    ...state,
    portfolioRiskState: undefined,
    audit: persistedAudit,
  });
  assert.deepEqual(restarted.portfolioRiskState.accounts, []);
  const hydrated = hydratePortfolioRiskState(restarted);
  assert.equal(hydrated.accounts[0].accountId, ACCOUNT_ID);
  assert.equal(hydrated.accounts[0].highWaterNavUsd, 120000);
  assert.equal(hydrated.accounts[0].source, 'append_only_audit');
});

test('replaying the same high-water update does not duplicate its deterministic audit event', () => {
  const state = createInitialOperatorState(NOW);
  state.accounts[0].nav = 110000;
  applyPortfolioHighWaterUpdates(state, { now: '2026-09-11T05:11:00.000Z' });
  const auditCount = state.audit.length;
  hydratePortfolioRiskState(state);
  applyPortfolioHighWaterUpdates(state, { now: '2026-09-11T05:11:30.000Z' });
  assert.equal(state.audit.length, auditCount);
});
