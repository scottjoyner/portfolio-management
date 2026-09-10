import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildCapitalRiskSnapshot,
  evaluateCapitalRiskSnapshot,
} from '../packages/execution/src/capitalRiskSnapshot.mjs';
import { buildTradeIntentEnvelope, stableHash } from '../packages/execution/src/overseer.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-09-10T22:30:00.000Z';

function stateFixture() {
  const state = createInitialOperatorState(NOW);
  state.killSwitch = { enabled: false, reason: 'hardening_test_ready', updatedAt: NOW };
  state.marketDataSnapshots = [{
    id: 'md-hardening-btc',
    symbol: 'BTC-USD',
    venue: 'coinbase-paper',
    bid: 99990,
    ask: 100010,
    status: 'connected',
    source: 'hardening-test',
    timestamp: NOW,
  }];
  return state;
}

function order(overrides = {}) {
  return {
    id: 'ord-hardening-1',
    marketId: 'BTC-USD',
    symbol: 'BTC-USD',
    venue: 'coinbase-paper',
    side: 'buy',
    quantity: 0.05,
    price: 100000,
    orderType: 'market',
    timeInForce: 'GTC',
    confidenceScore: 0.9,
    ...overrides,
  };
}

function envelope(overrides = {}) {
  return buildTradeIntentEnvelope({
    strategyId: 'strategy-hardening',
    accountId: 'acct-paper-primary',
    mode: 'paper',
    venue: 'coinbase-paper',
    symbol: 'BTC-USD',
    side: 'buy',
    confidenceScore: 0.9,
    entryPrice: 100000,
    takeProfitPrice: 105000,
    stopLossPrice: 97000,
    orders: [order()],
    ...overrides,
  });
}

function evaluate(state, intent = envelope(), options = {}) {
  const tradeIntentHash = stableHash(intent);
  const snapshot = buildCapitalRiskSnapshot({
    state,
    source: {
      source: 'hardening-test-state',
      revision: 'hardening-r1',
      observedAt: NOW,
    },
    tradeIntentEnvelope: intent,
    tradeIntentHash,
    now: NOW,
    ...options,
  });
  return {
    snapshot,
    decision: evaluateCapitalRiskSnapshot(snapshot, { tradeIntentHash, now: NOW }),
  };
}

test('full multi-order notional is counted instead of the first order quantity only', () => {
  const state = stateFixture();
  state.config.maxPositionSizeUsd = 15000;
  const intent = envelope({
    orders: [
      order({ id: 'ord-hardening-a', quantity: 0.08 }),
      order({ id: 'ord-hardening-b', quantity: 0.08 }),
    ],
  });
  const { snapshot, decision } = evaluate(state, intent);

  assert.equal(snapshot.orderBundle.quantity, 0.16);
  assert.equal(snapshot.limits.orderNotionalUsd, 16000);
  assert.equal(snapshot.limits.projectedExposureUsd, 16000);
  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('notional_limit_exceeded'));
});

test('mixed-symbol order bundles fail closed', () => {
  const state = stateFixture();
  const intent = envelope({
    orders: [
      order({ id: 'ord-btc' }),
      order({ id: 'ord-eth', marketId: 'ETH-USD', symbol: 'ETH-USD' }),
    ],
  });
  const { decision } = evaluate(state, intent);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:order_bundle_mixed_symbol'));
});

test('mixed-side order bundles fail closed', () => {
  const state = stateFixture();
  const intent = envelope({
    orders: [order({ id: 'ord-buy' }), order({ id: 'ord-sell', side: 'sell' })],
  });
  const { decision } = evaluate(state, intent);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:order_bundle_mixed_side'));
});

test('mixed-venue order bundles fail closed', () => {
  const state = stateFixture();
  const intent = envelope({
    orders: [order({ id: 'ord-cb' }), order({ id: 'ord-other', venue: 'kraken-paper' })],
  });
  const { decision } = evaluate(state, intent);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:order_bundle_mixed_venue'));
});

test('disconnected accounts cannot authorize execution', () => {
  const state = stateFixture();
  state.accounts[0].status = 'disconnected';
  const { decision } = evaluate(state);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:account_not_connected'));
});

test('non-USD cash accounts fail closed until conversion authority exists', () => {
  const state = stateFixture();
  state.accounts[0].currency = 'EUR';
  const { decision } = evaluate(state);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:account_currency_not_usd'));
});

test('disconnected market snapshots cannot satisfy the freshness gate', () => {
  const state = stateFixture();
  state.marketDataSnapshots[0].status = 'disconnected';
  const { decision } = evaluate(state);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:market_data_not_ready'));
});

test('instrument venue must match the execution venue', () => {
  const state = stateFixture();
  state.marketDataSnapshots[0].venue = 'kraken-paper';
  const intent = envelope({
    venue: 'kraken-paper',
    orders: [order({ venue: 'kraken-paper' })],
  });
  const { decision } = evaluate(state, intent);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:instrument_venue_mismatch'));
  assert.ok(decision.reasons.includes('pair_not_approved'));
});

test('same-account pending cash is reserved even when it belongs to another symbol', () => {
  const state = stateFixture();
  state.executions.push({
    id: 'pending-eth-cash',
    accountId: 'acct-paper-primary',
    symbol: 'ETH-USD',
    side: 'buy',
    status: 'draft',
    notional: 97000,
    orders: [{ symbol: 'ETH-USD', side: 'buy', quantity: 1, price: 97000 }],
    fills: [],
  });
  const intent = envelope({ orders: [order({ quantity: 0.05 })] });
  const { snapshot, decision } = evaluate(state, intent);

  assert.equal(snapshot.pendingExecutionState.pendingBuyExposureUsd, 97000);
  assert.equal(snapshot.accountState.availableCashAfterPendingUsd, 3000);
  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('insufficient_balance'));
});

test('sell intent cannot pass without sufficient account-owned inventory', () => {
  const state = stateFixture();
  const intent = envelope({
    side: 'sell',
    orders: [order({ side: 'sell', quantity: 0.05 })],
  });
  const { decision } = evaluate(state, intent);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('insufficient_balance'));
});

test('economic forecast expiry constrains the capital-risk snapshot lifetime', () => {
  const state = stateFixture();
  const forecastExpiry = '2026-09-10T22:30:10.000Z';
  state.priceForecasts.push({
    id: 'forecast-hardening',
    status: 'valid',
    symbol: 'BTC-USD',
    expiresAt: forecastExpiry,
    asOf: NOW,
  });
  state.executionCostSnapshots.push({
    id: 'cost-hardening',
    symbol: 'BTC-USD',
    validUntil: '2026-09-10T22:30:20.000Z',
    createdAt: NOW,
  });
  state.economicDecisions.push({
    id: 'decision-hardening',
    symbol: 'BTC-USD',
    forecastId: 'forecast-hardening',
    executionCostSnapshotId: 'cost-hardening',
    modelQuoteId: null,
    executionAllowed: true,
    netExecutableEdgeUsd: 25,
    createdAt: NOW,
  });
  const intent = envelope({
    sourceAgentId: 'agent-hardening',
    economicDecisionId: 'decision-hardening',
    forecastId: 'forecast-hardening',
    executionCostSnapshotId: 'cost-hardening',
    netExecutableEdgeUsd: 25,
  });
  const { snapshot, decision } = evaluate(state, intent);

  assert.equal(decision.approved, true);
  assert.equal(snapshot.validUntil, forecastExpiry);
});

test('economic decision must be regenerated after model-cost reconciliation', () => {
  const state = stateFixture();
  state.priceForecasts.push({
    id: 'forecast-reconcile',
    status: 'valid',
    symbol: 'BTC-USD',
    expiresAt: '2026-09-10T22:35:00.000Z',
    asOf: NOW,
  });
  state.executionCostSnapshots.push({
    id: 'cost-reconcile',
    symbol: 'BTC-USD',
    validUntil: '2026-09-10T22:35:00.000Z',
    createdAt: NOW,
  });
  state.modelUsageLedger.push({
    id: 'quote-reconcile',
    status: 'reconciled',
    requestedAt: '2026-09-10T22:29:00.000Z',
    reconciledAt: '2026-09-10T22:29:50.000Z',
  });
  state.economicDecisions.push({
    id: 'decision-reconcile',
    symbol: 'BTC-USD',
    forecastId: 'forecast-reconcile',
    executionCostSnapshotId: 'cost-reconcile',
    modelQuoteId: 'quote-reconcile',
    executionAllowed: true,
    netExecutableEdgeUsd: 25,
    createdAt: '2026-09-10T22:29:40.000Z',
  });
  const intent = envelope({
    sourceAgentId: 'agent-hardening',
    economicDecisionId: 'decision-reconcile',
    forecastId: 'forecast-reconcile',
    executionCostSnapshotId: 'cost-reconcile',
    modelQuoteId: 'quote-reconcile',
    netExecutableEdgeUsd: 25,
  });
  const { decision } = evaluate(state, intent);

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_invalid:edge_lineage:economic_decision_requires_post_reconciliation_refresh'));
});
