import test from 'node:test';
import assert from 'node:assert/strict';

import {
  buildCapitalRiskSnapshot,
  evaluateCapitalRiskSnapshot,
  verifyCapitalRiskSnapshot,
} from '../packages/execution/src/capitalRiskSnapshot.mjs';
import { buildTradeIntentEnvelope, stableHash } from '../packages/execution/src/overseer.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

const NOW = '2026-09-10T20:30:00.000Z';

function goodState(now = NOW) {
  const state = createInitialOperatorState(now);
  state.killSwitch = { enabled: false, reason: 'operator_ready', updatedAt: now };
  state.marketDataSnapshots = [{
    id: 'md-btc-risk-test',
    symbol: 'BTC-USD',
    venue: 'coinbase',
    bid: 99990,
    ask: 100010,
    status: 'connected',
    source: 'risk-test',
    timestamp: now,
  }];
  return state;
}

function intent(overrides = {}) {
  return buildTradeIntentEnvelope({
    strategyId: 'strategy-risk-test',
    accountId: 'acct-paper-primary',
    mode: 'paper',
    venue: 'coinbase',
    symbol: 'BTC-USD',
    side: 'buy',
    quantity: 0.1,
    confidenceScore: 0.9,
    entryPrice: 100000,
    takeProfitPrice: 105000,
    stopLossPrice: 97000,
    orders: [{
      id: 'order-risk-test',
      marketId: 'BTC-USD',
      symbol: 'BTC-USD',
      venue: 'coinbase',
      side: 'buy',
      quantity: 0.1,
      price: 100000,
      orderType: 'market',
      timeInForce: 'GTC',
      confidenceScore: 0.9,
    }],
    ...overrides,
  });
}

function snapshotFor(state = goodState(), intentEnvelope = intent(), sourceOverrides = {}, options = {}) {
  const tradeIntentHash = stableHash(intentEnvelope);
  return buildCapitalRiskSnapshot({
    state,
    source: {
      source: 'test_operator_state',
      revision: 'risk-test-revision-1',
      observedAt: NOW,
      ...sourceOverrides,
    },
    tradeIntentEnvelope: intentEnvelope,
    tradeIntentHash,
    now: NOW,
    ...options,
  });
}

test('healthy authoritative paper state produces an approved canonical risk decision', () => {
  const envelope = intent();
  const snapshot = snapshotFor(goodState(), envelope);
  const decision = evaluateCapitalRiskSnapshot(snapshot, { tradeIntentHash: stableHash(envelope), now: NOW });

  assert.equal(decision.approved, true);
  assert.deepEqual(decision.reasons, []);
  assert.equal(snapshot.snapshotHash.length, 64);
  assert.equal(snapshot.tradeIntentHash, stableHash(envelope));
  assert.equal(snapshot.accountState.found, true);
  assert.equal(snapshot.killSwitchState.enabled, false);
  assert.equal(snapshot.marketDataState.fresh, true);
  assert.equal(snapshot.riskInputs.balanceSufficient, true);
  assert.equal(snapshot.edgeState.required, false);
});

test('null kill-switch timestamp is unavailable rather than Unix epoch', () => {
  const state = goodState();
  state.killSwitch.updatedAt = null;
  const decision = evaluateCapitalRiskSnapshot(snapshotFor(state), { now: NOW });

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_missing:kill_switch_state'));
});

test('stale authoritative provider observation fails closed', () => {
  const snapshot = snapshotFor(goodState(), intent(), { observedAt: '2026-09-10T20:28:00.000Z' });
  const decision = evaluateCapitalRiskSnapshot(snapshot, { now: NOW });

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_stale:authoritative_state'));
});

test('stale market data fails closed and preserves risk-engine reason', () => {
  const state = goodState();
  state.marketDataSnapshots[0].timestamp = '2026-09-10T20:20:00.000Z';
  const decision = evaluateCapitalRiskSnapshot(snapshotFor(state), { now: NOW });

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_stale:market_data'));
  assert.ok(decision.reasons.includes('orderbook_stale'));
});

test('insufficient cash is derived from the account rather than caller risk data', () => {
  const state = goodState();
  state.accounts[0].cash = 100;
  const decision = evaluateCapitalRiskSnapshot(snapshotFor(state), { now: NOW });

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('insufficient_balance'));
});

test('enabled kill switch rejects even when every other source is healthy', () => {
  const state = goodState();
  state.killSwitch = { enabled: true, reason: 'operator_stop', updatedAt: NOW };
  const decision = evaluateCapitalRiskSnapshot(snapshotFor(state), { now: NOW });

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('kill_switch_on'));
});

test('pending exposure is included in the projected notional ceiling', () => {
  const state = goodState();
  state.config.maxPositionSizeUsd = 15000;
  state.executions.push({
    id: 'pending-existing',
    accountId: 'acct-paper-primary',
    symbol: 'BTC-USD',
    side: 'buy',
    status: 'draft',
    notional: 6000,
    orders: [{ symbol: 'BTC-USD', side: 'buy', quantity: 0.06, price: 100000 }],
    fills: [],
  });
  const snapshot = snapshotFor(state);
  const decision = evaluateCapitalRiskSnapshot(snapshot, { now: NOW });

  assert.equal(snapshot.pendingExecutionState.pendingExposureUsd, 6000);
  assert.equal(snapshot.limits.projectedExposureUsd, 16000);
  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('notional_limit_exceeded'));
});

test('unresolved reconciliation audit evidence blocks admission', () => {
  const state = goodState();
  state.executions.push({
    id: 'filled-with-recon-issue',
    accountId: 'acct-paper-primary',
    symbol: 'BTC-USD',
    status: 'filled',
    orders: [{ venue: 'coinbase', marketId: 'BTC-USD', quantity: 0.01 }],
    fills: [{ venue: 'coinbase', marketId: 'BTC-USD', quantity: 0.01, settlementStatus: 'settled' }],
  });
  state.audit.push({
    id: 'audit-recon-issue',
    action: 'execution_recon_issues',
    actor: 'reconciliation-engine',
    at: NOW,
    details: 'filled-with-recon-issue',
  });
  const decision = evaluateCapitalRiskSnapshot(snapshotFor(state), { now: NOW });

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('unresolved_reconciliation_discrepancy'));
});

test('a later clean reconciliation clears an earlier explicit issue', () => {
  const state = goodState();
  state.executions.push({
    id: 'filled-reconciled',
    accountId: 'acct-paper-primary',
    symbol: 'BTC-USD',
    status: 'filled',
    orders: [{ venue: 'coinbase', marketId: 'BTC-USD', quantity: 0.01 }],
    fills: [{ venue: 'coinbase', marketId: 'BTC-USD', quantity: 0.01, settlementStatus: 'settled' }],
  });
  state.audit.push(
    { id: 'audit-recon-old', action: 'execution_recon_issues', at: '2026-09-10T20:29:00.000Z', details: 'filled-reconciled' },
    { id: 'audit-recon-new', action: 'execution_reconciled', at: NOW, details: 'filled-reconciled' },
  );
  const decision = evaluateCapitalRiskSnapshot(snapshotFor(state), { now: NOW });

  assert.equal(decision.approved, true);
});

test('agent edge is reconstructed from persisted economic decision lineage', () => {
  const state = goodState();
  state.priceForecasts.push({
    id: 'forecast-risk-test',
    status: 'valid',
    symbol: 'BTC-USD',
    expiresAt: '2026-09-10T20:35:00.000Z',
    asOf: NOW,
  });
  state.executionCostSnapshots.push({
    id: 'cost-risk-test',
    symbol: 'BTC-USD',
    validUntil: '2026-09-10T20:35:00.000Z',
    createdAt: NOW,
  });
  state.economicDecisions.push({
    id: 'decision-risk-test',
    symbol: 'BTC-USD',
    forecastId: 'forecast-risk-test',
    executionCostSnapshotId: 'cost-risk-test',
    modelQuoteId: null,
    executionAllowed: true,
    netExecutableEdgeUsd: 50,
    createdAt: NOW,
  });
  const envelope = intent({
    sourceAgentId: 'agent-risk-test',
    economicDecisionId: 'decision-risk-test',
    forecastId: 'forecast-risk-test',
    executionCostSnapshotId: 'cost-risk-test',
    netExecutableEdgeUsd: 50,
  });
  const snapshot = snapshotFor(state, envelope);
  const decision = evaluateCapitalRiskSnapshot(snapshot, { tradeIntentHash: stableHash(envelope), now: NOW });

  assert.equal(decision.approved, true);
  assert.equal(snapshot.edgeState.required, true);
  assert.equal(snapshot.edgeState.verified, true);
  assert.equal(snapshot.edgeState.netExecutableEdgeUsd, 50);
  assert.ok(snapshot.edgeState.edgeBps > 0);
});

test('caller edge mismatch cannot override persisted economic lineage', () => {
  const state = goodState();
  state.priceForecasts.push({ id: 'forecast-edge', status: 'valid', symbol: 'BTC-USD', expiresAt: '2026-09-10T20:35:00.000Z', asOf: NOW });
  state.executionCostSnapshots.push({ id: 'cost-edge', symbol: 'BTC-USD', validUntil: '2026-09-10T20:35:00.000Z', createdAt: NOW });
  state.economicDecisions.push({
    id: 'decision-edge',
    symbol: 'BTC-USD',
    forecastId: 'forecast-edge',
    executionCostSnapshotId: 'cost-edge',
    executionAllowed: true,
    netExecutableEdgeUsd: 50,
    createdAt: NOW,
  });
  const envelope = intent({
    sourceAgentId: 'agent-risk-test',
    economicDecisionId: 'decision-edge',
    forecastId: 'forecast-edge',
    executionCostSnapshotId: 'cost-edge',
    netExecutableEdgeUsd: 40,
  });
  const decision = evaluateCapitalRiskSnapshot(snapshotFor(state, envelope), { tradeIntentHash: stableHash(envelope), now: NOW });

  assert.equal(decision.approved, false);
  assert.ok(decision.reasons.includes('capital_risk_missing:edge_lineage:caller_edge_mismatch'));
});

test('snapshot tampering is detected after hashing', () => {
  const envelope = intent();
  const snapshot = snapshotFor(goodState(), envelope);
  snapshot.accountState.cashUsd = 1;
  const verification = verifyCapitalRiskSnapshot(snapshot, { tradeIntentHash: stableHash(envelope), now: NOW });

  assert.equal(verification.ok, false);
  assert.ok(verification.reasons.includes('capital_risk_snapshot_hash_mismatch'));
});
