import assert from 'node:assert/strict';
import test from 'node:test';

import { stableHash } from '../packages/execution/src/overseerLegacy.mjs';
import {
  CERTIFIED_SPOT_LIFECYCLE_METHOD,
  deriveCertifiedSpotLifecycleAction,
  validateCertifiedSpotLifecycle,
  verifyCertifiedSpotLifecycleAuthorization,
} from '../packages/execution/src/certifiedSpotLifecycle.mjs';

function certification(overrides = {}) {
  const lifecycle = {
    method: CERTIFIED_SPOT_LIFECYCLE_METHOD,
    position_mode: 'spot_long_only',
    dataset_kind: 'coinbase_candles',
    dataset_symbol: 'BTC-USD',
    granularity_seconds: 3600,
    warmup_bars: 30,
    fee_bps: 10,
    max_hold_bars: 3,
    buy_while_flat: 'open_long',
    buy_while_long: 'noop',
    sell_while_flat: 'noop',
    sell_while_long: 'close_long',
    max_hold_on_quiet_bar: true,
    same_bar_reentry_after_forced_exit: false,
    close_open_trade_at_observation_end: true,
  };
  return {
    certified: true,
    runtime_identity_hash: 'a'.repeat(64),
    execution_lifecycle_certified: true,
    spot_execution_attestation_hash: 'b'.repeat(64),
    spot_execution_lifecycle: lifecycle,
    spot_execution_lifecycle_hash: stableHash(lifecycle),
    ...overrides,
  };
}

const base = {
  certification: certification(),
  symbol: 'BTC-USD',
  observedAt: '2026-09-13T12:00:00.000Z',
};

test('certification binds exact spot-long-only lifecycle', () => {
  const result = validateCertifiedSpotLifecycle(base.certification, { symbol: 'BTC-USD' });
  assert.equal(result.ok, true, result.reasons.join(','));

  const wrongSymbol = validateCertifiedSpotLifecycle(base.certification, { symbol: 'ETH-USD' });
  assert.equal(wrongSymbol.ok, false);
  assert.ok(wrongSymbol.reasons.includes('spot_execution_symbol_mismatch'));
});

test('BUY opens only while flat and never pyramids', () => {
  const opened = deriveCertifiedSpotLifecycleAction({
    ...base,
    signalAction: 'BUY',
    openQuantity: 0,
  });
  assert.equal(opened.ok, true);
  assert.equal(opened.authorization.decision, 'OPEN_LONG');
  assert.equal(opened.authorization.side, 'buy');

  const repeated = deriveCertifiedSpotLifecycleAction({
    ...base,
    signalAction: 'BUY',
    openQuantity: 0.25,
    positionOpenedAt: '2026-09-13T11:00:00.000Z',
  });
  assert.equal(repeated.authorization.decision, 'NOOP');
  assert.equal(repeated.authorization.reason, 'buy_while_long');
});

test('SELL closes a long but is a no-op while flat', () => {
  const flat = deriveCertifiedSpotLifecycleAction({
    ...base,
    signalAction: 'SELL',
    openQuantity: 0,
  });
  assert.equal(flat.authorization.decision, 'NOOP');
  assert.equal(flat.authorization.reason, 'sell_while_flat');

  const long = deriveCertifiedSpotLifecycleAction({
    ...base,
    signalAction: 'SELL',
    openQuantity: 0.25,
    positionOpenedAt: '2026-09-13T11:00:00.000Z',
  });
  assert.equal(long.authorization.decision, 'CLOSE_LONG');
  assert.equal(long.authorization.side, 'sell');
  assert.equal(long.authorization.reason, 'sell_while_long');
});

test('max hold wins before current signal and blocks same-bar re-entry', () => {
  const result = deriveCertifiedSpotLifecycleAction({
    certification: base.certification,
    symbol: 'BTC-USD',
    signalAction: 'BUY',
    openQuantity: 0.25,
    positionOpenedAt: '2026-09-13T09:00:00.000Z',
    observedAt: '2026-09-13T12:00:00.000Z',
  });
  assert.equal(result.authorization.decision, 'CLOSE_LONG');
  assert.equal(result.authorization.reason, 'max_hold');
  assert.equal(result.authorization.side, 'sell');
});

test('authorization is hash-bound to runtime identity, lifecycle and symbol', () => {
  const result = deriveCertifiedSpotLifecycleAction({
    ...base,
    signalAction: 'BUY',
    openQuantity: 0,
  });
  const verified = verifyCertifiedSpotLifecycleAuthorization(result.authorization, {
    certification: base.certification,
    symbol: 'BTC-USD',
    requiredDecision: 'OPEN_LONG',
  });
  assert.equal(verified.ok, true, verified.reasons.join(','));

  const tampered = { ...result.authorization, openQuantity: 99 };
  const rejected = verifyCertifiedSpotLifecycleAuthorization(tampered, {
    certification: base.certification,
    symbol: 'BTC-USD',
    requiredDecision: 'OPEN_LONG',
  });
  assert.equal(rejected.ok, false);
  assert.ok(rejected.reasons.includes('certified_spot_lifecycle_authorization_hash_mismatch'));
});

test('a boolean certification flag cannot substitute for fresh spot evidence', () => {
  const bad = certification({
    spot_execution_attestation_hash: null,
    spot_execution_lifecycle_hash: '0'.repeat(64),
  });
  const result = deriveCertifiedSpotLifecycleAction({
    certification: bad,
    symbol: 'BTC-USD',
    signalAction: 'BUY',
    openQuantity: 0,
  });
  assert.equal(result.ok, false);
  assert.ok(result.reasons.includes('spot_execution_attestation_hash_required'));
  assert.ok(result.reasons.includes('spot_execution_lifecycle_hash_mismatch'));
});
