import assert from 'node:assert/strict';
import test from 'node:test';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import {
  certifiedShadowSnapshot,
  createCertifiedShadowTrial,
  matureCertifiedShadowTrials,
  recordCertifiedShadowSignalObservation,
  stableShadowHash,
  validateCertifiedShadowIdentity,
} from '../packages/economics/src/certifiedShadowAttribution.mjs';
import { evaluateEconomicDecision } from '../packages/economics/src/economicDecisionEngine.mjs';

const T0 = '2026-09-12T16:00:00.000Z';
const T1 = '2026-09-12T17:00:00.000Z';
const T2 = '2026-09-12T18:00:00.000Z';

function lifecycle(overrides = {}) {
  return {
    method: 'canonical_replay_lifecycle_v1',
    dataset_kind: 'spot',
    dataset_symbol: 'BTC-USD',
    granularity_seconds: 3600,
    warmup_bars: 30,
    fee_bps: 7.5,
    max_hold_bars: 12,
    exit_on_opposite_signal: true,
    close_open_trade_at_observation_end: true,
    ...overrides,
  };
}

function certification(overrides = {}) {
  const replay = overrides.replay_execution_config || lifecycle();
  return {
    certified: true,
    runtime_identity_hash: 'a'.repeat(64),
    candidate_id: 'challenger-shadow-1',
    candidate_source_sha: 'b'.repeat(40),
    strategy_name: 'rsi_revert',
    strategy_config_hash: 'c'.repeat(64),
    alpha_validation_evidence_hash: 'd'.repeat(64),
    terminal_holdout_evidence_hash: 'e'.repeat(64),
    replay_attestation_hash: 'f'.repeat(64),
    runtime_binary_sha256: '1'.repeat(64),
    runtime_evaluator: 'rust_core.run_rsi_revert_opens_configured_py',
    replay_execution_config: replay,
    replay_execution_config_hash: stableShadowHash(replay),
    certification_scope: 'configured_signal_only_v1',
    execution_lifecycle_certified: false,
    ...overrides,
  };
}

function signal({ action = 'SELL', price = 100, at = T0, cert = certification() } = {}) {
  return {
    symbol: 'BTC-USD',
    strategy: 'rsi_revert',
    action,
    price,
    observed_at: at,
    source: 'certified-shadow-test',
    trade_plan: { runtime_certification: cert },
  };
}

function stateWithSignal({ action = 'SELL', cert = certification(), at = T0, price = 100 } = {}) {
  const state = createInitialOperatorState(T0);
  const recorded = recordCertifiedShadowSignalObservation(state, signal({ action, cert, at, price }), at);
  assert.ok(recorded.signalObservation);
  state.opportunities.push({
    id: 'opp-shadow-1',
    symbol: 'BTC-USD',
    strategyId: 'rsi_revert',
    // Deliberately contradictory scanner lifecycle. Canonical shadow direction
    // must come from the certified signal observation instead.
    positionSide: 'long',
    tradeIntent: action === 'SELL' ? 'exit' : 'entry',
    totalMoneyRisked: 1000,
    entryPrice: price,
    tradePlan: {
      plan_type: action === 'SELL' ? 'exit' : 'entry',
      position_side: 'long',
      runtime_certification: cert,
    },
    certifiedShadowSignalObservationId: recorded.signalObservation.id,
    createdAt: at,
    updatedAt: at,
  });
  return { state, observation: recorded.signalObservation };
}

function addEconomics(state, {
  expectedReturnBps = -100,
  now = T0,
  notionalUsd = 1000,
  totalExecutionCostUsd = 2,
} = {}) {
  state.priceForecasts.push({
    id: 'forecast-shadow-1',
    status: 'valid',
    symbol: 'BTC-USD',
    opportunityId: 'opp-shadow-1',
    expectedReturnBps,
    expectedVolatilityBps: 0,
    probabilityUp: expectedReturnBps >= 0 ? 0.6 : 0.4,
    expiresAt: '2026-09-12T20:00:00.000Z',
    createdAt: now,
  });
  state.executionCostSnapshots.push({
    id: 'cost-shadow-1',
    symbol: 'BTC-USD',
    notionalUsd,
    quantity: 10,
    referencePrice: 100,
    totalExecutionCostUsd,
    latencyDecayUsd: 0,
    validUntil: '2026-09-12T20:00:00.000Z',
    createdAt: now,
  });
  state.marketDataSnapshots.push({
    id: 'snapshot-entry',
    symbol: 'BTC-USD',
    bid: 99.9,
    ask: 100.1,
    mid: 100,
    timestamp: now,
  });
}


test('shadow certification fails closed on replay lifecycle tampering', () => {
  const cert = certification();
  cert.replay_execution_config.fee_bps = 99;
  const validation = validateCertifiedShadowIdentity(cert);
  assert.equal(validation.ok, false);
  assert.ok(validation.reasons.includes('canonical_replay_lifecycle_hash_mismatch'));
});


test('SELL trial direction comes from certified observation, never heuristic opportunity lifecycle', () => {
  const { state, observation } = stateWithSignal({ action: 'SELL' });
  addEconomics(state);
  const decision = {
    id: 'decision-shadow-1',
    opportunityId: 'opp-shadow-1',
    symbol: 'BTC-USD',
    executionCostSnapshotId: 'cost-shadow-1',
    predictedEdgeUsd: 10,
    netExecutableEdgeUsd: 8,
    executionCostsUsd: 2,
    modelCostUsd: 0,
    latencyDecayUsd: 0,
    uncertaintyReserveUsd: 0,
    executionAllowed: true,
    createdAt: T0,
  };
  state.economicDecisions.push(decision);

  const result = createCertifiedShadowTrial(state, {
    opportunityId: 'opp-shadow-1',
    economicDecision: decision,
  }, T0);
  assert.ok(result.shadowTrial);
  assert.equal(result.shadowTrial.side, 'SELL');
  assert.equal(result.shadowTrial.signalObservedAt, observation.observedAt);
  assert.equal(result.shadowTrial.signalPrice, 100);
  assert.equal(result.shadowTrial.executionLifecycleCertified, false);
});


test('certified SELL edge is signed but admission waits for forward shadow calibration', () => {
  const { state } = stateWithSignal({ action: 'SELL' });
  addEconomics(state, { expectedReturnBps: -100 });

  const result = evaluateEconomicDecision(state, {
    opportunityId: 'opp-shadow-1',
    forecastId: 'forecast-shadow-1',
    executionCostSnapshotId: 'cost-shadow-1',
    minimumNetEdgeUsd: 0,
    uncertaintyReserveFraction: 0,
  }, T0);

  assert.ok(result.economicDecision);
  assert.equal(result.economicDecision.predictedEdgeUsd, 10);
  assert.equal(result.economicDecision.rawNetExecutableEdgeUsd, 8);
  assert.equal(result.economicDecision.netExecutableEdgeUsd, 8);
  assert.equal(result.economicDecision.executionAllowed, false);
  assert.equal(result.economicDecision.shadowCalibration.ready, false);
  assert.ok(result.economicDecision.blockers.includes('certified_shadow_calibration_insufficient_samples'));
  assert.ok(result.certifiedShadowTrial);
  assert.equal(result.certifiedShadowTrial.side, 'SELL');
  assert.equal(result.certifiedShadowTrial.predictedNetExecutableEdgeUsd, 8);
});


test('certified SELL rejects bullish default edge instead of treating it as profitable', () => {
  const { state } = stateWithSignal({ action: 'SELL' });
  addEconomics(state, { expectedReturnBps: 100 });

  const result = evaluateEconomicDecision(state, {
    opportunityId: 'opp-shadow-1',
    forecastId: 'forecast-shadow-1',
    executionCostSnapshotId: 'cost-shadow-1',
    minimumNetEdgeUsd: 0,
    uncertaintyReserveFraction: 0,
  }, T0);

  assert.equal(result.economicDecision.predictedEdgeUsd, -10);
  assert.equal(result.economicDecision.netExecutableEdgeUsd, -12);
  assert.equal(result.economicDecision.executionAllowed, false);
  assert.ok(result.certifiedShadowTrial, 'shadow measurement should include rejected economic decisions');
});


test('opposite certified signal closes SELL shadow trial and attributes edge/cost/latency error', () => {
  const cert = certification();
  const { state } = stateWithSignal({ action: 'SELL', cert });
  addEconomics(state, { expectedReturnBps: -100 });
  const decisionResult = evaluateEconomicDecision(state, {
    opportunityId: 'opp-shadow-1',
    forecastId: 'forecast-shadow-1',
    executionCostSnapshotId: 'cost-shadow-1',
    minimumNetEdgeUsd: 0,
    uncertaintyReserveFraction: 0,
  }, T0);
  assert.ok(decisionResult.certifiedShadowTrial);

  state.marketDataSnapshots.push({
    id: 'snapshot-exit',
    symbol: 'BTC-USD',
    bid: 89.9,
    ask: 90.1,
    mid: 90,
    timestamp: T1,
  });
  const opposite = recordCertifiedShadowSignalObservation(state, signal({
    action: 'BUY',
    price: 90,
    at: T1,
    cert,
  }), T1);
  assert.ok(opposite.signalObservation);

  const matured = matureCertifiedShadowTrials(state, T1);
  assert.equal(matured.shadowOutcomes.length, 1);
  const outcome = matured.shadowOutcomes[0];
  assert.equal(outcome.exitReason, 'opposite_signal');
  assert.equal(outcome.side, 'SELL');
  assert.equal(outcome.referenceGrossPnlUsd, 100);
  assert.equal(outcome.canonicalReplayFeeUsd, 1.5);
  assert.equal(outcome.entryImplementationShortfallUsd, 1);
  assert.equal(outcome.exitImplementationShortfallUsd, 1);
  assert.equal(outcome.simulatedExecutionCostsUsd, 3.5);
  assert.equal(outcome.canonicalShadowNetPnlUsd, 96.5);
  assert.equal(outcome.predictedNetExecutableEdgeUsd, 8);
  assert.equal(outcome.edgeErrorUsd, 88.5);
  assert.equal(outcome.predictionSignCorrect, true);

  const snapshot = certifiedShadowSnapshot(state);
  assert.equal(snapshot.summary.samples, 1);
  assert.equal(snapshot.summary.openTrials, 0);
  assert.equal(snapshot.summary.closedTrials, 1);
  assert.equal(snapshot.summary.signAccuracy, 1);
});


test('max-hold canonical exit waits for the first post-horizon market snapshot', () => {
  const cert = certification({ replay_execution_config: lifecycle({ max_hold_bars: 1 }) });
  const { state } = stateWithSignal({ action: 'BUY', cert });
  addEconomics(state, { expectedReturnBps: 50 });
  const decisionResult = evaluateEconomicDecision(state, {
    opportunityId: 'opp-shadow-1',
    forecastId: 'forecast-shadow-1',
    executionCostSnapshotId: 'cost-shadow-1',
    predictedEdgeUsd: 5,
    minimumNetEdgeUsd: 0,
    uncertaintyReserveFraction: 0,
  }, T0);
  assert.ok(decisionResult.certifiedShadowTrial);

  const pending = matureCertifiedShadowTrials(state, T1);
  assert.equal(pending.shadowOutcomes.length, 0);
  assert.equal(pending.pendingShadowTrials[0].blocker, 'post_max_hold_market_snapshot_required');

  state.marketDataSnapshots.push({
    id: 'snapshot-max-hold',
    symbol: 'BTC-USD',
    bid: 104.9,
    ask: 105.1,
    mid: 105,
    timestamp: T2,
  });
  const matured = matureCertifiedShadowTrials(state, T2);
  assert.equal(matured.shadowOutcomes.length, 1);
  assert.equal(matured.shadowOutcomes[0].exitReason, 'max_hold');
  assert.equal(matured.shadowOutcomes[0].sourceSnapshotId, 'snapshot-max-hold');
});
