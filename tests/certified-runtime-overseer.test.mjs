import assert from 'node:assert/strict';
import test from 'node:test';

import {
  OVERSEER_POLICY_VERSION,
  OVERSEER_SCHEMA_VERSION,
  evaluateTradeIntent,
  verifyOverseerDecision,
} from '../packages/execution/src/overseer.mjs';

function request(overrides = {}) {
  return {
    strategyId: 'rsi_revert',
    opportunityId: 'opp-1',
    sourceAgentId: 'strategy-comparison-scanner',
    mode: 'paper',
    symbol: 'BTC-USD',
    venue: 'coinbase-paper',
    side: 'buy',
    tradeIntent: 'entry',
    executionPurpose: 'open_long',
    economicDecisionId: 'economic-1',
    executionCostSnapshotId: 'cost-1',
    netExecutableEdgeUsd: 12.5,
    confidenceScore: 0.8,
    orders: [{
      id: 'order-1',
      strategyId: 'rsi_revert',
      opportunityId: 'opp-1',
      symbol: 'BTC-USD',
      venue: 'coinbase-paper',
      side: 'buy',
      quantity: 0.01,
      price: 50000,
      confidenceScore: 0.8,
    }],
    ...overrides,
  };
}

function certification(overrides = {}) {
  return {
    certified: true,
    runtime_identity_hash: 'a'.repeat(64),
    candidate_id: 'challenger-1',
    candidate_source_sha: 'b'.repeat(40),
    strategy_name: 'rsi_revert',
    strategy_config_hash: 'c'.repeat(64),
    alpha_validation_evidence_hash: 'd'.repeat(64),
    terminal_holdout_evidence_hash: 'e'.repeat(64),
    replay_attestation_hash: 'f'.repeat(64),
    runtime_binary_sha256: '1'.repeat(64),
    runtime_evaluator: 'rust_core.run_rsi_revert_opens_configured_py',
    certification_scope: 'configured_signal_only_v1',
    execution_lifecycle_certified: false,
    ...overrides,
  };
}

const automaticOptions = {
  requireApproval: false,
  requireRiskCheck: false,
  now: '2026-09-12T18:45:00.000Z',
};


test('automated strategy entry rejects missing certified runtime identity', () => {
  const result = evaluateTradeIntent(request(), automaticOptions);
  assert.equal(result.overseerDecision.approved, false);
  assert.equal(result.overseerDecision.decision, 'REJECT');
  assert.ok(result.overseerDecision.reasons.includes('certified_runtime_identity_required'));
});


test('signal-only certification cannot authorize automated execution lifecycle', () => {
  const result = evaluateTradeIntent(request({
    tradePlan: { runtime_certification: certification() },
  }), automaticOptions);
  assert.equal(result.overseerDecision.approved, false);
  assert.ok(result.overseerDecision.reasons.includes('certified_execution_lifecycle_required'));
});


test('automated entry independently requires positive executable edge', () => {
  const result = evaluateTradeIntent(request({
    netExecutableEdgeUsd: 0,
    tradePlan: { runtime_certification: certification({ execution_lifecycle_certified: true }) },
  }), automaticOptions);
  assert.equal(result.overseerDecision.approved, false);
  assert.ok(result.overseerDecision.reasons.includes('net_executable_edge_usd_not_positive'));
});


test('fully certified positive-edge automated entry passes paper overseer gate', () => {
  const result = evaluateTradeIntent(request({
    tradePlan: { runtime_certification: certification({
      certification_scope: 'configured_execution_v1',
      execution_lifecycle_certified: true,
    }) },
  }), automaticOptions);
  assert.equal(result.overseerDecision.approved, true, result.overseerDecision.reasons.join(','));
  assert.equal(result.overseerDecision.decision, 'PAPER');
  assert.equal(result.overseerDecision.schemaVersion, OVERSEER_SCHEMA_VERSION);
  assert.equal(result.overseerDecision.policyVersion, OVERSEER_POLICY_VERSION);
  assert.equal(OVERSEER_SCHEMA_VERSION, 4);

  const verification = verifyOverseerDecision(result.overseerDecision, {
    intentHash: result.tradeIntentHash,
    riskDecisionHash: result.overseerDecision.riskDecisionHash,
    capitalRiskSnapshotHash: null,
    capitalRiskPolicyVersion: null,
    now: '2026-09-12T18:45:01.000Z',
  });
  assert.equal(verification.ok, true, verification.reasons.join(','));
});


test('human-reviewed strategy entry does not pretend certification is execution authority', () => {
  const result = evaluateTradeIntent(request(), {
    ...automaticOptions,
    requireApproval: true,
  });
  assert.equal(result.overseerDecision.approved, true, result.overseerDecision.reasons.join(','));
  assert.equal(result.overseerDecision.requiresHumanApproval, true);
});
