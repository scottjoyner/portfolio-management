import test from 'node:test';
import assert from 'node:assert/strict';

import {
  researchCertificationIdentity,
  researchStableHash,
  runtimeBindingIdentity,
  verifyResearchCertification,
} from '../packages/execution/src/researchCertification.mjs';
import { strategySignalToOpportunityInput } from '../apps/api/src/opportunityGenerator.mjs';

function certification() {
  const config = { period: 14, oversold: 30, overbought: 70 };
  const metrics = {
    win_rate: 0.66,
    trade_count: 20,
    total_return_pct: 8,
    sharpe: 1.3,
    profit_factor: 1.5,
    max_drawdown_pct: 5,
  };
  const cert = {
    schema_version: 1,
    certification_type: 'research_tournament_terminal_runtime_v1',
    candidate_id: 'challenger-rsi',
    strategy_name: 'rsi_revert',
    symbol: 'BTC-USD',
    granularity: '3600',
    candidate_source_sha: 'a'.repeat(40),
    candidate_config_hash: researchStableHash(config),
    alpha_validation_evidence_hash: 'b'.repeat(64),
    terminal_holdout_evidence_hash: 'c'.repeat(64),
    experiment_id: 'experiment-runtime',
    experiment_hash: 'd'.repeat(64),
    selection_hash: 'e'.repeat(64),
    terminal_result_hash: 'f'.repeat(64),
    terminal_metrics_hash: researchStableHash(metrics),
    replay_attestation_hash: '1'.repeat(64),
    evaluation_lineage_id: 'lineage-eval',
    candidate_config: config,
    terminal_metrics: metrics,
  };
  cert.certification_hash = researchStableHash(researchCertificationIdentity(cert));
  const binding = {
    method: 'configured_rust_signal_v1',
    certification_hash: cert.certification_hash,
    strategy_name: cert.strategy_name,
    strategy_config_hash: cert.candidate_config_hash,
    symbol: cert.symbol,
    granularity: cert.granularity,
  };
  binding.runtime_binding_hash = researchStableHash(runtimeBindingIdentity(binding));
  cert.runtime_binding = binding;
  return cert;
}

function signal(cert = certification()) {
  return {
    tournament_certified: true,
    research_certification: cert,
    strategy: 'rsi_revert',
    symbol: 'BTC-USD',
    product_id: 'BTC-USD',
    action: 'BUY',
    trade_intent: 'entry',
    execution_purpose: 'open_long',
    weighted_confidence: 0.73,
    win_rate: 0.66,
    price: 100,
    entry_price: 100,
    stop_loss_price: 95,
    take_profit_price: 110,
    trade_plan: {
      plan_type: 'entry',
      execution_purpose: 'open_long',
      position_side: 'long',
      entry_price: 100,
      stop_loss_price: 95,
      take_profit_price: 110,
    },
    source: 'live_cli',
    reason: 'configured RSI',
  };
}

test('valid exact runtime certification is preserved but not auto-admitted yet', () => {
  const cert = certification();
  assert.deepEqual(
    verifyResearchCertification(cert, { strategyId: 'rsi_revert', symbol: 'BTC-USD' }).reasons,
    [],
  );
  const input = strategySignalToOpportunityInput(signal(cert));
  assert.equal(input.sourceAgentId, 'certified-strategy-runtime');
  assert.equal(input.backtestStatus, 'tournament_terminal_promoted_exact_runtime');
  assert.equal(input.executionAdmission.status, 'research_certified_pending_executable_edge');
  assert.equal(input.executionAdmission.autoDraftEligible, false);
  assert.equal(input.executionAdmission.certificationHash, cert.certification_hash);
  assert.equal(input.researchCertification.certification_hash, cert.certification_hash);
  assert.equal(input.evidence[0].tournamentCertified, true);
  assert.equal(input.evidence[0].researchCertificationHash, cert.certification_hash);
});

test('tampered runtime certification is demoted to screen-only', () => {
  const cert = certification();
  cert.symbol = 'ETH-USD';
  const check = verifyResearchCertification(cert, { strategyId: 'rsi_revert', symbol: 'BTC-USD' });
  assert.equal(check.ok, false);
  const input = strategySignalToOpportunityInput(signal(cert));
  assert.equal(input.sourceAgentId, 'strategy-comparison-scanner');
  assert.equal(input.executionAdmission.status, 'screen_only');
  assert.equal(input.executionAdmission.autoDraftEligible, false);
  assert.equal(input.researchCertification, null);
  assert.equal(input.evidence[0].tournamentCertified, false);
});
