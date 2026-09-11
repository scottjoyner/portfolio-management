import assert from 'node:assert/strict';
import test from 'node:test';

import { strategySignalToOpportunityInput } from '../apps/api/src/opportunityGenerator.mjs';
import { createOpportunity, decideOpportunity } from '../apps/api/src/opportunityFlows.mjs';
import {
  RESEARCH_CERTIFICATION_TYPE,
  RESEARCH_RUNTIME_BINDING_METHOD,
  researchStableHash,
} from '../packages/execution/src/researchCertification.mjs';
import { evaluateTradeIntent } from '../packages/execution/src/overseer.mjs';
import { normalizeExecutionRecord } from '../packages/storage/src/executionRepository.mjs';

function validCertification() {
  const identity = {
    schema_version: 1,
    certification_type: RESEARCH_CERTIFICATION_TYPE,
    candidate_id: 'challenger-rsi-btc-hourly',
    strategy_name: 'rsi_revert',
    symbol: 'BTC-USD',
    granularity: '3600',
    candidate_source_sha: 'a'.repeat(40),
    candidate_config_hash: 'b'.repeat(64),
    alpha_validation_evidence_hash: 'c'.repeat(64),
    terminal_holdout_evidence_hash: 'd'.repeat(64),
    experiment_hash: 'e'.repeat(64),
    selection_hash: 'f'.repeat(64),
    terminal_result_hash: '1'.repeat(64),
    terminal_metrics_hash: '2'.repeat(64),
  };
  const certificationHash = researchStableHash(identity);
  const binding = {
    method: RESEARCH_RUNTIME_BINDING_METHOD,
    certification_hash: certificationHash,
    strategy_name: identity.strategy_name,
    strategy_config_hash: identity.candidate_config_hash,
    symbol: identity.symbol,
    granularity: identity.granularity,
  };
  return {
    ...identity,
    candidate_config: { period: 14, oversold: 30, overbought: 70 },
    terminal_metrics: { trade_count: 12, win_rate: 0.67, total_return_pct: 8.5 },
    certification_hash: certificationHash,
    runtime_binding: {
      ...binding,
      runtime_binding_hash: researchStableHash(binding),
    },
  };
}

function entrySignal({ certified = false } = {}) {
  const certification = certified ? validCertification() : null;
  return {
    product_id: 'BTC-USD',
    symbol: 'BTC-USD',
    strategy: 'rsi_revert',
    action: 'BUY',
    trade_intent: 'entry',
    weighted_confidence: 0.8,
    win_rate: 0.6,
    notional_usd: 1000,
    estimated_fees: 5,
    estimated_slippage: 3,
    tournament_certified: certified,
    research_certification: certification,
    trade_plan: {
      plan_type: 'entry',
      position_side: 'long',
      execution_purpose: 'open_long',
      entry_price: 100,
      take_profit_price: 110,
      stop_loss_price: 95,
    },
  };
}

function state() {
  return {
    config: { maxPositionSizeUsd: 50000 },
    strategies: [{ id: 'rsi_revert' }],
    backtests: [],
  };
}

function createScannerOpportunity(targetState, { certified = false } = {}) {
  return createOpportunity(
    targetState,
    {
      ...strategySignalToOpportunityInput(entrySignal({ certified })),
      strategyId: 'rsi_revert',
      status: 'needs_review',
      approvalStatus: 'needs_review',
    },
    '2026-09-11T22:00:00.000Z',
  );
}

test('uncertified strategy-scanner entry remains non-executable', () => {
  const targetState = state();
  const created = createScannerOpportunity(targetState);
  assert.equal(created.errors, undefined);
  assert.equal(created.opportunity.backtestStatus, 'same_window_30d_screen_uncertified');

  const result = decideOpportunity(targetState, created.opportunity.id, {
    status: 'approved',
    reviewer: 'test',
  }, '2026-09-11T22:01:00.000Z');

  assert.deepEqual(result.errors, ['research_certification_required']);
  assert.equal(created.opportunity.status, 'needs_review');
  assert.equal(targetState.executions.length, 0);
});

test('certified strategy entry still requires a current economic decision', () => {
  const targetState = state();
  const created = createScannerOpportunity(targetState, { certified: true });
  assert.equal(created.errors, undefined);
  assert.equal(created.opportunity.backtestStatus, 'tournament_terminal_certified_runtime');

  const result = decideOpportunity(targetState, created.opportunity.id, {
    status: 'approved',
    reviewer: 'test',
  }, '2026-09-11T22:01:00.000Z');

  assert.deepEqual(result.errors, ['strategy_entry_economic_approval_required']);
  assert.equal(targetState.executions.length, 0);
});

test('certified positive-edge strategy entry can create a paper draft', () => {
  const targetState = state();
  const created = createScannerOpportunity(targetState, { certified: true });
  const opportunity = created.opportunity;
  opportunity.economicDecisionId = 'economic-decision-1';
  opportunity.economicExecutionAllowed = true;
  opportunity.netExecutableEdgeUsd = 12.5;

  const result = decideOpportunity(targetState, opportunity.id, {
    status: 'approved',
    reviewer: 'test',
  }, '2026-09-11T22:01:00.000Z');

  assert.equal(result.errors, undefined);
  assert.equal(result.opportunity.status, 'approved');
  assert.equal(result.execution.status, 'draft');
  assert.equal(result.execution.economicDecisionId, 'economic-decision-1');
  assert.equal(result.execution.netExecutableEdgeUsd, 12.5);
  assert.equal(
    result.execution.researchCertification.certification_hash,
    opportunity.researchCertification.certification_hash,
  );
});

test('certified strategy entry with nonpositive executable edge is rejected', () => {
  const targetState = state();
  const created = createScannerOpportunity(targetState, { certified: true });
  created.opportunity.economicDecisionId = 'economic-decision-1';
  created.opportunity.economicExecutionAllowed = true;
  created.opportunity.netExecutableEdgeUsd = 0;

  const result = decideOpportunity(targetState, created.opportunity.id, {
    status: 'approved',
    reviewer: 'test',
  }, '2026-09-11T22:01:00.000Z');

  assert.deepEqual(result.errors, ['strategy_entry_positive_executable_edge_required']);
  assert.equal(targetState.executions.length, 0);
});

test('uncertified strategy exit remains available for risk reduction', () => {
  const targetState = state();
  const signal = entrySignal();
  signal.action = 'SELL';
  signal.trade_intent = 'exit';
  signal.trade_plan = {
    ...signal.trade_plan,
    plan_type: 'exit',
    execution_purpose: 'take_profit_exit',
  };
  const created = createOpportunity(targetState, {
    ...strategySignalToOpportunityInput(signal),
    strategyId: 'rsi_revert',
  }, '2026-09-11T22:00:00.000Z');

  const result = decideOpportunity(targetState, created.opportunity.id, {
    status: 'approved',
    reviewer: 'test',
  }, '2026-09-11T22:01:00.000Z');

  assert.equal(result.errors, undefined);
  assert.equal(result.execution.side, 'sell');
  assert.equal(result.execution.tradeIntent, 'exit');
});

test('overseer requires a valid research binding only for scanner entries', () => {
  const common = {
    mode: 'paper',
    strategyId: 'rsi_revert',
    symbol: 'BTC-USD',
    side: 'buy',
    tradeIntent: 'entry',
    executionPurpose: 'open_long',
    confidenceScore: 0.8,
    orders: [{ side: 'buy', symbol: 'BTC-USD', quantity: 1, price: 100 }],
  };
  const options = {
    requireRiskCheck: false,
    requireApproval: false,
    now: '2026-09-11T22:00:00.000Z',
  };

  const manual = evaluateTradeIntent({ ...common, sourceAgentId: 'operator' }, options);
  assert.equal(manual.overseerDecision.approved, true);

  const uncertified = evaluateTradeIntent({
    ...common,
    sourceAgentId: 'strategy-comparison-scanner',
  }, options);
  assert.equal(uncertified.overseerDecision.approved, false);
  assert.ok(uncertified.overseerDecision.reasons.includes('research_certification_required'));

  const certification = validCertification();
  const certified = evaluateTradeIntent({
    ...common,
    sourceAgentId: 'strategy-comparison-scanner',
    researchCertification: certification,
    requiresResearchCertification: true,
  }, options);
  assert.equal(certified.overseerDecision.approved, true, certified.overseerDecision.reasons);
  assert.equal(
    certified.tradeIntentEnvelope.researchCertification.certification_hash,
    certification.certification_hash,
  );

  const tampered = structuredClone(certification);
  tampered.runtime_binding.runtime_binding_hash = '0'.repeat(64);
  const rejected = evaluateTradeIntent({
    ...common,
    sourceAgentId: 'strategy-comparison-scanner',
    researchCertification: tampered,
  }, options);
  assert.equal(rejected.overseerDecision.approved, false);
  assert.ok(rejected.overseerDecision.reasons.includes('research_runtime_binding_hash_mismatch'));
});

test('durable execution metadata preserves research certification', () => {
  const certification = validCertification();
  const record = normalizeExecutionRecord({
    id: 'execution-certified-1',
    idempotencyKey: 'execution-certified-1',
    strategyId: 'rsi_revert',
    sourceAgentId: 'strategy-comparison-scanner',
    symbol: 'BTC-USD',
    venue: 'coinbase-paper',
    side: 'buy',
    status: 'draft',
    quantity: 1,
    notionalUsd: 100,
    requestedPrice: 100,
    researchCertification: certification,
    requiresResearchCertification: true,
  }, '2026-09-11T22:00:00.000Z');

  assert.equal(
    record.metadata.researchCertification.certification_hash,
    certification.certification_hash,
  );
  assert.equal(record.metadata.requiresResearchCertification, true);
});
