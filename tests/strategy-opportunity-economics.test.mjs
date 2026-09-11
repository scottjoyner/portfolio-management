import assert from 'node:assert/strict';
import test from 'node:test';

import { strategySignalToOpportunityInput } from '../apps/api/src/opportunityGenerator.mjs';
import { createOpportunity, decideOpportunity } from '../apps/api/src/opportunityFlows.mjs';

function entrySignal() {
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

function baseState() {
  return {
    config: { maxPositionSizeUsd: 50000 },
    strategies: [{ id: 'rsi_revert' }],
    backtests: [],
  };
}

test('strategy opportunity expected value is payoff-based USD, not confidence score points', () => {
  const input = strategySignalToOpportunityInput(entrySignal());

  assert.equal(input.totalMoneyRisked, 1000);
  assert.equal(input.potentialUpside, 100);
  assert.equal(input.maxLoss, 50);
  assert.equal(input.winProbability, 0.6);
  assert.equal(input.lossProbability, 0.4);
  assert.equal(input.grossExpectedValue, 40);
  assert.equal(input.expectedValue, 40);
  assert.equal(input.rewardRiskRatio, 2);
  assert.equal(input.backtestStatus, 'same_window_30d_screen_uncertified');
  assert.equal(input.evidence[0].expectedValueUnit, 'USD');
  assert.equal(input.evidence[0].expectedValueMethod, 'tp_sl_payoff_expected_pnl_v1');
  assert.equal(input.evidence[0].tournamentCertified, false);
});

test('risk-reduction exits are not assigned invented entry edge', () => {
  const signal = entrySignal();
  signal.action = 'SELL';
  signal.trade_intent = 'exit';
  signal.trade_plan = {
    ...signal.trade_plan,
    plan_type: 'exit',
    execution_purpose: 'take_profit_exit',
  };

  const input = strategySignalToOpportunityInput(signal);

  assert.equal(input.tradeIntent, 'exit');
  assert.equal(input.grossExpectedValue, 0);
  assert.equal(input.expectedValue, 0);
  assert.equal(input.evidence[0].expectedValueMethod, 'risk_reduction_not_edge_scored_v1');
});

test('opportunity sizing does not inflate requested notional with a context-free Kelly fraction', () => {
  const state = baseState();
  const input = strategySignalToOpportunityInput(entrySignal());
  const result = createOpportunity(state, input, '2026-09-11T20:00:00.000Z');

  assert.equal(result.errors, undefined);
  const opportunity = result.opportunity;
  assert.equal(opportunity.grossExpectedValue, 40);
  assert.equal(opportunity.netExpectedValue, 32);
  assert.equal(opportunity.expectedReturn, 40);
  assert.equal(opportunity.expectedRisk, 20);
  assert.equal(opportunity.positionSizing.recommendedSize, 1000);
  assert.equal(opportunity.positionSizing.capitalAtRisk, 50);
  assert.equal(opportunity.positionSizing.requestedNotional, 1000);
  assert.equal(opportunity.positionSizing.sizingAuthority, 'requested_notional_capped');
  assert.equal(opportunity.positionSizing.kellyCapped, 0.25);
});

test('approval fails closed instead of inventing a 1000 dollar execution size', () => {
  const state = baseState();
  const created = createOpportunity(state, {
    sourceAgentId: 'test',
    marketType: 'crypto_spot',
    venue: 'coinbase-paper',
    symbol: 'BTC-USD',
    title: 'zero-size test',
    confidenceScore: 0.8,
    winProbability: 0.6,
    lossProbability: 0.4,
    totalMoneyRisked: 0,
    maxLoss: 0,
    potentialUpside: 0,
    grossExpectedValue: 0,
  }, '2026-09-11T20:00:00.000Z');

  assert.equal(created.errors, undefined);
  const decision = decideOpportunity(state, created.opportunity.id, {
    status: 'approved',
    reviewer: 'test',
  }, '2026-09-11T20:01:00.000Z');

  assert.deepEqual(decision.errors, ['execution_size_required']);
  assert.equal(created.opportunity.status, 'needs_review');
  assert.equal(state.executions.length, 0);
});

test('explicit zero max position cap remains zero and blocks execution', () => {
  const state = baseState();
  state.config.maxPositionSizeUsd = 0;
  const created = createOpportunity(
    state,
    strategySignalToOpportunityInput(entrySignal()),
    '2026-09-11T20:00:00.000Z',
  );

  assert.equal(created.errors, undefined);
  assert.equal(created.opportunity.positionSizing.maxPositionSize, 0);
  assert.equal(created.opportunity.positionSizing.recommendedSize, 0);

  const decision = decideOpportunity(state, created.opportunity.id, {
    status: 'approved',
    reviewer: 'test',
  }, '2026-09-11T20:01:00.000Z');

  assert.deepEqual(decision.errors, ['execution_size_required']);
  assert.equal(created.opportunity.status, 'needs_review');
  assert.equal(state.executions.length, 0);
});
