import test from 'node:test';
import assert from 'node:assert/strict';

import { createInitialState } from '../apps/api/src/server.p1.mjs';
import {
  evaluateRemoteIntelligencePolicy,
  historicalRemoteValueEstimate,
} from '../apps/api/src/intelligencePolicy.mjs';

function attributedState(remoteValues, localValues) {
  const state = createInitialState('2026-07-31T07:00:00.000Z');
  state.config.intelligenceRoutingPolicy = {
    mode: 'economic_auto',
    remoteSpendCapUsdPerDay: 2,
    remoteSpendCapUsdPerRequest: 0.25,
    minimumRemoteValueCoverage: 3,
    fallbackToLocalOnRemoteBlock: true,
  };
  state.modelUsageLedger = [];
  state.agentAttributionRecords = [];

  for (const [locality, values] of [['remote', remoteValues], ['local', localValues]]) {
    values.forEach((incrementalValueUsd, index) => {
      const quoteId = `${locality}-quote-${index + 1}`;
      state.modelUsageLedger.push({
        id: quoteId,
        localOrRemote: locality,
        model: locality === 'remote' ? 'openrouter/value-model' : 'qwen-local',
        status: 'reconciled',
        actualCostUsd: locality === 'remote' ? 0.02 : 0.001,
        requestedAt: `2026-07-30T0${index}:00:00.000Z`,
      });
      state.agentAttributionRecords.push({
        id: `${locality}-attribution-${index + 1}`,
        modelQuoteIds: [quoteId],
        incrementalValueUsd,
        observedAt: `2026-07-30T0${index}:30:00.000Z`,
      });
    });
  }
  return state;
}

const deployed = {
  REMOTE_LLM_EXECUTION_ENABLED: 'true',
  OPENROUTER_API_KEY: 'configured',
  LOCAL_LLM_NODES_JSON: '[{"id":"x1-370"}]',
};

test('economic auto remains fail closed until both routes have enough settled attribution', () => {
  const state = attributedState([0.8, 0.9, 0.85, 0.82], [0.1, 0.12, 0.08, 0.11, 0.09]);
  const learned = historicalRemoteValueEstimate(state);
  assert.equal(learned.available, false);
  assert.equal(learned.remote.observations, 4);
  assert.equal(learned.local.observations, 5);

  const decision = evaluateRemoteIntelligencePolicy(state, { estimatedCostUsd: 0.02 }, deployed);
  assert.equal(decision.allowed, false);
  assert.equal(decision.expectedDecisionImprovementUsd, null);
  assert.ok(decision.blockers.includes('expected_remote_decision_improvement_required'));
});

test('economic auto uses a conservative learned uplift after sufficient local and remote outcomes', () => {
  const state = attributedState(
    [0.8, 0.9, 0.85, 0.82, 0.88],
    [0.1, 0.12, 0.08, 0.11, 0.09],
  );
  const learned = historicalRemoteValueEstimate(state);
  assert.equal(learned.available, true);
  assert.equal(learned.remote.observations, 5);
  assert.equal(learned.local.observations, 5);
  assert.ok(learned.expectedDecisionImprovementUsd > 0.6);
  assert.equal(learned.source, 'historical_remote_lower_bound_minus_local_upper_bound');

  const decision = evaluateRemoteIntelligencePolicy(state, { estimatedCostUsd: 0.02 }, deployed);
  assert.equal(decision.allowed, true);
  assert.equal(decision.expectedValueSource, 'historical_remote_lower_bound_minus_local_upper_bound');
  assert.ok(decision.valueCoverage > 30);
});

test('explicit opportunity evidence overrides learned history for the current comparison', () => {
  const state = attributedState(
    [0.8, 0.9, 0.85, 0.82, 0.88],
    [0.1, 0.12, 0.08, 0.11, 0.09],
  );
  const decision = evaluateRemoteIntelligencePolicy(state, {
    estimatedCostUsd: 0.02,
    expectedDecisionImprovementUsd: 0.03,
  }, deployed);
  assert.equal(decision.expectedValueSource, 'request_evidence');
  assert.equal(decision.allowed, false);
  assert.ok(decision.blockers.includes('remote_value_coverage_below_policy'));
});
