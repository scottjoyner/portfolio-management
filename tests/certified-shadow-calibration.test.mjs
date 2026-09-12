import assert from 'node:assert/strict';
import test from 'node:test';

import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';
import {
  applyCertifiedShadowCalibration,
  buildCertifiedShadowCalibration,
  SHADOW_CALIBRATION_MIN_DISTINCT_DAYS,
  SHADOW_CALIBRATION_MIN_SAMPLES,
} from '../packages/economics/src/certifiedShadowCalibration.mjs';

const IDENTITY = 'a'.repeat(64);
const SYMBOL = 'BTC-USD';

function stateWithOutcomes({ samples = 20, actualPerTrial = 5, predictedPerTrial = 10, duplicateFirst = false } = {}) {
  const state = createInitialOperatorState('2026-09-01T00:00:00.000Z');
  state.economicMaintenance.certifiedShadowAttribution = {
    schemaVersion: 1,
    signalObservations: [],
    trials: [],
    outcomes: [],
  };
  for (let index = 0; index < samples; index += 1) {
    const day = 1 + Math.floor(index / 4);
    const hour = 8 + (index % 4) * 2;
    const dayText = String(day).padStart(2, '0');
    const hourText = String(hour).padStart(2, '0');
    const exitHourText = String(hour + 1).padStart(2, '0');
    const observedAt = `2026-09-${dayText}T${hourText}:00:00.000Z`;
    const trialId = `trial-${index}`;
    state.economicMaintenance.certifiedShadowAttribution.trials.push({
      id: trialId,
      runtimeIdentityHash: IDENTITY,
      symbol: SYMBOL,
      side: 'BUY',
      signalObservedAt: observedAt,
    });
    state.economicMaintenance.certifiedShadowAttribution.outcomes.push({
      id: `outcome-${index}`,
      trialId,
      runtimeIdentityHash: IDENTITY,
      symbol: SYMBOL,
      side: 'BUY',
      predictedNetExecutableEdgeUsd: predictedPerTrial,
      canonicalShadowNetPnlUsd: actualPerTrial,
      executionCostForecastErrorUsd: 0.5,
      latencyForecastErrorUsd: 0.25,
      exitObservedAt: `2026-09-${dayText}T${exitHourText}:00:00.000Z`,
      createdAt: `2026-09-${dayText}T${exitHourText}:01:00.000Z`,
    });
  }
  if (duplicateFirst) {
    state.economicMaintenance.certifiedShadowAttribution.trials.push({
      id: 'trial-duplicate',
      runtimeIdentityHash: IDENTITY,
      symbol: SYMBOL,
      side: 'BUY',
      signalObservedAt: '2026-09-01T08:00:00.000Z',
    });
    state.economicMaintenance.certifiedShadowAttribution.outcomes.push({
      id: 'outcome-duplicate',
      trialId: 'trial-duplicate',
      runtimeIdentityHash: IDENTITY,
      symbol: SYMBOL,
      side: 'BUY',
      predictedNetExecutableEdgeUsd: 1000,
      canonicalShadowNetPnlUsd: 1000,
      executionCostForecastErrorUsd: 0,
      latencyForecastErrorUsd: 0,
      exitObservedAt: '2026-09-01T10:00:00.000Z',
      createdAt: '2026-09-01T10:01:00.000Z',
    });
  }
  return state;
}


test('calibration stays unavailable before enough forward samples and days', () => {
  const state = stateWithOutcomes({ samples: SHADOW_CALIBRATION_MIN_SAMPLES - 1 });
  const calibration = buildCertifiedShadowCalibration(state, {
    runtimeIdentityHash: IDENTITY,
    symbol: SYMBOL,
  });
  assert.equal(calibration.ready, false);
  assert.equal(calibration.edgeMultiplier, null);
  assert.ok(calibration.blockers.includes('certified_shadow_calibration_insufficient_samples'));
});


test('ready calibration only discounts edge and never boosts it', () => {
  const state = stateWithOutcomes({ samples: 20, actualPerTrial: 5, predictedPerTrial: 10 });
  const calibration = buildCertifiedShadowCalibration(state, {
    runtimeIdentityHash: IDENTITY,
    symbol: SYMBOL,
  });
  assert.equal(calibration.ready, true);
  assert.equal(calibration.samples, SHADOW_CALIBRATION_MIN_SAMPLES);
  assert.equal(calibration.distinctDays, SHADOW_CALIBRATION_MIN_DISTINCT_DAYS);
  assert.equal(calibration.rawCaptureRatio, 0.5);
  assert.equal(calibration.signAccuracy, 1);
  assert.equal(calibration.edgeMultiplier, 0.5);

  const decision = { netExecutableEdgeUsd: 20, executionAllowed: true, blockers: [] };
  applyCertifiedShadowCalibration(decision, calibration, { minimumNetEdgeUsd: 0 });
  assert.equal(decision.rawNetExecutableEdgeUsd, 20);
  assert.equal(decision.shadowCalibratedNetExecutableEdgeUsd, 10);
  assert.equal(decision.netExecutableEdgeUsd, 10);
  assert.equal(decision.executionAllowed, true);

  const outperforming = stateWithOutcomes({ samples: 20, actualPerTrial: 20, predictedPerTrial: 10 });
  const capped = buildCertifiedShadowCalibration(outperforming, {
    runtimeIdentityHash: IDENTITY,
    symbol: SYMBOL,
  });
  assert.equal(capped.rawCaptureRatio, 2);
  assert.equal(capped.edgeMultiplier, 1, 'forward results must never inflate model edge');
});


test('poor sign accuracy constrains calibration even when aggregate dollars look good', () => {
  const state = stateWithOutcomes({ samples: 20, actualPerTrial: 20, predictedPerTrial: 10 });
  const outcomes = state.economicMaintenance.certifiedShadowAttribution.outcomes;
  for (let index = 0; index < 8; index += 1) outcomes[index].canonicalShadowNetPnlUsd = -1;
  const calibration = buildCertifiedShadowCalibration(state, {
    runtimeIdentityHash: IDENTITY,
    symbol: SYMBOL,
  });
  assert.equal(calibration.ready, true);
  assert.equal(calibration.signAccuracy, 0.6);
  assert.equal(calibration.edgeMultiplier, 0.6);
});


test('repeated economic recalculation around one signal cannot manufacture sample count', () => {
  const state = stateWithOutcomes({ samples: 20, duplicateFirst: true });
  const calibration = buildCertifiedShadowCalibration(state, {
    runtimeIdentityHash: IDENTITY,
    symbol: SYMBOL,
  });
  assert.equal(calibration.samples, 20);
});


test('economic admission fails closed while calibration is immature or discounts below minimum edge', () => {
  const immature = buildCertifiedShadowCalibration(stateWithOutcomes({ samples: 3 }), {
    runtimeIdentityHash: IDENTITY,
    symbol: SYMBOL,
  });
  const first = { netExecutableEdgeUsd: 25, executionAllowed: true, blockers: [] };
  applyCertifiedShadowCalibration(first, immature);
  assert.equal(first.executionAllowed, false);
  assert.equal(first.shadowCalibratedNetExecutableEdgeUsd, null);
  assert.ok(first.blockers.includes('certified_shadow_calibration_insufficient_samples'));

  const mature = buildCertifiedShadowCalibration(stateWithOutcomes({ samples: 20, actualPerTrial: 2, predictedPerTrial: 10 }), {
    runtimeIdentityHash: IDENTITY,
    symbol: SYMBOL,
  });
  const second = { netExecutableEdgeUsd: 10, executionAllowed: true, blockers: [] };
  applyCertifiedShadowCalibration(second, mature, { minimumNetEdgeUsd: 3 });
  assert.equal(second.netExecutableEdgeUsd, 2);
  assert.equal(second.executionAllowed, false);
  assert.ok(second.blockers.includes('shadow_calibrated_edge_insufficient'));
});
