import { stableShadowHash } from './certifiedShadowAttribution.mjs';

export const SHADOW_CALIBRATION_SCHEMA_VERSION = 1;
export const SHADOW_CALIBRATION_POLICY_VERSION = 'certified_shadow_edge_discount_v1';
export const SHADOW_CALIBRATION_MIN_SAMPLES = 20;
export const SHADOW_CALIBRATION_MIN_DISTINCT_DAYS = 5;

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function round(value, digits = 6) {
  if (!Number.isFinite(Number(value))) return null;
  const scale = 10 ** digits;
  return Math.round(Number(value) * scale) / scale;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function observationDay(value) {
  const date = new Date(value || 0);
  return Number.isFinite(date.getTime()) ? date.toISOString().slice(0, 10) : null;
}

function calibrationRows(state, runtimeIdentityHash, symbol) {
  const shadow = state?.economicMaintenance?.certifiedShadowAttribution;
  const outcomes = Array.isArray(shadow?.outcomes) ? shadow.outcomes : [];
  const trials = Array.isArray(shadow?.trials) ? shadow.trials : [];
  const trialById = new Map(trials.map(row => [row.id, row]));
  const bySignalEpisode = new Map();

  for (const outcome of outcomes) {
    if (outcome?.runtimeIdentityHash !== runtimeIdentityHash || outcome?.symbol !== symbol) continue;
    const predicted = finite(outcome.predictedNetExecutableEdgeUsd, null);
    const actual = finite(outcome.canonicalShadowNetPnlUsd, null);
    if (!(predicted > 0) || actual == null) continue;
    const trial = trialById.get(outcome.trialId) || null;
    const observedAt = trial?.signalObservedAt || outcome.exitObservedAt || outcome.createdAt || null;
    const episodeKey = trial?.certifiedShadowSignalObservationId
      || `${runtimeIdentityHash}|${symbol}|${trial?.side || outcome.side || ''}|${observedAt || outcome.trialId}`;
    const row = {
      outcomeId: outcome.id,
      trialId: outcome.trialId,
      episodeKey,
      observedAt,
      day: observationDay(observedAt),
      predictedNetExecutableEdgeUsd: predicted,
      canonicalShadowNetPnlUsd: actual,
      predictionSignCorrect: actual > 0,
      executionCostForecastErrorUsd: finite(outcome.executionCostForecastErrorUsd, 0),
      latencyForecastErrorUsd: finite(outcome.latencyForecastErrorUsd, 0),
    };
    // Economic recomputation can create more than one trial around one certified
    // signal. Count the signal episode once so recalculation frequency cannot
    // manufacture calibration sample size. Keep the latest forecast for that
    // episode because it is the closest economic estimate to the eventual exit.
    const previous = bySignalEpisode.get(episodeKey);
    if (!previous || new Date(outcome.createdAt || 0) >= new Date(previous.createdAt || 0)) {
      bySignalEpisode.set(episodeKey, { ...row, createdAt: outcome.createdAt || observedAt });
    }
  }
  return [...bySignalEpisode.values()].sort(
    (a, b) => new Date(a.observedAt || 0) - new Date(b.observedAt || 0),
  );
}

export function buildCertifiedShadowCalibration(
  state,
  { runtimeIdentityHash, symbol } = {},
  now = new Date().toISOString(),
) {
  const identity = String(runtimeIdentityHash || '').trim();
  const market = String(symbol || '').trim();
  if (!identity || !market) {
    return {
      schemaVersion: SHADOW_CALIBRATION_SCHEMA_VERSION,
      policyVersion: SHADOW_CALIBRATION_POLICY_VERSION,
      status: 'not_applicable',
      ready: false,
      runtimeIdentityHash: identity || null,
      symbol: market || null,
      samples: 0,
      distinctDays: 0,
      minimumSamples: SHADOW_CALIBRATION_MIN_SAMPLES,
      minimumDistinctDays: SHADOW_CALIBRATION_MIN_DISTINCT_DAYS,
      edgeMultiplier: null,
      blockers: ['certified_shadow_identity_required'],
      generatedAt: now,
    };
  }

  const rows = calibrationRows(state, identity, market);
  const days = [...new Set(rows.map(row => row.day).filter(Boolean))];
  const blockers = [];
  if (rows.length < SHADOW_CALIBRATION_MIN_SAMPLES) blockers.push('certified_shadow_calibration_insufficient_samples');
  if (days.length < SHADOW_CALIBRATION_MIN_DISTINCT_DAYS) blockers.push('certified_shadow_calibration_insufficient_days');

  const predictedTotal = rows.reduce((sum, row) => sum + row.predictedNetExecutableEdgeUsd, 0);
  const actualTotal = rows.reduce((sum, row) => sum + row.canonicalShadowNetPnlUsd, 0);
  const rawCaptureRatio = predictedTotal > 0 ? actualTotal / predictedTotal : null;
  const boundedCaptureRatio = rawCaptureRatio == null ? null : clamp(rawCaptureRatio, 0, 1);
  const signAccuracy = rows.length
    ? rows.filter(row => row.predictionSignCorrect).length / rows.length
    : null;
  const ready = blockers.length === 0;
  // This gate is intentionally one-way: forward evidence can discount an edge
  // but can never increase it above the uncalibrated economic model.
  const edgeMultiplier = ready
    ? clamp(Math.min(boundedCaptureRatio ?? 0, signAccuracy ?? 0), 0, 1)
    : null;
  const outcomeIds = rows.map(row => row.outcomeId);
  const core = {
    schemaVersion: SHADOW_CALIBRATION_SCHEMA_VERSION,
    policyVersion: SHADOW_CALIBRATION_POLICY_VERSION,
    status: ready ? 'ready' : 'insufficient_forward_evidence',
    ready,
    runtimeIdentityHash: identity,
    symbol: market,
    samples: rows.length,
    distinctDays: days.length,
    minimumSamples: SHADOW_CALIBRATION_MIN_SAMPLES,
    minimumDistinctDays: SHADOW_CALIBRATION_MIN_DISTINCT_DAYS,
    positivePredictionOnly: true,
    noUpwardAdjustment: true,
    predictedNetExecutableEdgeUsd: round(predictedTotal, 6),
    canonicalShadowNetPnlUsd: round(actualTotal, 6),
    rawCaptureRatio: round(rawCaptureRatio, 6),
    boundedCaptureRatio: round(boundedCaptureRatio, 6),
    signAccuracy: round(signAccuracy, 6),
    edgeMultiplier: round(edgeMultiplier, 6),
    executionCostForecastErrorUsd: round(rows.reduce((sum, row) => sum + row.executionCostForecastErrorUsd, 0), 6),
    latencyForecastErrorUsd: round(rows.reduce((sum, row) => sum + row.latencyForecastErrorUsd, 0), 6),
    outcomeIds,
    outcomesHash: stableShadowHash(outcomeIds),
    blockers,
    generatedAt: now,
  };
  return { ...core, calibrationHash: stableShadowHash(core) };
}

export function applyCertifiedShadowCalibration(decision, calibration, { minimumNetEdgeUsd = 0 } = {}) {
  if (!decision || typeof decision !== 'object') throw new TypeError('economic decision is required');
  if (!calibration || typeof calibration !== 'object') throw new TypeError('shadow calibration is required');
  const rawNet = finite(decision.netExecutableEdgeUsd, null);
  if (rawNet == null) throw new TypeError('economic decision net executable edge must be finite');
  const minimum = Math.max(0, finite(minimumNetEdgeUsd, 0));
  decision.rawNetExecutableEdgeUsd = round(rawNet, 6);
  decision.shadowCalibration = calibration;
  decision.shadowCalibrationHash = calibration.calibrationHash || null;

  if (calibration.ready !== true || !Number.isFinite(Number(calibration.edgeMultiplier))) {
    decision.executionAllowed = false;
    decision.shadowCalibratedNetExecutableEdgeUsd = null;
    decision.blockers = [...new Set([...(decision.blockers || []), ...(
      calibration.blockers?.length ? calibration.blockers : ['certified_shadow_calibration_required']
    )])];
    return decision;
  }

  const multiplier = clamp(Number(calibration.edgeMultiplier), 0, 1);
  const calibrated = rawNet > 0 ? rawNet * multiplier : rawNet;
  decision.shadowCalibratedNetExecutableEdgeUsd = round(calibrated, 6);
  decision.netExecutableEdgeUsd = round(calibrated, 6);
  if (!(calibrated > minimum)) {
    decision.executionAllowed = false;
    decision.blockers = [...new Set([...(decision.blockers || []), 'shadow_calibrated_edge_insufficient'])];
  }
  return decision;
}
