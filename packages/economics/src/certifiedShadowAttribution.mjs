import { createHash } from 'node:crypto';

const SHADOW_SCHEMA_VERSION = 1;
const REQUIRED_CERTIFICATION_FIELDS = [
  'runtime_identity_hash',
  'candidate_id',
  'candidate_source_sha',
  'strategy_name',
  'strategy_config_hash',
  'alpha_validation_evidence_hash',
  'terminal_holdout_evidence_hash',
  'replay_attestation_hash',
  'runtime_binary_sha256',
  'runtime_evaluator',
  'replay_execution_config_hash',
];

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function nonNegative(value, fallback = 0) {
  const number = finite(value, fallback);
  return number >= 0 ? number : fallback;
}

function round(value, digits = 6) {
  if (value == null || !Number.isFinite(Number(value))) return null;
  const factor = 10 ** digits;
  return Math.round(Number(value) * factor) / factor;
}

function canonicalize(value) {
  if (Array.isArray(value)) return value.map(canonicalize);
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.keys(value).sort().map(key => [key, canonicalize(value[key])]));
  }
  if (typeof value === 'number' && !Number.isFinite(value)) throw new TypeError('non_finite_canonical_value');
  return value;
}

export function stableShadowHash(payload) {
  return createHash('sha256').update(JSON.stringify(canonicalize(payload)), 'utf8').digest('hex');
}

export function ensureCertifiedShadowState(state) {
  state.economicMaintenance ||= {};
  const existing = state.economicMaintenance.certifiedShadowAttribution;
  if (!existing || typeof existing !== 'object') {
    state.economicMaintenance.certifiedShadowAttribution = {
      schemaVersion: SHADOW_SCHEMA_VERSION,
      signalObservations: [],
      trials: [],
      outcomes: [],
    };
  }
  const shadow = state.economicMaintenance.certifiedShadowAttribution;
  shadow.schemaVersion = SHADOW_SCHEMA_VERSION;
  shadow.signalObservations = Array.isArray(shadow.signalObservations) ? shadow.signalObservations : [];
  shadow.trials = Array.isArray(shadow.trials) ? shadow.trials : [];
  shadow.outcomes = Array.isArray(shadow.outcomes) ? shadow.outcomes : [];
  return shadow;
}

function certificationFrom(value) {
  if (!value || typeof value !== 'object') return null;
  return value.runtimeCertification
    || value.runtime_certification
    || value.tradePlan?.runtimeCertification
    || value.tradePlan?.runtime_certification
    || value.trade_plan?.runtimeCertification
    || value.trade_plan?.runtime_certification
    || null;
}

export function validateCertifiedShadowIdentity(certification) {
  const reasons = [];
  if (!certification || typeof certification !== 'object' || certification.certified !== true) {
    return { ok: false, reasons: ['certified_runtime_identity_required'] };
  }
  for (const field of REQUIRED_CERTIFICATION_FIELDS) {
    if (certification[field] === undefined || certification[field] === null || certification[field] === '') {
      reasons.push(`certified_runtime_identity_missing_field:${field}`);
    }
  }
  const lifecycle = certification.replay_execution_config;
  if (!lifecycle || typeof lifecycle !== 'object') {
    reasons.push('canonical_replay_lifecycle_required');
  } else {
    if (lifecycle.method !== 'canonical_replay_lifecycle_v1') reasons.push('canonical_replay_lifecycle_method_mismatch');
    if (!(finite(lifecycle.granularity_seconds, 0) > 0)) reasons.push('canonical_replay_granularity_invalid');
    if (finite(lifecycle.warmup_bars, -1) < 0) reasons.push('canonical_replay_warmup_invalid');
    if (finite(lifecycle.fee_bps, -1) < 0) reasons.push('canonical_replay_fee_invalid');
    if (finite(lifecycle.max_hold_bars, -1) < 0) reasons.push('canonical_replay_max_hold_invalid');
    if (lifecycle.exit_on_opposite_signal !== true) reasons.push('canonical_replay_opposite_exit_required');
    try {
      if (stableShadowHash(lifecycle) !== certification.replay_execution_config_hash) {
        reasons.push('canonical_replay_lifecycle_hash_mismatch');
      }
    } catch {
      reasons.push('canonical_replay_lifecycle_not_canonicalizable');
    }
  }
  return { ok: reasons.length === 0, reasons: [...new Set(reasons)] };
}

function sideFrom(value) {
  const raw = String(value || '').toUpperCase();
  if (raw === 'BUY' || raw === 'LONG') return 'BUY';
  if (raw === 'SELL' || raw === 'SHORT') return 'SELL';
  return null;
}

function marketPrice(snapshot) {
  const direct = finite(snapshot?.mid ?? snapshot?.price ?? snapshot?.close ?? snapshot?.lastPrice, null);
  if (direct != null && direct > 0) return direct;
  const bid = finite(snapshot?.bid, null);
  const ask = finite(snapshot?.ask, null);
  return bid != null && ask != null && bid > 0 && ask > 0 ? (bid + ask) / 2 : null;
}

function sideAwareFill(snapshot, side, fallback) {
  const bid = finite(snapshot?.bid, null);
  const ask = finite(snapshot?.ask, null);
  if (side === 'BUY' && ask != null && ask > 0) return ask;
  if (side === 'SELL' && bid != null && bid > 0) return bid;
  return marketPrice(snapshot) ?? fallback;
}

function snapshotsForSymbol(state, symbol) {
  return [...(state.marketDataSnapshots || [])]
    .filter(row => row.symbol === symbol || row.marketId === symbol)
    .filter(row => marketPrice(row) != null)
    .sort((a, b) => new Date(a.timestamp || a.asOf || a.createdAt || 0) - new Date(b.timestamp || b.asOf || b.createdAt || 0));
}

function latestSnapshotAtOrBefore(state, symbol, at) {
  const target = new Date(at).getTime();
  return snapshotsForSymbol(state, symbol)
    .filter(row => new Date(row.timestamp || row.asOf || row.createdAt || 0).getTime() <= target)
    .at(-1) || null;
}

function firstSnapshotAtOrAfter(state, symbol, at) {
  const target = new Date(at).getTime();
  return snapshotsForSymbol(state, symbol)
    .find(row => new Date(row.timestamp || row.asOf || row.createdAt || 0).getTime() >= target) || null;
}

function nextShadowId(prefix, rows, seed) {
  const digest = stableShadowHash(seed).slice(0, 20);
  const candidate = `${prefix}-${digest}`;
  if (!rows.some(row => row.id === candidate)) return candidate;
  return `${candidate}-${rows.length + 1}`;
}

export function recordCertifiedShadowSignalObservation(state, signal = {}, now = new Date().toISOString()) {
  const shadow = ensureCertifiedShadowState(state);
  const certification = certificationFrom(signal);
  const validation = validateCertifiedShadowIdentity(certification);
  if (!validation.ok) return { skipped: true, reason: 'uncertified_signal', validationReasons: validation.reasons };

  const symbol = String(signal.symbol || signal.product_id || certification.replay_execution_config?.dataset_symbol || '').trim();
  const action = sideFrom(signal.action || signal.side || signal.direction);
  const price = finite(signal.price ?? signal.entry_price ?? signal.trade_plan?.entry_price, null);
  if (!symbol || !action || !(price > 0)) return { skipped: true, reason: 'shadow_signal_fields_invalid' };
  if (symbol !== certification.replay_execution_config.dataset_symbol) {
    return { skipped: true, reason: 'certified_signal_outside_replay_symbol' };
  }

  const observedAt = signal.observed_at || signal.timestamp || signal.generated_at || now;
  const seed = {
    runtimeIdentityHash: certification.runtime_identity_hash,
    symbol,
    action,
    price: round(price, 10),
    observedAt,
  };
  const id = `shadow-signal-${stableShadowHash(seed).slice(0, 20)}`;
  const existing = shadow.signalObservations.find(row => row.id === id);
  if (existing) return { signalObservation: existing, idempotent: true };

  const observation = {
    id,
    runtimeIdentityHash: certification.runtime_identity_hash,
    replayExecutionConfigHash: certification.replay_execution_config_hash,
    candidateId: certification.candidate_id,
    candidateSourceSha: certification.candidate_source_sha,
    strategyName: certification.strategy_name,
    strategyConfigHash: certification.strategy_config_hash,
    symbol,
    action,
    signalPrice: round(price, 10),
    observedAt,
    source: signal.source || 'certified_strategy_scanner',
    recordedAt: now,
  };
  shadow.signalObservations.push(observation);
  return { signalObservation: observation };
}

function latestMatchingSignal(shadow, certification, symbol, side, at) {
  const ceiling = new Date(at).getTime();
  return [...shadow.signalObservations]
    .filter(row => row.runtimeIdentityHash === certification.runtime_identity_hash)
    .filter(row => row.symbol === symbol && row.action === side)
    .filter(row => new Date(row.observedAt || 0).getTime() <= ceiling)
    .sort((a, b) => new Date(b.observedAt || 0) - new Date(a.observedAt || 0))[0] || null;
}

function decisionForTrial(state, body, opportunity) {
  if (body.economicDecision && typeof body.economicDecision === 'object') return body.economicDecision;
  const id = body.economicDecisionId || opportunity?.economicDecisionId;
  return (state.economicDecisions || []).find(row => row.id === id) || null;
}

function executionCostForDecision(state, decision) {
  if (!decision?.executionCostSnapshotId) return null;
  return (state.executionCostSnapshots || []).find(row => row.id === decision.executionCostSnapshotId) || null;
}

export function createCertifiedShadowTrial(state, body = {}, now = new Date().toISOString()) {
  const shadow = ensureCertifiedShadowState(state);
  const opportunity = body.opportunity
    || (state.opportunities || []).find(row => row.id === body.opportunityId)
    || null;
  if (!opportunity) return { skipped: true, reason: 'shadow_opportunity_required' };

  const certification = body.runtimeCertification || certificationFrom(opportunity);
  const validation = validateCertifiedShadowIdentity(certification);
  if (!validation.ok) return { skipped: true, reason: 'certified_runtime_identity_required', validationReasons: validation.reasons };

  const decision = decisionForTrial(state, body, opportunity);
  if (!decision) return { skipped: true, reason: 'economic_decision_required' };
  const existing = shadow.trials.find(row => row.economicDecisionId === decision.id && row.runtimeIdentityHash === certification.runtime_identity_hash);
  if (existing) return { shadowTrial: existing, idempotent: true };

  const symbol = String(body.symbol || opportunity.symbol || '').trim();
  if (!symbol) return { skipped: true, reason: 'shadow_symbol_required' };
  if (symbol !== certification.replay_execution_config.dataset_symbol) {
    return { skipped: true, reason: 'certified_signal_outside_replay_symbol' };
  }
  const side = sideFrom(body.side || opportunity.side || opportunity.positionSide || opportunity.tradePlan?.side);
  if (!side) return { skipped: true, reason: 'shadow_side_required' };

  const cost = executionCostForDecision(state, decision);
  const referencePrice = finite(
    body.entryReferencePrice
      ?? opportunity.entryPrice
      ?? opportunity.tradePlan?.entry_price
      ?? cost?.referencePrice,
    null,
  );
  if (!(referencePrice > 0)) return { skipped: true, reason: 'shadow_entry_reference_price_required' };

  const notionalUsd = finite(body.notionalUsd ?? cost?.notionalUsd ?? opportunity.totalMoneyRisked, null);
  let quantity = finite(body.quantity ?? cost?.quantity, null);
  if (!(quantity > 0) && notionalUsd > 0) quantity = notionalUsd / referencePrice;
  if (!(quantity > 0)) return { skipped: true, reason: 'shadow_quantity_required' };

  const latestSnapshot = latestSnapshotAtOrBefore(state, symbol, now);
  const entryFillPrice = finite(body.shadowEntryFillPrice, sideAwareFill(latestSnapshot, side, referencePrice));
  if (!(entryFillPrice > 0)) return { skipped: true, reason: 'shadow_entry_fill_price_required' };

  const signal = latestMatchingSignal(shadow, certification, symbol, side, now);
  const signalObservedAt = body.signalObservedAt || signal?.observedAt || opportunity.createdAt || now;
  const signalPrice = finite(body.signalPrice ?? signal?.signalPrice, referencePrice);
  const decisionAt = decision.createdAt || now;
  const lifecycle = certification.replay_execution_config;
  const maxHoldMs = Number(lifecycle.max_hold_bars || 0) > 0
    ? Number(lifecycle.max_hold_bars) * Number(lifecycle.granularity_seconds) * 1000
    : 0;
  const maxHoldAt = maxHoldMs > 0
    ? new Date(new Date(signalObservedAt).getTime() + maxHoldMs).toISOString()
    : null;
  const entryImplementationShortfallUsd = side === 'BUY'
    ? (entryFillPrice - referencePrice) * quantity
    : (referencePrice - entryFillPrice) * quantity;
  const latencyMoveCostUsd = side === 'BUY'
    ? (referencePrice - signalPrice) * quantity
    : (signalPrice - referencePrice) * quantity;

  const trial = {
    id: nextShadowId('shadow-trial', shadow.trials, {
      economicDecisionId: decision.id,
      runtimeIdentityHash: certification.runtime_identity_hash,
    }),
    status: 'open',
    certificationScope: certification.certification_scope || 'configured_signal_only_v1',
    executionLifecycleCertified: certification.execution_lifecycle_certified === true,
    runtimeIdentityHash: certification.runtime_identity_hash,
    replayExecutionConfigHash: certification.replay_execution_config_hash,
    replayExecutionConfig: lifecycle,
    candidateId: certification.candidate_id,
    candidateSourceSha: certification.candidate_source_sha,
    strategyName: certification.strategy_name,
    strategyConfigHash: certification.strategy_config_hash,
    symbol,
    side,
    opportunityId: opportunity.id,
    economicDecisionId: decision.id,
    executionCostSnapshotId: decision.executionCostSnapshotId || null,
    signalObservedAt,
    signalPrice: round(signalPrice, 10),
    decisionAt,
    decisionLatencyMs: Math.max(0, new Date(decisionAt).getTime() - new Date(signalObservedAt).getTime()),
    entryReferencePrice: round(referencePrice, 10),
    shadowEntryFillPrice: round(entryFillPrice, 10),
    quantity: round(quantity, 12),
    notionalUsd: round(notionalUsd ?? quantity * referencePrice, 6),
    entryImplementationShortfallUsd: round(entryImplementationShortfallUsd, 6),
    latencyMoveCostUsd: round(latencyMoveCostUsd, 6),
    predictedEdgeUsd: round(decision.predictedEdgeUsd, 6),
    predictedNetExecutableEdgeUsd: round(decision.netExecutableEdgeUsd, 6),
    predictedExecutionCostsUsd: round(decision.executionCostsUsd, 6),
    predictedModelCostUsd: round(decision.modelCostUsd, 8),
    predictedLatencyDecayUsd: round(decision.latencyDecayUsd, 6),
    uncertaintyReserveUsd: round(decision.uncertaintyReserveUsd, 6),
    economicExecutionAllowed: decision.executionAllowed === true,
    maxHoldAt,
    openedAt: now,
    closedAt: null,
    outcomeId: null,
  };
  shadow.trials.push(trial);
  return { shadowTrial: trial };
}

function implementationShortfall(side, referencePrice, fillPrice, quantity) {
  if (side === 'BUY') return (fillPrice - referencePrice) * quantity;
  return (referencePrice - fillPrice) * quantity;
}

function closeTrial(state, trial, {
  exitReason,
  exitObservedAt,
  exitReferencePrice,
  shadowExitFillPrice,
  sourceSnapshotId = null,
  sourceSignalObservationId = null,
  actualExecutionCostsUsd = null,
} = {}, now = new Date().toISOString()) {
  const shadow = ensureCertifiedShadowState(state);
  if (trial.status !== 'open') {
    const existing = shadow.outcomes.find(row => row.id === trial.outcomeId);
    return { shadowOutcome: existing || null, idempotent: true };
  }
  const exitReference = finite(exitReferencePrice, null);
  const exitFill = finite(shadowExitFillPrice, exitReference);
  if (!(exitReference > 0) || !(exitFill > 0)) return { skipped: true, reason: 'shadow_exit_price_required' };

  const quantity = Number(trial.quantity);
  const referenceGrossPnlUsd = trial.side === 'BUY'
    ? (exitReference - trial.entryReferencePrice) * quantity
    : (trial.entryReferencePrice - exitReference) * quantity;
  const fillGrossPnlUsd = trial.side === 'BUY'
    ? (exitFill - trial.shadowEntryFillPrice) * quantity
    : (trial.shadowEntryFillPrice - exitFill) * quantity;
  const closeSide = trial.side === 'BUY' ? 'SELL' : 'BUY';
  const exitImplementationShortfallUsd = implementationShortfall(closeSide, exitReference, exitFill, quantity);
  const totalImplementationShortfallUsd = Number(trial.entryImplementationShortfallUsd || 0) + exitImplementationShortfallUsd;
  const canonicalFeeUsd = Number(trial.notionalUsd || (trial.entryReferencePrice * quantity))
    * Number(trial.replayExecutionConfig.fee_bps || 0) / 10000 * 2;
  const simulatedExecutionCostsUsd = totalImplementationShortfallUsd + canonicalFeeUsd;
  const canonicalShadowNetPnlUsd = referenceGrossPnlUsd - simulatedExecutionCostsUsd;
  const explicitExecutionCosts = finite(actualExecutionCostsUsd, null);
  const economicallyCostedShadowNetPnlUsd = explicitExecutionCosts == null
    ? null
    : referenceGrossPnlUsd - explicitExecutionCosts - Number(trial.predictedModelCostUsd || 0);
  const predictedNet = finite(trial.predictedNetExecutableEdgeUsd, null);
  const predictedGross = finite(trial.predictedEdgeUsd, null);
  const edgeErrorUsd = predictedNet == null ? null : canonicalShadowNetPnlUsd - predictedNet;
  const grossEdgeErrorUsd = predictedGross == null ? null : referenceGrossPnlUsd - predictedGross;
  const executionCostForecastErrorUsd = finite(trial.predictedExecutionCostsUsd, null) == null
    ? null
    : simulatedExecutionCostsUsd - Number(trial.predictedExecutionCostsUsd);
  const latencyForecastErrorUsd = finite(trial.predictedLatencyDecayUsd, null) == null
    ? null
    : Number(trial.latencyMoveCostUsd || 0) - Number(trial.predictedLatencyDecayUsd);

  const outcome = {
    id: nextShadowId('shadow-outcome', shadow.outcomes, { trialId: trial.id, exitObservedAt, exitReason }),
    trialId: trial.id,
    runtimeIdentityHash: trial.runtimeIdentityHash,
    replayExecutionConfigHash: trial.replayExecutionConfigHash,
    candidateId: trial.candidateId,
    strategyName: trial.strategyName,
    symbol: trial.symbol,
    side: trial.side,
    exitReason,
    exitObservedAt: exitObservedAt || now,
    exitReferencePrice: round(exitReference, 10),
    shadowExitFillPrice: round(exitFill, 10),
    referenceGrossPnlUsd: round(referenceGrossPnlUsd, 6),
    fillGrossPnlUsd: round(fillGrossPnlUsd, 6),
    canonicalReplayFeeUsd: round(canonicalFeeUsd, 6),
    entryImplementationShortfallUsd: round(trial.entryImplementationShortfallUsd, 6),
    exitImplementationShortfallUsd: round(exitImplementationShortfallUsd, 6),
    totalImplementationShortfallUsd: round(totalImplementationShortfallUsd, 6),
    simulatedExecutionCostsUsd: round(simulatedExecutionCostsUsd, 6),
    canonicalShadowNetPnlUsd: round(canonicalShadowNetPnlUsd, 6),
    actualExecutionCostsUsd: round(explicitExecutionCosts, 6),
    economicallyCostedShadowNetPnlUsd: round(economicallyCostedShadowNetPnlUsd, 6),
    predictedEdgeUsd: predictedGross == null ? null : round(predictedGross, 6),
    predictedNetExecutableEdgeUsd: predictedNet == null ? null : round(predictedNet, 6),
    edgeErrorUsd: round(edgeErrorUsd, 6),
    absoluteEdgeErrorUsd: edgeErrorUsd == null ? null : round(Math.abs(edgeErrorUsd), 6),
    grossEdgeErrorUsd: round(grossEdgeErrorUsd, 6),
    edgeCaptureRatio: predictedNet && predictedNet !== 0 ? round(canonicalShadowNetPnlUsd / predictedNet, 6) : null,
    predictionSignCorrect: predictedNet == null || predictedNet === 0
      ? null
      : (predictedNet > 0) === (canonicalShadowNetPnlUsd > 0),
    predictedExecutionCostsUsd: round(trial.predictedExecutionCostsUsd, 6),
    executionCostForecastErrorUsd: round(executionCostForecastErrorUsd, 6),
    decisionLatencyMs: trial.decisionLatencyMs,
    latencyMoveCostUsd: round(trial.latencyMoveCostUsd, 6),
    predictedLatencyDecayUsd: round(trial.predictedLatencyDecayUsd, 6),
    latencyForecastErrorUsd: round(latencyForecastErrorUsd, 6),
    uncertaintyReserveUsd: round(trial.uncertaintyReserveUsd, 6),
    economicExecutionAllowed: trial.economicExecutionAllowed,
    sourceSnapshotId,
    sourceSignalObservationId,
    createdAt: now,
  };
  shadow.outcomes.push(outcome);
  trial.status = 'closed';
  trial.closedAt = outcome.exitObservedAt;
  trial.outcomeId = outcome.id;
  trial.exitReason = exitReason;
  return { shadowOutcome: outcome };
}

export function recordCertifiedShadowOutcome(state, body = {}, now = new Date().toISOString()) {
  const shadow = ensureCertifiedShadowState(state);
  const trial = shadow.trials.find(row => row.id === body.trialId);
  if (!trial) return { errors: ['certified_shadow_trial_not_found'] };
  return closeTrial(state, trial, {
    exitReason: body.exitReason || 'manual_shadow_outcome',
    exitObservedAt: body.exitObservedAt || now,
    exitReferencePrice: body.exitReferencePrice ?? body.exitPrice,
    shadowExitFillPrice: body.shadowExitFillPrice ?? body.exitFillPrice ?? body.exitPrice,
    sourceSnapshotId: body.sourceSnapshotId || null,
    sourceSignalObservationId: body.sourceSignalObservationId || null,
    actualExecutionCostsUsd: body.actualExecutionCostsUsd,
  }, now);
}

export function matureCertifiedShadowTrials(state, now = new Date().toISOString()) {
  const shadow = ensureCertifiedShadowState(state);
  const outcomes = [];
  const pending = [];
  const nowMs = new Date(now).getTime();
  for (const trial of shadow.trials.filter(row => row.status === 'open')) {
    const startMs = new Date(trial.signalObservedAt || trial.openedAt || 0).getTime();
    const oppositeAction = trial.side === 'BUY' ? 'SELL' : 'BUY';
    const opposite = shadow.signalObservations
      .filter(row => row.runtimeIdentityHash === trial.runtimeIdentityHash)
      .filter(row => row.symbol === trial.symbol && row.action === oppositeAction)
      .filter(row => new Date(row.observedAt || 0).getTime() > startMs)
      .sort((a, b) => new Date(a.observedAt || 0) - new Date(b.observedAt || 0))[0] || null;

    let exitReason = null;
    let observedAt = null;
    let referencePrice = null;
    let fillPrice = null;
    let sourceSnapshotId = null;
    let sourceSignalObservationId = null;

    if (opposite) {
      exitReason = 'opposite_signal';
      observedAt = opposite.observedAt;
      referencePrice = opposite.signalPrice;
      const snapshot = firstSnapshotAtOrAfter(state, trial.symbol, observedAt)
        || latestSnapshotAtOrBefore(state, trial.symbol, observedAt);
      const closeSide = trial.side === 'BUY' ? 'SELL' : 'BUY';
      fillPrice = sideAwareFill(snapshot, closeSide, referencePrice);
      sourceSnapshotId = snapshot?.id || null;
      sourceSignalObservationId = opposite.id;
    } else if (trial.maxHoldAt && new Date(trial.maxHoldAt).getTime() <= nowMs) {
      const snapshot = firstSnapshotAtOrAfter(state, trial.symbol, trial.maxHoldAt);
      if (!snapshot) {
        pending.push({ trialId: trial.id, blocker: 'post_max_hold_market_snapshot_required' });
        continue;
      }
      exitReason = 'max_hold';
      observedAt = snapshot.timestamp || snapshot.asOf || snapshot.createdAt || now;
      referencePrice = marketPrice(snapshot);
      const closeSide = trial.side === 'BUY' ? 'SELL' : 'BUY';
      fillPrice = sideAwareFill(snapshot, closeSide, referencePrice);
      sourceSnapshotId = snapshot.id || null;
    } else {
      pending.push({ trialId: trial.id, blocker: 'canonical_exit_not_reached' });
      continue;
    }

    const result = closeTrial(state, trial, {
      exitReason,
      exitObservedAt: observedAt,
      exitReferencePrice: referencePrice,
      shadowExitFillPrice: fillPrice,
      sourceSnapshotId,
      sourceSignalObservationId,
    }, now);
    if (result.shadowOutcome) outcomes.push(result.shadowOutcome);
    else pending.push({ trialId: trial.id, blocker: result.reason || 'shadow_close_failed' });
  }
  return { shadowOutcomes: outcomes, pendingShadowTrials: pending };
}

function summarizeRows(rows) {
  if (!rows.length) {
    return {
      samples: 0,
      predictedNetExecutableEdgeUsd: 0,
      canonicalShadowNetPnlUsd: 0,
      edgeErrorUsd: null,
      meanAbsoluteEdgeErrorUsd: null,
      signAccuracy: null,
      shadowWinRate: null,
      edgeCaptureRatio: null,
      executionCostForecastErrorUsd: null,
      latencyForecastErrorUsd: null,
    };
  }
  const sum = (key) => rows.reduce((total, row) => total + Number(row[key] || 0), 0);
  const signRows = rows.filter(row => row.predictionSignCorrect !== null);
  const predicted = sum('predictedNetExecutableEdgeUsd');
  const actual = sum('canonicalShadowNetPnlUsd');
  return {
    samples: rows.length,
    predictedNetExecutableEdgeUsd: round(predicted, 6),
    canonicalShadowNetPnlUsd: round(actual, 6),
    edgeErrorUsd: round(actual - predicted, 6),
    meanAbsoluteEdgeErrorUsd: round(rows.reduce((total, row) => total + Number(row.absoluteEdgeErrorUsd || 0), 0) / rows.length, 6),
    signAccuracy: signRows.length ? round(signRows.filter(row => row.predictionSignCorrect).length / signRows.length, 6) : null,
    shadowWinRate: round(rows.filter(row => Number(row.canonicalShadowNetPnlUsd) > 0).length / rows.length, 6),
    edgeCaptureRatio: predicted !== 0 ? round(actual / predicted, 6) : null,
    executionCostForecastErrorUsd: round(sum('executionCostForecastErrorUsd'), 6),
    latencyForecastErrorUsd: round(sum('latencyForecastErrorUsd'), 6),
  };
}

export function summarizeCertifiedShadowAttribution(state) {
  const shadow = ensureCertifiedShadowState(state);
  const byIdentity = {};
  const bySymbol = {};
  for (const row of shadow.outcomes) {
    (byIdentity[row.runtimeIdentityHash] ||= []).push(row);
    (bySymbol[row.symbol] ||= []).push(row);
  }
  return {
    ...summarizeRows(shadow.outcomes),
    openTrials: shadow.trials.filter(row => row.status === 'open').length,
    closedTrials: shadow.trials.filter(row => row.status === 'closed').length,
    signalObservations: shadow.signalObservations.length,
    byRuntimeIdentity: Object.fromEntries(Object.entries(byIdentity).map(([key, rows]) => [key, summarizeRows(rows)])),
    bySymbol: Object.fromEntries(Object.entries(bySymbol).map(([key, rows]) => [key, summarizeRows(rows)])),
  };
}

export function certifiedShadowSnapshot(state) {
  const shadow = ensureCertifiedShadowState(state);
  return {
    schemaVersion: shadow.schemaVersion,
    signalObservations: shadow.signalObservations,
    trials: shadow.trials,
    outcomes: shadow.outcomes,
    summary: summarizeCertifiedShadowAttribution(state),
  };
}
