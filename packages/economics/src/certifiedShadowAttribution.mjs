import * as legacy from './certifiedShadowAttributionLegacy.mjs';

export * from './certifiedShadowAttributionLegacy.mjs';

function certificationFromOpportunity(opportunity = {}) {
  return opportunity?.runtimeCertification
    || opportunity?.runtime_certification
    || opportunity?.tradePlan?.runtimeCertification
    || opportunity?.tradePlan?.runtime_certification
    || null;
}

function resolveOpportunity(state, body = {}) {
  return body.opportunity
    || (state.opportunities || []).find(row => row.id === body.opportunityId)
    || null;
}

function resolveSignalObservation(state, opportunity, body = {}) {
  const shadow = legacy.ensureCertifiedShadowState(state);
  const observationId = body.certifiedShadowSignalObservationId
    || opportunity?.certifiedShadowSignalObservationId
    || null;
  if (!observationId) return null;
  return shadow.signalObservations.find(row => row.id === observationId) || null;
}

function openCanonicalTrial(state, certification, symbol) {
  const shadow = legacy.ensureCertifiedShadowState(state);
  return shadow.trials.find(row =>
    row.status === 'open'
    && row.runtimeIdentityHash === certification.runtime_identity_hash
    && row.symbol === symbol,
  ) || null;
}

export function createCertifiedShadowTrial(state, body = {}, now = new Date().toISOString()) {
  const opportunity = resolveOpportunity(state, body);
  if (!opportunity) return { skipped: true, reason: 'shadow_opportunity_required' };

  const certification = body.runtimeCertification || certificationFromOpportunity(opportunity);
  const validation = legacy.validateCertifiedShadowIdentity(certification);
  if (!validation.ok) {
    return {
      skipped: true,
      reason: 'certified_runtime_identity_required',
      validationReasons: validation.reasons,
    };
  }

  const observation = resolveSignalObservation(state, opportunity, body);
  if (!observation) {
    return { skipped: true, reason: 'certified_shadow_signal_observation_required' };
  }
  if (observation.runtimeIdentityHash !== certification.runtime_identity_hash) {
    return { skipped: true, reason: 'certified_shadow_signal_identity_mismatch' };
  }
  if (observation.replayExecutionConfigHash !== certification.replay_execution_config_hash) {
    return { skipped: true, reason: 'certified_shadow_replay_lifecycle_mismatch' };
  }
  if (observation.symbol !== opportunity.symbol) {
    return { skipped: true, reason: 'certified_shadow_signal_symbol_mismatch' };
  }
  if (!['BUY', 'SELL'].includes(observation.action)) {
    return { skipped: true, reason: 'certified_shadow_signal_action_invalid' };
  }

  // Canonical replay owns exactly one position for a runtime identity + symbol.
  // Repeated same-side signals while open do not create additional positions.
  // An opposite signal closes the existing position and does not reverse/open a
  // new position on that same signal bar.
  const openTrial = openCanonicalTrial(state, certification, observation.symbol);
  if (openTrial) {
    if (openTrial.side === observation.action) {
      return {
        skipped: true,
        reason: 'canonical_position_already_open',
        existingShadowTrialId: openTrial.id,
      };
    }
    const matured = legacy.matureCertifiedShadowTrials(state, now);
    const closedShadowOutcome = matured.shadowOutcomes.find(row => row.trialId === openTrial.id) || null;
    return {
      skipped: true,
      reason: 'canonical_exit_signal_not_entry',
      existingShadowTrialId: openTrial.id,
      closedShadowOutcome,
      pendingShadowTrials: matured.pendingShadowTrials,
    };
  }

  // Canonical replay treats BUY/SELL as the position-opening direction while
  // flat. Do not reuse the scanner opportunity's heuristic TP/SL lifecycle:
  // that path can label SELL as an exit-long and would corrupt shadow P&L.
  return legacy.createCertifiedShadowTrial(state, {
    ...body,
    opportunity,
    runtimeCertification: certification,
    certifiedShadowSignalObservationId: observation.id,
    side: observation.action,
    signalObservedAt: observation.observedAt,
    signalPrice: observation.signalPrice,
    symbol: observation.symbol,
  }, now);
}
