import * as legacy from './opportunityGeneratorLegacy.mjs';
import { recordCertifiedShadowSignalObservation } from '../../../packages/economics/src/certifiedShadowAttribution.mjs';

export * from './opportunityGeneratorLegacy.mjs';

function certificationFromSignal(signal = {}) {
  return signal?.runtimeCertification
    || signal?.runtime_certification
    || signal?.tradePlan?.runtimeCertification
    || signal?.tradePlan?.runtime_certification
    || signal?.trade_plan?.runtimeCertification
    || signal?.trade_plan?.runtime_certification
    || null;
}

function attachObservationToOpportunity(opportunities = [], signal, observation) {
  if (!observation) return;
  const symbol = signal?.symbol || signal?.product_id;
  const strategy = signal?.strategy;
  const certification = certificationFromSignal(signal);
  const matching = opportunities.find(row =>
    row?.symbol === symbol
    && (!strategy || row?.strategyId === strategy || row?.tradePlan?.strategy === strategy)
    && (!certification?.runtime_identity_hash
      || row?.tradePlan?.runtime_certification?.runtime_identity_hash === certification.runtime_identity_hash),
  );
  if (!matching) return;
  matching.certifiedShadowSignalObservationId = observation.id;
  matching.runtimeIdentityHash = observation.runtimeIdentityHash;
  matching.replayExecutionConfigHash = observation.replayExecutionConfigHash;
  matching.shadowMeasurementStatus = 'signal_observed';
}

export async function generateOpportunitiesFromStrategySignals(state, options = {}) {
  const result = await legacy.generateOpportunitiesFromStrategySignals(state, options);
  const rawSignals = Array.isArray(result?.scan?.signals) ? result.scan.signals : [];
  const opportunities = Array.isArray(result?.signals) ? result.signals : [];
  const shadowSignalObservations = [];
  const shadowSignalSkips = [];
  const observedAt = options.now || new Date().toISOString();

  for (const signal of rawSignals) {
    const recorded = recordCertifiedShadowSignalObservation(state, signal, observedAt);
    if (recorded?.signalObservation) {
      shadowSignalObservations.push(recorded.signalObservation);
      attachObservationToOpportunity(opportunities, signal, recorded.signalObservation);
    } else if (recorded?.reason && recorded.reason !== 'uncertified_signal') {
      shadowSignalSkips.push({
        symbol: signal?.symbol || signal?.product_id || null,
        strategy: signal?.strategy || null,
        reason: recorded.reason,
        validationReasons: recorded.validationReasons || [],
      });
    }
  }

  return {
    ...result,
    shadowSignalObservations,
    shadowSignalSkips,
  };
}
