import * as legacy from './opportunityGeneratorLegacy.mjs';
import { createOpportunity, ensureOpportunityState } from './opportunityFlows.mjs';
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

function isDuplicateStrategyOpportunity(state, signal) {
  const symbol = signal?.symbol || signal?.product_id;
  return state.opportunities.some(opp =>
    opp.symbol === symbol
    && opp.evidence?.some(e => e.type === 'strategy_live_30d_test' && e.strategy === signal.strategy)
    && ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status)
  );
}

export function generateOpportunitiesFromStrategyScan(state, scan = {}, options = {}) {
  ensureOpportunityState(state);
  const rawSignals = Array.isArray(scan.signals) ? scan.signals : [];
  const errors = Array.isArray(scan.errors) ? [...scan.errors] : [];
  const created = [];
  const shadowSignalObservations = [];
  const shadowSignalSkips = [];
  const observedAt = options.now || new Date().toISOString();

  // Record every verified certified observation, including an opposite signal
  // that does not create another opportunity because an existing review object
  // is still open. Canonical shadow exits depend on those observations.
  for (const signal of rawSignals) {
    const recorded = recordCertifiedShadowSignalObservation(state, signal, observedAt);
    if (recorded?.signalObservation) {
      shadowSignalObservations.push(recorded.signalObservation);
    } else if (recorded?.reason && recorded.reason !== 'uncertified_signal') {
      shadowSignalSkips.push({
        symbol: signal?.symbol || signal?.product_id || null,
        strategy: signal?.strategy || null,
        reason: recorded.reason,
        validationReasons: recorded.validationReasons || [],
      });
    }
  }

  for (const signal of rawSignals) {
    const symbol = signal.symbol || signal.product_id;
    if (!symbol || !signal.strategy) continue;
    if (isDuplicateStrategyOpportunity(state, signal)) continue;

    const strategyId = state.strategies.some(strategy => strategy.id === signal.strategy)
      ? signal.strategy
      : null;
    const opportunityResult = createOpportunity(state, {
      ...legacy.strategySignalToOpportunityInput(signal),
      strategyId,
      status: 'needs_review',
      approvalStatus: 'needs_review',
    });
    if (opportunityResult.errors) {
      errors.push({
        symbol,
        strategy: signal.strategy,
        code: 'opportunity_generation_failed',
        errors: opportunityResult.errors,
      });
      continue;
    }

    const observation = shadowSignalObservations.find(row =>
      row.symbol === symbol
      && row.strategyName === signal.strategy
      && row.runtimeIdentityHash === certificationFromSignal(signal)?.runtime_identity_hash
    );
    attachObservationToOpportunity([opportunityResult.opportunity], signal, observation);
    created.push(opportunityResult.opportunity);
  }

  return {
    scan,
    signals: created,
    executions: [],
    errors,
    shadowSignalObservations,
    shadowSignalSkips,
  };
}

export async function generateOpportunitiesFromStrategySignals(state, options = {}) {
  // Production workers can perform the Python/network scan before taking the
  // serializable mutation lock and pass the immutable result here.
  const scan = options.scanResult || legacy.runStrategySignalScanner(options);
  return generateOpportunitiesFromStrategyScan(state, scan, options);
}
