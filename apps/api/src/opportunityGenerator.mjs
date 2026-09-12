import * as legacy from './opportunityGeneratorLegacy.mjs';
import { createOpportunity, ensureOpportunityState } from './opportunityFlows.mjs';
import { recordCertifiedShadowSignalObservation } from '../../../packages/economics/src/certifiedShadowAttribution.mjs';

export * from './opportunityGeneratorLegacy.mjs';

const CERTIFIED_SHADOW_SCOPE = 'certified_raw_signal_v1';

function certificationFromSignal(signal = {}) {
  return signal?.runtimeCertification
    || signal?.runtime_certification
    || signal?.tradePlan?.runtimeCertification
    || signal?.tradePlan?.runtime_certification
    || signal?.trade_plan?.runtimeCertification
    || signal?.trade_plan?.runtime_certification
    || null;
}

function isCertifiedShadowMeasurementSignal(signal = {}) {
  const certification = certificationFromSignal(signal);
  return signal?.shadow_only === true
    && signal?.measurement_scope === CERTIFIED_SHADOW_SCOPE
    && certification?.certified === true
    && certification?.shadow_measurement_scope === CERTIFIED_SHADOW_SCOPE;
}

function attachObservationToOpportunity(opportunity, signal, observation, status = 'signal_observed') {
  if (!opportunity || !observation) return;
  const certification = certificationFromSignal(signal);
  opportunity.certifiedShadowSignalObservationId = observation.id;
  opportunity.runtimeIdentityHash = observation.runtimeIdentityHash;
  opportunity.replayExecutionConfigHash = observation.replayExecutionConfigHash;
  opportunity.deploymentBundleHash = certification?.deployment_bundle_hash || null;
  opportunity.shadowMeasurementStatus = status;
}

function isDuplicateStrategyOpportunity(state, signal) {
  const symbol = signal?.symbol || signal?.product_id;
  return state.opportunities.some(opp =>
    opp.symbol === symbol
    && opp.evidence?.some(e => e.type === 'strategy_live_30d_test' && e.strategy === signal.strategy)
    && ['needs_review', 'approved', 'research_requested', 'deferred'].includes(opp.status)
  );
}

function shadowMeasurementBusy(state, signal) {
  const symbol = signal?.symbol || signal?.product_id;
  const runtimeIdentityHash = certificationFromSignal(signal)?.runtime_identity_hash;
  if (!symbol || !runtimeIdentityHash) return true;
  const shadow = state.economicMaintenance?.certifiedShadowAttribution;
  const openTrial = Array.isArray(shadow?.trials) && shadow.trials.some(row =>
    row.status === 'open'
    && row.symbol === symbol
    && row.runtimeIdentityHash === runtimeIdentityHash
  );
  if (openTrial) return true;
  return state.opportunities.some(row =>
    row.symbol === symbol
    && row.runtimeIdentityHash === runtimeIdentityHash
    && row.shadowMeasurementStatus === 'awaiting_economics'
  );
}

function certifiedShadowOpportunityInput(signal, options = {}) {
  const symbol = signal.symbol || signal.product_id;
  const price = Number(signal.price || signal.entry_price || 0);
  const confidence = Number(signal.raw_confidence ?? signal.weighted_confidence ?? 0.5);
  const action = String(signal.action || '').toUpperCase();
  const certification = certificationFromSignal(signal);
  const notional = Number(options.shadowNotionalUsd || options.shadow_notional_usd || 1000);
  const validNotional = Number.isFinite(notional) && notional > 0 ? notional : 1000;
  const tradePlan = {
    ...(signal.trade_plan && typeof signal.trade_plan === 'object' ? signal.trade_plan : {}),
    plan_type: 'shadow_measurement',
    execution_purpose: 'shadow_only',
    position_side: action === 'SELL' ? 'short' : 'long',
    entry_price: price,
    runtime_certification: certification,
  };
  return {
    sourceAgentId: 'certified-shadow-worker',
    marketType: 'crypto_spot_shadow',
    venue: 'coinbase-paper',
    symbol,
    marketSlug: String(symbol || '').toLowerCase().replace(/[^a-z0-9]+/g, '-'),
    title: `${symbol} ${action} — certified forward shadow measurement`,
    recommendation: 'shadow_measurement_only',
    confidenceScore: Number.isFinite(confidence) ? Math.max(0, Math.min(1, confidence)) : 0.5,
    winProbability: 0.5,
    lossProbability: 0.5,
    expectedValue: 0,
    grossExpectedValue: 0,
    totalMoneyRisked: validNotional,
    maxLoss: 0,
    potentialUpside: 0,
    liquidityScore: 50,
    dataFreshnessScore: 100,
    backtestStatus: 'certified_forward_shadow_unexecuted',
    estimatedFees: 0,
    estimatedSlippage: 0,
    estimatedGas: 0,
    agentResearchCost: 0,
    modelInferenceCost: 0,
    tradeIntent: 'shadow_measurement',
    executionPurpose: 'shadow_only',
    positionSide: action === 'SELL' ? 'short' : 'long',
    entryPrice: price,
    tradePlan,
    executionAdmission: {
      policy: 'certified_shadow_measurement_only',
      status: 'measurement_only',
      autoDraftEligible: false,
      researchCertification: 'certified_signal',
      reason: 'shadow_measurement_has_no_execution_authority',
    },
    status: 'rejected',
    approvalStatus: 'rejected',
    notes: 'Forward certified shadow measurement only. No execution draft, order, allocation, or capital authority.',
    evidence: [{
      type: 'certified_forward_shadow_signal',
      measurementScope: CERTIFIED_SHADOW_SCOPE,
      strategy: signal.strategy,
      action,
      observedAt: signal.observed_at || signal.generated_at || null,
      canonicalBarStart: signal.canonical_bar_start || null,
      canonicalBarClosedAt: signal.canonical_bar_closed_at || null,
      runtimeIdentityHash: certification?.runtime_identity_hash || null,
      replayExecutionConfigHash: certification?.replay_execution_config_hash || null,
      deploymentBundleHash: certification?.deployment_bundle_hash || null,
      sameWindowScreenApplied: false,
      executionCanaryApplied: false,
    }],
  };
}

export function generateOpportunitiesFromStrategyScan(state, scan = {}, options = {}) {
  ensureOpportunityState(state);
  const rawSignals = Array.isArray(scan.signals) ? scan.signals : [];
  const errors = Array.isArray(scan.errors) ? [...scan.errors] : [];
  const created = [];
  const shadowSignalObservations = [];
  const shadowSignalSkips = [];
  const observedAt = options.now || new Date().toISOString();

  // Record every verified certified observation first, even when an open trial
  // means no new measurement candidate should be created. Canonical opposite
  // signals need to reach the shadow ledger in order to close that trial.
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
    const measurementOnly = isCertifiedShadowMeasurementSignal(signal);
    if (measurementOnly && shadowMeasurementBusy(state, signal)) continue;
    if (!measurementOnly && isDuplicateStrategyOpportunity(state, signal)) continue;

    const strategyId = state.strategies.some(strategy => strategy.id === signal.strategy)
      ? signal.strategy
      : null;
    const opportunityInput = measurementOnly
      ? certifiedShadowOpportunityInput(signal, options)
      : {
          ...legacy.strategySignalToOpportunityInput(signal),
          status: 'needs_review',
          approvalStatus: 'needs_review',
        };
    const opportunityResult = createOpportunity(state, {
      ...opportunityInput,
      strategyId,
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

    const certification = certificationFromSignal(signal);
    const observation = shadowSignalObservations.find(row =>
      row.symbol === symbol
      && row.strategyName === signal.strategy
      && row.runtimeIdentityHash === certification?.runtime_identity_hash
      && row.observedAt === (signal.observed_at || signal.timestamp || signal.generated_at || observedAt)
    ) || shadowSignalObservations.find(row =>
      row.symbol === symbol
      && row.strategyName === signal.strategy
      && row.runtimeIdentityHash === certification?.runtime_identity_hash
    );
    attachObservationToOpportunity(
      opportunityResult.opportunity,
      signal,
      observation,
      measurementOnly ? 'awaiting_economics' : 'signal_observed',
    );
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
  // Production workers can perform Python/network work before taking the
  // serializable mutation lock and pass the immutable result here.
  const scan = options.scanResult || legacy.runStrategySignalScanner(options);
  return generateOpportunitiesFromStrategyScan(state, scan, options);
}
