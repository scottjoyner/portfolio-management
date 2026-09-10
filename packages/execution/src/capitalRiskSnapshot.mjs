import { evaluateRisk } from '../../risk/src/engine.mjs';
import { stableHash } from './overseer.mjs';

export const CAPITAL_RISK_SCHEMA_VERSION = 1;
export const CAPITAL_RISK_POLICY_VERSION = 'canonical-capital-risk-v1';
export const DEFAULT_AUTHORITATIVE_STATE_MAX_AGE_MS = 30_000;
export const DEFAULT_MARKET_DATA_MAX_AGE_MS = 120_000;
export const DEFAULT_MIN_EDGE_BPS = 1;

const PENDING_EXECUTION_STATUSES = new Set(['draft', 'approved', 'submitted', 'partially_filled']);

function finite(value, fallback = null) {
  if (value === null || value === undefined || value === '') return fallback;
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function timestamp(value) {
  if (value === null || value === undefined || value === '') return null;
  const ms = new Date(value).getTime();
  return Number.isFinite(ms) ? ms : null;
}

function iso(value) {
  const ms = timestamp(value);
  return ms == null ? null : new Date(ms).toISOString();
}

function unique(values = []) {
  return [...new Set(values.filter(Boolean))];
}

function clone(value) {
  if (value === undefined) return undefined;
  if (typeof structuredClone === 'function') return structuredClone(value);
  return JSON.parse(JSON.stringify(value));
}

function latestByTimestamp(rows = []) {
  return [...rows].sort((a, b) => (timestamp(b?.timestamp ?? b?.asOf ?? b?.updatedAt ?? b?.createdAt) || 0)
    - (timestamp(a?.timestamp ?? a?.asOf ?? a?.updatedAt ?? a?.createdAt) || 0))[0] || null;
}

function paperVenueCompatible(intentVenue, marketVenue, mode) {
  const left = String(intentVenue || '').toLowerCase();
  const right = String(marketVenue || '').toLowerCase();
  if (!left || !right || left === right) return true;
  if (!['paper', 'demo'].includes(String(mode || '').toLowerCase())) return false;
  if (left === 'paper' || left === 'demo') return true;
  const normalize = value => value.replace(/-paper$|-demo$/, '');
  return normalize(left) === normalize(right);
}

function marketReferencePrice(snapshot) {
  const direct = finite(snapshot?.mid ?? snapshot?.price ?? snapshot?.close ?? snapshot?.lastPrice, null);
  if (direct != null && direct > 0) return direct;
  const bid = finite(snapshot?.bid, null);
  const ask = finite(snapshot?.ask, null);
  if (bid != null && ask != null && bid > 0 && ask > 0) return (bid + ask) / 2;
  return null;
}

function requestedQuantity(intent) {
  const direct = finite(intent?.quantity, null);
  if (direct != null && direct > 0) return direct;
  const quantities = (intent?.orders || []).map(order => finite(order?.quantity, null));
  if (quantities.length && quantities.every(value => value != null && value > 0)) {
    return quantities.reduce((sum, value) => sum + value, 0);
  }
  return null;
}

function pendingExecutionNotional(execution, marketPrice) {
  const direct = finite(execution?.notional ?? execution?.notionalUsd, null);
  if (direct != null && direct >= 0) return direct;
  const orders = Array.isArray(execution?.orders) ? execution.orders : [];
  let total = 0;
  for (const order of orders) {
    const quantity = finite(order?.quantity, null);
    const price = finite(order?.price, marketPrice);
    if (quantity == null || quantity < 0 || price == null || price <= 0) return null;
    total += quantity * price;
  }
  return orders.length ? total : null;
}

function positionQuantity(position) {
  return finite(position?.quantity ?? position?.qty, null);
}

function executionMatchesIntent(execution, intent, excludeExecutionId) {
  if (!execution || execution.id === excludeExecutionId) return false;
  if (intent?.symbol && execution.symbol && execution.symbol !== intent.symbol) return false;
  if (intent?.accountId && execution.accountId && execution.accountId !== intent.accountId) return false;
  return true;
}

function sourceRecord(sourceType, sourceId, observedAt, value, extra = {}) {
  return {
    sourceType,
    sourceId: sourceId ?? null,
    observedAt: observedAt ?? null,
    sourceHash: stableHash(value),
    ...extra,
  };
}

function reconciliationState(state, intent, excludeExecutionId, observedAt) {
  if (!Array.isArray(state?.executions) || !Array.isArray(state?.audit)) {
    return {
      available: false,
      unresolved: true,
      issues: ['reconciliation_state_unavailable'],
      relevantExecutionIds: [],
      source: null,
    };
  }

  const relevant = state.executions.filter(execution => executionMatchesIntent(execution, intent, excludeExecutionId));
  const relevantIds = new Set(relevant.map(row => row.id).filter(Boolean));
  const issues = [];

  for (const execution of relevant) {
    const fills = Array.isArray(execution.fills) ? execution.fills : [];
    if (fills.some(fill => String(fill?.settlementStatus || '').toLowerCase() === 'pending')) {
      issues.push(`pending_settlement:${execution.id}`);
    }
    if (String(execution.status || '').toLowerCase() === 'filled') {
      const expected = (execution.orders || []).reduce((sum, order) => sum + Math.max(0, finite(order?.quantity, 0)), 0);
      const actual = fills.reduce((sum, fill) => sum + Math.max(0, finite(fill?.quantity, 0)), 0);
      if (Math.abs(expected - actual) > 0.0001) issues.push(`fill_quantity_mismatch:${execution.id}`);
    }
  }

  const latestReconByExecution = new Map();
  for (const event of state.audit) {
    if (!['execution_reconciled', 'execution_recon_issues'].includes(event?.action)) continue;
    const executionId = event?.details ?? event?.payload?.executionId ?? null;
    if (!executionId || !relevantIds.has(executionId)) continue;
    const eventAt = timestamp(event?.at ?? event?.timestamp ?? event?.createdAt);
    const current = latestReconByExecution.get(executionId);
    if (!current || (eventAt || 0) >= (current.at || 0)) {
      latestReconByExecution.set(executionId, { action: event.action, at: eventAt || 0 });
    }
  }
  for (const [executionId, event] of latestReconByExecution) {
    if (event.action === 'execution_recon_issues') issues.push(`reconciliation_issue:${executionId}`);
  }

  const facts = {
    relevantExecutionIds: [...relevantIds].sort(),
    issues: unique(issues).sort(),
    latestReconciliationActions: [...latestReconByExecution.entries()]
      .map(([executionId, event]) => ({ executionId, action: event.action, at: event.at ? new Date(event.at).toISOString() : null }))
      .sort((a, b) => a.executionId.localeCompare(b.executionId)),
  };
  return {
    available: true,
    unresolved: facts.issues.length > 0,
    issues: facts.issues,
    relevantExecutionIds: facts.relevantExecutionIds,
    source: sourceRecord('execution_reconciliation', 'operator-state:executions+audit', observedAt, facts),
  };
}

function economicEdgeState(state, intent, orderNotionalUsd, nowMs, policy) {
  const required = Boolean(
    intent?.sourceAgentId
    || intent?.economicDecisionId
    || intent?.modelQuoteId
    || intent?.forecastId
    || intent?.executionCostSnapshotId
    || intent?.netExecutableEdgeUsd != null
  );
  const minEdgeBps = finite(state?.config?.minExecutionEdgeBps, finite(policy.minEdgeBps, DEFAULT_MIN_EDGE_BPS));
  if (!required) {
    return {
      required: false,
      verified: true,
      edgeBps: 0,
      minEdgeBps: 0,
      netExecutableEdgeUsd: null,
      decisionId: null,
      blockers: [],
      sources: [sourceRecord('risk_policy', 'manual-paper-edge-policy', new Date(nowMs).toISOString(), { edgeRequired: false })],
    };
  }

  const blockers = [];
  const decision = (state?.economicDecisions || []).find(row => row?.id === intent?.economicDecisionId) || null;
  if (!decision) blockers.push('economic_decision_required');
  if (decision && decision.executionAllowed !== true) blockers.push('economic_decision_execution_not_allowed');
  if (decision?.symbol && intent?.symbol && decision.symbol !== intent.symbol) blockers.push('economic_decision_symbol_mismatch');
  if (intent?.forecastId && decision?.forecastId !== intent.forecastId) blockers.push('economic_decision_forecast_mismatch');
  if (intent?.executionCostSnapshotId && decision?.executionCostSnapshotId !== intent.executionCostSnapshotId) blockers.push('economic_decision_cost_snapshot_mismatch');
  if (intent?.modelQuoteId && decision?.modelQuoteId !== intent.modelQuoteId) blockers.push('economic_decision_model_quote_mismatch');

  const forecast = decision?.forecastId
    ? (state?.priceForecasts || []).find(row => row?.id === decision.forecastId) || null
    : null;
  const executionCost = decision?.executionCostSnapshotId
    ? (state?.executionCostSnapshots || []).find(row => row?.id === decision.executionCostSnapshotId) || null
    : null;
  const quote = decision?.modelQuoteId
    ? (state?.modelUsageLedger || []).find(row => row?.id === decision.modelQuoteId) || null
    : null;

  if (!forecast) blockers.push('economic_forecast_required');
  else {
    if (forecast.status !== 'valid' || (timestamp(forecast.expiresAt) || 0) < nowMs) blockers.push('economic_forecast_stale');
    if (forecast.symbol && intent?.symbol && forecast.symbol !== intent.symbol) blockers.push('economic_forecast_symbol_mismatch');
  }
  if (!executionCost) blockers.push('economic_execution_cost_required');
  else if ((timestamp(executionCost.validUntil) || 0) < nowMs) blockers.push('economic_execution_cost_stale');
  if (decision?.modelQuoteId) {
    if (!quote) blockers.push('model_quote_required');
    else if (quote.status !== 'reconciled') blockers.push('model_usage_not_reconciled');
  }

  const netExecutableEdgeUsd = finite(decision?.netExecutableEdgeUsd, null);
  if (netExecutableEdgeUsd == null) blockers.push('net_executable_edge_required');
  const edgeBps = netExecutableEdgeUsd != null && orderNotionalUsd > 0
    ? (netExecutableEdgeUsd / orderNotionalUsd) * 10_000
    : -1_000_000_000;
  if (intent?.netExecutableEdgeUsd != null && netExecutableEdgeUsd != null) {
    const caller = finite(intent.netExecutableEdgeUsd, null);
    if (caller == null || Math.abs(caller - netExecutableEdgeUsd) > 1e-6) blockers.push('caller_edge_mismatch');
  }

  const sources = [];
  if (decision) sources.push(sourceRecord('economic_decision', decision.id, decision.createdAt || null, decision));
  if (forecast) sources.push(sourceRecord('price_forecast', forecast.id, forecast.asOf || forecast.createdAt || null, forecast));
  if (executionCost) sources.push(sourceRecord('execution_cost_snapshot', executionCost.id, executionCost.createdAt || null, executionCost));
  if (quote) sources.push(sourceRecord('model_usage', quote.id, quote.reconciledAt || quote.requestedAt || null, quote));

  return {
    required,
    verified: blockers.length === 0,
    edgeBps,
    minEdgeBps,
    netExecutableEdgeUsd,
    decisionId: decision?.id || null,
    forecastId: forecast?.id || null,
    executionCostSnapshotId: executionCost?.id || null,
    modelQuoteId: quote?.id || null,
    blockers: unique(blockers),
    sources,
  };
}

function validUntilForSources(nowMs, providerObservedMs, maxProviderAgeMs, marketObservedMs, maxMarketAgeMs, edgeState) {
  const candidates = [nowMs + 30_000];
  if (providerObservedMs != null) candidates.push(providerObservedMs + maxProviderAgeMs);
  if (marketObservedMs != null) candidates.push(marketObservedMs + maxMarketAgeMs);
  const sourceExpiries = edgeState?.required ? [
    edgeState?.forecast?.expiresAt,
    edgeState?.executionCost?.validUntil,
  ] : [];
  for (const value of sourceExpiries) {
    const ms = timestamp(value);
    if (ms != null) candidates.push(ms);
  }
  return new Date(Math.min(...candidates.filter(Number.isFinite))).toISOString();
}

export function buildCapitalRiskSnapshot({
  state,
  source = {},
  tradeIntentEnvelope,
  tradeIntentHash,
  now = new Date().toISOString(),
  excludeExecutionId = null,
  policy = {},
} = {}) {
  const nowMs = timestamp(now);
  if (nowMs == null) throw new TypeError('capital risk evaluation time invalid');
  const observedAt = iso(source.observedAt);
  const observedMs = timestamp(source.observedAt);
  const maxAuthoritativeStateAgeMs = Math.max(1, finite(policy.maxAuthoritativeStateAgeMs, DEFAULT_AUTHORITATIVE_STATE_MAX_AGE_MS));
  const maxMarketDataAgeMs = Math.max(1, finite(state?.config?.maxOrderbookStalenessMs, finite(policy.maxMarketDataAgeMs, DEFAULT_MARKET_DATA_MAX_AGE_MS)));
  const missingRequiredState = [];
  const staleRequiredState = [];
  const derivationReasons = [];
  const sources = [];

  if (!state || typeof state !== 'object') missingRequiredState.push('authoritative_state');
  if (observedMs == null) missingRequiredState.push('authoritative_state_observed_at');
  else if (observedMs > nowMs + 1000 || nowMs - observedMs > maxAuthoritativeStateAgeMs) staleRequiredState.push('authoritative_state');

  const intent = tradeIntentEnvelope || {};
  const accountId = intent.accountId || null;
  const symbol = intent.symbol || intent.orders?.[0]?.symbol || null;
  const venue = intent.venue || intent.orders?.[0]?.venue || null;
  const mode = String(intent.mode || 'paper').toLowerCase();
  const side = String(intent.side || intent.orders?.[0]?.side || '').toLowerCase();
  const quantity = requestedQuantity(intent);
  if (!accountId) missingRequiredState.push('account_id');
  if (!symbol) missingRequiredState.push('symbol');
  if (quantity == null || quantity <= 0) missingRequiredState.push('order_quantity');

  const account = (state?.accounts || []).find(row => row?.id === accountId) || null;
  const accountCash = finite(account?.cash, null);
  const accountNav = finite(account?.nav, null);
  if (!account) missingRequiredState.push('account');
  if (account && accountCash == null) missingRequiredState.push('account_cash');
  if (account && !iso(account.updatedAt ?? account.asOf ?? account.timestamp)) missingRequiredState.push('account_timestamp');
  if (account) sources.push(sourceRecord('account', account.id, account.updatedAt ?? account.asOf ?? account.timestamp ?? observedAt, {
    id: account.id,
    status: account.status ?? null,
    currency: account.currency ?? null,
    cash: accountCash,
    nav: accountNav,
    updatedAt: iso(account.updatedAt ?? account.asOf ?? account.timestamp),
  }));

  const killSwitch = state?.killSwitch;
  const killSwitchUpdatedAt = iso(killSwitch?.updatedAt);
  const killSwitchValid = killSwitch && typeof killSwitch.enabled === 'boolean' && killSwitchUpdatedAt != null;
  if (!killSwitchValid) missingRequiredState.push('kill_switch_state');
  if (killSwitchValid) sources.push(sourceRecord('kill_switch', 'operator-store:kill-switch', observedAt, {
    enabled: killSwitch.enabled,
    reason: killSwitch.reason ?? null,
    updatedAt: killSwitchUpdatedAt,
  }));

  const snapshots = (state?.marketDataSnapshots || []).filter(row => row?.symbol === symbol && paperVenueCompatible(venue, row?.venue, mode));
  const market = latestByTimestamp(snapshots);
  const marketObservedMs = timestamp(market?.timestamp ?? market?.asOf ?? market?.createdAt);
  const marketAgeMs = marketObservedMs == null ? null : Math.max(0, nowMs - marketObservedMs);
  const marketPrice = marketReferencePrice(market);
  if (!market || marketObservedMs == null || marketPrice == null) missingRequiredState.push('market_data');
  else if (marketObservedMs > nowMs + 1000 || marketAgeMs > maxMarketDataAgeMs) staleRequiredState.push('market_data');
  if (market) sources.push(sourceRecord('market_data', market.id || `${symbol}:${venue || 'any'}`, iso(market?.timestamp ?? market?.asOf ?? market?.createdAt), {
    id: market.id ?? null,
    symbol: market.symbol ?? null,
    venue: market.venue ?? null,
    bid: finite(market.bid, null),
    ask: finite(market.ask, null),
    referencePrice: marketPrice,
    status: market.status ?? null,
    source: market.source ?? null,
    timestamp: iso(market?.timestamp ?? market?.asOf ?? market?.createdAt),
  }));

  const entryPrice = finite(intent.entryPrice ?? intent.orders?.[0]?.price, null);
  const riskUnitPrice = marketPrice == null
    ? entryPrice
    : side === 'buy' && entryPrice != null
      ? Math.max(marketPrice, entryPrice)
      : marketPrice;
  const orderNotionalUsd = quantity != null && riskUnitPrice != null && riskUnitPrice > 0
    ? quantity * riskUnitPrice
    : null;
  if (orderNotionalUsd == null || orderNotionalUsd <= 0) missingRequiredState.push('order_notional');

  const pendingExecutions = (state?.executions || []).filter(execution => (
    PENDING_EXECUTION_STATUSES.has(String(execution?.status || '').toLowerCase())
    && executionMatchesIntent(execution, { accountId, symbol }, excludeExecutionId)
  ));
  let pendingExposureUsd = 0;
  let pendingBuyExposureUsd = 0;
  let pendingSellQuantity = 0;
  for (const execution of pendingExecutions) {
    const amount = pendingExecutionNotional(execution, marketPrice);
    if (amount == null) {
      missingRequiredState.push(`pending_execution_notional:${execution.id || 'unknown'}`);
      continue;
    }
    pendingExposureUsd += amount;
    const executionSide = String(execution.side || execution.orders?.[0]?.side || '').toLowerCase();
    if (executionSide === 'buy') pendingBuyExposureUsd += amount;
    if (executionSide === 'sell') {
      const pendingQuantity = finite(execution.quantity, null)
        ?? (execution.orders || []).reduce((sum, order) => sum + Math.max(0, finite(order?.quantity, 0)), 0);
      pendingSellQuantity += Math.max(0, pendingQuantity || 0);
    }
  }

  const positions = (state?.positions || []).filter(position => {
    if (position?.symbol !== symbol) return false;
    if (accountId && position?.accountId && position.accountId !== accountId) return false;
    return String(position?.status || 'open').toLowerCase() !== 'closed';
  });
  let positionExposureUsd = 0;
  let positionQuantityTotal = 0;
  let sellAccountLineageComplete = true;
  for (const position of positions) {
    const positionQty = positionQuantity(position);
    if (positionQty == null) {
      missingRequiredState.push(`position_quantity:${position.id || 'unknown'}`);
      continue;
    }
    const mark = finite(position.markPrice ?? position.currentPrice ?? position.averagePrice, marketPrice);
    if (mark == null || mark <= 0) {
      missingRequiredState.push(`position_mark:${position.id || 'unknown'}`);
      continue;
    }
    positionExposureUsd += Math.abs(positionQty * mark);
    positionQuantityTotal += Math.max(0, positionQty);
    if (accountId && !position.accountId) sellAccountLineageComplete = false;
  }
  if (side === 'sell' && positions.length && !sellAccountLineageComplete) missingRequiredState.push('position_account_lineage');

  const maxPositionSizeUsd = finite(state?.config?.maxPositionSizeUsd, null);
  if (maxPositionSizeUsd == null || maxPositionSizeUsd <= 0) missingRequiredState.push('max_position_size');
  const projectedExposureUsd = orderNotionalUsd == null ? null : positionExposureUsd + pendingExposureUsd + orderNotionalUsd;
  const availableCashAfterPendingUsd = accountCash == null ? null : accountCash - pendingBuyExposureUsd;
  let balanceSufficient = false;
  if (side === 'buy') {
    balanceSufficient = availableCashAfterPendingUsd != null && orderNotionalUsd != null && availableCashAfterPendingUsd >= orderNotionalUsd;
  } else if (side === 'sell') {
    balanceSufficient = sellAccountLineageComplete && quantity != null && positionQuantityTotal - pendingSellQuantity >= quantity;
  }

  const instrument = (state?.instruments || []).find(row => row?.symbol === symbol) || null;
  if (!Array.isArray(state?.instruments)) missingRequiredState.push('instrument_catalog');
  const pairApproved = Boolean(instrument && String(instrument.status || '').toLowerCase() === 'active');
  if (instrument) sources.push(sourceRecord('instrument', instrument.id || instrument.symbol, observedAt, instrument));
  const complianceApproved = ['paper', 'demo'].includes(mode) && pairApproved;

  const reconciliation = reconciliationState(state, { accountId, symbol }, excludeExecutionId, observedAt);
  if (!reconciliation.available) missingRequiredState.push('reconciliation_state');
  if (reconciliation.source) sources.push(reconciliation.source);

  const edgeState = economicEdgeState(state, intent, orderNotionalUsd || 0, nowMs, policy);
  if (!edgeState.verified) missingRequiredState.push(...edgeState.blockers.map(reason => `edge_lineage:${reason}`));
  sources.push(...edgeState.sources);

  const riskInputs = {
    killSwitch: killSwitchValid ? killSwitch.enabled : true,
    unresolvedRecon: !reconciliation.available || reconciliation.unresolved,
    pairApproved,
    complianceApproved,
    orderBookAgeMs: marketAgeMs == null ? 1_000_000_000 : Math.round(marketAgeMs),
    maxOrderbookStalenessMs: Math.round(maxMarketDataAgeMs),
    edgeBps: Number.isFinite(edgeState.edgeBps) ? edgeState.edgeBps : -1_000_000_000,
    minEdgeBps: Number.isFinite(edgeState.minEdgeBps) ? edgeState.minEdgeBps : DEFAULT_MIN_EDGE_BPS,
    balanceSufficient,
    notionalMicros: projectedExposureUsd == null ? 9_000_000_000_000_000 : Math.round(projectedExposureUsd * 1_000_000),
    maxNotionalMicros: maxPositionSizeUsd == null ? 0 : Math.round(maxPositionSizeUsd * 1_000_000),
    live: mode === 'live',
    runtimeConfirmed: false,
    credentialsPresent: false,
    venueModeExplicit: false,
    liveTrading: false,
    paperTrading: mode !== 'live',
  };

  const providerFacts = {
    source: source.source || source.type || 'operator_store',
    revision: source.revision ?? null,
    observedAt,
    schemaVersion: state?.schemaVersion ?? null,
  };
  if (state && observedAt) sources.unshift(sourceRecord('operator_state', providerFacts.source, observedAt, providerFacts, { revision: providerFacts.revision }));

  const validUntil = validUntilForSources(nowMs, observedMs, maxAuthoritativeStateAgeMs, marketObservedMs, maxMarketDataAgeMs, edgeState);
  const core = {
    schemaVersion: CAPITAL_RISK_SCHEMA_VERSION,
    policyVersion: CAPITAL_RISK_POLICY_VERSION,
    tradeIntentHash: tradeIntentHash || null,
    accountId,
    mode,
    symbol,
    venue,
    observedAt: new Date(nowMs).toISOString(),
    validUntil,
    sources,
    accountState: {
      found: Boolean(account),
      status: account?.status ?? null,
      currency: account?.currency ?? null,
      cashUsd: accountCash,
      navUsd: accountNav,
      availableCashAfterPendingUsd,
    },
    positionState: {
      positionIds: positions.map(row => row.id).filter(Boolean).sort(),
      openQuantity: positionQuantityTotal,
      currentExposureUsd: positionExposureUsd,
      accountLineageComplete: sellAccountLineageComplete,
    },
    pendingExecutionState: {
      executionIds: pendingExecutions.map(row => row.id).filter(Boolean).sort(),
      pendingExposureUsd,
      pendingBuyExposureUsd,
      pendingSellQuantity,
    },
    killSwitchState: {
      available: killSwitchValid,
      enabled: killSwitchValid ? killSwitch.enabled : null,
      reason: killSwitch?.reason ?? null,
      updatedAt: killSwitchUpdatedAt,
    },
    reconciliationState: {
      available: reconciliation.available,
      unresolved: reconciliation.unresolved,
      issues: reconciliation.issues,
      relevantExecutionIds: reconciliation.relevantExecutionIds,
    },
    marketDataState: {
      snapshotId: market?.id ?? null,
      timestamp: marketObservedMs == null ? null : new Date(marketObservedMs).toISOString(),
      ageMs: marketAgeMs,
      maxAgeMs: maxMarketDataAgeMs,
      referencePrice: marketPrice,
      source: market?.source ?? null,
      fresh: marketObservedMs != null && marketAgeMs <= maxMarketDataAgeMs,
    },
    edgeState: {
      required: edgeState.required,
      verified: edgeState.verified,
      economicDecisionId: edgeState.decisionId,
      forecastId: edgeState.forecastId ?? null,
      executionCostSnapshotId: edgeState.executionCostSnapshotId ?? null,
      modelQuoteId: edgeState.modelQuoteId ?? null,
      netExecutableEdgeUsd: edgeState.netExecutableEdgeUsd,
      edgeBps: edgeState.edgeBps,
      minEdgeBps: edgeState.minEdgeBps,
      blockers: edgeState.blockers,
    },
    limits: {
      maxPositionSizeUsd,
      orderNotionalUsd,
      projectedExposureUsd,
    },
    riskInputs,
    missingRequiredState: unique(missingRequiredState).sort(),
    staleRequiredState: unique(staleRequiredState).sort(),
    derivationReasons: unique(derivationReasons).sort(),
  };
  return { ...core, snapshotHash: stableHash(core) };
}

export function verifyCapitalRiskSnapshot(snapshot, { tradeIntentHash, now = new Date().toISOString() } = {}) {
  const reasons = [];
  if (!snapshot || typeof snapshot !== 'object') return { ok: false, reasons: ['capital_risk_snapshot_required'] };
  if (snapshot.schemaVersion !== CAPITAL_RISK_SCHEMA_VERSION || snapshot.policyVersion !== CAPITAL_RISK_POLICY_VERSION) {
    reasons.push('capital_risk_policy_version_mismatch');
  }
  if (tradeIntentHash && snapshot.tradeIntentHash !== tradeIntentHash) reasons.push('capital_risk_intent_hash_mismatch');
  const suppliedHash = snapshot.snapshotHash;
  const core = { ...snapshot };
  delete core.snapshotHash;
  try {
    if (!suppliedHash || stableHash(core) !== suppliedHash) reasons.push('capital_risk_snapshot_hash_mismatch');
  } catch {
    reasons.push('capital_risk_snapshot_not_canonicalizable');
  }
  const nowMs = timestamp(now);
  const validUntilMs = timestamp(snapshot.validUntil);
  if (nowMs == null || validUntilMs == null) reasons.push('capital_risk_snapshot_expiry_invalid');
  else if (nowMs > validUntilMs) reasons.push('capital_risk_snapshot_expired');
  if (!snapshot.riskInputs || typeof snapshot.riskInputs !== 'object') reasons.push('capital_risk_inputs_required');
  if (!Array.isArray(snapshot.missingRequiredState)) reasons.push('capital_risk_missing_state_invalid');
  if (!Array.isArray(snapshot.staleRequiredState)) reasons.push('capital_risk_stale_state_invalid');
  return { ok: reasons.length === 0, reasons: unique(reasons) };
}

export function evaluateCapitalRiskSnapshot(snapshot, options = {}) {
  const verification = verifyCapitalRiskSnapshot(snapshot, options);
  const reasons = [...verification.reasons];
  for (const field of snapshot?.missingRequiredState || []) reasons.push(`capital_risk_missing:${field}`);
  for (const field of snapshot?.staleRequiredState || []) reasons.push(`capital_risk_stale:${field}`);
  if (snapshot?.riskInputs) {
    const evaluated = evaluateRisk(snapshot.riskInputs);
    reasons.push(...evaluated.reasons);
  }
  const normalizedReasons = unique(reasons);
  const core = {
    approved: normalizedReasons.length === 0,
    reasons: normalizedReasons,
    source: 'canonical_capital_risk_snapshot',
    snapshotHash: snapshot?.snapshotHash || null,
    riskInputsHash: snapshot?.riskInputs ? stableHash(snapshot.riskInputs) : null,
    policyVersion: snapshot?.policyVersion || CAPITAL_RISK_POLICY_VERSION,
  };
  return { ...core, decisionHash: stableHash(core) };
}
