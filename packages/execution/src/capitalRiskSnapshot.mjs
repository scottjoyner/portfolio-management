import { evaluateRisk } from '../../risk/src/engine.mjs';
import { stableHash } from './overseer.mjs';

export const CAPITAL_RISK_SCHEMA_VERSION = 1;
export const CAPITAL_RISK_POLICY_VERSION = 'canonical-capital-risk-v1';
export const DEFAULT_AUTHORITATIVE_STATE_MAX_AGE_MS = 30_000;
export const DEFAULT_MARKET_DATA_MAX_AGE_MS = 120_000;
export const DEFAULT_MIN_EDGE_BPS = 1;

const PENDING_EXECUTION_STATUSES = new Set(['draft', 'approved', 'submitted', 'partially_filled']);
const CASH_SIDES = new Set(['buy', 'yes', 'no']);
const MARKET_READY_STATUSES = new Set(['connected', 'active', 'ready']);

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

function latestByTimestamp(rows = []) {
  return [...rows].sort((a, b) => (timestamp(b?.timestamp ?? b?.asOf ?? b?.updatedAt ?? b?.createdAt) || 0)
    - (timestamp(a?.timestamp ?? a?.asOf ?? a?.updatedAt ?? a?.createdAt) || 0))[0] || null;
}

function normalizeVenue(value) {
  return String(value || '').trim().toLowerCase();
}

function paperVenueCompatible(intentVenue, sourceVenue, mode) {
  const left = normalizeVenue(intentVenue);
  const right = normalizeVenue(sourceVenue);
  if (!left || !right || left === right) return true;
  if (!['paper', 'demo'].includes(String(mode || '').toLowerCase())) return false;
  if (left === 'paper' || left === 'demo' || right === 'paper' || right === 'demo') return true;
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

function deriveOrderBundle(intent = {}, marketPrice = null) {
  const orders = Array.isArray(intent.orders) ? intent.orders : [];
  const first = orders[0] || {};
  const symbol = intent.symbol || first.symbol || null;
  const venue = intent.venue || first.venue || null;
  const side = String(intent.side || first.side || '').toLowerCase();
  const invalid = [];
  const facts = [];
  let quantity = 0;
  let orderNotionalUsd = 0;

  if (!orders.length) invalid.push('order_bundle_empty');
  for (let index = 0; index < orders.length; index += 1) {
    const order = orders[index] || {};
    const orderSymbol = order.symbol || symbol;
    const orderVenue = order.venue || venue;
    const orderSide = String(order.side || side || '').toLowerCase();
    const orderQuantity = finite(order.quantity, null);
    if (symbol && orderSymbol && orderSymbol !== symbol) invalid.push('order_bundle_mixed_symbol');
    if (side && orderSide && orderSide !== side) invalid.push('order_bundle_mixed_side');
    if (venue && orderVenue && normalizeVenue(orderVenue) !== normalizeVenue(venue)) invalid.push('order_bundle_mixed_venue');
    if (orderQuantity == null || orderQuantity <= 0) invalid.push(`order_quantity:${index}`);

    const requestedPrice = finite(order.price ?? intent.entryPrice, null);
    let conservativePrice = marketPrice;
    if (CASH_SIDES.has(orderSide) && requestedPrice != null && requestedPrice > 0) {
      conservativePrice = conservativePrice == null ? requestedPrice : Math.max(conservativePrice, requestedPrice);
    } else if (conservativePrice == null && requestedPrice != null && requestedPrice > 0) {
      conservativePrice = requestedPrice;
    }
    if (conservativePrice == null || conservativePrice <= 0) invalid.push(`order_reference_price:${index}`);

    const notional = orderQuantity != null && orderQuantity > 0 && conservativePrice != null && conservativePrice > 0
      ? orderQuantity * conservativePrice
      : null;
    if (notional != null) {
      quantity += orderQuantity;
      orderNotionalUsd += notional;
    }
    facts.push({
      id: order.id ?? null,
      symbol: orderSymbol ?? null,
      venue: orderVenue ?? null,
      side: orderSide || null,
      quantity: orderQuantity,
      requestedPrice,
      conservativePrice,
      notionalUsd: notional,
    });
  }

  return {
    symbol,
    venue,
    side,
    quantity: quantity > 0 ? quantity : null,
    orderNotionalUsd: orderNotionalUsd > 0 ? orderNotionalUsd : null,
    orders: facts,
    invalid: unique(invalid).sort(),
  };
}

function pendingExecutionNotional(execution, marketPrice, targetSymbol) {
  const direct = finite(execution?.notional ?? execution?.notionalUsd, null);
  if (direct != null && direct >= 0) return direct;
  const orders = Array.isArray(execution?.orders) ? execution.orders : [];
  let total = 0;
  for (const order of orders) {
    const quantity = finite(order?.quantity, null);
    const sameSymbol = (order?.symbol || execution?.symbol || null) === targetSymbol;
    const price = finite(order?.price ?? execution?.entryPrice, sameSymbol ? marketPrice : null);
    if (quantity == null || quantity < 0 || price == null || price <= 0) return null;
    total += quantity * price;
  }
  return orders.length ? total : null;
}

function positionQuantity(position) {
  return finite(position?.quantity ?? position?.qty, null);
}

function executionSameAccount(execution, accountId) {
  if (!execution) return false;
  if (!accountId) return true;
  return execution.accountId === accountId;
}

function executionSameSymbol(execution, symbol) {
  if (!execution || !symbol) return true;
  return execution.symbol === symbol || (!execution.symbol && (execution.orders || []).some(order => order?.symbol === symbol));
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

  const relevant = state.executions.filter(execution => {
    if (!execution || execution.id === excludeExecutionId) return false;
    if (intent?.accountId && execution.accountId) return execution.accountId === intent.accountId;
    return executionSameSymbol(execution, intent?.symbol);
  });
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
      forecastId: null,
      executionCostSnapshotId: null,
      modelQuoteId: null,
      blockers: [],
      expiryCandidates: [],
      sources: [sourceRecord('risk_policy', 'manual-paper-edge-policy', new Date(nowMs).toISOString(), { edgeRequired: false })],
    };
  }

  const blockers = [];
  const decision = (state?.economicDecisions || []).find(row => row?.id === intent?.economicDecisionId) || null;
  if (!decision) blockers.push('economic_decision_required');
  if (decision && decision.executionAllowed !== true) blockers.push('economic_decision_execution_not_allowed');
  if (decision?.supersededByReconciliation === true) blockers.push('economic_decision_superseded');
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

  const forecastExpiry = timestamp(forecast?.expiresAt);
  const executionCostExpiry = timestamp(executionCost?.validUntil);
  if (!forecast) blockers.push('economic_forecast_required');
  else {
    if (forecast.status !== 'valid' || forecastExpiry == null || forecastExpiry < nowMs) blockers.push('economic_forecast_stale');
    if (forecast.symbol && intent?.symbol && forecast.symbol !== intent.symbol) blockers.push('economic_forecast_symbol_mismatch');
  }
  if (!executionCost) blockers.push('economic_execution_cost_required');
  else {
    if (executionCostExpiry == null || executionCostExpiry < nowMs) blockers.push('economic_execution_cost_stale');
    if (executionCost.symbol && intent?.symbol && executionCost.symbol !== intent.symbol) blockers.push('economic_execution_cost_symbol_mismatch');
  }

  const expiryCandidates = [forecastExpiry, executionCostExpiry].filter(Number.isFinite);
  if (decision?.modelQuoteId) {
    if (!quote) blockers.push('model_quote_required');
    else {
      if (quote.status !== 'reconciled') blockers.push('model_usage_not_reconciled');
      const decisionCreatedAt = timestamp(decision.createdAt);
      const quoteReconciledAt = timestamp(quote.reconciledAt);
      if (quoteReconciledAt != null && (decisionCreatedAt == null || decisionCreatedAt < quoteReconciledAt)) {
        blockers.push('economic_decision_requires_post_reconciliation_refresh');
      }
      const quoteRequestedAt = timestamp(quote.requestedAt);
      const quoteMaxAgeSeconds = Math.max(1, finite(state?.config?.maximumModelPricingAgeSeconds, 86400));
      if (quoteRequestedAt == null || quoteRequestedAt < nowMs - quoteMaxAgeSeconds * 1000) blockers.push('model_quote_stale');
      else expiryCandidates.push(quoteRequestedAt + quoteMaxAgeSeconds * 1000);
    }
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
    expiryCandidates,
    sources,
  };
}

function validUntilForSources(nowMs, providerObservedMs, maxProviderAgeMs, marketObservedMs, maxMarketAgeMs, edgeState) {
  const candidates = [nowMs + 30_000];
  if (providerObservedMs != null) candidates.push(providerObservedMs + maxProviderAgeMs);
  if (marketObservedMs != null) candidates.push(marketObservedMs + maxMarketAgeMs);
  for (const value of edgeState?.expiryCandidates || []) {
    if (Number.isFinite(value)) candidates.push(value);
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
  const invalidRequiredState = [];
  const derivationReasons = [];
  const sources = [];

  if (!state || typeof state !== 'object') missingRequiredState.push('authoritative_state');
  if (observedMs == null) missingRequiredState.push('authoritative_state_observed_at');
  else if (observedMs > nowMs + 1000 || nowMs - observedMs > maxAuthoritativeStateAgeMs) staleRequiredState.push('authoritative_state');

  const intent = tradeIntentEnvelope || {};
  const accountId = intent.accountId || null;
  const mode = String(intent.mode || 'paper').toLowerCase();
  const provisionalSymbol = intent.symbol || intent.orders?.[0]?.symbol || null;
  const provisionalVenue = intent.venue || intent.orders?.[0]?.venue || null;
  if (!accountId) missingRequiredState.push('account_id');
  if (!provisionalSymbol) missingRequiredState.push('symbol');

  const account = (state?.accounts || []).find(row => row?.id === accountId) || null;
  const accountCash = finite(account?.cash, null);
  const accountNav = finite(account?.nav, null);
  const accountUpdatedAt = iso(account?.updatedAt ?? account?.asOf ?? account?.timestamp);
  if (!account) missingRequiredState.push('account');
  if (account && accountCash == null) missingRequiredState.push('account_cash');
  if (account && accountUpdatedAt == null) missingRequiredState.push('account_timestamp');
  if (account && String(account.status || '').toLowerCase() !== 'connected') invalidRequiredState.push('account_not_connected');
  if (account && String(account.currency || '').toUpperCase() !== 'USD') invalidRequiredState.push('account_currency_not_usd');
  if (account) sources.push(sourceRecord('account', account.id, accountUpdatedAt ?? observedAt, {
    id: account.id,
    status: account.status ?? null,
    currency: account.currency ?? null,
    cash: accountCash,
    nav: accountNav,
    updatedAt: accountUpdatedAt,
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

  const symbolMarketSnapshots = (state?.marketDataSnapshots || []).filter(row => row?.symbol === provisionalSymbol);
  const venueMarketSnapshots = symbolMarketSnapshots.filter(row => paperVenueCompatible(provisionalVenue, row?.venue, mode));
  if (symbolMarketSnapshots.length && !venueMarketSnapshots.length) invalidRequiredState.push('market_venue_mismatch');
  const market = latestByTimestamp(venueMarketSnapshots);
  const marketObservedMs = timestamp(market?.timestamp ?? market?.asOf ?? market?.createdAt);
  const marketAgeMs = marketObservedMs == null ? null : Math.max(0, nowMs - marketObservedMs);
  const marketPrice = marketReferencePrice(market);
  if (!market || marketObservedMs == null || marketPrice == null) missingRequiredState.push('market_data');
  else {
    if (marketObservedMs > nowMs + 1000 || marketAgeMs > maxMarketDataAgeMs) staleRequiredState.push('market_data');
    if (!MARKET_READY_STATUSES.has(String(market.status || '').toLowerCase())) invalidRequiredState.push('market_data_not_ready');
  }
  if (market) sources.push(sourceRecord('market_data', market.id || `${provisionalSymbol}:${provisionalVenue || 'any'}`, iso(market?.timestamp ?? market?.asOf ?? market?.createdAt), {
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

  const orderBundle = deriveOrderBundle(intent, marketPrice);
  invalidRequiredState.push(...orderBundle.invalid);
  const symbol = orderBundle.symbol || provisionalSymbol;
  const venue = orderBundle.venue || provisionalVenue;
  const side = orderBundle.side;
  const quantity = orderBundle.quantity;
  const orderNotionalUsd = orderBundle.orderNotionalUsd;
  if (quantity == null || quantity <= 0) missingRequiredState.push('order_quantity');
  if (orderNotionalUsd == null || orderNotionalUsd <= 0) missingRequiredState.push('order_notional');

  const allPendingExecutions = (state?.executions || []).filter(execution => (
    PENDING_EXECUTION_STATUSES.has(String(execution?.status || '').toLowerCase())
    && execution?.id !== excludeExecutionId
  ));
  const unscopedPendingExecutions = allPendingExecutions.filter(execution => !execution?.accountId);
  for (const execution of unscopedPendingExecutions) {
    missingRequiredState.push(`pending_execution_account_lineage:${execution?.id || 'unknown'}`);
  }
  const pendingAccountExecutions = allPendingExecutions.filter(execution => executionSameAccount(execution, accountId));
  const pendingSymbolExecutions = pendingAccountExecutions.filter(execution => executionSameSymbol(execution, symbol));
  let pendingIncreaseExposureUsd = 0;
  let pendingSellExposureUsd = 0;
  let pendingBuyExposureUsd = 0;
  let pendingSellQuantity = 0;
  for (const execution of pendingAccountExecutions) {
    const amount = pendingExecutionNotional(execution, marketPrice, symbol);
    if (amount == null) {
      missingRequiredState.push(`pending_execution_notional:${execution.id || 'unknown'}`);
      continue;
    }
    const executionSide = String(execution.side || execution.orders?.[0]?.side || '').toLowerCase();
    if (CASH_SIDES.has(executionSide)) pendingBuyExposureUsd += amount;
    if (executionSameSymbol(execution, symbol)) {
      if (CASH_SIDES.has(executionSide)) pendingIncreaseExposureUsd += amount;
      else if (executionSide === 'sell') pendingSellExposureUsd += amount;
    }
    if (executionSide === 'sell' && executionSameSymbol(execution, symbol)) {
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
  const projectedExposureUsd = orderNotionalUsd == null
    ? null
    : side === 'sell'
      ? Math.max(0, positionExposureUsd - pendingSellExposureUsd - orderNotionalUsd)
      : positionExposureUsd + pendingIncreaseExposureUsd + orderNotionalUsd;
  const availableCashAfterPendingUsd = accountCash == null ? null : accountCash - pendingBuyExposureUsd;
  let balanceSufficient = false;
  if (CASH_SIDES.has(side)) {
    balanceSufficient = availableCashAfterPendingUsd != null && orderNotionalUsd != null && availableCashAfterPendingUsd >= orderNotionalUsd;
  } else if (side === 'sell') {
    balanceSufficient = sellAccountLineageComplete && quantity != null && positionQuantityTotal - pendingSellQuantity >= quantity;
  } else if (side) {
    invalidRequiredState.push('order_side_unsupported');
  }

  const symbolInstruments = Array.isArray(state?.instruments) ? state.instruments.filter(row => row?.symbol === symbol) : [];
  if (!Array.isArray(state?.instruments)) missingRequiredState.push('instrument_catalog');
  const instrument = symbolInstruments.find(row => paperVenueCompatible(venue, row?.venue, mode)) || null;
  if (!instrument) {
    if (symbolInstruments.length) invalidRequiredState.push('instrument_venue_mismatch');
    else missingRequiredState.push('instrument');
  }
  const pairApproved = Boolean(instrument && String(instrument.status || '').toLowerCase() === 'active');
  if (instrument && !pairApproved) invalidRequiredState.push('instrument_not_active');
  if (instrument) sources.push(sourceRecord('instrument', instrument.id || instrument.symbol, observedAt, instrument));
  const complianceApproved = ['paper', 'demo'].includes(mode) && pairApproved;

  const reconciliation = reconciliationState(state, { accountId, symbol }, excludeExecutionId, observedAt);
  if (!reconciliation.available) missingRequiredState.push('reconciliation_state');
  if (reconciliation.source) sources.push(reconciliation.source);

  const edgeState = economicEdgeState(state, intent, orderNotionalUsd || 0, nowMs, policy);
  if (!edgeState.verified) invalidRequiredState.push(...edgeState.blockers.map(reason => `edge_lineage:${reason}`));
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
    orderBundle,
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
      accountExecutionIds: pendingAccountExecutions.map(row => row.id).filter(Boolean).sort(),
      symbolExecutionIds: pendingSymbolExecutions.map(row => row.id).filter(Boolean).sort(),
      unscopedExecutionIds: unscopedPendingExecutions.map(row => row.id).filter(Boolean).sort(),
      pendingExposureUsd: pendingIncreaseExposureUsd,
      pendingIncreaseExposureUsd,
      pendingSellExposureUsd,
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
      status: market?.status ?? null,
      fresh: marketObservedMs != null && marketAgeMs <= maxMarketDataAgeMs,
    },
    edgeState: {
      required: edgeState.required,
      verified: edgeState.verified,
      economicDecisionId: edgeState.decisionId,
      forecastId: edgeState.forecastId,
      executionCostSnapshotId: edgeState.executionCostSnapshotId,
      modelQuoteId: edgeState.modelQuoteId,
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
    invalidRequiredState: unique(invalidRequiredState).sort(),
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
  if (!Array.isArray(snapshot.invalidRequiredState)) reasons.push('capital_risk_invalid_state_invalid');
  return { ok: reasons.length === 0, reasons: unique(reasons) };
}

export function evaluateCapitalRiskSnapshot(snapshot, options = {}) {
  const verification = verifyCapitalRiskSnapshot(snapshot, options);
  const reasons = [...verification.reasons];
  for (const field of snapshot?.missingRequiredState || []) reasons.push(`capital_risk_missing:${field}`);
  for (const field of snapshot?.staleRequiredState || []) reasons.push(`capital_risk_stale:${field}`);
  for (const field of snapshot?.invalidRequiredState || []) reasons.push(`capital_risk_invalid:${field}`);
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
