import { buildTradeIntentEnvelope, stableHash } from './overseer.mjs';

export const PORTFOLIO_ALLOCATION_SCHEMA_VERSION = 1;
export const PORTFOLIO_ALLOCATION_POLICY_VERSION = 'canonical-portfolio-allocation-v1';
export const DEFAULT_PORTFOLIO_ALLOCATION_TTL_MS = 30_000;

const INCREASING_SIDES = new Set(['buy', 'yes', 'no']);
const REDUCING_SIDES = new Set(['sell']);
const PENDING_EXECUTION_STATUSES = new Set(['draft', 'approved', 'submitted', 'partially_filled']);
const MARKET_READY_STATUSES = new Set(['connected', 'active', 'ready']);

export const DEFAULT_PORTFOLIO_POLICY = Object.freeze({
  maxPortfolioDrawdownPct: 15,
  drawdownThrottleStartPct: 10,
  maxGrossLeverage: 1.5,
  maxNetExposurePct: 1.0,
  maxSymbolExposurePct: 0.20,
  maxStrategyExposurePct: 0.30,
  maxCorrelationClusterExposurePct: 0.40,
  maxSingleTradePct: 0.10,
  minCashBufferPct: 0.10,
  maxSpreadBps: 100,
  maxLiquidityParticipationPct: 0.01,
  unknownLiquidityMaxTradePct: 0.05,
  maxCovarianceRiskPct: 0.45,
  minApprovedNotionalUsd: 100,
  sameClusterFallbackCorrelation: 0.75,
  crossClusterFallbackCorrelation: 0.25,
  defaultCryptoVolatility: 0.80,
  defaultEquityVolatility: 0.30,
  defaultOtherVolatility: 1.00,
  correlationClusters: {
    'crypto-beta': ['BTC-USD', 'ETH-USD', 'SOL-USD'],
  },
});

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

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
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

function latestByTimestamp(rows = []) {
  return [...rows].sort((a, b) => (
    timestamp(b?.timestamp ?? b?.asOf ?? b?.updatedAt ?? b?.createdAt) || 0
  ) - (
    timestamp(a?.timestamp ?? a?.asOf ?? a?.updatedAt ?? a?.createdAt) || 0
  ))[0] || null;
}

function marketReferencePrice(snapshot) {
  const direct = finite(snapshot?.mid ?? snapshot?.price ?? snapshot?.close ?? snapshot?.lastPrice, null);
  if (direct != null && direct > 0) return direct;
  const bid = finite(snapshot?.bid, null);
  const ask = finite(snapshot?.ask, null);
  if (bid != null && ask != null && bid > 0 && ask > 0) return (bid + ask) / 2;
  return null;
}

function marketSpreadBps(snapshot) {
  const direct = finite(snapshot?.spreadBps ?? snapshot?.spread_bps, null);
  if (direct != null && direct >= 0) return direct;
  const bid = finite(snapshot?.bid, null);
  const ask = finite(snapshot?.ask, null);
  if (bid == null || ask == null || bid <= 0 || ask <= 0 || ask < bid) return null;
  const mid = (bid + ask) / 2;
  return mid > 0 ? ((ask - bid) / mid) * 10_000 : null;
}

function marketVolumeUsd(snapshot, referencePrice) {
  for (const candidate of [
    snapshot?.volume24hUsd,
    snapshot?.quoteVolume24hUsd,
    snapshot?.notionalVolume24hUsd,
    snapshot?.quoteVolume,
  ]) {
    const value = finite(candidate, null);
    if (value != null && value > 0) return value;
  }
  const baseVolume = finite(snapshot?.volume24h ?? snapshot?.baseVolume24h ?? snapshot?.volume, null);
  if (baseVolume != null && baseVolume > 0 && referencePrice != null && referencePrice > 0) {
    return baseVolume * referencePrice;
  }
  return null;
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

function numericPolicy(configured, overrides, key, fallback, { min = 0, max = Number.POSITIVE_INFINITY } = {}) {
  const value = finite(overrides?.[key], finite(configured?.[key], fallback));
  if (value == null) return fallback;
  return clamp(value, min, max);
}

export function resolvePortfolioPolicy(state, overrides = {}) {
  const configured = state?.config?.capitalPolicy || {};
  const configuredClusters = configured.correlationClusters && typeof configured.correlationClusters === 'object'
    ? configured.correlationClusters
    : DEFAULT_PORTFOLIO_POLICY.correlationClusters;
  const overrideClusters = overrides.correlationClusters && typeof overrides.correlationClusters === 'object'
    ? overrides.correlationClusters
    : configuredClusters;
  const correlationClusters = Object.fromEntries(
    Object.entries(overrideClusters)
      .map(([cluster, symbols]) => [cluster, Array.isArray(symbols) ? unique(symbols.map(String)).sort() : []])
      .filter(([, symbols]) => symbols.length),
  );
  return {
    maxPortfolioDrawdownPct: numericPolicy(configured, overrides, 'maxPortfolioDrawdownPct', DEFAULT_PORTFOLIO_POLICY.maxPortfolioDrawdownPct, { min: 0.1, max: 100 }),
    drawdownThrottleStartPct: numericPolicy(configured, overrides, 'drawdownThrottleStartPct', DEFAULT_PORTFOLIO_POLICY.drawdownThrottleStartPct, { min: 0, max: 100 }),
    maxGrossLeverage: numericPolicy(configured, overrides, 'maxGrossLeverage', DEFAULT_PORTFOLIO_POLICY.maxGrossLeverage, { min: 0.01, max: 10 }),
    maxNetExposurePct: numericPolicy(configured, overrides, 'maxNetExposurePct', DEFAULT_PORTFOLIO_POLICY.maxNetExposurePct, { min: 0.01, max: 10 }),
    maxSymbolExposurePct: numericPolicy(configured, overrides, 'maxSymbolExposurePct', DEFAULT_PORTFOLIO_POLICY.maxSymbolExposurePct, { min: 0.001, max: 1 }),
    maxStrategyExposurePct: numericPolicy(configured, overrides, 'maxStrategyExposurePct', DEFAULT_PORTFOLIO_POLICY.maxStrategyExposurePct, { min: 0.001, max: 1 }),
    maxCorrelationClusterExposurePct: numericPolicy(configured, overrides, 'maxCorrelationClusterExposurePct', DEFAULT_PORTFOLIO_POLICY.maxCorrelationClusterExposurePct, { min: 0.001, max: 1 }),
    maxSingleTradePct: numericPolicy(configured, overrides, 'maxSingleTradePct', DEFAULT_PORTFOLIO_POLICY.maxSingleTradePct, { min: 0.001, max: 1 }),
    minCashBufferPct: numericPolicy(configured, overrides, 'minCashBufferPct', DEFAULT_PORTFOLIO_POLICY.minCashBufferPct, { min: 0, max: 1 }),
    maxSpreadBps: numericPolicy(configured, overrides, 'maxSpreadBps', DEFAULT_PORTFOLIO_POLICY.maxSpreadBps, { min: 0, max: 10_000 }),
    maxLiquidityParticipationPct: numericPolicy(configured, overrides, 'maxLiquidityParticipationPct', DEFAULT_PORTFOLIO_POLICY.maxLiquidityParticipationPct, { min: 0.000001, max: 1 }),
    unknownLiquidityMaxTradePct: numericPolicy(configured, overrides, 'unknownLiquidityMaxTradePct', DEFAULT_PORTFOLIO_POLICY.unknownLiquidityMaxTradePct, { min: 0.0001, max: 1 }),
    maxCovarianceRiskPct: numericPolicy(configured, overrides, 'maxCovarianceRiskPct', DEFAULT_PORTFOLIO_POLICY.maxCovarianceRiskPct, { min: 0.001, max: 10 }),
    minApprovedNotionalUsd: numericPolicy(configured, overrides, 'minApprovedNotionalUsd', DEFAULT_PORTFOLIO_POLICY.minApprovedNotionalUsd, { min: 0, max: 1_000_000_000 }),
    sameClusterFallbackCorrelation: numericPolicy(configured, overrides, 'sameClusterFallbackCorrelation', DEFAULT_PORTFOLIO_POLICY.sameClusterFallbackCorrelation, { min: -1, max: 1 }),
    crossClusterFallbackCorrelation: numericPolicy(configured, overrides, 'crossClusterFallbackCorrelation', DEFAULT_PORTFOLIO_POLICY.crossClusterFallbackCorrelation, { min: -1, max: 1 }),
    defaultCryptoVolatility: numericPolicy(configured, overrides, 'defaultCryptoVolatility', DEFAULT_PORTFOLIO_POLICY.defaultCryptoVolatility, { min: 0.01, max: 10 }),
    defaultEquityVolatility: numericPolicy(configured, overrides, 'defaultEquityVolatility', DEFAULT_PORTFOLIO_POLICY.defaultEquityVolatility, { min: 0.01, max: 10 }),
    defaultOtherVolatility: numericPolicy(configured, overrides, 'defaultOtherVolatility', DEFAULT_PORTFOLIO_POLICY.defaultOtherVolatility, { min: 0.01, max: 10 }),
    correlationClusters,
  };
}

function clusterForSymbol(symbol, policy) {
  for (const [cluster, symbols] of Object.entries(policy.correlationClusters || {})) {
    if (symbols.includes(symbol)) return cluster;
  }
  return 'unclassified';
}

function marketForSymbol(state, symbol, venue = null, mode = 'paper') {
  const candidates = (state?.marketDataSnapshots || []).filter(row => row?.symbol === symbol);
  const compatible = venue
    ? candidates.filter(row => paperVenueCompatible(venue, row?.venue, mode))
    : candidates;
  return latestByTimestamp(compatible);
}

function strategyKey(value) {
  return value || 'unattributed';
}

function sideSign(side) {
  const normalized = String(side || '').toLowerCase();
  if (INCREASING_SIDES.has(normalized)) return 1;
  if (REDUCING_SIDES.has(normalized)) return -1;
  return 0;
}

function addMap(map, key, amount) {
  if (!key || !Number.isFinite(amount)) return;
  map.set(key, (map.get(key) || 0) + amount);
}

function mapObject(map) {
  return Object.fromEntries([...map.entries()].sort(([a], [b]) => String(a).localeCompare(String(b))));
}

function accountRiskRecord(state, accountId) {
  const root = state?.portfolioRiskState;
  const rows = Array.isArray(root?.accounts)
    ? root.accounts
    : root?.accounts && typeof root.accounts === 'object'
      ? Object.entries(root.accounts).map(([id, value]) => ({ accountId: id, ...(value || {}) }))
      : [];
  return rows.find(row => row?.accountId === accountId || row?.id === accountId) || null;
}

function accountOwned(row, accountId, state) {
  if (!row) return false;
  if (!accountId) return true;
  if (row.accountId) return row.accountId === accountId;
  return Array.isArray(state?.accounts) && state.accounts.length === 1 && state.accounts[0]?.id === accountId;
}

function positionExposure(position, state, mode) {
  const quantity = finite(position?.quantity ?? position?.qty, null);
  if (quantity == null) return { ok: false, reason: `position_quantity:${position?.id || 'unknown'}` };
  const symbol = position?.symbol || null;
  if (!symbol) return { ok: false, reason: `position_symbol:${position?.id || 'unknown'}` };
  const market = marketForSymbol(state, symbol, position?.venue || null, mode);
  const mark = finite(position?.markPrice ?? position?.currentPrice ?? position?.averagePrice, marketReferencePrice(market));
  if (mark == null || mark <= 0) return { ok: false, reason: `position_mark:${position?.id || symbol}` };
  const explicitSide = String(position?.side || position?.positionSide || '').toLowerCase();
  const sign = explicitSide === 'short' || explicitSide === 'sell' ? -1 : quantity < 0 ? -1 : 1;
  return {
    ok: true,
    symbol,
    strategyId: strategyKey(position?.strategyId),
    signedUsd: sign * Math.abs(quantity * mark),
    grossUsd: Math.abs(quantity * mark),
    quantity,
    mark,
  };
}

function orderNotional(order, execution, state, mode, targetVenue = null) {
  const quantity = finite(order?.quantity, null);
  const symbol = order?.symbol || execution?.symbol || null;
  if (quantity == null || quantity <= 0 || !symbol) return null;
  const market = marketForSymbol(state, symbol, order?.venue || execution?.venue || targetVenue, mode);
  const price = finite(order?.price ?? execution?.entryPrice, marketReferencePrice(market));
  if (price == null || price <= 0) return null;
  return quantity * price;
}

function requestBundle(request, state) {
  const orders = Array.isArray(request?.orders) ? request.orders : [];
  const first = orders[0] || {};
  const symbol = request?.symbol || first.symbol || null;
  const venue = request?.venue || first.venue || null;
  const side = String(request?.side || first.side || '').toLowerCase();
  const mode = String(request?.mode || 'paper').toLowerCase();
  const market = symbol ? marketForSymbol(state, symbol, venue, mode) : null;
  const marketPrice = marketReferencePrice(market);
  const invalid = [];
  let quantity = 0;
  let requestedNotionalUsd = 0;
  if (!orders.length) invalid.push('order_bundle_empty');
  orders.forEach((order, index) => {
    const orderSymbol = order?.symbol || symbol;
    const orderVenue = order?.venue || venue;
    const orderSide = String(order?.side || side || '').toLowerCase();
    const orderQuantity = finite(order?.quantity, null);
    if (symbol && orderSymbol && symbol !== orderSymbol) invalid.push('order_bundle_mixed_symbol');
    if (venue && orderVenue && normalizeVenue(venue) !== normalizeVenue(orderVenue)) invalid.push('order_bundle_mixed_venue');
    if (side && orderSide && side !== orderSide) invalid.push('order_bundle_mixed_side');
    if (!INCREASING_SIDES.has(orderSide) && !REDUCING_SIDES.has(orderSide)) invalid.push(`order_side:${index}`);
    if (orderQuantity == null || orderQuantity <= 0) invalid.push(`order_quantity:${index}`);
    const requestedPrice = finite(order?.price ?? request?.entryPrice, null);
    let conservativePrice = marketPrice;
    if (INCREASING_SIDES.has(orderSide) && requestedPrice != null && requestedPrice > 0) {
      conservativePrice = conservativePrice == null ? requestedPrice : Math.max(conservativePrice, requestedPrice);
    } else if (conservativePrice == null && requestedPrice != null && requestedPrice > 0) {
      conservativePrice = requestedPrice;
    }
    if (conservativePrice == null || conservativePrice <= 0) invalid.push(`order_reference_price:${index}`);
    if (orderQuantity != null && orderQuantity > 0 && conservativePrice != null && conservativePrice > 0) {
      quantity += orderQuantity;
      requestedNotionalUsd += orderQuantity * conservativePrice;
    }
  });
  return {
    symbol,
    venue,
    side,
    mode,
    market,
    marketPrice,
    quantity: quantity > 0 ? quantity : null,
    requestedNotionalUsd: requestedNotionalUsd > 0 ? requestedNotionalUsd : null,
    invalid: unique(invalid).sort(),
  };
}

function deriveExposureState(state, accountId, mode, policy, missing) {
  const symbolExposure = new Map();
  const strategyExposure = new Map();
  const clusterGrossExposure = new Map();
  const positionIds = [];
  const pendingExecutionIds = [];
  let pendingBuyCashUsd = 0;

  for (const position of state?.positions || []) {
    if (String(position?.status || 'open').toLowerCase() === 'closed') continue;
    if (!accountOwned(position, accountId, state)) {
      if (!position?.accountId && Array.isArray(state?.accounts) && state.accounts.length > 1) {
        missing.push(`position_account_lineage:${position?.id || 'unknown'}`);
      }
      continue;
    }
    const exposure = positionExposure(position, state, mode);
    if (!exposure.ok) {
      missing.push(exposure.reason);
      continue;
    }
    positionIds.push(position.id || `${exposure.symbol}:position`);
    addMap(symbolExposure, exposure.symbol, exposure.signedUsd);
    addMap(strategyExposure, exposure.strategyId, Math.abs(exposure.signedUsd));
  }

  for (const execution of state?.executions || []) {
    if (!PENDING_EXECUTION_STATUSES.has(String(execution?.status || '').toLowerCase())) continue;
    if (!execution?.accountId) {
      missing.push(`pending_execution_account_lineage:${execution?.id || 'unknown'}`);
      continue;
    }
    if (execution.accountId !== accountId) continue;
    pendingExecutionIds.push(execution.id || 'unknown');
    const orders = Array.isArray(execution.orders) ? execution.orders : [];
    if (!orders.length) {
      const direct = finite(execution?.notional ?? execution?.notionalUsd, null);
      const sign = sideSign(execution?.side);
      if (direct == null || direct < 0 || sign === 0 || !execution?.symbol) {
        missing.push(`pending_execution_notional:${execution?.id || 'unknown'}`);
        continue;
      }
      addMap(symbolExposure, execution.symbol, sign * direct);
      addMap(strategyExposure, strategyKey(execution.strategyId), Math.abs(direct));
      if (sign > 0) pendingBuyCashUsd += direct;
      continue;
    }
    for (const order of orders) {
      const sign = sideSign(order?.side || execution?.side);
      const symbol = order?.symbol || execution?.symbol || null;
      const amount = orderNotional(order, execution, state, mode);
      if (!symbol || sign === 0 || amount == null) {
        missing.push(`pending_execution_notional:${execution?.id || 'unknown'}`);
        continue;
      }
      addMap(symbolExposure, symbol, sign * amount);
      addMap(strategyExposure, strategyKey(execution.strategyId || order?.strategyId), Math.abs(amount));
      if (sign > 0) pendingBuyCashUsd += amount;
    }
  }

  for (const [symbol, signed] of symbolExposure.entries()) {
    addMap(clusterGrossExposure, clusterForSymbol(symbol, policy), Math.abs(signed));
  }
  const grossExposureUsd = [...symbolExposure.values()].reduce((sum, value) => sum + Math.abs(value), 0);
  const netExposureUsd = [...symbolExposure.values()].reduce((sum, value) => sum + value, 0);
  return {
    positionIds: positionIds.sort(),
    pendingExecutionIds: pendingExecutionIds.sort(),
    symbolExposure,
    strategyExposure,
    clusterGrossExposure,
    pendingBuyCashUsd,
    grossExposureUsd,
    netExposureUsd,
  };
}

function assetClassForSymbol(state, symbol) {
  return (state?.instruments || []).find(row => row?.symbol === symbol)?.assetClass || '';
}

function volatilityForSymbol(state, symbol, policy, sourceFacts) {
  const explicit = finite(state?.portfolioRiskState?.volatilityBySymbol?.[symbol], null);
  if (explicit != null && explicit > 0) {
    sourceFacts.volatilitySources[symbol] = 'portfolio_risk_state';
    return explicit;
  }
  const market = marketForSymbol(state, symbol);
  const marketVol = finite(market?.annualizedVolatility ?? market?.volatility, null);
  if (marketVol != null && marketVol > 0) {
    sourceFacts.volatilitySources[symbol] = 'market_data';
    return marketVol > 5 ? marketVol / 100 : marketVol;
  }
  const assetClass = assetClassForSymbol(state, symbol).toLowerCase();
  sourceFacts.volatilitySources[symbol] = 'policy_fallback';
  if (assetClass.includes('crypto')) return policy.defaultCryptoVolatility;
  if (assetClass.includes('equity') || assetClass.includes('stock')) return policy.defaultEquityVolatility;
  return policy.defaultOtherVolatility;
}

function correlationForSymbols(state, left, right, policy, sourceFacts) {
  if (left === right) return 1;
  const matrix = state?.portfolioRiskState?.correlationMatrix || {};
  const explicit = finite(matrix?.[left]?.[right], finite(matrix?.[right]?.[left], null));
  if (explicit != null && explicit >= -1 && explicit <= 1) {
    sourceFacts.explicitCorrelationPairs.push(`${left}|${right}`);
    return explicit;
  }
  const sameCluster = clusterForSymbol(left, policy) === clusterForSymbol(right, policy);
  return sameCluster ? policy.sameClusterFallbackCorrelation : policy.crossClusterFallbackCorrelation;
}

function covarianceRiskUsd(state, exposures, policy, sourceFacts) {
  const symbols = [...exposures.keys()].filter(symbol => Math.abs(exposures.get(symbol) || 0) > 1e-9).sort();
  if (!symbols.length) return 0;
  let variance = 0;
  for (const left of symbols) {
    const leftExposure = exposures.get(left) || 0;
    const leftVol = volatilityForSymbol(state, left, policy, sourceFacts);
    for (const right of symbols) {
      const rightExposure = exposures.get(right) || 0;
      const rightVol = volatilityForSymbol(state, right, policy, sourceFacts);
      const corr = correlationForSymbols(state, left, right, policy, sourceFacts);
      variance += leftExposure * rightExposure * leftVol * rightVol * corr;
    }
  }
  return Math.sqrt(Math.max(0, variance));
}

function withTargetExposure(exposures, symbol, deltaUsd) {
  const next = new Map(exposures);
  next.set(symbol, (next.get(symbol) || 0) + deltaUsd);
  return next;
}

function covarianceAllowedNotional(state, exposures, symbol, requestedUsd, navUsd, policy, sourceFacts) {
  const limitUsd = navUsd * policy.maxCovarianceRiskPct;
  const currentRiskUsd = covarianceRiskUsd(state, exposures, policy, sourceFacts);
  if (currentRiskUsd >= limitUsd - 1e-9) return { currentRiskUsd, limitUsd, allowedUsd: 0 };
  const fullRiskUsd = covarianceRiskUsd(state, withTargetExposure(exposures, symbol, requestedUsd), policy, sourceFacts);
  if (fullRiskUsd <= limitUsd) return { currentRiskUsd, limitUsd, allowedUsd: requestedUsd, fullRiskUsd };
  let low = 0;
  let high = requestedUsd;
  for (let index = 0; index < 48; index += 1) {
    const mid = (low + high) / 2;
    const risk = covarianceRiskUsd(state, withTargetExposure(exposures, symbol, mid), policy, sourceFacts);
    if (risk <= limitUsd) low = mid;
    else high = mid;
  }
  return { currentRiskUsd, limitUsd, allowedUsd: low, fullRiskUsd };
}

function economicLineagePresent(request) {
  return Boolean(
    request?.sourceAgentId
    || request?.economicDecisionId
    || request?.modelQuoteId
    || request?.forecastId
    || request?.executionCostSnapshotId
    || request?.netExecutableEdgeUsd != null
  );
}

function scaleRequest(request, scale) {
  const bounded = clamp(scale, 0, 1);
  if (bounded >= 1 - 1e-12) return structuredClone(request);
  const scaled = structuredClone(request);
  if (Array.isArray(scaled.orders)) {
    scaled.orders = scaled.orders.map(order => ({
      ...order,
      quantity: finite(order?.quantity, 0) > 0 ? Number((Number(order.quantity) * bounded).toFixed(12)) : order?.quantity,
      ...(finite(order?.notional, null) != null ? { notional: Number((Number(order.notional) * bounded).toFixed(6)) } : {}),
    }));
  }
  if (finite(scaled.quantity, null) != null) scaled.quantity = Number((Number(scaled.quantity) * bounded).toFixed(12));
  if (finite(scaled.notional, null) != null) scaled.notional = Number((Number(scaled.notional) * bounded).toFixed(6));
  if (finite(scaled.notionalUsd, null) != null) scaled.notionalUsd = Number((Number(scaled.notionalUsd) * bounded).toFixed(6));
  return scaled;
}

function allocationExpiry(nowMs, sourceObservedMs, marketObservedMs) {
  const candidates = [nowMs + DEFAULT_PORTFOLIO_ALLOCATION_TTL_MS];
  if (sourceObservedMs != null) candidates.push(sourceObservedMs + DEFAULT_PORTFOLIO_ALLOCATION_TTL_MS);
  if (marketObservedMs != null) candidates.push(marketObservedMs + DEFAULT_PORTFOLIO_ALLOCATION_TTL_MS);
  return new Date(Math.min(...candidates)).toISOString();
}

export function allocatePortfolioRequest({
  state,
  source = {},
  request = {},
  now = new Date().toISOString(),
  policy: policyOverrides = {},
} = {}) {
  const nowMs = timestamp(now);
  if (nowMs == null) throw new TypeError('portfolio allocation evaluation time invalid');
  const policy = resolvePortfolioPolicy(state, policyOverrides);
  const requestedEnvelope = buildTradeIntentEnvelope(request);
  const requestedIntentHash = stableHash(requestedEnvelope);
  const bundle = requestBundle(request, state);
  const reasons = [];
  const missingRequiredState = [];
  const invalidRequiredState = [...bundle.invalid];
  const exitOnlyReasons = [];
  const scalingConstraints = [];
  const sources = [];
  const accountId = request?.accountId || null;
  const account = (state?.accounts || []).find(row => row?.id === accountId) || null;
  const navUsd = finite(account?.nav, null);
  const cashUsd = finite(account?.cash, null);
  if (!state || typeof state !== 'object') missingRequiredState.push('authoritative_state');
  if (!accountId) missingRequiredState.push('account_id');
  if (!account) missingRequiredState.push('account');
  if (account && (navUsd == null || navUsd <= 0)) missingRequiredState.push('account_nav');
  if (account && cashUsd == null) missingRequiredState.push('account_cash');
  if (account) sources.push(sourceRecord('account', account.id, iso(account.updatedAt ?? account.asOf ?? account.timestamp ?? source.observedAt), {
    id: account.id,
    navUsd,
    cashUsd,
    status: account.status ?? null,
    currency: account.currency ?? null,
  }));

  const sourceObservedAt = iso(source.observedAt);
  const sourceObservedMs = timestamp(source.observedAt);
  if (sourceObservedMs == null) missingRequiredState.push('authoritative_state_observed_at');
  else if (sourceObservedMs > nowMs + 1000 || nowMs - sourceObservedMs > DEFAULT_PORTFOLIO_ALLOCATION_TTL_MS) {
    invalidRequiredState.push('authoritative_state_stale');
  }

  const exposure = deriveExposureState(state, accountId, bundle.mode, policy, missingRequiredState);
  const maxConcurrentTrades = Math.max(1, finite(state?.config?.maxConcurrentTrades, 5));
  const activeTradeCount = exposure.positionIds.length + exposure.pendingExecutionIds.length;
  if (activeTradeCount >= maxConcurrentTrades) exitOnlyReasons.push('max_concurrent_trades_reached');
  const highWaterRecord = accountRiskRecord(state, accountId);
  const storedHighWaterUsd = finite(highWaterRecord?.highWaterNavUsd, null);
  const effectiveHighWaterUsd = navUsd != null && storedHighWaterUsd != null ? Math.max(navUsd, storedHighWaterUsd) : storedHighWaterUsd;
  const drawdownPct = effectiveHighWaterUsd != null && effectiveHighWaterUsd > 0 && navUsd != null
    ? Math.max(0, ((effectiveHighWaterUsd - navUsd) / effectiveHighWaterUsd) * 100)
    : null;
  if (storedHighWaterUsd == null || storedHighWaterUsd <= 0) {
    missingRequiredState.push('portfolio_high_water');
    exitOnlyReasons.push('high_water_mark_unavailable');
  } else {
    sources.push(sourceRecord('portfolio_high_water', accountId, iso(highWaterRecord?.highWaterAt ?? highWaterRecord?.updatedAt), highWaterRecord));
  }

  if (drawdownPct != null && drawdownPct >= policy.maxPortfolioDrawdownPct) {
    exitOnlyReasons.push('max_drawdown_reached');
  }

  if (navUsd != null && navUsd > 0) {
    if (exposure.grossExposureUsd > navUsd * policy.maxGrossLeverage + 1e-6) exitOnlyReasons.push('gross_leverage_already_exceeded');
    if (Math.abs(exposure.netExposureUsd) > navUsd * policy.maxNetExposurePct + 1e-6) exitOnlyReasons.push('net_exposure_already_exceeded');
  }

  const sourceFacts = { volatilitySources: {}, explicitCorrelationPairs: [] };
  const covariance = navUsd != null && navUsd > 0 && bundle.symbol && bundle.requestedNotionalUsd != null
    ? covarianceAllowedNotional(state, exposure.symbolExposure, bundle.symbol, bundle.requestedNotionalUsd, navUsd, policy, sourceFacts)
    : { currentRiskUsd: null, limitUsd: navUsd == null ? null : navUsd * policy.maxCovarianceRiskPct, allowedUsd: 0 };
  if (covariance.currentRiskUsd != null && covariance.limitUsd != null && covariance.currentRiskUsd >= covariance.limitUsd - 1e-9) {
    exitOnlyReasons.push('covariance_risk_already_exceeded');
  }

  const riskStateFacts = state?.portfolioRiskState && typeof state.portfolioRiskState === 'object'
    ? {
        updatedAt: state.portfolioRiskState.updatedAt ?? null,
        correlationMatrix: state.portfolioRiskState.correlationMatrix || {},
        volatilityBySymbol: state.portfolioRiskState.volatilityBySymbol || {},
      }
    : null;
  if (riskStateFacts) sources.push(sourceRecord('portfolio_risk_state', 'operator-state:portfolio-risk', iso(riskStateFacts.updatedAt), riskStateFacts));

  const targetMarketObservedMs = timestamp(bundle.market?.timestamp ?? bundle.market?.asOf ?? bundle.market?.createdAt);
  const spreadBps = marketSpreadBps(bundle.market);
  const volumeUsd = marketVolumeUsd(bundle.market, bundle.marketPrice);
  if (bundle.market) sources.push(sourceRecord('allocation_market_data', bundle.market.id || bundle.symbol, iso(bundle.market?.timestamp ?? bundle.market?.asOf ?? bundle.market?.createdAt), {
    id: bundle.market.id ?? null,
    symbol: bundle.market.symbol ?? null,
    venue: bundle.market.venue ?? null,
    status: bundle.market.status ?? null,
    referencePrice: bundle.marketPrice,
    spreadBps,
    volumeUsd,
  }));
  if (!bundle.market || bundle.marketPrice == null) missingRequiredState.push('market_data');
  else if (!MARKET_READY_STATUSES.has(String(bundle.market.status || '').toLowerCase())) invalidRequiredState.push('market_data_not_ready');
  if (spreadBps != null && spreadBps > policy.maxSpreadBps) exitOnlyReasons.push('liquidity_spread_limit');

  const increasing = INCREASING_SIDES.has(bundle.side);
  const reducing = REDUCING_SIDES.has(bundle.side);
  if (!increasing && !reducing && bundle.side) invalidRequiredState.push('order_side_unsupported');
  const requestedNotionalUsd = bundle.requestedNotionalUsd;
  let approvedNotionalUsd = requestedNotionalUsd || 0;

  const budgets = {};
  if (increasing && navUsd != null && navUsd > 0 && requestedNotionalUsd != null) {
    budgets.singleTradeUsd = navUsd * policy.maxSingleTradePct;
    budgets.cashReserveUsd = Math.max(0, (cashUsd ?? 0) - exposure.pendingBuyCashUsd - navUsd * policy.minCashBufferPct);
    budgets.grossExposureUsd = Math.max(0, navUsd * policy.maxGrossLeverage - exposure.grossExposureUsd);
    budgets.netExposureUsd = Math.max(0, navUsd * policy.maxNetExposurePct - Math.max(0, exposure.netExposureUsd));
    budgets.symbolExposureUsd = Math.max(0, navUsd * policy.maxSymbolExposurePct - Math.max(0, exposure.symbolExposure.get(bundle.symbol) || 0));
    const hardPositionLimitUsd = finite(state?.config?.maxPositionSizeUsd, null);
    if (hardPositionLimitUsd != null && hardPositionLimitUsd > 0) {
      budgets.canonicalPositionLimitUsd = Math.max(0, hardPositionLimitUsd - Math.max(0, exposure.symbolExposure.get(bundle.symbol) || 0));
    }
    const strategy = strategyKey(request?.strategyId);
    budgets.strategyExposureUsd = Math.max(0, navUsd * policy.maxStrategyExposurePct - Math.max(0, exposure.strategyExposure.get(strategy) || 0));
    const cluster = clusterForSymbol(bundle.symbol, policy);
    budgets.clusterExposureUsd = Math.max(0, navUsd * policy.maxCorrelationClusterExposurePct - Math.max(0, exposure.clusterGrossExposure.get(cluster) || 0));
    const throttle = drawdownPct == null || drawdownPct <= policy.drawdownThrottleStartPct
      ? 1
      : clamp(
          (policy.maxPortfolioDrawdownPct - drawdownPct)
            / Math.max(1e-9, policy.maxPortfolioDrawdownPct - policy.drawdownThrottleStartPct),
          0,
          1,
        );
    budgets.drawdownThrottleUsd = requestedNotionalUsd * throttle;
    budgets.covarianceRiskUsd = Math.max(0, covariance.allowedUsd || 0);
    budgets.liquidityUsd = volumeUsd != null && volumeUsd > 0
      ? volumeUsd * policy.maxLiquidityParticipationPct
      : navUsd * policy.unknownLiquidityMaxTradePct;

    for (const [name, value] of Object.entries(budgets)) {
      if (Number.isFinite(value) && value < approvedNotionalUsd - 1e-6) scalingConstraints.push(name);
      if (Number.isFinite(value)) approvedNotionalUsd = Math.min(approvedNotionalUsd, Math.max(0, value));
    }
    if (exitOnlyReasons.length) approvedNotionalUsd = 0;
  }

  if (missingRequiredState.length || invalidRequiredState.length) {
    if (increasing) approvedNotionalUsd = 0;
  }

  if (reducing) approvedNotionalUsd = requestedNotionalUsd || 0;

  if (approvedNotionalUsd > 0 && approvedNotionalUsd < policy.minApprovedNotionalUsd) {
    scalingConstraints.push('minimum_approved_notional');
    approvedNotionalUsd = 0;
  }

  const preliminaryScale = requestedNotionalUsd && requestedNotionalUsd > 0
    ? clamp(approvedNotionalUsd / requestedNotionalUsd, 0, 1)
    : 0;
  const exactEconomicNotional = economicLineagePresent(request);
  let approved = approvedNotionalUsd > 0 && missingRequiredState.length === 0 && invalidRequiredState.length === 0;
  let decision = 'REJECT';
  let suggestedNotionalUsd = null;

  if (reducing && approvedNotionalUsd > 0 && invalidRequiredState.length === 0) {
    approved = true;
    decision = exitOnlyReasons.length ? 'EXIT_ONLY' : 'APPROVE';
  } else if (increasing && approved) {
    decision = preliminaryScale >= 1 - 1e-9 ? 'APPROVE' : 'REDUCE';
  }

  if (increasing && exactEconomicNotional && preliminaryScale > 0 && preliminaryScale < 1 - 1e-9) {
    suggestedNotionalUsd = approvedNotionalUsd;
    approvedNotionalUsd = 0;
    approved = false;
    decision = 'REJECT';
    reasons.push('economic_reprice_required_after_allocation');
  }

  if (increasing && exitOnlyReasons.length) reasons.push(...exitOnlyReasons.map(reason => `exit_only:${reason}`));
  if (missingRequiredState.length) reasons.push(...unique(missingRequiredState).sort().map(reason => `portfolio_missing:${reason}`));
  if (invalidRequiredState.length) reasons.push(...unique(invalidRequiredState).sort().map(reason => `portfolio_invalid:${reason}`));
  if (scalingConstraints.length && decision === 'REDUCE') reasons.push(...unique(scalingConstraints).sort().map(reason => `portfolio_scaled:${reason}`));
  if (!approved && reasons.length === 0) reasons.push('portfolio_allocation_rejected');

  const finalScale = requestedNotionalUsd && requestedNotionalUsd > 0
    ? clamp(approvedNotionalUsd / requestedNotionalUsd, 0, 1)
    : 0;
  const executionRequest = approved && increasing ? scaleRequest(request, finalScale) : structuredClone(request);
  const approvedEnvelope = buildTradeIntentEnvelope(executionRequest);
  const approvedIntentHash = stableHash(approvedEnvelope);
  const decisionCore = {
    decision,
    approved,
    reasons: unique(reasons),
    requestedNotionalUsd,
    approvedNotionalUsd,
    suggestedNotionalUsd,
    scale: finalScale,
    exitOnly: exitOnlyReasons.length > 0,
  };
  const allocationDecisionHash = stableHash(decisionCore);
  const validUntil = allocationExpiry(nowMs, sourceObservedMs, targetMarketObservedMs);
  const core = {
    schemaVersion: PORTFOLIO_ALLOCATION_SCHEMA_VERSION,
    policyVersion: PORTFOLIO_ALLOCATION_POLICY_VERSION,
    requestedIntentHash,
    approvedIntentHash,
    accountId,
    strategyId: request?.strategyId || null,
    mode: bundle.mode,
    symbol: bundle.symbol,
    venue: bundle.venue,
    side: bundle.side,
    observedAt: new Date(nowMs).toISOString(),
    validUntil,
    decision,
    approved,
    reasons: decisionCore.reasons,
    allocationDecisionHash,
    requestedNotionalUsd,
    approvedNotionalUsd,
    suggestedNotionalUsd,
    scale: finalScale,
    exitOnly: exitOnlyReasons.length > 0,
    exitOnlyReasons: unique(exitOnlyReasons).sort(),
    scalingConstraints: unique(scalingConstraints).sort(),
    policy,
    budgets,
    accountState: {
      navUsd,
      cashUsd,
      highWaterNavUsd: storedHighWaterUsd,
      effectiveHighWaterNavUsd: effectiveHighWaterUsd,
      drawdownPct,
      pendingBuyCashUsd: exposure.pendingBuyCashUsd,
      activeTradeCount,
      maxConcurrentTrades,
    },
    exposureState: {
      grossExposureUsd: exposure.grossExposureUsd,
      netExposureUsd: exposure.netExposureUsd,
      symbolExposureUsd: mapObject(exposure.symbolExposure),
      strategyExposureUsd: mapObject(exposure.strategyExposure),
      clusterGrossExposureUsd: mapObject(exposure.clusterGrossExposure),
      positionIds: exposure.positionIds,
      pendingExecutionIds: exposure.pendingExecutionIds,
    },
    covarianceState: {
      currentRiskUsd: covariance.currentRiskUsd,
      fullRequestedRiskUsd: covariance.fullRiskUsd ?? null,
      limitUsd: covariance.limitUsd,
      allowedNotionalUsd: covariance.allowedUsd,
      volatilitySources: sourceFacts.volatilitySources,
      explicitCorrelationPairs: unique(sourceFacts.explicitCorrelationPairs).sort(),
    },
    liquidityState: {
      spreadBps,
      maxSpreadBps: policy.maxSpreadBps,
      volumeUsd,
      maxParticipationPct: policy.maxLiquidityParticipationPct,
      unknownLiquidityFallback: volumeUsd == null,
    },
    missingRequiredState: unique(missingRequiredState).sort(),
    invalidRequiredState: unique(invalidRequiredState).sort(),
    sources,
  };
  return {
    allocation: { ...core, allocationHash: stableHash(core) },
    executionRequest,
  };
}

export function verifyPortfolioAllocationSnapshot(snapshot, {
  tradeIntentHash,
  now = new Date().toISOString(),
  requireApproved = true,
} = {}) {
  const reasons = [];
  if (!snapshot || typeof snapshot !== 'object') return { ok: false, reasons: ['portfolio_allocation_required'] };
  if (snapshot.schemaVersion !== PORTFOLIO_ALLOCATION_SCHEMA_VERSION || snapshot.policyVersion !== PORTFOLIO_ALLOCATION_POLICY_VERSION) {
    reasons.push('portfolio_allocation_policy_version_mismatch');
  }
  if (tradeIntentHash && snapshot.approvedIntentHash !== tradeIntentHash) reasons.push('portfolio_allocation_intent_hash_mismatch');
  const core = { ...snapshot };
  const suppliedHash = core.allocationHash;
  delete core.allocationHash;
  try {
    if (!suppliedHash || stableHash(core) !== suppliedHash) reasons.push('portfolio_allocation_hash_mismatch');
  } catch {
    reasons.push('portfolio_allocation_not_canonicalizable');
  }
  const decisionCore = {
    decision: snapshot.decision,
    approved: snapshot.approved,
    reasons: Array.isArray(snapshot.reasons) ? snapshot.reasons : [],
    requestedNotionalUsd: snapshot.requestedNotionalUsd ?? null,
    approvedNotionalUsd: snapshot.approvedNotionalUsd ?? null,
    suggestedNotionalUsd: snapshot.suggestedNotionalUsd ?? null,
    scale: snapshot.scale ?? null,
    exitOnly: Boolean(snapshot.exitOnly),
  };
  try {
    if (!snapshot.allocationDecisionHash || stableHash(decisionCore) !== snapshot.allocationDecisionHash) {
      reasons.push('portfolio_allocation_decision_hash_mismatch');
    }
  } catch {
    reasons.push('portfolio_allocation_decision_not_canonicalizable');
  }
  const nowMs = timestamp(now);
  const validUntilMs = timestamp(snapshot.validUntil);
  if (nowMs == null || validUntilMs == null) reasons.push('portfolio_allocation_expiry_invalid');
  else if (nowMs > validUntilMs) reasons.push('portfolio_allocation_expired');
  if (requireApproved && snapshot.approved !== true) {
    reasons.push(...(Array.isArray(snapshot.reasons) && snapshot.reasons.length ? snapshot.reasons : ['portfolio_allocation_rejected']));
  }
  return { ok: reasons.length === 0, reasons: unique(reasons) };
}
