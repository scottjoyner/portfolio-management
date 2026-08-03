import {
  buildPriceForecast,
  economicDashboard,
  ensureEconomicState,
  evaluateEconomicDecision,
  ingestExecutionCostSnapshot,
  ingestModelPricingCatalog,
  quoteModelRequest,
  reconcileModelUsage,
  recordAgentAttribution,
  recordForecastOutcome,
  summarizeAgentAttribution,
  summarizeForecastCalibration,
} from '../../../packages/economics/src/economicDecisionEngine.mjs';
import { fetchOpenRouterCatalog, runEconomicMaintenance } from './economicMaintenance.mjs';
import {
  discoverLocalIntelligenceNodes,
  executeEconomicIntelligence,
  quoteLocalIntelligence,
} from './intelligenceExecution.mjs';

function routeMatch(pathname, pattern) {
  const pathParts = pathname.split('/').filter(Boolean);
  const patternParts = pattern.split('/').filter(Boolean);
  if (pathParts.length !== patternParts.length) return null;
  const params = {};
  for (let index = 0; index < patternParts.length; index += 1) {
    if (patternParts[index].startsWith(':')) params[patternParts[index].slice(1)] = decodeURIComponent(pathParts[index]);
    else if (patternParts[index] !== pathParts[index]) return null;
  }
  return params;
}

async function mutate(store, fn, successStatus = 200) {
  const result = await store.mutate(async state => {
    ensureEconomicState(state);
    return fn(state);
  });
  if (result?.errors?.length) {
    const missing = result.errors.some(error => error.endsWith('_not_found'));
    const conflict = result.errors.some(error => error.endsWith('_conflict') || error.includes('already_'));
    return { status: missing ? 404 : conflict ? 409 : 400, body: { ok: false, errors: result.errors } };
  }
  return { status: successStatus, body: { ok: true, ...result } };
}

function stateObservations(state, body = {}) {
  if (Array.isArray(body.observations) || Array.isArray(body.prices)) return body;
  const symbol = body.symbol;
  const snapshots = (state.marketDataSnapshots || [])
    .filter(row => !symbol || row.symbol === symbol || row.marketId === symbol)
    .map(row => ({
      id: row.id,
      price: row.mid ?? row.price ?? row.close ?? row.lastPrice ?? (Number(row.bid) > 0 && Number(row.ask) > 0 ? (Number(row.bid) + Number(row.ask)) / 2 : null),
      timestamp: row.timestamp || row.asOf || row.createdAt,
      volume: row.volume ?? row.volume24h,
      spreadBps: row.spreadBps,
    }))
    .filter(row => Number(row.price) > 0)
    .sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0));
  return { ...body, observations: snapshots, sourceSnapshotIds: snapshots.map(row => row.id).filter(Boolean) };
}

function latestForSymbol(rows = [], symbol) {
  return [...rows]
    .filter(row => !symbol || row.symbol === symbol)
    .sort((a, b) => new Date(b.createdAt || b.asOf || b.requestedAt || 0) - new Date(a.createdAt || a.asOf || a.requestedAt || 0))[0] || null;
}

function quoteForOpportunity(rows = [], opportunityId) {
  if (!opportunityId) return null;
  return [...rows]
    .filter(row => row.opportunityId === opportunityId)
    .sort((a, b) => new Date(b.requestedAt || 0) - new Date(a.requestedAt || 0))[0] || null;
}

async function fetchCoinbaseEconomics(body = {}) {
  const symbol = body.symbol || 'BTC-USD';
  const side = String(body.side || 'buy').toLowerCase() === 'sell' ? 'sell' : 'buy';
  const notionalUsd = Number(body.notionalUsd || body.notional || 0);
  if (!Number.isFinite(notionalUsd) || notionalUsd <= 0) return { errors: ['execution_notional_required'] };

  const { getDefaultRegistry } = await import('../../../packages/adapters/src/adapterRegistry.mjs');
  const registry = getDefaultRegistry();
  const adapter = registry.getAdapterForVenue('coinbase');
  if (!adapter) return { errors: ['coinbase_adapter_unavailable'] };
  try { await adapter.connect?.(); } catch { /* read-only preview may still be available */ }

  const quote = await adapter.getQuote(symbol);
  if (!quote || Number(quote.mid) <= 0) return { errors: ['coinbase_quote_unavailable'] };
  const quantity = Number(body.quantity || (notionalUsd / Number(quote.mid)));
  const previewPayload = {
    product_id: symbol,
    side,
    base_size: side === 'sell' ? String(quantity) : undefined,
    quote_size: side === 'buy' ? String(notionalUsd) : undefined,
  };

  const rawFeeResult = typeof adapter.getFeeSummary === 'function'
    ? await adapter.getFeeSummary()
    : typeof adapter._cli === 'function'
      ? await adapter._cli('transaction_summary')
      : null;
  const feeSummary = rawFeeResult?.ok === false ? null : (rawFeeResult?.data || rawFeeResult || null);

  let preview = null;
  if (typeof adapter._cli === 'function') {
    const raw = await adapter._cli('preview_order', previewPayload);
    if (raw?.ok && raw.data) preview = raw.data;
  }
  if (!preview) {
    const parsed = await adapter.previewOrder({
      symbol,
      marketId: symbol,
      side,
      quantity,
      notional: notionalUsd,
      price: quote.mid,
      slippageBps: quote.spreadBps || 0,
    });
    if (!parsed?.ok) return { errors: parsed?.errors || ['coinbase_preview_unavailable'] };
    preview = parsed.preview || {};
  }

  return {
    snapshotInput: {
      venue: 'coinbase',
      symbol,
      side,
      notionalUsd,
      quantity,
      referencePrice: quote.mid,
      spreadBps: quote.spreadBps,
      liquidity: preview.liquidity || 'taker',
      feeSummary: feeSummary || {},
      preview,
      source: feeSummary ? 'coinbase_preview_and_transaction_summary' : 'coinbase_preview',
    },
  };
}

async function fetchMaintenanceEvidence(body = {}, env = process.env, fetchImpl = globalThis.fetch) {
  let catalog = body.catalog;
  let quotes = body.quotes;

  if (!catalog) {
    try {
      catalog = await fetchOpenRouterCatalog(env, fetchImpl);
    } catch (error) {
      return { errors: [`pricing_refresh_failed:${String(error?.message || error)}`] };
    }
  }

  if (!quotes) {
    try {
      const { fetchQuotes } = await import('../../../packages/execution/src/paperSweeper.mjs');
      quotes = await fetchQuotes();
    } catch (error) {
      return { errors: [`market_quote_refresh_failed:${String(error?.message || error)}`] };
    }
  }

  return { catalog, quotes };
}

export function isEconomicRoute(pathname) {
  return pathname === '/api/economics/dashboard'
    || pathname === '/api/economics/model-pricing'
    || pathname === '/api/economics/model-pricing/refresh'
    || pathname === '/api/economics/model-quotes'
    || pathname === '/api/economics/model-usage/reconcile'
    || pathname === '/api/economics/intelligence/nodes'
    || pathname === '/api/economics/intelligence/execute'
    || pathname === '/api/economics/maintenance/run'
    || pathname === '/api/economics/forecasts'
    || pathname === '/api/economics/execution-costs'
    || pathname === '/api/economics/coinbase/refresh'
    || pathname === '/api/economics/decisions/evaluate'
    || pathname === '/api/economics/attribution'
    || pathname === '/api/economics/calibration'
    || /^\/api\/economics\/forecasts\/[^/]+\/outcome$/.test(pathname);
}

export async function handleEconomicRoute({ method, pathname, state, store, readJsonBody, env = process.env, fetchImpl = globalThis.fetch, now = new Date().toISOString() }) {
  if (!isEconomicRoute(pathname)) return null;
  ensureEconomicState(state);

  if (method === 'GET' && pathname === '/api/economics/dashboard') {
    return { status: 200, body: { ok: true, ...economicDashboard(state) } };
  }
  if (method === 'GET' && pathname === '/api/economics/model-pricing') {
    return { status: 200, body: { ok: true, snapshots: state.modelPricingSnapshots, latest: state.modelPricingSnapshots.at(-1) || null } };
  }
  if (method === 'GET' && pathname === '/api/economics/intelligence/nodes') {
    return discoverLocalIntelligenceNodes({ env, fetchImpl });
  }
  if (method === 'GET' && pathname === '/api/economics/forecasts') {
    return { status: 200, body: { ok: true, forecasts: state.priceForecasts } };
  }
  if (method === 'GET' && pathname === '/api/economics/execution-costs') {
    return { status: 200, body: { ok: true, executionCosts: state.executionCostSnapshots } };
  }
  if (method === 'GET' && pathname === '/api/economics/attribution') {
    return { status: 200, body: { ok: true, records: state.agentAttributionRecords, pending: state.economicAttributionQueue, summary: summarizeAgentAttribution(state) } };
  }
  if (method === 'GET' && pathname === '/api/economics/calibration') {
    return { status: 200, body: { ok: true, outcomes: state.forecastOutcomes, summary: summarizeForecastCalibration(state) } };
  }

  if (method === 'POST' && pathname === '/api/economics/model-pricing/refresh') {
    const body = await readJsonBody();
    let catalog = body.catalog;
    if (!catalog) {
      try {
        catalog = await fetchOpenRouterCatalog(env, fetchImpl);
      } catch (error) {
        return { status: 503, body: { ok: false, error: String(error?.message || error) } };
      }
    }
    return mutate(store, current => ingestModelPricingCatalog(current, { ...body, catalog }, now), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/model-quotes') {
    const body = await readJsonBody();
    if (body.localOrRemote === 'local' || (env.LOCAL_LLM_EXECUTION_REQUIRED === 'true' && body.localOrRemote !== 'remote')) {
      return quoteLocalIntelligence({ store, body: { ...body, localOrRemote: 'local' }, env, fetchImpl, now });
    }
    if (env.REMOTE_LLM_EXECUTION_ENABLED !== 'true') {
      return { status: 409, body: { ok: false, errors: ['remote_llm_execution_disabled'] } };
    }
    return mutate(store, current => quoteModelRequest(current, body, now), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/model-usage/reconcile') {
    const body = await readJsonBody();
    return mutate(store, current => reconcileModelUsage(current, body, now));
  }

  if (method === 'POST' && pathname === '/api/economics/intelligence/execute') {
    const body = await readJsonBody();
    return executeEconomicIntelligence({ store, body, env, fetchImpl, now });
  }

  if (method === 'POST' && pathname === '/api/economics/maintenance/run') {
    const body = await readJsonBody();
    const evidence = await fetchMaintenanceEvidence(body, env, fetchImpl);
    if (evidence.errors) return { status: 503, body: { ok: false, errors: evidence.errors } };
    return mutate(store, current => runEconomicMaintenance(current, {
      now,
      env,
      catalog: evidence.catalog,
      quotes: evidence.quotes,
    }));
  }

  if (method === 'POST' && pathname === '/api/economics/forecasts') {
    const body = await readJsonBody();
    return mutate(store, current => buildPriceForecast(current, stateObservations(current, body), now), 201);
  }

  const forecastOutcome = routeMatch(pathname, '/api/economics/forecasts/:id/outcome');
  if (method === 'POST' && forecastOutcome) {
    const body = await readJsonBody();
    return mutate(store, current => recordForecastOutcome(current, { ...body, forecastId: forecastOutcome.id }, now), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/execution-costs') {
    const body = await readJsonBody();
    return mutate(store, current => ingestExecutionCostSnapshot(current, body, now), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/coinbase/refresh') {
    const body = await readJsonBody();
    let evidence;
    try {
      evidence = await fetchCoinbaseEconomics(body);
    } catch (error) {
      return { status: 503, body: { ok: false, errors: [`coinbase_refresh_failed:${String(error?.message || error)}`] } };
    }
    if (evidence.errors) return { status: 400, body: { ok: false, errors: evidence.errors } };
    return mutate(store, current => ingestExecutionCostSnapshot(current, evidence.snapshotInput, now), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/decisions/evaluate') {
    const body = await readJsonBody();
    return mutate(store, current => {
      const opportunity = current.opportunities?.find(row => row.id === body.opportunityId);
      const symbol = body.symbol || opportunity?.symbol;
      const economicModelQuote = body.modelQuote
        || (body.modelQuoteId ? undefined : quoteForOpportunity(current.modelUsageLedger, body.opportunityId));
      const enriched = {
        ...body,
        requiredCostCoverageMultiple: body.requiredCostCoverageMultiple ?? current.config?.requiredIntelligenceCostCoverageMultiple,
        forecast: body.forecast || (!body.forecastId ? latestForSymbol(current.priceForecasts, symbol) : undefined),
        modelQuote: economicModelQuote,
        executionCostSnapshot: body.executionCostSnapshot || (!body.executionCostSnapshotId ? latestForSymbol(current.executionCostSnapshots, symbol) : undefined),
      };
      const result = evaluateEconomicDecision(current, enriched, now);
      if (result.economicDecision && opportunity) {
        opportunity.economicDecisionId = result.economicDecision.id;
        opportunity.modelQuoteId = result.economicDecision.modelQuoteId;
        opportunity.forecastId = result.economicDecision.forecastId;
        opportunity.executionCostSnapshotId = result.economicDecision.executionCostSnapshotId;
        opportunity.netExecutableEdgeUsd = result.economicDecision.netExecutableEdgeUsd;
        opportunity.economicExecutionAllowed = result.economicDecision.executionAllowed;
        opportunity.economicDecisionBlockers = result.economicDecision.blockers;
        opportunity.updatedAt = now;
      }
      return result;
    }, 201);
  }

  if (method === 'POST' && pathname === '/api/economics/attribution') {
    const body = await readJsonBody();
    return mutate(store, current => recordAgentAttribution(current, body, now), 201);
  }

  return { status: 405, body: { ok: false, error: 'method_not_allowed' } };
}
