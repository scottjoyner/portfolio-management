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
    return { status: missing ? 404 : 400, body: { ok: false, errors: result.errors } };
  }
  return { status: successStatus, body: { ok: true, ...result } };
}

async function fetchOpenRouterCatalog(env = process.env, fetchImpl = globalThis.fetch) {
  if (typeof fetchImpl !== 'function') throw new Error('fetch_unavailable');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Number(env.OPENROUTER_PRICING_TIMEOUT_MS || 10000));
  try {
    const headers = { accept: 'application/json' };
    if (env.OPENROUTER_API_KEY) headers.authorization = `Bearer ${env.OPENROUTER_API_KEY}`;
    if (env.OPENROUTER_APP_URL) headers['HTTP-Referer'] = env.OPENROUTER_APP_URL;
    if (env.OPENROUTER_APP_NAME) headers['X-Title'] = env.OPENROUTER_APP_NAME;
    const response = await fetchImpl(env.OPENROUTER_MODELS_URL || 'https://openrouter.ai/api/v1/models', {
      method: 'GET',
      headers,
      signal: controller.signal,
    });
    if (!response.ok) throw new Error(`openrouter_models_http_${response.status}`);
    return response.json();
  } finally {
    clearTimeout(timeout);
  }
}

function stateObservations(state, body = {}) {
  if (Array.isArray(body.observations) || Array.isArray(body.prices)) return body;
  const symbol = body.symbol;
  const snapshots = (state.marketDataSnapshots || [])
    .filter(row => !symbol || row.symbol === symbol || row.marketId === symbol)
    .map(row => ({
      price: row.mid ?? row.price ?? row.close ?? row.lastPrice,
      timestamp: row.timestamp || row.asOf || row.createdAt,
      volume: row.volume ?? row.volume24h,
    }))
    .filter(row => Number(row.price) > 0)
    .sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0));
  return { ...body, observations: snapshots };
}

function latestForSymbol(rows = [], symbol) {
  return [...rows]
    .filter(row => !symbol || row.symbol === symbol)
    .sort((a, b) => new Date(b.createdAt || b.asOf || b.requestedAt || 0) - new Date(a.createdAt || a.asOf || a.requestedAt || 0))[0] || null;
}

async function refreshCoinbaseEconomics(state, body = {}) {
  const symbol = body.symbol || 'BTC-USD';
  const side = String(body.side || 'buy').toLowerCase() === 'sell' ? 'sell' : 'buy';
  const notionalUsd = Number(body.notionalUsd || body.notional || 0);
  if (!Number.isFinite(notionalUsd) || notionalUsd <= 0) return { errors: ['execution_notional_required'] };
  const { getDefaultRegistry } = await import('../../../packages/adapters/src/adapterRegistry.mjs');
  const registry = getDefaultRegistry();
  const adapter = registry.getAdapterForVenue('coinbase');
  if (!adapter) return { errors: ['coinbase_adapter_unavailable'] };
  try { await adapter.connect?.(); } catch { /* preview can still fall back safely */ }
  const quote = await adapter.getQuote(symbol);
  if (!quote || Number(quote.mid) <= 0) return { errors: ['coinbase_quote_unavailable'] };
  const quantity = Number(body.quantity || (notionalUsd / Number(quote.mid)));
  const feeSummaryPromise = typeof adapter.getFeeSummary === 'function'
    ? adapter.getFeeSummary()
    : typeof adapter._cli === 'function'
      ? adapter._cli('transaction_summary')
      : Promise.resolve(null);
  const [feeSummary, previewResult] = await Promise.all([
    feeSummaryPromise,
    adapter.previewOrder({
      symbol,
      marketId: symbol,
      side,
      quantity,
      notional: notionalUsd,
      price: quote.mid,
      slippageBps: quote.spreadBps || 0,
    }),
  ]);
  if (!previewResult?.ok) return { errors: previewResult?.errors || ['coinbase_preview_unavailable'] };
  return ingestExecutionCostSnapshot(state, {
    venue: 'coinbase',
    symbol,
    side,
    notionalUsd,
    quantity,
    referencePrice: quote.mid,
    spreadBps: quote.spreadBps,
    liquidity: previewResult.preview?.liquidity || 'taker',
    feeSummary: feeSummary?.data || feeSummary || {},
    preview: previewResult.preview || {},
    source: feeSummary ? 'coinbase_preview_and_transaction_summary' : 'coinbase_preview',
  });
}

export function isEconomicRoute(pathname) {
  return pathname === '/api/economics/dashboard'
    || pathname === '/api/economics/model-pricing'
    || pathname === '/api/economics/model-pricing/refresh'
    || pathname === '/api/economics/model-quotes'
    || pathname === '/api/economics/model-usage/reconcile'
    || pathname === '/api/economics/forecasts'
    || pathname === '/api/economics/execution-costs'
    || pathname === '/api/economics/coinbase/refresh'
    || pathname === '/api/economics/decisions/evaluate'
    || pathname === '/api/economics/attribution'
    || pathname === '/api/economics/calibration'
    || /^\/api\/economics\/forecasts\/[^/]+\/outcome$/.test(pathname);
}

export async function handleEconomicRoute({ method, pathname, state, store, readJsonBody, env = process.env, fetchImpl = globalThis.fetch }) {
  if (!isEconomicRoute(pathname)) return null;
  ensureEconomicState(state);

  if (method === 'GET' && pathname === '/api/economics/dashboard') {
    return { status: 200, body: { ok: true, ...economicDashboard(state) } };
  }
  if (method === 'GET' && pathname === '/api/economics/model-pricing') {
    return { status: 200, body: { ok: true, snapshots: state.modelPricingSnapshots, latest: state.modelPricingSnapshots.at(-1) || null } };
  }
  if (method === 'GET' && pathname === '/api/economics/forecasts') {
    return { status: 200, body: { ok: true, forecasts: state.priceForecasts } };
  }
  if (method === 'GET' && pathname === '/api/economics/execution-costs') {
    return { status: 200, body: { ok: true, executionCosts: state.executionCostSnapshots } };
  }
  if (method === 'GET' && pathname === '/api/economics/attribution') {
    return { status: 200, body: { ok: true, records: state.agentAttributionRecords, summary: summarizeAgentAttribution(state) } };
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
    return mutate(store, current => ingestModelPricingCatalog(current, { ...body, catalog }), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/model-quotes') {
    const body = await readJsonBody();
    return mutate(store, current => quoteModelRequest(current, body), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/model-usage/reconcile') {
    const body = await readJsonBody();
    return mutate(store, current => reconcileModelUsage(current, body));
  }

  if (method === 'POST' && pathname === '/api/economics/forecasts') {
    const body = await readJsonBody();
    return mutate(store, current => buildPriceForecast(current, stateObservations(current, body)), 201);
  }

  const forecastOutcome = routeMatch(pathname, '/api/economics/forecasts/:id/outcome');
  if (method === 'POST' && forecastOutcome) {
    const body = await readJsonBody();
    return mutate(store, current => recordForecastOutcome(current, { ...body, forecastId: forecastOutcome.id }), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/execution-costs') {
    const body = await readJsonBody();
    return mutate(store, current => ingestExecutionCostSnapshot(current, body), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/coinbase/refresh') {
    const body = await readJsonBody();
    return mutate(store, current => refreshCoinbaseEconomics(current, body), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/decisions/evaluate') {
    const body = await readJsonBody();
    return mutate(store, current => {
      const symbol = body.symbol || current.opportunities?.find(row => row.id === body.opportunityId)?.symbol;
      const enriched = {
        ...body,
        forecast: body.forecast || (!body.forecastId ? latestForSymbol(current.priceForecasts, symbol) : undefined),
        modelQuote: body.modelQuote || (!body.modelQuoteId ? latestForSymbol(current.modelUsageLedger, null) : undefined),
        executionCostSnapshot: body.executionCostSnapshot || (!body.executionCostSnapshotId ? latestForSymbol(current.executionCostSnapshots, symbol) : undefined),
      };
      const result = evaluateEconomicDecision(current, enriched);
      if (result.economicDecision && body.opportunityId) {
        const opportunity = current.opportunities?.find(row => row.id === body.opportunityId);
        if (opportunity) {
          opportunity.economicDecisionId = result.economicDecision.id;
          opportunity.economicExecutionAllowed = result.economicDecision.executionAllowed;
          opportunity.economicDecisionBlockers = result.economicDecision.blockers;
          opportunity.updatedAt = new Date().toISOString();
        }
      }
      return result;
    }, 201);
  }

  if (method === 'POST' && pathname === '/api/economics/attribution') {
    const body = await readJsonBody();
    return mutate(store, current => recordAgentAttribution(current, body), 201);
  }

  return { status: 405, body: { ok: false, error: 'method_not_allowed' } };
}
