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

async function refreshCoinbaseEconomics(state, body = {}) {
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

  return ingestExecutionCostSnapshot(state, {
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
  });
}

function openRouterHeaders(env) {
  const headers = {
    accept: 'application/json',
    'content-type': 'application/json',
    authorization: `Bearer ${env.OPENROUTER_API_KEY}`,
  };
  if (env.OPENROUTER_APP_URL) headers['HTTP-Referer'] = env.OPENROUTER_APP_URL;
  if (env.OPENROUTER_APP_NAME) headers['X-Title'] = env.OPENROUTER_APP_NAME;
  return headers;
}

async function fetchGenerationUsage(generationId, env, fetchImpl, signal) {
  if (!generationId) return null;
  const endpoint = env.OPENROUTER_GENERATION_URL || 'https://openrouter.ai/api/v1/generation';
  const response = await fetchImpl(`${endpoint}?id=${encodeURIComponent(generationId)}`, {
    method: 'GET',
    headers: openRouterHeaders(env),
    signal,
  });
  if (!response.ok) return null;
  const payload = await response.json();
  const data = payload?.data || payload;
  if (!data) return null;
  return {
    prompt_tokens: Number(data.tokens_prompt || data.native_tokens_prompt || 0),
    completion_tokens: Number(data.tokens_completion || data.native_tokens_completion || 0),
    cost: Number(data.total_cost),
    native_tokens_reasoning: Number(data.native_tokens_reasoning || 0),
    native_tokens_cached: Number(data.native_tokens_cached || 0),
    upstream_inference_cost: Number(data.upstream_inference_cost || 0),
    generation: data,
  };
}

async function executeOpenRouterIntelligence(state, body, env, fetchImpl, now) {
  ensureEconomicState(state);
  if (!env.OPENROUTER_API_KEY) return { errors: ['openrouter_api_key_required'] };
  if (typeof fetchImpl !== 'function') return { errors: ['fetch_unavailable'] };
  const quote = state.modelUsageLedger.find(row => row.id === body.modelQuoteId);
  const decision = state.economicDecisions.find(row => row.id === body.economicDecisionId);
  const errors = [];
  if (!quote) errors.push('model_quote_not_found');
  if (!decision) errors.push('economic_decision_not_found');
  if (quote && quote.localOrRemote !== 'remote') errors.push('remote_model_quote_required');
  if (quote && !['quoted', 'failed', 'usage_pending'].includes(quote.status)) errors.push('model_quote_already_consumed');
  if (decision && decision.intelligenceAllowed !== true) errors.push('intelligence_purchase_not_economic');
  if (quote && decision && decision.modelQuoteId !== quote.id) errors.push('economic_decision_model_quote_mismatch');
  const messages = Array.isArray(body.messages) && body.messages.length
    ? body.messages
    : body.prompt
      ? [{ role: 'user', content: String(body.prompt) }]
      : [];
  if (!messages.length) errors.push('model_messages_required');
  if (errors.length) return { errors };

  const job = body.researchJobId ? state.researchJobs?.find(row => row.id === body.researchJobId) : null;
  if (body.researchJobId && !job) return { errors: ['research_job_not_found'] };
  const costRow = job ? state.agentCostLedger?.find(row => row.jobId === job.id) : null;
  quote.researchJobId = job?.id || quote.researchJobId || null;
  quote.status = 'running';
  quote.startedAt = now;
  if (job) {
    job.status = 'running';
    job.startedAt ||= now;
    job.completedAt = null;
    job.modelQuoteId = quote.id;
    job.economicDecisionId = decision.id;
    job.pricingSnapshotId = quote.pricingSnapshotId;
  }
  if (costRow) {
    costRow.modelQuoteId = quote.id;
    costRow.economicDecisionId = decision.id;
    costRow.pricingSnapshotId = quote.pricingSnapshotId;
    costRow.remoteApiCost = quote.estimatedCostUsd;
    costRow.costSource = 'pre_call_estimate';
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), Number(env.OPENROUTER_COMPLETION_TIMEOUT_MS || 120000));
  try {
    const requestBody = {
      model: quote.model,
      messages,
      usage: { include: true },
      provider: quote.providerPreferences || undefined,
      max_tokens: Number(body.maxCompletionTokens || quote.completionTokens || 0) || undefined,
      temperature: Number.isFinite(Number(body.temperature)) ? Number(body.temperature) : 0.2,
      response_format: body.responseFormat,
      tools: body.tools,
      tool_choice: body.toolChoice,
    };
    for (const key of Object.keys(requestBody)) if (requestBody[key] === undefined) delete requestBody[key];
    const response = await fetchImpl(env.OPENROUTER_CHAT_URL || 'https://openrouter.ai/api/v1/chat/completions', {
      method: 'POST',
      headers: openRouterHeaders(env),
      body: JSON.stringify(requestBody),
      signal: controller.signal,
    });
    const payload = await response.json().catch(() => ({}));
    if (!response.ok) {
      quote.status = 'failed';
      quote.failureReason = payload?.error?.message || `openrouter_chat_http_${response.status}`;
      if (job) {
        job.status = 'failed';
        job.failureReason = quote.failureReason;
        job.completedAt = new Date().toISOString();
      }
      return { errors: [quote.failureReason] };
    }

    let usage = payload.usage || null;
    if (!Number.isFinite(Number(usage?.cost))) usage = await fetchGenerationUsage(payload.id, env, fetchImpl, controller.signal) || usage;
    if (!Number.isFinite(Number(usage?.cost))) {
      quote.status = 'usage_pending';
      quote.generationId = payload.id || null;
      quote.failureReason = 'provider_usage_cost_unavailable';
      if (job) {
        job.status = 'running';
        job.failureReason = quote.failureReason;
      }
      return { errors: ['provider_usage_cost_unavailable'] };
    }

    const reconciled = reconcileModelUsage(state, {
      quoteId: quote.id,
      generationId: payload.id,
      usage,
      jobStatus: 'completed',
    }, new Date().toISOString());
    if (reconciled.errors) return reconciled;
    if (job) {
      job.responseSummary = {
        generationId: payload.id || null,
        finishReasons: (payload.choices || []).map(choice => choice.finish_reason).filter(Boolean),
        choiceCount: Array.isArray(payload.choices) ? payload.choices.length : 0,
      };
    }
    return {
      modelResponse: {
        id: payload.id || null,
        model: payload.model || quote.model,
        choices: payload.choices || [],
        usage,
      },
      modelUsage: reconciled.modelUsage,
      researchJob: job || null,
      economicDecisionRefreshRequired: true,
    };
  } catch (error) {
    quote.status = 'failed';
    quote.failureReason = error?.name === 'AbortError' ? 'openrouter_completion_timeout' : String(error?.message || error);
    if (job) {
      job.status = 'failed';
      job.failureReason = quote.failureReason;
      job.completedAt = new Date().toISOString();
    }
    return { errors: [quote.failureReason] };
  } finally {
    clearTimeout(timeout);
  }
}

export function isEconomicRoute(pathname) {
  return pathname === '/api/economics/dashboard'
    || pathname === '/api/economics/model-pricing'
    || pathname === '/api/economics/model-pricing/refresh'
    || pathname === '/api/economics/model-quotes'
    || pathname === '/api/economics/model-usage/reconcile'
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
    return mutate(store, current => quoteModelRequest(current, body, now), 201);
  }

  if (method === 'POST' && pathname === '/api/economics/model-usage/reconcile') {
    const body = await readJsonBody();
    return mutate(store, current => reconcileModelUsage(current, body, now));
  }

  if (method === 'POST' && pathname === '/api/economics/intelligence/execute') {
    const body = await readJsonBody();
    return mutate(store, current => executeOpenRouterIntelligence(current, body, env, fetchImpl, now), 200);
  }

  if (method === 'POST' && pathname === '/api/economics/maintenance/run') {
    const body = await readJsonBody();
    return mutate(store, current => runEconomicMaintenance(current, {
      now,
      env,
      fetchImpl,
      catalog: body.catalog,
      quotes: body.quotes,
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
    return mutate(store, current => refreshCoinbaseEconomics(current, body), 201);
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
