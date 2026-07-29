const OPENROUTER_PRICING_FIELDS = [
  'prompt',
  'completion',
  'request',
  'image',
  'web_search',
  'internal_reasoning',
  'input_cache_read',
  'input_cache_write',
];

const DEFAULT_WEIGHTS = {
  naive: 0.15,
  momentum: 0.40,
  meanReversion: 0.25,
  microstructure: 0.20,
};

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function nonNegative(value, fallback = 0) {
  const number = finite(value, fallback);
  return Number.isFinite(number) && number >= 0 ? number : fallback;
}

function clamp(value, min, max) {
  return Math.min(max, Math.max(min, value));
}

function round(value, digits = 6) {
  if (!Number.isFinite(value)) return null;
  const scale = 10 ** digits;
  return Math.round(value * scale) / scale;
}

function timestamp(value) {
  const date = value ? new Date(value) : new Date();
  return Number.isNaN(date.getTime()) ? null : date;
}

function nextRecordId(prefix, collection = []) {
  const existing = new Set(collection.map(row => row?.id).filter(Boolean));
  let index = collection.length + 1;
  let candidate = `${prefix}-${String(index).padStart(4, '0')}`;
  while (existing.has(candidate)) {
    index += 1;
    candidate = `${prefix}-${String(index).padStart(4, '0')}`;
  }
  return candidate;
}

function mean(values) {
  return values.length ? values.reduce((sum, value) => sum + value, 0) / values.length : 0;
}

function standardDeviation(values) {
  if (values.length < 2) return 0;
  const average = mean(values);
  return Math.sqrt(values.reduce((sum, value) => sum + ((value - average) ** 2), 0) / (values.length - 1));
}

function normalCdf(value) {
  const sign = value < 0 ? -1 : 1;
  const x = Math.abs(value) / Math.sqrt(2);
  const t = 1 / (1 + 0.3275911 * x);
  const polynomial = (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
  const erf = sign * (1 - polynomial * Math.exp(-(x ** 2)));
  return 0.5 * (1 + erf);
}

function pricingNumber(value) {
  const parsed = finite(value, 0);
  return parsed != null && parsed >= 0 ? parsed : 0;
}

export function ensureEconomicState(state) {
  state.modelPricingSnapshots ||= [];
  state.modelUsageLedger ||= [];
  state.priceForecasts ||= [];
  state.executionCostSnapshots ||= [];
  state.economicDecisions ||= [];
  state.agentAttributionRecords ||= [];
  state.forecastOutcomes ||= [];
  return state;
}

export function normalizeOpenRouterCatalog(payload = {}, now = new Date().toISOString(), source = 'openrouter_models_api') {
  const rows = Array.isArray(payload) ? payload : Array.isArray(payload.data) ? payload.data : [];
  const models = rows.map(row => {
    const pricing = {};
    for (const field of OPENROUTER_PRICING_FIELDS) pricing[field] = pricingNumber(row?.pricing?.[field]);
    return {
      id: String(row?.id || row?.slug || '').trim(),
      name: row?.name || row?.id || null,
      contextLength: finite(row?.context_length ?? row?.top_provider?.context_length, null),
      pricing,
      supportedParameters: Array.isArray(row?.supported_parameters) ? row.supported_parameters : [],
      architecture: row?.architecture || null,
      topProvider: row?.top_provider || null,
    };
  }).filter(row => row.id);

  return {
    provider: 'openrouter',
    source,
    fetchedAt: now,
    pricingUnit: 'usd_per_token_or_unit',
    modelCount: models.length,
    models,
  };
}

export function ingestModelPricingCatalog(state, payload = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const normalized = normalizeOpenRouterCatalog(payload.catalog ?? payload, now, payload.source || 'openrouter_models_api');
  if (!normalized.models.length) return { errors: ['model_pricing_catalog_empty'] };
  const snapshot = {
    id: nextRecordId('pricing', state.modelPricingSnapshots),
    ...normalized,
  };
  state.modelPricingSnapshots.push(snapshot);
  state.audit?.push?.({
    id: nextRecordId('audit', state.audit),
    action: 'model_pricing_refreshed',
    actor: payload.actor || 'economic-decision-engine',
    at: now,
    details: snapshot.id,
    payload: { provider: snapshot.provider, modelCount: snapshot.modelCount },
  });
  return { pricingSnapshot: snapshot };
}

export function latestPricingSnapshot(state, provider = 'openrouter') {
  ensureEconomicState(state);
  return [...state.modelPricingSnapshots]
    .filter(row => row.provider === provider)
    .sort((a, b) => new Date(b.fetchedAt || 0) - new Date(a.fetchedAt || 0))[0] || null;
}

export function findModelPricing(snapshot, model) {
  if (!snapshot || !model) return null;
  return snapshot.models?.find(row => row.id === model)
    || snapshot.models?.find(row => row.name === model)
    || null;
}

function localModelQuote(body = {}) {
  const runtimeHours = nonNegative(body.runtimeSeconds, 0) / 3600;
  const watts = nonNegative(body.estimatedWatts, 0);
  const electricity = nonNegative(body.electricityRatePerKwh, 0.14);
  const depreciation = nonNegative(body.hardwareDepreciationPerHour, 0);
  const cost = (runtimeHours * watts / 1000 * electricity) + (runtimeHours * depreciation);
  return {
    estimatedCostUsd: round(cost, 6),
    pricingBreakdown: {
      electricityUsd: round(runtimeHours * watts / 1000 * electricity, 6),
      depreciationUsd: round(runtimeHours * depreciation, 6),
    },
    providerPreferences: null,
  };
}

export function quoteModelRequest(state, body = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const localOrRemote = body.localOrRemote === 'local' ? 'local' : 'remote';
  const model = String(body.model || '').trim();
  if (!model) return { errors: ['model_required'] };

  let quote;
  let snapshot = null;
  let pricing = null;
  if (localOrRemote === 'local') {
    quote = localModelQuote(body);
  } else {
    snapshot = body.pricingSnapshotId
      ? state.modelPricingSnapshots.find(row => row.id === body.pricingSnapshotId)
      : latestPricingSnapshot(state, body.provider || 'openrouter');
    if (!snapshot) return { errors: ['model_pricing_snapshot_required'] };
    const fetchedAt = timestamp(snapshot.fetchedAt);
    const maxAgeSeconds = nonNegative(body.maxPricingAgeSeconds, 86400);
    if (!fetchedAt || (Date.now() - fetchedAt.getTime()) / 1000 > maxAgeSeconds) return { errors: ['model_pricing_snapshot_stale'] };
    const modelRecord = findModelPricing(snapshot, model);
    if (!modelRecord) return { errors: ['model_pricing_not_found'] };
    pricing = modelRecord.pricing;

    const promptTokens = nonNegative(body.promptTokens, 0);
    const completionTokens = nonNegative(body.completionTokens ?? body.maxCompletionTokens, 0);
    const reasoningTokens = nonNegative(body.reasoningTokens ?? body.maxReasoningTokens, 0);
    const cacheReadTokens = nonNegative(body.cacheReadTokens, 0);
    const cacheWriteTokens = nonNegative(body.cacheWriteTokens, 0);
    const webSearchRequests = nonNegative(body.webSearchRequests, 0);
    const imageUnits = nonNegative(body.imageUnits, 0);
    const requests = Math.max(1, nonNegative(body.requests, 1));
    const reasoningPrice = pricing.internal_reasoning > 0 ? pricing.internal_reasoning : pricing.completion;
    const estimatedCost =
      (promptTokens * pricing.prompt)
      + (completionTokens * pricing.completion)
      + (reasoningTokens * reasoningPrice)
      + (cacheReadTokens * pricing.input_cache_read)
      + (cacheWriteTokens * pricing.input_cache_write)
      + (webSearchRequests * pricing.web_search)
      + (imageUnits * pricing.image)
      + (requests * pricing.request);

    quote = {
      estimatedCostUsd: round(estimatedCost, 8),
      pricingBreakdown: {
        promptUsd: round(promptTokens * pricing.prompt, 8),
        completionUsd: round(completionTokens * pricing.completion, 8),
        reasoningUsd: round(reasoningTokens * reasoningPrice, 8),
        cacheReadUsd: round(cacheReadTokens * pricing.input_cache_read, 8),
        cacheWriteUsd: round(cacheWriteTokens * pricing.input_cache_write, 8),
        webSearchUsd: round(webSearchRequests * pricing.web_search, 8),
        imageUsd: round(imageUnits * pricing.image, 8),
        requestUsd: round(requests * pricing.request, 8),
      },
      providerPreferences: {
        sort: body.providerSort || 'price',
        max_price: {
          prompt: round(pricing.prompt * 1_000_000, 6),
          completion: round(pricing.completion * 1_000_000, 6),
          request: round(pricing.request, 8),
          image: round(pricing.image, 8),
        },
      },
    };
  }

  const record = {
    id: nextRecordId('model-quote', state.modelUsageLedger),
    status: 'quoted',
    provider: body.provider || (localOrRemote === 'local' ? 'local' : 'openrouter'),
    model,
    localOrRemote,
    pricingSnapshotId: snapshot?.id || null,
    promptTokens: nonNegative(body.promptTokens, 0),
    completionTokens: nonNegative(body.completionTokens ?? body.maxCompletionTokens, 0),
    reasoningTokens: nonNegative(body.reasoningTokens ?? body.maxReasoningTokens, 0),
    cacheReadTokens: nonNegative(body.cacheReadTokens, 0),
    cacheWriteTokens: nonNegative(body.cacheWriteTokens, 0),
    webSearchRequests: nonNegative(body.webSearchRequests, 0),
    imageUnits: nonNegative(body.imageUnits, 0),
    estimatedCostUsd: quote.estimatedCostUsd,
    actualCostUsd: null,
    costVarianceUsd: null,
    pricingBreakdown: quote.pricingBreakdown,
    providerPreferences: quote.providerPreferences,
    opportunityId: body.opportunityId || null,
    decisionId: body.decisionId || null,
    requestedAt: now,
    reconciledAt: null,
    generationId: null,
    usage: null,
  };
  state.modelUsageLedger.push(record);
  return { modelQuote: record };
}

export function reconcileModelUsage(state, body = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const quote = state.modelUsageLedger.find(row => row.id === body.quoteId);
  if (!quote) return { errors: ['model_quote_not_found'] };
  const usage = body.usage || body.response?.usage || {};
  const actualCost = finite(body.actualCostUsd ?? usage.cost, null);
  if (actualCost == null || actualCost < 0) return { errors: ['actual_model_cost_required'] };
  const promptDetails = usage.prompt_tokens_details || {};
  const completionDetails = usage.completion_tokens_details || {};
  quote.status = 'reconciled';
  quote.generationId = body.generationId || body.response?.id || quote.generationId;
  quote.actualCostUsd = round(actualCost, 8);
  quote.costVarianceUsd = round(actualCost - Number(quote.estimatedCostUsd || 0), 8);
  quote.promptTokensActual = nonNegative(usage.prompt_tokens, 0);
  quote.completionTokensActual = nonNegative(usage.completion_tokens, 0);
  quote.reasoningTokensActual = nonNegative(completionDetails.reasoning_tokens, 0);
  quote.cacheReadTokensActual = nonNegative(promptDetails.cached_tokens, 0);
  quote.cacheWriteTokensActual = nonNegative(promptDetails.cache_write_tokens, 0);
  quote.upstreamInferenceCostUsd = finite(usage.cost_details?.upstream_inference_cost, null);
  quote.usage = usage;
  quote.reconciledAt = now;

  const costRow = state.agentCostLedger?.find(row => row.modelQuoteId === quote.id || (quote.decisionId && row.decisionId === quote.decisionId));
  if (costRow) {
    costRow.remoteApiCost = quote.localOrRemote === 'remote' ? quote.actualCostUsd : 0;
    costRow.localComputeCost = quote.localOrRemote === 'local' ? quote.actualCostUsd : 0;
    costRow.generationId = quote.generationId;
    costRow.pricingSnapshotId = quote.pricingSnapshotId;
    costRow.modelQuoteId = quote.id;
    costRow.costReconciledAt = now;
  }
  return { modelUsage: quote };
}

function normalizeObservations(body = {}) {
  const source = Array.isArray(body.observations) ? body.observations : Array.isArray(body.prices) ? body.prices : [];
  return source.map((row, index) => {
    if (typeof row === 'number') return { price: row, timestamp: null, index };
    return {
      price: finite(row?.price ?? row?.close ?? row?.mid, null),
      timestamp: row?.timestamp || row?.time || row?.at || row?.start || null,
      volume: finite(row?.volume, null),
      index,
    };
  }).filter(row => row.price != null && row.price > 0);
}

export function buildPriceForecast(state, body = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const symbol = String(body.symbol || '').trim();
  if (!symbol) return { errors: ['forecast_symbol_required'] };
  const observations = normalizeObservations(body);
  if (observations.length < 5) return { errors: ['forecast_requires_five_prices'] };

  const latestObservation = observations.at(-1);
  const latestDate = timestamp(latestObservation.timestamp);
  const maxDataAgeSeconds = nonNegative(body.maxDataAgeSeconds, 180);
  if (latestDate && (new Date(now).getTime() - latestDate.getTime()) / 1000 > maxDataAgeSeconds) return { errors: ['forecast_market_data_stale'] };

  const prices = observations.map(row => row.price);
  const returns = [];
  for (let index = 1; index < prices.length; index += 1) returns.push(Math.log(prices[index] / prices[index - 1]));
  const recentReturns = returns.slice(-Math.min(12, returns.length));
  const recentPrices = prices.slice(-Math.min(20, prices.length));
  const currentPrice = prices.at(-1);
  const rollingMean = mean(recentPrices);
  const momentumReturn = mean(recentReturns.slice(-Math.min(5, recentReturns.length)));
  const meanReversionReturn = rollingMean > 0 ? clamp((rollingMean - currentPrice) / currentPrice, -0.03, 0.03) : 0;
  const orderBookImbalance = clamp(finite(body.orderBookImbalance, 0), -1, 1);
  const spreadBps = nonNegative(body.spreadBps, 0);
  const microstructureReturn = orderBookImbalance * Math.max(0.0001, spreadBps / 10000) * 0.5;
  const weights = { ...DEFAULT_WEIGHTS, ...(body.weights || {}) };
  const weightTotal = Object.values(weights).reduce((sum, value) => sum + Math.max(0, finite(value, 0)), 0) || 1;
  const normalizedWeights = Object.fromEntries(Object.entries(weights).map(([key, value]) => [key, Math.max(0, finite(value, 0)) / weightTotal]));

  const intervalMinutes = finite(body.observationIntervalMinutes, null)
    || (() => {
      const first = timestamp(observations.at(-2)?.timestamp);
      const last = timestamp(observations.at(-1)?.timestamp);
      return first && last ? Math.max(1 / 60, (last.getTime() - first.getTime()) / 60000) : 1;
    })();
  const horizonMinutes = Math.max(1, nonNegative(body.horizonMinutes, 60));
  const horizonScale = Math.sqrt(horizonMinutes / Math.max(intervalMinutes, 1 / 60));
  const perIntervalExpected =
    (normalizedWeights.naive * 0)
    + (normalizedWeights.momentum * momentumReturn)
    + (normalizedWeights.meanReversion * meanReversionReturn)
    + (normalizedWeights.microstructure * microstructureReturn);
  const volatility = Math.max(standardDeviation(recentReturns), 0.000001);
  const horizonVolatility = volatility * horizonScale;
  const expectedLogReturn = clamp(perIntervalExpected * horizonScale, -3 * horizonVolatility, 3 * horizonVolatility);
  const z80 = 1.281551565545;
  const expectedPrice = currentPrice * Math.exp(expectedLogReturn);
  const p10 = currentPrice * Math.exp(expectedLogReturn - z80 * horizonVolatility);
  const p50 = expectedPrice;
  const p90 = currentPrice * Math.exp(expectedLogReturn + z80 * horizonVolatility);
  const probabilityUp = clamp(normalCdf(expectedLogReturn / horizonVolatility), 0.001, 0.999);
  const expectedReturnBps = (expectedPrice / currentPrice - 1) * 10000;
  const volatilityBps = horizonVolatility * 10000;
  const regime = volatilityBps >= 250
    ? 'extreme_volatility'
    : volatilityBps >= 120
      ? Math.abs(expectedReturnBps) >= 40 ? 'high_volatility_trend' : 'high_volatility_range'
      : Math.abs(expectedReturnBps) >= 25 ? 'moderate_trend' : 'low_volatility_range';
  const generatedAt = timestamp(now) || new Date();
  const ttlSeconds = Math.max(30, nonNegative(body.ttlSeconds, Math.min(300, horizonMinutes * 60 * 0.1)));

  const forecast = {
    id: nextRecordId('forecast', state.priceForecasts),
    status: 'valid',
    symbol,
    venue: body.venue || 'coinbase',
    asOf: generatedAt.toISOString(),
    horizonMinutes,
    currentPrice: round(currentPrice, 8),
    expectedPrice: round(expectedPrice, 8),
    p10Price: round(p10, 8),
    p50Price: round(p50, 8),
    p90Price: round(p90, 8),
    expectedReturnBps: round(expectedReturnBps, 4),
    probabilityUp: round(probabilityUp, 6),
    expectedVolatilityBps: round(volatilityBps, 4),
    regime,
    modelVersion: body.modelVersion || 'deterministic-price-ensemble-v1',
    calibrationError: finite(body.calibrationError, null),
    expiresAt: new Date(generatedAt.getTime() + ttlSeconds * 1000).toISOString(),
    components: {
      naiveReturn: 0,
      momentumReturn: round(momentumReturn * horizonScale, 8),
      meanReversionReturn: round(meanReversionReturn * horizonScale, 8),
      microstructureReturn: round(microstructureReturn * horizonScale, 8),
      weights: normalizedWeights,
    },
    observationCount: observations.length,
    sourceSnapshotIds: Array.isArray(body.sourceSnapshotIds) ? body.sourceSnapshotIds : [],
    opportunityId: body.opportunityId || null,
    createdAt: now,
  };
  state.priceForecasts.push(forecast);
  return { priceForecast: forecast };
}

export function recordForecastOutcome(state, body = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const forecast = state.priceForecasts.find(row => row.id === body.forecastId);
  if (!forecast) return { errors: ['price_forecast_not_found'] };
  const actualPrice = finite(body.actualPrice, null);
  if (actualPrice == null || actualPrice <= 0) return { errors: ['actual_price_required'] };
  const actualUp = actualPrice > forecast.currentPrice ? 1 : 0;
  const probability = Number(forecast.probabilityUp || 0.5);
  const outcome = {
    id: nextRecordId('forecast-outcome', state.forecastOutcomes),
    forecastId: forecast.id,
    symbol: forecast.symbol,
    actualPrice,
    observedAt: body.observedAt || now,
    predictedUpProbability: probability,
    actualUp,
    directionCorrect: (probability >= 0.5) === Boolean(actualUp),
    brierScore: round((probability - actualUp) ** 2, 8),
    absoluteErrorPct: round(Math.abs(actualPrice - forecast.expectedPrice) / actualPrice * 100, 6),
    insideP10P90: actualPrice >= forecast.p10Price && actualPrice <= forecast.p90Price,
    regime: forecast.regime,
    modelVersion: forecast.modelVersion,
  };
  state.forecastOutcomes.push(outcome);
  return { forecastOutcome: outcome };
}

export function summarizeForecastCalibration(state) {
  ensureEconomicState(state);
  const rows = state.forecastOutcomes;
  if (!rows.length) return { samples: 0, brierScore: null, directionalAccuracy: null, p10P90Coverage: null, meanAbsoluteErrorPct: null, byRegime: {} };
  const byRegime = {};
  for (const row of rows) {
    byRegime[row.regime] ||= [];
    byRegime[row.regime].push(row);
  }
  const summarize = values => ({
    samples: values.length,
    brierScore: round(mean(values.map(row => row.brierScore)), 6),
    directionalAccuracy: round(mean(values.map(row => row.directionCorrect ? 1 : 0)), 6),
    p10P90Coverage: round(mean(values.map(row => row.insideP10P90 ? 1 : 0)), 6),
    meanAbsoluteErrorPct: round(mean(values.map(row => row.absoluteErrorPct)), 6),
  });
  return {
    ...summarize(rows),
    byRegime: Object.fromEntries(Object.entries(byRegime).map(([regime, values]) => [regime, summarize(values)])),
  };
}

export function ingestExecutionCostSnapshot(state, body = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const notionalUsd = finite(body.notionalUsd ?? body.notional, null);
  if (notionalUsd == null || notionalUsd <= 0) return { errors: ['execution_notional_required'] };
  const feeSummary = body.feeSummary || {};
  const feeTier = feeSummary.fee_tier || feeSummary.feeTier || {};
  const preview = body.preview || {};
  const liquidity = String(body.liquidity || preview.liquidity || 'taker').toLowerCase() === 'maker' ? 'maker' : 'taker';
  const makerRate = nonNegative(feeTier.maker_fee_rate ?? feeTier.makerFeeRate, 0);
  const takerRate = nonNegative(feeTier.taker_fee_rate ?? feeTier.takerFeeRate, 0);
  const feeRate = liquidity === 'maker' ? makerRate : takerRate;
  const previewCommission = finite(
    body.commissionUsd
      ?? preview.commission_total
      ?? preview.commissionTotal
      ?? preview.estimatedFee
      ?? preview.commission_detail_total?.total_commission,
    null,
  );
  const commissionUsd = previewCommission != null ? previewCommission : notionalUsd * feeRate;
  const referencePrice = finite(body.referencePrice ?? preview.referencePrice, null);
  const fillPrice = finite(preview.est_average_filled_price ?? preview.average_filled_price ?? preview.estimatedPrice, null);
  const quantity = finite(body.quantity, null);
  const priceSlippageUsd = referencePrice != null && fillPrice != null && quantity != null
    ? Math.abs(fillPrice - referencePrice) * quantity
    : null;
  const slippageUsd = finite(body.slippageUsd ?? preview.estimatedSlippage, priceSlippageUsd)
    ?? (notionalUsd * nonNegative(body.slippageBps ?? preview.slippageBps, 0) / 10000);
  const spreadBps = nonNegative(body.spreadBps, 0);
  const spreadCostUsd = finite(body.spreadCostUsd, notionalUsd * spreadBps / 20000);
  const fundingBorrowUsd = nonNegative(body.fundingBorrowUsd, 0);
  const gasUsd = nonNegative(body.gasUsd, 0);
  const latencyDecayUsd = nonNegative(body.latencyDecayUsd, 0);
  const totalExecutionCostUsd = commissionUsd + slippageUsd + spreadCostUsd + fundingBorrowUsd + gasUsd + latencyDecayUsd;
  const validUntil = body.validUntil || preview.validUntil || new Date(new Date(now).getTime() + 30000).toISOString();
  const snapshot = {
    id: nextRecordId('execution-cost', state.executionCostSnapshots),
    venue: body.venue || 'coinbase',
    symbol: body.symbol || null,
    side: body.side || null,
    liquidity,
    notionalUsd: round(notionalUsd, 6),
    makerFeeRate: makerRate,
    takerFeeRate: takerRate,
    appliedFeeRate: feeRate,
    commissionUsd: round(commissionUsd, 6),
    slippageUsd: round(slippageUsd, 6),
    spreadCostUsd: round(spreadCostUsd, 6),
    fundingBorrowUsd: round(fundingBorrowUsd, 6),
    gasUsd: round(gasUsd, 6),
    latencyDecayUsd: round(latencyDecayUsd, 6),
    totalExecutionCostUsd: round(totalExecutionCostUsd, 6),
    estimatedFillPrice: fillPrice,
    previewId: preview.preview_id || preview.previewId || null,
    feePricingTier: feeTier.pricing_tier || feeTier.pricingTier || null,
    validUntil,
    source: body.source || 'coinbase_preview_and_fee_tier',
    createdAt: now,
  };
  state.executionCostSnapshots.push(snapshot);
  return { executionCostSnapshot: snapshot };
}

function resolveByIdOrObject(collection, id, objectValue) {
  if (objectValue && typeof objectValue === 'object') return objectValue;
  return id ? collection.find(row => row.id === id) || null : null;
}

export function evaluateEconomicDecision(state, body = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const forecast = resolveByIdOrObject(state.priceForecasts, body.forecastId, body.forecast);
  const modelQuote = resolveByIdOrObject(state.modelUsageLedger, body.modelQuoteId, body.modelQuote);
  const executionCost = resolveByIdOrObject(state.executionCostSnapshots, body.executionCostSnapshotId, body.executionCostSnapshot);
  const errors = [];
  if (!forecast) errors.push('price_forecast_required');
  if (!executionCost) errors.push('execution_cost_snapshot_required');
  if (body.requestRemoteModel === true && !modelQuote) errors.push('model_quote_required');
  if (errors.length) return { errors };

  const nowMs = new Date(now).getTime();
  const forecastFresh = forecast.status === 'valid' && new Date(forecast.expiresAt || 0).getTime() >= nowMs;
  const executionCostFresh = new Date(executionCost.validUntil || 0).getTime() >= nowMs;
  const quoteFresh = !modelQuote || new Date(modelQuote.requestedAt || 0).getTime() >= nowMs - nonNegative(body.maxQuoteAgeSeconds, 900) * 1000;
  const notionalUsd = nonNegative(body.notionalUsd ?? executionCost.notionalUsd, 0);
  const predictedEdgeUsd = finite(body.predictedEdgeUsd, notionalUsd * Number(forecast.expectedReturnBps || 0) / 10000);
  const modelCostUsd = nonNegative(modelQuote?.estimatedCostUsd, 0);
  const executionCostsUsd = nonNegative(executionCost.totalExecutionCostUsd, 0);
  const uncertaintyReserveUsd = nonNegative(body.uncertaintyReserveUsd, notionalUsd * Number(forecast.expectedVolatilityBps || 0) / 10000 * nonNegative(body.uncertaintyReserveFraction, 0.25));
  const latencyDecayUsd = nonNegative(body.latencyDecayUsd, executionCost.latencyDecayUsd || 0);
  const expectedDecisionImprovementUsd = nonNegative(body.expectedDecisionImprovementUsd, 0);
  const probabilityDecisionChanges = clamp(finite(body.probabilityDecisionChanges, 0), 0, 1);
  const additionalExecutionCostUsd = nonNegative(body.additionalExecutionCostUsd, 0);
  const maximumIntelligenceSpendUsd = Math.max(0,
    probabilityDecisionChanges * expectedDecisionImprovementUsd
      - additionalExecutionCostUsd
      - latencyDecayUsd
      - uncertaintyReserveUsd,
  );
  const requiredCostCoverageMultiple = Math.max(1, nonNegative(body.requiredCostCoverageMultiple, 3));
  const expectedUpliftCoverage = modelCostUsd > 0 ? expectedDecisionImprovementUsd / modelCostUsd : null;
  const intelligenceAllowed = !body.requestRemoteModel || (
    quoteFresh
    && modelCostUsd <= maximumIntelligenceSpendUsd
    && expectedDecisionImprovementUsd >= modelCostUsd * requiredCostCoverageMultiple
  );
  const netExecutableEdgeUsd = predictedEdgeUsd - executionCostsUsd - modelCostUsd - uncertaintyReserveUsd - latencyDecayUsd;
  const minimumNetEdgeUsd = nonNegative(body.minimumNetEdgeUsd, 0);
  const executionAllowed = forecastFresh
    && executionCostFresh
    && intelligenceAllowed
    && netExecutableEdgeUsd > minimumNetEdgeUsd;

  const uncertainty = clamp(finite(body.decisionUncertainty, 1 - Math.abs(Number(forecast.probabilityUp || 0.5) - 0.5) * 2), 0, 1);
  let selectedTier = 'deterministic';
  if (body.requestRemoteModel === true && intelligenceAllowed) selectedTier = uncertainty >= 0.65 ? 'premium_remote' : 'cheap_remote';
  else if (body.localModelAvailable === true && uncertainty >= 0.35) selectedTier = 'local_model';

  const blockers = [];
  if (!forecastFresh) blockers.push('forecast_stale_or_invalid');
  if (!executionCostFresh) blockers.push('execution_cost_snapshot_stale');
  if (!quoteFresh) blockers.push('model_quote_stale');
  if (!intelligenceAllowed) blockers.push('intelligence_purchase_not_economic');
  if (netExecutableEdgeUsd <= minimumNetEdgeUsd) blockers.push('net_executable_edge_insufficient');

  const decision = {
    id: nextRecordId('economic-decision', state.economicDecisions),
    opportunityId: body.opportunityId || forecast.opportunityId || null,
    symbol: body.symbol || forecast.symbol,
    forecastId: forecast.id || null,
    modelQuoteId: modelQuote?.id || null,
    executionCostSnapshotId: executionCost.id || null,
    predictedEdgeUsd: round(predictedEdgeUsd, 6),
    executionCostsUsd: round(executionCostsUsd, 6),
    modelCostUsd: round(modelCostUsd, 8),
    uncertaintyReserveUsd: round(uncertaintyReserveUsd, 6),
    latencyDecayUsd: round(latencyDecayUsd, 6),
    netExecutableEdgeUsd: round(netExecutableEdgeUsd, 6),
    maximumIntelligenceSpendUsd: round(maximumIntelligenceSpendUsd, 8),
    expectedDecisionImprovementUsd: round(expectedDecisionImprovementUsd, 6),
    probabilityDecisionChanges: round(probabilityDecisionChanges, 6),
    requiredCostCoverageMultiple,
    expectedUpliftCoverage: round(expectedUpliftCoverage, 6),
    intelligenceAllowed,
    executionAllowed,
    selectedTier,
    providerPreferences: selectedTier.endsWith('remote') ? modelQuote?.providerPreferences || null : null,
    blockers,
    forecastFresh,
    executionCostFresh,
    quoteFresh,
    createdAt: now,
  };
  state.economicDecisions.push(decision);
  if (modelQuote && !modelQuote.decisionId) modelQuote.decisionId = decision.id;
  return { economicDecision: decision };
}

export function recordAgentAttribution(state, body = {}, now = new Date().toISOString()) {
  ensureEconomicState(state);
  const agentCostUsd = nonNegative(body.agentCostUsd, 0);
  const realizedPnlUsd = finite(body.realizedPnlUsd, null);
  const counterfactualPnlUsd = finite(body.counterfactualPnlUsd, null);
  if (realizedPnlUsd == null || counterfactualPnlUsd == null) return { errors: ['realized_and_counterfactual_pnl_required'] };
  const botAction = body.botAction || 'unknown';
  const agentAction = body.agentAction || 'unknown';
  const finalAction = body.finalAction || agentAction;
  const changedDecision = String(agentAction) !== String(botAction);
  const incrementalValueUsd = realizedPnlUsd - counterfactualPnlUsd;
  const record = {
    id: nextRecordId('agent-attribution', state.agentAttributionRecords),
    opportunityId: body.opportunityId || null,
    executionId: body.executionId || null,
    decisionId: body.decisionId || null,
    modelQuoteIds: Array.isArray(body.modelQuoteIds) ? body.modelQuoteIds : [],
    botAction,
    agentAction,
    finalAction,
    changedDecision,
    realizedPnlUsd: round(realizedPnlUsd, 6),
    counterfactualPnlUsd: round(counterfactualPnlUsd, 6),
    incrementalValueUsd: round(incrementalValueUsd, 6),
    agentCostUsd: round(agentCostUsd, 8),
    incrementalRoi: agentCostUsd > 0 ? round(incrementalValueUsd / agentCostUsd, 6) : null,
    avoidedLossValueUsd: counterfactualPnlUsd < realizedPnlUsd && counterfactualPnlUsd < 0 ? round(Math.min(-counterfactualPnlUsd, incrementalValueUsd), 6) : 0,
    harmfulOverride: changedDecision && incrementalValueUsd < 0,
    profitableOverride: changedDecision && incrementalValueUsd > 0,
    observedAt: body.observedAt || now,
  };
  state.agentAttributionRecords.push(record);
  return { agentAttribution: record };
}

export function summarizeAgentAttribution(state) {
  ensureEconomicState(state);
  const rows = state.agentAttributionRecords;
  const changed = rows.filter(row => row.changedDecision);
  const totalCost = rows.reduce((sum, row) => sum + Number(row.agentCostUsd || 0), 0);
  const incrementalValue = rows.reduce((sum, row) => sum + Number(row.incrementalValueUsd || 0), 0);
  const profitable = changed.filter(row => row.profitableOverride);
  const harmful = changed.filter(row => row.harmfulOverride);
  return {
    observations: rows.length,
    changedDecisions: changed.length,
    changeRate: rows.length ? round(changed.length / rows.length, 6) : null,
    incrementalPnlUsd: round(incrementalValue, 6),
    agentCostUsd: round(totalCost, 8),
    incrementalRoi: totalCost > 0 ? round(incrementalValue / totalCost, 6) : null,
    costPerChangedDecisionUsd: changed.length ? round(totalCost / changed.length, 8) : null,
    costPerProfitableChangedDecisionUsd: profitable.length ? round(totalCost / profitable.length, 8) : null,
    agentOverrideWinRate: changed.length ? round(profitable.length / changed.length, 6) : null,
    harmfulOverrides: harmful.length,
    profitableOverrides: profitable.length,
    avoidedLossValueUsd: round(rows.reduce((sum, row) => sum + Number(row.avoidedLossValueUsd || 0), 0), 6),
  };
}

export function economicDashboard(state) {
  ensureEconomicState(state);
  const latest = rows => [...rows].sort((a, b) => new Date(b.createdAt || b.fetchedAt || b.requestedAt || 0) - new Date(a.createdAt || a.fetchedAt || a.requestedAt || 0))[0] || null;
  return {
    pricing: {
      latestSnapshot: latestPricingSnapshot(state),
      quoteCount: state.modelUsageLedger.length,
      unreconciledQuotes: state.modelUsageLedger.filter(row => row.status !== 'reconciled').length,
      recentQuotes: [...state.modelUsageLedger].slice(-20).reverse(),
    },
    forecasts: {
      latest: latest(state.priceForecasts),
      recent: [...state.priceForecasts].slice(-20).reverse(),
      calibration: summarizeForecastCalibration(state),
    },
    executionCosts: {
      latest: latest(state.executionCostSnapshots),
      recent: [...state.executionCostSnapshots].slice(-20).reverse(),
    },
    decisions: {
      latest: latest(state.economicDecisions),
      recent: [...state.economicDecisions].slice(-20).reverse(),
      allowed: state.economicDecisions.filter(row => row.executionAllowed).length,
      blocked: state.economicDecisions.filter(row => !row.executionAllowed).length,
    },
    attribution: summarizeAgentAttribution(state),
  };
}
