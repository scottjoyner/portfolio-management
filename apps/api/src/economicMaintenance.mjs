import {
  buildPriceForecast,
  ensureEconomicState,
  ingestModelPricingCatalog,
  latestPricingSnapshot,
  matureForecastOutcomes,
  pruneEconomicState,
  queueOrRecordSettlementAttribution,
} from '../../../packages/economics/src/economicDecisionEngine.mjs';

function finite(value, fallback = null) {
  const number = Number(value);
  return Number.isFinite(number) ? number : fallback;
}

function positive(value, fallback) {
  const number = finite(value, fallback);
  return number > 0 ? number : fallback;
}

function uniqueSnapshotId(symbol, now) {
  const slug = String(symbol).toLowerCase().replace(/[^a-z0-9]+/g, '-');
  return `md-economic-${slug}-${new Date(now).getTime()}`;
}

function snapshotPrice(snapshot) {
  const direct = finite(snapshot?.mid ?? snapshot?.price ?? snapshot?.close ?? snapshot?.lastPrice, null);
  if (direct != null && direct > 0) return direct;
  const bid = finite(snapshot?.bid, null);
  const ask = finite(snapshot?.ask, null);
  return bid != null && ask != null && bid > 0 && ask > 0 ? (bid + ask) / 2 : null;
}

function configuredSymbols(state, env = process.env) {
  const configured = state.config?.economicForecastSymbols;
  if (Array.isArray(configured) && configured.length) return [...new Set(configured.map(String).filter(Boolean))];
  if (env.ECONOMIC_FORECAST_SYMBOLS) return [...new Set(env.ECONOMIC_FORECAST_SYMBOLS.split(',').map(value => value.trim()).filter(Boolean))];
  return ['BTC-USD', 'ETH-USD', 'SOL-USD'];
}

function pricingIsStale(state, now, env = process.env) {
  const snapshot = latestPricingSnapshot(state);
  if (!snapshot) return true;
  const ageSeconds = (new Date(now).getTime() - new Date(snapshot.fetchedAt || 0).getTime()) / 1000;
  const maximum = positive(env.ECONOMIC_PRICING_REFRESH_SECONDS, positive(state.config?.economicPricingRefreshSeconds, 21600));
  return ageSeconds > maximum;
}

export async function fetchOpenRouterCatalog(env = process.env, fetchImpl = globalThis.fetch) {
  if (typeof fetchImpl !== 'function') throw new Error('fetch_unavailable');
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), positive(env.OPENROUTER_PRICING_TIMEOUT_MS, 10000));
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

async function defaultQuoteFetcher() {
  const { fetchQuotes } = await import('../../../packages/execution/src/paperSweeper.mjs');
  return fetchQuotes();
}

function appendQuoteSnapshots(state, quotes, symbols, now) {
  const appended = [];
  for (const symbol of symbols) {
    const quote = quotes?.[symbol];
    const mid = finite(quote?.mid, null);
    const bid = finite(quote?.bid, null);
    const ask = finite(quote?.ask, null);
    if (mid == null || mid <= 0) continue;
    const previous = [...state.marketDataSnapshots]
      .filter(row => row.symbol === symbol && row.source === 'economic-maintenance')
      .sort((a, b) => new Date(b.timestamp || 0) - new Date(a.timestamp || 0))[0];
    if (previous && previous.timestamp === now && snapshotPrice(previous) === mid) continue;
    const spreadBps = finite(quote?.spreadBps, bid != null && ask != null && mid > 0 ? ((ask - bid) / mid) * 10000 : 0);
    const snapshot = {
      id: uniqueSnapshotId(symbol, now),
      symbol,
      venue: 'coinbase',
      assetClass: 'crypto',
      bid,
      ask,
      mid,
      price: mid,
      spreadBps,
      volume24h: finite(quote?.volume24h, 0),
      liquidityScore: Math.max(0, Math.min(100, Math.round(100 - Math.max(0, spreadBps || 0) / 2))),
      volatilityScore: finite(quote?.volatilityScore, 50),
      status: 'connected',
      timestamp: now,
      source: 'economic-maintenance',
    };
    state.marketDataSnapshots.push(snapshot);
    appended.push(snapshot);
  }
  return appended;
}

function trimMarketHistory(state, symbols) {
  const limit = Math.max(20, positive(state.config?.economicMarketDataHistoryLimit, 720));
  const symbolSet = new Set(symbols);
  const retained = state.marketDataSnapshots.filter(row => !symbolSet.has(row.symbol) || row.source !== 'economic-maintenance');
  for (const symbol of symbols) {
    const rows = state.marketDataSnapshots
      .filter(row => row.symbol === symbol && row.source === 'economic-maintenance')
      .sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0))
      .slice(-limit);
    retained.push(...rows);
  }
  state.marketDataSnapshots = retained;
}

function buildForecasts(state, symbols, now) {
  const created = [];
  const skipped = [];
  for (const symbol of symbols) {
    const snapshots = state.marketDataSnapshots
      .filter(row => row.symbol === symbol)
      .map(row => ({
        id: row.id,
        price: snapshotPrice(row),
        timestamp: row.timestamp || row.asOf || row.createdAt,
        volume: row.volume24h ?? row.volume,
        spreadBps: row.spreadBps,
      }))
      .filter(row => row.price != null && row.price > 0)
      .sort((a, b) => new Date(a.timestamp || 0) - new Date(b.timestamp || 0));
    if (snapshots.length < 5) {
      skipped.push({ symbol, blocker: 'forecast_requires_five_prices', observations: snapshots.length });
      continue;
    }
    const latestForecast = [...state.priceForecasts]
      .filter(row => row.symbol === symbol)
      .sort((a, b) => new Date(b.asOf || b.createdAt || 0) - new Date(a.asOf || a.createdAt || 0))[0];
    const latestObservationAt = new Date(snapshots.at(-1).timestamp || 0).getTime();
    const forecastAt = new Date(latestForecast?.asOf || 0).getTime();
    if (latestForecast?.status === 'valid' && new Date(latestForecast.expiresAt || 0) >= new Date(now) && forecastAt >= latestObservationAt) {
      skipped.push({ symbol, blocker: 'fresh_forecast_exists', forecastId: latestForecast.id });
      continue;
    }
    const latestSnapshot = snapshots.at(-1);
    const result = buildPriceForecast(state, {
      symbol,
      venue: 'coinbase',
      observations: snapshots.slice(-120),
      spreadBps: finite(latestSnapshot.spreadBps, 0),
      horizonMinutes: positive(state.config?.economicForecastHorizonMinutes, 15),
      ttlSeconds: positive(state.config?.economicForecastTtlSeconds, 120),
      maxDataAgeSeconds: positive(state.config?.maximumForecastDataAgeSeconds, 180),
      sourceSnapshotIds: snapshots.slice(-120).map(row => row.id).filter(Boolean),
      modelVersion: state.config?.economicForecastModelVersion || 'deterministic-price-ensemble-v1',
    }, now);
    if (result.priceForecast) created.push(result.priceForecast);
    else skipped.push({ symbol, blocker: result.errors?.[0] || 'forecast_failed' });
  }
  return { created, skipped };
}

function processSettlementAttribution(state, now) {
  const recorded = [];
  const pending = [];
  for (const execution of state.executions || []) {
    const result = queueOrRecordSettlementAttribution(state, execution, now);
    if (result.agentAttribution && !result.idempotent) recorded.push(result.agentAttribution);
    if (result.attributionPending) pending.push(result.attributionPending);
  }
  return { recorded, pending };
}

export async function runEconomicMaintenance(state, options = {}) {
  ensureEconomicState(state);
  const now = options.now instanceof Date ? options.now.toISOString() : options.now || new Date().toISOString();
  const env = { ...process.env, ...(options.env || {}) };
  const report = {
    ok: true,
    startedAt: now,
    pricingRefreshed: false,
    marketSnapshotsAdded: 0,
    forecastsCreated: 0,
    forecastOutcomesCreated: 0,
    attributionsRecorded: 0,
    attributionsPending: 0,
    warnings: [],
    details: {},
  };
  state.economicMaintenance = {
    ...(state.economicMaintenance || {}),
    status: 'running',
    lastRunAt: now,
    warnings: [],
  };

  if (pricingIsStale(state, now, env)) {
    try {
      const catalog = options.catalog || await fetchOpenRouterCatalog(env, options.fetchImpl || globalThis.fetch);
      const result = ingestModelPricingCatalog(state, { catalog, source: options.catalog ? 'maintenance_injected_catalog' : 'openrouter_models_api', actor: 'economic-maintenance' }, now);
      if (result.errors) report.warnings.push(...result.errors);
      else report.pricingRefreshed = true;
    } catch (error) {
      report.warnings.push(`pricing_refresh_failed:${String(error?.message || error)}`);
    }
  }

  const symbols = configuredSymbols(state, env);
  let quotes = options.quotes || null;
  try {
    quotes ||= await (options.quoteFetcher || defaultQuoteFetcher)();
    const appended = appendQuoteSnapshots(state, quotes, symbols, now);
    report.marketSnapshotsAdded = appended.length;
    report.details.marketSnapshots = appended.map(row => row.id);
    trimMarketHistory(state, symbols);
  } catch (error) {
    report.warnings.push(`market_quote_refresh_failed:${String(error?.message || error)}`);
  }

  const forecasts = buildForecasts(state, symbols, now);
  report.forecastsCreated = forecasts.created.length;
  report.details.forecasts = forecasts.created.map(row => row.id);
  report.details.forecastSkips = forecasts.skipped;

  const outcomes = matureForecastOutcomes(state, now);
  report.forecastOutcomesCreated = outcomes.forecastOutcomes.length;
  report.details.pendingForecastOutcomes = outcomes.pendingForecastOutcomes;

  const attribution = processSettlementAttribution(state, now);
  report.attributionsRecorded = attribution.recorded.length;
  report.attributionsPending = state.economicAttributionQueue.length;
  report.details.attributionRecorded = attribution.recorded.map(row => row.id);
  report.details.attributionPending = attribution.pending.map(row => row.id);

  report.details.pruned = pruneEconomicState(state, state.config?.economicStateRetention || {}).removed;
  report.ok = report.warnings.length === 0;
  report.completedAt = new Date().toISOString();
  state.economicMaintenance = {
    status: report.ok ? 'ok' : 'degraded',
    lastRunAt: now,
    lastSuccessAt: report.ok ? report.completedAt : state.economicMaintenance?.lastSuccessAt || null,
    completedAt: report.completedAt,
    warnings: report.warnings,
    counters: {
      pricingRefreshed: report.pricingRefreshed ? 1 : 0,
      marketSnapshotsAdded: report.marketSnapshotsAdded,
      forecastsCreated: report.forecastsCreated,
      forecastOutcomesCreated: report.forecastOutcomesCreated,
      attributionsRecorded: report.attributionsRecorded,
      attributionsPending: report.attributionsPending,
    },
  };
  state.audit?.push?.({
    id: `audit-economic-${new Date(now).getTime()}`,
    action: 'economic_maintenance_completed',
    actor: 'economic-maintenance',
    at: report.completedAt,
    details: state.economicMaintenance.status,
    payload: state.economicMaintenance.counters,
  });
  return report;
}

export function startEconomicMaintenance({ store, env = process.env, fetchImpl = globalThis.fetch, quoteFetcher } = {}) {
  if (!store) throw new Error('economic_maintenance_store_required');
  const enabled = env.ECONOMIC_RUNTIME_ENABLED === 'true';
  if (!enabled) return null;
  const intervalMs = Math.max(10000, positive(env.ECONOMIC_MAINTENANCE_INTERVAL_MS, 60000));
  let running = false;
  const run = async () => {
    if (running) return { ok: false, skipped: true, reason: 'economic_maintenance_already_running' };
    running = true;
    try {
      return await store.mutate(state => runEconomicMaintenance(state, { env, fetchImpl, quoteFetcher }));
    } finally {
      running = false;
    }
  };
  const timer = setInterval(() => { run().catch(() => {}); }, intervalMs);
  timer.unref?.();
  const initialTimer = setTimeout(() => { run().catch(() => {}); }, Math.min(1000, intervalMs));
  initialTimer.unref?.();
  return {
    intervalMs,
    run,
    stop() {
      clearInterval(timer);
      clearTimeout(initialTimer);
    },
  };
}
