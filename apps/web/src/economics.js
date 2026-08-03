const ECONOMICS_ROUTE = '/api/economics/dashboard';
const MAINTENANCE_ROUTE = '/api/economics/maintenance/run';
const PRICING_REFRESH_ROUTE = '/api/economics/model-pricing/refresh';
const PRICING_MAX_AGE_SECONDS = 86400;

const economicsState = {
  data: null,
  error: null,
  refreshing: false,
  action: null,
  actionMessage: '',
};

const finite = value => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
const money = value => finite(value)
  ? Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 4 })
  : '—';
const number = (value, digits = 2) => finite(value)
  ? Number(value).toLocaleString('en-US', { maximumFractionDigits: digits })
  : '—';
const percent = value => finite(value) ? `${(Number(value) * 100).toFixed(1)}%` : '—';
const signedBps = value => finite(value) ? `${Number(value) >= 0 ? '+' : ''}${number(value)} bps` : '—';
const titleWords = value => String(value || 'unknown').replaceAll('_', ' ').replaceAll('-', ' ').replace(/\b\w/g, letter => letter.toUpperCase());
const escapeHtml = value => String(value ?? '')
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;').replaceAll("'", '&#039;');
const valueClass = value => Number(value) > 0 ? 'positive' : Number(value) < 0 ? 'negative' : '';
const dateTime = value => {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString('en-US');
};
const relativeTime = value => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  const seconds = Math.round((Date.now() - date.getTime()) / 1000);
  const absolute = Math.abs(seconds);
  if (absolute < 45) return seconds >= 0 ? 'just now' : 'in moments';
  if (absolute < 3600) return `${Math.round(absolute / 60)}m ${seconds >= 0 ? 'ago' : 'from now'}`;
  if (absolute < 86400) return `${Math.round(absolute / 3600)}h ${seconds >= 0 ? 'ago' : 'from now'}`;
  return `${Math.round(absolute / 86400)}d ${seconds >= 0 ? 'ago' : 'from now'}`;
};

function latestDecision(data) {
  return data?.decisions?.latest || data?.decisions?.recent?.[0] || null;
}

function latestQuote(data, decision = latestDecision(data)) {
  const quotes = data?.pricing?.recentQuotes || [];
  return quotes.find(row => row.id === decision?.modelQuoteId) || quotes[0] || null;
}

function latestExecutionCost(data, decision = latestDecision(data)) {
  const rows = data?.executionCosts?.recent || [];
  return rows.find(row => row.id === decision?.executionCostSnapshotId) || data?.executionCosts?.latest || rows[0] || null;
}

function pricingFresh(data) {
  const snapshot = data?.pricing?.latestSnapshot;
  if (!snapshot?.fetchedAt) return false;
  return (Date.now() - new Date(snapshot.fetchedAt).getTime()) / 1000 <= PRICING_MAX_AGE_SECONDS;
}

function forecastFresh(forecast) {
  return Boolean(forecast?.status === 'valid' && new Date(forecast.expiresAt || 0) >= new Date());
}

function quoteCost(quote) {
  return quote?.actualCostUsd ?? quote?.authoritativeCostUsd ?? quote?.estimatedCostUsd ?? null;
}

function actionError(body, fallback) {
  if (Array.isArray(body?.errors) && body.errors.length) return body.errors.join(', ');
  return body?.error || body?.reason || fallback;
}

async function request(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || 'GET',
    headers: { accept: 'application/json', 'content-type': 'application/json', ...(options.headers || {}) },
    body: options.body === undefined ? undefined : JSON.stringify(options.body),
  });
  const body = await response.json().catch(() => ({}));
  if (!response.ok || body.ok === false) throw new Error(actionError(body, `${path} returned ${response.status}`));
  return body;
}

function injectStylesheet() {
  if (document.querySelector('link[data-economic-styles]')) return;
  const link = document.createElement('link');
  link.rel = 'stylesheet';
  link.href = '/ui/economics.css';
  link.dataset.economicStyles = 'true';
  document.head.append(link);
}

function viewMarkup() {
  return `
    <section id="economics" class="view">
      <div class="view-intro economic-view-intro">
        <div>
          <p class="eyebrow">ECONOMIC DECISION ENGINE</p>
          <h2>Is the edge worth the intelligence?</h2>
          <p>Follow the complete economic lifecycle from market evidence and forecast through model pricing, provider-reported cost, execution permission, and settled counterfactual attribution.</p>
        </div>
        <div>
          <div class="economic-heading-actions">
            <button id="economic-run-maintenance" type="button" class="economic-action-primary">Run maintenance</button>
            <button id="economic-refresh-pricing" type="button">Refresh pricing</button>
            <span id="economic-overall" class="leader-pill">Waiting for economic evidence</span>
          </div>
          <p id="economic-action-status" class="economic-action-status" aria-live="polite"></p>
        </div>
      </div>

      <div id="economic-alert" class="alert-stack" aria-live="polite"></div>
      <div id="economic-lifecycle" class="economic-lifecycle" aria-label="Economic decision lifecycle"></div>
      <div id="economic-summary" class="economic-summary-grid"></div>

      <div class="economic-grid">
        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">PRICE FORECAST</p><h2>Calibrated market range</h2></div><span id="economic-forecast-status" class="badge">unknown</span></div>
          <div id="economic-forecast" class="economic-body"></div>
        </article>

        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">INTELLIGENCE PURCHASE</p><h2>Estimate versus actual cost</h2></div><span id="economic-intelligence-status" class="badge">unknown</span></div>
          <div id="economic-intelligence" class="economic-body"></div>
        </article>

        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">EXECUTABLE EDGE</p><h2>Forecast after every cost</h2></div><span id="economic-execution-status" class="badge">unknown</span></div>
          <div id="economic-edge" class="economic-body"></div>
        </article>

        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">MAINTENANCE</p><h2>Economic runtime health</h2></div><span id="economic-maintenance-status" class="badge">unknown</span></div>
          <div id="economic-maintenance" class="economic-body"></div>
        </article>

        <article class="economic-card wide">
          <div class="economic-card-head"><div><p class="section-kicker">AGENT ATTRIBUTION</p><h2>Value versus the bot counterfactual</h2></div><span id="economic-attribution-status" class="badge">observing</span></div>
          <div id="economic-attribution" class="economic-body"></div>
        </article>

        <article class="economic-card wide">
          <div class="economic-card-head"><div><p class="section-kicker">MODEL GOVERNANCE</p><h2>Pricing freshness, usage reconciliation, and calibration</h2></div><span id="economic-pricing-status" class="badge">unknown</span></div>
          <div id="economic-governance" class="economic-body"></div>
        </article>

        <article class="economic-card wide">
          <div class="economic-card-head"><div><p class="section-kicker">DECISION HISTORY</p><h2>Recent economic gates</h2></div><span id="economic-decision-count" class="count-pill">0</span></div>
          <div id="economic-decisions" class="economic-body"></div>
        </article>
      </div>
    </section>`;
}

function injectView() {
  injectStylesheet();
  if (!document.getElementById('economics')) {
    const system = document.getElementById('system');
    system?.insertAdjacentHTML('beforebegin', viewMarkup());
  }

  if (!document.querySelector('[data-economic-nav]')) {
    const nav = document.getElementById('nav-tabs');
    const controlLabel = [...(nav?.querySelectorAll('.nav-label') || [])].find(row => row.textContent.trim() === 'Control');
    const link = document.createElement('a');
    link.href = '#economics';
    link.dataset.economicNav = 'true';
    link.innerHTML = '<span class="nav-index">07</span><span>Economics</span><b id="nav-economic-count" class="nav-count warning">0</b>';
    if (controlLabel) nav.insertBefore(link, controlLabel);
    else nav?.append(link);
    const systemIndex = document.querySelector('a[href="#system"] .nav-index');
    if (systemIndex) systemIndex.textContent = '08';
  }

  const safetyStrip = document.querySelector('.safety-strip');
  if (safetyStrip && !document.getElementById('strip-economics')) {
    const raceBadge = document.getElementById('race-validity');
    const item = document.createElement('div');
    item.innerHTML = '<small>Economics</small><strong id="strip-economics">Unknown</strong>';
    safetyStrip.insertBefore(item, raceBadge || null);
    safetyStrip.classList.add('economic-enabled');
  }
}

function showEconomics() {
  document.querySelectorAll('.view').forEach(view => view.classList.toggle('active-view', view.id === 'economics'));
  document.querySelectorAll('#nav-tabs a').forEach(link => link.classList.toggle('nav-active', link.dataset.economicNav === 'true'));
  const title = document.getElementById('page-title');
  const subtitle = document.getElementById('page-subtitle');
  if (title) title.textContent = 'Economics';
  if (subtitle) subtitle.textContent = 'Market evidence, model cost, execution permission, maintenance health, and settled counterfactual value in one lifecycle.';
  history.replaceState(null, '', '#economics');
  window.scrollTo({ top: 0, behavior: 'smooth' });
  refreshEconomics();
}

function badge(id, label, status = 'warn') {
  const element = document.getElementById(id);
  if (!element) return;
  element.textContent = label;
  element.className = `badge ${status === 'ok' ? 'badge-ok' : status === 'error' ? 'badge-err' : 'badge-warn'}`;
}

function lifecycleState(data) {
  const forecast = data?.forecasts?.latest;
  const decision = latestDecision(data);
  const quote = latestQuote(data, decision);
  const marketReady = Number(forecast?.observationCount || 0) >= 5;
  const forecastReady = forecastFresh(forecast);
  const pricingReady = pricingFresh(data);
  const quoteReady = Boolean(quote);
  const reconciled = quote?.status === 'reconciled';
  const executable = decision?.executionAllowed === true;
  return [
    { label: 'Market data', detail: marketReady ? `${number(forecast.observationCount, 0)} observations` : 'Need five prices', state: marketReady ? 'complete' : 'blocked' },
    { label: 'Forecast', detail: forecastReady ? `Expires ${relativeTime(forecast.expiresAt)}` : forecast ? 'Expired or invalid' : 'Not generated', state: forecastReady ? 'complete' : 'blocked' },
    { label: 'Pricing', detail: pricingReady ? 'Catalog fresh' : 'Refresh required', state: pricingReady ? 'complete' : 'blocked' },
    { label: 'Model quote', detail: quoteReady ? money(quote.estimatedCostUsd) : 'No quote', state: quoteReady ? 'complete' : 'blocked' },
    { label: 'Actual usage', detail: reconciled ? money(quote.actualCostUsd) : quoteReady ? titleWords(quote.status) : 'Not started', state: reconciled ? 'complete' : quoteReady ? 'warning' : 'blocked' },
    { label: 'Trade gate', detail: executable ? `${money(decision.netExecutableEdgeUsd)} net edge` : decision ? titleWords(decision.blockers?.[0] || 'blocked') : 'Not evaluated', state: executable ? 'complete' : decision ? 'blocked' : 'warning' },
  ];
}

function renderLifecycle(data) {
  const container = document.getElementById('economic-lifecycle');
  if (!container) return;
  container.innerHTML = lifecycleState(data).map((row, index) => `
    <div class="economic-lifecycle-step ${row.state}">
      <span class="economic-lifecycle-dot">${index + 1}</span>
      <strong>${escapeHtml(row.label)}</strong>
      <small>${escapeHtml(row.detail)}</small>
    </div>`).join('');
}

function renderSummary(data) {
  const forecast = data?.forecasts?.latest;
  const decision = latestDecision(data);
  const quote = latestQuote(data, decision);
  const maintenance = data?.maintenance || {};
  const pendingAttribution = Number(data?.attribution?.pendingAttribution ?? data?.attribution?.pending?.length ?? 0);
  const rows = [
    ['Forecast edge', signedBps(forecast?.expectedReturnBps), forecast ? `${escapeHtml(forecast.symbol || 'unknown')} · ${number(forecast.horizonMinutes, 0)}m` : 'No forecast'],
    ['Model cost', money(quoteCost(quote)), quote?.status === 'reconciled' ? 'Provider-reported actual' : quote ? 'Pre-call estimate' : 'No quote'],
    ['Net executable edge', money(decision?.netExecutableEdgeUsd), decision?.executionAllowed ? 'Execution allowed' : 'Execution blocked'],
    ['Pending attribution', number(pendingAttribution, 0), pendingAttribution ? 'Counterfactual evidence needed' : 'No unresolved records'],
    ['Maintenance', titleWords(maintenance.status || 'never_run'), maintenance.lastRunAt ? `Ran ${relativeTime(maintenance.lastRunAt)}` : 'Worker has not run'],
  ];
  const container = document.getElementById('economic-summary');
  if (container) container.innerHTML = rows.map(([label, value, detail]) => `<article class="economic-summary-card"><small>${escapeHtml(label)}</small><strong>${value}</strong><span>${detail}</span></article>`).join('');
}

function rangePosition(value, low, high) {
  if (!finite(value) || !finite(low) || !finite(high) || Number(high) <= Number(low)) return 50;
  return Math.max(0, Math.min(100, ((Number(value) - Number(low)) / (Number(high) - Number(low))) * 100));
}

function renderForecast(data) {
  const forecast = data?.forecasts?.latest;
  const container = document.getElementById('economic-forecast');
  if (!container) return;
  if (!forecast) {
    badge('economic-forecast-status', 'No forecast', 'warn');
    container.innerHTML = '<p class="economic-empty">No probabilistic price forecast has been published. The trade gate remains closed until at least five fresh observations produce a forecast.</p>';
    return;
  }

  const fresh = forecastFresh(forecast);
  badge('economic-forecast-status', fresh ? 'Fresh' : 'Expired', fresh ? 'ok' : 'error');
  const low = Math.min(Number(forecast.p10Price), Number(forecast.currentPrice), Number(forecast.expectedPrice));
  const high = Math.max(Number(forecast.p90Price), Number(forecast.currentPrice), Number(forecast.expectedPrice));
  const currentPosition = rangePosition(forecast.currentPrice, low, high);
  const expectedPosition = rangePosition(forecast.expectedPrice, low, high);
  const p10Position = rangePosition(forecast.p10Price, low, high);
  const p90Position = rangePosition(forecast.p90Price, low, high);
  const bandWidth = Math.max(1, p90Position - p10Position);

  container.innerHTML = `
    <div class="economic-hero"><strong>${money(forecast.expectedPrice)}</strong><span>${escapeHtml(forecast.symbol)} · ${number(forecast.horizonMinutes, 0)}m expected price</span></div>
    <p class="economic-range">80% range ${money(forecast.p10Price)} – ${money(forecast.p90Price)} · current ${money(forecast.currentPrice)}</p>
    <div class="economic-range-chart" aria-label="Forecast price range">
      <div class="economic-range-track"></div>
      <div class="economic-range-band" style="left:${p10Position}%;width:${bandWidth}%"></div>
      <div class="economic-range-marker" style="left:${currentPosition}%"><span>Current</span></div>
      <div class="economic-range-marker expected" style="left:${expectedPosition}%"><span>Expected</span></div>
    </div>
    <div class="economic-range-labels"><span>${money(low)}</span><span>${money(high)}</span></div>
    <div class="economic-metrics">
      <div><small>Probability up</small><strong>${percent(forecast.probabilityUp)}</strong></div>
      <div><small>Expected return</small><strong>${signedBps(forecast.expectedReturnBps)}</strong></div>
      <div><small>Expected volatility</small><strong>${number(forecast.expectedVolatilityBps)} bps</strong></div>
      <div><small>Observations</small><strong>${number(forecast.observationCount, 0)}</strong></div>
      <div><small>Regime</small><strong>${escapeHtml(titleWords(forecast.regime))}</strong></div>
      <div><small>Model</small><strong>${escapeHtml(forecast.modelVersion || 'unknown')}</strong></div>
      <div><small>Generated</small><strong>${escapeHtml(relativeTime(forecast.asOf || forecast.createdAt))}</strong></div>
      <div><small>Expires</small><strong>${escapeHtml(relativeTime(forecast.expiresAt))}</strong></div>
      <div><small>Outcome target</small><strong>${escapeHtml(dateTime(forecast.targetObservedAt))}</strong></div>
    </div>
    <p class="economic-footer-note">This is a probabilistic range, not a guaranteed target. Expired forecasts cannot authorize execution.</p>`;
}

function renderIntelligence(data) {
  const decision = latestDecision(data);
  const quote = latestQuote(data, decision);
  const container = document.getElementById('economic-intelligence');
  if (!container) return;
  if (!quote) {
    badge('economic-intelligence-status', 'No quote', 'warn');
    container.innerHTML = '<p class="economic-empty">No scoped model request has been quoted. Remote intelligence requires a fresh pricing snapshot, an explicit opportunity, and a value-of-information decision.</p>';
    return;
  }

  const reconciled = quote.status === 'reconciled';
  const failed = quote.status === 'failed';
  const allowed = decision?.intelligenceAllowed === true;
  const statusLabel = failed ? 'Request failed' : reconciled ? 'Actual cost reconciled' : allowed ? 'Purchase allowed' : 'Purchase blocked';
  badge('economic-intelligence-status', statusLabel, failed ? 'error' : reconciled ? 'ok' : allowed ? 'warn' : 'error');
  const estimate = Number(quote.estimatedCostUsd || 0);
  const actual = finite(quote.actualCostUsd) ? Number(quote.actualCostUsd) : null;
  const variance = actual == null ? null : actual - estimate;
  const costSource = quote.costSource || (reconciled ? 'provider_reported_actual' : 'pre_call_estimate');

  container.innerHTML = `
    <div class="economic-hero"><strong>${money(quoteCost(quote))}</strong><span>${escapeHtml(quote.model || 'Unknown model')}</span></div>
    <p class="economic-range">${reconciled ? `Actual provider cost · estimate ${money(estimate)}` : `Conservative pre-call estimate · ${escapeHtml(titleWords(quote.status))}`} · ${escapeHtml(titleWords(decision?.decisionPhase || 'not evaluated'))}</p>
    <div class="economic-metrics">
      <div><small>Provider</small><strong>${escapeHtml(titleWords(quote.provider || 'unknown'))}</strong></div>
      <div><small>Cost source</small><strong>${escapeHtml(titleWords(costSource))}</strong></div>
      <div><small>Cost variance</small><strong class="${valueClass(variance == null ? 0 : -variance)}">${variance == null ? '—' : money(variance)}</strong></div>
      <div><small>Maximum spend</small><strong>${money(decision?.maximumIntelligenceSpendUsd)}</strong></div>
      <div><small>Expected uplift</small><strong>${money(decision?.expectedDecisionImprovementUsd)}</strong></div>
      <div><small>Uplift coverage</small><strong>${number(decision?.expectedUpliftCoverage)}×</strong></div>
      <div><small>Required coverage</small><strong>${number(decision?.requiredCostCoverageMultiple)}×</strong></div>
      <div><small>Generation ID</small><strong>${escapeHtml(quote.generationId || 'not assigned')}</strong></div>
      <div><small>Pricing snapshot</small><strong>${escapeHtml(quote.pricingSnapshotId || 'local')}</strong></div>
    </div>
    ${quote.failureReason ? `<div class="economic-status-note error">${escapeHtml(titleWords(quote.failureReason))}</div>` : ''}
    <p class="economic-footer-note">Provider-reported actual cost becomes authoritative after reconciliation. A pre-call purchase decision never authorizes the trade by itself.</p>`;
}

function renderEdge(data) {
  const decision = latestDecision(data);
  const executionCost = latestExecutionCost(data, decision);
  const container = document.getElementById('economic-edge');
  if (!container) return;
  if (!decision) {
    badge('economic-execution-status', 'Not evaluated', 'warn');
    container.innerHTML = '<p class="economic-empty">No complete economic decision exists. A fresh forecast, venue-cost snapshot, and reconciled model usage record are required for paid-agent execution.</p>';
    return;
  }

  badge('economic-execution-status', decision.executionAllowed ? 'Execution allowed' : 'Execution blocked', decision.executionAllowed ? 'ok' : 'error');
  const rows = [
    ['Gross predicted edge', decision.predictedEdgeUsd],
    ['Venue execution costs', -Number(decision.executionCostsUsd || 0)],
    ['Model / API cost', -Number(decision.modelCostUsd || 0)],
    ['Uncertainty reserve', -Number(decision.uncertaintyReserveUsd || 0)],
    ['Latency decay', -Number(decision.latencyDecayUsd || 0)],
  ];
  const evidence = [
    ['Forecast', decision.forecastFresh],
    ['Venue preview', decision.executionCostFresh],
    ['Quote freshness', decision.quoteFresh],
    ['Usage reconciled', decision.modelUsageReconciled],
    ['Intelligence economic', decision.intelligenceAllowed],
  ];

  container.innerHTML = `
    <div class="economic-waterfall">${rows.map(([label, value]) => `<div class="economic-waterfall-row"><span>${escapeHtml(label)}</span><strong class="${Number(value) < 0 ? 'negative' : 'positive'}">${money(value)}</strong></div>`).join('')}<div class="economic-waterfall-row total"><span>Net executable edge</span><strong class="${valueClass(decision.netExecutableEdgeUsd)}">${money(decision.netExecutableEdgeUsd)}</strong></div></div>
    <div class="economic-evidence-row">${evidence.map(([label, ok]) => `<span class="economic-chip ${ok ? 'ok' : 'error'}">${escapeHtml(label)}: ${ok ? 'yes' : 'no'}</span>`).join('')}</div>
    ${decision.blockers?.length ? `<div class="economic-blockers">${decision.blockers.map(row => `<span class="economic-blocker">${escapeHtml(titleWords(row))}</span>`).join('')}</div>` : ''}
    <div class="economic-metrics">
      <div><small>Decision phase</small><strong>${escapeHtml(titleWords(decision.decisionPhase || 'unknown'))}</strong></div>
      <div><small>Symbol</small><strong>${escapeHtml(decision.symbol || executionCost?.symbol || 'unknown')}</strong></div>
      <div><small>Notional</small><strong>${money(executionCost?.notionalUsd)}</strong></div>
      <div><small>Execution-cost source</small><strong>${escapeHtml(titleWords(executionCost?.source || 'unknown'))}</strong></div>
      <div><small>Preview valid until</small><strong>${escapeHtml(relativeTime(executionCost?.validUntil))}</strong></div>
      <div><small>Cost snapshot ID</small><strong>${escapeHtml(decision.executionCostSnapshotId || 'none')}</strong></div>
    </div>
    <p class="economic-footer-note">Execution permission expires with its evidence. A positive gross forecast alone is insufficient.</p>`;
}

function renderMaintenance(data) {
  const maintenance = data?.maintenance || {};
  const status = maintenance.status || 'never_run';
  const healthy = status === 'ok';
  const degraded = status === 'degraded';
  badge('economic-maintenance-status', titleWords(status), healthy ? 'ok' : degraded ? 'error' : 'warn');
  const counters = maintenance.counters || {};
  const warnings = maintenance.warnings || [];
  const container = document.getElementById('economic-maintenance');
  if (!container) return;

  container.innerHTML = `
    <div class="economic-maintenance-layout">
      <div>
        <div class="economic-hero"><strong>${healthy ? 'Healthy' : degraded ? 'Degraded' : 'Idle'}</strong><span>${maintenance.lastRunAt ? `last run ${relativeTime(maintenance.lastRunAt)}` : 'no recorded run'}</span></div>
        <div class="economic-metrics">
          <div><small>Pricing refreshed</small><strong>${number(counters.pricingRefreshed, 0)}</strong></div>
          <div><small>Market snapshots</small><strong>${number(counters.marketSnapshotsAdded, 0)}</strong></div>
          <div><small>Forecasts created</small><strong>${number(counters.forecastsCreated, 0)}</strong></div>
          <div><small>Outcomes matured</small><strong>${number(counters.forecastOutcomesCreated, 0)}</strong></div>
          <div><small>Attributions recorded</small><strong>${number(counters.attributionsRecorded, 0)}</strong></div>
          <div><small>Attributions pending</small><strong>${number(counters.attributionsPending, 0)}</strong></div>
        </div>
      </div>
      <div class="economic-maintenance-actions">
        <div class="economic-status-note ${degraded ? 'error' : !healthy ? 'warning' : ''}">${healthy ? 'Pricing, quote history, forecasts, forecast outcomes, and paid-agent settlement attribution completed without warnings.' : degraded ? 'The last run completed with warnings. Review the evidence below before trusting new economic decisions.' : 'The maintenance worker has not completed a run. Use the guarded action or supervise pnpm economics:worker.'}</div>
        <div class="economic-list">${warnings.length ? warnings.map(warning => `<div class="economic-list-row"><div><strong>${escapeHtml(titleWords(warning))}</strong><small>Maintenance remained fail-closed for this dependency.</small></div><span>Review</span></div>`).join('') : '<div class="economic-list-row"><div><strong>No maintenance warnings</strong><small>Unknown evidence still remains unknown; this only describes the last recorded maintenance run.</small></div><span>Observed</span></div>'}</div>
      </div>
    </div>`;
}

function renderAttribution(data) {
  const summary = data?.attribution || {};
  const pending = Array.isArray(summary.pending) ? summary.pending : [];
  const observed = Number(summary.observations || 0) > 0;
  const pendingCount = Number(summary.pendingAttribution ?? pending.length ?? 0);
  const container = document.getElementById('economic-attribution');
  if (!container) return;
  badge('economic-attribution-status', observed ? `${number(summary.observations, 0)} outcomes` : pendingCount ? `${pendingCount} pending` : 'No outcomes', observed ? 'ok' : pendingCount ? 'warn' : 'warn');

  const summaryMarkup = observed ? `
    <div class="economic-hero"><strong class="${valueClass(summary.incrementalPnlUsd)}">${money(summary.incrementalPnlUsd)}</strong><span>incremental P&amp;L versus bot counterfactual</span></div>
    <div class="economic-metrics">
      <div><small>Changed decisions</small><strong>${number(summary.changedDecisions, 0)}</strong></div>
      <div><small>Agent cost</small><strong>${money(summary.agentCostUsd)}</strong></div>
      <div><small>Incremental ROI</small><strong>${number(summary.incrementalRoi)}×</strong></div>
      <div><small>Override win rate</small><strong>${percent(summary.agentOverrideWinRate)}</strong></div>
      <div><small>Profitable overrides</small><strong>${number(summary.profitableOverrides, 0)}</strong></div>
      <div><small>Harmful overrides</small><strong>${number(summary.harmfulOverrides, 0)}</strong></div>
      <div><small>Avoided loss value</small><strong>${money(summary.avoidedLossValueUsd)}</strong></div>
      <div><small>Cost / changed decision</small><strong>${money(summary.costPerChangedDecisionUsd)}</strong></div>
      <div><small>Pending attribution</small><strong>${number(pendingCount, 0)}</strong></div>
    </div>` : '<p class="economic-empty">No settled paid-agent outcome has been compared with a deterministic bot counterfactual. Agent value remains unproven until attribution records exist.</p>';

  const pendingMarkup = pending.length ? `<div class="economic-pending-list">${pending.slice(0, 10).map(row => `<div class="economic-list-row"><div><strong>${escapeHtml(row.executionId || row.id || 'Pending attribution')}</strong><small>${escapeHtml((row.blockers || []).map(titleWords).join(' · ') || 'Missing attribution evidence')}</small></div><span>${escapeHtml(row.opportunityId || 'No opportunity')}</span></div>`).join('')}</div>` : '<div class="economic-status-note">No unresolved paid-agent attribution records are currently queued.</div>';
  container.innerHTML = `${summaryMarkup}${pendingMarkup}<p class="economic-footer-note">The UI never fabricates counterfactual P&amp;L. Missing realized or bot-replay evidence remains visibly pending.</p>`;
}

function renderGovernance(data) {
  const pricing = data?.pricing || {};
  const snapshot = pricing.latestSnapshot;
  const calibration = data?.forecasts?.calibration || {};
  const fresh = pricingFresh(data);
  const quotes = pricing.recentQuotes || [];
  const container = document.getElementById('economic-governance');
  if (!container) return;
  badge('economic-pricing-status', fresh ? 'Pricing fresh' : 'Pricing stale / missing', fresh ? 'ok' : 'error');

  container.innerHTML = `
    <div class="economic-metrics">
      <div><small>Catalog models</small><strong>${number(snapshot?.modelCount, 0)}</strong></div>
      <div><small>Pricing fetched</small><strong>${snapshot ? escapeHtml(relativeTime(snapshot.fetchedAt)) : '—'}</strong></div>
      <div><small>Pricing snapshots</small><strong>${number(data?.pricing?.latestSnapshot ? 1 : 0, 0)} latest</strong></div>
      <div><small>Unreconciled quotes</small><strong>${number(pricing.unreconciledQuotes, 0)}</strong></div>
      <div><small>Awaiting reconciliation</small><strong>${number(data?.decisions?.awaitingReconciliation, 0)}</strong></div>
      <div><small>Pending forecast outcomes</small><strong>${number(data?.forecasts?.pendingOutcomes, 0)}</strong></div>
      <div><small>Calibration samples</small><strong>${number(calibration.samples, 0)}</strong></div>
      <div><small>Brier score</small><strong>${number(calibration.brierScore, 4)}</strong></div>
      <div><small>Directional accuracy</small><strong>${percent(calibration.directionalAccuracy)}</strong></div>
      <div><small>80% interval coverage</small><strong>${percent(calibration.p10P90Coverage)}</strong></div>
      <div><small>Mean absolute error</small><strong>${calibration.meanAbsoluteErrorPct == null ? '—' : `${number(calibration.meanAbsoluteErrorPct)}%`}</strong></div>
      <div><small>Decisions blocked</small><strong>${number(data?.decisions?.blocked, 0)}</strong></div>
    </div>
    <div class="economic-pending-list">${quotes.length ? quotes.slice(0, 8).map(quote => `<div class="economic-list-row"><div><strong>${escapeHtml(quote.model || 'Unknown model')}</strong><small>${escapeHtml(titleWords(quote.costSource || quote.status))} · ${escapeHtml(quote.generationId || quote.id || 'No generation ID')}</small></div><span>${money(quoteCost(quote))}<br>${escapeHtml(relativeTime(quote.reconciledAt || quote.requestedAt))}</span></div>`).join('') : '<div class="economic-list-row"><div><strong>No model quotes</strong><small>No paid intelligence request has entered the economic ledger.</small></div><span>Empty</span></div>'}</div>
    <p class="economic-footer-note">Pricing age, provider usage, and forecast calibration remain separate evidence. Fresh pricing does not imply a calibrated forecast, and a reconciled model bill does not imply profitable intelligence.</p>`;
}

function renderDecisions(data) {
  const decisions = data?.decisions?.recent || [];
  const container = document.getElementById('economic-decisions');
  const countElement = document.getElementById('economic-decision-count');
  if (countElement) countElement.textContent = String(decisions.length);
  if (!container) return;
  if (!decisions.length) {
    container.innerHTML = '<p class="economic-empty">No economic decisions have been evaluated.</p>';
    return;
  }
  container.innerHTML = `<div class="economic-table-wrap"><table class="economic-table"><thead><tr><th>Created</th><th>Symbol</th><th>Phase</th><th>Model cost</th><th>Net edge</th><th>Permission</th><th>Blockers</th></tr></thead><tbody>${decisions.slice(0, 20).map(row => `<tr><td><strong>${escapeHtml(relativeTime(row.createdAt))}</strong><small>${escapeHtml(row.id || 'unknown')}</small></td><td>${escapeHtml(row.symbol || '—')}</td><td>${escapeHtml(titleWords(row.decisionPhase || 'unknown'))}</td><td><strong>${money(row.modelCostUsd)}</strong><small>${escapeHtml(titleWords(row.modelCostSource || 'unknown'))}</small></td><td class="${valueClass(row.netExecutableEdgeUsd)}">${money(row.netExecutableEdgeUsd)}</td><td>${row.executionAllowed ? '<span class="badge badge-ok">Allowed</span>' : '<span class="badge badge-err">Blocked</span>'}</td><td>${escapeHtml((row.blockers || []).map(titleWords).join(', ') || 'None')}</td></tr>`).join('')}</tbody></table></div>`;
}

function economicIssues(data) {
  const issues = [];
  const maintenance = data?.maintenance || {};
  const decision = latestDecision(data);
  const pending = Number(data?.attribution?.pendingAttribution ?? data?.attribution?.pending?.length ?? 0);
  if (!pricingFresh(data)) issues.push('model pricing is stale or missing');
  if (!['ok'].includes(maintenance.status)) issues.push(maintenance.status === 'degraded' ? 'economic maintenance is degraded' : 'economic maintenance has not completed');
  if (Number(data?.pricing?.unreconciledQuotes || 0) > 0) issues.push(`${number(data.pricing.unreconciledQuotes, 0)} model quote${Number(data.pricing.unreconciledQuotes) === 1 ? '' : 's'} need actual-cost reconciliation`);
  if (pending > 0) issues.push(`${number(pending, 0)} settled agent outcome${pending === 1 ? '' : 's'} need counterfactual evidence`);
  if (decision && !decision.executionAllowed && decision.blockers?.length) issues.push(`latest trade gate: ${titleWords(decision.blockers[0])}`);
  return issues;
}

function renderTodayBridge(data) {
  const issues = economicIssues(data);
  const decision = latestDecision(data);
  const strip = document.getElementById('strip-economics');
  if (strip) strip.textContent = decision?.executionAllowed ? 'Executable' : issues.length ? `${issues.length} issue${issues.length === 1 ? '' : 's'}` : 'Observed';

  const queue = document.getElementById('command-queue');
  if (!queue) return;
  let existing = queue.querySelector('[data-economic-attention]');
  const queueCount = document.getElementById('queue-count');
  if (!issues.length) {
    if (existing) {
      existing.remove();
      if (queueCount && finite(queueCount.textContent)) queueCount.textContent = String(Math.max(0, Number(queueCount.textContent) - 1));
    }
    return;
  }

  const markup = `<span class="attention-marker ${issues.some(issue => /degraded|stale|missing/i.test(issue)) ? 'error' : ''}"></span><span class="attention-copy"><strong>Economic engine needs ${issues.length} action${issues.length === 1 ? '' : 's'}</strong><small>${escapeHtml(issues.slice(0, 3).join(' · '))}</small></span><span class="attention-action">Open →</span>`;
  if (!existing) {
    existing = document.createElement('button');
    existing.type = 'button';
    existing.className = 'attention-item';
    existing.dataset.viewTarget = 'economics';
    existing.dataset.economicAttention = 'true';
    existing.innerHTML = markup;
    queue.append(existing);
    if (queueCount && finite(queueCount.textContent)) queueCount.textContent = String(Number(queueCount.textContent) + 1);
  } else {
    existing.innerHTML = markup;
  }
}

function renderOverall(data) {
  const decision = latestDecision(data);
  const quote = latestQuote(data, decision);
  const issues = economicIssues(data);
  const overall = document.getElementById('economic-overall');
  if (overall) overall.textContent = decision?.executionAllowed
    ? `${money(decision.netExecutableEdgeUsd)} net executable edge`
    : quote && quote.status !== 'reconciled'
      ? `Actual usage pending: ${titleWords(quote.status)}`
      : decision
        ? `Blocked: ${titleWords(decision.blockers?.[0] || 'economic gate')}`
        : 'Waiting for economic evidence';

  const navCount = document.getElementById('nav-economic-count');
  if (navCount) navCount.textContent = String(issues.length);
  renderTodayBridge(data);
}

function renderAlerts(data) {
  const alert = document.getElementById('economic-alert');
  if (!alert) return;
  const warnings = data?.maintenance?.warnings || [];
  const parts = [];
  if (economicsState.error) parts.push(`<div class="alert error"><span>${escapeHtml(economicsState.error)}</span></div>`);
  if (warnings.length) parts.push(...warnings.slice(0, 3).map(warning => `<div class="alert error"><span>${escapeHtml(titleWords(warning))}</span></div>`));
  alert.innerHTML = parts.join('');
}

function render(data) {
  if (!data) {
    renderAlerts(data);
    return;
  }
  renderLifecycle(data);
  renderSummary(data);
  renderForecast(data);
  renderIntelligence(data);
  renderEdge(data);
  renderMaintenance(data);
  renderAttribution(data);
  renderGovernance(data);
  renderDecisions(data);
  renderOverall(data);
  renderAlerts(data);
}

function setActionUi(action, message = '') {
  economicsState.action = action;
  economicsState.actionMessage = message;
  const maintenanceButton = document.getElementById('economic-run-maintenance');
  const pricingButton = document.getElementById('economic-refresh-pricing');
  if (maintenanceButton) {
    maintenanceButton.disabled = Boolean(action);
    maintenanceButton.textContent = action === 'maintenance' ? 'Running…' : 'Run maintenance';
  }
  if (pricingButton) {
    pricingButton.disabled = Boolean(action);
    pricingButton.textContent = action === 'pricing' ? 'Refreshing…' : 'Refresh pricing';
  }
  const status = document.getElementById('economic-action-status');
  if (status) status.textContent = message;
}

async function runAction(kind) {
  if (economicsState.action) return;
  const path = kind === 'pricing' ? PRICING_REFRESH_ROUTE : MAINTENANCE_ROUTE;
  setActionUi(kind, kind === 'pricing' ? 'Refreshing the versioned OpenRouter pricing catalog…' : 'Running one guarded economic-maintenance cycle…');
  try {
    const body = await request(path, { method: 'POST', body: {} });
    const detail = kind === 'pricing'
      ? `${number(body.pricingSnapshot?.modelCount, 0)} models captured in ${body.pricingSnapshot?.id || 'a new snapshot'}.`
      : `${number(body.marketSnapshotsAdded, 0)} quotes, ${number(body.forecastsCreated, 0)} forecasts, ${number(body.forecastOutcomesCreated, 0)} outcomes, and ${number(body.attributionsRecorded, 0)} attributions processed.`;
    setActionUi(null, detail);
    await refreshEconomics(true);
  } catch (error) {
    setActionUi(null, `Action failed: ${error.message}`);
    economicsState.error = error.message;
    render(economicsState.data);
  }
}

async function refreshEconomics(force = false) {
  if (economicsState.refreshing && !force) return;
  economicsState.refreshing = true;
  try {
    economicsState.data = await request(ECONOMICS_ROUTE);
    economicsState.error = null;
  } catch (error) {
    economicsState.error = error.message;
  } finally {
    economicsState.refreshing = false;
    render(economicsState.data);
  }
}

function bindEvents() {
  document.addEventListener('click', event => {
    const economicTarget = event.target.closest('[data-economic-nav], [data-view-target="economics"]');
    if (economicTarget) {
      event.preventDefault();
      event.stopImmediatePropagation();
      showEconomics();
      return;
    }
    if (event.target.closest('#economic-run-maintenance')) runAction('maintenance');
    if (event.target.closest('#economic-refresh-pricing')) runAction('pricing');
  }, true);

  window.addEventListener('hashchange', () => {
    if (location.hash === '#economics') showEconomics();
  });

  const queue = document.getElementById('command-queue');
  if (queue) {
    const observer = new MutationObserver(() => {
      if (!economicsState.data || queue.querySelector('[data-economic-attention]')) return;
      queueMicrotask(() => renderTodayBridge(economicsState.data));
    });
    observer.observe(queue, { childList: true });
  }

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) refreshEconomics();
  });
}

injectView();
bindEvents();
if (location.hash === '#economics') showEconomics();
refreshEconomics();
setInterval(() => {
  if (!document.hidden) refreshEconomics();
}, 5000);
