const ECONOMICS_ROUTE = '/api/economics/dashboard';

const economicsState = {
  data: null,
  error: null,
  refreshing: false,
};

const money = value => Number.isFinite(Number(value))
  ? Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 4 })
  : '—';
const number = (value, digits = 2) => Number.isFinite(Number(value))
  ? Number(value).toLocaleString('en-US', { maximumFractionDigits: digits })
  : '—';
const percent = value => Number.isFinite(Number(value)) ? `${(Number(value) * 100).toFixed(1)}%` : '—';
const titleWords = value => String(value || 'unknown').replaceAll('_', ' ').replace(/\b\w/g, letter => letter.toUpperCase());
const escapeHtml = value => String(value ?? '')
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;').replaceAll("'", '&#039;');
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

function injectStyles() {
  if (document.getElementById('economic-ui-styles')) return;
  const style = document.createElement('style');
  style.id = 'economic-ui-styles';
  style.textContent = `
    .economic-grid { display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:12px; }
    .economic-card { border:1px solid var(--line); border-radius:12px; background:linear-gradient(180deg,rgba(21,31,43,.96),rgba(14,22,32,.96)); box-shadow:var(--shadow); overflow:hidden; }
    .economic-card.wide { grid-column:1/-1; }
    .economic-card-head { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; padding:13px 14px; border-bottom:1px solid var(--line-soft); }
    .economic-card-head h2 { margin-top:3px; font-size:16px; }
    .economic-body { padding:14px; }
    .economic-hero { display:flex; align-items:baseline; gap:10px; flex-wrap:wrap; }
    .economic-hero strong { font-size:clamp(27px,3.4vw,42px); letter-spacing:-.04em; font-variant-numeric:tabular-nums; }
    .economic-hero span { color:var(--muted); font-size:12px; }
    .economic-range { margin-top:8px; color:var(--muted); font-size:12px; }
    .economic-metrics { display:grid; grid-template-columns:repeat(3,minmax(0,1fr)); gap:8px; margin-top:13px; }
    .economic-metrics div { min-width:0; padding:9px; border:1px solid var(--line-soft); border-radius:8px; background:rgba(7,11,17,.34); }
    .economic-metrics small,.economic-metrics strong { display:block; }
    .economic-metrics small { color:var(--muted); font-size:9px; text-transform:uppercase; letter-spacing:.06em; }
    .economic-metrics strong { margin-top:4px; overflow:hidden; text-overflow:ellipsis; font-size:12px; font-variant-numeric:tabular-nums; }
    .economic-waterfall { display:grid; gap:7px; }
    .economic-waterfall-row { display:grid; grid-template-columns:minmax(0,1fr) auto; gap:12px; padding-bottom:7px; border-bottom:1px solid var(--line-soft); font-size:12px; }
    .economic-waterfall-row span { color:var(--muted); }
    .economic-waterfall-row.total { padding-top:4px; border-bottom:0; font-weight:850; }
    .economic-blockers { display:flex; flex-wrap:wrap; gap:6px; margin-top:12px; }
    .economic-blocker { padding:4px 7px; border-radius:999px; color:var(--amber); background:rgba(242,197,98,.11); font-size:10px; }
    .economic-empty { color:var(--muted); line-height:1.55; }
    .economic-footer-note { margin-top:12px; color:var(--muted-2); font-size:10px; line-height:1.5; }
    @media (max-width:900px) { .economic-grid { grid-template-columns:1fr; } .economic-card.wide { grid-column:auto; } }
    @media (max-width:620px) { .economic-metrics { grid-template-columns:repeat(2,minmax(0,1fr)); } }
  `;
  document.head.append(style);
}

function viewMarkup() {
  return `
    <section id="economics" class="view">
      <div class="view-intro">
        <div><p class="eyebrow">ECONOMIC DECISION ENGINE</p><h2>Is the edge worth the intelligence?</h2><p>Forecast value, venue costs, model pricing, uncertainty, and counterfactual agent value are evaluated before paid intelligence or execution is allowed.</p></div>
        <span id="economic-overall" class="leader-pill">Waiting for economic evidence</span>
      </div>
      <div id="economic-alert" class="alert-stack"></div>
      <div class="economic-grid">
        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">PRICE FORECAST</p><h2>Calibrated market range</h2></div><span id="economic-forecast-status" class="badge">unknown</span></div>
          <div id="economic-forecast" class="economic-body"></div>
        </article>
        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">INTELLIGENCE PURCHASE</p><h2>Why pay for a model?</h2></div><span id="economic-intelligence-status" class="badge">unknown</span></div>
          <div id="economic-intelligence" class="economic-body"></div>
        </article>
        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">EXECUTABLE EDGE</p><h2>Forecast after every cost</h2></div><span id="economic-execution-status" class="badge">unknown</span></div>
          <div id="economic-edge" class="economic-body"></div>
        </article>
        <article class="economic-card">
          <div class="economic-card-head"><div><p class="section-kicker">AGENT ATTRIBUTION</p><h2>Value versus the bot counterfactual</h2></div><span id="economic-attribution-status" class="badge">observing</span></div>
          <div id="economic-attribution" class="economic-body"></div>
        </article>
        <article class="economic-card wide">
          <div class="economic-card-head"><div><p class="section-kicker">MODEL GOVERNANCE</p><h2>Pricing freshness and forecast calibration</h2></div><span id="economic-pricing-status" class="badge">unknown</span></div>
          <div id="economic-governance" class="economic-body"></div>
        </article>
      </div>
    </section>`;
}

function injectView() {
  injectStyles();
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
    link.innerHTML = '<span class="nav-index">07</span><span>Economics</span><b id="nav-economic-count" class="nav-count">0</b>';
    if (controlLabel) nav.insertBefore(link, controlLabel);
    else nav?.append(link);
    const systemIndex = document.querySelector('a[href="#system"] .nav-index');
    if (systemIndex) systemIndex.textContent = '08';
    link.addEventListener('click', event => {
      event.preventDefault();
      event.stopImmediatePropagation();
      showEconomics();
    }, true);
  }
}

function showEconomics() {
  document.querySelectorAll('.view').forEach(view => view.classList.toggle('active-view', view.id === 'economics'));
  document.querySelectorAll('#nav-tabs a').forEach(link => link.classList.toggle('nav-active', link.dataset.economicNav === 'true'));
  const title = document.getElementById('page-title');
  const subtitle = document.getElementById('page-subtitle');
  if (title) title.textContent = 'Economics';
  if (subtitle) subtitle.textContent = 'What the forecast is worth, what intelligence costs, and whether an execution still has edge after every attributable cost.';
  history.replaceState(null, '', '#economics');
  window.scrollTo({ top: 0, behavior: 'smooth' });
  refreshEconomics();
}

function badge(id, label, status) {
  const element = document.getElementById(id);
  if (!element) return;
  element.textContent = label;
  element.className = `badge ${status === 'ok' ? 'badge-ok' : status === 'error' ? 'badge-err' : 'badge-warn'}`;
}

function renderForecast(data) {
  const forecast = data?.forecasts?.latest;
  const container = document.getElementById('economic-forecast');
  if (!container) return;
  if (!forecast) {
    badge('economic-forecast-status', 'No forecast', 'warn');
    container.innerHTML = '<p class="economic-empty">No probabilistic price forecast has been published. Paid-agent execution remains blocked without one.</p>';
    return;
  }
  const fresh = forecast.status === 'valid' && new Date(forecast.expiresAt) >= new Date();
  badge('economic-forecast-status', fresh ? 'Fresh' : 'Expired', fresh ? 'ok' : 'error');
  container.innerHTML = `
    <div class="economic-hero"><strong>${money(forecast.expectedPrice)}</strong><span>${escapeHtml(forecast.symbol)} · ${number(forecast.horizonMinutes, 0)}m horizon</span></div>
    <p class="economic-range">80% range ${money(forecast.p10Price)} – ${money(forecast.p90Price)} · current ${money(forecast.currentPrice)}</p>
    <div class="economic-metrics">
      <div><small>Probability up</small><strong>${percent(forecast.probabilityUp)}</strong></div>
      <div><small>Expected return</small><strong>${number(forecast.expectedReturnBps)} bps</strong></div>
      <div><small>Expected volatility</small><strong>${number(forecast.expectedVolatilityBps)} bps</strong></div>
      <div><small>Regime</small><strong>${escapeHtml(titleWords(forecast.regime))}</strong></div>
      <div><small>Model</small><strong>${escapeHtml(forecast.modelVersion)}</strong></div>
      <div><small>Expires</small><strong>${escapeHtml(relativeTime(forecast.expiresAt))}</strong></div>
    </div>`;
}

function renderIntelligence(data) {
  const decision = data?.decisions?.latest;
  const quote = data?.pricing?.recentQuotes?.find(row => row.id === decision?.modelQuoteId) || data?.pricing?.recentQuotes?.[0];
  const container = document.getElementById('economic-intelligence');
  if (!container) return;
  if (!decision && !quote) {
    badge('economic-intelligence-status', 'No purchase', 'warn');
    container.innerHTML = '<p class="economic-empty">No model request has been priced. Remote intelligence cannot be purchased until a quote and value-of-information decision exist.</p>';
    return;
  }
  const allowed = decision?.intelligenceAllowed === true;
  badge('economic-intelligence-status', allowed ? 'Economically allowed' : 'Blocked', allowed ? 'ok' : 'error');
  container.innerHTML = `
    <div class="economic-hero"><strong>${money(quote?.actualCostUsd ?? quote?.estimatedCostUsd)}</strong><span>${escapeHtml(quote?.model || 'No model selected')}</span></div>
    <p class="economic-range">${quote?.actualCostUsd != null ? `Actual provider cost · estimate ${money(quote.estimatedCostUsd)}` : 'Pre-call conservative quote'} · ${escapeHtml(titleWords(decision?.selectedTier || 'unselected'))}</p>
    <div class="economic-metrics">
      <div><small>Maximum spend</small><strong>${money(decision?.maximumIntelligenceSpendUsd)}</strong></div>
      <div><small>Expected uplift</small><strong>${money(decision?.expectedDecisionImprovementUsd)}</strong></div>
      <div><small>Required coverage</small><strong>${number(decision?.requiredCostCoverageMultiple)}×</strong></div>
      <div><small>Uplift coverage</small><strong>${number(decision?.expectedUpliftCoverage)}×</strong></div>
      <div><small>Quote status</small><strong>${escapeHtml(titleWords(quote?.status))}</strong></div>
      <div><small>Pricing snapshot</small><strong>${escapeHtml(quote?.pricingSnapshotId || 'local')}</strong></div>
    </div>`;
}

function renderEdge(data) {
  const decision = data?.decisions?.latest;
  const container = document.getElementById('economic-edge');
  if (!container) return;
  if (!decision) {
    badge('economic-execution-status', 'Not evaluated', 'warn');
    container.innerHTML = '<p class="economic-empty">No complete economic decision exists. A forecast, venue-cost snapshot, and model quote are required for paid-agent execution.</p>';
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
  container.innerHTML = `<div class="economic-waterfall">${rows.map(([label, value]) => `<div class="economic-waterfall-row"><span>${escapeHtml(label)}</span><strong class="${Number(value) < 0 ? 'negative' : 'positive'}">${money(value)}</strong></div>`).join('')}<div class="economic-waterfall-row total"><span>Net executable edge</span><strong class="${Number(decision.netExecutableEdgeUsd) > 0 ? 'positive' : 'negative'}">${money(decision.netExecutableEdgeUsd)}</strong></div></div>
    ${decision.blockers?.length ? `<div class="economic-blockers">${decision.blockers.map(row => `<span class="economic-blocker">${escapeHtml(titleWords(row))}</span>`).join('')}</div>` : ''}
    <p class="economic-footer-note">Execution permission expires with the underlying forecast, model quote, or venue preview. A positive gross forecast alone is insufficient.</p>`;
}

function renderAttribution(data) {
  const summary = data?.attribution || {};
  const container = document.getElementById('economic-attribution');
  if (!container) return;
  const observed = Number(summary.observations || 0) > 0;
  badge('economic-attribution-status', observed ? `${summary.observations} outcomes` : 'No outcomes', observed ? 'ok' : 'warn');
  container.innerHTML = observed ? `
    <div class="economic-hero"><strong class="${Number(summary.incrementalPnlUsd) >= 0 ? 'positive' : 'negative'}">${money(summary.incrementalPnlUsd)}</strong><span>incremental P&amp;L versus bot counterfactual</span></div>
    <div class="economic-metrics">
      <div><small>Changed decisions</small><strong>${number(summary.changedDecisions, 0)}</strong></div>
      <div><small>Agent cost</small><strong>${money(summary.agentCostUsd)}</strong></div>
      <div><small>Incremental ROI</small><strong>${number(summary.incrementalRoi)}×</strong></div>
      <div><small>Override win rate</small><strong>${percent(summary.agentOverrideWinRate)}</strong></div>
      <div><small>Harmful overrides</small><strong>${number(summary.harmfulOverrides, 0)}</strong></div>
      <div><small>Avoided loss value</small><strong>${money(summary.avoidedLossValueUsd)}</strong></div>
    </div>` : '<p class="economic-empty">No settled outcome has been compared with the deterministic bot counterfactual. Agent value remains unproven until attribution records exist.</p>';
}

function renderGovernance(data) {
  const pricing = data?.pricing || {};
  const snapshot = pricing.latestSnapshot;
  const calibration = data?.forecasts?.calibration || {};
  const container = document.getElementById('economic-governance');
  if (!container) return;
  const pricingFresh = snapshot && (Date.now() - new Date(snapshot.fetchedAt).getTime()) / 1000 <= 86400;
  badge('economic-pricing-status', pricingFresh ? 'Pricing fresh' : 'Pricing stale / missing', pricingFresh ? 'ok' : 'error');
  const navCount = document.getElementById('nav-economic-count');
  const issueCount = Number(pricing.unreconciledQuotes || 0) + Number(data?.decisions?.blocked || 0) + (pricingFresh ? 0 : 1);
  if (navCount) navCount.textContent = String(issueCount);
  container.innerHTML = `
    <div class="economic-metrics">
      <div><small>Catalog models</small><strong>${number(snapshot?.modelCount, 0)}</strong></div>
      <div><small>Pricing fetched</small><strong>${snapshot ? escapeHtml(relativeTime(snapshot.fetchedAt)) : '—'}</strong></div>
      <div><small>Unreconciled quotes</small><strong>${number(pricing.unreconciledQuotes, 0)}</strong></div>
      <div><small>Calibration samples</small><strong>${number(calibration.samples, 0)}</strong></div>
      <div><small>Brier score</small><strong>${number(calibration.brierScore, 4)}</strong></div>
      <div><small>Directional accuracy</small><strong>${percent(calibration.directionalAccuracy)}</strong></div>
      <div><small>80% interval coverage</small><strong>${percent(calibration.p10P90Coverage)}</strong></div>
      <div><small>Mean absolute error</small><strong>${calibration.meanAbsoluteErrorPct == null ? '—' : `${number(calibration.meanAbsoluteErrorPct)}%`}</strong></div>
      <div><small>Decisions blocked</small><strong>${number(data?.decisions?.blocked, 0)}</strong></div>
    </div>
    <p class="economic-footer-note">Provider-reported actual cost becomes authoritative after reconciliation. Forecast confidence should be discounted until calibration has enough samples across market regimes.</p>`;
}

function render(data) {
  renderForecast(data);
  renderIntelligence(data);
  renderEdge(data);
  renderAttribution(data);
  renderGovernance(data);
  const decision = data?.decisions?.latest;
  const overall = document.getElementById('economic-overall');
  if (overall) overall.textContent = decision?.executionAllowed
    ? `${money(decision.netExecutableEdgeUsd)} net executable edge`
    : decision
      ? `Blocked: ${titleWords(decision.blockers?.[0] || 'economic gate')}`
      : 'Waiting for economic evidence';
  const alert = document.getElementById('economic-alert');
  if (alert) alert.innerHTML = economicsState.error
    ? `<div class="alert error"><span>${escapeHtml(economicsState.error)}</span></div>`
    : '';
}

async function refreshEconomics() {
  if (economicsState.refreshing) return;
  economicsState.refreshing = true;
  try {
    const response = await fetch(ECONOMICS_ROUTE, { headers: { accept: 'application/json' } });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.error || `Economics API ${response.status}`);
    economicsState.data = body;
    economicsState.error = null;
  } catch (error) {
    economicsState.error = error.message;
  } finally {
    economicsState.refreshing = false;
    render(economicsState.data);
  }
}

injectView();
if (location.hash === '#economics') showEconomics();
refreshEconomics();
setInterval(() => {
  if (!document.hidden) refreshEconomics();
}, 5000);
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) refreshEconomics();
});
