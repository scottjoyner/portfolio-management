const $ = s => document.querySelector(s);
const $$ = s => document.querySelectorAll(s);
const fmt = v => JSON.stringify(v, null, 2);
const money = v => `$${Number(v || 0).toLocaleString(undefined, { maximumFractionDigits: 2 })}`;
const pct = v => `${(Number(v || 0) * 100).toFixed(1)}%`;
const timeAgo = ts => { const s = Math.floor((Date.now() - new Date(ts).getTime()) / 1000); return s < 5 ? 'just now' : s < 60 ? `${s}s ago` : s < 3600 ? `${Math.floor(s / 60)}m ago` : s < 86400 ? `${Math.floor(s / 3600)}h ago` : `${Math.floor(s / 86400)}d ago`; };

async function api(path, opts = {}) {
  const res = await fetch(path, { headers: { 'content-type': 'application/json', ...(opts.headers || {}) }, ...opts, body: opts.body ? JSON.stringify(opts.body) : undefined });
  const data = await res.json();
  if (!res.ok && !data) throw new Error(`Request failed: ${res.status}`);
  return data;
}

let activeTab = 'dashboard';
let pollingId = null;
let quotePollingId = null;
let prevPrices = {};
let allState = { summary: {}, accounts: [], positions: [], opportunities: [], executions: [], feed: [], config: {} };
let opportunityViewState = { status: 'all', venue: 'all', query: '' };
let polymarketSelection = null;
let auditViewState = { level: 'all', query: '' };

const CAPITAL_PRESETS = {
  conservative: { name: 'Conservative', targets: { reserve: 0.65, core: 0.25, opportunity: 0.10 }, coreAllowlist: ['BTC', 'ETH'], coreMinAllocationPct: 15, coreBatchFraction: 0.04, opportunityBatchFraction: 0.02 },
  balanced: { name: 'Balanced', targets: { reserve: 0.50, core: 0.20, opportunity: 0.30 }, coreAllowlist: ['BTC', 'ETH'], coreMinAllocationPct: 10, coreBatchFraction: 0.05, opportunityBatchFraction: 0.03 },
  aggressive: { name: 'Aggressive', targets: { reserve: 0.35, core: 0.20, opportunity: 0.45 }, coreAllowlist: ['BTC', 'ETH', 'SOL'], coreMinAllocationPct: 8, coreBatchFraction: 0.06, opportunityBatchFraction: 0.05 }
};

function updateSidebarStats(summary) {
  const c = summary.counts || {};
  $('#s-exec-count').textContent = c.executions || 0;
  $('#s-opp-count').textContent = c.opportunities || 0;
  $('#s-pos-count').textContent = c.positions || 0;
  $('#s-audit-count').textContent = c.auditEvents || 0;
}

function switchTab(tab) {
  activeTab = tab;
  $$('#nav-tabs a').forEach(a => a.classList.toggle('nav-active', a.getAttribute('href') === `#${tab}`));
  renderCurrentTab();
}

function renderFeed() {
  const feed = allState.feed || [];
  const el = $('#activity-feed');
  const count = $('#feed-count');
  if (!feed.length) { el.innerHTML = '<div class="feed-empty">No events yet — submit an execution or create an opportunity</div>'; count.textContent = '0 events'; return; }
  count.textContent = `${feed.length} events`;
  el.innerHTML = feed.slice(0, 40).map(e => {
    const actionClass = e.action?.includes('approv') ? 'feed-approve' : e.action?.includes('reject') || e.action?.includes('fail') || e.action?.includes('cancel') ? 'feed-reject' : e.action?.includes('fill') || e.action?.includes('settle') ? 'feed-fill' : 'feed-info';
    return `<div class="feed-item ${actionClass}"><span class="feed-dot"></span><span class="feed-action">${e.action || e.type}</span><span class="feed-detail">${(e.details || '').slice(0, 80)}</span><span class="feed-meta"><span class="label">${e.actor || ''}</span><span class="label">${e.timestamp ? timeAgo(e.timestamp) : ''}</span></span></div>`;
  }).join('');
}

async function pollFeed() {
  try {
    const data = await api('/api/activity-feed');
    allState.feed = data.feed || [];
  } catch { /* ignore */ }
  renderFeed();
}

async function pollQuotes() {
  const data = await api('/api/market-data/live-quotes').catch(() => ({ quotes: {} }));
  const quotes = data.quotes || {};
  const el = $('#ticker-inner');
  if (!el) return;
  const items = [];
  for (const [sym, q] of Object.entries(quotes)) {
    const prev = prevPrices[sym];
    const dir = prev !== undefined ? (q.mid > prev ? 'up' : q.mid < prev ? 'down' : 'same') : 'same';
    const dirArrow = dir === 'up' ? '\u25B2' : dir === 'down' ? '\u25BC' : '\u25C6';
    const dirClass = dir === 'up' ? 'ticker-up' : dir === 'down' ? 'ticker-down' : '';
    prevPrices[sym] = q.mid;
    items.push(`<div class="ticker-item ${dirClass}"><span class="ticker-sym">${sym.split('-')[0]}</span><span class="ticker-mid">${q.mid.toFixed(2)}</span><span class="ticker-arrow">${dirArrow}</span><span class="ticker-spread">${q.spreadBps.toFixed(1)}bps</span></div>`);
  }
  el.innerHTML = items.join('');
}

async function fetchAll() {
  const [summary, config] = await Promise.all([
    api('/api/operator/summary').catch(() => ({ counts: {} })),
    api('/api/config').catch(() => ({ config: {} }))
  ]);
  allState.summary = summary;
  allState.config = config.config || {};
  updateSidebarStats(summary);
  $('#topbar-refresh-time').textContent = new Date().toLocaleTimeString();
  return summary;
}

function renderKPI(summary) {
  const c = summary.counts || {};
  const el = $('#kpi-strip');
  el.innerHTML = [
    ['Executions', c.executions || 0],
    ['Filled', c.executions_filled || 0],
    ['Pending', c.executions_pending || 0],
    ['Failed', c.executions_failed || 0],
    ['Fills', c.settlement_fills || 0],
    ['Settled', c.settlement_settled || 0],
    ['Opportunities', c.opportunities || 0],
    ['Strategies', c.strategies || 0],
    ['Positions', c.positions || 0],
    ['Audit', c.auditEvents || 0]
  ].map(([label, val]) => `<div class="kpi-card"><span class="label">${label}</span><strong>${val}</strong></div>`).join('');
}

function getCapitalPolicy() {
  return allState.config.capitalPolicy || allState.config.capital_policy || CAPITAL_PRESETS.balanced;
}

function renderCapitalPolicySummary() {
  const policy = getCapitalPolicy();
  const t = policy.targets || {};
  return `<div class="mini-feed"><div class="mini-item"><span class="badge badge-ok">${policy.presetName || 'custom'}</span><span>Reserve ${pct(t.reserve || 0)} · Core ${pct(t.core || 0)} · Opportunity ${pct(t.opportunity || 0)}</span><span class="label">Core allowlist: ${(policy.coreAllowlist || []).join(', ')}</span></div></div>`;
}

function buildCapitalPolicyForm(policy = getCapitalPolicy()) {
  const presetName = policy.presetName || 'custom';
  const t = policy.targets || {};
  const allowlist = (policy.coreAllowlist || []).join(', ');
  const presetOptions = ['<option value="custom">Custom</option>'].concat(Object.entries(CAPITAL_PRESETS).map(([id, p]) => `<option value="${id}" ${id === presetName ? 'selected' : ''}>${p.name}</option>`)).join('');
  return `
    <div class="settings-grid">
      <fieldset>
        <legend>Capital Policy</legend>
        <label>Preset <select id="cfg-capitalPreset" onchange="applyCapitalPreset(this.value)">${presetOptions}</select></label>
        <label>Reserve % <input type="number" id="cfg-capitalReserve" value="${((t.reserve ?? 0.5) * 100).toFixed(0)}" min="0" max="100" step="1" /></label>
        <label>Core % <input type="number" id="cfg-capitalCore" value="${((t.core ?? 0.2) * 100).toFixed(0)}" min="0" max="100" step="1" /></label>
        <label>Opportunity % <input type="number" id="cfg-capitalOpportunity" value="${((t.opportunity ?? 0.3) * 100).toFixed(0)}" min="0" max="100" step="1" /></label>
        <label>Core Batch % <input type="number" id="cfg-capitalCoreBatch" value="${((policy.coreBatchFraction ?? 0.05) * 100).toFixed(0)}" min="0" max="25" step="1" /></label>
        <label>Opp Batch % <input type="number" id="cfg-capitalOppBatch" value="${((policy.opportunityBatchFraction ?? 0.03) * 100).toFixed(0)}" min="0" max="25" step="1" /></label>
        <label>Core Min Alloc % <input type="number" id="cfg-capitalCoreMin" value="${policy.coreMinAllocationPct ?? 10}" min="0" max="100" step="1" /></label>
        <label>Core Allowlist <textarea id="cfg-capitalAllowlist" rows="2">${allowlist}</textarea></label>
      </fieldset>
    </div>`;
}

function applyCapitalPreset(presetId) {
  const preset = CAPITAL_PRESETS[presetId];
  if (!preset) return;
  const t = preset.targets || {};
  const setVal = (id, val) => { const el = document.getElementById(id); if (el) el.value = val; };
  setVal('cfg-capitalReserve', ((t.reserve || 0) * 100).toFixed(0));
  setVal('cfg-capitalCore', ((t.core || 0) * 100).toFixed(0));
  setVal('cfg-capitalOpportunity', ((t.opportunity || 0) * 100).toFixed(0));
  setVal('cfg-capitalCoreBatch', (preset.coreBatchFraction * 100).toFixed(0));
  setVal('cfg-capitalOppBatch', (preset.opportunityBatchFraction * 100).toFixed(0));
  setVal('cfg-capitalCoreMin', preset.coreMinAllocationPct.toFixed(0));
  setVal('cfg-capitalAllowlist', (preset.coreAllowlist || []).join(', '));
}

window.applyCapitalPreset = applyCapitalPreset;

function renderDashboardTab() {
  const summary = allState.summary;
  const c = summary.counts || {};
  const opps = allState.opportunities || [];
  const execs = allState.executions || [];
  const accounts = allState.accounts || [];
  const nav = accounts.reduce((s, a) => s + Number(a.nav || 0), 0);
  const cash = accounts.reduce((s, a) => s + Number(a.cash || 0), 0);
  const capitalPolicyHtml = renderCapitalPolicySummary();
  return `
    <div class="tab-panel">
      <div class="panel-header"><span class="eyebrow">Dashboard</span><span class="label">Overview — ${new Date().toLocaleString()}</span></div>
      <div class="stat-cards">
        ${[['Total NAV', money(nav)], ['Total Cash', money(cash)], ['Open Trades', c.executions_pending || 0], ['Opportunities', c.opportunities || 0]].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}
      </div>
      <h3>Capital Policy</h3>
      ${capitalPolicyHtml}
      <div class="grid-2col">
        <div>
          <h3>Recent Opportunities</h3>
          <div class="mini-feed">${opps.slice(-5).reverse().map(o => `<div class="mini-item"><span class="badge ${o.approvalStatus === 'approved' ? 'badge-ok' : o.approvalStatus === 'rejected' ? 'badge-err' : 'badge-warn'}">${o.approvalStatus || 'pending'}</span><span>${o.title || o.symbol}</span><span class="label">${money(o.totalMoneyRisked)}</span></div>`).join('') || '<div class="label">No opportunities yet</div>'}</div>
        </div>
        <div>
          <h3>Recent Executions</h3>
          <div class="mini-feed">${execs.slice(-5).reverse().map(e => `<div class="mini-item"><span class="badge ${e.status === 'filled' ? 'badge-ok' : e.status === 'failed' ? 'badge-err' : 'badge-warn'}">${e.status}</span><span>${e.strategyId || e.id}</span><span class="label">${(e.confidenceScore * 100).toFixed(0)}%</span></div>`).join('') || '<div class="label">No executions yet</div>'}</div>
        </div>
      </div>
      <div class="button-row"><button class="btn" data-nav="opportunities">View All Opportunities</button><button class="btn" data-nav="execution">Open Execution Console</button></div>
    </div>
  `;
}

async function loadPortfolioTab() {
  const [accts, pos] = await Promise.all([api('/api/accounts'), api('/api/positions')]).catch(() => [{ accounts: [] }, { positions: [] }]);
  allState.accounts = accts.accounts || [];
  const accounts = allState.accounts;
  const positions = pos.positions || [];
  const nav = accounts.reduce((s, a) => s + Number(a.nav || 0), 0);
  const cash = accounts.reduce((s, a) => s + Number(a.cash || 0), 0);
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Portfolio</span><span class="label">${accounts.length} accounts · ${positions.length} positions</span></div>`;
  html += `<div class="stat-cards">${[['Total NAV', money(nav)], ['Total Cash', money(cash)], ['Positions', positions.length]].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}</div>`;
  html += `<h3>Accounts</h3><div class="card-grid">${accounts.map(a => `<div class="data-card"><h4>${a.name}</h4><p><span class="badge">${a.status}</span> ${a.provider || ''}</p><p><strong>${money(a.nav)}</strong> NAV · ${money(a.cash)} cash</p><p class="label">${a.currency || 'USD'}</p></div>`).join('') || '<p class="label">No accounts</p>'}</div>`;
  html += `<h3>Positions</h3><div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Qty</th><th>Avg Price</th><th>Mark</th><th>Status</th></tr></thead><tbody>${positions.map(p => `<tr><td><strong>${p.symbol}</strong></td><td>${p.quantity}</td><td>${money(p.averagePrice || p.average_price)}</td><td>${p.markPrice || p.mark_price ? money(p.markPrice || p.mark_price) : '—'}</td><td><span class="badge">${p.status}</span></td></tr>`).join('') || '<tr><td colspan="5" class="label">No positions</td></tr>'}</tbody></table></div>`;
  html += '</div>';
  return html;
}

async function loadMarketsTab() {
  const data = await api('/api/market-data/snapshots').catch(() => ({ snapshots: [] }));
  const snapshots = data.snapshots || [];
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Markets</span><span class="label">${snapshots.length} symbols</span></div>`;
  html += `<div class="ticker-strip">${snapshots.map(m => `<div class="ticker"><span class="ticker-sym">${m.symbol}</span><span class="ticker-price">${money(m.ask)}</span><span class="ticker-spread label">${m.spreadBps} bps · LQ ${m.liquidityScore}</span></div>`).join('') || '<p class="label">No market data</p>'}</div>`;
  html += `<div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Venue</th><th>Bid/Ask</th><th>Spread</th><th>Liquidity</th><th>Status</th></tr></thead><tbody>${snapshots.map(m => `<tr><td><strong>${m.symbol}</strong></td><td>${m.venue}</td><td>${money(m.bid)} / ${money(m.ask)}</td><td>${m.spreadBps} bps</td><td>${m.liquidityScore}/100</td><td><span class="badge">${m.status}</span></td></tr>`).join('') || '<tr><td colspan="6" class="label">No data</td></tr>'}</tbody></table></div>`;
  html += '</div>';
  return html;
}

async function loadSignalsTab() {
  const data = await api('/api/execution/graph-signals').catch(() => ({ signals: [] }));
  const signals = data.signals || [];
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Graph-Alpha-Bot Signals</span><span class="label">${signals.length} signals</span><button id="ingest-signals" class="btn">Ingest as Opportunities</button></div>`;
  html += `<div class="table-wrap"><table><thead><tr><th>Symbol</th><th>Score</th><th>Source</th><th>Strategy</th><th>Direction</th><th>Conviction</th><th>Timestamp</th></tr></thead><tbody>${signals.map(s => `<tr><td><strong>${s.symbol}</strong></td><td>${(s.score * 100).toFixed(0)}%</td><td>${s.source}</td><td>${s.strategyName}</td><td>${s.direction}</td><td><span class="badge ${s.conviction === 'high' ? 'badge-ok' : 'badge-warn'}">${s.conviction}</span></td><td class="label">${s.ts ? new Date(s.ts).toLocaleString() : '—'}</td></tr>`).join('') || '<tr><td colspan="7" class="label">No signals yet</td></tr>'}</tbody></table></div>`;
  html += '</div>';
  return { html, signals };
}

function renderOpportunityCard(opp) {
  const sizing = opp.positionSizing || {};
  return `<div class="data-card opportunity-card" data-opp-id="${opp.id}"><div class="card-top"><span class="badge ${opp.approvalStatus === 'approved' ? 'badge-ok' : opp.approvalStatus === 'rejected' ? 'badge-err' : 'badge-warn'}">${opp.approvalStatus || 'pending'}</span><span class="label">${opp.venue} · ${opp.marketType}</span></div><h4>${opp.title || opp.market || opp.symbol}</h4><div class="opp-metrics"><span><b>${pct(opp.winProbability)}</b><small>win</small></span><span><b>${pct(opp.lossProbability)}</b><small>loss</small></span><span><b>${money(opp.totalMoneyRisked)}</b><small>risk</small></span><span><b>${money(opp.netExpectedValue)}</b><small>EV</small></span><span><b>${money(opp.potentialUpside)}</b><small>upside</small></span><span><b>${pct(sizing.kellyCapped)}</b><small>kelly</small></span></div><details class="risk-breakdown"><summary>Risk Breakdown</summary><div class="risk-grid"><div><span class="label">Recommendation</span><strong>${opp.recommendation || opp.recommend || '—'}</strong></div><div><span class="label">Capital Required</span><strong>${money(opp.capitalRequired || opp.totalMoneyRisked)}</strong></div><div><span class="label">Max Loss</span><strong>${money(opp.maxLoss || 0)}</strong></div><div><span class="label">Reward / Risk</span><strong>${Number(opp.rewardRiskRatio || sizing.rewardRiskRatio || 0).toFixed(2)}x</strong></div><div><span class="label">Liquidity</span><strong>${pct(opp.liquidityScore || 0)}</strong></div><div><span class="label">Freshness</span><strong>${pct(opp.dataFreshnessScore || 0)}</strong></div><div><span class="label">Backtest</span><strong>${opp.backtestStatus || opp.backtestId || '—'}</strong></div><div><span class="label">Reason</span><strong>${opp.reason || '—'}</strong></div></div></details><div class="opp-actions">${opp.approvalStatus !== 'approved' && opp.approvalStatus !== 'rejected' ? `<button class="btn btn-sm btn-ok" data-opp-decision="approve" data-opp-id="${opp.id}">Approve</button><button class="btn btn-sm btn-err" data-opp-decision="reject" data-opp-id="${opp.id}">Reject</button>` : ''}<button class="btn btn-sm" data-opp-decision="request-research" data-opp-id="${opp.id}">Research</button></div></div>`;
}

async function loadOpportunitiesTab() {
  const data = await api('/api/opportunity-dashboard').catch(() => ({ opportunities: [], riskBreakdowns: [], researchJobs: [], agentCostLedger: [] }));
  allState.opportunities = data.opportunities || [];
  allState.riskBreakdowns = data.riskBreakdowns || [];
  allState.researchJobs = data.researchJobs || [];
  allState.agentCostSummary = data.agentCostSummary || {};
  const opps = allState.opportunities;
  const venues = [...new Set(opps.map(o => o.venue).filter(Boolean))].sort();
  const visible = opps.filter(o => {
    const status = (o.approvalStatus || o.status || 'pending').toLowerCase();
    const venue = (o.venue || '').toLowerCase();
    const query = `${o.title || ''} ${o.market || ''} ${o.symbol || ''} ${o.reason || ''}`.toLowerCase();
    const q = (opportunityViewState.query || '').trim().toLowerCase();
    const statusOk = opportunityViewState.status === 'all' || status === opportunityViewState.status;
    const venueOk = opportunityViewState.venue === 'all' || venue.includes(opportunityViewState.venue);
    const queryOk = !q || query.includes(q);
    return statusOk && venueOk && queryOk;
  });
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Opportunities</span><span class="label">${visible.length}/${opps.length} candidates</span><button id="gen-from-connectors" class="btn">Generate from Connectors</button></div>`;
  html += `<div class="settings-grid" style="padding:0.75rem"><fieldset><legend>Feed Filters</legend><label>Status<select id="opp-filter-status"><option value="all" ${opportunityViewState.status === 'all' ? 'selected' : ''}>All</option><option value="pending" ${opportunityViewState.status === 'pending' ? 'selected' : ''}>Pending</option><option value="approved" ${opportunityViewState.status === 'approved' ? 'selected' : ''}>Approved</option><option value="rejected" ${opportunityViewState.status === 'rejected' ? 'selected' : ''}>Rejected</option><option value="needs_review" ${opportunityViewState.status === 'needs_review' ? 'selected' : ''}>Needs Review</option></select></label><label>Venue<select id="opp-filter-venue"><option value="all" ${opportunityViewState.venue === 'all' ? 'selected' : ''}>All</option>${venues.map(v => `<option value="${v}" ${opportunityViewState.venue === v ? 'selected' : ''}>${v}</option>`).join('')}</select></label><label>Search<input id="opp-filter-query" value="${opportunityViewState.query || ''}" placeholder="symbol, venue, reason" /></label></fieldset><fieldset><legend>Summary</legend><div class="stat-cards">${[['Candidates', opps.length], ['Research jobs', (allState.researchJobs || []).length], ['Cost events', (allState.agentCostSummary || {}).totalCostEvents || 0]].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}</div><div class="label" style="margin-top:0.5rem">Recent research and cost data appear here when opportunities request more evidence.</div></fieldset></div>`;
  html += `<div class="card-grid">${visible.map(renderOpportunityCard).join('') || '<p class="label">No opportunities match the filters. Generate from connectors or ingest signals.</p>'}</div>`;
  html += '</div>';
  return html;
}

async function loadExecutionTab() {
  const data = await api('/api/executions').catch(() => ({ executions: [] }));
  allState.executions = data.executions || [];
  const execs = allState.executions;
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Execution Engine</span><span class="label">${execs.length} executions</span><button id="refresh-exec" class="btn">Refresh</button></div>`;
  html += `<div class="stat-cards">${[['Total', execs.length], ['Filled', execs.filter(e => e.status === 'filled').length], ['Pending', execs.filter(e => e.status === 'draft' || e.status === 'submitted').length], ['Failed', execs.filter(e => e.status === 'failed').length]].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}</div>`;
  html += `<div class="table-wrap"><table><thead><tr><th>ID</th><th>Strategy</th><th>Mode</th><th>Status</th><th>Confidence</th><th>Orders</th><th>Fills</th><th>Settlement</th><th>Started</th><th>Actions</th></tr></thead><tbody>${execs.map(e => `<tr><td><code>${e.id ? e.id.slice(0, 12) + '…' : '—'}</code></td><td>${e.strategyId || '—'}</td><td><span class="badge">${e.mode}</span></td><td><span class="badge ${e.status === 'filled' ? 'badge-ok' : e.status === 'failed' ? 'badge-err' : e.status === 'draft' ? 'badge-warn' : ''}">${e.status}</span></td><td>${e.confidenceScore ? (e.confidenceScore * 100).toFixed(1) + '%' : '—'}</td><td>${(e.orders || []).length}</td><td>${(e.fills || []).length}</td><td><span class="label">${(e.fills || []).filter(f => f.settlementStatus === 'settled').length}/${(e.fills || []).length} settled</span></td><td class="label">${e.startedAt ? new Date(e.startedAt).toLocaleString() : '—'}</td><td class="action-cell">${e.status === 'draft' ? `<button class="btn btn-sm btn-ok" data-exec-action="approve" data-exec-id="${e.id}">Approve</button>` : ''}${e.status === 'draft' || e.status === 'submitted' ? `<button class="btn btn-sm btn-err" data-exec-action="cancel" data-exec-id="${e.id}">Cancel</button>` : ''}${(e.fills || []).length ? `<button class="btn btn-sm" data-exec-action="reconcile" data-exec-id="${e.id}">Reconcile</button>` : ''}</td></tr>`).join('') || '<tr><td colspan="10" class="label">No executions yet</td></tr>'}</tbody></table></div>`;
  html += '</div>';
  return html;
}

async function loadKalshiTab() {
  const [markets, balance] = await Promise.all([api('/api/kalshi/markets'), api('/api/kalshi/balance')]).catch(() => [{ markets: [] }, { balance: {} }]);
  const mktList = markets.markets || [];
  const bal = balance.balance || {};
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Kalshi</span><span class="label">${mktList.length} markets</span></div>`;
  html += `<div class="stat-cards">${[['Balance', money(bal.balance || bal.available)], ['Portfolio', money(bal.portfolio || 0)], ['Available', money(bal.available || 0)]].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}</div>`;
  html += `<div class="table-wrap"><table><thead><tr><th>Market</th><th>Liquidity</th><th>Yes Bid/Ask</th><th>No Bid/Ask</th><th>Volume</th></tr></thead><tbody>${mktList.map(m => `<tr><td><strong>${m.title || m.id}</strong>${!m.has_liquidity ? ' <span class="label">(no bids)</span>' : ''}</td><td><span class="badge ${m.has_liquidity ? 'badge-green' : ''}">${m.has_liquidity ? 'active' : 'no liq'}</span></td><td>${m.yes_bid ? '$' + m.yes_bid.toFixed(2) : '—'} / ${m.yes_ask ? '$' + m.yes_ask.toFixed(2) : '—'}</td><td>${m.no_bid ? '$' + m.no_bid.toFixed(2) : '—'} / ${m.no_ask ? '$' + m.no_ask.toFixed(2) : '—'}</td><td>${(m.volume || 0).toLocaleString()}</td></tr>`).join('') || '<tr><td colspan="5" class="label">No markets available</td></tr>'}</tbody></table></div>`;
  const withLiq = mktList.filter(m => m.has_liquidity).length;
  if (!mktList.length) html += '<p class="label">No Kalshi markets loaded from API.</p>';
  else html += `<p class="label">${mktList.length} markets loaded, ${withLiq} with active bids.</p>`;
  html += '</div>';
  return html;
}

async function loadPolymarketTab() {
  const [markets, balance] = await Promise.all([api('/api/polymarket/markets'), api('/api/polymarket/balance')]).catch(() => [{ markets: [] }, { balance: {} }]);
  const mktList = markets.markets || [];
  const bal = balance.balance || {};
  if (!polymarketSelection && mktList.length) polymarketSelection = mktList[0].id || mktList[0].marketId || mktList[0].conditionId;
  const selected = mktList.find(m => (m.id || m.marketId || m.conditionId) === polymarketSelection) || mktList[0] || null;
  const orderbook = selected ? await api('/api/polymarket/orderbook/' + encodeURIComponent(selected.id || selected.marketId || selected.conditionId)).catch(() => ({ orderbook: {} })) : { orderbook: {} };
  const ob = orderbook.orderbook || {};
  const bids = (ob.bids || ob.buy || []).slice(0, 5);
  const asks = (ob.asks || ob.sell || []).slice(0, 5);
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Polymarket</span><span class="label">${mktList.length} markets</span></div>`;
  html += `<div class="stat-cards">${[['USDC', money(bal.usdc || 0)], ['MATIC', `${bal.polygonMatic || 0} POL`], ['Pending', money(bal.pending || 0)]].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}</div>`;
  html += `<div class="settings-grid" style="padding:0.75rem"><fieldset><legend>Markets</legend><label>Selected Market<select id="poly-market-select">${mktList.map(m => {
    const id = m.id || m.marketId || m.conditionId || '';
    return `<option value="${id}" ${selected && id === (selected.id || selected.marketId || selected.conditionId) ? 'selected' : ''}>${m.question || m.title || id}</option>`;
  }).join('')}</select></label><div class="label">Choose a market to inspect its book and spread.</div></fieldset><fieldset><legend>Selected Book</legend>${selected ? `<div class="mini-feed"><div class="mini-item"><span class="badge badge-ok">${selected.question || selected.title || selected.id}</span><span>${selected.outcomePrices ? (Number(selected.outcomePrices[0]) * 100).toFixed(1) + '¢ yes / ' + (Number(selected.outcomePrices[1]) * 100).toFixed(1) + '¢ no' : '—'}</span><span class="label">Volume ${Number(selected.volume || 0).toLocaleString()} · Liquidity ${selected.liquidity ? money(selected.liquidity) : '—'}</span></div></div>` : '<div class="label">No market selected</div>'}<div class="table-wrap" style="margin-top:0.5rem"><table><thead><tr><th>Bid</th><th>Ask</th><th>Size</th></tr></thead><tbody>${(bids.length || asks.length) ? `${bids.map(b => `<tr><td>${Number(b.price ?? b[0] ?? 0).toFixed(3)}</td><td>bid</td><td>${Number(b.size ?? b[1] ?? 0).toFixed(2)}</td></tr>`).join('')}${asks.map(a => `<tr><td>${Number(a.price ?? a[0] ?? 0).toFixed(3)}</td><td>ask</td><td>${Number(a.size ?? a[1] ?? 0).toFixed(2)}</td></tr>`).join('')}` : '<tr><td colspan="3" class="label">No orderbook data</td></tr>'}</tbody></table></div></fieldset></div>`;
  html += `<div class="table-wrap"><table><thead><tr><th>Question</th><th>Yes Price</th><th>No Price</th><th>Volume</th><th>Liquidity</th><th>End Date</th></tr></thead><tbody>${mktList.map(m => `<tr><td><strong>${m.question || m.id}</strong></td><td>${m.outcomePrices ? (Number(m.outcomePrices[0]) * 100).toFixed(1) + '¢' : '—'}</td><td>${m.outcomePrices ? (Number(m.outcomePrices[1]) * 100).toFixed(1) + '¢' : '—'}</td><td>${(m.volume || 0).toLocaleString()}</td><td>${m.liquidity ? money(m.liquidity) : '—'}</td><td class="label">${m.endDate ? new Date(m.endDate).toLocaleDateString() : '—'}</td></tr>`).join('') || '<tr><td colspan="6" class="label">No markets available</td></tr>'}</tbody></table></div>`;
  if (!mktList.length) html += '<p class="label">No real Polymarket markets loaded. Check API connectivity.</p>';
  html += '</div>';
  return html;
}

async function loadAuditTab() {
  const [auditData, verifyData] = await Promise.all([
    api('/api/audit').catch(() => ({ audit: [] })),
    api('/api/audit/verify').catch(() => ({ ok: false, reason: 'unavailable' }))
  ]);
  const audit = auditData.audit || [];
  allState.audit = audit;
  const q = (auditViewState.query || '').trim().toLowerCase();
  const visible = audit.filter(e => {
    const text = `${e.action || ''} ${e.actor || ''} ${e.details || ''}`.toLowerCase();
    const okQ = !q || text.includes(q);
    const okLvl = auditViewState.level === 'all' || (e.action || '').toLowerCase().includes(auditViewState.level);
    return okQ && okLvl;
  });
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Audit</span><span class="label">${visible.length}/${audit.length} events</span></div>`;
  html += `<div class="stat-cards">${[['Integrity', verifyData.ok ? 'OK' : 'FAIL'], ['Reason', verifyData.reason || '—'], ['Events', audit.length]].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}</div>`;
  html += `<div class="settings-grid" style="padding:0.75rem"><fieldset><legend>Filters</legend><label>Level<select id="audit-filter-level"><option value="all" ${auditViewState.level === 'all' ? 'selected' : ''}>All</option><option value="execution" ${auditViewState.level === 'execution' ? 'selected' : ''}>Execution</option><option value="opportunity" ${auditViewState.level === 'opportunity' ? 'selected' : ''}>Opportunity</option><option value="research" ${auditViewState.level === 'research' ? 'selected' : ''}>Research</option><option value="config" ${auditViewState.level === 'config' ? 'selected' : ''}>Config</option><option value="kill" ${auditViewState.level === 'kill' ? 'selected' : ''}>Kill switch</option></select></label><label>Search<input id="audit-filter-query" value="${auditViewState.query || ''}" placeholder="actor, action, details" /></label></fieldset><fieldset><legend>Integrity</legend><div class="mini-feed"><div class="mini-item"><span class="badge ${verifyData.ok ? 'badge-ok' : 'badge-err'}">${verifyData.ok ? 'Verified' : 'Unverified'}</span><span>${verifyData.ok ? 'Audit chain is intact' : 'Audit integrity check failed'}</span><span class="label">${verifyData.requestId ? 'Request ' + verifyData.requestId : ''}</span></div></div></fieldset></div>`;
  html += `<div class="table-wrap"><table><thead><tr><th>Time</th><th>Action</th><th>Actor</th><th>Details</th></tr></thead><tbody>${visible.slice().reverse().map(e => `<tr><td class="label">${e.at ? new Date(e.at).toLocaleString() : (e.timestamp ? new Date(e.timestamp).toLocaleString() : '—')}</td><td><span class="badge">${e.action || e.type || 'audit'}</span></td><td>${e.actor || '—'}</td><td>${e.details || '—'}</td></tr>`).join('') || '<tr><td colspan="4" class="label">No audit events</td></tr>'}</tbody></table></div>`;
  html += '</div>';
  return html;
}

async function loadSweeperTab() {
  const [sweepResult, historyData] = await Promise.all([
    api('/api/paper/sweep', { method: 'POST', body: { maxMarkets: 150 } }).catch(() => ({ signals: [] })),
    api('/api/paper/sweep/history').catch(() => ({ history: [], pnl: [] }))
  ]);
  const signals = sweepResult.signals || [];
  const history = historyData.history || [];
  const pnl = historyData.pnl || [];
  const lastSweep = history[history.length - 1];

  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Paper Sweeper</span><span class="label">${signals.length} signals · ${history.length} sweeps</span><button id="run-sweep" class="btn">Run Sweep</button></div>`;

  // Stats
  const strategies = [...new Set(signals.map(s => s.strategy))];
  html += `<div class="stat-cards">${[
    ['Signals', signals.length],
    ['Strategies', strategies.length],
    ['Scanned', sweepResult.scanned || 0],
    ['Sweeps', history.length]
  ].map(([l, v]) => `<div class="stat-card"><span class="label">${l}</span><strong>${v}</strong></div>`).join('')}</div>`;

  if (lastSweep) {
    html += `<p class="sweep-info">Last sweep: ${new Date(lastSweep.timestamp).toLocaleString()} — ${lastSweep.signalCount} raw signals across ${Object.keys(lastSweep.strategies || {}).length} strategies</p>`;
  }

  // P&L by strategy
  if (pnl.length) {
    html += `<h3>P&L by Strategy</h3><div class="table-wrap"><table><thead><tr><th>Strategy</th><th>Total Signals</th><th>Avg Confidence</th><th>Wins/Losses</th></tr></thead><tbody>${pnl.map(p => `<tr><td><strong>${p.strategy}</strong></td><td>${p.totalSignals}</td><td>${p.avgConfidence ? (p.avgConfidence * 100).toFixed(1) + '%' : '—'}</td><td><span class="badge badge-ok">${p.wins}</span> / <span class="badge badge-err">${p.losses}</span></td></tr>`).join('')}</tbody></table></div>`;
  }

  // Signals grouped by strategy
  const byStrat = {};
  for (const s of signals) {
    if (!byStrat[s.strategy]) byStrat[s.strategy] = [];
    byStrat[s.strategy].push(s);
  }

  html += `<h3>Signals by Strategy</h3><div class="sweep-strategy-list">`;
  for (const [stratName, sigs] of Object.entries(byStrat).sort((a, b) => b[1].length - a[1].length)) {
    const displayName = sigs[0].strategyName || stratName;
    html += `<details class="sweep-group" open><summary><span class="sweep-group-header"><span class="eyebrow">${displayName}</span><span class="label">${sigs.length} signals</span><span class="sweep-group-confidence">avg ${(sigs.reduce((s, x) => s + x.confidence, 0) / sigs.length * 100).toFixed(1)}%</span></span></summary><div class="sweep-signal-list">`;
    for (const s of sigs) {
      const actionClass = s.action === 'buy' ? 'signal-buy' : s.action === 'sell' ? 'signal-sell' : '';
      html += `<div class="sweep-signal ${actionClass}">
        <span class="signal-product"><strong>${s.productId}</strong></span>
        <span class="signal-action badge ${s.action === 'buy' ? 'badge-ok' : 'badge-err'}">${s.action}</span>
        <span class="signal-confidence">${(s.confidence * 100).toFixed(0)}%</span>
        <span class="signal-qty label">${Number(s.quantity || 0).toFixed(4)}</span>
        <span class="signal-price">${Number(s.price || 0).toFixed(4)}</span>
        <span class="signal-reason label">${s.reason || ''}</span>
        ${s.regime ? `<span class="signal-regime label">${s.regime}</span>` : ''}
      </div>`;
    }
    html += `</div></details>`;
  }
  html += `</div></div>`;
  return html;
}

async function loadAdaptersTab() {
  const data = await api('/api/execution/adapters').catch(() => ({ adapters: [] }));
  const adapters = data.adapters || [];
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Broker Adapters</span><span class="label">${adapters.length} registered</span><button id="refresh-adapters" class="btn">Refresh</button></div>`;
  html += `<div class="table-wrap"><table><thead><tr><th>Adapter</th><th>Venue</th><th>Mode</th><th>Status</th></tr></thead><tbody>${adapters.map(a => `<tr><td>${a.name}</td><td><span class="badge">${a.venue}</span></td><td><span class="badge">${a.mode}</span></td><td><span class="badge ${a.connected ? 'badge-ok' : 'badge-err'}">${a.connected ? 'Connected' : 'Disconnected'}</span></td></tr>`).join('') || '<tr><td colspan="4" class="label">No adapters registered</td></tr>'}</tbody></table></div>`;
  const p1 = await api('/api/paper-executions').catch(() => ({ executions: [] }));
  const papers = p1.executions || [];
  html += `<h3>Paper Executions</h3><div class="card-grid">${papers.map(p => `<div class="data-card"><h4>${p.id}</h4><p><span class="badge">${p.status}</span> ${p.mode || 'paper'}</p><p class="label">Strategy: ${p.strategyId}</p></div>`).join('') || '<p class="label">No paper executions</p>'}</div>`;
  html += '</div>';
  return html;
}

async function loadSettingsTab() {
  const data = await api('/api/config').catch(() => ({ config: {} }));
  const cfg = data.config || {};
  allState.config = cfg;
  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Settings</span><span class="label">Trading Parameters, Capital Policy & API Keys</span><button id="save-settings" class="btn">Save</button></div>`;
  html += `<div class="settings-grid"><fieldset><legend>Trading Parameters</legend>`;
  html += `<label>Confidence Threshold <input type="number" id="cfg-confidenceThreshold" value="${cfg.confidenceThreshold || 0.60}" step="0.05" min="0" max="1" /></label>`;
  html += `<label>Approval Threshold <input type="number" id="cfg-approvalThreshold" value="${cfg.approvalThreshold || 0.80}" step="0.05" min="0" max="1" /></label>`;
  html += `<label>Max Position ($) <input type="number" id="cfg-maxPositionSizeUsd" value="${cfg.maxPositionSizeUsd || 50000}" step="1000" min="0" /></label>`;
  html += `<label>Holding Period (days) <input type="number" id="cfg-defaultHoldingPeriodDays" value="${cfg.defaultHoldingPeriodDays || 7}" min="1" /></label>`;
  html += `<label>Max Concurrent Trades <input type="number" id="cfg-maxConcurrentTrades" value="${cfg.maxConcurrentTrades || 5}" min="1" /></label>`;
  html += `</fieldset><fieldset><legend>API Keys</legend>`;
  html += `<label>Coinbase Key <input type="password" id="cfg-coinbaseApiKey" value="${cfg.coinbaseApiKey || ''}" /></label>`;
  html += `<label>Coinbase Secret <input type="password" id="cfg-coinbaseApiSecret" value="${cfg.coinbaseApiSecret || ''}" /></label>`;
  html += `<label>Kalshi Email <input type="text" id="cfg-kalshiEmail" value="${cfg.kalshiEmail || ''}" /></label>`;
  html += `<label>Kalshi Password <input type="password" id="cfg-kalshiPassword" value="${cfg.kalshiPassword || ''}" /></label>`;
  html += `<label>Polymarket Key <input type="password" id="cfg-polymarketApiKey" value="${cfg.polymarketApiKey || ''}" /></label>`;
  html += `<label>Polymarket Wallet <input type="text" id="cfg-polymarketWalletAddress" value="${cfg.polymarketWalletAddress || ''}" /></label>`;
  html += `</fieldset></div></div>`;
  html += buildCapitalPolicyForm(cfg.capitalPolicy || cfg.capital_policy || CAPITAL_PRESETS.balanced);
  return html;
}

const SECRET_FRESHNESS = {
  fresh: { label: 'Fresh', cls: 'badge-ok' },
  due_soon: { label: 'Due Soon', cls: 'badge-warn' },
  expired: { label: 'Expired', cls: 'badge-err' },
  unknown: { label: 'Unknown', cls: 'badge-warn' },
};

function secretsAutoRotateSummary(view) {
  const state = view.autoRotateEnabled
    ? `Auto-rotate ON · rotation age ${view.rotationDays}d`
    : 'Auto-rotate OFF';
  return `<span class="label">${state}</span>`;
}

async function loadSecretsTab() {
  const data = await api('/api/secrets').catch(() => ({ secrets: { providers: [], secrets: [], autoRotateEnabled: false, rotationDays: 30 } }));
  const view = data.secrets || {};
  const providers = view.providers || [];
  const secrets = view.secrets || [];

  let html = `<div class="tab-panel"><div class="panel-header"><span class="eyebrow">Secrets & Credential Rotation</span>${secretsAutoRotateSummary(view)}<button id="refresh-secrets" class="btn">Refresh</button></div>`;

  // Auto-rotate control panel
  html += `<div class="settings-grid" style="padding:0.75rem"><fieldset><legend>Auto-Rotation</legend>`;
  html += `<label style="display:flex;align-items:center;gap:0.5rem"><input type="checkbox" id="sec-auto" ${view.autoRotateEnabled ? 'checked' : ''} /> Enable scheduled auto-rotation</label>`;
  html += `<label>Rotation age (days) <input type="number" id="sec-rotation-days" value="${view.rotationDays || 30}" min="1" max="365" /></label>`;
  html += `<label>Run every (days) <input type="number" id="sec-interval-days" value="${view.autoRotateIntervalMs ? Math.round(view.autoRotateIntervalMs / 86400000) : 30}" min="1" max="365" /></label>`;
  html += `<div class="opp-actions"><button id="sec-auto-save" class="btn btn-sm">Save Auto-Rotate</button><button id="sec-auto-run" class="btn btn-sm">Run Due Rotations Now</button></div>`;
  html += `</fieldset><fieldset><legend>Providers</legend><div class="stat-cards">`;
  for (const p of providers) {
    const fs = SECRET_FRESHNESS[p.freshnessState] || SECRET_FRESHNESS.unknown;
    html += `<div class="stat-card"><span class="label">${p.label}</span><span class="badge ${fs.cls}">${fs.label}</span>${p.rotatable ? `<button class="btn btn-sm sec-rotate" data-provider="${p.id}" style="margin-top:0.4rem">Rotate</button>` : ''}</div>`;
  }
  html += `</div></fieldset></div>`;

  // Manual update form
  html += `<div class="settings-grid" style="padding:0.75rem"><fieldset><legend>Update Credentials</legend>`;
  for (const s of secrets) {
    const fs = SECRET_FRESHNESS[s.freshness.state] || SECRET_FRESHNESS.unknown;
    const age = s.freshness.daysOld != null ? `${s.freshness.daysOld}d old` : 'never set';
    html += `<div class="secret-field"><label>${s.label} ${s.required ? '<span class="label">required</span>' : ''}`;
    html += `<input type="${s.kind === 'opaque' ? 'password' : 'text'}" id="sec-${s.key}" placeholder="${s.set ? s.masked + ' (' + age + ')' : 'not set'}" /></label>`;
    html += `<span class="badge ${fs.cls}">${fs.label}</span></div>`;
  }
  html += `<div class="opp-actions"><button id="sec-save" class="btn">Save Credentials</button></div>`;
  html += `</fieldset></div></div>`;
  return html;
}

async function renderCurrentTab() {
  const target = $('#tab-content');
  target.innerHTML = '<div class="tab-loading">Loading…</div>';
  let html = '';
  switch (activeTab) {
    case 'dashboard': html = renderDashboardTab(); break;
    case 'portfolio': html = await loadPortfolioTab(); break;
    case 'markets': html = await loadMarketsTab(); break;
    case 'signals': html = (await loadSignalsTab()).html; break;
    case 'opportunities': html = await loadOpportunitiesTab(); break;
    case 'execution': html = await loadExecutionTab(); break;
    case 'kalshi': html = await loadKalshiTab(); break;
    case 'polymarket': html = await loadPolymarketTab(); break;
    case 'sweeper': html = await loadSweeperTab(); break;
    case 'adapters': html = await loadAdaptersTab(); break;
    case 'secrets': html = await loadSecretsTab(); break;
    case 'settings': html = await loadSettingsTab(); break;
    case 'audit': html = await loadAuditTab(); break;
    default: html = `<div class="tab-panel"><p class="label">Unknown tab: ${activeTab}</p></div>`;
  }
  
  // Integrate P1 panels (Accounts, Templates, Paper, Adapters) for trading/BOT related tabs
  if (activeTab === 'trader' || activeTab === 'bots' || activeTab === 'trader' || activeTab === 'strategies') {
    html += p1Html;
  }
  
  target.innerHTML = html;
  wireTabActions();
}

function wireTabActions() {
  $$('[data-opp-decision]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const id = btn.dataset.oppId;
      const action = btn.dataset.oppDecision;
      if (action === 'request-research') await api(`/api/opportunities/${id}/request-research`, { method: 'POST', body: { localOrRemote: 'local', model: 'local-review-model', totalTokens: 8000, runtimeSeconds: 90, systemBudgetOverride: true } });
      else await api(`/api/opportunities/${id}/${action}`, { method: 'POST', body: { reviewer: 'operator-ui', reason: `${action} from dashboard` } });
      await refresh();
    });
  });
  $$('[data-exec-action]').forEach(btn => {
    btn.addEventListener('click', async () => {
      const id = btn.dataset.execId;
      const action = btn.dataset.execAction;
      if (action === 'approve') await api(`/api/execution/${id}/approve`, { method: 'POST' });
      else if (action === 'cancel') await api(`/api/execution/${id}/cancel`, { method: 'POST' });
      else if (action === 'reconcile') await api(`/api/execution/${id}/reconcile`, { method: 'POST' });
      await refresh();
    });
  });
  $$('[data-nav]').forEach(btn => {
    btn.addEventListener('click', () => switchTab(btn.dataset.nav));
  });
  const ingBtn = $('#ingest-signals');
  if (ingBtn) ingBtn.addEventListener('click', async () => { await api('/api/execution/graph-signals/ingest', { method: 'POST' }); await refresh(); });
  const genBtn = $('#gen-from-connectors');
  if (genBtn) genBtn.addEventListener('click', async () => { await api('/api/opportunities/generate-from-connectors', { method: 'POST' }); await refresh(); });
  const oppStatus = $('#opp-filter-status');
  if (oppStatus) oppStatus.addEventListener('change', e => { opportunityViewState.status = e.target.value; renderCurrentTab(); });
  const oppVenue = $('#opp-filter-venue');
  if (oppVenue) oppVenue.addEventListener('change', e => { opportunityViewState.venue = e.target.value; renderCurrentTab(); });
  const oppQuery = $('#opp-filter-query');
  if (oppQuery) oppQuery.addEventListener('input', e => { opportunityViewState.query = e.target.value; renderCurrentTab(); });
  const polySelect = $('#poly-market-select');
  if (polySelect) polySelect.addEventListener('change', e => { polymarketSelection = e.target.value; renderCurrentTab(); });
  const auditLevel = $('#audit-filter-level');
  if (auditLevel) auditLevel.addEventListener('change', e => { auditViewState.level = e.target.value; renderCurrentTab(); });
  const auditQuery = $('#audit-filter-query');
  if (auditQuery) auditQuery.addEventListener('input', e => { auditViewState.query = e.target.value; renderCurrentTab(); });
  const refExec = $('#refresh-exec');
  if (refExec) refExec.addEventListener('click', () => refresh());
  const refAdpt = $('#refresh-adapters');
  if (refAdpt) refAdpt.addEventListener('click', () => refresh());
  const saveBtn = $('#save-settings');
  if (saveBtn) saveBtn.addEventListener('click', saveSettings);
  // Secrets tab handlers
  const refSec = $('#refresh-secrets');
  if (refSec) refSec.addEventListener('click', () => refresh());
  const secSave = $('#sec-save');
  if (secSave) secSave.addEventListener('click', saveSecrets);
  const secAutoSave = $('#sec-auto-save');
  if (secAutoSave) secAutoSave.addEventListener('click', saveSecretAutoRotate);
  const secAutoRun = $('#sec-auto-run');
  if (secAutoRun) secAutoRun.addEventListener('click', runSecretAutoRotate);
  $$('.sec-rotate').forEach(btn => {
    btn.addEventListener('click', async () => {
      await api(`/api/secrets/rotate/${btn.dataset.provider}`, { method: 'POST' });
      await refresh();
    });
  });
  const runSweep = $('#run-sweep');
  if (runSweep) runSweep.addEventListener('click', () => refresh());
}

async function saveSettings() {
  const fields = ['confidenceThreshold', 'approvalThreshold', 'maxPositionSizeUsd', 'defaultHoldingPeriodDays', 'maxConcurrentTrades',
    'coinbaseApiKey', 'coinbaseApiSecret', 'kalshiEmail', 'kalshiPassword', 'polymarketApiKey', 'polymarketWalletAddress', 'polymarketPrivateKey'];
  const body = {};
  for (const f of fields) {
    const el = document.getElementById(`cfg-${f}`);
    if (el) body[f] = el.value;
  }
  body.confidenceThreshold = Number(body.confidenceThreshold);
  body.approvalThreshold = Number(body.approvalThreshold);
  body.maxPositionSizeUsd = Number(body.maxPositionSizeUsd);
  body.defaultHoldingPeriodDays = Number(body.defaultHoldingPeriodDays);
  body.maxConcurrentTrades = Number(body.maxConcurrentTrades);
  body.capitalPolicy = {
    presetName: document.getElementById('cfg-capitalPreset')?.value || 'custom',
    targets: {
      reserve: Number(document.getElementById('cfg-capitalReserve')?.value || 0) / 100,
      core: Number(document.getElementById('cfg-capitalCore')?.value || 0) / 100,
      opportunity: Number(document.getElementById('cfg-capitalOpportunity')?.value || 0) / 100,
    },
    coreBatchFraction: Number(document.getElementById('cfg-capitalCoreBatch')?.value || 0) / 100,
    opportunityBatchFraction: Number(document.getElementById('cfg-capitalOppBatch')?.value || 0) / 100,
    coreMinAllocationPct: Number(document.getElementById('cfg-capitalCoreMin')?.value || 0),
    coreAllowlist: (document.getElementById('cfg-capitalAllowlist')?.value || '').split(',').map(v => v.trim()).filter(Boolean)
  };
  await api('/api/config', { method: 'POST', body });
  await refresh();
}

async function saveSecrets() {
  const keys = ['coinbaseApiKey', 'coinbaseApiSecret', 'kalshiEmail', 'kalshiPassword', 'polymarketApiKey', 'polymarketWalletAddress', 'polymarketPrivateKey'];
  const body = {};
  for (const k of keys) {
    const el = document.getElementById(`sec-${k}`);
    if (el && el.value) body[k] = el.value;
  }
  await api('/api/secrets', { method: 'PUT', body });
  await refresh();
}

async function saveSecretAutoRotate() {
  const enabled = document.getElementById('sec-auto')?.checked || false;
  const rotationDays = Number(document.getElementById('sec-rotation-days')?.value || 30);
  const intervalDays = Number(document.getElementById('sec-interval-days')?.value || 30);
  await api('/api/secrets/auto-rotate/config', { method: 'POST', body: { enabled, rotationDays, intervalDays } });
  await refresh();
}

async function runSecretAutoRotate() {
  await api('/api/secrets/auto-rotate/run', { method: 'POST' });
  await refresh();
}

async function refresh() {
  const summary = await fetchAll();
  renderKPI(summary);
  await pollFeed();
  await renderCurrentTab();
}

function startPolling() {
  if (pollingId) clearInterval(pollingId);
  pollingId = setInterval(async () => {
    await pollFeed();
    const summary = await api('/api/operator/summary').catch(() => ({ counts: {} }));
    allState.summary = summary;
    updateSidebarStats(summary);
  }, 5000);
}

function startQuotePolling() {
  if (quotePollingId) clearInterval(quotePollingId);
  pollQuotes();
  quotePollingId = setInterval(pollQuotes, 3000);
}

document.addEventListener('click', e => {
  if (e.target.id === 'btn-refresh') { e.preventDefault(); refresh(); }
  if (e.target.id === 'btn-kill-switch') { e.preventDefault(); api('/api/kill-switch', { method: 'POST', body: { enabled: !(allState.summary.killSwitch?.enabled), reason: 'operator_ui' } }).then(refresh); }
});

window.addEventListener('hashchange', () => {
  const tab = location.hash.replace('#', '') || 'dashboard';
  switchTab(tab);
});

async function init() {
  const startTab = location.hash.replace('#', '') || 'dashboard';
  await refresh();
  switchTab(startTab);
  startPolling();
  startQuotePolling();
}

init().catch(err => { $('#tab-content').innerHTML = `<div class="tab-panel"><p class="label">Error: ${err.message}</p></div>`; });
