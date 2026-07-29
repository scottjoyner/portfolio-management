const $ = selector => document.querySelector(selector);
const $$ = selector => [...document.querySelectorAll(selector)];

const state = {
  competition: null,
  truth: null,
  costs: { costs: [], summary: {} },
  executions: [],
  opportunities: [],
  feed: [],
  budgets: [],
  budgetApprovals: [],
  lastError: null,
};

const escapeHtml = value => String(value ?? '')
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;').replaceAll("'", '&#039;');

const money = value => Number.isFinite(Number(value))
  ? Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 })
  : '—';
const pct = value => Number.isFinite(Number(value)) ? `${Number(value).toFixed(2)}%` : '—';
const ratio = value => Number.isFinite(Number(value)) ? `${Number(value).toFixed(2)}×` : '—';
const count = value => Number.isFinite(Number(value)) ? Number(value).toLocaleString('en-US') : '—';
const dateTime = value => {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString();
};
const valueClass = value => Number(value) > 0 ? 'positive' : Number(value) < 0 ? 'negative' : '';

async function api(path, options = {}) {
  try {
    const response = await fetch(path, {
      headers: { 'content-type': 'application/json', ...(options.headers || {}) },
      ...options,
      body: options.body ? JSON.stringify(options.body) : undefined,
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(body.error || body.reason || `${response.status} ${response.statusText}`);
    return body;
  } catch (error) {
    state.lastError = `${path}: ${error.message}`;
    return null;
  }
}

function setText(id, value, className = '') {
  const element = document.getElementById(id);
  if (!element) return;
  element.textContent = value;
  if (className) element.className = className;
}

function competitor(side) {
  return state.competition?.competitors?.[side] || {};
}

function renderRaceHeader() {
  const standings = state.competition?.standings || {};
  const valid = standings.valid_for_ranking === true;
  setText('race-validity', valid ? 'Ranking valid' : 'Ranking blocked', `badge ${valid ? 'badge-ok' : 'badge-warn'}`);
  setText('race-leader', valid ? `${String(standings.leader).toUpperCase()} leads` : 'No trustworthy winner yet');
  setText('race-edge', valid ? `${money(standings.edge_usd)} net-equity edge` : 'Resolve ledger warnings before ranking');
  setText('race-cost-coverage', `Agent cost coverage ${ratio(standings.agent_cost_coverage_ratio)}`);
  setText('head-to-head-alpha', standings.agent_alpha_after_cost_pct_points == null ? '—' : `${standings.agent_alpha_after_cost_pct_points >= 0 ? '+' : ''}${pct(standings.agent_alpha_after_cost_pct_points)}`);
}

function renderCompetitor(side) {
  const row = competitor(side);
  const prefix = side;
  setText(`${prefix}-status`, row.status || 'unknown', `badge ${row.status === 'ok' ? 'badge-ok' : row.status === 'stale' ? 'badge-warn' : 'badge-err'}`);
  setText(`${prefix}-net-equity`, money(row.net_equity_usd));
  setText(`${prefix}-gross-pnl`, money(row.gross_pnl_usd), valueClass(row.gross_pnl_usd));
  setText(`${prefix}-return`, pct(row.net_return_pct), valueClass(row.net_return_pct));
  setText(`${prefix}-dd`, pct(row.max_drawdown_pct), Number(row.max_drawdown_pct) > 10 ? 'negative' : '');
  setText(`${prefix}-win`, row.win_rate == null ? '—' : pct(Number(row.win_rate) * 100));
  setText(`${prefix}-trades`, count(row.round_trips));
  if (side === 'agent') setText('agent-cost', money(row.operating_cost_usd));
  if (side === 'bot') setText('bot-fees', money(row.fees_paid_usd ?? row.fees_from_trades_usd));
}

function renderAlerts() {
  const warnings = new Set([
    ...(state.competition?.warnings || []),
    ...(state.truth?.warnings || []),
    ...(state.lastError ? [state.lastError] : []),
  ]);
  const stack = $('#alert-stack');
  if (!warnings.size) {
    stack.innerHTML = '<div class="alert">All currently observed competition contracts are satisfied.</div>';
    return;
  }
  stack.innerHTML = [...warnings].slice(0, 8).map(warning =>
    `<div class="alert ${/corrupt|invalid|failed|missing/i.test(warning) ? 'error' : ''}">${escapeHtml(warning.replaceAll('_', ' '))}</div>`
  ).join('');
}

function renderQueue() {
  const items = [];
  const standings = state.competition?.standings || {};
  if (!standings.valid_for_ranking) items.push(['Ranking blocked', 'Fix stale, unmarked, mismatched, or invalid ledgers before declaring a winner.']);
  if ((standings.agent_cost_coverage_ratio ?? Infinity) < 1) items.push(['Agent below API break-even', `${money(standings.agent_break_even_gap_usd)} more gross P&L is required to cover attributable model cost.`]);
  if ((state.costs?.summary?.pendingBudgetApprovals || 0) > 0) items.push(['Research spend awaiting approval', `${state.costs.summary.pendingBudgetApprovals} budget request(s) need an operator decision.`]);
  if (state.truth?.execution_decision?.status !== 'ok') items.push(['Execution evidence unavailable', 'The canonical execution decision is unknown; trading should remain fail-closed.']);
  const target = $('#command-queue');
  setText('queue-count', String(items.length));
  target.innerHTML = items.length ? items.map(([title, detail]) =>
    `<div class="list-card"><strong>${escapeHtml(title)}</strong><small>${escapeHtml(detail)}</small></div>`
  ).join('') : '<div class="list-card"><strong>No immediate operator action</strong><small>The observed paper competition is healthy.</small></div>';
}

function renderActivity() {
  const rows = state.feed.slice(0, 12);
  setText('activity-count', String(rows.length));
  $('#activity-feed').innerHTML = rows.length ? rows.map(row =>
    `<div class="list-card"><strong>${escapeHtml(row.action || row.type || 'event')}</strong><small>${escapeHtml(row.details || '')} · ${escapeHtml(dateTime(row.timestamp || row.at))}</small></div>`
  ).join('') : '<div class="list-card"><strong>No recent events</strong><small>The activity API returned an empty feed.</small></div>';
}

function renderRaceTable() {
  const rows = ['agent', 'bot'].map(side => competitor(side));
  $('#race-table').innerHTML = rows.map(row => `<tr>
    <td><strong>${escapeHtml(row.label || row.side || 'unknown')}</strong></td>
    <td>${money(row.gross_equity_usd)}</td>
    <td>${money(row.operating_cost_usd)}</td>
    <td class="${valueClass(row.net_pnl_usd)}">${money(row.net_equity_usd)}</td>
    <td class="${valueClass(row.net_pnl_usd)}">${money(row.net_pnl_usd)}</td>
    <td class="${valueClass(row.net_return_pct)}">${pct(row.net_return_pct)}</td>
    <td>${pct(row.max_drawdown_pct)}</td>
    <td>${row.win_rate == null ? '—' : pct(Number(row.win_rate) * 100)}</td>
  </tr>`).join('');
  const contracts = state.competition?.contracts || {};
  $('#race-contract').textContent = `Bot equity: ${contracts.bot_equity || 'unknown'} · Agent score: ${contracts.agent_score || 'unknown'} · Leader: ${contracts.leader || 'unknown'}`;
}

function renderTrades() {
  setText('trade-count', String(state.executions.length));
  $('#trade-rows').innerHTML = state.executions.slice(-100).reverse().map(row => `<tr>
    <td>${escapeHtml(dateTime(row.startedAt || row.createdAt))}</td>
    <td>${escapeHtml(row.tags?.competitor || row.strategyId || row.sourceAgentId || 'unknown')}</td>
    <td>${escapeHtml(row.symbol || '—')}</td>
    <td>${escapeHtml(row.side || row.orders?.[0]?.side || '—')}</td>
    <td><span class="badge">${escapeHtml(row.status || 'unknown')}</span></td>
    <td>${money(row.notional || row.totalMoneyRisked)}</td>
    <td>${row.confidenceScore == null ? '—' : pct(Number(row.confidenceScore) * 100)}</td>
  </tr>`).join('') || '<tr><td colspan="7" class="muted">No executions available.</td></tr>';
}

function renderSignals() {
  setText('signal-count', String(state.opportunities.length));
  $('#signal-rows').innerHTML = state.opportunities.slice(-100).reverse().map(row => `<tr>
    <td>${escapeHtml(row.sourceAgentId || row.strategyId || 'unknown')}</td>
    <td>${escapeHtml(row.symbol || row.marketSlug || row.title || '—')}</td>
    <td>${escapeHtml(row.recommendation || 'review')}</td>
    <td>${pct(Number(row.confidenceScore || 0) * 100)}</td>
    <td>${money(row.grossExpectedValue)}</td>
    <td>${money(Number(row.agentResearchCost || 0) + Number(row.modelInferenceCost || 0))}</td>
    <td class="${valueClass(row.netExpectedValue)}">${money(row.netExpectedValue)}</td>
    <td><span class="badge">${escapeHtml(row.approvalStatus || row.status || 'unknown')}</span></td>
  </tr>`).join('') || '<tr><td colspan="8" class="muted">No normalized signal opportunities available.</td></tr>';
}

function renderLearning() {
  const agent = competitor('agent');
  const bot = competitor('bot');
  const cards = [
    ['Promotion gate', state.competition?.standings?.valid_for_ranking ? 'The race is measurable. Strategy changes still require out-of-sample evidence and rollback metadata.' : 'Learning and self-modification must remain blocked until the race is measurable.'],
    ['Paid inference objective', `The agent must cover ${money(agent.operating_cost_usd)} of attributable model/compute cost and exceed the bot's ${pct(bot.net_return_pct)} net return without violating risk limits.`],
    ['Evidence gap', 'Current state exposes costs and trade outcomes, but not a durable decision → prompt → model → signal → trade → outcome → parameter-change lineage graph.'],
    ['Required next contract', 'Every autonomous change needs a version, hypothesis, train/evaluation window, challenger result, risk delta, approval state, deployment canary, and rollback pointer.'],
  ];
  $('#learning-content').innerHTML = cards.map(([title, text]) =>
    `<div class="list-card"><strong>${escapeHtml(title)}</strong><small>${escapeHtml(text)}</small></div>`
  ).join('');
}

function renderCosts() {
  const summary = state.costs?.summary || {};
  const agent = competitor('agent');
  const rows = [
    ['Attributed race cost', money(agent.operating_cost_usd)],
    ['Remote model ledger', money(summary.remoteModelCostUsd)],
    ['Local compute ledger', money(summary.localModelCostUsd)],
    ['Cost / opportunity', money(summary.costPerOpportunityUsd)],
    ['Daily budget', money(summary.dailyBudgetUsd)],
    ['Coverage ratio', ratio(state.competition?.standings?.agent_cost_coverage_ratio)],
  ];
  $('#cost-kpis').innerHTML = rows.map(([label, value]) => `<div><small>${escapeHtml(label)}</small><b>${escapeHtml(value)}</b></div>`).join('');
  setText('budget-approval-count', String(state.budgetApprovals.length));
  $('#budget-approval-rows').innerHTML = state.budgetApprovals.slice(-20).reverse().map(row =>
    `<div class="list-card"><strong>${escapeHtml(row.agentId)} · ${escapeHtml(row.status)}</strong><small>${money(row.projectedCost)} · ${count(row.projectedTokens)} tokens · ${escapeHtml(row.marketScope || 'general')}</small></div>`
  ).join('') || '<div class="list-card"><strong>No budget approvals</strong><small>Paid-agent jobs should be linked to explicit cost limits.</small></div>';
}

function renderRisk() {
  const agent = competitor('agent');
  const bot = competitor('bot');
  $('#risk-content').innerHTML = [
    ['Agent drawdown', pct(agent.max_drawdown_pct)],
    ['Bot drawdown', pct(bot.max_drawdown_pct)],
    ['Agent open positions', count(agent.open_positions)],
    ['Bot open positions', count(bot.open_positions)],
    ['Canonical execution', state.truth?.execution_decision?.value || 'unknown'],
    ['Trading mode', state.truth?.trading_mode?.value || 'unknown'],
  ].map(([label, value]) => `<div class="list-card"><strong>${escapeHtml(label)}</strong><small>${escapeHtml(value)}</small></div>`).join('');
  $('#validity-content').innerHTML = (state.competition?.warnings || []).map(warning =>
    `<div class="list-card"><strong>${escapeHtml(warning.replaceAll('_', ' '))}</strong><small>Ranking remains fail-closed until this evidence is resolved.</small></div>`
  ).join('') || '<div class="list-card"><strong>Competition valid</strong><small>Both books are fresh, comparable, and cost-adjusted.</small></div>';
}

function renderSystemTruth() {
  const truth = state.truth || {};
  setText('truth-mode', truth.trading_mode?.value || 'unknown');
  setText('truth-feed', truth.feed?.heartbeat?.freshness || 'unknown');
  setText('truth-cache', truth.cache?.status || 'unknown');
  setText('truth-services', truth.services?.trader?.status || (truth.services?.trader?.available === false ? 'unavailable' : 'unknown'));
  const paper_book = truth.paper_book || {};
  setText('truth-paper-book', paper_book.status === 'ok' ? `${money(paper_book.cash_usd)} cash / ${count(paper_book.open_positions)} positions` : 'unknown');
  const execution_decision = truth.execution_decision || {};
  setText('truth-execution-decision', execution_decision.value || 'unknown');
  const terminal = truth.terminal || {};
  setText('truth-terminal', terminal.url || 'unknown');
  $('#system-warnings').innerHTML = (truth.warnings || []).map(warning =>
    `<div class="list-card"><strong>${escapeHtml(warning)}</strong><small>Source-labelled system truth warning.</small></div>`
  ).join('') || '<div class="list-card"><strong>No system warnings</strong><small>All currently probed sources are fresh.</small></div>';
}

function renderAll() {
  renderRaceHeader();
  renderCompetitor('agent');
  renderCompetitor('bot');
  renderAlerts();
  renderQueue();
  renderActivity();
  renderRaceTable();
  renderTrades();
  renderSignals();
  renderLearning();
  renderCosts();
  renderRisk();
  renderSystemTruth();
}

async function refreshAll() {
  state.lastError = null;
  const [competition, truth, costs, executions, opportunities, feed, budgets, budgetApprovals] = await Promise.all([
    api('/api/competition'),
    api('/api/system-truth'),
    api('/api/agents/costs'),
    api('/api/executions'),
    api('/api/opportunities'),
    api('/api/activity-feed'),
    api('/api/agents/budgets'),
    api('/api/agents/budget-approvals'),
  ]);
  if (competition) state.competition = competition;
  if (truth) state.truth = truth;
  if (costs) state.costs = costs;
  if (executions) state.executions = executions.executions || [];
  if (opportunities) state.opportunities = opportunities.opportunities || [];
  if (feed) state.feed = feed.feed || [];
  if (budgets) state.budgets = budgets.budgets || [];
  if (budgetApprovals) state.budgetApprovals = budgetApprovals.budgetApprovals || [];

  const connected = Boolean(competition || truth);
  $('#connection-dot').className = `status-dot ${connected ? 'live' : 'dead'}`;
  setText('connection-label', connected ? 'Connected' : 'Degraded');
  setText('last-refresh', new Date().toLocaleTimeString());
  renderAll();
}

function activateView(id) {
  $$('.view').forEach(view => view.classList.toggle('active-view', view.id === id));
  $$('#nav-tabs a').forEach(link => link.classList.toggle('nav-active', link.getAttribute('href') === `#${id}`));
}

$('#nav-tabs').addEventListener('click', event => {
  const link = event.target.closest('a');
  if (!link) return;
  event.preventDefault();
  const id = link.getAttribute('href').slice(1);
  history.replaceState(null, '', `#${id}`);
  activateView(id);
});

$('#refresh-button').addEventListener('click', refreshAll);
window.addEventListener('hashchange', () => activateView(location.hash.slice(1) || 'overview'));

activateView(location.hash.slice(1) || 'overview');
refreshAll();
setInterval(refreshAll, 5000);
