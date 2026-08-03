const $ = selector => document.querySelector(selector);
const $$ = selector => [...document.querySelectorAll(selector)];

const executionAuthorityLabel = 'Local operator execution (non-canonical)';

const state = {
  competition: null,
  truth: null,
  costs: { costs: [], summary: {} },
  executions: [],
  executionEvents: [],
  opportunities: [],
  feed: [],
  budgets: [],
  budgetApprovals: [],
  positions: [],
  accounts: [],
  quotes: {},
  lastError: null,
  refreshing: false,
  ui: {
    executionStatus: 'all',
    executionOwner: 'all',
    executionSearch: '',
  },
};

const pageCopy = {
  overview: ['Today', 'What the bot and agent are doing, why they are doing it, and what needs attention.'],
  trades: ['Execution', 'Every execution, order, fill, settlement, and failure in one traceable lifecycle.'],
  positions: ['Positions', 'Current exposure across the operator state and both competition books.'],
  signals: ['Decisions', 'What the bot and agent considered before anything reached execution.'],
  race: ['Competition', 'The shared-epoch, cost-adjusted race between the deterministic bot and paid agent.'],
  agent: ['Agent', 'Paid inference economics, budget controls, and learning evidence.'],
  system: ['Risk & System', 'Source-labelled health, risk, execution mode, and competition validity.'],
};

const escapeHtml = value => String(value ?? '')
  .replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;')
  .replaceAll('"', '&quot;').replaceAll("'", '&#039;');

const finite = value => value !== null && value !== undefined && value !== '' && Number.isFinite(Number(value));
const money = value => finite(value)
  ? Number(value).toLocaleString('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 2 })
  : '—';
const number = (value, digits = 2) => finite(value)
  ? Number(value).toLocaleString('en-US', { maximumFractionDigits: digits })
  : '—';
const pct = value => finite(value) ? `${Number(value).toFixed(2)}%` : '—';
const ratio = value => finite(value) ? `${Number(value).toFixed(2)}×` : '—';
const count = value => finite(value) ? Number(value).toLocaleString('en-US') : '—';
const dateTime = value => {
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? '—' : date.toLocaleString();
};
const relativeTime = value => {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return '—';
  const seconds = Math.round((Date.now() - date.getTime()) / 1000);
  const abs = Math.abs(seconds);
  if (abs < 45) return seconds >= 0 ? 'just now' : 'soon';
  if (abs < 3600) return `${Math.round(abs / 60)}m ${seconds >= 0 ? 'ago' : 'from now'}`;
  if (abs < 86400) return `${Math.round(abs / 3600)}h ${seconds >= 0 ? 'ago' : 'from now'}`;
  return `${Math.round(abs / 86400)}d ${seconds >= 0 ? 'ago' : 'from now'}`;
};
const valueClass = value => Number(value) > 0 ? 'positive' : Number(value) < 0 ? 'negative' : '';
const words = value => String(value || 'unknown').replaceAll('_', ' ').replaceAll('-', ' ');
const titleWords = value => words(value).replace(/\b\w/g, letter => letter.toUpperCase());

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

function executionOwner(row = {}) {
  const raw = String(row.tags?.competitor || row.competitor || row.sourceAgentId || row.agentId || row.strategyId || row.owner || '').toLowerCase();
  if (/agent|hermes|openrouter|model/.test(raw)) return 'agent';
  if (/bot|eventtrader|deterministic|v4/.test(raw)) return 'bot';
  return 'other';
}

function executionLabel(row = {}) {
  const owner = executionOwner(row);
  if (owner === 'agent') return 'Paid agent';
  if (owner === 'bot') return 'Deterministic bot';
  return row.sourceAgentId || row.strategyId || row.owner || 'Unknown owner';
}

function executionSymbol(row = {}) {
  return row.symbol || row.marketId || row.orders?.[0]?.symbol || row.orders?.[0]?.marketId || '—';
}

function executionSide(row = {}) {
  return row.side || row.orders?.[0]?.side || row.tradePlan?.position_side || '—';
}

function executionNotional(row = {}) {
  return row.notional ?? row.totalMoneyRisked ?? row.orders?.reduce((sum, order) => sum + Number(order.notional || 0), 0);
}

function executionTimestamp(row = {}) {
  return row.updatedAt || row.completedAt || row.startedAt || row.createdAt || row.submittedAt;
}

function isExecutionActive(status) {
  return ['draft', 'pending', 'pending_approval', 'approved', 'planned', 'submitted', 'open', 'partially_filled', 'reconciling'].includes(String(status || '').toLowerCase());
}

function isExecutionFailed(status) {
  return ['failed', 'rejected', 'cancelled', 'canceled', 'error', 'blocked'].includes(String(status || '').toLowerCase());
}

function isExecutionFilled(row = {}) {
  const status = String(row.status || '').toLowerCase();
  return ['filled', 'completed', 'settled'].includes(status) || (row.fills?.length || 0) > 0;
}

function isExecutionSettled(row = {}) {
  const fills = Array.isArray(row.fills) ? row.fills : [];
  return String(row.status || '').toLowerCase() === 'settled'
    || (fills.length > 0 && fills.every(fill => String(fill.settlementStatus || '').toLowerCase() === 'settled'));
}

function statusBadge(status) {
  const normalized = String(status || 'unknown').toLowerCase();
  const className = isExecutionFailed(normalized) ? 'badge-err' : isExecutionActive(normalized) ? 'badge-warn' : ['ok', 'filled', 'completed', 'settled', 'approved', 'open'].includes(normalized) ? 'badge-ok' : '';
  return `<span class="badge ${className}">${escapeHtml(titleWords(normalized))}</span>`;
}

function normalizedPositions() {
  const rows = Array.isArray(state.positions) ? state.positions : [];
  return rows.map(position => {
    const symbol = position.symbol || position.productId || position.product_id || position.currency || '—';
    const quote = state.quotes?.[symbol] || {};
    const quantity = Number(position.quantity ?? position.base ?? position.size ?? position.balance ?? 0);
    const average = Number(position.averagePrice ?? position.average_price ?? position.entryPrice ?? position.entry_price ?? 0);
    const mark = Number(position.markPrice ?? position.mark_price ?? quote.mid ?? quote.bid ?? 0);
    const marketValue = finite(position.marketValue ?? position.market_value)
      ? Number(position.marketValue ?? position.market_value)
      : quantity > 0 && mark > 0 ? quantity * mark : null;
    const unrealized = finite(position.unrealizedPnl ?? position.unrealized_pnl)
      ? Number(position.unrealizedPnl ?? position.unrealized_pnl)
      : average > 0 && mark > 0 && quantity > 0 ? (mark - average) * quantity : null;
    return {
      ...position,
      symbol,
      quantity,
      average,
      mark,
      marketValue,
      unrealized,
      source: position.source || position.venue || position.provider || 'operator state',
      status: position.status || 'open',
    };
  }).filter(position => position.quantity > 0 || position.status === 'open');
}

function allWarnings() {
  return [...new Set([
    ...(state.competition?.warnings || []),
    ...(state.truth?.warnings || []),
    ...(state.lastError ? [state.lastError] : []),
  ])];
}

function renderTopStatus() {
  const truth = state.truth || {};
  const standings = state.competition?.standings || {};
  const mode = truth.trading_mode?.value || 'unknown';
  const execution = truth.execution_decision?.value || truth.execution_decision?.status || 'unknown';
  const feed = truth.feed?.heartbeat?.freshness || truth.feed?.status || 'unknown';
  const valid = standings.valid_for_ranking === true;

  setText('strip-mode', titleWords(mode));
  setText('strip-execution', titleWords(execution));
  setText('strip-feed', titleWords(feed));
  setText('strip-competition', valid ? `${titleWords(standings.leader)} leading` : 'Ranking blocked');
  setText('sidebar-mode', `Mode: ${titleWords(mode)}`);
  setText('race-validity', valid ? 'Ranking valid' : 'Ranking blocked', `badge ${valid ? 'badge-ok' : 'badge-warn'}`);
}

function renderAlerts() {
  const warnings = allWarnings();
  const stack = $('#alert-stack');
  if (!warnings.length) {
    stack.innerHTML = '<div class="alert info"><span>Observed system, accounting, and execution contracts are currently satisfied.</span><button type="button" data-view-target="system">Review system</button></div>';
    return;
  }
  stack.innerHTML = warnings.slice(0, 4).map(warning => {
    const critical = /corrupt|invalid|failed|missing|unavailable|mismatch/i.test(warning);
    return `<div class="alert ${critical ? 'error' : ''}"><span>${escapeHtml(titleWords(warning))}</span><button type="button" data-view-target="system">Inspect</button></div>`;
  }).join('');
}

function renderDailyBrief() {
  const positions = normalizedPositions();
  const agent = competitor('agent');
  const bot = competitor('bot');
  const standings = state.competition?.standings || {};
  const active = state.executions.filter(row => isExecutionActive(row.status));
  const failed = state.executions.filter(row => isExecutionFailed(row.status));
  const mode = titleWords(state.truth?.trading_mode?.value || 'unknown');
  const last = state.feed[0] || state.executionEvents[0] || null;
  const observedOpen = Math.max(
    positions.length,
    Number(agent.open_positions || 0) + Number(bot.open_positions || 0),
  );

  let title = `${mode} trading is being observed`;
  let copy = `${count(observedOpen)} open position${observedOpen === 1 ? '' : 's'} and ${count(active.length)} execution${active.length === 1 ? '' : 's'} in progress.`;
  if (failed.length) copy += ` ${count(failed.length)} execution${failed.length === 1 ? '' : 's'} require failure or rejection review.`;
  if (standings.valid_for_ranking) {
    copy += ` The ${standings.leader === 'agent' ? 'paid agent' : standings.leader === 'bot' ? 'deterministic bot' : 'two books'} ${standings.leader === 'tie' ? 'are tied' : 'currently leads'} by ${money(standings.edge_usd)} after agent cost.`;
  } else {
    copy += ' The competition is not currently allowed to name a winner.';
  }
  if (String(state.truth?.execution_decision?.status || '').toLowerCase() !== 'ok') {
    title = 'Trading evidence needs operator review';
  }

  setText('daily-brief-title', title);
  setText('daily-brief-copy', copy);
  setText('last-event-label', last ? titleWords(last.action || last.type || 'event') : 'No event observed');
  setText('last-event-time', last ? `${relativeTime(last.timestamp || last.at)} · ${dateTime(last.timestamp || last.at)}` : '—');

  setText('kpi-open-positions', count(observedOpen));
  setText('kpi-pending-executions', count(active.length));
  setText('kpi-agent-cost', money(agent.operating_cost_usd));
  setText('kpi-race-standing', standings.valid_for_ranking ? titleWords(standings.leader) : 'Blocked');
  setText('kpi-race-note', standings.valid_for_ranking ? `${money(standings.edge_usd)} normalized edge` : 'Resolve validity warnings');
}

function attentionItems() {
  const items = [];
  const standings = state.competition?.standings || {};
  const failed = state.executions.filter(row => isExecutionFailed(row.status));
  const active = state.executions.filter(row => isExecutionActive(row.status));
  const pendingBudgets = state.budgetApprovals.filter(row => ['pending', 'requested', 'review'].includes(String(row.status || '').toLowerCase()));
  const warnings = allWarnings();

  if (failed.length) items.push({ severity: 'error', title: `${failed.length} execution${failed.length === 1 ? '' : 's'} failed, rejected, or cancelled`, detail: 'Review the execution lifecycle and event evidence before retrying or changing strategy behavior.', view: 'trades' });
  if (!standings.valid_for_ranking) items.push({ severity: 'warning', title: 'Competition ranking is blocked', detail: 'The UI will not name a winner until both books share a valid epoch and fresh comparable accounting.', view: 'race' });
  if ((standings.agent_cost_coverage_ratio ?? Infinity) < 1) items.push({ severity: 'warning', title: 'Paid agent has not covered inference cost', detail: `${money(standings.agent_break_even_gap_usd)} more gross P&L is required to reach API and compute break-even.`, view: 'agent' });
  if (pendingBudgets.length) items.push({ severity: 'warning', title: `${pendingBudgets.length} agent budget request${pendingBudgets.length === 1 ? '' : 's'} awaiting review`, detail: 'Paid research should remain linked to an explicit opportunity, budget, and expected-value hypothesis.', view: 'agent' });
  if (warnings.length) items.push({ severity: 'error', title: `${warnings.length} system or accounting warning${warnings.length === 1 ? '' : 's'}`, detail: 'Inspect source-labelled evidence. Unknown or stale truth keeps the system fail-closed.', view: 'system' });
  if (!items.length && active.length) items.push({ severity: 'ok', title: `${active.length} execution${active.length === 1 ? '' : 's'} progressing normally`, detail: 'No operator intervention is currently indicated. Continue observing the lifecycle through settlement.', view: 'trades' });
  if (!items.length) items.push({ severity: 'ok', title: 'No immediate operator action', detail: 'The observed paper system is healthy and no executions require intervention.', view: 'overview' });
  return items;
}

function renderQueue() {
  const items = attentionItems();
  setText('queue-count', String(items.filter(item => item.severity !== 'ok').length));
  $('#command-queue').innerHTML = items.slice(0, 6).map(item => `<button type="button" class="attention-item" data-view-target="${escapeHtml(item.view)}">
    <span class="attention-marker ${item.severity === 'error' ? 'error' : item.severity === 'ok' ? 'ok' : ''}"></span>
    <span class="attention-copy"><strong>${escapeHtml(item.title)}</strong><small>${escapeHtml(item.detail)}</small></span>
    <span class="attention-action">Open →</span>
  </button>`).join('');
}

function renderPipeline() {
  const signals = state.opportunities.length;
  const approved = state.opportunities.filter(row => String(row.approvalStatus || row.status || '').toLowerCase() === 'approved').length;
  const executions = state.executions.length;
  const filled = state.executions.filter(isExecutionFilled).length;
  const settled = state.executions.filter(isExecutionSettled).length;
  const steps = [
    ['Signals', signals],
    ['Approved', approved],
    ['Executions', executions],
    ['Filled', filled],
    ['Settled', settled],
  ];
  $('#execution-pipeline').innerHTML = steps.map(([label, value], index) => `<div class="pipeline-step ${value > 0 ? 'active' : index > 1 && executions > 0 ? 'warn' : ''}">
    <span class="pipeline-number">${escapeHtml(count(value))}</span><small>${escapeHtml(label)}</small>
  </div>`).join('');
  const failed = state.executions.filter(row => isExecutionFailed(row.status)).length;
  setText('pipeline-health', failed ? `${failed} need review` : 'Flow observed', `badge ${failed ? 'badge-warn' : 'badge-ok'}`);
}

function renderPositionPreview() {
  const rows = normalizedPositions().slice(0, 6);
  $('#position-preview').innerHTML = rows.length ? rows.map(row => `<div class="compact-row">
    <div><strong>${escapeHtml(row.symbol)}</strong><small>${escapeHtml(titleWords(row.source))} · ${escapeHtml(number(row.quantity, 8))} units</small></div>
    <div class="row-value"><strong>${money(row.marketValue)}</strong><small class="${valueClass(row.unrealized)}">${row.unrealized == null ? 'P&L unavailable' : `${money(row.unrealized)} unrealized`}</small></div>
  </div>`).join('') : '<div class="empty-state">No operator positions are currently reported.</div>';
}

function renderActivity() {
  const rows = state.feed.slice(0, 10);
  setText('activity-count', String(rows.length));
  $('#activity-feed').innerHTML = rows.length ? rows.map(row => `<div class="timeline-item">
    <span class="timeline-dot ${escapeHtml(row.type || '')}"></span>
    <div class="timeline-copy"><strong>${escapeHtml(titleWords(row.action || row.type || 'event'))}</strong><small>${escapeHtml(row.details || row.actor || 'No additional detail')}</small></div>
    <span class="timeline-time" title="${escapeHtml(dateTime(row.timestamp || row.at))}">${escapeHtml(relativeTime(row.timestamp || row.at))}</span>
  </div>`).join('') : '<div class="empty-state">No recent activity was returned.</div>';
}

function renderOverview() {
  renderDailyBrief();
  renderQueue();
  renderPipeline();
  renderPositionPreview();
  renderActivity();
}

function executionEvents(row) {
  return state.executionEvents
    .filter(event => event.executionId === row.id || event.execution_id === row.id)
    .sort((a, b) => new Date(a.timestamp || a.at || 0) - new Date(b.timestamp || b.at || 0));
}

function executionStages(row) {
  const status = String(row.status || '').toLowerCase();
  const failed = isExecutionFailed(status);
  return [
    ['Decision', true],
    ['Approved', !['draft', 'pending', 'pending_approval', 'rejected'].includes(status) && !failed],
    ['Submitted', ['submitted', 'open', 'partially_filled', 'filled', 'completed', 'settled'].includes(status)],
    ['Filled', isExecutionFilled(row)],
    ['Settled', isExecutionSettled(row)],
  ].map(([label, complete]) => `<span class="stage-chip ${failed && label !== 'Decision' ? 'failed' : complete ? 'complete' : ''}">${escapeHtml(label)}</span>`).join('');
}

function filteredExecutions() {
  const search = state.ui.executionSearch.trim().toLowerCase();
  return state.executions.filter(row => {
    const status = String(row.status || '').toLowerCase();
    const owner = executionOwner(row);
    const statusMatch = state.ui.executionStatus === 'all'
      || (state.ui.executionStatus === 'active' && isExecutionActive(status))
      || (state.ui.executionStatus === 'filled' && isExecutionFilled(row))
      || (state.ui.executionStatus === 'failed' && isExecutionFailed(status));
    const ownerMatch = state.ui.executionOwner === 'all' || owner === state.ui.executionOwner;
    const searchValue = [row.id, executionSymbol(row), row.strategyId, row.sourceAgentId, row.status, executionSide(row)].join(' ').toLowerCase();
    return statusMatch && ownerMatch && (!search || searchValue.includes(search));
  }).sort((a, b) => new Date(executionTimestamp(b) || 0) - new Date(executionTimestamp(a) || 0));
}

function renderExecutionSummary() {
  const active = state.executions.filter(row => isExecutionActive(row.status)).length;
  const filled = state.executions.filter(isExecutionFilled).length;
  const failed = state.executions.filter(row => isExecutionFailed(row.status)).length;
  $('#execution-summary-chips').innerHTML = [
    ['In progress', active], ['Filled', filled], ['Need review', failed], ['Total', state.executions.length],
  ].map(([label, value]) => `<span class="summary-chip">${escapeHtml(label)} <b>${escapeHtml(count(value))}</b></span>`).join('');
}

function renderExecutions() {
  renderExecutionSummary();
  const rows = filteredExecutions();
  setText('trade-count', `${rows.length} shown`);
  $('#execution-list').innerHTML = rows.length ? rows.map(row => {
    const events = executionEvents(row);
    const orders = Array.isArray(row.orders) ? row.orders : [];
    const fills = Array.isArray(row.fills) ? row.fills : [];
    const error = row.error || row.reason || row.errors?.join(', ') || 'None reported';
    return `<details class="execution-card">
      <summary class="execution-summary">
        <div><strong>${escapeHtml(executionSymbol(row))}</strong><small>${escapeHtml(executionSide(row).toUpperCase())}</small></div>
        <div><strong>${escapeHtml(executionLabel(row))}</strong><small>${escapeHtml(row.strategyId || row.sourceAgentId || 'No strategy ID')}</small></div>
        <div><strong>${escapeHtml(row.id || 'Unknown execution')}</strong><small>${escapeHtml(relativeTime(executionTimestamp(row)))}</small></div>
        <div>${statusBadge(row.status)}</div>
        <div><strong>${money(executionNotional(row))}</strong><small>${row.confidenceScore == null ? 'confidence unavailable' : `${pct(Number(row.confidenceScore) * 100)} confidence`}</small></div>
        <span class="execution-chevron">›</span>
      </summary>
      <div class="execution-detail">
        <div>
          <div class="execution-stage">${executionStages(row)}</div>
          <div class="detail-grid">
            <div><small>Created</small><strong>${escapeHtml(dateTime(row.createdAt || row.startedAt))}</strong></div>
            <div><small>Last update</small><strong>${escapeHtml(dateTime(executionTimestamp(row)))}</strong></div>
            <div><small>Orders</small><strong>${escapeHtml(count(orders.length))}</strong></div>
            <div><small>Fills</small><strong>${escapeHtml(count(fills.length))}</strong></div>
            <div><small>Entry / target / stop</small><strong>${escapeHtml([row.entryPrice ?? row.tradePlan?.entry_price, row.takeProfitPrice ?? row.tradePlan?.take_profit_price, row.stopLossPrice ?? row.tradePlan?.stop_loss_price].map(value => finite(value) ? money(value) : '—').join(' / '))}</strong></div>
            <div><small>Error or rejection</small><strong>${escapeHtml(error)}</strong></div>
          </div>
        </div>
        <div>
          <p class="section-kicker">EVENT HISTORY</p>
          <div class="event-list">${events.length ? events.map(event => `<div class="event-row"><strong>${escapeHtml(titleWords(event.type || event.action || 'event'))}</strong> · ${escapeHtml(relativeTime(event.timestamp || event.at))}${event.details ? ` · ${escapeHtml(event.details)}` : ''}</div>`).join('') : '<div class="event-row">No execution events were returned for this ID.</div>'}</div>
        </div>
      </div>
    </details>`;
  }).join('') : '<div class="panel empty-state">No executions match the selected filters.</div>';
}

function renderPositions() {
  const rows = normalizedPositions();
  const operatorValue = rows.reduce((sum, row) => sum + Number(row.marketValue || 0), 0);
  const unrealizedKnown = rows.filter(row => row.unrealized != null);
  const unrealized = unrealizedKnown.reduce((sum, row) => sum + Number(row.unrealized || 0), 0);
  const reportedBooks = Number(competitor('agent').open_positions || 0) + Number(competitor('bot').open_positions || 0);
  $('#position-summary-chips').innerHTML = [
    ['Operator rows', rows.length], ['Observed market value', money(operatorValue)], ['Known unrealized', money(unrealized)], ['Competition-book positions', reportedBooks],
  ].map(([label, value]) => `<span class="summary-chip">${escapeHtml(label)} <b>${escapeHtml(value)}</b></span>`).join('');
  setText('position-count', String(rows.length));
  $('#position-rows').innerHTML = rows.length ? rows.map(row => `<tr>
    <td><strong>${escapeHtml(row.symbol)}</strong></td>
    <td>${escapeHtml(titleWords(row.source))}</td>
    <td>${escapeHtml(number(row.quantity, 8))}</td>
    <td>${row.average > 0 ? money(row.average) : '—'}</td>
    <td>${row.mark > 0 ? money(row.mark) : '—'}</td>
    <td>${money(row.marketValue)}</td>
    <td class="${valueClass(row.unrealized)}">${money(row.unrealized)}</td>
    <td>${statusBadge(row.status)}</td>
  </tr>`).join('') : '<tr><td colspan="8" class="empty-state">No open operator positions are currently reported.</td></tr>';
}

function decisionSource(row = {}) {
  const raw = String(row.sourceAgentId || row.strategyId || row.source || 'unknown');
  return /agent|hermes|openrouter|model/i.test(raw) ? 'Paid agent' : /bot|eventtrader|v4|deterministic/i.test(raw) ? 'Deterministic bot' : raw;
}

function renderDecisions() {
  const rows = [...state.opportunities].sort((a, b) => new Date(b.updatedAt || b.createdAt || 0) - new Date(a.updatedAt || a.createdAt || 0));
  const approved = rows.filter(row => String(row.approvalStatus || row.status || '').toLowerCase() === 'approved').length;
  const agentRows = rows.filter(row => decisionSource(row) === 'Paid agent').length;
  const netPositive = rows.filter(row => Number(row.netExpectedValue || 0) > 0).length;
  $('#decision-summary-chips').innerHTML = [
    ['Considered', rows.length], ['Approved', approved], ['Agent-generated', agentRows], ['Positive net EV', netPositive],
  ].map(([label, value]) => `<span class="summary-chip">${escapeHtml(label)} <b>${escapeHtml(count(value))}</b></span>`).join('');
  $('#decision-list').innerHTML = rows.length ? rows.map(row => {
    const cost = Number(row.agentResearchCost || 0) + Number(row.modelInferenceCost || 0);
    const rationale = row.rationale || row.reason || row.thesis || row.details || row.summary || 'No decision rationale was persisted with this opportunity.';
    return `<article class="decision-card">
      <div><strong>${escapeHtml(row.symbol || row.marketSlug || row.title || 'Unknown market')}</strong><small>${escapeHtml(decisionSource(row))}</small></div>
      <div><strong>${escapeHtml(titleWords(row.recommendation || row.side || 'review'))}</strong><small>${row.confidenceScore == null ? 'Confidence unavailable' : `${pct(Number(row.confidenceScore) * 100)} confidence`}</small></div>
      <div class="decision-rationale"><strong>Why</strong><small>${escapeHtml(rationale)}</small></div>
      <div><strong class="${valueClass(row.netExpectedValue)}">${money(row.netExpectedValue)}</strong><small>${money(row.grossExpectedValue)} gross · ${money(cost)} cost</small></div>
      <div>${statusBadge(row.approvalStatus || row.status)}</div>
    </article>`;
  }).join('') : '<div class="panel empty-state">No normalized opportunities or decisions are available.</div>';
}

function renderRace() {
  const standings = state.competition?.standings || {};
  const valid = standings.valid_for_ranking === true;
  setText('race-leader', valid ? `${titleWords(standings.leader)} leads by ${money(standings.edge_usd)}` : 'No trustworthy winner yet');
  setText('race-edge', valid ? `${money(standings.edge_usd)} normalized net-equity edge` : 'Resolve ledger warnings before ranking');
  setText('race-cost-coverage', `Agent cost coverage ${ratio(standings.agent_cost_coverage_ratio)}`);
  setText('head-to-head-alpha', standings.agent_alpha_after_cost_pct_points == null ? '—' : `${standings.agent_alpha_after_cost_pct_points >= 0 ? '+' : ''}${pct(standings.agent_alpha_after_cost_pct_points)}`);

  for (const side of ['agent', 'bot']) {
    const row = competitor(side);
    setText(`${side}-status`, row.status || 'unknown', `badge ${row.status === 'ok' ? 'badge-ok' : row.status === 'stale' ? 'badge-warn' : 'badge-err'}`);
    setText(`${side}-net-equity`, money(row.net_equity_usd));
    setText(`${side}-gross-pnl`, money(row.gross_pnl_usd), valueClass(row.gross_pnl_usd));
    setText(`${side}-return`, pct(row.net_return_pct), valueClass(row.net_return_pct));
    setText(`${side}-dd`, pct(row.max_drawdown_pct), Math.abs(Number(row.max_drawdown_pct)) > 10 ? 'negative' : '');
    setText(`${side}-win`, row.win_rate == null ? '—' : pct(Number(row.win_rate) * 100));
    setText(`${side}-trades`, count(row.round_trips));
  }
  setText('agent-cost', money(competitor('agent').operating_cost_usd));
  setText('bot-fees', money(competitor('bot').fees_paid_usd ?? competitor('bot').fees_from_trades_usd));

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
  $('#race-contract').textContent = `Shared epoch: ${state.competition?.epoch?.id || state.competition?.competition_epoch_id || 'unknown'} · Bot equity: ${contracts.bot_equity || 'unknown'} · Agent score: ${contracts.agent_score || 'unknown'} · Leader: ${contracts.leader || 'unknown'}`;

  const warnings = state.competition?.warnings || [];
  $('#validity-content').innerHTML = warnings.length ? warnings.map(warning => `<div class="compact-row"><div><strong>${escapeHtml(titleWords(warning))}</strong><small>Ranking remains fail-closed until this evidence is resolved.</small></div><span class="badge badge-warn">Blocked</span></div>`).join('') : '<div class="compact-row"><div><strong>Competition evidence is valid</strong><small>Both books are fresh, share an epoch, and use cost-adjusted comparable accounting.</small></div><span class="badge badge-ok">Valid</span></div>';
}

function renderAgent() {
  const summary = state.costs?.summary || {};
  const agent = competitor('agent');
  const standings = state.competition?.standings || {};
  const objective = Number(standings.agent_cost_coverage_ratio || 0) >= 1
    ? `Inference covered ${ratio(standings.agent_cost_coverage_ratio)}`
    : `${money(standings.agent_break_even_gap_usd)} to break-even`;
  setText('agent-objective', objective);
  const rows = [
    ['Attributed epoch cost', money(agent.operating_cost_usd)],
    ['Remote model ledger', money(summary.remoteModelCostUsd)],
    ['Local compute ledger', money(summary.localModelCostUsd)],
    ['Cost / opportunity', money(summary.costPerOpportunityUsd)],
    ['Daily budget', money(summary.dailyBudgetUsd)],
    ['Gross P&L / cost', ratio(standings.agent_cost_coverage_ratio)],
    ['Net P&L after cost', money(agent.net_pnl_usd)],
    ['Break-even gap', money(standings.agent_break_even_gap_usd)],
  ];
  $('#cost-kpis').innerHTML = rows.map(([label, value]) => `<div><small>${escapeHtml(label)}</small><b>${escapeHtml(value)}</b></div>`).join('');

  setText('budget-approval-count', String(state.budgetApprovals.length));
  $('#budget-approval-rows').innerHTML = state.budgetApprovals.length ? state.budgetApprovals.slice(-20).reverse().map(row => `<div class="compact-row">
    <div><strong>${escapeHtml(row.agentId || 'Agent budget')}</strong><small>${money(row.projectedCost)} · ${count(row.projectedTokens)} tokens · ${escapeHtml(row.marketScope || 'general')}</small></div>${statusBadge(row.status)}
  </div>`).join('') : '<div class="empty-state">No paid-agent budget approvals are pending or recorded.</div>';

  const bot = competitor('bot');
  const cards = [
    ['Measurability', standings.valid_for_ranking ? 'The shared-epoch race is measurable. Promotions still require out-of-sample evidence and rollback metadata.' : 'Learning and promotion remain blocked while the competition cannot be measured fairly.'],
    ['Economic objective', `The agent must cover ${money(agent.operating_cost_usd)} of attributable cost and exceed the bot's ${pct(bot.net_return_pct)} net return within the same risk limits.`],
    ['Decision lineage', 'Each model request should connect to its prompt, cost, signal, execution, fill, outcome, and resulting challenger hypothesis.'],
    ['Promotion contract', 'A challenger needs a version, evaluation window, minimum trades, regime diversity, risk delta, canary result, approval, and rollback pointer.'],
  ];
  $('#learning-content').innerHTML = cards.map(([title, text]) => `<div class="learning-card"><strong>${escapeHtml(title)}</strong><small>${escapeHtml(text)}</small></div>`).join('');
}

function renderSystem() {
  const truth = state.truth || {};
  setText('truth-mode', truth.trading_mode?.value || 'unknown');
  setText('truth-feed', truth.feed?.heartbeat?.freshness || truth.feed?.status || 'unknown');
  setText('truth-cache', truth.cache?.status || 'unknown');
  setText('truth-services', truth.services?.trader?.status || (truth.services?.trader?.available === false ? 'unavailable' : 'unknown'));
  const paperBook = truth.paper_book || {};
  setText('truth-paper-book', paperBook.status === 'ok' ? `${money(paperBook.cash_usd)} cash / ${count(paperBook.open_positions)} positions` : 'unknown');
  setText('truth-execution-decision', truth.execution_decision?.value || truth.execution_decision?.status || 'unknown');
  setText('truth-terminal', truth.terminal?.url || 'unknown');

  const agent = competitor('agent');
  const bot = competitor('bot');
  const positions = normalizedPositions();
  const knownExposure = positions.reduce((sum, position) => sum + Number(position.marketValue || 0), 0);
  $('#risk-content').innerHTML = [
    ['Agent max drawdown', pct(agent.max_drawdown_pct), Math.abs(Number(agent.max_drawdown_pct)) > 10 ? 'badge-err' : ''],
    ['Bot max drawdown', pct(bot.max_drawdown_pct), Math.abs(Number(bot.max_drawdown_pct)) > 10 ? 'badge-err' : ''],
    ['Agent open positions', count(agent.open_positions), ''],
    ['Bot open positions', count(bot.open_positions), ''],
    ['Known operator exposure', money(knownExposure), ''],
    ['Execution authority', executionAuthorityLabel, 'badge-ok'],
  ].map(([label, value, badgeClass]) => `<div class="compact-row"><div><strong>${escapeHtml(label)}</strong><small>${escapeHtml(value)}</small></div>${badgeClass ? `<span class="badge ${badgeClass}">${badgeClass === 'badge-ok' ? 'Guarded' : 'Review'}</span>` : ''}</div>`).join('');

  const warnings = allWarnings();
  setText('system-warning-count', String(warnings.length));
  setText('system-overall', warnings.length ? `${warnings.length} warning${warnings.length === 1 ? '' : 's'} need review` : 'Observed system healthy');
  $('#system-warnings').innerHTML = warnings.length ? warnings.map(warning => `<div class="compact-row"><div><strong>${escapeHtml(titleWords(warning))}</strong><small>Source-labelled evidence remains unresolved. No optimistic default was applied.</small></div><span class="badge badge-warn">Review</span></div>`).join('') : '<div class="compact-row"><div><strong>No system warnings</strong><small>All currently probed sources are fresh and internally consistent.</small></div><span class="badge badge-ok">Healthy</span></div>';
}

function renderNavigationCounts() {
  const activeExecutions = state.executions.filter(row => isExecutionActive(row.status) || isExecutionFailed(row.status)).length;
  setText('nav-execution-count', String(activeExecutions));
  setText('nav-position-count', String(normalizedPositions().length));
  setText('nav-decision-count', String(state.opportunities.length));
  setText('nav-warning-count', String(allWarnings().length));
}

function renderAll() {
  renderTopStatus();
  renderAlerts();
  renderOverview();
  renderExecutions();
  renderPositions();
  renderDecisions();
  renderRace();
  renderAgent();
  renderSystem();
  renderNavigationCounts();
}

async function refreshAll() {
  if (state.refreshing) return;
  state.refreshing = true;
  state.lastError = null;
  $('#refresh-button').disabled = true;
  setText('connection-label', 'Refreshing');

  const [competition, truth, costs, executions, executionEvents, opportunities, feed, budgets, budgetApprovals, positions, quotes] = await Promise.all([
    api('/api/competition'),
    api('/api/system-truth'),
    api('/api/agents/costs'),
    api('/api/executions'),
    api('/api/execution/events'),
    api('/api/opportunities'),
    api('/api/activity-feed'),
    api('/api/agents/budgets'),
    api('/api/agents/budget-approvals'),
    api('/api/positions'),
    api('/api/market-data/live-quotes'),
  ]);

  if (competition) state.competition = competition;
  if (truth) state.truth = truth;
  if (costs) state.costs = costs;
  if (executions) state.executions = executions.executions || [];
  if (executionEvents) state.executionEvents = executionEvents.events || [];
  if (opportunities) state.opportunities = opportunities.opportunities || [];
  if (feed) state.feed = feed.feed || [];
  if (budgets) state.budgets = budgets.budgets || [];
  if (budgetApprovals) state.budgetApprovals = budgetApprovals.budgetApprovals || [];
  if (positions) {
    state.positions = positions.positions || [];
    state.accounts = positions.accounts || [];
  }
  if (quotes) state.quotes = quotes.quotes || {};

  const connected = Boolean(competition || truth || executions);
  $('#connection-dot').className = `status-dot ${connected ? 'live' : 'dead'}`;
  setText('connection-label', connected ? 'Connected' : 'Degraded');
  setText('last-refresh', `Updated ${new Date().toLocaleTimeString()}`);
  $('#refresh-button').disabled = false;
  state.refreshing = false;
  renderAll();
}

function activateView(id) {
  const target = pageCopy[id] ? id : 'overview';
  $$('.view').forEach(view => view.classList.toggle('active-view', view.id === target));
  $$('#nav-tabs a').forEach(link => link.classList.toggle('nav-active', link.getAttribute('href') === `#${target}`));
  setText('page-title', pageCopy[target][0]);
  setText('page-subtitle', pageCopy[target][1]);
  window.scrollTo({ top: 0, behavior: 'smooth' });
}

function navigateTo(id) {
  history.replaceState(null, '', `#${id}`);
  activateView(id);
}

$('#nav-tabs').addEventListener('click', event => {
  const link = event.target.closest('a');
  if (!link) return;
  event.preventDefault();
  navigateTo(link.getAttribute('href').slice(1));
});

document.addEventListener('click', event => {
  const target = event.target.closest('[data-view-target]');
  if (!target) return;
  navigateTo(target.dataset.viewTarget);
});

$('#execution-status-filter').addEventListener('change', event => {
  state.ui.executionStatus = event.target.value;
  renderExecutions();
});
$('#execution-owner-filter').addEventListener('change', event => {
  state.ui.executionOwner = event.target.value;
  renderExecutions();
});
$('#execution-search').addEventListener('input', event => {
  state.ui.executionSearch = event.target.value;
  renderExecutions();
});
$('#refresh-button').addEventListener('click', refreshAll);
window.addEventListener('hashchange', () => activateView(location.hash.slice(1) || 'overview'));
document.addEventListener('visibilitychange', () => {
  if (!document.hidden) refreshAll();
});

activateView(location.hash.slice(1) || 'overview');
refreshAll();
setInterval(() => {
  if (!document.hidden) refreshAll();
}, 5000);
