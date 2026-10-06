/* PM Terminal — client application.
 *
 * Three things this file is deliberate about:
 *
 * 1. Per-panel isolation. Every panel fetches its own endpoints and renders its
 *    own error. One dead endpoint used to blank the entire page; now it degrades
 *    a single card and the operator still sees everything else.
 *
 * 2. The operator token never lives in the page. Browsing is unauthenticated.
 *    Actions that move capital or change risk posture require a bearer token the
 *    operator pastes in, held in sessionStorage so it dies with the tab and is
 *    never written to disk or to localStorage. The token is never rendered, only
 *    reported as present/absent.
 *
 * 3. Nothing here decides to trade. The order ticket submits what the operator
 *    typed and reports what the server said. All approval, kill-switch and
 *    execution authority stays server-side.
 */
'use strict';

const API = {
  // ── read-only: safe to call without a token ──
  health: '/health',
  ready: '/ready',
  summary: '/portfolio/summary',
  equity: '/equity-summary',
  positions: '/positions',
  accounts: '/accounts',
  executionStatus: '/execution/status',
  brackets: '/execution/brackets',
  approvals: '/approvals',
  watchlist: '/market/watchlist?limit=30',
  universe: '/market/universe',
  // Built per request from state.symbol / state.granularity (see candlesPanel).
  // Both are user-controlled, so both go through encodeURIComponent.
  candles: (symbol, granularity) =>
    `/market/candles?symbol=${encodeURIComponent(symbol)}&granularity=${encodeURIComponent(granularity)}&limit=200`,
  regime: '/market/regime',
  crossAsset: '/market/cross-asset-regime',
  intelligence: '/market/intelligence',
  opportunities: '/signals/opportunities',
  signalFeed: '/signals/feed',
  strategyPerf: '/strategies/performance',
  rebalance: '/strategies/rebalance',
  rebalancePresets: '/strategies/rebalance/presets',
  stairstep: '/strategies/stairstep',
  capitalBuckets: '/capital/buckets',
  capitalConfig: '/capital/config',
  bucketPresets: '/capital/bucket-presets',
  washSale: '/optimizer/wash-sale',
  srLevels: '/optimizer/sr-levels',
  performance: '/performance',
  arbOpportunities: '/arbitrage/opportunities',
  arbStatus: '/arbitrage/execution-status',
  divergence: '/crypto-divergence',
  research: '/research/hypotheses',
  backtests: '/backtests/experiments',
  competition: '/competition',
  killSwitch: '/kill-switch',

  // ── mutating: require the operator token ──
  orderSubmit: '/orders/submit',
  approve: (token) => `/approvals/approve/${token}`,
  deny: (token) => `/approvals/deny/${token}`,
  cancelBracket: '/execution/brackets/cancel',
  cancelAllBrackets: '/execution/brackets/cancel-all',
  setKillSwitch: '/kill-switch',
};

const TOKEN_KEY = 'pm.operator.token';
const THEME_KEY = 'pm.theme';

/* Per-request deadline. Generous enough for a cold upstream fetch, short enough
 * that a stalled panel cannot hold the refresh cycle open. */
const REQUEST_TIMEOUT_MS = 20000;

/* The granularity <select> values are seconds, which is meaningless to read on a
 * chart. Label them once here so the option list and the chart agree. */
const GRANULARITY_LABELS = {
  60: '1m', 300: '5m', 900: '15m', 1800: '30m',
  3600: '1H', 21600: '6H', 86400: '1D',
};

const state = {
  view: 'overview',
  symbol: 'BTC-USD',
  granularity: 3600,
  side: 'BUY',
  lastOk: null,
  consecutiveFailures: 0,
  pollMs: 15000,
  timer: null,
};

const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

/* ── token ──────────────────────────────────────────────────────────────── */

const token = {
  get() {
    try { return sessionStorage.getItem(TOKEN_KEY) || ''; } catch (_) { return ''; }
  },
  set(value) {
    try {
      if (value) sessionStorage.setItem(TOKEN_KEY, value);
      else sessionStorage.removeItem(TOKEN_KEY);
    } catch (_) { /* private mode: actions stay locked rather than crashing */ }
    renderTokenState();
  },
  present() { return this.get().length > 0; },
};

/* ── http ───────────────────────────────────────────────────────────────── */

/* Never throws. Returns {ok, status, data, error} so a panel can decide how to
 * degrade without a try/catch at every call site.
 *
 * Every request is bounded by an AbortController. Several endpoints here are
 * slow rather than broken — /market/watchlist does a live Coinbase pair
 * discovery and can take tens of seconds — and without a deadline one of them
 * stalls the whole sequential refresh loop and every other panel keeps showing
 * its previous contents with no indication that anything went wrong. A timeout
 * fails that one panel and lets the rest of the cycle proceed. */
async function request(path, {
  method = 'GET', body, auth = false, timeout = REQUEST_TIMEOUT_MS, accept = null,
} = {}) {
  const headers = {};
  if (body !== undefined) headers['Content-Type'] = 'application/json';
  if (auth) {
    const value = token.get();
    if (!value) return { ok: false, status: 401, data: null, error: 'operator token required' };
    headers.Authorization = `Bearer ${value}`;
  }
  const controller = typeof AbortController === 'function' ? new AbortController() : null;
  const timer = controller ? setTimeout(() => controller.abort(), timeout) : null;
  let response;
  try {
    response = await fetch(path, {
      method,
      headers,
      body: body === undefined ? undefined : JSON.stringify(body),
      signal: controller ? controller.signal : undefined,
    });
  } catch (err) {
    const aborted = err && err.name === 'AbortError';
    return {
      ok: false,
      status: 0,
      data: null,
      // An abort is a timeout, not an unreachable server. Keeping these distinct
      // matters: "unreachable" tells an operator to go and restart something,
      // when the truth is one slow upstream call.
      error: aborted ? `timed out after ${Math.round(timeout / 1000)}s` : String(err),
    };
  } finally {
    if (timer) clearTimeout(timer);
  }
  let data = null;
  try { data = await response.json(); } catch (_) { /* empty or non-JSON body */ }
  // `accept` lets a caller treat a specific non-2xx code as a real answer rather
  // than a failure. /ready is the case that matters: it answers 503 precisely
  // when the system is not ready, with a `reason` the operator needs. Treating
  // that as an error replaced "not ready: blocked by a safety gate" with
  // "Service health could not load", which is both useless and alarming.
  // `Array.isArray`, not a truthiness test: with accept left as null,
  // `false || (null && ...)` evaluates to null rather than false, so `ok` was not
  // a boolean at all. Falsy, so nothing broke visibly -- but a caller comparing
  // it strictly saw null.
  const ok = response.ok
    || (Array.isArray(accept) && accept.includes(response.status));
  return { ok: Boolean(ok), status: response.status, data, error: data && data.error };
}

const get = (path) => request(path).then((r) => (r.ok ? r.data || {} : null));

/* ── formatting ─────────────────────────────────────────────────────────── */

/* Precision scales with magnitude so a large total and a crypto quantity are
 * both legible in the same column. Exact zero is special-cased: it falls into
 * the smallest bucket and rendered as "0.000000", which reads as a missing
 * value rather than a measured zero. */
function num(value, digits) {
  if (value === null || value === undefined || Number.isNaN(value)) return '--';
  // Normalise -0 first: its toLocaleString is "-0", and a signed-zero P&L cell
  // reads as a real negative number rather than a rounded-to-nothing one.
  const n = Number(value) || 0;
  const abs = Math.abs(n);
  const d = digits !== undefined ? digits
    : n === 0 ? 0
      : abs >= 1000 ? 0 : abs >= 1 ? 2 : abs >= 0.01 ? 4 : 6;
  return n.toLocaleString(undefined, { minimumFractionDigits: d, maximumFractionDigits: d });
}

/* Currency always carries cents. num() scales precision by magnitude, which is
 * right for a bare number but wrong for money: a $85,545.50 mark price rendered
 * as "$85,546" (whole dollars) and a zero balance rendered as "$0.000000"
 * (six decimals, because abs < 0.01 falls through num()'s smallest bucket).
 * Dropping cents on a position's entry price loses the information an operator
 * checks P&L against. Large values get thousands separators, not fewer digits. */
function money(value, { signed = false } = {}) {
  if (value === null || value === undefined || Number.isNaN(value)) return '--';
  const n = Number(value);
  const sign = signed && n > 0 ? '+' : n < 0 ? '-' : '';
  const abs = Math.abs(n);
  return `${sign}$${abs.toLocaleString(undefined, {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  })}`;
}

function pct(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(value)) return '--';
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

function when(iso) {
  if (!iso) return '--';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return String(iso);
  return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function ago(seconds) {
  if (seconds === null || seconds === undefined) return '--';
  const s = Math.max(0, Math.round(Number(seconds)));
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d`;
}

/* Age from an ISO timestamp. Kept separate from ago(), which takes a number of
 * seconds: passing an ISO string to ago() yields NaN and renders "--", which is
 * indistinguishable from a fresh record. */
function ageFrom(iso) {
  if (!iso) return '--';
  const then = Date.parse(iso);
  if (Number.isNaN(then)) return '--';
  return ago((Date.now() - then) / 1000);
}

function esc(value) {
  return String(value === null || value === undefined ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

/* Direction is carried by an arrow as well as colour, so it survives
 * greyscale, colour-blindness and high-contrast mode. */
function dir(value) {
  const n = Number(value);
  if (!Number.isFinite(n) || n === 0) return '<span class="dim">&mdash;</span>';
  const cls = n > 0 ? 'up' : 'down';
  const arrow = n > 0 ? '▲' : '▼';
  return `<span class="${cls}">${arrow} ${money(Math.abs(n))}</span>`;
}

function signedClass(value) {
  const n = Number(value);
  return Number.isFinite(n) && n < 0 ? 'down' : Number.isFinite(n) && n > 0 ? 'up' : 'dim';
}

/* ── toasts & banners ───────────────────────────────────────────────────── */

function toast(message, kind = '') {
  const host = $('#toasts');
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.setAttribute('role', kind === 'bad' ? 'alert' : 'status');
  el.textContent = message;
  host.appendChild(el);
  setTimeout(() => el.remove(), kind === 'bad' ? 8000 : 4500);
}

/* ── panels ─────────────────────────────────────────────────────────────── */

/* A panel is defined once and may appear in more than one view (health is shown
 * on Overview and on System). One spec therefore owns an *id*, and the render
 * target is every element tagged `data-panel="<id>"` — so a duplicated card
 * costs one request, not two, and the two copies cannot drift apart.
 *
 * Each panel renders its own skeleton and its own error. The previous build let
 * a single failing endpoint blank the whole page. */
const panels = [];

/* Every option is forwarded explicitly. This signature went stale once already:
 * `slow` and `accept` were added at the call sites while this line kept the old
 * destructuring list, so both were silently dropped -- the watchlist lost its
 * "this takes a while" note and the health panel went back to rendering /ready's
 * 503 as an error. Silently, because destructuring a missing key is not an error.
 *
 * test_options_are_all_forwarded asserts the forwarding, so the next option added
 * at a call site cannot be dropped here without a test failing. */
function panel(id, {
  title, endpoint, render, poll = true, auth = false, method, body,
  timeout = undefined, slow = false, accept = null,
}) {
  panels.push({
    id, title, endpoint, render, poll, auth, method, body, timeout, slow, accept,
  });
}

const bodiesFor = (id) => $$(`[data-panel="${id}"]`);

/* Panels show a skeleton until their first response. A slow panel says so: the
 * watchlist does a live pair discovery that takes around half a minute cold, and
 * three unlabelled grey bars for that long are indistinguishable from a broken
 * dashboard. `slow` is declared per panel and only changes the copy. */
function showSkeleton(el, { slow = false, label = '' } = {}) {
  if ($('.skeleton', el)) return;
  const note = slow
    ? '<p class="dim loading-note" role="status">Loading\u2026 this reads live Coinbase data on the first request, which can take about half a minute.</p>'
    : '';
  el.innerHTML = `${note}<div class="skeleton" aria-hidden="true"></div>`.repeat(slow ? 3 : 1)
    + `<span class="sr-only">Loading${label ? ` ${esc(label)}` : ''}</span>`;
}

function showPanelError(el, spec, res) {
  const why = res.status === 401
    ? 'This needs the operator token — use Unlock actions.'
    : res.status === 0
      ? 'The dashboard server is unreachable.'
      : `HTTP ${res.status}${res.error ? ` — ${res.error}` : ''}`;
  el.innerHTML = `<div class="panel-error" role="alert"><div>
      <strong>${esc(spec.title)} could not load</strong>${esc(why)}</div></div>`;
}

async function refreshPanel(spec) {
  const targets = bodiesFor(spec.id);
  if (!targets.length) return;
  targets.forEach((el) => showSkeleton(el, { slow: spec.slow, label: spec.title }));
  // endpoint may be a thunk: the chart's URL depends on the selected symbol
  // and granularity, so it cannot be a constant captured at registration.
  const url = typeof spec.endpoint === 'function' ? spec.endpoint() : spec.endpoint;
  const res = await request(url, {
    method: spec.method || 'GET',
    body: spec.body,
    auth: spec.auth,
    timeout: spec.timeout,
    accept: spec.accept,
  });
  if (!res.ok) {
    targets.forEach((el) => showPanelError(el, spec, res));
    return;
  }
  targets.forEach((el) => {
    try {
      spec.render(el, res.data || {});
      // The renderer may have created token-gated buttons. Re-apply gating: a
      // control that is enabled while locked misrepresents what will happen,
      // even though the server would refuse it.
      applyTokenGating(el);
    } catch (err) {
      // A render bug must not take the page down with it.
      showPanelError(el, spec, { status: 0, error: `render failed: ${err}` });
    }
  });
}

/* Panels refresh concurrently, capped. Awaiting them one at a time made the
 * poll period the sum of every panel's latency: /market/watchlist is 7th of 16
 * and takes ~26s cold, so every panel after it sat on its loading skeleton
 * until watchlist finished, and with a 60s ceiling on watchlist the effective
 * poll period was over a minute instead of the configured 15s. A browser run
 * caught this; no unit test could, because each renderer is independently
 * correct.
 *
 * The cap keeps sixteen simultaneous requests off one threaded server without
 * reintroducing the head-of-line blocking: five in flight means a slow panel
 * costs one slot, not the whole cycle. */
const MAX_CONCURRENT_PANELS = 5;

async function refreshAll({ immediate = false } = {}) {
  if (!immediate && document.hidden) return;
  // The header carries liveness, readiness and the kill switch, so it has to
  // ride the same poll as the panels. Polling panels alone left the kill switch
  // showing whatever it was at page load. It is fast and stays sequential so
  // the kill-switch state is settled before any panel renders its actions.
  await refreshHeader();

  const queue = panels.filter((spec) => spec.poll);
  let cursor = 0;
  const worker = async () => {
    while (cursor < queue.length) {
      const spec = queue[cursor];
      cursor += 1;
      await refreshPanel(spec);
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(MAX_CONCURRENT_PANELS, queue.length) }, worker),
  );
  updateRefreshAge();
}

/* ── equity chart ───────────────────────────────────────────────────────── */

function drawEquity(container, points) {
  const width = 720;
  const height = 220;
  const pad = { top: 12, right: 8, bottom: 20, left: 52 };
  if (!Array.isArray(points) || points.length < 2) {
    container.innerHTML = '<p class="empty">No equity history yet.</p>';
    return;
  }
  // Extract first, filter second. Reading p.equity inside the map threw a
  // TypeError on a null entry, which took the whole panel down instead of
  // skipping the bad point — a gap in the curve should cost one point, not the
  // chart.
  const values = points
    .map((p) => Number(
      p === null || p === undefined ? NaN
        : typeof p === 'object' ? (p.equity ?? p.value)
          : p,
    ))
    .filter((v) => Number.isFinite(v));
  if (values.length < 2) {
    container.innerHTML = '<p class="empty">No equity history yet.</p>';
    return;
  }
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const span = hi - lo || 1;
  const x = (i) => pad.left + (i / (values.length - 1)) * (width - pad.left - pad.right);
  const y = (v) => pad.top + (1 - (v - lo) / span) * (height - pad.top - pad.bottom);
  const line = values.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
  const area = `${line} L${x(values.length - 1).toFixed(1)},${height - pad.bottom} L${x(0).toFixed(1)},${height - pad.bottom} Z`;

  const ticks = [hi, lo + span / 2, lo];
  const grid = ticks.map((t) => `<line class="grid-line" x1="${pad.left}" x2="${width - pad.right}"
      y1="${y(t).toFixed(1)}" y2="${y(t).toFixed(1)}"></line>
      <text class="axis-label" x="${pad.left - 6}" y="${(y(t) + 3).toFixed(1)}" text-anchor="end">${num(t, 0)}</text>`).join('');

  container.innerHTML = `<svg class="chart" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"
      role="img" aria-label="Equity curve, ${values.length} points, from ${num(values[0])} to ${num(values[values.length - 1])}">
      <defs><linearGradient id="equityFill" x1="0" y1="0" x2="0" y2="1">
        <stop offset="0%" stop-color="var(--accent)" stop-opacity="0.28"></stop>
        <stop offset="100%" stop-color="var(--accent)" stop-opacity="0"></stop>
      </linearGradient></defs>
      ${grid}
      <path class="area" d="${area}"></path>
      <path class="line" d="${line}"></path>
      <circle class="marker" cx="${x(values.length - 1).toFixed(1)}" cy="${y(values[values.length - 1]).toFixed(1)}" r="3.5"></circle>
    </svg>`;
}

/* ── views ──────────────────────────────────────────────────────────────── */

const VIEWS = [
  ['overview', 'Overview'],
  ['positions', 'Positions'],
  ['approvals', 'Approvals'],
  ['opportunities', 'Opportunities'],
  ['market', 'Market'],
  ['signals', 'Signals'],
  ['strategies', 'Strategies'],
  ['capital', 'Capital'],
  ['research', 'Research'],
  ['system', 'System'],
];

function navigate(view) {
  if (!VIEWS.some(([id]) => id === view)) view = 'overview';
  state.view = view;
  $$('.view').forEach((el) => { el.hidden = el.id !== `view-${view}`; });
  $$('.nav a').forEach((el) => {
    if (el.dataset.view === view) el.setAttribute('aria-current', 'page');
    else el.removeAttribute('aria-current');
  });
  if (location.hash.slice(1) !== view) history.replaceState(null, '', `#${view}`);

  // The chart is excluded from the poll because it is a Coinbase CLI call, but
  // that left it blank on arrival: entering the Market view fetched nothing, and
  // it only appeared once you touched the granularity selector or clicked a
  // watchlist row. Fetch it when its view is actually opened.
  if (view === 'market') {
    const chart = panels.find((p) => p.id === 'candles');
    if (chart) refreshPanel(chart);
  }

  refreshAll({ immediate: true });
}

function renderNav() {
  const groups = [
    ['Trade', ['overview', 'positions', 'approvals', 'opportunities']],
    ['Analyse', ['market', 'signals', 'strategies']],
    ['Operate', ['capital', 'research', 'system']],
  ];
  const nav = $('#nav');
  nav.innerHTML = groups.map(([label, ids]) => `
    <div class="nav-group">
      <h3>${esc(label)}</h3>
      ${ids.map((id) => {
        const name = (VIEWS.find(([v]) => v === id) || [id, id])[1];
        // The badge is aria-hidden: it sits inside the anchor, so without this
        // a screen reader announces the link as "Approvals1". The count is
        // conveyed in the link's aria-label instead.
        return `<a href="#${id}" data-view="${id}" aria-label="${esc(name)}">${esc(name)}`
          + `<span class="count" data-count="${id}" aria-hidden="true" hidden></span></a>`;
      }).join('')}
    </div>`).join('');
  nav.addEventListener('click', (event) => {
    const link = event.target.closest('a[data-view]');
    if (!link) return;
    event.preventDefault();
    navigate(link.dataset.view);
  });
}

/* Badge on a nav link. The number itself is aria-hidden so it does not run into
 * the link's accessible name; the count is announced through the link's
 * aria-label instead, so "Approvals" with 3 pending reads as "Approvals, 3
 * pending" rather than "Approvals3". */
function setCount(view, value, alert = false, noun = 'pending') {
  const el = $(`[data-count="${view}"]`);
  if (!el) return;
  const link = el.closest('a[data-view]');
  const base = link && VIEWS.find(([id]) => id === view);
  const name = base ? base[1] : view;
  if (value === null || value === undefined || value === 0) {
    el.hidden = true;
    if (link) link.setAttribute('aria-label', name);
    return;
  }
  el.hidden = false;
  const shown = value > 99 ? '99+' : String(value);
  el.textContent = shown;
  el.classList.toggle('alert', Boolean(alert));
  if (link) {
    link.setAttribute('aria-label', `${name}, ${shown} ${noun}${value === 1 ? '' : 's'}`);
  }
}

/* ── theme ──────────────────────────────────────────────────────────────── */

function applyTheme(theme) {
  document.documentElement.setAttribute('data-theme', theme);
  try { localStorage.setItem(THEME_KEY, theme); } catch (_) { /* ignore */ }
  const btn = $('#theme-btn');
  if (btn) btn.textContent = { dark: '◐ Dark', light: '◑ Light', contrast: '◈ Contrast' }[theme] || '◐ Dark';
}

function cycleTheme() {
  const order = ['dark', 'light', 'contrast'];
  const current = document.documentElement.getAttribute('data-theme') || 'dark';
  applyTheme(order[(order.indexOf(current) + 1) % order.length]);
}

/* ── token UI ───────────────────────────────────────────────────────────── */

/* Applied to every [data-needs-token] control, whenever the DOM may have gained
 * new ones.
 *
 * This has to be re-run after every panel render, not only when the token
 * changes. Panels rebuild their tables each poll, so the Approve/Deny buttons
 * are new elements every cycle: gating them only at unlock time left those
 * buttons enabled and unexplained while locked. The server still refused them
 * with 401, so no capital moved — but the page fail-opened and told the
 * operator a control was available when it could not work. */
function applyTokenGating(root = document) {
  const present = token.present();
  const why = 'Requires the operator token — use Unlock actions';
  $$('[data-needs-token]', root).forEach((el) => {
    el.disabled = !present;
    el.title = present ? '' : why;
  });
}

function renderTokenState() {
  const bar = $('#token-bar');
  if (!bar) return;
  const present = token.present();
  bar.classList.toggle('unlocked', present);
  $('#token-state').textContent = present
    ? 'Actions unlocked for this tab'
    : 'Read-only — actions need the operator token';
  $('#token-btn').textContent = present ? 'Lock actions' : 'Unlock actions';
  $('#token-btn').className = present ? 'btn sm' : 'btn sm primary';
  applyTokenGating();
}

async function promptToken() {
  if (token.present()) { token.set(''); toast('Actions locked', ''); return; }
  const value = window.prompt(
    'Paste the dashboard operator token.\n\n'
    + 'Stored in sessionStorage only: it is cleared when this tab closes and is '
    + 'never written to disk. Read-only views do not need it.\n\n'
    + 'Get it with:  cat ~/.config/portfolio-management/dashboard_token',
  );
  if (value === null) return;
  if (!value.trim()) return;
  token.set(value.trim());
  toast('Actions unlocked for this tab', 'ok');
  refreshAll({ immediate: true });
}

/* ── header ─────────────────────────────────────────────────────────────── */

function setHeader(field, value, cls = '') {
  const el = $(`#h-${field}`);
  if (el) { el.innerHTML = value; el.className = `v mono ${cls}`; }
}

async function refreshHeader() {
  const res = await request(API.health);
  const dot = $('#live-dot');
  const label = $('#status-text');
  if (res.ok && res.data) {
    const status = String(res.data.status || res.data.state || '').toLowerCase();
    const healthy = status === 'healthy' || status === 'ok';
    dot.className = `dot ${healthy ? 'live' : 'bad'}`;
    label.textContent = healthy ? 'healthy' : (res.data.detail || status || 'unknown');
    state.consecutiveFailures = 0;
    state.lastOk = Date.now();
  } else {
    dot.className = 'dot bad';
    label.textContent = res.status === 0 ? 'unreachable' : `HTTP ${res.status}`;
    state.consecutiveFailures += 1;
  }

  const ready = await get(API.ready);
  const rdot = $('#ready-dot');
  const rlabel = $('#ready-label');
  if (ready) {
    const ok = ready.ready !== false;
    rdot.className = `dot ${ok ? 'live' : 'warn'}`;
    rlabel.textContent = ok ? 'ready' : (ready.reason || 'not ready');
    rlabel.title = ready.halted_by_operator ? 'halted by operator (kill switch)' : (ready.reason || '');
  }

  const [summary, equity] = await Promise.all([get(API.summary), get(API.equity)]);
  // Each source names the same fields differently, so read them by preference
  // rather than assuming one shape. `firstDefined` returns the first argument
  // that is actually present (0 and '' count as present; null/undefined do not).
  const firstDefined = (...values) => values.find((v) => v !== null && v !== undefined);

  const total = firstDefined(summary && summary.total_value,
    summary && summary.equity,
    equity && equity.total_value,
    equity && equity.equity);
  setHeader('equity', money(total), 'lg');

  const pnl = firstDefined(equity && equity.daily_pnl, equity && equity.pnl,
    equity && equity.day_pnl, summary && summary.daily_pnl, summary && summary.pnl);
  setHeader('pnl', money(pnl, { signed: true }),
    pnl > 0 ? 'up' : pnl < 0 ? 'down' : '');

  const dd = firstDefined(equity && equity.drawdown, summary && summary.drawdown);
  setHeader('dd', pct(dd), dd > 0.05 ? 'down' : '');

  const pos = firstDefined(summary && summary.position_count,
    summary && (summary.positions || []).length);
  setHeader('pos', pos ?? '--');

  // Kill switch state comes from /ready, not from GET /kill-switch: the route is
  // POST-only, so a GET returns 404 and a dashboard reading it looks broken while
  // the switch is perfectly fine. /ready already reports kill_switch_active.
  const engaged = !!(ready && ready.kill_switch_active);
  const btn = $('#ks-btn');
  btn.textContent = engaged ? 'KILL SWITCH ENGAGED' : 'Kill switch off';
  btn.className = `btn sm ${engaged ? 'danger' : ''}`;
  btn.setAttribute('aria-pressed', engaged ? 'true' : 'false');
  btn.dataset.needsToken = '';
  // The click handler reads this back to decide whether to engage or release.
  btn.dataset.engaged = engaged ? 'true' : 'false';

  renderBanner(ready);
  const sub = $('#mode-sub');
  if (sub) sub.textContent = (res.data && (res.data.mode || res.data.mode_sub)) || 'portfolio management';
}

/* Readiness and the kill switch are the two states that change what an
 * operator is allowed to do, so they get a persistent banner rather than only a
 * coloured dot. An operator halt is deliberately styled as a halt, not a fault:
 * pulling the kill switch is correct behaviour, not something to page on. */
function renderBanner(ready) {
  const slot = $('#banner-slot');
  if (!slot) return;
  const ks = $('#ks-btn');
  const engaged = ks && ks.dataset.engaged === 'true';
  const notReady = ready && ready.ready === false;

  if (engaged) {
    slot.innerHTML = `<div class="banner danger"><span class="dot bad"></span>
      <span><strong>Kill switch engaged.</strong> All trading is halted.
      ${esc(ready && ready.reason ? ready.reason : 'No orders will be released.')}</span></div>`;
  } else if (notReady) {
    slot.innerHTML = `<div class="banner warn"><span class="dot warn"></span>
      <span><strong>Not ready.</strong> ${esc(ready.reason || 'The system reports it should not be trading.')}</span></div>`;
  } else {
    slot.innerHTML = '';
  }
}

function updateRefreshAge() {
  const el = $('#refresh-age');
  if (!el || !state.lastOk) return;
  el.textContent = `updated ${when(new Date(state.lastOk).toISOString())}`;
}

/* ── panels: registration ───────────────────────────────────────────────── */

function registerPanels() {
  panel('health', {
    title: 'Service health', endpoint: API.ready,
    // /ready answers 503 when the system should not be trading, and that payload
    // is the whole point of the panel. A 503 here is information, not a fault.
    accept: [503],
    render(el, data) {
      // RUNNING is the healthy state; BLOCKED means a safety gate refused to
      // start the child and it will NOT come back on its own. Styling BLOCKED as
      // merely "not green" would understate that.
      const statePill = (state) => {
        const cls = state === 'RUNNING' ? 'positive'
          : state === 'BLOCKED' ? 'warn'
            : 'negative';
        return `<span class="pill ${cls}">${esc(state || 'UNKNOWN')}</span>`;
      };
      const children = Array.isArray(data.children) ? data.children : [];
      const blocked = Array.isArray(data.blocked) ? data.blocked : [];
      const degraded = Array.isArray(data.degraded) ? data.degraded : [];

      const flags = [];
      if (data.kill_switch_active) {
        flags.push('<span class="pill danger">kill switch engaged &mdash; intentional halt</span>');
      }
      if (blocked.length) {
        flags.push(`<span class="pill warn">blocked by a safety gate: ${esc(blocked.join(', '))}</span>`);
      }
      if (degraded.length) {
        flags.push(`<span class="pill warn">degraded: ${esc(degraded.join(', '))}</span>`);
      }

      const summary = `
        <p style="margin:0 0 var(--sp-3)">
          ${data.ready ? '<span class="pill positive">ready</span>' : '<span class="pill warn">not ready</span>'}
          ${flags.join(' ')}
        </p>`;

      if (!children.length) {
        el.innerHTML = summary
          + `<p class="empty">${esc(data.reason || 'No supervised children reported.')}</p>
             <p class="dim" style="text-align:left">This comes from <code>/ready</code>, which asks
             "should the system be trading" &mdash; separately from <code>/health</code>, which only asks
             whether this process is alive.</p>`;
        return;
      }

      el.innerHTML = `${summary}
        <table><thead><tr><th>Child</th><th>State</th><th class="num">PID</th></tr></thead>
        <tbody>${children.map((c) => `<tr>
          <td>${esc(c.name)}</td>
          <td>${statePill(c.state)}</td>
          <td class="num dim">${esc(c.pid ?? '--')}</td>
        </tr>`).join('')}</tbody></table>
        <p class="dim" style="text-align:left">
          supervisor ${data.supervisor_running ? 'running' : '<strong>not running</strong>'}
          &middot; state age ${ago(data.supervisor_state_age_sec)}
          ${data.halted_by_operator ? ' &middot; <strong>halted by operator</strong>' : ''}
        </p>
        ${blocked.length ? `<p class="dim" style="text-align:left">A BLOCKED child is a safety gate
          working correctly. It will not restart itself; an operator must resolve the cause.</p>` : ''}`;
    },
  });

  panel('equity', {
    title: 'Equity', endpoint: API.equity,
    render(el, data) {
      const points = data.equity_curve || data.curve || data.history || [];
      drawEquity($('#equity-chart'), points);
      const rows = [
        ['Realised P&L', money(data.realized_pnl, { signed: true }), signedClass(data.realized_pnl)],
        ['Unrealised P&L', money(data.unrealized_pnl, { signed: true }), signedClass(data.unrealized_pnl)],
        ['Drawdown', pct(data.drawdown), ''],
        ['Peak equity', money(data.peak_equity), ''],
      ];
      $('#equity-stats').innerHTML = rows.map(([k, v, c]) =>
        `<tr><td class="dim">${k}</td><td class="num ${c}">${v}</td></tr>`).join('');
    },
  });

  panel('positions', {
    title: 'Open positions', endpoint: API.positions,
    render(el, data) {
      // Real fields: instrument/symbol, side|classification (always "LONG"),
      // quantity, entry_price_usd, current_price_usd, unrealized_pnl_usd,
      // unrealized_pnl_pct, status, venue. The old version read p.qty,
      // p.entry_price, p.mark_price, p.pnl and p.strategy — none of which exist,
      // so every column rendered blank or "--".
      const rows = Array.isArray(data.positions) ? data.positions : [];
      setCount('positions', rows.length);
      const totals = Number.isFinite(data.total_unrealized_pnl_usd)
        ? `<p class="dim" style="margin:var(--sp-3) 0 0">
             ${rows.length} position${rows.length === 1 ? '' : 's'} ·
             unrealised <span class="${signedClass(data.total_unrealized_pnl_usd)}">${money(data.total_unrealized_pnl_usd, { signed: true })}</span>
             (${num(data.total_unrealized_pnl_pct, 2)}%)</p>`
        : '';
      if (!rows.length) {
        el.innerHTML = '<p class="empty">No open positions.</p>' + totals;
        return;
      }
      el.innerHTML = `
        <div class="table-scroll"><table>
          <thead><tr><th>Instrument</th><th>Side</th><th class="num">Qty</th>
            <th class="num">Entry</th><th class="num">Mark</th><th class="num">P&amp;L</th>
            <th class="num">%</th><th>Venue</th></tr></thead>
          <tbody>${rows.map((p) => `<tr>
            <td class="mono">${esc(p.instrument || p.symbol || '--')}</td>
            <td><span class="pill positive">${esc(p.side || p.classification || '--')}</span></td>
            <td class="num">${num(p.quantity)}</td>
            <td class="num">${money(p.entry_price_usd)}</td>
            <td class="num">${money(p.current_price_usd)}</td>
            <td class="num ${signedClass(p.unrealized_pnl_usd)}">${money(p.unrealized_pnl_usd, { signed: true })}</td>
            <td class="num ${signedClass(p.unrealized_pnl_pct)}">${num(p.unrealized_pnl_pct, 2)}%</td>
            <td class="dim">${esc(p.venue || '--')}</td>
          </tr>`).join('')}</tbody></table></div>${totals}`;
    },
  });

  panel('brackets', {
    title: 'Brackets', endpoint: API.brackets,
    render(el, data) {
      // /execution/brackets returns {"brackets": {<bracket_id>: {...}}}. The
      // map's key IS the bracket id that /execution/brackets/cancel expects, and
      // it is the only place that id appears — the record does not repeat it.
      // Iterating the object as an array rendered "No protective brackets" even
      // while brackets were live, and the Cancel button sent an empty id.
      const map = (data.brackets && typeof data.brackets === 'object') ? data.brackets : {};
      const rows = Object.entries(map).map(([id, b]) => ({ id, ...b }));
      if (!rows.length) {
        el.innerHTML = '<p class="empty">No protective brackets.</p>';
        return;
      }
      el.innerHTML = `
        <div class="table-scroll"><table>
          <thead><tr><th>Product</th><th class="num">Stop</th><th class="num">Target</th>
            <th class="num">Entry</th><th>State</th><th></th></tr></thead>
          <tbody>${rows.map((b) => `<tr>
            <td class="mono">${esc(b.product_id || b.symbol || '--')}</td>
            <td class="num">${money(b.stop_price ?? b.stop)}</td>
            <td class="num">${money(b.target_price ?? b.target)}</td>
            <td class="num dim">${money(b.entry_price ?? b.entry)}</td>
            <td><span class="pill">${esc(b.state || b.status || 'active')}</span></td>
            <td class="num"><button class="btn sm danger" data-cancel-bracket="${esc(b.id)}"
              data-needs-token>Cancel</button></td>
          </tr>`).join('')}</tbody></table></div>
        <p class="dim" style="text-align:left">Cancelling removes the stop and target, leaving the
        position unprotected.</p>`;
    },
  });

  panel('approvals', {
    title: 'Pending approvals', endpoint: API.approvals,
    render(el, data) {
      // /approvals returns {approvals, summary}. Each row carries token,
      // strategy_id, instrument, quantity_usd, expected_fee, risk_score, status,
      // auto_approved and created_at — there is no side/size/reason field, so
      // reading those rendered an empty table with -- in every column.
      const rows = Array.isArray(data.approvals) ? data.approvals : [];
      const summary = data.summary || {};
      const pending = rows.filter((a) => a.status === 'pending' && !a.auto_approved);
      setCount('approvals', pending.length, pending.length > 0);

      const counts = [
        ['pending', summary.pending_count],
        ['approved', summary.approved_count],
        ['rejected', summary.rejected_count],
      ].filter(([, n]) => Number.isFinite(n));

      const head = counts.length
        ? `<p class="dim" style="margin:0 0 var(--sp-3)">${counts
            .map(([k, n]) => `${esc(k)} <strong class="mono">${n}</strong>`).join(' &middot; ')}</p>`
        : '';

      if (!rows.length) {
        el.innerHTML = head + '<p class="empty">Nothing waiting for approval.</p>';
        return;
      }

      el.innerHTML = `${head}
        <div class="table-scroll"><table>
          <thead><tr>
            <th>Instrument</th><th>Source</th><th class="num">Size</th>
            <th class="num">Fee</th><th class="num">Risk</th><th>Status</th>
            <th class="num">Age</th><th></th>
          </tr></thead>
          <tbody>${rows.map((a) => {
            const id = a.token || a.id || '';
            const pendingRow = a.status === 'pending' && !a.auto_approved;
            return `<tr>
              <td class="mono">${esc(a.instrument || '--')}</td>
              <td class="dim">${esc(a.strategy_id || '--')}</td>
              <td class="num">${money(a.quantity_usd)}</td>
              <td class="num dim">${money(a.expected_fee)}</td>
              <td class="num">${num(a.risk_score, 2)}</td>
              <td><span class="pill ${pendingRow ? 'warn' : a.status === 'approved' ? 'positive' : 'dim'}">
                ${esc(a.auto_approved ? 'auto' : (a.status || 'pending'))}</span></td>
              <td class="num dim">${ageFrom(a.created_at)}</td>
              <td class="num">${pendingRow
                ? `<button class="btn sm" data-approve="${esc(id)}" data-needs-token>Approve</button>
                   <button class="btn sm danger" data-deny="${esc(id)}" data-needs-token>Deny</button>`
                : '<span class="dim">&mdash;</span>'}</td>
            </tr>`;
          }).join('')}</tbody></table></div>
        <p class="dim" style="text-align:left">Approving releases a real order on the next
        optimizer tick. Use <kbd>Unlock actions</kbd> first.</p>`;
    },
  });

  panel('opportunities', {
    title: 'Opportunities', endpoint: API.opportunities,
    render(el, data) {
      // The endpoint returns {status, source, queue, signals, total_signals,
      // buy_signals, sell_signals, quality_score, new_listing_signals}. There is
      // no `opportunities` key, so the old `data.opportunities || data` fell
      // through to the whole envelope object, hit the Array.isArray guard and
      // reported "No opportunities" even when the queue was full.
      const list = (Array.isArray(data.signals) ? data.signals
        : Array.isArray(data.queue) ? data.queue : []).slice(0, 100);
      setCount('opportunities', Number.isFinite(data.total_signals)
        ? data.total_signals : list.length);
      const head = `
        <p class="dim" style="margin:0 0 var(--sp-3)">
          <span class="pill">${esc(data.status || 'unknown')}</span>
          source <strong>${esc(data.source || '--')}</strong>
          ${Number.isFinite(data.total_signals) ? `&middot; ${data.total_signals} total` : ''}
          ${Number.isFinite(data.quality_score) ? `&middot; quality ${num(data.quality_score, 2)}` : ''}
        </p>`;
      if (!list.length) {
        el.innerHTML = head + '<p class="empty">No opportunities.</p>';
        return;
      }
      el.innerHTML = `${head}
        <div class="table-scroll"><table>
          <thead><tr><th class="num">Score</th><th>Product</th><th>Side</th>
            <th class="num">Size</th><th class="num">Confidence</th><th>Type</th>
            <th>Reason</th></tr></thead>
          <tbody>${list.map((o) => {
            const side = String(o.side || o.action || '').toUpperCase();
            return `<tr>
              <td class="num">${num(o.opportunity_score ?? o.priority)}</td>
              <td class="mono">${esc(o.product_id || o.symbol || o.currency || '--')}</td>
              <td><span class="pill ${side === 'BUY' ? 'positive' : 'negative'}">${esc(side || '--')}</span></td>
              <td class="num">${money(o.size_usd)}</td>
              <td class="num">${pct(o.final_confidence ?? o.confidence)}</td>
              <td class="dim">${esc(o.trade_style || o.opp_type || '--')}</td>
              <td class="dim">${esc(o.signal_reason || o.reason || '')}</td>
            </tr>`;
          }).join('')}</tbody></table></div>`;
    },
  });

  panel('watchlist', {
    title: 'Watchlist', endpoint: API.watchlist,
    slow: true,
    // This endpoint does a live pair discovery plus a batch candle fetch on a
    // cold cache, which takes far longer than the default deadline. The server
    // caches for 30s, so a longer client timeout costs nothing after the first
    // call; without it the panel permanently showed "timed out".
    timeout: 60000,
    render(el, data) {
      // Actual shape: {watchlist: [{symbol, base, last, change_pct, spark,
      // regime}], offline}. `change_pct` is already a percentage (2.5 means
      // +2.5%), and there is no volume field — the previous version read
      // change_24h/volume_24h, so both columns rendered empty.
      const list = Array.isArray(data.watchlist) ? data.watchlist : [];
      if (!list.length) {
        el.innerHTML = '<p class="empty">No watchlist data.</p>';
        return;
      }
      const offline = data.offline
        ? '<p class="dim" style="margin:0 0 var(--sp-2)">Serving from the durable cache; the live feed did not respond.</p>'
        : '';
      el.innerHTML = `${offline}
        <div class="table-scroll"><table>
          <thead><tr><th>Product</th><th class="num">Last</th>
            <th class="num">30 bars</th><th>Regime</th><th></th></tr></thead>
          <tbody>${list.map((w) => `<tr>
            <td class="mono">${esc(w.symbol || '--')}</td>
            <td class="num">${money(w.last)}</td>
            <td class="num ${signedClass(w.change_pct)}">
              ${w.change_pct === null || w.change_pct === undefined ? '--'
                : `${w.change_pct > 0 ? '+' : ''}${num(w.change_pct, 2)}%`}</td>
            <td><span class="pill">${esc(w.regime || '--')}</span></td>
            <td class="num"><button class="btn sm" data-symbol="${esc(w.symbol)}">Chart</button></td>
          </tr>`).join('')}</tbody></table></div>`;
    },
  });

  panel('candles', {
    title: 'Chart',
    // Depends on the selected symbol and granularity, so the URL is computed
    // per refresh rather than being a fixed constant. Excluded from the normal
    // poll: it is a coinbase CLI call, and re-pulling it every 15s for a chart
    // nobody is looking at is not worth the latency.
    endpoint: () => API.candles(state.symbol, state.granularity),
    poll: false,
    render(el, data) {
      // Rows are {t,o,h,l,c,v} — the close is `c`, not `close`. The REST feed hands
      // back positional tuples and the server maps them to these short keys, so
      // reading `.close` yields undefined for every bar.
      // The feed returns oldest-first (verified: t ascends across the response).
      // Sorting by t rather than reversing is deliberate — it also corrects a
      // response that arrives out of order from the durable-cache fallback, and
      // a reversed series silently draws a right-to-left chart.
      const list = (Array.isArray(data.candles) ? data.candles : [])
        .map((row) => ({ t: Number(row.t), close: Number(row.c ?? row.close) }))
        .filter((row) => Number.isFinite(row.close) && Number.isFinite(row.t))
        .sort((a, b) => a.t - b.t);
      const host = el;
      if (list.length < 2) {
        host.innerHTML = `<p class="empty">No candles returned for ${esc(state.symbol)}
          at ${esc(String(state.granularity))}s.</p>`;
        return;
      }
      const closes = list.map((row) => row.close);
      const hi = Math.max(...closes);
      const lo = Math.min(...closes);
      const span = hi - lo || 1;
      const w = 720;
      const h = 200;
      const pad = { top: 10, right: 8, bottom: 18, left: 54 };
      const x = (i) => pad.left + (i / (closes.length - 1)) * (w - pad.left - pad.right);
      const y = (v) => pad.top + (1 - (v - lo) / span) * (h - pad.top - pad.bottom);
      const path = closes.map((v, i) => `${i ? 'L' : 'M'}${x(i).toFixed(1)},${y(v).toFixed(1)}`).join(' ');
      const grid = [hi, lo + span / 2, lo].map((t) =>
        `<line class="grid-line" x1="${pad.left}" x2="${w - pad.right}" y1="${y(t).toFixed(1)}" y2="${y(t).toFixed(1)}"></line>
         <text class="axis-label" x="${pad.left - 6}" y="${(y(t) + 3).toFixed(1)}" text-anchor="end">${num(t, 0)}</text>`).join('');
      const last = closes[closes.length - 1];
      const first = closes[0];
      const changePct = first ? ((last - first) / first) * 100 : 0;
      const granLabel = GRANULARITY_LABELS[state.granularity] || `${state.granularity}s`;
      const title = $('#candles-title');
      if (title) title.textContent = `${state.symbol} · ${granLabel}`;
      host.innerHTML = `
        <p class="dim" style="margin:0 0 var(--sp-2)">
          <strong class="mono">${esc(state.symbol)}</strong>
          <span class="pill">${esc(granLabel)}</span>
          ${money(first)} &rarr; ${money(last)}
          <span class="${signedClass(changePct)}">${changePct > 0 ? '+' : ''}${num(changePct, 2)}%</span></p>
        <svg class="chart" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none" role="img"
             aria-label="${esc(state.symbol)} close over the last ${list.length} ${esc(granLabel)} bars">
          ${grid}<path class="line" d="${path}"></path>
          <circle class="marker" cx="${x(closes.length - 1).toFixed(1)}" cy="${y(last).toFixed(1)}" r="3.5"></circle>
        </svg>`;
    },
  });

  panel('regime', {
    title: 'Market regime', endpoint: API.regime,
    render(el, data) {
      // Shape: {current_regime:{state, confidence_score, volatility_score,
      // liquidity_score, avg_spread_bps}, sentiment:{bullish_pct, bearish_pct,
      // total_signals}, symbols_tracked}. The old version read data.regime and
      // data.volatility, which are one level too shallow, so every row was "--".
      const r = data.current_regime || {};
      const sent = data.sentiment || {};
      const stateName = r.state || '--';
      const rows = [
        ['Regime', stateName],
        ['Confidence', pct(r.confidence_score)],
        ['Volatility score', num(r.volatility_score, 0)],
        ['Liquidity score', num(r.liquidity_score, 0)],
        ['Avg spread', `${num(r.avg_spread_bps, 1)} bps`],
        ['Bullish', `${num(sent.bullish_pct, 0)}%`],
        ['Bearish', `${num(sent.bearish_pct, 0)}%`],
        ['Symbols tracked', num(data.symbols_tracked, 0)],
      ];
      el.innerHTML = `
        <p style="margin:0 0 var(--sp-3)"><span class="pill">${esc(stateName)}</span></p>
        <table><tbody>${rows.map(([k, v]) =>
          `<tr><td class="dim">${esc(k)}</td><td class="num">${esc(v)}</td></tr>`).join('')}</tbody></table>`;
    },
  });

  panel('signal-feed', {
    title: 'Signal feed', endpoint: API.signalFeed,
    render(el, data) {
      // Real fields per signal: side/action, symbol/product_id/instrument,
      // strategy_name/strategy, confidence/final_confidence,
      // opportunity_score/priority, size_usd, signal_reason/reason,
      // trade_style, updated_at/ts. The old version read s.ts and s.strategy,
      // which are absent, so the Time and Strategy columns were always empty.
      const list = (Array.isArray(data.signals) ? data.signals
        : Array.isArray(data.queue) ? data.queue : []).slice(0, 100);
      const head = `
        <p class="dim" style="margin:0 0 var(--sp-3)">
          <span class="pill">${esc(data.status || 'unknown')}</span>
          source <strong>${esc(data.source || '--')}</strong>
          ${Number.isFinite(data.total_signals) ? `&middot; ${data.total_signals} signals` : ''}
          ${Number.isFinite(data.buy_signals) ? `&middot; ${data.buy_signals} buy` : ''}
          ${Number.isFinite(data.sell_signals) ? `&middot; ${data.sell_signals} sell` : ''}
        </p>`;
      if (!list.length) {
        el.innerHTML = head + '<p class="empty">No recent signals.</p>';
        return;
      }
      const sideOf = (sig) => String(sig.side || sig.action || '').toUpperCase();
      el.innerHTML = `${head}
        <div class="table-scroll"><table>
          <thead><tr><th class="num">Score</th><th>Symbol</th><th>Side</th>
            <th>Strategy</th><th class="num">Size</th><th class="num">Confidence</th>
            <th>Reason</th></tr></thead>
          <tbody>${list.map((sig) => {
            const side = sideOf(sig);
            const score = sig.opportunity_score ?? sig.priority ?? sig.final_confidence;
            const conf = sig.final_confidence ?? sig.confidence;
            return `<tr>
              <td class="num">${num(score)}</td>
              <td class="mono">${esc(sig.symbol || sig.product_id || sig.instrument || '--')}</td>
              <td><span class="pill ${side === 'BUY' ? 'positive' : 'negative'}">${esc(side || '--')}</span></td>
              <td class="dim">${esc(sig.strategy_name || sig.strategy || '--')}</td>
              <td class="num">${money(sig.size_usd)}</td>
              <td class="num">${pct(conf)}</td>
              <td class="dim">${esc(sig.signal_reason || sig.reason || '')}</td>
            </tr>`;
          }).join('')}</tbody></table></div>`;
    },
  });

  panel('strategy-perf', {
    title: 'Strategy performance', endpoint: API.strategyPerf,
    render(el, data) {
      // Rows are {name, strategy_id, status, win_rate, total_signals,
      // avg_confidence}. There are no trades/pnl/disabled fields — reading them
      // gave a column of zeros next to an "active" pill for everything.
      const list = Array.isArray(data.strategies) ? data.strategies : [];
      if (!list.length) {
        el.innerHTML = '<p class="empty">No strategy results.</p>';
        return;
      }
      el.innerHTML = `
        <div class="table-scroll"><table>
          <thead><tr><th>Strategy</th><th>Status</th><th class="num">Signals</th>
            <th class="num">Win rate</th><th class="num">Avg confidence</th></tr></thead>
          <tbody>${list.map((s) => `<tr>
            <td class="mono">${esc(s.name || s.strategy_id || '--')}</td>
            <td><span class="pill ${s.status === 'active' ? 'positive' : 'dim'}">${esc(s.status || 'unknown')}</span></td>
            <td class="num">${num(s.total_signals, 0)}</td>
            <td class="num">${pct(s.win_rate)}</td>
            <td class="num">${pct(s.avg_confidence)}</td>
          </tr>`).join('')}</tbody></table></div>`;
    },
  });

  panel('capital', {
    title: 'Capital buckets', endpoint: API.capitalBuckets, auth: true,
    render(el, data) {
      const rows = data.buckets || data || [];
      const list = Array.isArray(rows) ? rows : [];
      if (!list.length) {
        el.innerHTML = '<p class="empty">No bucket configuration.</p>';
        return;
      }
      el.innerHTML = `
        <table><thead><tr><th>Bucket</th><th class="num">Target</th><th class="num">Value</th><th>Drift</th></tr></thead>
        <tbody>${list.map((b) => `<tr>
          <td>${esc(b.bucket_id || b.name)}</td>
          <td class="num">${money(b.target_usd ?? b.target)}</td>
          <td class="num">${money(b.value_usd ?? b.value)}</td>
          <td class="num ${Math.abs(b.drift_pct || 0) > 5 ? 'down' : 'dim'}">${pct(b.drift_pct)}</td>
        </tr>`).join('')}</tbody></table>`;
    },
  });

  panel('wash-sale', {
    title: 'Wash-sale cooldown', endpoint: API.washSale,
    render(el, data) {
      // cooldowns is an object keyed by base asset with a seconds-remaining
      // value, not a list of records. The old version treated it as an array,
      // so the panel always claimed there were no cooldowns — the exact moment
      // it matters most is when a cooldown is active.
      const map = data.cooldowns && typeof data.cooldowns === 'object'
        ? data.cooldowns : {};
      const entries = Object.entries(map).filter(([, v]) => Number(v) > 0)
        .sort((a, b) => Number(b[1]) - Number(a[1]));
      if (!entries.length) {
        el.innerHTML = `<p class="empty">No active cooldowns.</p>${
          data.updated_at ? `<p class="dim" style="text-align:left">updated ${esc(String(data.updated_at))}</p>` : ''}`;
        return;
      }
      el.innerHTML = `
        <table><thead><tr><th>Asset</th><th class="num">Remaining</th></tr></thead>
        <tbody>${entries.map(([asset, secs]) =>
          `<tr><td class="mono">${esc(asset)}</td><td class="num">${ago(secs)}</td></tr>`).join('')}</tbody></table>`;
    },
  });

  panel('research', {
    title: 'Research hypotheses', endpoint: API.research,
    render(el, data) {
      const rows = data.hypotheses || data || [];
      const list = Array.isArray(rows) ? rows : [];
      if (!list.length) {
        el.innerHTML = '<p class="empty">No hypotheses.</p>';
        return;
      }
      // Rows are {name, description, confidence_score, market_state, strategy_type}.
      // The previous version read h.text/h.hypothesis/h.confidence, none of
      // which exist, so every row rendered an empty cell and "--" confidence.
      el.innerHTML = `
        <div class="table-scroll"><table>
          <thead><tr><th>Hypothesis</th><th>Type</th><th>Market</th>
            <th class="num">Confidence</th></tr></thead>
          <tbody>${list.map((h) => `<tr>
            <td><strong>${esc(h.name || '--')}</strong><br>
              <span class="dim">${esc(h.description || '')}</span></td>
            <td><span class="pill">${esc(h.strategy_type || '--')}</span></td>
            <td>${esc(h.market_state || '--')}</td>
            <td class="num">${pct(h.confidence_score)}</td></tr>`).join('')}</tbody></table></div>
        <p class="dim" style="text-align:left">${esc(data.total_hypotheses ?? list.length)} tracked.</p>`;
    },
  });

  panel('backtests', {
    title: 'Backtest experiments', endpoint: API.backtests,
    render(el, data) {
      const rows = data.experiments || data || [];
      const list = Array.isArray(rows) ? rows : [];
      if (!list.length) {
        el.innerHTML = '<p class="empty">No experiments.</p>';
        return;
      }
      // Rows carry {name, strategy, verdict, passed, win_rate, win_rate_pct, sharpe,
      // profit_factor, total_return, n_strategies_tested, ensemble, regime,
      // updated_at}. There is no drawdown or trades column, so those headers
      // were always blank; verdict/passed and the tested-strategy count are the
      // fields that actually discriminate a result.
      const verdictPill = (e) => {
        const v = String(e.verdict || (e.passed ? 'PASS' : 'FAIL')).toUpperCase();
        const cls = e.passed || v === 'PASS' ? 'positive' : 'negative';
        return `<span class="pill ${cls}">${esc(v)}</span>`;
      };
      el.innerHTML = `
        <div class="table-scroll"><table>
          <thead><tr><th>Name</th><th>Verdict</th><th class="num">Win rate</th>
            <th class="num">Sharpe</th><th class="num">Profit factor</th>
            <th class="num">Return</th><th class="num">Strategies</th></tr></thead>
          <tbody>${list.map((e) => `<tr>
            <td>${esc(e.name || e.strategy || '--')}</td>
            <td>${verdictPill(e)}</td>
            <td class="num">${pct(e.win_rate_pct ?? e.win_rate)}</td>
            <td class="num">${num(e.sharpe, 2)}</td>
            <td class="num">${num(e.profit_factor, 2)}</td>
            <td class="num ${signedClass(e.total_return)}">${pct(e.total_return)}</td>
            <td class="num dim">${num(e.n_strategies_tested, 0)}</td></tr>`).join('')}</tbody></table></div>
        <p class="dim" style="text-align:left">${esc(data.count ?? list.length)} experiments.</p>`;
    },
  });

  panel('accounts', {
    title: 'Accounts', endpoint: API.accounts,
    render(el, data) {
      // Flat per-account records: {id, name, display_name, cash, nav,
      // current_balance_usd, status, provider, mode, buying_power}. There is no
      // nested balances[] array, so the old render printed a heading and an
      // empty table for every account.
      const list = Array.isArray(data.accounts) ? data.accounts : [];
      if (!list.length) {
        el.innerHTML = '<p class="empty">No accounts reported.</p>';
        return;
      }
      el.innerHTML = `
        <div class="table-scroll"><table>
          <thead><tr><th>Account</th><th>Provider</th><th>Mode</th>
            <th class="num">Cash</th><th class="num">NAV</th>
            <th class="num">Buying power</th><th>Status</th></tr></thead>
          <tbody>${list.map((a) => `<tr>
            <td class="mono">${esc(a.name || a.display_name || a.id || '--')}</td>
            <td class="dim">${esc(a.provider || '--')}</td>
            <td><span class="pill">${esc(a.mode || '--')}</span></td>
            <td class="num">${money(a.cash ?? a.current_balance_usd)}</td>
            <td class="num">${money(a.nav)}</td>
            <td class="num">${money(a.buying_power ?? a.buyingPower)}</td>
            <td><span class="pill ${a.status === 'active' ? 'positive' : 'dim'}">${esc(a.status || '--')}</span></td>
          </tr>`).join('')}</tbody></table></div>`;
    },
  });

}

/* ── candles endpoint (dynamic) ─────────────────────────────────────────── */

API.candles = () => `/market/candles?symbol=${encodeURIComponent(state.symbol)}`
  + `&granularity=${state.granularity}&limit=200`;

/* ── actions ────────────────────────────────────────────────────────────── */

async function submitOrder(event) {
  event.preventDefault();
  const msg = $('#oe-msg');
  const body = {
    // The endpoint reads `symbol` and `size_usd`; it derives base_qty and the
    // bracket prices itself. Sending product_id/size/stop_price produces a 400
    // "symbol and side(BUY|SELL) required" that looks like a server fault.
    symbol: $('#oe-symbol').value.trim().toUpperCase(),
    side: state.side,
    size_usd: $('#oe-size').value.trim(),
  };
  // stop_pct/target_pct are percentages of the fetched price, not absolute
  // prices, and they default server-side to 3% / 6%.
  const stop = $('#oe-stop').value.trim();
  const target = $('#oe-target').value.trim();
  if (stop) body.stop_pct = stop;
  if (target) body.target_pct = target;
  if (stop && stop > 100) {
    msg.className = 'msg bad';
    msg.textContent = 'Stop must be a percentage of the price, e.g. 3 for 3%.';
    return;
  }
  if (target && target > 100) {
    msg.className = 'msg bad';
    msg.textContent = 'Target must be a percentage of the price, e.g. 6 for 6%.';
    return;
  }

  msg.className = 'msg';
  msg.textContent = 'Submitting…';
  const res = await request(API.orderSubmit, { method: 'POST', body, auth: true });
  if (res.ok && res.data && res.data.ok) {
    msg.className = 'msg ok';
    msg.textContent = `Submitted. Pending approval${res.data.token ? ` · ref ${String(res.data.token).slice(0, 8)}` : ''}.`;
    toast('Order submitted for approval', 'ok');
  } else if (res.status === 401) {
    msg.className = 'msg bad';
    msg.textContent = 'Operator token required — use Unlock actions.';
  } else {
    msg.className = 'msg bad';
    msg.textContent = `Rejected: ${res.error || res.status}`;
  }
}

async function act(path, { method = 'POST', confirm: message, done, body } = {}) {
  if (confirm && !window.confirm(confirm)) return;
  const res = await request(path, { method, body, auth: true });
  if (res.status === 401) {
    toast('Operator token required — use Unlock actions', 'bad');
    return;
  }
  if (res.ok && (!res.data || res.data.ok !== false)) {
    toast(done || 'Done', 'ok');
    refreshAll({ immediate: true });
  } else {
    toast(`Failed: ${res.error || res.status}`, 'bad');
  }
}

function bindActions() {
  $('#oe-form').addEventListener('submit', submitOrder);
  $$('.side-toggle button').forEach((btn) => btn.addEventListener('click', () => {
    state.side = btn.dataset.side;
    $$('.side-toggle button').forEach((b) => b.setAttribute('aria-pressed', String(b === btn)));
  }));
  $('#token-btn').addEventListener('click', promptToken);
  $('#theme-btn').addEventListener('click', cycleTheme);
  // The handler reads `enabled` from the JSON body; a query parameter is ignored,
  // which would silently toggle nothing while reporting success.
  $('#ks-btn').addEventListener('click', () => act(
    API.setKillSwitch,
    {
      body: { enabled: $('#ks-btn').dataset.engaged !== 'true' },
      confirm: 'Change the kill switch? This halts or resumes all trading.',
      done: 'Kill switch updated',
    },
  ));
  $('#refresh-btn').addEventListener('click', () => refreshAll({ immediate: true }));
  $('#cancel-all').addEventListener('click', () => act(API.cancelAllBrackets, {
    method: 'POST',
    confirm: 'Cancel every protective bracket? Open positions lose their stops and targets.',
    done: 'All brackets cancelled',
  }));

  // Delegated: panel tables are re-rendered constantly, so per-row listeners
  // would have to be rebound on every poll.
  document.addEventListener('click', (event) => {
    const approve = event.target.closest('[data-approve]');
    if (approve) {
      act(API.approve(approve.dataset.approve), {
        confirm: 'Approve this order? It will be released on the next optimizer tick.',
        done: 'Approved',
      });
      return;
    }
    const deny = event.target.closest('[data-deny]');
    if (deny) {
      act(API.deny(deny.dataset.deny), { confirm: 'Deny this order?', done: 'Denied' });
      return;
    }
    const cancel = event.target.closest('[data-cancel-bracket]');
    if (cancel) {
      act(API.cancelBracket, {
        body: { bracket_id: cancel.dataset.cancelBracket },
        confirm: 'Cancel this bracket? The position will lose its stop and target.',
        done: 'Bracket cancelled',
      });
      return;
    }
    const symbol = event.target.closest('[data-symbol]');
    if (symbol) {
      state.symbol = symbol.dataset.symbol;
      navigate('market');
      refreshPanel(panels.find((p) => p.id === 'candles'));
    }
  });

  $('#granularity').addEventListener('change', (event) => {
    state.granularity = Number(event.target.value);
    refreshPanel(panels.find((p) => p.id === 'candles'));
  });

  document.addEventListener('visibilitychange', () => {
    if (!document.hidden) refreshAll({ immediate: true });
  });

  window.addEventListener('hashchange', () => navigate(location.hash.slice(1)));

  document.addEventListener('keydown', (event) => {
    if (event.target.matches('input, select, textarea')) return;
    if (event.metaKey || event.ctrlKey || event.altKey) return;
    const map = { 1: 'overview', 2: 'positions', 3: 'approvals', 4: 'opportunities', 5: 'market', 6: 'strategies', 7: 'system' };
    if (map[event.key]) { event.preventDefault(); navigate(map[event.key]); }
    else if (event.key === 'r') { event.preventDefault(); refreshAll({ immediate: true }); }
    else if (event.key === 't') { event.preventDefault(); cycleTheme(); }
    else if (event.key === '?') { event.preventDefault(); $('#shortcuts').showModal(); }
    else if (event.key === 'Escape') { $('#shortcuts').close(); }
  });
}

/* ── polling ────────────────────────────────────────────────────────────── */

/* One timer, always. The cadence widens when the server is unhealthy so a
 * struggling dashboard does not pile requests onto it, and refreshes are skipped
 * entirely while the tab is hidden. An earlier draft installed a second timer
 * to "slow down", which produced two overlapping schedules. */
function startPolling() {
  if (state.timer) clearTimeout(state.timer);
  const tick = async () => {
    await refreshAll();
    // Re-arm with the current cadence so a recovery speeds polling back up.
    state.timer = setTimeout(tick, state.consecutiveFailures > 2
      ? state.pollMs * 3
      : state.pollMs);
  };
  state.timer = setTimeout(tick, state.pollMs);
}

/* ── boot ───────────────────────────────────────────────────────────────── */

function boot() {
  let theme = 'dark';
  try { theme = localStorage.getItem(THEME_KEY) || 'dark'; } catch (_) { /* ignore */ }
  applyTheme(theme);

  registerPanels();

  // Placeholder skeletons, so the page is never a blank shell. refreshPanel
  // replaces these on its first pass, and a pre-rendered skeleton used to
  // suppress the "this is slow, please wait" note a slow panel needs — the
  // watchlist then sat on three unlabelled grey bars for half a minute.
  // Only for panels that are not polling, since those have no other path to
  // getting one; polling panels get theirs within a tick.
  panels.filter((p) => !p.poll).forEach((spec) => {
    bodiesFor(spec.id).forEach((el) => showSkeleton(el, { slow: spec.slow, label: spec.title }));
  });

  renderNav();
  renderTokenState();
  bindActions();

  $('#granularity').value = String(state.granularity);
  navigate(location.hash.slice(1) || 'overview');
  refreshHeader();
  startPolling();
}

document.addEventListener('DOMContentLoaded', boot);
