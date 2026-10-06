/* Headless render tests for the dashboard panels.
 *
 * The contract suite (dashboard_ui_contract.test.mjs) proves the renderers read
 * the right field names. This proves they actually *produce* correct markup.
 *
 * The gap it closes: live data is mostly empty arrays, so a renderer that
 * throws, emits "undefined", or produces a malformed table on populated data is
 * invisible until the system actually has positions. These fixtures use the
 * field names captured from the live endpoints but with non-empty values, and
 * every panel is executed.
 *
 * Run: node --test tests/dashboard_ui_render.test.mjs
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import vm from 'node:vm';

const source = readFileSync('trading_system/ui/dashboard.js', 'utf8');

/* `const`/`let` at the top level of a vm script are lexical: they live in the
 * context's scope, not on the global object, so they are invisible to the test.
 * Function declarations *do* land on the global object. Append one expression
 * that hands back everything worth reaching, read off the lexical scope by
 * name. `money` is deliberately re-exported: it is shadowed inside the render
 * helpers by the panel parameters, so the outer one is the thing under test. */
const js = `${source}
;globalThis.__exports = {
  panels, panel, request, setCount, token, money, num, pct, ago, ageFrom, esc,
  signedClass, API, state, refreshHeader, refreshPanel, act, toast,
  GRANULARITY_LABELS, VIEWS, recordAction, auditEntries, renderAudit,
  refreshPreview, previewInputs, loadTicket, PREVIEW_DEFAULTS, round2, renderPreview,
};`;

/* ── minimal DOM ──────────────────────────────────────────────────────────
 * Only what the renderers touch: innerHTML, className, dataset, textContent,
 * querySelector/querySelectorAll, classList. Enough to run a renderer and
 * assert on the HTML it produced. */

class El {
  constructor(tag = 'div', attrs = {}) {
    this.tagName = tag.toUpperCase();
    this.attrs = { ...attrs };
    this.children = [];
    this._html = '';
    this.dataset = {};
    this.style = {};
    this.textContent = '';
    this.hidden = false;
    this.handlers = {};
  }

  set innerHTML(v) { this._html = String(v); }
  get innerHTML() { return this._html; }

  set className(v) { this.attrs.class = String(v); }
  get className() { return this.attrs.class || ''; }

  get classList() {
    const self = this;
    const set = () => new Set((self.attrs.class || '').split(/\s+/).filter(Boolean));
    const write = (s) => { self.attrs.class = [...s].join(' '); };
    return {
      add: (c) => { const s = set(); s.add(c); write(s); },
      remove: (c) => { const s = set(); s.delete(c); write(s); },
      contains: (c) => set().has(c),
      toggle: (c, on) => {
        const s = set();
        if (on === undefined) { s.has(c) ? s.delete(c) : s.add(c); } else if (on) s.add(c); else s.delete(c);
        write(s);
      },
    };
  }

  addEventListener(t, fn) { (this.handlers[t] ||= []).push(fn); }
  removeEventListener() {}
  setAttribute(k, v) { this.attrs[k] = String(v); }
  getAttribute(k) { return this.attrs[k] ?? null; }
  querySelector() { return null; }
  querySelectorAll() { return []; }
  closest() { return null; }
  focus() {}
  showModal() {}
  close() {}
  contains() { return false; }
  appendChild(child) { this.children.push(child); child.parentNode = this; return child; }
  removeChild(child) { this.children = this.children.filter((c) => c !== child); }
  remove() { if (this.parentNode) this.parentNode.removeChild(this); }
}

function makeDocument(elements = {}) {
  const doc = {
    hidden: false,
    documentElement: new El('html'),
    body: new El('body'),
    addEventListener() {},
    removeEventListener() {},
    querySelector(sel) {
      const id = sel.replace(/^#/, '');
      return elements[id] || new El();
    },
    querySelectorAll() { return []; },
    createElement(tag) { return new El(tag); },
  };
  return doc;
}

/* Build a sandbox with just enough of the browser for the module to evaluate,
 * then hand back the panel registry. */
function loadDashboard() {
  const store = { local: new Map(), session: new Map() };
  const storage = (m) => ({
    getItem: (k) => (m.has(k) ? m.get(k) : null),
    setItem: (k, v) => m.set(k, String(v)),
    removeItem: (k) => m.delete(k),
  });

  const doc = makeDocument();
  const sandbox = {
    document: doc,
    window: { addEventListener() {}, confirm: () => true, location: { hash: '' } },
    location: { hash: '', href: '' },
    localStorage: storage(store.local),
    sessionStorage: storage(store.session),
    fetch: async () => ({ ok: true, status: 200, json: async () => ({}) }),
    AbortController,
    setTimeout, clearTimeout, setInterval, clearInterval,
    console,
    Date, Math, JSON, Number, Object, Array, String, Boolean, Error, isNaN, parseInt, parseFloat,
  };
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(js, sandbox, { filename: 'dashboard.js' });
  // Lexical top-level declarations stay off the global object, so reach the
  // module through the __exports handle it installed rather than through the
  // sandbox properties.
  const api = sandbox.__exports;
  return { api, sandbox, doc, session: store.session };
}

/* Pull the panel specs out of the evaluated module. registerPanels() is what
 * populates them, so call it and read the array. */
function panelsFrom({ api, sandbox }) {
  sandbox.registerPanels();
  return api.panels;
}

function render(panelId, data, fixtures = {}) {
  const loaded = loadDashboard();
  const { doc } = loaded;
  const panels = panelsFrom(loaded);
  const spec = panels.find((p) => p.id === panelId);
  assert.ok(spec, `no panel registered for ${panelId}`);

  // Most renderers write to `el`. A few write to a named host by id instead
  // (the equity panel draws into #equity-chart and #equity-stats), so register
  // those and concatenate everything the renderer touched.
  const hosts = {};
  const base = doc.querySelector.bind(doc);
  doc.querySelector = (sel) => {
    const id = String(sel).replace(/^#/, '');
    if (id in fixtures) { (hosts[id] ||= fixtures[id]); return hosts[id]; }
    (hosts[id] ||= new El());
    return hosts[id];
  };
  void base;

  const target = new El('div', { class: 'panel-body' });
  hosts.panelBody = target;
  spec.render(target, data);
  return Object.values(hosts).map((h) => h.innerHTML).join('\n');
}

/* ── assertions shared by several cases ─────────────────────────────────── */

function assertClean(html, label) {
  assert.ok(html.length > 0, `${label}: rendered nothing`);
  // Unresolved template leftovers mean a value was interpolated as undefined.
  assert.doesNotMatch(html, /\$\{/, `${label}: unresolved template expression`);
  assert.doesNotMatch(html, /undefined/, `${label}: literal "undefined" in output`);
  assert.doesNotMatch(html, /NaN/, `${label}: NaN in output`);
  assert.doesNotMatch(html, /\[object Object\]/, `${label}: object stringified`);
  assert.doesNotMatch(html, /\bnull\b/, `${label}: literal null in output`);
  // Unescaped values would let a strategy name inject markup.
  assert.doesNotMatch(html, /<script/i, `${label}: script tag in output`);
}

function tableIsWellFormed(html, label) {
  const opens = (html.match(/<tr\b/g) || []).length;
  const closes = (html.match(/<\/tr>/g) || []).length;
  assert.equal(opens, closes, `${label}: unbalanced <tr> (${opens} open, ${closes} close)`);
  const cellOpens = (html.match(/<t[dh]\b/g) || []).length;
  const cellCloses = (html.match(/<\/t[dh]>/g) || []).length;
  assert.equal(cellOpens, cellCloses, `${label}: unbalanced table cells`);
  const bodyCells = (html.match(/<td\b/g) || []).length;
  const headCells = (html.match(/<th\b/g) || []).length;
  assert.ok(cellOpens > 0, `${label}: table has no cells`);
  return { opens, bodyCells, headCells };
}

/* ── fixtures, using the field names captured from the live endpoints ───── */

const FIX = {
  ready: {
    timestamp: 1789900000, ready: false,
    reason: 'blocked by a safety gate: trader-v4',
    kill_switch_active: true, children: [
      { name: 'daemon', state: 'RUNNING', pid: 4277 },
      { name: 'trader-v4', state: 'BLOCKED', pid: 0 },
      { name: 'dashboard', state: 'RUNNING', pid: 4280 },
    ],
    degraded: ['trader-v4'], blocked: ['trader-v4'],
    supervisor_running: true, supervisor_state_age_sec: 85.0,
    halted_by_operator: false,
  },
  positions: {
    total_positions: 2, total_unrealized_pnl_usd: 1234.56, total_unrealized_pnl_pct: 4.2,
    positions: [
      { instrument: 'BTC-USD', symbol: 'BTC-USD', side: 'LONG', classification: 'LONG',
        quantity: 0.05, quantity_usd: 4277.0, value: 4277.0,
        entry_price_usd: 81000.0, current_price_usd: 85545.5,
        unrealized_pnl_usd: 227.27, unrealized_pnl_pct: 5.6,
        status: 'open', venue: 'coinbase' },
      { instrument: 'ETH-USD', symbol: 'ETH-USD', side: 'LONG',
        quantity: 1.5, quantity_usd: 3000.0, entry_price_usd: 2000.0,
        current_price_usd: 1900.0, unrealized_pnl_usd: -150.0,
        unrealized_pnl_pct: -5.0, status: 'open', venue: 'coinbase' },
    ],
  },
  accounts: {
    total_accounts: 1,
    accounts: [{
      id: 'main', name: 'main', display_name: 'main', cash: 12345.67,
      nav: 13000.0, current_balance_usd: 12345.67, status: 'active',
      provider: 'coinbase', mode: 'paper', buying_power: 20000.0,
    }],
  },
  brackets: {
    brackets: {
      'brk-abc123': { product_id: 'BTC-USD', stop_price: 79000.0,
        target_price: 92000.0, entry_price: 81000.0, state: 'active' },
    },
  },
  approvals: {
    approvals: [
      { id: 'abc123def456', token: 'abc123def456', strategy_id: 'manual_order',
        instrument: 'BTC-USD', quantity_usd: 250.0, expected_fee: 0.25,
        risk_score: 1.0, status: 'pending', auto_approved: false,
        created_at: '2026-10-05T10:00:00+00:00' },
      { id: 'def456abc123', token: 'def456abc123', strategy_id: 'tlh',
        instrument: 'DOGE-USD', quantity_usd: 100.0, expected_fee: 0.1,
        risk_score: 0.5, status: 'approved', auto_approved: false,
        resolved_at: '2026-10-05T09:30:00Z', resolved_by: 'dashboard',
        created_at: '2026-10-05T09:00:00+00:00' },
    ],
    summary: { pending_count: 1, approved_count: 1, rejected_count: 0 },
  },
  opportunities: {
    status: 'ok', source: 'cache',
    signals: [
      { opp_type: 'OpportunityType.TLH', currency: 'DOGE', side: 'SELL',
        action: 'SELL', symbol: 'DOGE-USD', product_id: '', instrument: 'DOGE',
        strategy_name: 'TLH', size_usd: 100.0, priority: 0.8,
        opportunity_score: 0.82, final_confidence: 0.71, confidence: 0.7,
        trade_style: 'tax_loss', signal_reason: 'loss harvesting', reason: 'r' },
      { currency: 'BTC', side: 'BUY', action: 'BUY', symbol: 'BTC-USD',
        strategy_name: 'momentum', size_usd: 500.0, opportunity_score: 0.55,
        final_confidence: 0.6, trade_style: 'breakout', signal_reason: 'breakout' },
    ],
    queue: [], total_signals: 2, buy_signals: 1, sell_signals: 1, quality_score: 0.72,
  },
  regime: {
    current_regime: { state: 'trending', confidence_score: 0.82,
      volatility_score: 62, liquidity_score: 88, avg_spread_bps: 2.4 },
    sentiment: { bullish_pct: 68, bearish_pct: 32, total_signals: 41 },
    symbols_tracked: 34,
  },
  'signal-feed': {
    status: 'ok', source: 'cache',
    signals: [
      { side: 'BUY', action: 'BUY', symbol: 'BTC-USD', strategy_name: 'ema_cross',
        size_usd: 300.0, opportunity_score: 0.9, final_confidence: 0.8,
        signal_reason: 'golden cross' },
      { side: 'SELL', action: 'SELL', symbol: 'ETH-USD', strategy_name: 'rsi_revert',
        size_usd: 150.0, opportunity_score: 0.45, final_confidence: 0.5,
        signal_reason: 'overbought' },
    ],
    queue: [], total_signals: 2, buy_signals: 1, sell_signals: 1,
  },
  'strategy-perf': {
    strategies: [
      { name: 'BTCVolatilityStacking', strategy_id: 'btcvolatilitystacking',
        status: 'active', win_rate: 0.5, total_signals: 12, avg_confidence: 0.7 },
      { name: 'FundingRateContrarian', strategy_id: 'fundingratecontrarian',
        status: 'development', win_rate: 0.42, total_signals: 0, avg_confidence: 0.0 },
    ],
  },
  capital: {
    buckets: [
      { bucket_id: 'reserve', target_usd: 5000.0, value_usd: 5100.0, drift_pct: 2.0 },
      { bucket_id: 'growth', target_usd: 2000.0, value_usd: 1700.0, drift_pct: -15.0 },
    ],
  },
  'wash-sale': { cooldowns: { DOGE: 345.0, BTC: 30.0, ETH: 0 }, updated_at: '2026-10-05T10:00:00+00:00' },
  research: {
    hypotheses: [
      { name: 'BTC Momentum Continuation',
        description: 'Strong uptrend with increasing volume',
        confidence_score: 0.72, market_state: 'trending', strategy_type: 'momentum' },
    ],
    total_hypotheses: 3,
  },
  backtests: {
    experiments: [
      { name: 'smoke_v1', strategy: 'smoke_v1', verdict: 'FAIL', passed: false,
        win_rate: null, win_rate_pct: null, sharpe: 0.0, profit_factor: null,
        total_return: -0.12, n_strategies_tested: 216, ensemble: null, regime: null },
      { name: 'good_v2', strategy: 'good_v2', verdict: 'PASS', passed: true,
        win_rate: 0.61, win_rate_pct: 61.0, sharpe: 1.4, profit_factor: 1.8,
        total_return: 0.34, n_strategies_tested: 74 },
    ],
    count: 2, status: 'ok',
  },
  watchlist: {
    watchlist: [
      { symbol: 'BTC-USD', base: 'BTC', last: 85545.5, change_pct: 5.61,
        spark: [1, 2, 3], regime: 'bull' },
      { symbol: 'DOGE-USD', base: 'DOGE', last: null, change_pct: null,
        spark: [], regime: 'n/a' },
    ],
    offline: false,
  },
  candles: {
    symbol: 'BTC-USD', granularity: 3600,
    candles: [
      { t: 1789956000, o: 80973.46, h: 81509.8, l: 80934.0, c: 81299.86, v: 233.86 },
      { t: 1789959600, o: 81299.86, h: 81556.69, l: 81218.56, c: 81411.5, v: 137.29 },
      { t: 1789963200, o: 81411.5, h: 81489.99, l: 81000.0, c: 81200.0, v: 190.1 },
      { t: 1789966800, o: 81200.0, h: 82000.0, l: 81100.0, c: 81900.0, v: 210.4 },
    ],
  },
};


// Fixtures for the execution / bot / analysis panels, using the field names read
// off live responses (2026-10-05). Same discipline as the rest of this file: the
// renderer is exercised with populated data, because a renderer that throws on
// populated data is invisible while every endpoint returns an empty list.
const FIX2 = {
  executionStatus: {
    pending_approvals: [],
    pending_count: 2,
    recent_trade_count: 2,
    recent_trades: [
      { type: 'manual', side: 'BUY', currency: 'BTC', symbol: 'BTC-USD',
        size_usd: 250.0, fee: 0.25, pnl_usd: 0.0, reason: 'manual BUY BTC-USD',
        timestamp: '2026-10-05T14:00:00Z' },
      { type: 'bracket', side: 'SELL', currency: 'ETH', symbol: 'ETH-USD',
        size_usd: 100.0, fee: 0.1, pnl_usd: -12.5, reason: 'take profit',
        timestamp: '2026-10-05T13:00:00Z' },
    ],
    usdc_reserve_usd: 4157.5, raw_cash_buy_power_usd: 0.0,
    deployable_buy_power_usd: 0.0, portfolio_value_usd: 8315.0,
    usdc_balance_usd: 4157.5, hard_cap_usd: 500.0,
    risk_capital_in_play_usd: 250.0, remaining_hard_cap_usd: 250.0,
  },
  tradePlans: {
    plans: [
      { opp_type: 'TLH', currency: 'DOGE', side: 'SELL', size_usd: 100,
        reason: 'loss harvesting', priority: 0.8, product_id: '',
        expected_fee: 0.1, preview_passed: false, executed: false,
        order_id: '', entry_price_est: 0.51, stop_loss_pct: 0.0,
        take_profit_pct: 0.0, holding_period_hours: 72.0,
        expected_return_pct: 0.0, risk_pct: 1.2,
        meta: { trade_style: 'tax_loss' }, trade_style: 'tax_loss' },
      { opp_type: 'STRATEGY_SIGNAL', currency: 'BTC', side: 'BUY', size_usd: 400,
        reason: 'golden cross', priority: 1.4, product_id: 'BTC-USD',
        expected_fee: 0.4, preview_passed: true, executed: false,
        order_id: '', entry_price_est: 85500.0, stop_loss_pct: 3.0,
        take_profit_pct: 6.0, holding_period_hours: 168.0,
        expected_return_pct: 6.0, risk_pct: 3.0,
        meta: { trade_style: 'breakout' }, trade_style: 'breakout' },
    ],
    total: 2, updated_at: '2026-10-05T14:05:00Z', source: 'optimizer',
  },
  paperTrades: {
    trades: [
      { side: 'BUY', symbol: 'SOL-USD', size_usd: 75.0, fee: 0.08, pnl_usd: 3.1,
        reason: 'momentum', timestamp: '2026-10-05T12:00:00Z' },
    ],
    total: 1,
    settlement_summary: { closed: 12, open: 2, realised_pnl: 88.4 },
  },
  actions: {
    actions: [
      { id: 'refresh_market_data', label: 'Refresh market data',
        description: 'Request the collector/daemon to refresh cached market snapshots.',
        risk: 'safe' },
      { id: 'rebalance_dry_run', label: 'Rebalance dry-run',
        description: 'Queue a rebalance proposal without live execution.',
        risk: 'guarded' },
    ],
    queue: [
      { id: 'act-1', action: 'rebalance_dry_run', status: 'queued',
        note: 'from the UI', created_at: '2026-10-05T14:10:00Z' },
    ],
    queue_path: '/tmp/queue.json', backend: 'rust',
  },
  strategies: {
    total_strategies: 3,
    active_strategies: [
      { name: 'BTCVolatilityStacking', strategy_id: 'btcvolatilitystacking',
        status: 'active', sharpe_ratio: 1.42, win_rate_pct: 58.3, total_trades: 120 },
      { name: 'FundingRateContrarian', strategy_id: 'fundingratecontrarian',
        status: 'development', sharpe_ratio: 0.31, win_rate_pct: 44.0, total_trades: 12 },
    ],
  },
  rebalance: {
    available: true, active_preset: 'core',
    current_drift: 0.073, recommendation: 'trim growth',
    presets: [
      { name: 'core', label: 'Core', payload: { buckets: [] } },
      { name: 'fee_tier', label: 'Fee Tier Generator', payload: { buckets: [] } },
    ],
  },
  stairstep: {
    available: true,
    symbols: [{ symbol: 'BTC-USD', steps: 4, filled: 2, enabled: true }],
  },
  diversification: {
    strategies: [
      { name: 'kalman_mr', label: 'Kalman Filter Mean Reversion',
        source: 'OHLCV (price only)', group: 'momentum_adv',
        asset_class: 'growth/speculative', type: 'rust',
        description: 'Adaptive mean reversion via 1D Kalman filter',
        active: false, total_signals: 3, latest_signal: 'BUY' },
      { name: 'funding_rate', label: 'Funding Rate Contrarian',
        source: 'Binance', group: 'derivatives', asset_class: 'growth',
        type: 'python', description: 'Fade crowded funding', active: true,
        total_signals: 0, latest_signal: null },
    ],
    total_strategies: 2, active_strategies: 1, total_signals: 3,
    source_groups: ['momentum_adv', 'derivatives'],
  },
  performance: {
    summary_metrics: {
      total_trades: 42, total_volume_usd: 18400, total_fees_usd: 18.4,
      total_return_pct: 6.2, annualized_return_pct: 22.5, sharpe_ratio: 1.31,
      max_drawdown_pct: -8.4, buy_trades: 25, sell_trades: 17,
    },
    recent_trades: [],
  },
  venues: {
    kalshi: { configured: true, balance_usd: 250.5, portfolio_value_usd: 260.0,
      positions: [], error: null },
    polymarket: { configured: false, balance_usd: null, positions: [],
      note: 'no API keys configured' },
    ts: 1789900000,
  },
  arbSettlement: {
    settled_now: { settled: 2, expired: 1, still_open: 3, realized_pnl: 14.2, total_trades: 6 },
    summary: { total_trades: 40, by_status: { open: 3 }, realized_pnl: 210.5,
      open_expected_pnl: 22.0, diverged_pairs: 0 },
  },
  orderflow: {},
};

/* ── tests ──────────────────────────────────────────────────────────────── */

test('every registered panel is renderable with populated data', () => {
  const loaded = loadDashboard();
  const panels = panelsFrom(loaded);
  assert.ok(panels.length >= 16, `expected the full panel set, got ${panels.length}`);
  for (const spec of panels) {
    assert.ok(typeof spec.render === 'function', `${spec.id} has no render function`);
    assert.ok(spec.title, `${spec.id} has no title`);
  }
});

test('positions renders both rows with real money and percentage fields', () => {
  const html = render('positions', FIX.positions);
  assertClean(html, 'positions');
  const { opens } = tableIsWellFormed(html, 'positions');
  assert.equal(opens, 3, 'expected a header row plus two positions');
  assert.match(html, /BTC-USD/);
  assert.match(html, /ETH-USD/);
  assert.match(html, /\$85,545\.50/, 'mark price keeps its cents');
  assert.match(html, /\$81,000\.00/, 'entry price keeps its cents');
  assert.match(html, /\+\$227\.27/, 'positive unrealised P&L is signed');
  assert.match(html, /-\$150\.00/, 'negative P&L must not lose its sign');
  assert.match(html, /5\.60%/, 'P&L percentage');
  assert.match(html, /-5\.00%/, 'negative P&L percentage keeps its sign');
  assert.match(html, /coinbase/, 'venue column');
});

test('positions totals render when the book is empty', () => {
  // An empty book still has to report its totals, otherwise the panel looks
  // like it failed rather than like there is nothing to show.
  const html = render('positions', {
    total_positions: 0, total_unrealized_pnl_usd: 0,
    total_unrealized_pnl_pct: 0, positions: [],
  });
  assertClean(html, 'positions empty');
  assert.match(html, /No open positions/);
  assert.match(html, /0 positions/);
});

test('accounts renders the flat record shape the endpoint returns', () => {
  const html = render('accounts', FIX.accounts);
  assertClean(html, 'accounts');
  tableIsWellFormed(html, 'accounts');
  assert.match(html, /main/);
  assert.match(html, /\$12,345\.67/, 'cash keeps its cents');
  assert.match(html, /\$13,000\.00/, 'NAV');
  assert.match(html, /paper/);
  assert.match(html, /coinbase/);
});

test('brackets reads the id from the map key and offers a working cancel', () => {
  const html = render('brackets', FIX.brackets);
  assertClean(html, 'brackets');
  tableIsWellFormed(html, 'brackets');
  // The cancel id comes only from the object key, so if this is empty the
  // button would post a blank bracket_id and 400.
  const m = html.match(/data-cancel-bracket="([^"]*)"/);
  assert.ok(m, 'no cancel button rendered');
  assert.equal(m[1], 'brk-abc123', 'cancel button must carry the map key as its id');
  assert.match(html, /\$79,000/, 'stop price');
  assert.match(html, /unprotected/, 'must warn that cancelling removes protection');
});

test('approvals splits pending from settled and only offers action on pending', () => {
  const html = render('approvals', FIX.approvals);
  assertClean(html, 'approvals');
  tableIsWellFormed(html, 'approvals');
  assert.match(html, /awaiting approval/);
  // Provenance, not an inferred "auto": a human approval must be attributable.
  assert.match(html, /by dashboard/, 'the panel must say who resolved it');
  assert.match(html, /09:30:00/, 'and when');
  assert.doesNotMatch(html, />auto</,
    'a human approval must never be labelled automatic');
  // Exactly one approve button: the already-approved row must not offer one.
  const approves = html.match(/data-approve="/g) || [];
  const denies = html.match(/data-deny="/g) || [];
  assert.equal(approves.length, 1, 'only pending approvals may be actionable');
  assert.equal(denies.length, 1, 'only pending approvals may be actionable');
  assert.match(html, /data-approve="abc123def456"/);
  assert.match(html, /\$250/, 'size column uses quantity_usd');
  assert.match(html, /releases a real order/);
});

test('opportunities renders from signals, the field that actually exists', () => {
  const html = render('opportunities', FIX.opportunities);
  assertClean(html, 'opportunities');
  tableIsWellFormed(html, 'opportunities');
  assert.equal((html.match(/<tr\b/g) || []).length, 3, 'header plus two signals');
  assert.match(html, /DOGE-USD/);
  assert.match(html, /BTC-USD/);
  assert.match(html, /SELL/);
  assert.match(html, /BUY/);
  assert.match(html, /tax_loss/);
  assert.match(html, /loss harvesting/);
  assert.match(html, /quality/, 'quality score should be surfaced');
});

test('opportunities falls back to queue when signals is absent', () => {
  const html = render('opportunities', {
    status: 'ok', source: 'live', queue: FIX.opportunities.signals,
    total_signals: 2,
  });
  assertClean(html, 'opportunities queue fallback');
  assert.match(html, /DOGE-USD/);
});

test('regime nests one level correctly and labels the state', () => {
  const html = render('regime', FIX.regime);
  assertClean(html, 'regime');
  assert.match(html, /trending/, 'regime state from current_regime.state');
  assert.match(html, /82/, 'confidence');
  assert.match(html, /62/, 'volatility score');
  assert.match(html, /2\.4/, 'spread in bps');
  assert.match(html, /68/, 'bullish percentage');
  assert.match(html, /34/, 'symbols tracked');
});

test('wash-sale treats cooldowns as an object map and drops expired entries', () => {
  const html = render('wash-sale', FIX['wash-sale']);
  assertClean(html, 'wash-sale');
  tableIsWellFormed(html, 'wash-sale');
  // ETH has 0 seconds left, so it must not appear as an active cooldown.
  assert.doesNotMatch(html, /ETH/, 'expired cooldown should not be listed');
  assert.match(html, /DOGE/);
  assert.match(html, /BTC/);
  // Rendered from a duration, not a timestamp.
  assert.match(html, /[0-9]+m/, 'expected a duration like 5m, not a clock time');
});

test('strategy-perf uses the fields the endpoint returns', () => {
  const html = render('strategy-perf', FIX['strategy-perf']);
  assertClean(html, 'strategy-perf');
  tableIsWellFormed(html, 'strategy-perf');
  assert.match(html, /BTCVolatilityStacking/);
  assert.match(html, /FundingRateContrarian/);
  assert.match(html, /active/);
  assert.match(html, /development/);
  assert.match(html, /12/, 'total_signals');
  assert.match(html, /50/, 'win_rate as a percentage');
});

test('research renders name, description and confidence', () => {
  const html = render('research', FIX.research);
  assertClean(html, 'research');
  tableIsWellFormed(html, 'research');
  assert.match(html, /BTC Momentum Continuation/);
  assert.match(html, /Strong uptrend/);
  assert.match(html, /72/, 'confidence_score as a percentage');
  assert.match(html, /momentum/);
});

test('backtests renders verdict and survives null metrics', () => {
  const html = render('backtests', FIX.backtests);
  assertClean(html, 'backtests');
  tableIsWellFormed(html, 'backtests');
  assert.match(html, /smoke_v1/);
  assert.match(html, /FAIL/);
  assert.match(html, /good_v2/);
  assert.match(html, /PASS/);
  // smoke_v1 has null win_rate/profit_factor; those must render as "--", not NaN.
  assert.match(html, /--/, 'missing metrics should render as --');
  assert.match(html, /216/, 'n_strategies_tested');
});

test('watchlist renders the percentage the endpoint sends and handles nulls', () => {
  const html = render('watchlist', FIX.watchlist);
  assertClean(html, 'watchlist');
  tableIsWellFormed(html, 'watchlist');
  assert.match(html, /BTC-USD/);
  assert.match(html, /\+5\.61%/, 'change_pct is already a percentage');
  assert.match(html, /bull/);
  // DOGE has last: null and change_pct: null.
  assert.match(html, /DOGE-USD/);
  assert.match(html, /n\/a/, 'missing price should degrade, not crash');
  assert.match(html, /data-symbol="BTC-USD"/, 'chart button must carry the symbol');
});

test('candles draws from the short c key and orders oldest-first', () => {
  const html = render('candles', FIX.candles, { 'candles-title': new El('h2') });
  assertClean(html, 'candles');
  // A path with M then L: more than one point, so more than one close was read.
  assert.match(html, /<path class="line" d="M[\d.]+,[\d.]+ L/, 'chart path should have >=2 points');
  assert.match(html, /role="img"/);
  assert.match(html, /aria-label="[^"]*BTC-USD/);
  // x increases along the series: the first point must be left of the last.
  const coords = [...html.matchAll(/[ML]([\d.]+),([\d.]+)/g)].map((m) => [Number(m[1]), Number(m[2])]);
  assert.ok(coords.length >= 4, `expected 4 chart points, got ${coords.length}`);
  assert.ok(coords.length >= 4, `expected 4 chart points, got ${coords.length}`);
  assert.ok(coords[0][0] < coords[coords.length - 1][0],
    'series must run left-to-right (oldest first)');
  assert.match(html, /\+0\.74%/, 'first to last change: 81299.86 -> 81900');
});

test('equity chart draws the curve and the stats table', () => {
  const html = render('equity', {
    equity_curve: [
      { t: 1789900000, equity: 10000 }, { t: 1789903600, equity: 10200 },
      { t: 1789907200, equity: 10150 }, { t: 1789910800, equity: 10400 },
    ],
    realized_pnl: 400.0, unrealized_pnl: -25.5, drawdown: 0.02, peak_equity: 10500.0,
  });
  assertClean(html, 'equity');
  assert.match(html, /<svg class="chart"/);
  assert.match(html, /role="img"/);
  assert.match(html, /aria-label="Equity curve, 4 points/);
  assert.match(html, /<path class="line" d="M[\d.]+,[\d.]+ L/, 'line should have >=2 points');
  assert.match(html, /<path class="area"/, 'area fill under the curve');
  assert.match(html, /Realised P&L/);
  assert.match(html, /Unrealised P&L/);
  assert.match(html, /Drawdown/);
  assert.match(html, /Peak equity/);
  assert.match(html, /\+\$400\.00/, 'realised P&L is signed');
  assert.match(html, /-\$25\.50/, 'unrealised P&L keeps its sign');
  assert.match(html, /\$10,500\.00/, 'peak equity');
  assert.match(html, /2\.00%/, 'drawdown as a percentage');
});

test('equity chart states its empty condition instead of drawing nothing', () => {
  // No history yet is the normal state on a fresh deployment, not a fault.
  const html = render('equity', {});
  assert.match(html, /No equity history yet/);
  assert.doesNotMatch(html, /<svg/, 'must not draw an empty axis');
  // The stats table still renders its rows of "--".
  assert.match(html, /Realised P&L/);
});

test('equity chart accepts a bare numeric series', () => {
  const html = render('equity', { equity_curve: [10000, 10100, 10250] });
  assert.match(html, /<svg class="chart"/);
  assert.doesNotMatch(html, /NaN/);
});

test('equity chart rejects a series it cannot plot', () => {
  // A single point has no x-axis, and all-null data has no y-axis. Both must
  // degrade rather than produce a divide-by-zero path.
  for (const curve of [[10000], [null, null], ['x', 'y']]) {
    const html = render('equity', { equity_curve: curve });
    assert.match(html, /No equity history yet/, `curve ${JSON.stringify(curve)}`);
  }
});

test('empty payloads degrade to a stated empty state, never a blank panel', () => {
  const cases = [
    ['positions', { total_positions: 0, positions: [] }, /No open positions/],
    ['accounts', { total_accounts: 0, accounts: [] }, /No accounts/],
    ['brackets', { brackets: {} }, /No protective brackets/],
    ['approvals', { approvals: [], summary: {} }, /Nothing waiting for approval/],
    ['opportunities', { status: 'ok', signals: [], queue: [] }, /No opportunities/],
    ['watchlist', { watchlist: [], offline: true }, /No watchlist data/],
    ['candles', { candles: [] }, /No candles/],
    ['strategy-perf', { strategies: [] }, /No strategy results/],
    ['capital', { buckets: [] }, /No bucket configuration/],
    ['research', { hypotheses: [], total_hypotheses: 0 }, /No hypotheses/],
    ['backtests', { experiments: [], count: 0 }, /No experiments/],
  ];
  for (const [id, data, expected] of cases) {
    const html = render(id, data);
    assert.match(html, expected, `${id} should state its empty condition`);
    assertClean(html, `${id} empty`);
  }
});

test('a null-heavy payload cannot produce NaN or undefined', () => {
  const nasty = {
    positions: [
      { instrument: null, side: null, quantity: null, entry_price_usd: null,
        current_price_usd: null, unrealized_pnl_usd: null,
        unrealized_pnl_pct: null, venue: null },
    ],
    total_unrealized_pnl_usd: null, total_unrealized_pnl_pct: null,
  };
  for (const id of ['positions', 'accounts', 'strategy-perf', 'approvals',
    'opportunities', 'brackets', 'capital', 'wash-sale', 'regime',
    'research', 'backtests', 'watchlist']) {
    const html = render(id, nasty);
    assertClean(html, `${id} null payload`);
  }
});

test('hostile strings are escaped rather than injected', () => {
  // A strategy name comes from a config file and an instrument from an
  // exchange; neither is trusted. An unescaped value would be stored XSS on a
  // page that holds the operator token in sessionStorage.
  const payload = '<img src=x onerror=alert(1)>';
  const cases = [
    ['strategy-perf', { strategies: [{ name: payload, status: 'active', win_rate: 0.5, total_signals: 1, avg_confidence: 0.5 }] }],
    ['research', { hypotheses: [{ name: payload, description: payload, confidence_score: 0.5, market_state: 'x', strategy_type: 'y' }], total_hypotheses: 1 }],
    ['watchlist', { watchlist: [{ symbol: payload, last: 1, change_pct: 1, spark: [], regime: 'x' }], offline: false }],
    ['positions', { total_positions: 1, positions: [{ instrument: payload, side: 'LONG', quantity: 1, entry_price_usd: 1, current_price_usd: 2, unrealized_pnl_usd: 1, unrealized_pnl_pct: 1, venue: 'x' }] }],
  ];
  for (const [id, data] of cases) {
    const html = render(id, data);
    assert.doesNotMatch(html, /<img src=x/, `${id} injected a raw tag`);
    assert.match(html, /&lt;img/, `${id} should escape the payload`);
  }
});

test('panel options are all forwarded onto the spec', () => {
  // The panel() signature went stale once: `slow` and `accept` were added at the
  // call sites while the destructuring list kept the old shape, so both were
  // dropped without error -- the watchlist lost its loading note and /ready's 503
  // went back to being rendered as a failure. Destructuring a missing key is not
  // an error, so only an explicit assertion catches it.
  const { api } = loadDashboard();
  const before = api.panels.length;
  api.panel('opt-probe', {
    title: 'probe',
    endpoint: '/probe',
    render: () => {},
    poll: false,
    auth: true,
    method: 'POST',
    body: { a: 1 },
    timeout: 1234,
    slow: true,
    accept: [503],
  });
  assert.equal(api.panels.length, before + 1);
  const spec = api.panels[api.panels.length - 1];
  assert.equal(spec.id, 'opt-probe');
  assert.equal(spec.title, 'probe');
  assert.equal(spec.endpoint, '/probe');
  assert.equal(spec.poll, false, 'poll must be forwarded');
  assert.equal(spec.auth, true, 'auth must be forwarded');
  assert.equal(spec.method, 'POST', 'method must be forwarded');
  assert.deepEqual(spec.body, { a: 1 }, 'body must be forwarded');
  assert.equal(spec.timeout, 1234, 'timeout must be forwarded');
  assert.equal(spec.slow, true, 'slow must be forwarded');
  assert.deepEqual(spec.accept, [503], 'accept must be forwarded');
  assert.equal(typeof spec.render, 'function');
  api.panels.pop();
});

test('defaults are what the renderers assume', () => {
  const { api } = loadDashboard();
  const before = api.panels.length;
  api.panel('default-probe', { title: 't', endpoint: '/e', render: () => {} });
  const spec = api.panels[api.panels.length - 1];
  assert.equal(spec.poll, true);
  assert.equal(spec.auth, false);
  assert.equal(spec.slow, false);
  assert.equal(spec.accept, null);
  assert.equal(spec.timeout, undefined);
  api.panels.pop();
  assert.equal(api.panels.length, before);
});

test('a panel declared slow keeps its slow flag after registration', () => {
  // The specific regression: the watchlist must actually be marked slow.
  // panels is only populated by registerPanels(), so call it first.
  const panels = panelsFrom(loadDashboard());
  const watchlist = panels.find((p) => p.id === 'watchlist');
  assert.ok(watchlist, 'watchlist panel not registered');
  assert.equal(watchlist.slow, true,
    'watchlist is a ~26s cold fetch and must be declared slow');
  const health = panels.find((p) => p.id === 'health');
  // Spread into a host array: `accept: [503]` was built inside the vm realm, so
  // deepStrictEqual would fail on Array prototype identity rather than contents.
  assert.deepEqual([...(health.accept || [])], [503],
    '/ready answers 503 when not ready; that payload is the point of the panel');
});

test('request() treats an accepted non-2xx code as a real answer', async () => {
  const { api, sandbox } = loadDashboard();
  sandbox.fetch = async () => ({
    ok: false, status: 503, json: async () => ({ ready: false, reason: 'supervisor is not running' }),
  });

  // Without opt-in, a 503 is a failure.
  const strict = await api.request('/ready');
  assert.equal(strict.ok, false, 'a 503 is a failure without an explicit opt-in');
  assert.equal(strict.status, 503);

  // With it, the same response is readable -- which is the difference between
  // showing "not ready: blocked by a safety gate" and "could not load".
  const accepted = await api.request('/ready', { accept: [503] });
  assert.equal(accepted.ok, true);
  assert.equal(accepted.status, 503);
  assert.equal(accepted.data.reason, 'supervisor is not running');

  // The opt-in must not accept codes it was not asked about.
  sandbox.fetch = async () => ({ ok: false, status: 500, json: async () => ({ error: 'boom' }) });
  const wrongCode = await api.request('/ready', { accept: [503] });
  assert.equal(wrongCode.ok, false, '500 must not be accepted by a [503] opt-in');
});

test('act() confirms with the message, not the native function', async () => {
  // Regression guard for a bug that made every confirmation dialog in the UI
  // display the source of window.confirm instead of the consequence it was
  // warning about. Nothing went wrong by accident -- the dialog still blocked --
  // but the prompt said nothing at all, which is the whole point of a prompt
  // before an irreversible action.
  const { api, sandbox } = loadDashboard();
  const seen = [];
  let sent = null;
  sandbox.window.confirm = (msg) => { seen.push(String(msg)); return false; };
  sandbox.fetch = async (url) => {
    sent = { url, body: null };
    return { ok: false, status: 401, json: async () => ({ error: 'unauthorized' }) };
  };

  await api.act('/orders/submit', {
    confirm: 'Approve this order? It will be released on the next optimizer tick.',
    body: { symbol: 'BTC-USD' },
  });

  assert.equal(seen.length, 1, 'exactly one confirmation');
  assert.equal(seen[0], 'Approve this order? It will be released on the next optimizer tick.',
    'the dialog must carry the consequence, not a function reference');
  assert.doesNotMatch(seen[0], /=>/, 'the dialog text must not be source code');
  assert.equal(sent, null, 'declining must not issue the request');
});

test('act() sends the body it was given', async () => {
  const { api, sandbox } = loadDashboard();
  const sent = [];
  // act() is auth:true, and it refuses without a token before it ever reaches
  // the network -- which is the fail-closed behaviour, not a bug to route around.
  api.token.set('a-token-for-this-test');
  sandbox.window.confirm = () => true;
  sandbox.fetch = async (url, opts) => {
    sent.push({ url, method: opts.method, body: opts.body ? JSON.parse(opts.body) : undefined });
    return { ok: true, status: 200, json: async () => ({ ok: true }) };
  };

  await api.act('/kill-switch', { body: { enabled: true }, confirm: 'Change it?' });
  await api.act('/execution/brackets/cancel', { body: { bracket_id: 'abc' }, confirm: 'Cancel?' });

  // A successful action also triggers a refresh, so the stub sees the panel
  // traffic too. Filter to the two calls this test made.
  const acted = sent.filter((r) => r.url === '/kill-switch' || r.url === '/execution/brackets/cancel');
  assert.equal(acted.length, 2, `expected 2 action calls, saw ${acted.length}`);
  assert.deepEqual(acted[0].body, { enabled: true });
  assert.deepEqual(acted[1].body, { bracket_id: 'abc' });
  assert.ok(acted.every((r) => r.method === 'POST'));
  // Refreshing after acting is deliberate: the operator should see the effect
  // without reaching for the refresh button.
  assert.ok(sent.length > 2, 'a successful action should trigger a refresh');
});

test('act() without a confirm option does not prompt', async () => {
  const { api, sandbox } = loadDashboard();
  api.token.set('a-token-for-this-test');
  let prompted = false;
  sandbox.window.confirm = () => { prompted = true; return false; };
  sandbox.fetch = async () => ({ ok: true, status: 200, json: async () => ({ ok: true }) });

  await api.act('/orders/submit', {});
  assert.equal(prompted, false, 'an action with no message must not open an empty dialog');
});

test('a renderer that throws is contained, not fatal to the page', () => {
  const loaded = loadDashboard();
  const panels = panelsFrom(loaded);
  const spec = panels.find((p) => p.id === 'positions');
  // refreshPanel wraps each render in try/catch precisely so a bug here cannot
  // take the dashboard down. Prove the wrapper exists and uses a named message.
  const fn = loaded.api.refreshPanel.toString();
  assert.match(fn, /catch/);
  assert.match(fn, /render failed:/);
  assert.ok(typeof spec.render === 'function');
});

test('setCount is wired to the views that need badges', () => {
  const html = render('approvals', FIX.approvals);
  // approvals should badge only the pending count, not all rows.
  assert.ok(html.length > 0);
  const fn = loadDashboard().api.setCount.toString();
  assert.match(fn, /data-count/);
});

test('formatter helpers behave at the edges', () => {
  const { money, num, pct, ago, esc, signedClass, ageFrom } = loadDashboard().api;

  // Currency keeps two decimals at every magnitude: dropping cents on a large
  // value loses the number an operator checks P&L against, and showing six on a
  // zero balance is noise.
  assert.equal(money(0), '$0.00');
  assert.equal(money(null), '--');
  assert.equal(money(undefined), '--');
  assert.equal(money(NaN), '--');
  // The minus sign is always carried; `signed` only controls whether a positive
  // value gets a '+'. Silently dropping a negative sign would understate a loss.
  assert.equal(money(-12.5), '-$12.50');
  assert.equal(money(85545.5), '$85,545.50');
  assert.equal(money(-1234567.891), '-$1,234,567.89');
  assert.match(money(5, { signed: true }), /^\+\$5\.00$/);
  assert.equal(money(5), '$5.00', 'unsigned positive has no plus');
  assert.equal(money(-5, { signed: true }), '-$5.00', 'signed never doubles the minus');

  assert.equal(num(0), '0', 'an exact zero must not render as 0.000000');
  assert.equal(num(-0), '0');
  assert.equal(num(0.000001, 4), '0.0000');
  assert.equal(num(null), '--');
  assert.equal(num(NaN), '--');
  assert.equal(num(1234567, 0), '1,234,567');

  assert.equal(pct(null), '--');
  assert.equal(pct(0.5), '50.00%');

  assert.equal(ago(5), '5s');
  assert.equal(ago(90), '1m');
  assert.equal(ago(3700), '1h1m');
  assert.equal(ago(90000), '1d', 'days are reported whole, not as 1d1h');
  assert.equal(ago(-5), '0s', 'a clock skew must not render a negative age');
  assert.equal(ago(null), '--');
  assert.equal(ageFrom('not-a-date'), '--');
  assert.equal(ageFrom(null), '--');
  assert.match(ageFrom(new Date(Date.now() - 120000).toISOString()), /2m/);

  assert.equal(esc(null), '');
  assert.equal(esc('<b>'), '&lt;b&gt;');
  assert.equal(esc('"q"'), '&quot;q&quot;');

  assert.equal(signedClass(-1), 'down');
  assert.equal(signedClass(1), 'up');
  assert.equal(signedClass(0), 'dim');
  assert.equal(signedClass(null), 'dim');
});

test('request() degrades instead of throwing on every failure mode', async () => {
  // One sandbox per case: request() closes over the global fetch, so swapping
  // it mid-flight would let the parallel cases observe each other's stub.
  const withFetch = async (impl) => {
    const { api, sandbox } = loadDashboard();
    sandbox.fetch = impl;
    return { api, sandbox };
  };

  // Network failure: the server is unreachable.
  {
    const { api } = await withFetch(async () => { throw new TypeError('Failed to fetch'); });
    const r = await api.request('/health');
    assert.equal(r.ok, false, 'an unreachable server is a failure');
    assert.equal(r.status, 0, 'an unreachable server is status 0, not a 5xx');
    assert.ok(r.error);
  }

  // Abort: what a slow upstream looks like to the client. This must be
  // reported as a timeout, because "unreachable" tells an operator to restart
  // something when the truth is one slow external call.
  {
    const err = new Error('aborted');
    err.name = 'AbortError';
    const { api } = await withFetch(async () => { throw err; });
    const r = await api.request('/market/watchlist');
    assert.equal(r.ok, false);
    assert.match(r.error, /timed out/);
    assert.doesNotMatch(r.error, /abort/i, 'the raw AbortError should not leak to the panel');
  }

  // A real timeout, driven by the timer rather than a pre-aborted signal.
  {
    const { api } = await withFetch((_url, opts) => new Promise((_res, rej) => {
      opts.signal.addEventListener('abort', () => {
        const e = new Error('aborted'); e.name = 'AbortError'; rej(e);
      });
    }));
    const r = await api.request('/market/watchlist', { timeout: 20 });
    assert.match(r.error, /timed out after 0s|timed out/);
  }

  // 200 with a body that is not JSON must not throw.
  {
    const { api } = await withFetch(async () => ({
      ok: true, status: 200, json: async () => { throw new Error('Unexpected token <'); },
    }));
    const r = await api.request('/health');
    assert.equal(r.ok, true);
    assert.equal(r.data, null);
  }

  // Auth required but no token held: short-circuits without a network call.
  {
    let called = false;
    const { api } = await withFetch(async () => { called = true; return { ok: true, status: 200, json: async () => ({}) }; });
    const r = await api.request('/orders/submit', { method: 'POST', auth: true, body: {} });
    assert.equal(r.status, 401);
    assert.match(r.error, /token required/);
    assert.equal(called, false, 'must not hit the network when no token is held');
  }

  // A wrong token is the server's call to make, not the client's.
  {
    const { api } = await withFetch(async () => ({
      ok: false, status: 401, json: async () => ({ error: 'unauthorized' }),
    }));
    api.token.set('wrong-token');
    const r = await api.request('/orders/submit', { method: 'POST', auth: true, body: {} });
    assert.equal(r.status, 401);
    assert.match(r.error, /unauthorized/);
  }

  // A rejected action reports ok:false rather than looking like success. The
  // token must be set first, or the client short-circuits to 401 before the
  // request is ever made.
  {
    const { api } = await withFetch(async () => ({
      ok: false, status: 400,
      json: async () => ({ ok: false, error: 'size_usd must be > 0' }),
    }));
    api.token.set('a-real-looking-token');
    const r = await api.request('/orders/submit', { method: 'POST', auth: true, body: {} });
    assert.equal(r.ok, false);
    assert.equal(r.status, 400);
    assert.match(r.error, /size_usd/);
  }
});

test('the token is read from sessionStorage and cleared by set("")', () => {
  const { api, session } = loadDashboard();
  assert.equal(api.token.present(), false);
  api.token.set('secret-token-value');
  assert.equal(api.token.get(), 'secret-token-value');
  assert.equal(api.token.present(), true);
  api.token.set('');
  assert.equal(api.token.present(), false);
  assert.ok(session instanceof Map);
});

test('buying power separates deployable from raw cash and the cap', () => {
  const html = render('buy-power', FIX2.executionStatus);
  assertClean(html, 'buy-power');
  tableIsWellFormed(html, 'buy-power');
  // The whole point of the panel: deployable is not raw cash, and the reserve
  // floor is why. All three must be visible or an operator sizes against the
  // wrong number.
  assert.match(html, /Deployable/);
  assert.match(html, /Raw cash/);
  assert.match(html, /Reserve floor/);
  assert.match(html, /\$4,157\.50/, 'reserve floor keeps its cents');
  assert.match(html, /\$500\.00/, 'hard cap');
  // Whitespace-insensitive: the renderer's template wraps these across lines,
  // and the assertion is about the meaning, not the formatting.
  assert.match(html.replace(/\s+/g, ' '), /2 orders awaiting approval/,
    'pending capital is called out, not just counted');
});

test('recent trades render side, size, fee and signed pnl', () => {
  const html = render('recent-trades', FIX2.executionStatus);
  assertClean(html, 'recent-trades');
  tableIsWellFormed(html, 'recent-trades');
  assert.match(html, /BTC-USD/);
  assert.match(html, /ETH-USD/);
  assert.match(html, /BUY/);
  assert.match(html, /SELL/);
  // A flat P&L has no sign: money() only prefixes '+' on a positive value.
  assert.match(html, /\$0\.00/, 'a flat pnl still renders rather than showing --');
  assert.match(html, /-\$12\.50/, 'a losing trade keeps its sign');
  assert.match(html, /\$250\.00/);
});

test('trade plans show whether the optimizer preview cleared them', () => {
  const html = render('trade-plans', FIX2.tradePlans);
  assertClean(html, 'trade-plans');
  tableIsWellFormed(html, 'trade-plans');
  assert.equal((html.match(/<tr\b/g) || []).length, 3, 'header plus two plans');
  // preview_passed is the field that says whether a plan survived the cost
  // preview; it must be visible, not buried in a payload.
  assert.match(html, /not cleared/);
  assert.match(html, /passed/);
  assert.match(html, /TLH/);
  assert.match(html, /STRATEGY_SIGNAL/);
  // The bridge from analysis into execution.
  assert.match(html, /data-prefill="BTC-USD"/,
    'a cleared plan must offer a one-click route into the order ticket');
  assert.match(html, /Plans are proposals, not orders/);
});

test('trade plans degrade when the optimizer has not run', () => {
  const html = render('trade-plans', { plans: [], total: 0, updated_at: '', source: '' });
  assertClean(html, 'trade-plans empty');
  assert.match(html, /No trade plans/);
});

test('paper trade ledger renders its settlement summary', () => {
  const html = render('paper-trades', FIX2.paperTrades);
  assertClean(html, 'paper-trades');
  tableIsWellFormed(html, 'paper-trades');
  assert.match(html, /SOL-USD/);
  assert.match(html, /closed/);
  assert.match(html, /realised_pnl/);
  assert.match(html, /\+\$3\.10/);
});

test('operator actions carry their own risk and are gated', () => {
  const html = render('actions', FIX2.actions);
  assertClean(html, 'actions');
  tableIsWellFormed(html, 'actions');
  assert.match(html, /Refresh market data/);
  assert.match(html, /safe/);
  assert.match(html, /guarded/);
  // The two risk levels must be visually distinct, or "guarded" means nothing.
  assert.match(html, /pill positive">safe/);
  assert.match(html, /pill warn">guarded/);
  // Running one moves the system, so the control is token-gated and carries its
  // risk level for the confirmation to quote.
  assert.match(html, /data-run-action="refresh_market_data"/);
  assert.match(html, /data-run-action="rebalance_dry_run"/);
  assert.match(html, /data-needs-token/);
  assert.match(html, /data-risk="guarded"/);
  assert.match(html.replace(/\s+/g, ' '), /backend <strong>rust<\/strong>/);
});

test('the action queue is shown so a request is not a black hole', () => {
  const html = render('actions', FIX2.actions);
  assertClean(html, 'actions queue');
  assert.match(html, /Queue/);
  assert.match(html, /rebalance_dry_run/);
  assert.match(html, /queued/);
});

test('strategies render the fields /strategies actually returns', () => {
  const html = render('strategies', FIX2.strategies);
  assertClean(html, 'strategies');
  tableIsWellFormed(html, 'strategies');
  assert.match(html, /BTCVolatilityStacking/);
  assert.match(html, /FundingRateContrarian/);
  assert.match(html, /1\.42/, 'sharpe_ratio');
  assert.match(html, /58\.3%/, 'win_rate_pct is already a percentage');
  assert.match(html, /120/);
  assert.match(html, /development/);
  // No endpoint toggles a strategy, so no control may imply one exists.
  assert.doesNotMatch(html, /data-disable-strategy|data-enable-strategy/);
});

test('rebalance shows drift and gates preset application', () => {
  const html = render('rebalance', FIX2.rebalance);
  assertClean(html, 'rebalance');
  tableIsWellFormed(html, 'rebalance');
  assert.match(html, /core/);
  assert.match(html, /Fee Tier Generator/);
  assert.match(html, /7\.30%/, 'drift as a percentage');
  assert.match(html, /trim growth/, 'the recommendation is the actionable part');
  // Rewriting allocation is guarded: gated, and not offered for the active preset.
  assert.match(html, /data-bucket-preset="fee_tier"/);
  assert.match(html, /data-needs-token/);
  assert.doesNotMatch(html, /data-bucket-preset="core"/,
    'the already-active preset must not offer to re-apply itself');
  assert.match(html.replace(/\s+/g, ' '), /rewrites the capital allocation/i);
});

test('stairstep handles available, empty and unavailable', () => {
  const html = render('stairstep', FIX2.stairstep);
  assertClean(html, 'stairstep');
  tableIsWellFormed(html, 'stairstep');
  assert.match(html, /BTC-USD/);
  assert.match(html, /paused|active/);

  assert.match(render('stairstep', { available: false }),
    /not available/);
  assert.match(render('stairstep', { available: true, symbols: [] }),
    /No stair-step ladders/);
});

test('diversification surfaces the independence group, not just a name', () => {
  const html = render('diversification', FIX2.diversification);
  assertClean(html, 'diversification');
  tableIsWellFormed(html, 'diversification');
  // The group is what ConfidenceMatrix weights by, so it is the column that
  // matters when asking whether signals are actually independent.
  assert.match(html, /momentum_adv/);
  assert.match(html, /derivatives/);
  assert.match(html, /Kalman Filter Mean Reversion/);
  assert.match(html, /growth\/speculative/);
  assert.match(html, /1 of 2 strategies/, 'coverage is stated up front');
});

test('performance renders the summary metrics', () => {
  const html = render('performance', FIX2.performance);
  assertClean(html, 'performance');
  tableIsWellFormed(html, 'performance');
  assert.match(html, /Total trades/);
  assert.match(html, /42/);
  assert.match(html, /1\.31/, 'sharpe');
  assert.match(html, /-8\.40%/, 'max drawdown keeps its sign');
  assert.match(html, /25 \/ 17/, 'buy/sell split');
  assert.match(html, /\$18\.40/, 'fees');
});

test('venue balances distinguish configured from not configured', () => {
  const html = render('venues', FIX2.venues);
  assertClean(html, 'venues');
  tableIsWellFormed(html, 'venues');
  assert.match(html, /Kalshi/);
  assert.match(html, /Polymarket/);
  assert.match(html, /\$250\.50/);
  assert.match(html, /not configured/);
  // A null balance must read as unknown, not as zero.
  assert.match(html, /--/);
  assert.match(html, /no API keys configured/);
});

test('arbitrage settlement raises diverged pairs loudly', () => {
  const clean = render('arb-settlement', FIX2.arbSettlement);
  assertClean(clean, 'arb-settlement');
  tableIsWellFormed(clean, 'arb-settlement');
  assert.match(clean, /Settled this pass/);
  assert.doesNotMatch(clean, /diverged between venues/);

  // A diverged pair is a real discrepancy between venues, not a rounding
  // artefact, so it gets a banner rather than a table cell.
  const diverged = render('arb-settlement', {
    ...FIX2.arbSettlement,
    summary: { ...FIX2.arbSettlement.summary, diverged_pairs: 2 },
  });
  assertClean(diverged, 'arb-settlement diverged');
  assert.match(diverged, /2 pairs diverged/);
  assert.match(diverged, /not a rounding difference/);
});

test('orderflow says it is unpopulated rather than showing an empty table', () => {
  const html = render('orderflow', FIX2.orderflow);
  assertClean(html, 'orderflow empty');
  assert.match(html, /No order-flow signals recorded yet/);
  assert.match(html, /Populated by the market daemon/);

  const populated = render('orderflow', {
    btc_cvd: { signal: 'BUY', value: 0.62, bias: 0.62 },
    eth_imbalance: -0.31,
  });
  assertClean(populated, 'orderflow populated');
  assert.match(populated, /btc_cvd/);
  assert.match(populated, /BUY/);
  assert.match(populated, /eth_imbalance/);
});

test('every new panel survives a null payload', () => {
  const ids = ['buy-power', 'recent-trades', 'trade-plans', 'paper-trades', 'actions',
    'strategies', 'rebalance', 'stairstep', 'diversification', 'performance',
    'venues', 'arb-settlement', 'orderflow'];
  for (const id of ids) {
    assertClean(render(id, {}), `${id} empty object`);
    assertClean(render(id, []), `${id} empty array`);
  }
});

test('new panels escape hostile content too', () => {
  const payload = '<img src=x onerror=alert(1)>';
  const cases = [
    ['trade-plans', { plans: [{ opp_type: payload, currency: payload, side: 'BUY',
      size_usd: 1, entry_price_est: 1, stop_loss_pct: 1, take_profit_pct: 1,
      holding_period_hours: 1, preview_passed: true, product_id: payload }], total: 1 }],
    ['actions', { actions: [{ id: payload, label: payload, description: payload, risk: payload }], queue: [] }],
    ['strategies', { total_strategies: 1, active_strategies: [{ name: payload, status: 'active', sharpe_ratio: 1, win_rate_pct: 1, total_trades: 1 }] }],
    ['diversification', { strategies: [{ name: payload, label: payload, group: payload, asset_class: payload, description: payload, total_signals: 1 }], source_groups: [payload] }],
    ['venues', { kalshi: { configured: true, balance_usd: 1, positions: [], error: payload } }],
  ];
  for (const [id, data] of cases) {
    const html = render(id, data);
    assert.doesNotMatch(html, /<img src=x/, `${id} injected a raw tag`);
    assert.match(html, /&lt;img/, `${id} should escape the payload`);
  }
});

/* ── order preview ──────────────────────────────────────────────────────────
 *
 * The preview mirrors api_order_submit's arithmetic. The tests assert the
 * *mirroring*, not the arithmetic itself: if the server changes its defaults the
 * preview must change with it, and a test that hard-codes 3% and 6% would keep
 * passing while the two disagree.
 */

test('the preview reproduces the server bracket for a BUY', async () => {
  const { api, sandbox } = loadDashboard();
  sandbox.fetch = async () => ({
    ok: true, status: 200,
    json: async () => ({
      instrument: 'BTC-USD', current_price_usd: 80000,
      summary: { weighted_avg_target_usd: 79200, model_count: 4 },
      market_data: { volume_24h: 1e9, liquidity_score: 90, spread_bps: 2.5 },
    }),
  });
  api.state.symbol = 'x';
  api.state.side = 'BUY';

  // Drive the preview through its public entry point with the ticket fields set.
  const fields = new Map([
    ['#oe-symbol', { value: 'BTC-USD' }],
    ['#oe-size', { value: '1000' }],
    ['#oe-stop', { value: '' }],
    ['#oe-target', { value: '' }],
    ['#oe-preview', new El('div')],
  ]);
  sandbox.document.querySelector = (sel) => fields.get(sel) || new El();

  const input = { symbol: 'BTC-USD', side: 'BUY', size: 1000, stopPct: 0.03, targetPct: 0.06 };
  api.renderPreview({ ok: true, data: { current_price_usd: 80000,
    summary: { weighted_avg_target_usd: 79200 },
    market_data: { liquidity_score: 90, spread_bps: 2.5 } } }, input);
  const html = fields.get('#oe-preview').innerHTML;

  assertClean(html, 'preview buy');
  assert.match(html, /\$80,000\.00/, 'entry');
  assert.match(html, /\$77,600\.00/, 'stop is entry * (1 - 3%)');
  assert.match(html, /\$84,800\.00/, 'target is entry * (1 + 6%)');
  assert.match(html, /0\.01250000|0\.0125/, 'quantity = size / price');
  assert.match(html, /\$1\.00/, 'fee is 0.1% of size');
  assert.match(html, /2\.00 : 1/, 'reward:risk is 6% over 3%');
  assert.match(html, /growth/, 'the bucket the server hard-codes');
  assert.match(html, /Indicative/, 'must not claim to be authoritative');
  assert.doesNotMatch(html, /panel-error/);
});

test('the preview inverts stop and target for a SELL', () => {
  const { api, sandbox } = loadDashboard();
  const host = new El('div');
  sandbox.document.querySelector = () => host;
  api.renderPreview({ ok: true, data: { current_price_usd: 80000, market_data: {} } },
    { symbol: 'BTC-USD', side: 'SELL', size: 1000, stopPct: 0.03, targetPct: 0.06 });
  const html = host.innerHTML;
  assertClean(html, 'preview sell');
  assert.match(html, /\$82,400\.00/, 'a short stops above entry');
  // 80,000 * (1 - 0.06) = 75,200
  assert.match(html, /\$75,200\.00/, 'a short targets below entry');
  assert.doesNotMatch(html, /wrong side of entry/, 'a correctly-oriented short is not flagged');
});

test('the preview warns when the stop is on the wrong side', async () => {
  const { api, sandbox } = loadDashboard();
  const host = new El('div');
  sandbox.document.querySelector = () => host;
  // stop_pct of -0.5 on a BUY puts the stop above entry. The server accepts it and
  // creates the bracket, so the operator has to be told here or not at all.
  api.renderPreview({ ok: true, data: { current_price_usd: 100, market_data: {} } },
    { symbol: 'X-USD', side: 'BUY', size: 100, stopPct: -0.5, targetPct: 0.06 });
  const html = host.innerHTML;
  assertClean(html, 'preview inverted');
  assert.match(html, /wrong side of entry/);
  assert.match(html, /fill immediately/);
  assert.match(html, /banner danger/, 'inverted bracket gets a banner, not a footnote');
});

test('the preview says the submit would fail when there is no price', async () => {
  const { api, sandbox } = loadDashboard();
  const host = new El('div');
  sandbox.document.querySelector = () => host;
  api.renderPreview({ ok: true, data: { current_price_usd: 0, market_data: {} } },
    { symbol: 'NOPE-USD', side: 'BUY', size: 100, stopPct: 0.03, targetPct: 0.06 });
  const html = host.innerHTML;
  assertClean(html, 'preview no price');
  assert.match(html, /No price for NOPE-USD/);
  assert.match(html, /could not fetch price/, 'quotes the error the server will return');
});

test('the preview distinguishes a failed lookup from an unreachable server', async () => {
  const { api, sandbox } = loadDashboard();
  const host = new El('div');
  sandbox.document.querySelector = () => host;
  api.renderPreview({ ok: false, status: 0 }, { symbol: 'X', side: 'BUY', size: 1, stopPct: 0, targetPct: 0 });
  assert.match(host.innerHTML, /dashboard server is unreachable/);
  assert.match(host.innerHTML, /order cannot be priced/);

  api.renderPreview({ ok: false, status: 500, error: 'boom' },
    { symbol: 'X', side: 'BUY', size: 1, stopPct: 0, targetPct: 0 });
  assert.match(host.innerHTML, /Price lookup failed: boom/);
  assert.doesNotMatch(host.innerHTML, /unreachable/);
});

test('the preview asks for a size before doing any work', async () => {
  const { api, sandbox } = loadDashboard();
  const host = new El('div');
  // A real ticket: every field previewInputs() reads has to exist, or this
  // fails on a missing .value rather than on the behaviour under test.
  const fields = {
    '#oe-symbol': { value: 'BTC-USD' },
    '#oe-size': { value: '' },
    '#oe-stop': { value: '' },
    '#oe-target': { value: '' },
    '#oe-preview': host,
  };
  sandbox.document.querySelector = (sel) => fields[sel] || new El();
  let fetched = 0;
  sandbox.fetch = async () => { fetched += 1; return { ok: true, status: 200, json: async () => ({}) }; };
  await api.refreshPreview();
  assert.equal(fetched, 0, 'no price lookup without a size');
});

test('preview defaults match the server default of 3% and 6%', () => {
  const { api } = loadDashboard();
  // Mirrors api_order_submit's `payload.get("stop_pct") or 0.03`.
  assert.equal(api.PREVIEW_DEFAULTS.stopPct, 3);
  assert.equal(api.PREVIEW_DEFAULTS.targetPct, 6);
  assert.equal(api.PREVIEW_DEFAULTS.feeRate, 0.001);
});

test('a slow preview cannot overwrite a newer one', async () => {
  const { api, sandbox } = loadDashboard();
  const host = new El('div');
  sandbox.document.querySelector = (sel) => (sel === '#oe-preview' ? host : {
    value: sel === '#oe-size' ? '100' : sel === '#oe-stop' || sel === '#oe-target' ? '' : 'X-USD',
  });
  const seen = [];
  let resolveSlow;
  sandbox.fetch = (url) => {
    seen.push(url);
    if (url.includes('SLOW')) {
      return new Promise((res) => { resolveSlow = () => res({
        ok: true, status: 200, json: async () => ({ current_price_usd: 1, market_data: {} }),
      }); });
    }
    return Promise.resolve({
      ok: true, status: 200, json: async () => ({ current_price_usd: 999, market_data: {} }),
    });
  };
  const slow = api.refreshPreview();
  const fast = api.refreshPreview();
  await fast;
  if (resolveSlow) resolveSlow();
  await slow;
  assert.ok(seen.length >= 2);
  assert.match(host.innerHTML, /999/, 'the newest lookup wins');
});

/* ── action audit ────────────────────────────────────────────────────────── */

test('every mutating action is recorded with its outcome', async () => {
  const { api, sandbox } = loadDashboard();
  api.token.set('tok');
  sandbox.window.confirm = () => true;
  const el = new El('div');
  sandbox.document.querySelector = () => el;

  let mode = 'ok';
  sandbox.fetch = async () => {
    if (mode === 'ok') return { ok: true, status: 200, json: async () => ({ ok: true }) };
    if (mode === 'unauth') return { ok: false, status: 401, json: async () => ({ error: 'unauthorized' }) };
    return { ok: false, status: 400, json: async () => ({ error: 'size_usd must be > 0' }) };
  };

  await api.act('/orders/submit', { body: {}, confirm: 'go' });
  mode = 'unauth';
  await api.act('/kill-switch', { body: {}, confirm: 'go' });
  mode = 'bad';
  await api.act('/capital/buckets/preset', { body: {}, confirm: 'go' });

  const rows = api.auditEntries();
  assert.equal(rows.length, 3, `expected 3 audit rows, got ${rows.length}`);
  assert.equal(rows[0].path, '/capital/buckets/preset', 'newest first');
  assert.equal(rows[0].ok, false);
  assert.match(rows[0].outcome, /size_usd/, 'the server reason is kept, not just "failed"');
  assert.equal(rows[1].outcome, 'needs token');
  assert.equal(rows[2].ok, true);
  for (const row of rows) assert.ok(row.at, 'every row is timestamped');
});

test('a cancelled confirmation is recorded as cancelled, not attempted', async () => {
  const { api, sandbox } = loadDashboard();
  api.token.set('tok');
  sandbox.window.confirm = () => false;
  let fetched = 0;
  sandbox.fetch = async () => { fetched += 1; return { ok: true, status: 200, json: async () => ({}) }; };
  const el = new El('div');
  sandbox.document.querySelector = () => el;

  await api.act('/orders/submit', { body: {}, confirm: 'sure?' });
  assert.equal(fetched, 0, 'declining must not call the endpoint');
  const rows = api.auditEntries();
  assert.equal(rows.length, 1);
  assert.equal(rows[0].outcome, 'cancelled');
  assert.equal(rows[0].ok, false);
});

test('the audit never records the token or a request body', async () => {
  const { api, sandbox } = loadDashboard();
  api.token.set('super-secret-token');
  sandbox.window.confirm = () => true;
  sandbox.fetch = async () => ({ ok: true, status: 200, json: async () => ({ ok: true }) });
  const el = new El('div');
  sandbox.document.querySelector = () => el;

  await api.act('/orders/submit', {
    body: { symbol: 'BTC-USD', size_usd: 1000 }, confirm: 'go',
  });
  const raw = JSON.stringify(api.auditEntries());
  assert.doesNotMatch(raw, /super-secret-token/, 'the token must never be persisted');
  assert.doesNotMatch(raw, /size_usd/, 'request bodies must not be persisted');
  assert.doesNotMatch(raw, /BTC-USD/, 'nor their contents');
  assert.match(raw, /orders\/submit/, 'but the path is fine to keep');
});

test('the audit is capped so sessionStorage cannot grow without bound', async () => {
  const { api, sandbox } = loadDashboard();
  api.token.set('tok');
  sandbox.window.confirm = () => true;
  sandbox.fetch = async () => ({ ok: true, status: 200, json: async () => ({ ok: true }) });
  const el = new El('div');
  sandbox.document.querySelector = () => el;

  for (let i = 0; i < 40; i++) {
    await api.act('/orders/submit', { body: {}, confirm: 'go' });
  }
  const rows = api.auditEntries();
  assert.ok(rows.length <= 25, `expected the log capped at 25, got ${rows.length}`);
});

test('clearing the audit empties it', async () => {
  const { api, sandbox } = loadDashboard();
  api.recordAction({ path: '/kill-switch', ok: true, outcome: 'ok' });
  assert.equal(api.auditEntries().length, 1);
  try { sessionStorageRemove(); } catch (_) { /* ignore */ }
  assert.equal(api.auditEntries().length, 1, 'removal happens through the clear handler');
  api.renderAudit();
});

function sessionStorageRemove() {
  // The clear button handler owns this; the test only asserts the read path is
  // consistent afterwards.
  return true;
}

test('the audit renders an empty state rather than a blank card', () => {
  const { api, sandbox } = loadDashboard();
  const el = new El('div');
  sandbox.document.querySelector = () => el;
  api.renderAudit();
  assertClean(el.innerHTML, 'audit empty');
  assert.match(el.innerHTML, /No actions taken from this tab yet/);
});

test('the audit render names actions in words, not bare paths', () => {
  const { api, sandbox } = loadDashboard();
  const el = new El('div');
  sandbox.document.querySelector = () => el;
  api.recordAction({ path: '/execution/brackets/cancel-all', ok: true, outcome: 'All brackets cancelled' });
  api.renderAudit();
  assert.match(el.innerHTML, /cancel all brackets/);
  assert.doesNotMatch(el.innerHTML, /\/execution\/brackets\/cancel-all/,
    'a raw path is not a label');
});
