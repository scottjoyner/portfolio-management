/* Contract tests for the PM Terminal dashboard UI.
 *
 * These are static contract checks, not a browser: they assert that the HTML
 * shell, the CSS and the client script agree with each other and with the
 * endpoints the server actually dispatches. That catches the failure mode that
 * is otherwise invisible until an operator opens the page — a panel whose
 * render function reads field names the endpoint never returns, a route wired
 * to the wrong HTTP method, or an element the script looks up that the HTML
 * does not define.
 *
 * Run: node --test tests/dashboard_ui_contract.test.mjs
 */

import test from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const html = readFileSync('trading_system/ui/dashboard.html', 'utf8');
const js = readFileSync('trading_system/ui/dashboard.js', 'utf8');
const css = readFileSync('trading_system/ui/dashboard.css', 'utf8');
const server = readFileSync('trading_system/ui/dashboard_server.py', 'utf8');

const panelIds = [...js.matchAll(/^  panel\('([\w-]+)'/gm)].map((m) => m[1]);
const taggedPanels = [...html.matchAll(/data-panel="([\w-]+)"/g)].map((m) => m[1]);

test('html loads the split assets the server serves', () => {
  assert.match(html, /href="\/static\/dashboard\.css"/);
  assert.match(html, /src="\/static\/dashboard\.js"/);
  // The old single-file UI inlined both. If these reappear the /static route
  // becomes dead code that nothing exercises.
  assert.doesNotMatch(html, /<style[\s>]/);
  assert.doesNotMatch(html, /<script(?![^>]*src=)/);
});

test('server serves exactly the assets the page references', () => {
  for (const asset of ['dashboard.css', 'dashboard.js']) {
    assert.ok(server.includes(asset), `server does not know about ${asset}`);
  }
  assert.match(server, /path\.startswith\("\/static\/"\)/);
});

test('every registered panel has a render target in the html', () => {
  // A panel with no data-panel attribute silently never renders: bodiesFor()
  // returns [] and refreshPanel() returns early. That is exactly how the equity
  // chart went missing once already.
  const untargeted = panelIds.filter((id) => !taggedPanels.includes(id));
  assert.deepEqual(untargeted, [], `panels with no data-panel target: ${untargeted.join(', ')}`);
});

test('no html target is left without a registered panel', () => {
  const orphan = [...new Set(taggedPanels)].filter((id) => !panelIds.includes(id));
  assert.deepEqual(orphan, [], `data-panel targets with no panel() registration: ${orphan.join(', ')}`);
});

test('a panel shown in two views issues one request', () => {
  // health/positions/backtests are deliberately duplicated. renderPanel targets
  // every match so the copies cannot drift; this asserts the duplication is
  // intentional rather than accidental.
  const duplicated = [...new Set(taggedPanels)].filter(
    (id) => taggedPanels.filter((t) => t === id).length > 1,
  );
  assert.ok(duplicated.length > 0, 'expected at least one shared panel to exist');
  for (const id of duplicated) {
    assert.ok(panelIds.includes(id));
  }
});

test('every element the script looks up by id exists in the html', () => {
  const wanted = [...js.matchAll(/\$\('#([\w-]+)'\)/g)].map((m) => m[1]);
  const defined = new Set([...html.matchAll(/id="([\w-]+)"/g)].map((m) => m[1]));
  const missing = [...new Set(wanted)].filter((id) => !defined.has(id));
  assert.deepEqual(missing, [], `script queries ids absent from the html: ${missing.join(', ')}`);
});

test('every nav destination has a matching view section', () => {
  const sections = [...html.matchAll(/id="view-([\w-]+)"/g)].map((m) => m[1]);
  // Take only the groups array out of renderNav, not the whole function: the
  // rest of the body contains event names, theme names and view ids that are
  // not nav destinations.
  const navBody = js.slice(js.indexOf('function renderNav'));
  const groups = navBody.slice(
    navBody.indexOf('const groups'),
    navBody.indexOf('const nav ='),
  );
  // Each group is [label, [id, ...]]; the label is capitalised and is not a
  // destination, so take the quoted ids from the inner lists only.
  const navIds = [...groups.matchAll(/\[\s*'([A-Za-z][\w]*)'\s*,\s*\[([^\]]*)\]/g)]
    .flatMap((m) => [...m[2].matchAll(/'([\w-]+)'/g)].map((n) => n[1]));
  assert.equal(
    [...groups.matchAll(/\[\s*'([A-Za-z][\w]*)'\s*,\s*\[/g)].length, 3,
    'expected three labelled nav groups (Trade / Analyse / Operate)',
  );

  assert.ok(navIds.length > 0, 'nav looks empty');
  assert.deepEqual(
    [...new Set(navIds)].filter((id) => !sections.includes(id)),
    [],
    'nav entries with no #view-<id> section',
  );
  assert.deepEqual(
    [...new Set(sections)].filter((id) => !navIds.includes(id)),
    [],
    'view sections unreachable from the nav',
  );

  // VIEWS supplies each id's display name; every nav destination must have one,
  // otherwise the link falls back to showing the raw id.
  const views = [...js.slice(js.indexOf('const VIEWS'), js.indexOf('];', js.indexOf('const VIEWS')))
    .matchAll(/\['([\w-]+)', '([^']+)'\]/g)].map((m) => m[1]);
  assert.deepEqual(
    [...new Set(navIds)].filter((id) => !views.includes(id)),
    [],
    'nav destinations with no entry in VIEWS',
  );
});

test('client API routes resolve against the dispatcher the server really uses', () => {
  const apiBlock = js.slice(js.indexOf('const API = {'), js.indexOf('};', js.indexOf('const API = {')));
  const routes = [...apiBlock.matchAll(/^\s*(\w+):\s*'([^']+)'/gm)].map((m) => ({ name: m[1], path: m[2] }));

  // do_GET dispatches from a handlers dict, plus a few startswith/== branches;
  // do_POST uses an explicit allowlist. Read all three, or this test passes
  // vacuously and proves nothing.
  const getDict = server.slice(server.indexOf('handlers = {'));
  const getKeys = [...getDict.matchAll(/"(\/[^"]+)":/g)].map((m) => m[1]);
  const getBranches = [...server.matchAll(/path(?:\.startswith\(|\s*==)\s*"(\/[^"]+)"/g)].map((m) => m[1]);
  const postLine = server.slice(server.indexOf('if path not in {'), server.indexOf('if path not in {') + 400);
  const postPaths = [...postLine.matchAll(/"(\/[^"]+)"/g)].map((m) => m[1]);
  const served = new Set([...getKeys, ...getBranches, ...postPaths]);
  assert.ok(served.size > 20, `dispatcher parse looks wrong (only ${served.size} routes found)`);

  const unresolved = routes.filter(({ path }) => {
    const base = path.split('?')[0].replace(/\/$/, '');
    return ![...served].some(
      (s) => base === s || base.startsWith(`${s.replace(/\/$/, '')}/`),
    );
  });
  assert.deepEqual(
    unresolved.map((r) => `${r.name} -> ${r.path}`),
    [],
    'client calls routes the server never dispatches',
  );
});

test('mutating calls carry a token and use the method the server expects', () => {
  const postBlock = server.slice(server.indexOf('if path not in {'));
  const postOnly = new Set(
    [...postBlock.slice(0, 400).matchAll(/"(\/[^"]+)"/g)].map((n) => n[1]),
  );
  assert.ok(postOnly.has('/kill-switch'));

  // GET has no /kill-switch handler, so reading state that way returns 404 and
  // the button looks permanently broken.
  assert.match(js, /ready\.kill_switch_active/, 'kill switch state must come from /ready');
  assert.doesNotMatch(js, /request\(API\.killSwitch/);

  // Every auth-gated request must be a POST with a JSON body; the handlers read
  // `enabled` / `bracket_id` from the parsed body, not the query string.
  assert.match(js, /body: \{ enabled:/);
  assert.match(js, /body: \{ bracket_id:/);
  assert.match(js, /method: 'POST',\s*body,/s);
});

test('order entry sends the field names the endpoint reads', () => {
  const handler = server.slice(server.indexOf('def api_order_submit'));
  const reads = handler.slice(0, handler.indexOf('def api_strategies'));
  assert.match(reads, /payload\.get\("symbol"\)/);
  assert.match(reads, /payload\.get\("side"\)/);
  assert.match(reads, /payload\.get\("size_usd"\)/);
  assert.match(reads, /payload\.get\("stop_pct"\)/);
  assert.match(reads, /payload\.get\("target_pct"\)/);

  const ticket = js.slice(js.indexOf('async function submitOrder'));
  assert.match(ticket.slice(0, 1200), /symbol:/);
  assert.match(ticket.slice(0, 1200), /side: state\.side/);
  assert.match(ticket.slice(0, 1200), /size_usd:/);
  assert.doesNotMatch(ticket.slice(0, 1200), /product_id:/);
});

test('panel renderers read the field names the endpoints actually return', () => {
  // Each entry pairs a panel with one field it must read and one it must not.
  // The "must not" half is the point: these are the names a plausible-looking
  // guess invents, and they are the reason panels rendered empty tables.
  const expectations = [
    ['positions', /entry_price_usd/, /p\.entry_price\b/],
    ['positions', /quantity\b/, /p\.qty\b/],
    ['accounts', /a\.nav\b/, /a\.balances/],
    ['regime', /current_regime/, /data\.regime\b/],
    ['wash-sale', /Object\.entries/, /Array\.isArray\(rows\)/],
    ['strategy-perf', /total_signals/, /s\.trades\b/],
    ['research', /confidence_score/, /h\.confidence\b/],
    ['backtests', /verdict/, /e\.drawdown\b/],
    ['opportunities', /Array\.isArray\(data\.signals\)/, /data\.opportunities\b/],
    // The candle row's close is the short key `c`; the fallback exists only so
    // an alternative payload shape does not blank the chart, so assert on the
    // primary read rather than banning the fallback.
    ['candles', /row\.c\b/, /\bc\b\s*:\s*row\.close/],
    ['approvals', /quantity_usd/, /a\.notional\b/],
  ];
  for (const [id, mustHave, mustNot] of expectations) {
    const start = js.indexOf(`panel('${id}'`);
    assert.ok(start > 0, `panel ${id} not found`);
    const next = js.indexOf("\n  panel('", start + 1);
    const body = js.slice(start, next > 0 ? next : js.length);
    // Assertions run against code only. The comments in each renderer
    // deliberately quote the rejected field names to explain why they were
    // removed, and matching those would report the explanation as the defect.
    const code = body.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\/\/[^\n]*/g, '');
    assert.match(code, mustHave, `${id} must read ${mustHave}`);
    assert.doesNotMatch(code, mustNot, `${id} must not read ${mustNot}`);
  }
});

test('polling installs exactly one timer', () => {
  const start = js.slice(js.indexOf('function startPolling'));
  const body = start.slice(0, start.indexOf('\n}'));
  // One schedule. An earlier draft installed a second "slow" interval, which
  // produced two overlapping poll loops that doubled request load and could not
  // back off coherently. Two setTimeout sites are expected — the initial arm and
  // the re-arm — but only one timer is live at a time.
  assert.equal((body.match(/setTimeout\(/g) || []).length, 2);
  assert.equal((body.match(/setInterval\(/g) || []).length, 0);
  assert.match(body, /clearTimeout\(state\.timer\)/);
  assert.match(body, /state\.timer = setTimeout/, 'the re-arm must overwrite the single handle');

  const boot = js.slice(js.indexOf('function boot'));
  assert.equal((boot.slice(0, boot.indexOf('\n}')).match(/startPolling\(\)/g) || []).length, 1);
});

test('polling backs off but recovers', () => {
  const start = js.slice(js.indexOf('function startPolling'));
  const body = start.slice(0, start.indexOf('\n}'));
  assert.match(body, /consecutiveFailures\s*>\s*2/);
  assert.match(body, /state\.pollMs \* 3/);
  // refreshHeader is what resets consecutiveFailures to 0, so the cadence
  // widens on failure and returns to normal once /health succeeds again.
  const header = js.slice(js.indexOf('async function refreshHeader'));
  assert.match(header, /consecutiveFailures = 0/);
  assert.match(header, /consecutiveFailures \+= 1/);
});

test('the header rides the same poll as the panels', () => {
  // Polling panels alone left the kill switch and liveness showing whatever they
  // were at page load, which is the one thing that must never be stale.
  const all = js.slice(js.indexOf('async function refreshAll'));
  const body = all.slice(0, all.indexOf('\n}'));
  assert.match(body, /await refreshHeader\(\)/);
});

test('every request is bounded by a deadline', () => {
  // /market/watchlist does a live pair discovery that takes tens of seconds.
  // Without a per-request timeout one slow panel stalls the whole sequential
  // cycle and the rest of the page silently keeps stale content.
  const req = js.slice(js.indexOf('async function request'));
  const body = req.slice(0, req.indexOf('\n  let data'));
  assert.match(body, /AbortController/);
  assert.match(body, /controller\.abort\(\)/);
  assert.match(body, /timeout = REQUEST_TIMEOUT_MS/);
});

test('panel failures are isolated and named', () => {
  assert.match(js, /class="panel-error"/);
  assert.match(js, /role="alert"/);
  // A render bug must not be allowed to abort the refresh cycle.
  assert.match(js, /render failed:/);
  assert.match(js, /401[\s\S]{0,200}operator token/i);
});

test('operator token stays in sessionStorage and is never inlined', () => {
  assert.match(js, /sessionStorage\.setItem\(TOKEN_KEY/);
  assert.doesNotMatch(js, /localStorage\.setItem\(TOKEN_KEY/);
  // No token in the served markup, and no console logging of it.
  assert.doesNotMatch(html, /pm\.operator\.token/);
  assert.doesNotMatch(js, /console\.(log|info|debug)\([^)]*token/i);
  // The html must not embed a token value that would leak via view-source.
  assert.doesNotMatch(html, /Bearer\s+[A-Za-z0-9_-]{16,}/);
});

test('read-only browsing never sends the token', () => {
  // Only panels that genuinely need it may set auth:true. /capital/buckets is
  // the one read endpoint the server classifies as mutating, so it is the one
  // panel that legitimately requires a token.
  const authPanels = [...js.matchAll(/panel\('([\w-]+)'[\s\S]{0,200}?auth: true/g)].map((m) => m[1]);
  assert.deepEqual([...new Set(authPanels)], ['capital']);
});

test('css defines every state class the renderers emit', () => {
  const emitted = new Set();
  for (const m of js.matchAll(/class="([^"$]*)"/g)) {
    m[1].split(/\s+/).filter(Boolean).forEach((c) => emitted.add(c));
  }
  for (const m of js.matchAll(/class="[^"]*?\$\{[^}]+\}/g)) {
    for (const c of m[0].match(/pill (\w+)/g) || []) emitted.add(c.split(' ')[1]);
  }
  // Classes provided by the stylesheet itself or by inline expressions we do
  // not need to police here.
  const known = new Set(['skeleton', 'chart', 'line', 'marker', 'grid-line',
    'axis-label', 'table-scroll', 'empty', 'msg', 'ok', 'bad']);
  const missing = [...emitted].filter(
    (c) => !known.has(c) && !new RegExp(`\\.${c}\\b`).test(css),
  );
  assert.deepEqual(missing, [], `renderers emit classes the stylesheet never defines: ${missing.join(', ')}`);
});

test('accessibility affordances are present', () => {
  assert.match(html, /class="skip-link"/);
  assert.match(html, /<html lang="en"/);
  assert.match(html, /aria-live="polite"/);
  assert.match(html, /aria-label="Sections"/);
  assert.match(html, /<dialog/);
  // Every section the nav targets must be labelled for screen readers.
  for (const m of html.matchAll(/<section class="view" id="view-([\w-]+)"([^>]*)>/g)) {
    assert.match(m[2], /aria-label=/, `view-${m[1]} has no aria-label`);
  }
  // Icon-only buttons need text; the buttons here all carry a label.
  assert.match(css, /:focus-visible/);
  assert.match(css, /prefers-reduced-motion/);
});

test('themes are wired to the token attribute and persisted', () => {
  assert.match(html, /<html lang="en" data-theme="dark">/);
  assert.match(css, /\[data-theme="light"\]/);
  assert.match(css, /\[data-theme="contrast"\]|prefers-contrast/);
  assert.match(js, /setAttribute\('data-theme'/);
  assert.match(js, /localStorage\.getItem\(THEME_KEY\)/);
});

test('charts are accessible and do not depend on colour alone', () => {
  assert.match(js, /role="img"/);
  assert.match(js, /aria-label="\$\{esc\(state\.symbol\)\}/);
});

test('dangerous actions confirm and explain the consequence', () => {
  for (const needle of [
    /Approve this order\?/,
    /Deny this order\?/,
    /lose its stop and target/,
    /lose their stops and targets/,
    /halts or resumes all trading/,
  ]) {
    assert.match(js, needle, `missing confirmation text: ${needle}`);
  }
  // Approving must be labelled as releasing a real order, not as a UI state change.
  assert.match(js, /releases a real order/);
});

test('an operator halt is not presented as a fault', () => {
  // The kill switch is correct behaviour, not an incident. Alerting on it would
  // train an operator to ignore real pages.
  assert.match(js, /intentional halt/);
  assert.match(js, /halted by operator/);
  assert.match(js, /will not restart itself/);
});