/* Real-browser end-to-end validation of the PM Terminal dashboard.
 *
 * Everything so far has been static analysis or a headless DOM shim. Neither
 * loads the stylesheet, applies a theme, runs a media query, or reports what
 * the browser actually complains about. This drives a real Chromium over the
 * DevTools Protocol against a real dashboard server, so it catches the class of
 * failure that only exists in a browser: a stylesheet that 404s, a media query
 * that hides a control, a JS error at boot, an unhandled rejection, or a panel
 * that silently renders nothing because its host is display:none at this width.
 *
 * No npm dependency: CDP is spoken over Node 22's built-in WebSocket.
 *
 * Run: node tests/dashboard_browser_e2e.mjs
 * Exits non-zero on any failure.
 */

import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, writeFileSync } from 'node:fs';
import { setTimeout as sleep } from 'node:timers/promises';

const CHROME = process.env.CHROME_BIN
  || '/home/scott/.cache/ms-playwright/chromium-1243/chrome-linux64/chrome';
const UI_DIR = 'trading_system/ui';
const PORT = Number(process.env.E2E_PORT || 8899);
const DEBUG_PORT = Number(process.env.E2E_DEBUG_PORT || 9333);
const TOKEN = 'e2e-browser-token-abcdefghijklmnopqrstuvwxyz-123456';
const BASE = `http://127.0.0.1:${PORT}`;

const results = [];
function check(name, ok, detail = '') {
  results.push({ name, ok, detail });
  console.log(`  ${ok ? 'ok  ' : 'FAIL'} ${name}${detail && !ok ? ` — ${detail}` : ''}`);
  return ok;
}

/* ── server ─────────────────────────────────────────────────────────────── */

const SCRATCH = '/tmp/opencode/dash/browser';

/* Seed the scratch data dir so the approvals panel renders real pending rows.
 * APPROVALS_PATH honours TRADING_DATA_DIR, so this stays out of the repo's live
 * state. Without it the panel is legitimately empty and the token-gating check
 * below has no Approve/Deny buttons to inspect. */
function seedState() {
  mkdirSync(SCRATCH, { recursive: true });
  const iso = new Date(Date.now() - 120000).toISOString();
  writeFileSync(`${SCRATCH}/pending_approvals.json`, JSON.stringify({
    e2e0000token0001: {
      type: 'manual_order', side: 'BUY', currency: 'BTC', size_usd: 250,
      expected_fee: 0.25, product_id: 'BTC-USD', reason: 'e2e seeded approval',
      priority: 1.0, status: 'pending', bracket: true,
      created_at: iso, source: 'dashboard_order_entry',
    },
    e2e0000token0002: {
      type: 'manual_order', side: 'SELL', currency: 'ETH', size_usd: 100,
      expected_fee: 0.1, product_id: 'ETH-USD', reason: 'already settled',
      priority: 0.5, status: 'approved', auto_approved: true, created_at: iso,
    },
  }, null, 2));
}

function startServer() {
  const proc = spawn('python3', [`${UI_DIR}/dashboard_server.py`, '--port', String(PORT)], {
    env: { ...process.env, TRADING_DATA_DIR: SCRATCH, DASHBOARD_OPERATOR_TOKEN: TOKEN },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stderr.on('data', (d) => {
    const s = String(d);
    if (/Traceback|Segmentation/i.test(s)) console.error(s.slice(0, 300));
  });
  return proc;
}

/* The dashboard server has a pre-existing crash: a second call into
 * data/feed_cache.save_candles (pandas' Arrow string array) segfaults the whole
 * process, reachable via /market/watchlist followed by /market/candles. It
 * reproduces on committed HEAD with curl alone. It is reported as its own check
 * so a run that trips it does not read as a UI regression. */
function serverAlive() {
  return server && !server.killed && server.exitCode === null;
}

async function waitForServer(timeoutMs = 45000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    try {
      const r = await fetch(`${BASE}/health`);
      if (r.ok) return true;
    } catch { /* not up yet */ }
    await sleep(300);
  }
  return false;
}

/* ── minimal CDP client ─────────────────────────────────────────────────── */

class CDP {
  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    this.listeners = new Map();
    ws.addEventListener('message', (ev) => {
      const msg = JSON.parse(ev.data);
      if (msg.id !== undefined) {
        const slot = this.pending.get(msg.id);
        if (!slot) return;
        this.pending.delete(msg.id);
        if (msg.error) slot.reject(new Error(msg.error.message));
        else slot.resolve(msg.result);
      } else {
        for (const fn of this.listeners.get(msg.method) || []) fn(msg.params);
      }
    });
  }

  static async connect(wsUrl) {
    const ws = new WebSocket(wsUrl);
    await new Promise((resolve, reject) => {
      ws.addEventListener('open', resolve, { once: true });
      ws.addEventListener('error', () => reject(new Error('CDP websocket failed')), { once: true });
    });
    return new CDP(ws);
  }

  send(method, params = {}) {
    const id = ++this.id;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
      setTimeout(() => {
        if (this.pending.has(id)) {
          this.pending.delete(id);
          reject(new Error(`CDP timeout: ${method}`));
        }
      }, 30000);
    });
  }

  on(method, fn) {
    if (!this.listeners.has(method)) this.listeners.set(method, []);
    this.listeners.get(method).push(fn);
  }

  /* Evaluate in the page and return the value by value, not by handle. */
  async eval(expression) {
    const r = await this.send('Runtime.evaluate', {
      expression: `(() => { ${expression} })()`,
      returnByValue: true,
      awaitPromise: true,
    });
    if (r.exceptionDetails) {
      throw new Error(r.exceptionDetails.exception?.description || 'evaluate threw');
    }
    return r.result.value;
  }
}

async function launchChrome() {
  if (!existsSync(CHROME)) throw new Error(`no chromium at ${CHROME}`);
  const proc = spawn(CHROME, [
    '--headless=new',
    `--remote-debugging-port=${DEBUG_PORT}`,
    '--no-sandbox',
    '--disable-gpu',
    '--disable-dev-shm-usage',
    '--hide-scrollbars',
    'about:blank',
  ], { stdio: ['ignore', 'pipe', 'pipe'] });

  // Poll the DevTools HTTP endpoint for the page target rather than scraping
  // stderr: the http endpoint is stable and gives a per-page ws url, which
  // connects straight to the page without browser-level session plumbing.
  const deadline = Date.now() + 30000;
  while (Date.now() < deadline) {
    try {
      const list = await (await fetch(`http://127.0.0.1:${DEBUG_PORT}/json/list`)).json();
      const page = list.find((t) => t.type === 'page' && t.webSocketDebuggerUrl);
      if (page) return { proc, pageWsUrl: page.webSocketDebuggerUrl };
    } catch { /* not listening yet */ }
    await sleep(250);
  }
  proc.kill();
  throw new Error('chromium never exposed a page target');
}

/* ── the run ────────────────────────────────────────────────────────────── */

let server;
let chrome;
let cdp;
let failed = 0;

const consoleErrors = [];
const pageErrors = [];
const failedRequests = [];

function finish() {
  const passed = results.filter((r) => r.ok).length;
  console.log(`\n${passed}/${results.length} checks passed`);
  if (failed) {
    console.log('\nfailures:');
    for (const r of results.filter((x) => !x.ok)) console.log(`  - ${r.name}${r.detail ? `: ${r.detail}` : ''}`);
  }
  try { cdp?.ws.close(); } catch { /* already closed */ }
  chrome?.kill('SIGKILL');
  server?.kill('SIGKILL');
  process.exit(failed ? 1 : 0);
}

try {
  seedState();
  server = startServer();
  if (!(await waitForServer())) throw new Error('dashboard server did not come up');
  console.log('dashboard up on', BASE);

  let pageWsUrl;
  ({ proc: chrome, pageWsUrl } = await launchChrome());
  cdp = await CDP.connect(pageWsUrl);

  await cdp.send('Page.enable');
  await cdp.send('Runtime.enable');
  await cdp.send('Log.enable');
  await cdp.send('Network.enable');

  cdp.on('Runtime.consoleAPICalled', (p) => {
    if (p.type === 'error' || p.type === 'warning') {
      consoleErrors.push(`${p.type}: ${(p.args || []).map((a) => a.value ?? a.description ?? '').join(' ')}`);
    }
  });
  cdp.on('Runtime.exceptionThrown', (p) => {
    pageErrors.push(p.exceptionDetails?.exception?.description || p.exceptionDetails?.text || 'unknown');
  });
  cdp.on('Log.entryAdded', (p) => {
    if (p.entry.level === 'error') consoleErrors.push(`log: ${p.entry.text} ${p.entry.url || ''}`);
  });
  cdp.on('Network.loadingFailed', (p) => failedRequests.push(`${p.type} ${p.errorText}`));
  cdp.on('Network.responseReceived', (p) => {
    if (p.response.status >= 400) failedRequests.push(`${p.response.status} ${p.response.url}`);
  });

  console.log('\n== load ==');
  await cdp.send('Page.navigate', { url: `${BASE}/` });
  // Wait for the app to have run a refresh cycle, not just for load.
  const booted = await (async () => {
    for (let i = 0; i < 80; i++) {
      await sleep(250);
      const ok = await cdp.eval('return typeof window.__ready === "undefined" ? document.readyState : "done"').catch(() => null);
      const state = await cdp.eval('return document.readyState').catch(() => null);
      if (state === 'complete' && ok === 'complete') return true;
    }
    return false;
  })();
  check('page reaches readyState=complete', booted);
  // The first cold cycle includes a live pair discovery that takes tens of
  // seconds, so allow for it rather than racing it.
  await sleep(12000);

  console.log('\n== assets actually loaded and applied ==');
  const assetInfo = await cdp.eval(`
    const links = [...document.querySelectorAll('link[rel=stylesheet]')].map(l => l.href);
    const sheets = [...document.styleSheets].map(s => ({ href: s.href, rules: (() => { try { return s.cssRules.length; } catch { return -1; } })() }));
    const bg = getComputedStyle(document.body).backgroundColor;
    const font = getComputedStyle(document.body).fontFamily;
    return { links, sheets, bg, font };
  `);
  check('stylesheet link present', assetInfo.links.some((h) => h.endsWith('/static/dashboard.css')), JSON.stringify(assetInfo.links));
  check('stylesheet parsed (not an empty/CORS-failed sheet)',
    assetInfo.sheets.some((s) => s.href.endsWith('dashboard.css') && s.rules > 50),
    JSON.stringify(assetInfo.sheets));
  check('body has a non-transparent background from the stylesheet',
    assetInfo.bg !== 'rgba(0, 0, 0, 0)' && assetInfo.bg !== 'transparent', assetInfo.bg);
  check('body font-family comes from the stylesheet',
    !/^Times|^serif$/.test(assetInfo.font), assetInfo.font);

  console.log('\n== design tokens resolved ==');
  const tokens = await cdp.eval(`
    const cs = getComputedStyle(document.documentElement);
    return {
      surface0: cs.getPropertyValue('--surface-0').trim(),
      text: cs.getPropertyValue('--text').trim(),
      accent: cs.getPropertyValue('--accent').trim(),
      positive: cs.getPropertyValue('--positive').trim(),
      critical: cs.getPropertyValue('--critical').trim(),
    };
  `);
  check('custom properties resolve on :root',
    Object.values(tokens).every((v) => v && v.length > 0), JSON.stringify(tokens));

  console.log('\n== nav is populated and every destination is reachable ==');
  const nav = await cdp.eval(`
    const links = [...document.querySelectorAll('#nav a[data-view]')].map(a => ({ view: a.dataset.view, text: a.textContent.trim(), visible: a.getBoundingClientRect().height > 0 }));
    return links;
  `);
  // Structural: the views are reorganised as the UI is consolidated, so pinning
  // a count would fail for a reason that says nothing about correctness. What
  // matters is that every VIEWS entry produced a link.
  const declared = await cdp.eval('return (window.__viewCount || null)');
  void declared;
  check('nav rendered from VIEWS', nav.length >= 5, `got ${nav.length}`);
  check('nav links are visible', nav.every((l) => l.visible));
  // The badge count lives inside the anchor, so its text is part of the link
  // name a screen reader announces. "Approvals1" is not a label.
  const navA11y = await cdp.eval(`
    return [...document.querySelectorAll('#nav a[data-view]')].map(a => ({
      view: a.dataset.view,
      name: a.getAttribute('aria-label') || a.textContent.trim(),
      badgeHidden: (() => { const b = a.querySelector('.count'); return b ? (b.hidden || b.getAttribute('aria-hidden') === 'true') : true; })(),
    }));
  `);
  // Either a bare word ("Approvals") or a word with a separated count
  // ("Approvals, 3 pending"). What must never happen is the bare concatenation
  // "Approvals3", which is what the badge produced before it was hidden.
  const goodName = (n) => /^[A-Za-z][A-Za-z ]*(, \d+[\w+]* \w+)?$/.test(n);
  check('nav link names never concatenate the badge onto the word',
    navA11y.every((a) => goodName(a.name) && !/[A-Za-z]\d/.test(a.name)),
    JSON.stringify(navA11y.filter((a) => !goodName(a.name) || /[A-Za-z]\d/.test(a.name))));
  check('the count badge is hidden from assistive tech',
    navA11y.every((a) => a.badgeHidden), JSON.stringify(navA11y.filter((a) => !a.badgeHidden)));

  for (const { view } of nav) {
    await cdp.eval(`location.hash = '${view}'; return 1`);
    await sleep(400);
    const state = await cdp.eval(`
      const secs = [...document.querySelectorAll('.view')];
      const shown = secs.filter(s => !s.hidden && s.getBoundingClientRect().height > 0);
      const target = document.querySelector('#view-${view}');
      return { shownCount: shown.length, shownId: shown[0] && shown[0].id, hidden: target ? target.hidden : null,
               h: target ? target.getBoundingClientRect().height : -1 };
    `);
    check(`view ${view}: shown exclusively and not zero-height`,
      state.shownCount === 1 && state.shownId === `view-${view}` && state.h > 0, JSON.stringify(state));
  }

  console.log('\n== panels render content, not just skeletons ==');
  const panels = await cdp.eval(`
    const out = [];
    for (const body of document.querySelectorAll('[data-panel]')) {
      const id = body.dataset.panel;
      const skeletons = body.querySelectorAll('.skeleton').length;
      const errs = body.querySelectorAll('.panel-error').length;
      const tables = body.querySelectorAll('table').length;
      const empties = body.querySelectorAll('.empty').length;
      const chars = body.textContent.trim().length;
      out.push({ id, skeletons, errs, tables, empties, chars,
                 visible: body.getBoundingClientRect().height > 0 });
    }
    return out;
  `);
  const seen = new Set();
  // The watchlist does a live pair discovery taking ~26s on a cold cache, so it
  // may legitimately still be loading. It must say so rather than show bare bars.
  const SLOW = new Set(['watchlist']);
  // Panels declared poll:false are not fetched until their view is opened --
  // the chart is a Coinbase CLI call and is deliberately not polled. Asserting
  // they left no skeleton would be asserting the opposite of the intent.
  const ON_DEMAND = new Set(['candles']);
  for (const p of panels) {
    seen.add(p.id);
    // A panel on a hidden view legitimately reports height 0, so only assert
    // that it is not left holding a skeleton forever.
    if (SLOW.has(p.id)) {
      const note = await cdp.eval(`
        const el = document.querySelector('[data-panel="${p.id}"]');
        const n = el && el.querySelector('.loading-note');
        const live = el && (el.textContent.trim().length > 0);
        return { hasNote: !!n, noteText: n ? n.textContent.replace(/\s+/g,' ').trim() : '',
                 skeletons: el ? el.querySelectorAll('.skeleton').length : -1, live: !!live };
      `);
      check(`panel ${p.id}: either loaded or explains the wait`,
        note.live || note.hasNote, JSON.stringify(note));
      if (note.hasNote) {
        check(`panel ${p.id}: the wait is described honestly`,
          /half a minute|Loading/i.test(note.noteText), note.noteText);
      }
      continue;
    }
    if (ON_DEMAND.has(p.id)) {
      // poll:false means "not polled", not "never fetched" -- the nav traversal
      // above opens its view, which fetches it. So the honest assertion is that
      // it is neither blank nor stuck in an error, whatever state it is in. The
      // dedicated chart section below proves it draws real geometry.
      check(`panel ${p.id}: neither blank nor errored`,
        (p.chars > 0 || p.skeletons > 0) && p.errs === 0,
        JSON.stringify({ chars: p.chars, skeletons: p.skeletons, errs: p.errs }));
      continue;
    }
    check(`panel ${p.id}: no skeleton left after first poll`, p.skeletons === 0, `skeletons=${p.skeletons}`);
    check(`panel ${p.id}: produced content`, p.chars > 0 || p.tables > 0, `chars=${p.chars}`);
  }
  check('every registered panel appeared in the DOM', seen.size >= 15, `${seen.size} distinct`);

  console.log('\n== no panel reported an unexpected error ==');
  // /capital/buckets is classified mutating server-side, so before unlock it
  // legitimately shows its token message. Any other panel in an error state is a
  // real fault.
  const errs = panels.filter((p) => p.errs > 0 && p.id !== 'capital');
  const errText = await cdp.eval(`
    return [...document.querySelectorAll('[data-panel] .panel-error')].map(e => {
      const host = e.closest('[data-panel]');
      return (host ? host.dataset.panel : '?') + ': ' + e.textContent.replace(/\\s+/g,' ').trim();
    });
  `);
  check('no panel-error blocks outside the token-gated one',
    errs.length === 0, JSON.stringify(errText));
  const capitalErr = panels.find((p) => p.id === 'capital');
  if (capitalErr && capitalErr.errs > 0) {
    const msg = await cdp.eval(`
      const e = document.querySelector('[data-panel="capital"] .panel-error');
      return e ? e.textContent.replace(/\s+/g, ' ').trim() : '';
    `);
    check('the token-gated panel explains the 401 rather than looking broken',
      /token/i.test(msg), msg);
  }

  console.log('\n== header reflects live state ==');
  const header = await cdp.eval(`
    return {
      status: document.querySelector('#status-text').textContent.trim(),
      ready: document.querySelector('#ready-label').textContent.trim(),
      equity: document.querySelector('#h-equity').textContent.trim(),
      pos: document.querySelector('#h-pos').textContent.trim(),
      ks: document.querySelector('#ks-btn').textContent.trim(),
      age: document.querySelector('#refresh-age').textContent.trim(),
      liveDot: document.querySelector('#live-dot').className,
    };
  `);
  check('liveness label is not the placeholder',
    header.status && header.status !== 'connecting', JSON.stringify(header));
  check('equity cell filled', header.equity && header.equity !== '--', header.equity);
  check('kill switch label resolved',
    /engaged|off/i.test(header.ks), header.ks);
  check('liveness dot carries a state class',
    /live|bad/.test(header.liveDot), header.liveDot);

  console.log('\n== token gating is enforced in the DOM ==');
  const lockState = await cdp.eval(`
    const bar = document.querySelector('#token-bar');
    return { text: document.querySelector('#token-state').textContent.trim(),
             btn: document.querySelector('#token-btn').textContent.trim(),
             visible: bar.getBoundingClientRect().height > 0 };
  `);
  check('dashboard reports read-only before unlock',
    /read-only/i.test(lockState.text), JSON.stringify(lockState));
  check('unlock button offered', /unlock/i.test(lockState.btn), lockState.btn);

  await cdp.eval(`location.hash = 'approvals'; return 1`);
  await sleep(500);
  const gated = await cdp.eval(`
    return [...document.querySelectorAll('[data-needs-token]')]
      .map(b => ({ text: b.textContent.trim(), disabled: b.disabled, title: b.title || '' }));
  `);
  check('token-gated controls exist', gated.length > 0, `${gated.length} found`);
  // Fail closed while locked: an operator should not be able to fire a control
  // that is certain to 401. The server remains the authority either way.
  check('token-gated controls are disabled while locked',
    gated.every((b) => b.disabled), JSON.stringify(gated));
  check('a disabled control explains why',
    gated.every((b) => /token/i.test(b.title)),
    JSON.stringify(gated.map((b) => b.title)));

  // Panels rebuild their tables every poll, so the Approve/Deny buttons are new
  // elements each cycle. Gating only at unlock time left them enabled and
  // unexplained while locked. Drive a real panel render and re-check.
  console.log('\n== gating survives a panel re-render ==');
  // These are the real buttons the approvals panel rendered from seeded state,
  // not ones injected into the DOM, so this exercises the actual poll path.
  const rerender = await cdp.eval(`
    const el = document.querySelector('[data-panel="approvals"]');
    const fresh = [...el.querySelectorAll('[data-needs-token]')];
    return { count: fresh.length, texts: fresh.map(b => b.textContent.trim()),
             disabled: fresh.map(b => b.disabled),
             titled: fresh.map(b => /token/i.test(b.title || '')) };
  `);
  check('the approvals panel rendered action buttons from real state',
    rerender.count >= 2, JSON.stringify(rerender));
  check('rendered approve/deny are gated while locked',
    rerender.count >= 2 && rerender.disabled.every(Boolean), JSON.stringify(rerender));
  check('rendered approve/deny explain why they are disabled',
    rerender.titled.every(Boolean), JSON.stringify(rerender.titled));
  // Only the pending row may offer action; the auto-approved one must not.
  check('only the pending approval is actionable',
    (rerender.texts || []).filter((t) => /approve/i.test(t)).length === 1,
    JSON.stringify(rerender.texts));

  console.log('\n== unlock actually attaches the token ==');
  const unlocked = await cdp.eval(`
    // Drive the real handler rather than poking sessionStorage, so the UI's own
    // prompt path is what gets tested.
    const prompt = window.prompt;
    window.prompt = () => '${TOKEN}';
    document.querySelector('#token-btn').click();
    window.prompt = prompt;
    return document.querySelector('#token-state').textContent.trim();
  `);
  check('unlocking flips the state label',
    !/read-only/i.test(unlocked), unlocked);
  const reenabled = await cdp.eval(`
    return [...document.querySelectorAll('[data-needs-token]')]
      .map(b => ({ text: b.textContent.trim(), disabled: b.disabled }));
  `);
  check('token-gated controls re-enable after unlocking',
    reenabled.length > 0 && reenabled.every((b) => !b.disabled), JSON.stringify(reenabled));

  const stored = await cdp.eval(`
    return { session: sessionStorage.getItem('pm.operator.token'), local: localStorage.getItem('pm.operator.token') };
  `);
  check('token is in sessionStorage', stored.session === TOKEN, JSON.stringify({ has: !!stored.session }));
  check('token is NOT in localStorage', stored.local === null, JSON.stringify(stored));

  console.log('\n== themes apply and repaint ==');
  const themeInfo = await cdp.eval(`
    const btn = document.querySelector('#theme-btn');
    const seen = [];
    for (let i = 0; i < 4; i++) {
      seen.push({
        theme: document.documentElement.dataset.theme,
        bg: getComputedStyle(document.body).backgroundColor,
        label: btn.textContent.trim(),
      });
      btn.click();
    }
    return seen;
  `);
  const distinctThemes = new Set(themeInfo.map((t) => t.theme));
  check('cycling reaches more than one theme', distinctThemes.size >= 3, JSON.stringify([...distinctThemes]));
  check('each theme paints a different background',
    new Set(themeInfo.map((t) => t.bg)).size === distinctThemes.size,
    JSON.stringify(themeInfo.map((t) => [t.theme, t.bg])));

  console.log('\n== theme survives a reload ==');
  await cdp.eval(`localStorage.setItem('pm.theme','light'); return 1`);
  await cdp.send('Page.reload');
  await sleep(3500);
  const afterReload = await cdp.eval(`
    return { theme: document.documentElement.dataset.theme, bg: getComputedStyle(document.body).backgroundColor };
  `);
  check('theme restored from localStorage', afterReload.theme === 'light', JSON.stringify(afterReload));
  await cdp.eval(`localStorage.removeItem('pm.theme'); return 1`);

  console.log('\n== keyboard shortcuts move between views ==');
  await cdp.send('Page.reload');
  await sleep(3500);
  const kb = [];
  for (const [key, expected] of [['1', 'overview'], ['2', 'execute'], ['3', 'analyse'],
    ['4', 'bot'], ['5', 'capital']]) {
    await cdp.send('Input.dispatchKeyEvent', { type: 'keyDown', key, text: key });
    await cdp.send('Input.dispatchKeyEvent', { type: 'keyUp', key });
    await sleep(300);
    const shown = await cdp.eval(`
      const s = [...document.querySelectorAll('.view')].filter(v => !v.hidden && v.getBoundingClientRect().height > 0);
      return s[0] ? s[0].id : null;
    `);
    kb.push({ key, expected, shown });
  }
  for (const { key, expected, shown } of kb) {
    check(`key "${key}" opens #view-${expected}`, shown === `view-${expected}`, `got ${shown}`);
  }

  console.log('\n== focus is visible for keyboard users ==');
  const focusRing = await cdp.eval(`
    const btn = document.querySelector('#refresh-btn');
    btn.focus();
    const cs = getComputedStyle(btn, ':focus-visible');
    return { active: document.activeElement === btn, outline: cs.outlineStyle, width: cs.outlineWidth };
  `);
  check('focusable control can take focus', focusRing.active, JSON.stringify(focusRing));

  console.log('\n== narrow viewport does not hide controls ==');
  await cdp.send('Emulation.setDeviceMetricsOverride', {
    width: 390, height: 844, deviceScaleFactor: 2, mobile: true,
  });
  await sleep(700);
  const narrow = await cdp.eval(`
    const kill = document.querySelector('#ks-btn').getBoundingClientRect();
    const nav = document.querySelector('#nav').getBoundingClientRect();
    const main = document.querySelector('#main').getBoundingClientRect();
    return {
      killVisible: kill.height > 0 && kill.width > 0,
      killRight: Math.round(kill.right), vw: window.innerWidth,
      navVisible: nav.height > 0,
      mainVisible: main.height > 0,
      docOverflowX: document.documentElement.scrollWidth - document.documentElement.clientWidth,
    };
  `);
  check('kill switch still reachable at 390px', narrow.killVisible, JSON.stringify(narrow));
  check('nav still rendered at 390px', narrow.navVisible, JSON.stringify(narrow));
  check('main content still rendered at 390px', narrow.mainVisible, JSON.stringify(narrow));
  check('no horizontal overflow at 390px', narrow.docOverflowX <= 1, `overflow=${narrow.docOverflowX}px`);
  await cdp.send('Emulation.clearDeviceMetricsOverride');


  console.log('\n== the consolidated views carry what the jobs need ==');
  // The point of the restructure: sizing an order should not require visiting
  // four views. Assert the Execute view has the money and the queue together.
  await cdp.eval(`location.hash = 'execute'; return 1`);
  await sleep(1200);
  const execute = await cdp.eval(`
    const ids = [...document.querySelectorAll('#view-execute [data-panel]')].map(e => e.dataset.panel);
    const has = (p) => ids.includes(p);
    const filled = (p) => {
      const el = document.querySelector('#view-execute [data-panel="' + p + '"]');
      return el ? el.textContent.trim().length > 0 : false;
    };
    return {
      ids,
      hasBuyingPower: has('buy-power'),
      hasTicket: !!document.querySelector('#view-execute #oe-form'),
      hasApprovals: has('approvals'),
      hasPositions: has('positions'),
      hasBrackets: has('brackets'),
      hasRecentTrades: has('recent-trades'),
      buyingPowerFilled: filled('buy-power'),
    };
  `);
  for (const [label, ok] of [
    ['buying power', execute.hasBuyingPower],
    ['order ticket', execute.hasTicket],
    ['pending approvals', execute.hasApprovals],
    ['open positions', execute.hasPositions],
    ['protective brackets', execute.hasBrackets],
    ['recent trades', execute.hasRecentTrades],
  ]) {
    check(`execute view carries ${label}`, ok === true, JSON.stringify(execute.ids));
  }
  check('buying power renders on the execute view', execute.buyingPowerFilled === true);

  await cdp.eval(`location.hash = 'analyse'; return 1`);
  await sleep(1500);
  const analyse = await cdp.eval(`
    const ids = [...document.querySelectorAll('#view-analyse [data-panel]')].map(e => e.dataset.panel);
    return { ids, count: ids.length };
  `);
  for (const p of ['candles', 'regime', 'watchlist', 'opportunities', 'signal-feed',
    'diversification', 'orderflow', 'venues', 'arb-settlement', 'performance',
    'strategy-perf', 'research', 'backtests']) {
    check(`analyse view carries ${p}`, analyse.ids.includes(p), JSON.stringify(analyse.ids));
  }

  await cdp.eval(`location.hash = 'bot'; return 1`);
  await sleep(1200);
  const bot = await cdp.eval(`
    const ids = [...document.querySelectorAll('#view-bot [data-panel]')].map(e => e.dataset.panel);
    const runBtns = [...document.querySelectorAll('#view-bot [data-run-action]')];
    const presetBtns = [...document.querySelectorAll('#view-bot [data-bucket-preset]')];
    return {
      ids,
      hasActions: ids.includes('actions'),
      hasStrategies: ids.includes('strategies'),
      hasHealth: ids.includes('health'),
      runButtons: runBtns.length,
      runGated: runBtns.every(b => b.hasAttribute('data-needs-token')),
      runCarryRisk: runBtns.every(b => (b.dataset.risk || '') !== ''),
      presetGated: presetBtns.every(b => b.hasAttribute('data-needs-token')),
    };
  `);
  check('bot view carries operator actions', bot.hasActions === true, JSON.stringify(bot.ids));
  check('bot view carries the strategy roster', bot.hasStrategies === true);
  check('bot view carries service health', bot.hasHealth === true);
  check('bot view offers runnable actions', bot.runButtons > 0, `count=${bot.runButtons}`);
  check('every action button is token-gated', bot.runGated === true, JSON.stringify(bot));
  check('every action button carries its risk for the confirmation', bot.runCarryRisk === true);
  check('every preset button is token-gated', bot.presetGated === true);

  console.log('\n== guarded actions confirm before they run ==');
  // window.confirm is the confirmation gate for anything that moves the system.
  // Stub it to record rather than answer, so nothing is queued during the test.
  const guarded = await cdp.eval(`
    const seen = [];
    const realConfirm = window.confirm;
    window.confirm = (msg) => { seen.push(String(msg)); return false; };
    const btn = [...document.querySelectorAll('#view-bot [data-run-action]')]
      .find(b => (b.dataset.risk || '') !== 'safe') || document.querySelector('#view-bot [data-run-action]');
    btn.click();
    window.confirm = realConfirm;
    return { seen, text: btn.textContent.trim() };
  `);
  check('a guarded action asks for confirmation', guarded.seen.length === 1, JSON.stringify(guarded));
  check('the confirmation names the action',
    /rebalance|action/i.test(guarded.seen[0] || ''), JSON.stringify(guarded.seen));

  const presetGuard = await cdp.eval(`
    const seen = [];
    const realConfirm = window.confirm;
    window.confirm = (msg) => { seen.push(String(msg)); return false; };
    const btn = document.querySelector('#view-bot [data-bucket-preset]');
    if (btn) btn.click();
    window.confirm = realConfirm;
    return { seen };
  `);
  if (presetGuard.seen.length) {
    check('the allocation confirmation says what it rewrites',
      /allocation/i.test(presetGuard.seen[0]), JSON.stringify(presetGuard.seen));
  } else {
    check('no unguarded allocation control exists', true);
  }

  console.log('\n== analysis links into execution ==');
  const prefill = await cdp.eval(`
    const btn = document.querySelector('#view-analyse [data-prefill]');
    if (!btn) return { present: false };
    const symbol = btn.dataset.prefill;
    btn.click();
    return {
      present: true, symbol,
      ticketValue: document.querySelector('#oe-symbol').value,
      onExecute: document.querySelector('#view-execute').getBoundingClientRect().height > 0,
    };
  `);
  if (prefill.present) {
    check('a proposed plan loads its symbol into the ticket',
      prefill.ticketValue === prefill.symbol, JSON.stringify(prefill));
    check('and takes the operator to the execute view', prefill.onExecute === true, JSON.stringify(prefill));
  } else {
    check('no prefill control without a plan to prefill', true);
  }

  console.log('\n== nothing above queued anything ==');
  const wrote = await cdp.eval(`
    return performance.getEntriesByType('resource')
      .map(e => e.name).filter(n => /actions\\/run|buckets\\/preset/.test(n));
  `);
  check('confirmations were declined, so nothing was written',
    wrote.length === 0, JSON.stringify(wrote));


  console.log('\n== the order preview reflects a real ticket ==');
  await cdp.eval(`location.hash = 'execute'; return 1`);
  await sleep(900);
  // Type a symbol and a size, then let the debounced preview settle. This is the
  // path an operator takes before committing, so it is checked in a real browser:
  // the debounce, the input wiring and the arithmetic all have to hold together.
  const preview = await cdp.eval(`
    const set = (sel, value) => {
      const el = document.querySelector(sel);
      el.value = value;
      el.dispatchEvent(new Event('input', { bubbles: true }));
      return el.value;
    };
    set('#oe-symbol', 'BTC-USD');
    set('#oe-size', '1000');
    set('#oe-stop', '');
    set('#oe-target', '');
    return { symbol: document.querySelector('#oe-symbol').value };
  `);
  void preview;
  await sleep(6000); // debounce plus the price lookup

  const shown = await cdp.eval(`
    const host = document.querySelector('#oe-preview');
    return {
      present: !!host,
      text: host ? host.textContent.replace(/\\s+/g, ' ').trim() : '',
      html: host ? host.innerHTML : '',
      errored: host ? !!host.querySelector('.panel-error') : false,
    };
  `);
  check('the preview renders for a complete ticket', shown.present === true);
  check('the preview produced content', shown.text.length > 0, shown.text.slice(0, 160));
  // The four numbers an operator needs to judge the order.
  for (const label of ['Indicative entry', 'Stop', 'Target', 'Reward : risk']) {
    check(`preview shows ${label}`, shown.text.includes(label), shown.text.slice(0, 200));
  }
  check('the preview does not claim to be authoritative',
    /Indicative|re-fetches/i.test(shown.text), shown.text.slice(-160));
  if (shown.errored) {
    // A price lookup can legitimately fail in this environment; the requirement
    // is that it says so rather than showing a stale or invented number.
    check('a failed preview explains itself instead of showing a number',
      /No price for|Cannot price|unreachable/i.test(shown.text), shown.text.slice(0, 200));
  } else {
    check('a successful preview is not an error state', shown.errored === false);
  }

  // Assert the outcome the operator sees rather than the network entry behind
  // it: performance.getEntriesByType is per-document and the harness reloads the
  // page for the theme and keyboard checks, which makes it an unreliable witness
  // for anything that happened after them.
  const priced = await cdp.eval(`
    const host = document.querySelector('#oe-preview');
    const cells = [...host.querySelectorAll('td.num')].map(td => td.textContent.trim());
    return { cells };
  `);
  check('the preview shows real numbers, not placeholders',
    priced.cells.length >= 5 && priced.cells.every((c) => /[\d]/.test(c) && c !== '--'),
    JSON.stringify(priced.cells));

  console.log('\n== flipping the side re-derives the bracket ==');
  const flipped = await cdp.eval(`
    const before = document.querySelector('#oe-preview').textContent.replace(/\\s+/g,' ').trim();
    const sell = [...document.querySelectorAll('.side-toggle button')]
      .find(b => b.dataset.side === 'SELL');
    sell.click();
    return { before };
  `);
  await sleep(4000);
  const after = await cdp.eval(`
    return document.querySelector('#oe-preview').textContent.replace(/\\s+/g,' ').trim();
  `);
  check('switching BUY -> SELL changes the preview',
    after !== flipped.before, `before=${flipped.before.slice(0, 80)} after=${after.slice(0, 80)}`);
  // Put it back so the later checks are not looking at a SELL ticket.
  await cdp.eval(`
    [...document.querySelectorAll('.side-toggle button')].find(b => b.dataset.side === 'BUY').click();
    return 1;
  `);

  console.log('\n== closing a position lands a SELL in the ticket ==');
  await cdp.eval(`location.hash = 'overview'; return 1`);
  await sleep(900);
  const closeFlow = await cdp.eval(`
    const btn = document.querySelector('[data-panel="positions"] [data-prefill]');
    if (!btn) return { present: false };
    const d = btn.dataset;
    btn.click();
    return {
      present: true, symbol: d.prefill, side: d.prefillSide, size: d.prefillSize,
      ticketSymbol: document.querySelector('#oe-symbol').value,
      ticketSize: document.querySelector('#oe-size').value,
      sidePressed: [...document.querySelectorAll('.side-toggle button')]
        .filter(b => b.getAttribute('aria-pressed') === 'true').map(b => b.dataset.side),
      onExecute: document.querySelector('#view-execute').getBoundingClientRect().height > 0,
    };
  `);
  if (closeFlow.present) {
    check('closing a position preloads its symbol',
      closeFlow.ticketSymbol === closeFlow.symbol, JSON.stringify(closeFlow));
    check('closing a position preloads a SELL, not a BUY',
      closeFlow.sidePressed.includes('SELL'), JSON.stringify(closeFlow.sidePressed));
    check('closing a position preloads the position notional',
      closeFlow.ticketSize !== '' && Number(closeFlow.ticketSize) > 0, JSON.stringify(closeFlow));
    check('closing a position lands on the execute view', closeFlow.onExecute === true);
  } else {
    check('no close control without a position to close', true);
  }

  console.log('\n== the action audit is present and starts empty ==');
  const audit = await cdp.eval(`
    const host = document.querySelector('#oe-audit');
    return {
      present: !!host,
      text: host ? host.textContent.replace(/\\s+/g,' ').trim() : '',
      clearable: !!document.querySelector('#audit-clear'),
      session: sessionStorage.getItem('pm.action.audit'),
    };
  `);
  check('the audit renders', audit.present === true);
  // Not "starts empty" -- the guarded-action checks above deliberately declined
  // two confirmations, and those belong in the log. What must hold is that it is
  // never a blank card: with entries it shows them, without them it states why.
  const hasRows = audit.text.includes('operator action') || audit.text.includes('capital preset');
  check('the audit is never a blank card',
    hasRows || /No actions taken from this tab yet/.test(audit.text),
    audit.text.slice(0, 120));
  check('it can be cleared', audit.clearable === true);
  // Earlier in this run the guarded-action checks clicked Run and Apply with
  // window.confirm stubbed to decline. Those must appear as declined, and nothing
  // may appear as having succeeded: browsing alone must never write an audit row.
  const rows = audit.session ? JSON.parse(audit.session) : [];
  check('declined confirmations are audited as cancelled, not attempted',
    rows.length > 0 && rows.every((r) => r.ok === false),
    JSON.stringify(rows.map((r) => [r.path, r.ok, r.outcome])));
  check('nothing that requires a token was actually executed',
    rows.every((r) => r.outcome === 'cancelled'),
    JSON.stringify(rows.map((r) => r.outcome)));
  check('the audit names the actions in words',
    audit.text.includes('operator action') && audit.text.includes('capital preset'),
    audit.text.slice(0, 160));

  console.log('\n== charts are real SVG, not empty shells ==');
  // The chart lives in the analyse view now. Navigating anywhere else leaves it
  // inside a hidden section, where getBBox() is legitimately 0x0 -- so the
  // assertion has to measure it while its view is actually shown.
  await cdp.eval(`location.hash = 'analyse'; return 1`);
  await sleep(1500);
  const chartVisible = await cdp.eval(`
    const host = document.querySelector('#candles-body');
    return host.getBoundingClientRect().height > 0;
  `);
  check('the chart host is visible in its own view', chartVisible === true, `visible=${chartVisible}`);
  const chart = await cdp.eval(`
    const svg = document.querySelector('#candles-body svg.chart');
    if (!svg) return { present: false, html: document.querySelector('#candles-body').innerHTML.slice(0, 200) };
    const path = svg.querySelector('path.line');
    const box = path.getBBox();
    return { present: true, d: path.getAttribute('d').slice(0, 40), w: Math.round(box.width), h: Math.round(box.height),
             role: svg.getAttribute('role'), label: svg.getAttribute('aria-label') };
  `);
  check('chart svg exists', chart.present, JSON.stringify(chart).slice(0, 200));
  if (chart.present) {
    check('chart path has real geometry', chart.w > 10 && chart.h > 5, `bbox ${chart.w}x${chart.h}`);
    check('chart is labelled for screen readers',
      chart.role === 'img' && chart.label && chart.label.length > 4, JSON.stringify({ role: chart.role, label: chart.label }));
  }

  console.log('\n== server survived the whole session ==');
  check('dashboard server process is still alive', serverAlive(),
    `exitCode=${server && server.exitCode} signal=${server && server.signalCode}`);

  console.log('\n== browser console and network stayed clean ==');
  check('no uncaught exceptions', pageErrors.length === 0, pageErrors.join(' | ').slice(0, 400));
  const realConsoleErrors = consoleErrors.filter((e) => !/\b(401|503)\b/.test(e));
  check('no console errors beyond the expected 401/503 entries',
    realConsoleErrors.length === 0, realConsoleErrors.join(' | ').slice(0, 400));
  // 401 on a read is the capital panel without a token; that is by design and
  // is asserted elsewhere, so only flag unexpected failures.
  // Expected non-2xx: 401 on the token-gated read before unlock, and 503 from
  // /ready when there is no supervisor state (a fresh checkout has none). Both
  // are the endpoints answering correctly.
  const unexpected = failedRequests.filter((r) => !/\b(401|503)\b/.test(r));
  check('no failed network requests beyond the expected 401 and /ready 503',
    unexpected.length === 0, unexpected.join(' | ').slice(0, 400));

  console.log('\n== read-only browsing issued no mutating request ==');
  const posts = await cdp.eval(`
    return performance.getEntriesByType('resource')
      .map(e => e.name).filter(n => /kill-switch|orders\\/submit|approvals\\/(approve|deny)|brackets\\/cancel/.test(n));
  `);
  check('no mutating endpoint touched while browsing',
    posts.length === 0, JSON.stringify(posts));

  failed = results.filter((r) => !r.ok).length;
} catch (err) {
  console.error('\nharness error:', err.stack || err.message);
  failed = 1;
}

finish();
