import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { Readable } from 'node:stream';
import { handleRequest, createInitialState } from '../apps/api/src/server.p1.mjs';
import { MemoryOperatorStore } from '../packages/storage/src/operatorStore.mjs';

function req(url) {
  const stream = new Readable({ read() {} });
  stream.method = 'GET';
  stream.url = url;
  stream.headers = {};
  stream.push(null);
  return stream;
}

async function systemTruth({ state = createInitialState(), dataDir, env = {} } = {}) {
  const out = await handleRequest(req('/api/system-truth'), {
    store: new MemoryOperatorStore(state),
    dataDir,
    env: { OPERATOR_AUTH_REQUIRED: 'false', MODE: 'mock', ...env },
  });
  return { ...out, data: JSON.parse(out.body) };
}

const UNKNOWN_PAPER_BOOK = {
  gross_exposure_usd: null,
  open_positions: null,
  capital_in_play_usd: null,
  cash_usd: null,
  realized_pnl_usd: null,
  fees_paid_usd: null,
  state_age_sec: null,
  status: 'unknown',
  source: 'unknown',
};

const UNKNOWN_EXECUTION_DECISION = { value: 'unknown', status: 'unknown', source: 'unknown' };

test('system truth defaults paper book and execution decision to source-labelled unknowns', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'system-truth-'));
  const out = await systemTruth({ dataDir });

  assert.equal(out.status, 200);
  assert.match(out.data.generated_at, /Z$/);
  assert.deepEqual(Object.keys(out.data).sort(), ['cache', 'execution_decision', 'feed', 'generated_at', 'paper_book', 'services', 'terminal', 'trading_mode', 'warnings']);
  assert.deepEqual(out.data.paper_book, UNKNOWN_PAPER_BOOK);
  assert.deepEqual(out.data.execution_decision, UNKNOWN_EXECUTION_DECISION);
  assert.deepEqual(out.data.trading_mode, { value: 'unknown', source: 'unknown', status: 'unknown' });
  assert.equal(out.data.feed.heartbeat.freshness, 'unknown');
  assert.equal(out.data.services.snapshot.freshness, 'unknown');
  assert.deepEqual(out.data.terminal, { url: '/dashboard', source: 'dashboard_default', status: 'ok' });
  assert.ok(out.data.warnings.includes('system health snapshot is unknown'));
});

test('system truth renders paper book and execution decision only from a fresh valid system-health snapshot', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'system-truth-'));
  const state = createInitialState();
  state.config.tradingMode = 'live';
  state.positions = [{ symbol: 'BTC-USD', quantity: 99, markPrice: 99999, status: 'open' }];
  state.capitalInPlayUsd = 456789;
  await writeFile(join(dataDir, 'system-health.json'), JSON.stringify({
    trading_mode: 'paper',
    trader: {
      status: 'ok',
      paper_book: {
        gross_exposure_usd: 230,
        open_positions: 2,
        capital_in_play_usd: 123.46,
        cash_usd: 9876.54,
        realized_pnl_usd: -12.34,
        fees_paid_usd: 1.23,
        state_age_sec: 4.5,
        status: 'ok',
        source: 'paper_trader',
      },
      execution_decision: { value: 'allowed', status: 'ok', source: 'paper_trader' },
    },
  }));

  const out = await systemTruth({ state, dataDir });
  assert.deepEqual(out.data.trading_mode, { value: 'paper', source: 'system_health_snapshot', status: 'ok' });
  assert.deepEqual(out.data.paper_book, {
    gross_exposure_usd: 230,
    open_positions: 2,
    capital_in_play_usd: 123.46,
    cash_usd: 9876.54,
    realized_pnl_usd: -12.34,
    fees_paid_usd: 1.23,
    state_age_sec: 4.5,
    status: 'ok',
    source: 'paper_trader',
  });
  assert.deepEqual(out.data.execution_decision, { value: 'allowed', status: 'ok', source: 'paper_trader' });
  assert.equal(out.data.services.snapshot.freshness, 'fresh');
  assert.equal(out.data.services.snapshot.source, 'system_health_snapshot');
  assert.doesNotMatch(JSON.stringify(out.data), /operator_state/);
});

test('system truth fails closed when the snapshot is stale, invalid, or lacks execution decision', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'system-truth-'));
  const path = join(dataDir, 'system-health.json');
  const state = createInitialState();
  state.config.tradingMode = 'live';
  state.positions = [{ symbol: 'BTC-USD', quantity: 2, markPrice: 100, status: 'open' }];
  state.capitalInPlayUsd = 123.456;
  await writeFile(path, JSON.stringify({ trader: { paper_book: { gross_exposure_usd: 200, open_positions: 1, capital_in_play_usd: 200, status: 'ok', source: 'paper_trader' } } }));
  const staleAt = new Date(Date.now() - 181_000);
  await utimes(path, staleAt, staleAt);

  let out = await systemTruth({ state, dataDir });
  assert.deepEqual(out.data.paper_book, UNKNOWN_PAPER_BOOK);
  assert.deepEqual(out.data.execution_decision, UNKNOWN_EXECUTION_DECISION);
  assert.deepEqual(out.data.trading_mode, { value: 'unknown', source: 'unknown', status: 'unknown' });
  assert.ok(out.data.warnings.includes('system health snapshot is stale'));

  await writeFile(path, JSON.stringify({ trader: { paper_book: { gross_exposure_usd: 'not-a-number', status: 'ok', source: 'paper_trader' } } }));
  out = await systemTruth({ state, dataDir });
  assert.deepEqual(out.data.paper_book, UNKNOWN_PAPER_BOOK);
  assert.deepEqual(out.data.execution_decision, UNKNOWN_EXECUTION_DECISION);
  assert.ok(out.data.warnings.includes('paper book is unavailable from system health snapshot'));
});

test('system truth validates terminal links and does not use docker or shell health probes', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'system-truth-'));
  const unsafe = await systemTruth({ dataDir, env: { TRADING_TERMINAL_URL: 'javascript:alert(1)' } });
  assert.deepEqual(unsafe.data.terminal, { url: '/dashboard', source: 'dashboard_default', status: 'warn' });
  assert.ok(unsafe.data.warnings.includes('unsafe terminal URL; using dashboard default'));

  const sameOrigin = await systemTruth({ dataDir, env: { TRADING_TERMINAL_URL: '/terminal?tab=orders' } });
  assert.deepEqual(sameOrigin.data.terminal, { url: '/terminal?tab=orders', source: 'TRADING_TERMINAL_URL', status: 'ok' });

  const source = await readFile(new URL('../apps/api/src/systemTruth.mjs', import.meta.url), 'utf8');
  assert.doesNotMatch(source, /docker|child_process|exec\(|spawn\(|\/var\/run\/docker\.sock/i);
});

test('Portfolio OS renders and polls the canonical paper book System Truth strip every five seconds', async () => {
  const [html, app] = await Promise.all([
    readFile(new URL('../apps/web/src/index.html', import.meta.url), 'utf8'),
    readFile(new URL('../apps/web/src/app.js', import.meta.url), 'utf8'),
  ]);
  assert.match(html, /id="system-truth"/);
  for (const id of ['truth-mode', 'truth-feed', 'truth-cache', 'truth-services', 'truth-paper-book', 'truth-execution-decision', 'truth-terminal']) assert.match(html, new RegExp(`id="${id}"`));
  assert.doesNotMatch(html, /id="truth-exposure"/);
  assert.match(html, /Local operator execution \(non-canonical\)/);
  assert.match(app, /Local operator execution \(non-canonical\)/);
  assert.match(app, /paper_book/);
  assert.match(app, /execution_decision/);
  assert.match(app, /api\('\/api\/system-truth'\)/);
  assert.match(app, /setInterval[\s\S]*5000/);
});
