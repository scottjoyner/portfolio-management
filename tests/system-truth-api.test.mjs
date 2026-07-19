import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, readFile, writeFile } from 'node:fs/promises';
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

test('system truth defaults to source-labelled unknowns rather than inferring paper', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'system-truth-'));
  const out = await systemTruth({ dataDir });

  assert.equal(out.status, 200);
  assert.match(out.data.generated_at, /Z$/);
  assert.deepEqual(Object.keys(out.data).sort(), ['cache', 'exposure', 'feed', 'generated_at', 'services', 'terminal', 'trading_mode', 'warnings']);
  assert.deepEqual(out.data.trading_mode, { value: 'unknown', source: 'local_state', status: 'warn' });
  assert.equal(out.data.feed.heartbeat.freshness, 'unknown');
  assert.equal(out.data.services.snapshot.freshness, 'unknown');
  assert.equal(out.data.exposure.gross_exposure_usd, 0);
  assert.equal(out.data.exposure.status, 'ok');
  assert.deepEqual(out.data.terminal, { url: '/dashboard', source: 'dashboard_default', status: 'ok' });
  assert.ok(out.data.warnings.includes('trading mode is unknown'));
});

test('system truth uses only fresh explicit evidence and fails closed on conflicts', async () => {
  const dataDir = await mkdtemp(join(tmpdir(), 'system-truth-'));
  await writeFile(join(dataDir, 'system-health.json'), JSON.stringify({ trading_mode: 'live', services: { trader: 'ok' } }));
  const state = createInitialState();
  state.config.tradingMode = 'paper';

  const out = await systemTruth({ state, dataDir });
  assert.equal(out.data.trading_mode.value, 'unknown');
  assert.equal(out.data.trading_mode.source, 'mode_conflict');
  assert.equal(out.data.trading_mode.status, 'warn');
  assert.ok(out.data.warnings.includes('trading mode evidence conflicts'));

  state.config.tradingMode = 'live';
  const resolved = await systemTruth({ state, dataDir });
  assert.deepEqual(resolved.data.trading_mode, { value: 'live', source: 'system_health_snapshot+local_state', status: 'warn' });
  assert.equal(resolved.data.services.snapshot.freshness, 'fresh');
  assert.equal(resolved.data.services.snapshot.source, 'system_health_snapshot');
});

test('system truth reports only fully marked local open-position exposure', async () => {
  const state = createInitialState();
  state.positions = [
    { symbol: 'BTC-USD', quantity: 2, markPrice: 100, status: 'open' },
    { symbol: 'ETH-USD', quantity: -3, markPrice: 10, status: 'open' },
    { symbol: 'SOL-USD', quantity: 99, markPrice: 10, status: 'closed' },
  ];
  state.capitalInPlayUsd = 123.456;
  let out = await systemTruth({ state, dataDir: await mkdtemp(join(tmpdir(), 'system-truth-')) });
  assert.deepEqual(out.data.exposure, {
    gross_exposure_usd: 230,
    open_positions: 2,
    status: 'ok',
    source: 'operator_state_marked_positions',
    capital_in_play_usd: 123.46,
    capital_in_play_source: 'operator_state',
  });

  state.positions[1].markPrice = null;
  out = await systemTruth({ state, dataDir: await mkdtemp(join(tmpdir(), 'system-truth-')) });
  assert.equal(out.data.exposure.gross_exposure_usd, null);
  assert.equal(out.data.exposure.status, 'unknown');
  assert.ok(out.data.warnings.includes('gross exposure has unmarked positions'));
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

test('Portfolio OS renders and polls the compact System Truth strip every five seconds', async () => {
  const [html, app] = await Promise.all([
    readFile(new URL('../apps/web/src/index.html', import.meta.url), 'utf8'),
    readFile(new URL('../apps/web/src/app.js', import.meta.url), 'utf8'),
  ]);
  assert.match(html, /id="system-truth"/);
  for (const id of ['truth-mode', 'truth-feed', 'truth-cache', 'truth-services', 'truth-exposure', 'truth-terminal']) assert.match(html, new RegExp(`id="${id}"`));
  assert.match(app, /api\('\/api\/system-truth'\)/);
  assert.match(app, /setInterval[\s\S]*5000/);
});
