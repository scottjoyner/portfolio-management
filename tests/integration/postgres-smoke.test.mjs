import test from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { PostgresOperatorStoreP1 } from '../../packages/storage/src/postgresOperatorStoreP1.mjs';

const databaseUrl = process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/arb';
const runIntegration = process.env.RUN_POSTGRES_INTEGRATION === 'true';

function requireCommand(command) {
  const result = spawnSync(command, ['--version'], { encoding: 'utf8' });
  return result.status === 0;
}

test('postgres integration smoke test is opt-in', async t => {
  if (!runIntegration) {
    t.skip('Set RUN_POSTGRES_INTEGRATION=true to run against local Postgres.');
    return;
  }
  if (!requireCommand('psql')) {
    t.skip('psql is not installed in this environment.');
    return;
  }

  const migrate = spawnSync('node', ['scripts/migrate-postgres.mjs'], {
    env: { ...process.env, DATABASE_URL: databaseUrl },
    encoding: 'utf8'
  });
  assert.equal(migrate.status, 0, migrate.stderr || migrate.stdout);

  const store = new PostgresOperatorStoreP1({ databaseUrl });
  const state = await store.load();
  assert.ok(state.accounts.length >= 1);
  assert.ok(state.instruments.some(instrument => instrument.symbol === 'BTC-USD'));

  state.audit.push({ id: `audit-it-${Date.now()}`, action: 'postgres_integration_smoke', actor: 'test', at: new Date().toISOString(), details: 'postgres smoke passed' });
  await store.save(state);

  const reloaded = await store.load();
  assert.ok(reloaded.audit.some(event => event.action === 'postgres_integration_smoke'));
});
