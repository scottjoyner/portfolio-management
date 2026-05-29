import test from 'node:test';
import assert from 'node:assert/strict';
import { PostgresOperatorStore } from '../packages/storage/src/postgresOperatorStore.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

class FakePgClient {
  constructor() {
    this.tables = createInitialOperatorState();
    this.inTransaction = false;
    this.queries = [];
  }

  async query(sql, params = []) {
    this.queries.push({ sql, params });
    if (sql.includes("to_regclass('public.schema_migrations')")) {
      return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies' }] };
    }
    if (sql.startsWith('SELECT version FROM schema_migrations')) return { rows: [{ version: '001_operator_state' }] };
    if (sql === 'BEGIN') { this.inTransaction = true; return { rows: [] }; }
    if (sql === 'COMMIT') { this.inTransaction = false; return { rows: [] }; }
    if (sql === 'ROLLBACK') { this.inTransaction = false; return { rows: [] }; }

    if (sql.startsWith('SELECT * FROM strategies')) return { rows: this.tables.strategies.map(s => ({ id: s.id, name: s.name, version: s.version, status: s.status, risk_level: s.riskLevel, parameters_json: s.parameters, created_at: s.createdAt, updated_at: s.updatedAt })) };
    if (sql.startsWith('SELECT * FROM backtest_runs')) return { rows: this.tables.backtests.map(b => ({ id: b.id, strategy_id: b.strategyId, status: b.status, assumptions_json: b.assumptions, metrics_json: b.metrics, equity_curve_json: b.equityCurve, trades_json: b.trades, started_at: b.startedAt, completed_at: b.completedAt })) };
    if (sql.startsWith('SELECT * FROM approvals')) return { rows: this.tables.approvals.map(a => ({ id: a.id, strategy_id: a.strategyId, status: a.status, tier: a.tier, reason: a.reason, created_at: a.createdAt, reviewed_at: a.reviewedAt, reviewer: a.reviewer })) };
    if (sql.startsWith('SELECT * FROM positions')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM audit_events')) return { rows: this.tables.audit.map(a => ({ id: a.id, action: a.action, actor: a.actor, at: a.at, details: a.details, payload_json: a.payload || {} })) };
    if (sql.includes("FROM operator_flags WHERE key = 'kill_switch'")) return { rows: [{ key: 'kill_switch', value_json: this.tables.killSwitch, updated_at: this.tables.killSwitch.updatedAt }] };

    if (sql.startsWith('DELETE FROM')) {
      const table = sql.replace('DELETE FROM ', '').trim();
      if (table === 'strategies') this.tables.strategies = [];
      if (table === 'backtest_runs') this.tables.backtests = [];
      if (table === 'approvals') this.tables.approvals = [];
      if (table === 'positions') this.tables.positions = [];
      if (table === 'audit_events') this.tables.audit = [];
      if (table === 'operator_flags') this.tables.killSwitch = { enabled: false, reason: null, updatedAt: null };
      return { rows: [] };
    }

    if (sql.startsWith('INSERT INTO strategies')) {
      const [id, name, version, status, riskLevel, parameters, createdAt, updatedAt] = params;
      this.tables.strategies.push({ id, name, version, status, riskLevel, parameters: JSON.parse(parameters), createdAt, updatedAt });
      return { rows: [] };
    }
    if (sql.startsWith('INSERT INTO backtest_runs')) {
      const [id, strategyId, status, assumptions, metrics, equityCurve, trades, startedAt, completedAt] = params;
      this.tables.backtests.push({ id, strategyId, status, assumptions: JSON.parse(assumptions), metrics: JSON.parse(metrics), equityCurve: JSON.parse(equityCurve), trades: JSON.parse(trades), startedAt, completedAt });
      return { rows: [] };
    }
    if (sql.startsWith('INSERT INTO approvals')) {
      const [id, strategyId, status, tier, reason, createdAt, reviewedAt, reviewer] = params;
      this.tables.approvals.push({ id, strategyId, status, tier, reason, createdAt, reviewedAt, reviewer });
      return { rows: [] };
    }
    if (sql.startsWith('INSERT INTO audit_events')) {
      const [id, action, actor, at, details, payload] = params;
      this.tables.audit.push({ id, action, actor, at, details, payload: JSON.parse(payload) });
      return { rows: [] };
    }
    if (sql.startsWith('INSERT INTO operator_flags')) {
      this.tables.killSwitch = JSON.parse(params[1]);
      return { rows: [] };
    }

    return { rows: [] };
  }
}

test('postgres store reports migration status with injected client', async () => {
  const store = new PostgresOperatorStore({ client: new FakePgClient() });
  const migrations = await store.checkMigrations();
  assert.equal(migrations.ok, true);
  assert.deepEqual(migrations.applied, ['001_operator_state']);
  assert.equal(store.getStatus().kind, 'postgres');
  assert.equal(store.getStatus().sql, true);
});

test('postgres store loads and saves operator state through client mapping', async () => {
  const client = new FakePgClient();
  const store = new PostgresOperatorStore({ client });
  const loaded = await store.load();
  assert.equal(loaded.strategies.length, 2);
  loaded.killSwitch = { enabled: true, reason: 'pg_test', updatedAt: '2026-05-29T00:00:00.000Z' };
  loaded.strategies.push({ id: 'strategy-pg-test', name: 'PG Test', version: 1, status: 'draft', riskLevel: 'low', parameters: { symbol: 'AAPL' }, createdAt: '2026-05-29T00:00:00.000Z', updatedAt: '2026-05-29T00:00:00.000Z' });
  await store.save(loaded);
  const reloaded = await store.load();
  assert.equal(reloaded.killSwitch.enabled, true);
  assert.ok(reloaded.strategies.some(s => s.id === 'strategy-pg-test'));
  assert.ok(client.queries.some(q => q.sql === 'BEGIN'));
  assert.ok(client.queries.some(q => q.sql === 'COMMIT'));
});
