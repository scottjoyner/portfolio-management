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
      return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies', opportunities: 'opportunities' }] };
    }
    if (sql.startsWith('SELECT version FROM schema_migrations')) {
      return { rows: [
        { version: '001_operator_state' },
        { version: '002_operator_product_layer' },
        { version: '003_audit_and_certification' },
        { version: '004_opportunity_agent_workflow' },
      ] };
    }
    if (sql === 'BEGIN') { this.inTransaction = true; return { rows: [] }; }
    if (sql === 'COMMIT') { this.inTransaction = false; return { rows: [] }; }
    if (sql === 'ROLLBACK') { this.inTransaction = false; return { rows: [] }; }

    if (sql.startsWith('SELECT * FROM strategies')) return { rows: this.tables.strategies.map(strategy => ({ id: strategy.id, name: strategy.name, version: strategy.version, status: strategy.status, risk_level: strategy.riskLevel, parameters_json: strategy.parameters, created_at: strategy.createdAt, updated_at: strategy.updatedAt })) };
    if (sql.startsWith('SELECT * FROM backtest_runs')) return { rows: this.tables.backtests.map(backtest => ({ id: backtest.id, strategy_id: backtest.strategyId, status: backtest.status, assumptions_json: backtest.assumptions, metrics_json: backtest.metrics, equity_curve_json: backtest.equityCurve, trades_json: backtest.trades, started_at: backtest.startedAt, completed_at: backtest.completedAt })) };
    if (sql.startsWith('SELECT * FROM approvals')) return { rows: this.tables.approvals.map(approval => ({ id: approval.id, strategy_id: approval.strategyId, status: approval.status, tier: approval.tier, reason: approval.reason, created_at: approval.createdAt, reviewed_at: approval.reviewedAt, reviewer: approval.reviewer })) };
    if (sql.startsWith('SELECT * FROM positions')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM audit_events')) return { rows: this.tables.audit.map(event => ({ id: event.id, action: event.action, actor: event.actor, at: event.at, details: event.details, payload_json: event.payload || {} })) };
    if (sql.includes("FROM operator_flags WHERE key = 'kill_switch'")) return { rows: [{ key: 'kill_switch', value_json: this.tables.killSwitch, updated_at: this.tables.killSwitch.updatedAt }] };
    if (sql.startsWith('SELECT * FROM market_data_snapshots')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM agent_budgets')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM research_jobs')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM opportunities')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM risk_breakdowns')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM agent_cost_ledger')) return { rows: [] };

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
  assert.deepEqual(migrations.applied, [
    '001_operator_state',
    '002_operator_product_layer',
    '003_audit_and_certification',
    '004_opportunity_agent_workflow',
  ]);
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
  assert.ok(reloaded.strategies.some(strategy => strategy.id === 'strategy-pg-test'));
  assert.ok(client.queries.some(query => query.sql === 'BEGIN'));
  assert.ok(client.queries.some(query => query.sql === 'COMMIT'));
});
