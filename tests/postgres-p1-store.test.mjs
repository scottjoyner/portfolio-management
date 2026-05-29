import test from 'node:test';
import assert from 'node:assert/strict';
import { PostgresOperatorStoreP1 } from '../packages/storage/src/postgresOperatorStoreP1.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

class FakePgClientP1 {
  constructor() {
    this.tables = createInitialOperatorState();
    this.flags = new Map();
    this.queries = [];
  }

  async query(sql, params = []) {
    this.queries.push({ sql, params });
    if (sql.includes("to_regclass('public.schema_migrations')")) return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies' }] };
    if (sql.startsWith('SELECT version FROM schema_migrations')) return { rows: [{ version: '001_operator_state' }, { version: '002_operator_product_layer' }] };
    if (sql === 'BEGIN' || sql === 'COMMIT' || sql === 'ROLLBACK') return { rows: [] };
    if (sql.startsWith('SELECT * FROM strategies')) return { rows: this.tables.strategies.map(s => ({ id: s.id, name: s.name, version: s.version, status: s.status, risk_level: s.riskLevel, parameters_json: s.parameters, created_at: s.createdAt, updated_at: s.updatedAt })) };
    if (sql.startsWith('SELECT * FROM backtest_runs')) return { rows: this.tables.backtests.map(b => ({ id: b.id, strategy_id: b.strategyId, status: b.status, assumptions_json: b.assumptions, metrics_json: b.metrics, equity_curve_json: b.equityCurve, trades_json: b.trades, started_at: b.startedAt, completed_at: b.completedAt })) };
    if (sql.startsWith('SELECT * FROM approvals')) return { rows: this.tables.approvals.map(a => ({ id: a.id, strategy_id: a.strategyId, status: a.status, tier: a.tier, reason: a.reason, created_at: a.createdAt, reviewed_at: a.reviewedAt, reviewer: a.reviewer })) };
    if (sql.startsWith('SELECT * FROM positions')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM audit_events')) return { rows: this.tables.audit.map(a => ({ id: a.id, action: a.action, actor: a.actor, at: a.at, details: a.details, payload_json: a.payload || {} })) };
    if (sql.includes("WHERE key = 'kill_switch'")) return { rows: [{ key: 'kill_switch', value_json: this.tables.killSwitch, updated_at: this.tables.killSwitch.updatedAt }] };
    if (sql.includes("WHERE key IN ('accounts','instruments','strategy_templates','paper_executions')")) {
      return { rows: [...this.flags.entries()].map(([key, value]) => ({ key, value_json: value })) };
    }
    if (sql.startsWith('DELETE FROM')) {
      const table = sql.replace('DELETE FROM ', '').trim();
      if (table === 'operator_flags') { this.flags.clear(); this.tables.killSwitch = { enabled: false, reason: null, updatedAt: null }; }
      if (table === 'strategies') this.tables.strategies = [];
      if (table === 'backtest_runs') this.tables.backtests = [];
      if (table === 'approvals') this.tables.approvals = [];
      if (table === 'positions') this.tables.positions = [];
      if (table === 'audit_events') this.tables.audit = [];
      return { rows: [] };
    }
    if (sql.startsWith('INSERT INTO strategies')) { const [id, name, version, status, riskLevel, parameters, createdAt, updatedAt] = params; this.tables.strategies.push({ id, name, version, status, riskLevel, parameters: JSON.parse(parameters), createdAt, updatedAt }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO backtest_runs')) { const [id, strategyId, status, assumptions, metrics, equityCurve, trades, startedAt, completedAt] = params; this.tables.backtests.push({ id, strategyId, status, assumptions: JSON.parse(assumptions), metrics: JSON.parse(metrics), equityCurve: JSON.parse(equityCurve), trades: JSON.parse(trades), startedAt, completedAt }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO approvals')) { const [id, strategyId, status, tier, reason, createdAt, reviewedAt, reviewer] = params; this.tables.approvals.push({ id, strategyId, status, tier, reason, createdAt, reviewedAt, reviewer }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO audit_events')) { const [id, action, actor, at, details, payload] = params; this.tables.audit.push({ id, action, actor, at, details, payload: JSON.parse(payload) }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO operator_flags')) {
      const [key, value] = params;
      if (key === 'kill_switch') this.tables.killSwitch = JSON.parse(value);
      else this.flags.set(key, JSON.parse(value));
      return { rows: [] };
    }
    return { rows: [] };
  }
}

test('postgres P1 store persists product-layer arrays through operator flags', async () => {
  const client = new FakePgClientP1();
  const store = new PostgresOperatorStoreP1({ client });
  const state = await store.load();
  assert.ok(state.accounts.length >= 1);
  state.paperExecutions.push({ id: 'paper-p1', strategyId: state.strategies[0].id, accountId: 'acct-paper-primary', status: 'running', startedAt: '2026-05-29T00:00:00.000Z' });
  state.accounts[0].nav = 101234;
  await store.save(state);
  const reloaded = await store.load();
  assert.equal(reloaded.accounts[0].nav, 101234);
  assert.ok(reloaded.paperExecutions.some(execution => execution.id === 'paper-p1'));
  assert.equal(store.getStatus().productLayer, 'p1-json-flags');
});
