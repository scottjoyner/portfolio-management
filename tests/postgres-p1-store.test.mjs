import test from 'node:test';
import assert from 'node:assert/strict';
import { PostgresOperatorStoreP1 } from '../packages/storage/src/postgresOperatorStoreP1.mjs';
import { createInitialOperatorState } from '../packages/storage/src/operatorStore.mjs';

class FakePgClientP1 {
  constructor(options = {}) {
    this.tables = createInitialOperatorState();
    this.flags = new Map();
    this.queries = [];
    this.includeP1Migration = options.includeP1Migration !== false;
  }

  async query(sql, params = []) {
    this.queries.push({ sql, params });
    if (sql.includes("to_regclass('public.schema_migrations')")) return { rows: [{ schema_migrations: 'schema_migrations', strategies: 'strategies' }] };
    if (sql.startsWith('SELECT version FROM schema_migrations')) return { rows: this.includeP1Migration ? [{ version: '001_operator_state' }, { version: '002_operator_product_layer' }] : [{ version: '001_operator_state' }] };
    if (sql === 'BEGIN' || sql === 'COMMIT' || sql === 'ROLLBACK') return { rows: [] };
    if (sql.startsWith('SELECT * FROM strategies')) return { rows: this.tables.strategies.map(s => ({ id: s.id, name: s.name, version: s.version, status: s.status, risk_level: s.riskLevel, parameters_json: s.parameters, created_at: s.createdAt, updated_at: s.updatedAt })) };
    if (sql.startsWith('SELECT * FROM backtest_runs')) return { rows: this.tables.backtests.map(b => ({ id: b.id, strategy_id: b.strategyId, status: b.status, assumptions_json: b.assumptions, metrics_json: b.metrics, equity_curve_json: b.equityCurve, trades_json: b.trades, started_at: b.startedAt, completed_at: b.completedAt })) };
    if (sql.startsWith('SELECT * FROM approvals')) return { rows: this.tables.approvals.map(a => ({ id: a.id, strategy_id: a.strategyId, status: a.status, tier: a.tier, reason: a.reason, created_at: a.createdAt, reviewed_at: a.reviewedAt, reviewer: a.reviewer })) };
    if (sql.startsWith('SELECT * FROM positions')) return { rows: [] };
    if (sql.startsWith('SELECT * FROM audit_events')) return { rows: this.tables.audit.map(a => ({ id: a.id, action: a.action, actor: a.actor, at: a.at, details: a.details, payload_json: a.payload || {} })) };
    if (sql.includes("WHERE key = 'kill_switch'")) return { rows: [{ key: 'kill_switch', value_json: this.tables.killSwitch, updated_at: this.tables.killSwitch.updatedAt }] };
    if (sql.startsWith('SELECT * FROM accounts')) return { rows: this.tables.accounts.map(a => ({ id: a.id, name: a.name, provider: a.provider, status: a.status, currency: a.currency, cash: a.cash, nav: a.nav, updated_at: a.updatedAt })) };
    if (sql.startsWith('SELECT * FROM instruments')) return { rows: this.tables.instruments.map(i => ({ symbol: i.symbol, name: i.name, asset_class: i.assetClass, venue: i.venue, status: i.status, min_order_size: i.minOrderSize, price_precision: i.pricePrecision })) };
    if (sql.startsWith('SELECT * FROM strategy_templates')) return { rows: this.tables.strategyTemplates.map(t => ({ id: t.id, name: t.name, description: t.description, risk_level: t.riskLevel, parameter_schema_json: t.parameterSchema, created_at: t.createdAt, updated_at: t.updatedAt })) };
    if (sql.startsWith('SELECT * FROM paper_executions')) return { rows: this.tables.paperExecutions.map(e => ({ id: e.id, strategy_id: e.strategyId, account_id: e.accountId, status: e.status, mode: e.mode || 'paper', started_at: e.startedAt, stopped_at: e.stoppedAt, stop_reason: e.stopReason, last_heartbeat_at: e.lastHeartbeatAt, fills_json: e.fills || [] })) };
    if (sql.startsWith('DELETE FROM')) {
      const table = sql.replace('DELETE FROM ', '').trim();
      if (table === 'operator_flags') { this.flags.clear(); this.tables.killSwitch = { enabled: false, reason: null, updatedAt: null }; }
      if (table === 'strategies') this.tables.strategies = [];
      if (table === 'backtest_runs') this.tables.backtests = [];
      if (table === 'approvals') this.tables.approvals = [];
      if (table === 'positions') this.tables.positions = [];
      if (table === 'audit_events') this.tables.audit = [];
      if (table === 'accounts') this.tables.accounts = [];
      if (table === 'instruments') this.tables.instruments = [];
      if (table === 'strategy_templates') this.tables.strategyTemplates = [];
      if (table === 'paper_executions') this.tables.paperExecutions = [];
      return { rows: [] };
    }
    if (sql.startsWith('INSERT INTO strategies')) { const [id, name, version, status, riskLevel, parameters, createdAt, updatedAt] = params; this.tables.strategies.push({ id, name, version, status, riskLevel, parameters: JSON.parse(parameters), createdAt, updatedAt }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO backtest_runs')) { const [id, strategyId, status, assumptions, metrics, equityCurve, trades, startedAt, completedAt] = params; this.tables.backtests.push({ id, strategyId, status, assumptions: JSON.parse(assumptions), metrics: JSON.parse(metrics), equityCurve: JSON.parse(equityCurve), trades: JSON.parse(trades), startedAt, completedAt }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO approvals')) { const [id, strategyId, status, tier, reason, createdAt, reviewedAt, reviewer] = params; this.tables.approvals.push({ id, strategyId, status, tier, reason, createdAt, reviewedAt, reviewer }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO audit_events')) { const [id, action, actor, at, details, payload] = params; this.tables.audit.push({ id, action, actor, at, details, payload: JSON.parse(payload) }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO operator_flags')) { const [key, value] = params; if (key === 'kill_switch') this.tables.killSwitch = JSON.parse(value); else this.flags.set(key, JSON.parse(value)); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO accounts')) { const [id, name, provider, status, currency, cash, nav, updatedAt] = params; this.tables.accounts.push({ id, name, provider, status, currency, cash, nav, updatedAt }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO instruments')) { const [symbol, name, assetClass, venue, status, minOrderSize, pricePrecision] = params; this.tables.instruments.push({ symbol, name, assetClass, venue, status, minOrderSize, pricePrecision }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO strategy_templates')) { const [id, name, description, riskLevel, parameterSchema, createdAt, updatedAt] = params; this.tables.strategyTemplates.push({ id, name, description, riskLevel, parameterSchema: JSON.parse(parameterSchema), createdAt, updatedAt }); return { rows: [] }; }
    if (sql.startsWith('INSERT INTO paper_executions')) { const [id, strategyId, accountId, status, mode, startedAt, stoppedAt, stopReason, lastHeartbeatAt, fills] = params; this.tables.paperExecutions.push({ id, strategyId, accountId, status, mode, startedAt, stoppedAt, stopReason, lastHeartbeatAt, fills: JSON.parse(fills) }); return { rows: [] }; }
    return { rows: [] };
  }
}

test('postgres P1 store requires product-layer migration', async () => {
  const store = new PostgresOperatorStoreP1({ client: new FakePgClientP1({ includeP1Migration: false }) });
  const migrations = await store.checkMigrations();
  assert.equal(migrations.ok, false);
  assert.equal(migrations.reason, 'p1_product_layer_migration_missing');
});

test('postgres P1 store persists product-layer arrays through row tables', async () => {
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
  assert.equal(store.getStatus().productLayer, 'p1-row-tables');
  assert.ok(client.queries.some(call => call.sql.startsWith('INSERT INTO accounts')));
  assert.ok(client.queries.some(call => call.sql.startsWith('INSERT INTO paper_executions')));
  assert.equal(client.queries.some(call => call.sql.includes("WHERE key IN ('accounts','instruments','strategy_templates','paper_executions')")), false);
});
