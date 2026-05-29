import { createInitialOperatorState, normalizeOperatorState, MemoryOperatorStore } from './operatorStore.mjs';

function rowJson(value, fallback) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

function iso(value) {
  if (!value) return null;
  if (value instanceof Date) return value.toISOString();
  return String(value);
}

function statusFromError(error) {
  return error ? { ok: false, error: error.message || String(error) } : { ok: true };
}

export class PostgresOperatorStore extends MemoryOperatorStore {
  constructor(options = {}) {
    super(options.seedState || createInitialOperatorState());
    this.kind = 'postgres';
    this.durable = true;
    this.sql = true;
    this.databaseUrl = options.databaseUrl || process.env.DATABASE_URL || 'postgresql://postgres:postgres@localhost:5432/arb';
    this.client = options.client || null;
    this.bootstrap = options.bootstrap !== false;
    this.lastError = null;
    this.migrations = { ok: false, checked: false, applied: [] };
  }

  async getClient() {
    if (this.client) return this.client;
    let pg;
    try {
      pg = await import('pg');
    } catch (error) {
      throw new Error('pg_dependency_missing: install the pg package or inject a compatible test client');
    }
    const pool = new pg.Pool({ connectionString: this.databaseUrl });
    this.client = pool;
    return this.client;
  }

  async query(sql, params = []) {
    const client = await this.getClient();
    try {
      return await client.query(sql, params);
    } catch (error) {
      this.lastError = error;
      throw error;
    }
  }

  async checkMigrations() {
    try {
      const result = await this.query(
        "SELECT to_regclass('public.schema_migrations') AS schema_migrations, to_regclass('public.strategies') AS strategies"
      );
      const row = result.rows?.[0] || {};
      const baseTablesExist = Boolean(row.strategies);
      if (!baseTablesExist) {
        this.migrations = { ok: false, checked: true, applied: [], reason: 'operator_tables_missing' };
        return this.migrations;
      }
      if (!row.schema_migrations) {
        this.migrations = { ok: true, checked: true, applied: [], reason: 'schema_migrations_table_missing_legacy_ok' };
        return this.migrations;
      }
      const versions = await this.query('SELECT version FROM schema_migrations ORDER BY version ASC');
      this.migrations = { ok: true, checked: true, applied: versions.rows.map(r => r.version) };
      return this.migrations;
    } catch (error) {
      this.lastError = error;
      this.migrations = { ok: false, checked: true, applied: [], reason: error.message || String(error) };
      return this.migrations;
    }
  }

  async load() {
    await this.checkMigrations();
    if (!this.migrations.ok) throw new Error(`postgres_migrations_not_ready: ${this.migrations.reason || 'unknown'}`);

    const [strategies, backtests, approvals, positions, audit, flags] = await Promise.all([
      this.query('SELECT * FROM strategies ORDER BY created_at ASC, id ASC'),
      this.query('SELECT * FROM backtest_runs ORDER BY started_at ASC, id ASC'),
      this.query('SELECT * FROM approvals ORDER BY created_at ASC, id ASC'),
      this.query('SELECT * FROM positions ORDER BY opened_at ASC, id ASC'),
      this.query('SELECT * FROM audit_events ORDER BY at ASC, id ASC'),
      this.query("SELECT key, value_json, updated_at FROM operator_flags WHERE key = 'kill_switch'")
    ]);

    const state = normalizeOperatorState({
      schemaVersion: 1,
      strategies: strategies.rows.map(row => ({
        id: row.id,
        name: row.name,
        version: Number(row.version),
        status: row.status,
        riskLevel: row.risk_level,
        parameters: rowJson(row.parameters_json, {}),
        createdAt: iso(row.created_at),
        updatedAt: iso(row.updated_at)
      })),
      backtests: backtests.rows.map(row => ({
        id: row.id,
        strategyId: row.strategy_id,
        status: row.status,
        startedAt: iso(row.started_at),
        completedAt: iso(row.completed_at),
        assumptions: rowJson(row.assumptions_json, {}),
        metrics: rowJson(row.metrics_json, {}),
        equityCurve: rowJson(row.equity_curve_json, []),
        trades: rowJson(row.trades_json, [])
      })),
      approvals: approvals.rows.map(row => ({
        id: row.id,
        strategyId: row.strategy_id,
        status: row.status,
        tier: row.tier,
        reason: row.reason,
        createdAt: iso(row.created_at),
        reviewedAt: iso(row.reviewed_at),
        reviewer: row.reviewer
      })),
      positions: positions.rows.map(row => ({
        id: row.id,
        strategyId: row.strategy_id,
        symbol: row.symbol,
        quantity: Number(row.quantity),
        averagePrice: Number(row.average_price),
        markPrice: row.mark_price === null || row.mark_price === undefined ? null : Number(row.mark_price),
        status: row.status,
        openedAt: iso(row.opened_at),
        closedAt: iso(row.closed_at)
      })),
      audit: audit.rows.map(row => ({
        id: row.id,
        action: row.action,
        actor: row.actor,
        at: iso(row.at),
        details: row.details,
        payload: rowJson(row.payload_json, {})
      })),
      killSwitch: flags.rows[0] ? rowJson(flags.rows[0].value_json, { enabled: false, reason: null, updatedAt: null }) : { enabled: false, reason: null, updatedAt: null }
    });

    if (this.bootstrap && state.strategies.length === 0 && state.audit.length === 0) {
      const seeded = createInitialOperatorState();
      await this.save(seeded);
      this.state = seeded;
      return this.state;
    }

    this.state = state;
    return this.state;
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    await this.checkMigrations();
    if (!this.migrations.ok) throw new Error(`postgres_migrations_not_ready: ${this.migrations.reason || 'unknown'}`);

    await this.query('BEGIN');
    try {
      await this.query('DELETE FROM operator_flags');
      await this.query('DELETE FROM audit_events');
      await this.query('DELETE FROM positions');
      await this.query('DELETE FROM approvals');
      await this.query('DELETE FROM backtest_runs');
      await this.query('DELETE FROM strategies');

      for (const strategy of state.strategies) {
        await this.query(
          'INSERT INTO strategies (id, name, version, status, risk_level, parameters_json, created_at, updated_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
          [strategy.id, strategy.name, strategy.version || 1, strategy.status || 'draft', strategy.riskLevel || strategy.risk_level || 'medium', JSON.stringify(strategy.parameters || {}), strategy.createdAt || new Date().toISOString(), strategy.updatedAt || new Date().toISOString()]
        );
      }

      for (const backtest of state.backtests) {
        await this.query(
          'INSERT INTO backtest_runs (id, strategy_id, status, assumptions_json, metrics_json, equity_curve_json, trades_json, started_at, completed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
          [backtest.id, backtest.strategyId, backtest.status || 'completed', JSON.stringify(backtest.assumptions || {}), JSON.stringify(backtest.metrics || {}), JSON.stringify(backtest.equityCurve || []), JSON.stringify(backtest.trades || []), backtest.startedAt || new Date().toISOString(), backtest.completedAt || null]
        );
      }

      for (const approval of state.approvals) {
        await this.query(
          'INSERT INTO approvals (id, strategy_id, status, tier, reason, created_at, reviewed_at, reviewer) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)',
          [approval.id, approval.strategyId, approval.status || 'pending_review', approval.tier || 'canary', approval.reason || null, approval.createdAt || new Date().toISOString(), approval.reviewedAt || null, approval.reviewer || null]
        );
      }

      for (const position of state.positions) {
        await this.query(
          'INSERT INTO positions (id, strategy_id, symbol, quantity, average_price, mark_price, status, opened_at, closed_at) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)',
          [position.id, position.strategyId || null, position.symbol, position.quantity, position.averagePrice || position.average_price, position.markPrice || position.mark_price || null, position.status || 'open', position.openedAt || new Date().toISOString(), position.closedAt || null]
        );
      }

      for (const event of state.audit) {
        await this.query(
          'INSERT INTO audit_events (id, action, actor, at, details, payload_json) VALUES ($1,$2,$3,$4,$5,$6)',
          [event.id, event.action, event.actor || 'system', event.at || new Date().toISOString(), event.details || null, JSON.stringify(event.payload || {})]
        );
      }

      await this.query(
        'INSERT INTO operator_flags (key, value_json, updated_at) VALUES ($1,$2,$3)',
        ['kill_switch', JSON.stringify(state.killSwitch || { enabled: false, reason: null, updatedAt: null }), state.killSwitch?.updatedAt || new Date().toISOString()]
      );

      await this.query('COMMIT');
      this.state = state;
      return this.state;
    } catch (error) {
      await this.query('ROLLBACK').catch(() => {});
      this.lastError = error;
      throw error;
    }
  }

  async mutate(mutator) {
    const state = await this.load();
    const result = await mutator(state);
    await this.save(state);
    return result;
  }

  getStatus() {
    return {
      kind: this.kind,
      durable: this.durable,
      sql: this.sql,
      schemaVersion: this.state.schemaVersion,
      databaseUrlConfigured: Boolean(this.databaseUrl),
      migrations: this.migrations,
      connection: statusFromError(this.lastError)
    };
  }
}
