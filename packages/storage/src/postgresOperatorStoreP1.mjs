import { PostgresOperatorStore } from './postgresOperatorStore.mjs';
import { normalizeOperatorState } from './operatorStore.mjs';
import { OperatorRowRepository } from './operatorRowRepository.mjs';

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

export class PostgresOperatorStoreP1 extends PostgresOperatorStore {
  constructor(options = {}) {
    super(options);
    this.kind = 'postgres-p1';
    this.rows = new OperatorRowRepository(this);
  }

  async checkMigrations() {
    const migrations = await super.checkMigrations();
    if (!migrations.ok) return migrations;
    const hasP1 = migrations.applied.includes('002_operator_product_layer');
    if (!hasP1) {
      this.migrations = { ...migrations, ok: false, reason: 'p1_product_layer_migration_missing' };
      return this.migrations;
    }
    this.migrations = migrations;
    return this.migrations;
  }

  async load() {
    const base = await super.load();
    const [accounts, instruments, templates, paperExecutions] = await Promise.all([
      this.rows.listAccounts(),
      this.rows.listInstruments(),
      this.rows.listStrategyTemplates(),
      this.rows.listPaperExecutions()
    ]);
    this.state = normalizeOperatorState({
      ...base,
      accounts: accounts.map(row => ({
        id: row.id,
        name: row.name,
        provider: row.provider,
        status: row.status,
        currency: row.currency,
        cash: Number(row.cash || 0),
        nav: Number(row.nav || 0),
        updatedAt: iso(row.updated_at)
      })),
      instruments: instruments.map(row => ({
        symbol: row.symbol,
        name: row.name,
        assetClass: row.asset_class,
        venue: row.venue,
        status: row.status,
        minOrderSize: Number(row.min_order_size || 0),
        pricePrecision: Number(row.price_precision || 2)
      })),
      strategyTemplates: templates.map(row => ({
        id: row.id,
        name: row.name,
        description: row.description,
        riskLevel: row.risk_level,
        parameterSchema: rowJson(row.parameter_schema_json, {}),
        createdAt: iso(row.created_at),
        updatedAt: iso(row.updated_at)
      })),
      paperExecutions: paperExecutions.map(row => ({
        id: row.id,
        strategyId: row.strategy_id,
        accountId: row.account_id,
        status: row.status,
        mode: row.mode,
        startedAt: iso(row.started_at),
        stoppedAt: iso(row.stopped_at),
        stopReason: row.stop_reason,
        lastHeartbeatAt: iso(row.last_heartbeat_at),
        fills: rowJson(row.fills_json, [])
      }))
    });
    return this.state;
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    await super.save(state);
    await this.query('BEGIN');
    try {
      await this.rows.replaceProductLayer(state);
      await this.query('COMMIT');
      this.state = state;
      return this.state;
    } catch (error) {
      await this.query('ROLLBACK').catch(() => {});
      this.lastError = error;
      throw error;
    }
  }

  getStatus() {
    return { ...super.getStatus(), productLayer: 'p1-row-tables' };
  }
}
