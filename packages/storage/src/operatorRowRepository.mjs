export class OperatorRowRepository {
  constructor(store) {
    this.store = store;
  }

  async query(sql, params = []) {
    return this.store.query(sql, params);
  }

  async listStrategies() {
    const result = await this.query('SELECT * FROM strategies ORDER BY created_at ASC, id ASC');
    return result.rows;
  }

  async upsertStrategy(strategy) {
    await this.query(
      `INSERT INTO strategies (id, name, version, status, risk_level, parameters_json, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         version = EXCLUDED.version,
         status = EXCLUDED.status,
         risk_level = EXCLUDED.risk_level,
         parameters_json = EXCLUDED.parameters_json,
         updated_at = EXCLUDED.updated_at`,
      [strategy.id, strategy.name, strategy.version || 1, strategy.status || 'draft', strategy.riskLevel || 'medium', JSON.stringify(strategy.parameters || {}), strategy.createdAt || new Date().toISOString(), strategy.updatedAt || new Date().toISOString()]
    );
    return strategy;
  }

  async listAccounts() {
    const result = await this.query('SELECT * FROM accounts ORDER BY id ASC');
    return result.rows;
  }

  async upsertAccount(account) {
    await this.query(
      `INSERT INTO accounts (id, name, provider, status, currency, cash, nav, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         provider = EXCLUDED.provider,
         status = EXCLUDED.status,
         currency = EXCLUDED.currency,
         cash = EXCLUDED.cash,
         nav = EXCLUDED.nav,
         updated_at = EXCLUDED.updated_at`,
      [account.id, account.name, account.provider || 'paper', account.status || 'mock', account.currency || 'USD', Number(account.cash || 0), Number(account.nav || 0), account.updatedAt || new Date().toISOString()]
    );
    return account;
  }

  async listInstruments() {
    const result = await this.query('SELECT * FROM instruments ORDER BY symbol ASC');
    return result.rows;
  }

  async upsertInstrument(instrument) {
    await this.query(
      `INSERT INTO instruments (symbol, name, asset_class, venue, status, min_order_size, price_precision)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (symbol) DO UPDATE SET
         name = EXCLUDED.name,
         asset_class = EXCLUDED.asset_class,
         venue = EXCLUDED.venue,
         status = EXCLUDED.status,
         min_order_size = EXCLUDED.min_order_size,
         price_precision = EXCLUDED.price_precision`,
      [instrument.symbol, instrument.name, instrument.assetClass || instrument.asset_class || 'unknown', instrument.venue || 'unknown', instrument.status || 'active', Number(instrument.minOrderSize || instrument.min_order_size || 0), Number(instrument.pricePrecision || instrument.price_precision || 2)]
    );
    return instrument;
  }

  async listStrategyTemplates() {
    const result = await this.query('SELECT * FROM strategy_templates ORDER BY id ASC');
    return result.rows;
  }

  async upsertStrategyTemplate(template) {
    await this.query(
      `INSERT INTO strategy_templates (id, name, description, risk_level, parameter_schema_json, created_at, updated_at)
       VALUES ($1,$2,$3,$4,$5,$6,$7)
       ON CONFLICT (id) DO UPDATE SET
         name = EXCLUDED.name,
         description = EXCLUDED.description,
         risk_level = EXCLUDED.risk_level,
         parameter_schema_json = EXCLUDED.parameter_schema_json,
         updated_at = EXCLUDED.updated_at`,
      [template.id, template.name, template.description || null, template.riskLevel || template.risk_level || 'medium', JSON.stringify(template.parameterSchema || template.parameter_schema_json || {}), template.createdAt || new Date().toISOString(), template.updatedAt || new Date().toISOString()]
    );
    return template;
  }

  async listPaperExecutions() {
    const result = await this.query('SELECT * FROM paper_executions ORDER BY started_at ASC, id ASC');
    return result.rows;
  }

  async upsertPaperExecution(execution) {
    await this.query(
      `INSERT INTO paper_executions (id, strategy_id, account_id, status, mode, started_at, stopped_at, stop_reason, last_heartbeat_at, fills_json)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       ON CONFLICT (id) DO UPDATE SET
         status = EXCLUDED.status,
         stopped_at = EXCLUDED.stopped_at,
         stop_reason = EXCLUDED.stop_reason,
         last_heartbeat_at = EXCLUDED.last_heartbeat_at,
         fills_json = EXCLUDED.fills_json`,
      [execution.id, execution.strategyId, execution.accountId || null, execution.status, execution.mode || 'paper', execution.startedAt || new Date().toISOString(), execution.stoppedAt || null, execution.stopReason || null, execution.lastHeartbeatAt || null, JSON.stringify(execution.fills || [])]
    );
    return execution;
  }

  async replaceProductLayer({ accounts = [], instruments = [], strategyTemplates = [], paperExecutions = [] }) {
    await this.query('DELETE FROM paper_executions');
    await this.query('DELETE FROM accounts');
    await this.query('DELETE FROM instruments');
    await this.query('DELETE FROM strategy_templates');
    for (const account of accounts) await this.upsertAccount(account);
    for (const instrument of instruments) await this.upsertInstrument(instrument);
    for (const template of strategyTemplates) await this.upsertStrategyTemplate(template);
    for (const execution of paperExecutions) await this.upsertPaperExecution(execution);
  }

  async insertAudit(event) {
    await this.query(
      `INSERT INTO audit_events (id, action, actor, at, details, payload_json)
       VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (id) DO NOTHING`,
      [event.id, event.action, event.actor || 'system', event.at || new Date().toISOString(), event.details || null, JSON.stringify(event.payload || {})]
    );
    return event;
  }

  async upsertOperatorFlag(key, value) {
    await this.query(
      `INSERT INTO operator_flags (key, value_json, updated_at)
       VALUES ($1,$2,$3)
       ON CONFLICT (key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at`,
      [key, JSON.stringify(value), new Date().toISOString()]
    );
    return { key, value };
  }
}
