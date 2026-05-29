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
      [execution.id, execution.strategyId, execution.accountId || null, execution.status, execution.mode || 'paper', execution.startedAt, execution.stoppedAt || null, execution.stopReason || null, execution.lastHeartbeatAt || null, JSON.stringify(execution.fills || [])]
    );
    return execution;
  }
}
