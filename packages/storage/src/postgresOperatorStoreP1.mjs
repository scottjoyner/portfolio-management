import { PostgresOperatorStore } from './postgresOperatorStore.mjs';
import { normalizeOperatorState } from './operatorStore.mjs';

function rowJson(value, fallback) {
  if (value === null || value === undefined) return fallback;
  if (typeof value === 'string') return JSON.parse(value);
  return value;
}

export class PostgresOperatorStoreP1 extends PostgresOperatorStore {
  constructor(options = {}) {
    super(options);
    this.kind = 'postgres-p1';
  }

  async load() {
    const base = await super.load();
    const flags = await this.query("SELECT key, value_json FROM operator_flags WHERE key IN ('accounts','instruments','strategy_templates','paper_executions')");
    const flagMap = Object.fromEntries(flags.rows.map(row => [row.key, rowJson(row.value_json, [])]));
    this.state = normalizeOperatorState({
      ...base,
      accounts: flagMap.accounts || base.accounts,
      instruments: flagMap.instruments || base.instruments,
      strategyTemplates: flagMap.strategy_templates || base.strategyTemplates,
      paperExecutions: flagMap.paper_executions || base.paperExecutions
    });
    return this.state;
  }

  async save(nextState) {
    const state = normalizeOperatorState(nextState);
    await super.save(state);
    const now = new Date().toISOString();
    const rows = [
      ['accounts', state.accounts],
      ['instruments', state.instruments],
      ['strategy_templates', state.strategyTemplates],
      ['paper_executions', state.paperExecutions]
    ];
    for (const [key, value] of rows) {
      await this.query(
        'INSERT INTO operator_flags (key, value_json, updated_at) VALUES ($1,$2,$3) ON CONFLICT (key) DO UPDATE SET value_json = EXCLUDED.value_json, updated_at = EXCLUDED.updated_at',
        [key, JSON.stringify(value), now]
      );
    }
    this.state = state;
    return this.state;
  }

  getStatus() {
    return { ...super.getStatus(), productLayer: 'p1-json-flags' };
  }
}
