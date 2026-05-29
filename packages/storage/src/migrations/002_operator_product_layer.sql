-- 002_operator_product_layer.sql
-- P1 product-layer schema for accounts, instruments, templates, and paper runs.

CREATE TABLE IF NOT EXISTS accounts (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  provider TEXT NOT NULL,
  status TEXT NOT NULL,
  currency TEXT NOT NULL DEFAULT 'USD',
  cash NUMERIC NOT NULL DEFAULT 0,
  nav NUMERIC NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS instruments (
  symbol TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  asset_class TEXT NOT NULL,
  venue TEXT NOT NULL,
  status TEXT NOT NULL,
  min_order_size NUMERIC NOT NULL DEFAULT 0,
  price_precision INTEGER NOT NULL DEFAULT 2
);

CREATE TABLE IF NOT EXISTS strategy_templates (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  description TEXT,
  risk_level TEXT NOT NULL,
  parameter_schema_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS paper_executions (
  id TEXT PRIMARY KEY,
  strategy_id TEXT NOT NULL REFERENCES strategies(id),
  account_id TEXT REFERENCES accounts(id),
  status TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'paper',
  started_at TIMESTAMPTZ NOT NULL,
  stopped_at TIMESTAMPTZ,
  stop_reason TEXT,
  last_heartbeat_at TIMESTAMPTZ,
  fills_json JSONB NOT NULL DEFAULT '[]'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_accounts_provider ON accounts(provider);
CREATE INDEX IF NOT EXISTS idx_instruments_asset_class ON instruments(asset_class);
CREATE INDEX IF NOT EXISTS idx_strategy_templates_risk_level ON strategy_templates(risk_level);
CREATE INDEX IF NOT EXISTS idx_paper_executions_strategy_id ON paper_executions(strategy_id);
CREATE INDEX IF NOT EXISTS idx_paper_executions_status ON paper_executions(status);
