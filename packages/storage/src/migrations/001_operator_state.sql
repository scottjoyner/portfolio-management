-- 001_operator_state.sql
-- P0.3 baseline schema for durable operator state.
-- This is the canonical SQL target for the current file-backed repository layer.
-- The API currently persists to data/operator-state.json; this schema defines the
-- Postgres migration path for the next repository implementation slice.

CREATE TABLE IF NOT EXISTS strategies (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL CHECK (status IN ('draft', 'active', 'archived', 'blocked')),
  risk_level TEXT NOT NULL CHECK (risk_level IN ('low', 'medium', 'high')),
  parameters_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS backtest_runs (
  id TEXT PRIMARY KEY,
  strategy_id TEXT NOT NULL REFERENCES strategies(id),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
  assumptions_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  equity_curve_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  trades_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS approvals (
  id TEXT PRIMARY KEY,
  strategy_id TEXT NOT NULL REFERENCES strategies(id),
  status TEXT NOT NULL CHECK (status IN ('pending_review', 'approved', 'rejected', 'blocked')),
  tier TEXT NOT NULL CHECK (tier IN ('auto', 'canary', 'production')),
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  reviewed_at TIMESTAMPTZ,
  reviewer TEXT
);

CREATE TABLE IF NOT EXISTS positions (
  id TEXT PRIMARY KEY,
  strategy_id TEXT REFERENCES strategies(id),
  symbol TEXT NOT NULL,
  quantity NUMERIC NOT NULL,
  average_price NUMERIC NOT NULL,
  mark_price NUMERIC,
  status TEXT NOT NULL CHECK (status IN ('open', 'closed', 'reconciling')),
  opened_at TIMESTAMPTZ NOT NULL,
  closed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS audit_events (
  id TEXT PRIMARY KEY,
  action TEXT NOT NULL,
  actor TEXT NOT NULL,
  at TIMESTAMPTZ NOT NULL,
  details TEXT,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS operator_flags (
  key TEXT PRIMARY KEY,
  value_json JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_backtest_runs_strategy_id ON backtest_runs(strategy_id);
CREATE INDEX IF NOT EXISTS idx_approvals_strategy_id ON approvals(strategy_id);
CREATE INDEX IF NOT EXISTS idx_positions_strategy_id ON positions(strategy_id);
CREATE INDEX IF NOT EXISTS idx_audit_events_at ON audit_events(at DESC);
