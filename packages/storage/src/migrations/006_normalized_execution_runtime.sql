-- 006_normalized_execution_runtime.sql
-- Durable execution lifecycle records with optimistic versions and append-only events.

CREATE TABLE IF NOT EXISTS execution_records (
  id TEXT PRIMARY KEY,
  idempotency_key TEXT NOT NULL UNIQUE,
  opportunity_id TEXT,
  strategy_id TEXT,
  source_agent_id TEXT,
  economic_decision_id TEXT,
  model_quote_id TEXT,
  forecast_id TEXT,
  execution_cost_snapshot_id TEXT,
  symbol TEXT NOT NULL,
  venue TEXT NOT NULL,
  mode TEXT NOT NULL DEFAULT 'paper' CHECK (mode IN ('paper', 'live')),
  side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
  status TEXT NOT NULL DEFAULT 'draft'
    CHECK (status IN ('draft', 'approved', 'rejected', 'submitted', 'partially_filled', 'filled', 'settlement_pending', 'settled', 'failed', 'cancelled')),
  version BIGINT NOT NULL DEFAULT 1 CHECK (version > 0),
  quantity NUMERIC,
  notional_usd NUMERIC,
  requested_price NUMERIC,
  net_executable_edge_usd NUMERIC,
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  terminal_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_execution_records_status_updated
  ON execution_records(status, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_records_opportunity
  ON execution_records(opportunity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_records_economic_decision
  ON execution_records(economic_decision_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_execution_records_symbol_venue
  ON execution_records(symbol, venue, created_at DESC);

CREATE TABLE IF NOT EXISTS execution_events (
  event_id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES execution_records(id) ON DELETE RESTRICT,
  sequence_number BIGSERIAL NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  from_status TEXT,
  to_status TEXT,
  execution_version BIGINT NOT NULL CHECK (execution_version > 0),
  actor TEXT NOT NULL DEFAULT 'system',
  idempotency_key TEXT UNIQUE,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_execution_events_execution_sequence
  ON execution_events(execution_id, sequence_number ASC);
CREATE INDEX IF NOT EXISTS idx_execution_events_type_created
  ON execution_events(event_type, created_at DESC);

CREATE OR REPLACE FUNCTION prevent_execution_event_mutation()
RETURNS trigger AS $$
BEGIN
  RAISE EXCEPTION 'execution_events_are_append_only';
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS execution_events_append_only ON execution_events;
CREATE TRIGGER execution_events_append_only
  BEFORE UPDATE OR DELETE ON execution_events
  FOR EACH ROW EXECUTE FUNCTION prevent_execution_event_mutation();

CREATE TABLE IF NOT EXISTS execution_orders (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES execution_records(id) ON DELETE RESTRICT,
  client_order_id TEXT,
  venue_order_id TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  side TEXT NOT NULL CHECK (side IN ('buy', 'sell')),
  order_type TEXT NOT NULL DEFAULT 'market',
  time_in_force TEXT,
  quantity NUMERIC,
  limit_price NUMERIC,
  status TEXT NOT NULL DEFAULT 'planned',
  request_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  response_json JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_execution_orders_execution
  ON execution_orders(execution_id, created_at ASC);
CREATE INDEX IF NOT EXISTS idx_execution_orders_venue_order
  ON execution_orders(venue_order_id)
  WHERE venue_order_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS execution_fills (
  id TEXT PRIMARY KEY,
  execution_id TEXT NOT NULL REFERENCES execution_records(id) ON DELETE RESTRICT,
  order_id TEXT REFERENCES execution_orders(id) ON DELETE RESTRICT,
  venue_fill_id TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  quantity NUMERIC NOT NULL,
  price NUMERIC NOT NULL,
  fee_usd NUMERIC NOT NULL DEFAULT 0,
  liquidity TEXT,
  settlement_status TEXT NOT NULL DEFAULT 'pending',
  metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  filled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_execution_fills_execution
  ON execution_fills(execution_id, filled_at ASC);
CREATE INDEX IF NOT EXISTS idx_execution_fills_order
  ON execution_fills(order_id, filled_at ASC)
  WHERE order_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_execution_fills_settlement
  ON execution_fills(settlement_status, filled_at ASC)
  WHERE settlement_status <> 'settled';
