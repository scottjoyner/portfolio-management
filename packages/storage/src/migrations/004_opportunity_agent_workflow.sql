-- 004_opportunity_agent_workflow.sql
-- Durable schema target for API-backed opportunity review, risk, research jobs, agent budgets, costs, and market snapshots.
-- This remains paper/review workflow storage; it does not enable live execution.

CREATE TABLE IF NOT EXISTS market_data_snapshots (
  id TEXT PRIMARY KEY,
  symbol TEXT NOT NULL,
  venue TEXT NOT NULL,
  asset_class TEXT NOT NULL,
  bid NUMERIC,
  ask NUMERIC,
  spread_bps NUMERIC,
  volume_24h NUMERIC,
  liquidity_score NUMERIC,
  volatility_score NUMERIC,
  status TEXT NOT NULL DEFAULT 'watching',
  source TEXT NOT NULL,
  timestamp TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_budgets (
  agent_id TEXT PRIMARY KEY,
  daily_token_limit BIGINT NOT NULL DEFAULT 0,
  daily_cost_limit NUMERIC NOT NULL DEFAULT 0,
  per_job_token_limit BIGINT NOT NULL DEFAULT 0,
  per_market_cost_limit NUMERIC NOT NULL DEFAULT 0,
  require_approval_above_cost NUMERIC NOT NULL DEFAULT 0,
  enabled BOOLEAN NOT NULL DEFAULT true,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS budget_approvals (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  market_scope TEXT,
  opportunity_id TEXT,
  requested_by TEXT NOT NULL,
  reason TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('pending_review', 'approved', 'rejected', 'expired')),
  projected_cost NUMERIC NOT NULL DEFAULT 0,
  projected_tokens BIGINT NOT NULL DEFAULT 0,
  approved_cost_limit NUMERIC NOT NULL DEFAULT 0,
  approved_token_limit BIGINT NOT NULL DEFAULT 0,
  reviewer TEXT,
  decision_reason TEXT,
  requested_at TIMESTAMPTZ NOT NULL,
  reviewed_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS research_jobs (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  trigger_type TEXT NOT NULL,
  market_scope TEXT NOT NULL,
  symbol_scope TEXT,
  provider TEXT NOT NULL,
  model TEXT NOT NULL,
  local_or_remote TEXT NOT NULL CHECK (local_or_remote IN ('local', 'remote')),
  status TEXT NOT NULL CHECK (status IN ('queued', 'running', 'completed', 'failed', 'cancelled')),
  started_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  prompt_tokens BIGINT NOT NULL DEFAULT 0,
  completion_tokens BIGINT NOT NULL DEFAULT 0,
  total_tokens BIGINT NOT NULL DEFAULT 0,
  estimated_remote_cost NUMERIC NOT NULL DEFAULT 0,
  estimated_local_cost NUMERIC NOT NULL DEFAULT 0,
  budget_approval_id TEXT REFERENCES budget_approvals(id),
  opportunity_ids_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  failure_reason TEXT
);

CREATE TABLE IF NOT EXISTS opportunities (
  id TEXT PRIMARY KEY,
  source_agent_id TEXT NOT NULL,
  research_job_id TEXT REFERENCES research_jobs(id),
  strategy_id TEXT REFERENCES strategies(id),
  market_type TEXT NOT NULL,
  venue TEXT NOT NULL,
  symbol TEXT,
  market_slug TEXT,
  title TEXT NOT NULL,
  recommendation TEXT NOT NULL,
  confidence_score NUMERIC NOT NULL CHECK (confidence_score >= 0 AND confidence_score <= 1),
  win_probability NUMERIC NOT NULL CHECK (win_probability >= 0 AND win_probability <= 1),
  loss_probability NUMERIC NOT NULL CHECK (loss_probability >= 0 AND loss_probability <= 1),
  expected_value NUMERIC NOT NULL DEFAULT 0,
  gross_expected_value NUMERIC NOT NULL DEFAULT 0,
  total_money_risked NUMERIC NOT NULL DEFAULT 0 CHECK (total_money_risked >= 0),
  max_loss NUMERIC NOT NULL DEFAULT 0 CHECK (max_loss >= 0),
  potential_upside NUMERIC NOT NULL DEFAULT 0 CHECK (potential_upside >= 0),
  reward_risk_ratio NUMERIC NOT NULL DEFAULT 0,
  liquidity_score NUMERIC NOT NULL DEFAULT 50,
  data_freshness_score NUMERIC NOT NULL DEFAULT 70,
  backtest_id TEXT REFERENCES backtest_runs(id),
  backtest_status TEXT NOT NULL DEFAULT 'backtest_missing',
  risk_breakdown_id TEXT,
  status TEXT NOT NULL CHECK (status IN ('needs_review', 'approved', 'rejected', 'deferred', 'research_requested', 'blocked')),
  approval_status TEXT NOT NULL,
  estimated_fees NUMERIC NOT NULL DEFAULT 0,
  estimated_slippage NUMERIC NOT NULL DEFAULT 0,
  estimated_gas NUMERIC NOT NULL DEFAULT 0,
  agent_research_cost NUMERIC NOT NULL DEFAULT 0,
  model_inference_cost NUMERIC NOT NULL DEFAULT 0,
  net_expected_value NUMERIC NOT NULL DEFAULT 0,
  notes TEXT,
  evidence_json JSONB NOT NULL DEFAULT '[]'::jsonb,
  expires_at TIMESTAMPTZ,
  reviewed_at TIMESTAMPTZ,
  reviewer TEXT,
  decision_reason TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS risk_breakdowns (
  id TEXT PRIMARY KEY,
  scope TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  aggregate_score NUMERIC NOT NULL,
  capital_at_risk_score NUMERIC NOT NULL,
  liquidity_score NUMERIC NOT NULL,
  slippage_score NUMERIC NOT NULL,
  drawdown_score NUMERIC NOT NULL,
  volatility_score NUMERIC NOT NULL,
  correlation_score NUMERIC NOT NULL,
  model_confidence_score NUMERIC NOT NULL,
  data_freshness_score NUMERIC NOT NULL,
  agent_cost_score NUMERIC NOT NULL,
  explanation TEXT NOT NULL,
  generated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_cost_ledger (
  id TEXT PRIMARY KEY,
  agent_id TEXT NOT NULL,
  job_id TEXT NOT NULL REFERENCES research_jobs(id),
  model TEXT NOT NULL,
  provider TEXT NOT NULL,
  local_or_remote TEXT NOT NULL CHECK (local_or_remote IN ('local', 'remote')),
  prompt_tokens BIGINT NOT NULL DEFAULT 0,
  completion_tokens BIGINT NOT NULL DEFAULT 0,
  total_tokens BIGINT NOT NULL DEFAULT 0,
  remote_api_cost NUMERIC NOT NULL DEFAULT 0,
  local_compute_cost NUMERIC NOT NULL DEFAULT 0,
  allocated_opportunity_id TEXT REFERENCES opportunities(id),
  created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_market_data_snapshots_symbol ON market_data_snapshots(symbol, venue);
CREATE INDEX IF NOT EXISTS idx_budget_approvals_agent_status ON budget_approvals(agent_id, status);
CREATE INDEX IF NOT EXISTS idx_budget_approvals_market_scope ON budget_approvals(market_scope);
CREATE INDEX IF NOT EXISTS idx_research_jobs_agent_status ON research_jobs(agent_id, status);
CREATE INDEX IF NOT EXISTS idx_research_jobs_market_scope ON research_jobs(market_scope);
CREATE INDEX IF NOT EXISTS idx_research_jobs_budget_approval ON research_jobs(budget_approval_id);
CREATE INDEX IF NOT EXISTS idx_opportunities_status ON opportunities(status);
CREATE INDEX IF NOT EXISTS idx_opportunities_venue_market_type ON opportunities(venue, market_type);
CREATE INDEX IF NOT EXISTS idx_opportunities_source_agent ON opportunities(source_agent_id);
CREATE INDEX IF NOT EXISTS idx_risk_breakdowns_scope ON risk_breakdowns(scope, scope_id);
CREATE INDEX IF NOT EXISTS idx_agent_cost_ledger_agent ON agent_cost_ledger(agent_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_agent_cost_ledger_opportunity ON agent_cost_ledger(allocated_opportunity_id);
