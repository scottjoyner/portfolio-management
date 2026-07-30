-- 005_runtime_job_queue.sql
-- Durable coordination for economic maintenance and other singleton/recurring runtime work.
-- Jobs are claimed with SELECT ... FOR UPDATE SKIP LOCKED and protected by expiring leases.

CREATE TABLE IF NOT EXISTS runtime_jobs (
  id TEXT PRIMARY KEY,
  job_type TEXT NOT NULL,
  scope TEXT NOT NULL DEFAULT 'global',
  status TEXT NOT NULL DEFAULT 'queued'
    CHECK (status IN ('queued', 'running', 'retry', 'completed', 'failed', 'cancelled')),
  priority INTEGER NOT NULL DEFAULT 100,
  scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  attempt INTEGER NOT NULL DEFAULT 0 CHECK (attempt >= 0),
  max_attempts INTEGER NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  lease_owner TEXT,
  lease_expires_at TIMESTAMPTZ,
  idempotency_key TEXT NOT NULL UNIQUE,
  payload_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  result_json JSONB,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runtime_jobs_claim
  ON runtime_jobs(status, scheduled_at, priority, created_at)
  WHERE status IN ('queued', 'retry');

CREATE INDEX IF NOT EXISTS idx_runtime_jobs_lease
  ON runtime_jobs(lease_expires_at)
  WHERE status = 'running';

CREATE INDEX IF NOT EXISTS idx_runtime_jobs_type_scope
  ON runtime_jobs(job_type, scope, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_runtime_jobs_worker
  ON runtime_jobs(lease_owner, status)
  WHERE lease_owner IS NOT NULL;
