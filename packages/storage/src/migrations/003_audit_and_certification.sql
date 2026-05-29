-- 003_audit_and_certification.sql
-- First-production certification hardening.

ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS previous_hash TEXT;
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS event_hash TEXT;
ALTER TABLE audit_events ADD COLUMN IF NOT EXISTS sequence_number BIGINT;

CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_events_sequence_number ON audit_events(sequence_number) WHERE sequence_number IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_audit_events_event_hash ON audit_events(event_hash) WHERE event_hash IS NOT NULL;

CREATE TABLE IF NOT EXISTS adapter_certifications (
  id TEXT PRIMARY KEY,
  adapter_name TEXT NOT NULL,
  adapter_kind TEXT NOT NULL CHECK (adapter_kind IN ('market_data', 'broker_execution', 'onchain', 'settlement')),
  status TEXT NOT NULL CHECK (status IN ('draft', 'certified_paper', 'certified_live', 'revoked', 'blocked')),
  live_enabled BOOLEAN NOT NULL DEFAULT false,
  certified_at TIMESTAMPTZ,
  expires_at TIMESTAMPTZ,
  reviewer TEXT,
  evidence_json JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_adapter_certifications_adapter ON adapter_certifications(adapter_name, adapter_kind);
CREATE INDEX IF NOT EXISTS idx_adapter_certifications_status ON adapter_certifications(status);
