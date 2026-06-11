-- Add compensation retry columns to autopilot_run.
-- Supports exactly-once retry for stream_disconnected terminal failures.

ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS is_compensation BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS retry_of UUID REFERENCES autopilot_run(id) ON DELETE SET NULL;
ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS compensation_key TEXT;

-- Unique partial index for exactly-once deduplication: only one compensation
-- retry per original run. The UNIQUE constraint on compensation_key prevents
-- concurrent listeners/reconciler cycles from creating duplicate retries.
CREATE UNIQUE INDEX IF NOT EXISTS idx_autopilot_run_compensation_key
    ON autopilot_run(compensation_key)
    WHERE compensation_key IS NOT NULL AND is_compensation = true;

-- Index for fast dedupe lookups when checking if a compensation retry exists.
CREATE INDEX IF NOT EXISTS idx_autopilot_run_retry_of ON autopilot_run(retry_of) WHERE retry_of IS NOT NULL;

-- Index for reconciliation scans: find stuck issue_created runs ordered by age.
CREATE INDEX IF NOT EXISTS idx_autopilot_run_stuck ON autopilot_run(status, created_at)
    WHERE status = 'issue_created';
