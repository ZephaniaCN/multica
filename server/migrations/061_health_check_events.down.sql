-- Drop health check indexes
DROP INDEX IF EXISTS idx_autopilot_run_heartbeat;
DROP INDEX IF EXISTS idx_health_check_events_unresolved;
DROP INDEX IF NOT EXISTS idx_health_check_events_resource;
DROP INDEX IF NOT EXISTS idx_health_check_events_type;
DROP INDEX IF NOT EXISTS idx_health_check_events_workspace;

-- Remove health check columns from autopilot_runs
ALTER TABLE autopilot_run DROP COLUMN IF EXISTS health_check_metadata;
ALTER TABLE autopilot_run DROP COLUMN IF EXISTS stream_status;
ALTER TABLE autopilot_run DROP COLUMN IF EXISTS last_heartbeat_at;

-- Drop health check events table
DROP TABLE IF EXISTS health_check_events;