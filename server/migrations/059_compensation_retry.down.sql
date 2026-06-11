DROP INDEX IF EXISTS idx_autopilot_run_stuck;
DROP INDEX IF EXISTS idx_autopilot_run_retry_of;
ALTER TABLE autopilot_run DROP COLUMN IF EXISTS compensation_key;
ALTER TABLE autopilot_run DROP COLUMN IF EXISTS retry_of;
ALTER TABLE autopilot_run DROP COLUMN IF EXISTS is_compensation;
