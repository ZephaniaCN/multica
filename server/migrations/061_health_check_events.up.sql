-- Health check events table for execution container monitoring
CREATE TABLE IF NOT EXISTS health_check_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    workspace_id UUID NOT NULL REFERENCES workspace(id) ON DELETE CASCADE,
    check_type TEXT NOT NULL CHECK (check_type IN ('heartbeat', 'stream_disconnect', 'execution_timeout', 'state_consistency')),
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'critical')),
    resource_type TEXT NOT NULL CHECK (resource_type IN ('autopilot_run', 'agent_runtime', 'agent_task', 'issue')),
    resource_id UUID NOT NULL,
    details JSONB NOT NULL DEFAULT '{}',
    detected_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Indexes for efficient health check queries
CREATE INDEX IF NOT EXISTS idx_health_check_events_workspace ON health_check_events(workspace_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_health_check_events_type ON health_check_events(check_type, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_health_check_events_resource ON health_check_events(resource_type, resource_id, detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_health_check_events_unresolved ON health_check_events(severity, detected_at) WHERE resolved_at IS NULL;

-- Add health check configuration to autopilot_runs table for future extensibility
ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS last_heartbeat_at TIMESTAMPTZ;
ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS stream_status TEXT CHECK (stream_status IN ('connected', 'disconnected', 'unknown'));
ALTER TABLE autopilot_run ADD COLUMN IF NOT EXISTS health_check_metadata JSONB NOT NULL DEFAULT '{}';

CREATE INDEX IF NOT EXISTS idx_autopilot_run_heartbeat ON autopilot_run(last_heartbeat_at) WHERE last_heartbeat_at IS NOT NULL;