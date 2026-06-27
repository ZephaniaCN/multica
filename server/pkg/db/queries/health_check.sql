-- name: CreateHealthCheckEvent :one
INSERT INTO health_check_events (
    workspace_id,
    check_type,
    severity,
    resource_type,
    resource_id,
    details,
    detected_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)
RETURNING *;

-- name: GetUnresolvedHealthCheckEvents :many
SELECT * FROM health_check_events
WHERE resolved_at IS NULL
ORDER BY detected_at DESC;

-- name: GetHealthCheckEventsByResource :many
SELECT * FROM health_check_events
WHERE resource_type = $1 AND resource_id = $2
ORDER BY detected_at DESC
LIMIT $3;

-- name: ResolveHealthCheckEvent :exec
UPDATE health_check_events
SET resolved_at = now()
WHERE id = $1;

-- name: GetHealthCheckEventByID :one
SELECT * FROM health_check_events
WHERE id = $1;

-- name: GetStaleAutopilotRuns :many
-- Find autopilot runs that have been in non-terminal state for too long
SELECT
    ar.id,
    ar.workspace_id,
    ar.autopilot_id,
    ar.status,
    ar.triggered_at,
    ar.completed_at,
    ar.last_heartbeat_at,
    ar.stream_status,
    ar.issue_id,
    ar.task_id,
    a.title as autopilot_title,
    EXTRACT(EPOCH FROM (NOW() - ar.triggered_at)) / 3600.0 as hours_since_trigger
FROM autopilot_run ar
JOIN autopilot a ON ar.autopilot_id = a.id
WHERE ar.status IN ('pending', 'issue_created', 'running')
  AND NOW() - ar.triggered_at > INTERVAL '1 hour'
ORDER BY ar.triggered_at ASC;

-- name: GetAutopilotRunsWithoutHeartbeat :many
-- Find autopilot runs that haven't sent heartbeat recently
SELECT
    ar.id,
    ar.workspace_id,
    ar.autopilot_id,
    ar.status,
    ar.triggered_at,
    ar.last_heartbeat_at,
    ar.stream_status,
    a.title as autopilot_title,
    EXTRACT(EPOCH FROM (NOW() - ar.last_heartbeat_at)) / 3600.0 as hours_since_heartbeat
FROM autopilot_run ar
JOIN autopilot a ON ar.autopilot_id = a.id
WHERE ar.status = 'running'
  AND ar.last_heartbeat_at IS NOT NULL
  AND NOW() - ar.last_heartbeat_at > INTERVAL '15 minutes'
ORDER BY ar.last_heartbeat_at ASC;

-- name: GetStateInconsistentRuns :many
-- Find autopilot runs where the run status doesn't match the linked issue status
SELECT
    ar.id as run_id,
    ar.status as run_status,
    ar.workspace_id,
    ar.issue_id,
    i.status as issue_status,
    i.title as issue_title,
    ar.triggered_at,
    a.title as autopilot_title
FROM autopilot_run ar
JOIN autopilot a ON ar.autopilot_id = a.id
LEFT JOIN issue i ON ar.issue_id = i.id
WHERE ar.issue_id IS NOT NULL
  AND (
    (ar.status = 'completed' AND i.status != 'done') OR
    (ar.status = 'failed' AND i.status NOT IN ('todo', 'blocked')) OR
    (ar.status IN ('pending', 'running') AND i.status = 'done') OR
    (ar.status = 'skipped' AND i.status != 'cancelled')
  )
  AND ar.created_at > NOW() - INTERVAL '7 days';

-- name: UpdateAutopilotRunHeartbeat :exec
UPDATE autopilot_run
SET last_heartbeat_at = NOW(),
    updated_at = NOW()
WHERE id = $1;

-- name: UpdateAutopilotRunStreamStatus :exec
UPDATE autopilot_run
SET stream_status = $2,
    updated_at = NOW()
WHERE id = $1;

-- name: GetHealthCheckStats :one
-- Get statistics about health check events for monitoring
SELECT
    COUNT(*) as total_events,
    COUNT(*) FILTER (WHERE resolved_at IS NULL) as unresolved_events,
    COUNT(*) FILTER (WHERE severity = 'critical') as critical_events,
    COUNT(*) FILTER (WHERE check_type = 'heartbeat') as heartbeat_checks,
    COUNT(*) FILTER (WHERE check_type = 'stream_disconnect') as stream_disconnects,
    COUNT(*) FILTER (WHERE check_type = 'execution_timeout') as execution_timeouts,
    COUNT(*) FILTER (WHERE check_type = 'state_consistency') as state_inconsistencies,
    MAX(detected_at) as last_check_at
FROM health_check_events
WHERE detected_at > NOW() - INTERVAL '24 hours';