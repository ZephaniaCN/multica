package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

const (
	// healthCheckInterval is how often we run comprehensive health checks
	healthCheckInterval = 15 * time.Minute
	// staleRunThreshold is how long a run can be active before being considered stale
	staleRunThreshold = 4 * time.Hour
	// criticalRunThreshold is how long before a run is critically stale
	criticalRunThreshold = 12 * time.Hour
	// heartbeatTimeout is how long since last heartbeat before considering a run dead
	heartbeatTimeout = 30 * time.Minute
)

// StaleAutopilotRun represents an autopilot run that has been active too long
type StaleAutopilotRun struct {
	ID                 uuid.UUID
	WorkspaceID        uuid.UUID
	AutopilotID        uuid.UUID
	Status             string
	TriggeredAt         time.Time
	CompletedAt        pgtype.Time
	LastHeartbeatAt     pgtype.Time
	StreamStatus       string
	IssueID            pgtype.UUID
	TaskID             pgtype.UUID
	AutopilotTitle     string
	HoursSinceTrigger  float64
}

// StateInconsistentRun represents a run with mismatched run/issue status
type StateInconsistentRun struct {
	RunID          uuid.UUID
	RunStatus      string
	WorkspaceID    uuid.UUID
	IssueID        uuid.UUID
	IssueStatus    string
	IssueTitle     string
	TriggeredAt     time.Time
	AutopilotTitle  string
}

// runHealthCheckDetector runs the comprehensive health check system
func runHealthCheckDetector(ctx context.Context, queries *db.Queries, bus *events.Bus) {
	ticker := time.NewTicker(healthCheckInterval)
	defer ticker.Stop()

	// Run initial check on startup
	slog.Info("health check detector: starting initial comprehensive check")
	runAllHealthChecks(ctx, queries, bus)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			slog.Info("health check detector: running scheduled comprehensive check")
			runAllHealthChecks(ctx, queries, bus)
		}
	}
}

// runAllHealthChecks executes all health check categories
func runAllHealthChecks(ctx context.Context, queries *db.Queries, bus *events.Bus) {
	checkStaleAutopilotRuns(ctx, queries, bus)
	checkMissingHeartbeats(ctx, queries, bus)
	checkStateConsistency(ctx, queries, bus)
	reportHealthCheckStats(ctx, queries)
}

// checkStaleAutopilotRuns detects autopilot runs stuck in non-terminal states
func checkStaleAutopilotRuns(ctx context.Context, queries *db.Queries, bus *events.Bus) {
	// Get runs that have been active too long (basic stale detection)
	rows, err := queries.GetStaleAutopilotRuns(ctx, staleRunThreshold)
	if err != nil {
		slog.Warn("health check: failed to get stale autopilot runs", "error", err)
		return
	}

	if len(rows) == 0 {
		slog.Debug("health check: no stale autopilot runs found")
		return
	}

	slog.Info("health check: found stale autopilot runs", "count", len(rows))

	for _, row := range rows {
		severity := "warning"
		if row.HoursSinceTrigger >= float64(criticalRunThreshold.Hours()) {
			severity = "critical"
		}

		details := map[string]interface{}{
			"hours_since_trigger":   row.HoursSinceTrigger,
			"triggered_at":          row.TriggeredAt.Format(time.RFC3339),
			"status":                 row.Status,
			"autopilot_title":        row.AutopilotTitle,
			"last_heartbeat_at":      nil,
			"stream_status":          row.StreamStatus,
			"issue_id":               nil,
			"task_id":                nil,
		}

		if row.LastHeartbeatAt.Valid {
			details["last_heartbeat_at"] = row.LastHeartbeatAt.Time.Format(time.RFC3339)
		}
		if row.IssueID.Valid {
			details["issue_id"] = util.UUIDToString(row.IssueID)
		}
		if row.TaskID.Valid {
			details["task_id"] = util.UUIDToString(row.TaskID)
		}

		// Create health check event
		createHealthCheckEvent(ctx, queries, db.CreateHealthCheckEventParams{
			WorkspaceID: row.WorkspaceID,
			CheckType:   "execution_timeout",
			Severity:    severity,
			ResourceType: "autopilot_run",
			ResourceID:   row.ID,
			Details:      detailsJSON(details),
			DetectedAt:   pgtype.Timestamptz{Time: time.Now(), Valid: true},
		})

		slog.Warn("health check: stale autopilot run detected",
			"run_id", util.UUIDToString(row.ID),
			"autopilot", row.AutopilotTitle,
			"hours_active", row.HoursSinceTrigger,
			"severity", severity)
	}
}

// checkMissingHeartbeats detects runs that haven't sent recent heartbeats
func checkMissingHeartbeats(ctx context.Context, queries *db.Queries, bus *events.Bus) {
	rows, err := queries.GetAutopilotRunsWithoutHeartbeat(ctx)
	if err != nil {
		slog.Warn("health check: failed to get runs without heartbeat", "error", err)
		return
	}

	if len(rows) == 0 {
		slog.Debug("health check: all runs have recent heartbeats")
		return
	}

	slog.Info("health check: found runs without recent heartbeat", "count", len(rows))

	for _, row := range rows {
		details := map[string]interface{}{
			"hours_since_heartbeat": row.HoursSinceHeartbeat,
			"last_heartbeat_at":     row.LastHeartbeatAt.Time.Format(time.RFC3339),
			"status":                row.Status,
			"autopilot_title":       row.AutopilotTitle,
			"stream_status":         row.StreamStatus,
		}

		// Create health check event
		createHealthCheckEvent(ctx, queries, db.CreateHealthCheckEventParams{
			WorkspaceID: row.WorkspaceID,
			CheckType:   "heartbeat",
			Severity:    "warning",
			ResourceType: "autopilot_run",
			ResourceID:   row.ID,
			Details:      detailsJSON(details),
			DetectedAt:   pgtype.Timestamptz{Time: time.Now(), Valid: true},
		})

		// Update stream status to disconnected if heartbeat is missing
		_, err := queries.UpdateAutopilotRunStreamStatus(ctx, db.UpdateAutopilotRunStreamStatusParams{
			ID:           row.ID,
			StreamStatus: "disconnected",
		})
		if err != nil {
			slog.Warn("health check: failed to update stream status", "run_id", util.UUIDToString(row.ID), "error", err)
		}

		slog.Warn("health check: run without heartbeat detected",
			"run_id", util.UUIDToString(row.ID),
			"hours_since_heartbeat", row.HoursSinceHeartbeat)
	}
}

// checkStateConsistency detects runs where run status doesn't match issue status
func checkStateConsistency(ctx context.Context, queries *db.Queries, bus *events.Bus) {
	rows, err := queries.GetStateInconsistentRuns(ctx)
	if err != nil {
		slog.Warn("health check: failed to get state inconsistent runs", "error", err)
		return
	}

	if len(rows) == 0 {
		slog.Debug("health check: all runs have consistent states")
		return
	}

	slog.Info("health check: found state inconsistent runs", "count", len(rows))

	for _, row := range rows {
		details := map[string]interface{}{
			"run_status":      row.RunStatus,
			"issue_status":    row.IssueStatus,
			"issue_title":     row.IssueTitle,
			"triggered_at":    row.TriggeredAt.Format(time.RFC3339),
			"autopilot_title": row.AutopilotTitle,
		}

		// Create health check event
		createHealthCheckEvent(ctx, queries, db.CreateHealthCheckEventParams{
			WorkspaceID: row.WorkspaceID,
			CheckType:   "state_consistency",
			Severity:    "warning",
			ResourceType: "autopilot_run",
			ResourceID:   row.RunID,
			Details:      detailsJSON(details),
			DetectedAt:   pgtype.Timestamptz{Time: time.Now(), Valid: true},
		})

		slog.Warn("health check: state inconsistent run detected",
			"run_id", util.UUIDToString(row.RunID),
			"run_status", row.RunStatus,
			"issue_status", row.IssueStatus)
	}
}

// reportHealthCheckStats logs overall health check statistics
func reportHealthCheckStats(ctx context.Context, queries *db.Queries) {
	stats, err := queries.GetHealthCheckStats(ctx)
	if err != nil {
		slog.Warn("health check: failed to get statistics", "error", err)
		return
	}

	slog.Info("health check: statistics",
		"total_events", stats.TotalEvents,
		"unresolved_events", stats.UnresolvedEvents,
		"critical_events", stats.CriticalEvents,
		"last_check_at", stats.LastCheckAt.Time.Format(time.RFC3339))
}

// createHealthCheckEvent creates a health check event record
func createHealthCheckEvent(ctx context.Context, queries *db.Queries, params db.CreateHealthCheckEventParams) {
	_, err := queries.CreateHealthCheckEvent(ctx, params)
	if err != nil {
		slog.Error("health check: failed to create event", "error", err)
	}
}

// detailsJSON converts a map to JSON for database storage
func detailsJSON(details map[string]interface{}) pgtype.JSONB {
	data, _ := json.Marshal(details)
	return pgtype.JSONB{Bytes: data, Valid: true}
}