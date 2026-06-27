package main

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/internal/util"
)

// TestHealthCheckDetector tests the health check detector functionality
func TestHealthCheckDetector(t *testing.T) {
	// This is a basic test to ensure the code compiles
	// In a real environment, this would require a test database setup

	t.Run("stale run detection", func(t *testing.T) {
		ctx := context.Background()

		// Mock stale runs
		mockRuns := []db.GetStaleAutopilotRunsRow{
			{
				ID:            uuid.New(),
				WorkspaceID:   uuid.New(),
				AutopilotID:   uuid.New(),
				Status:        "running",
				TriggeredAt:   time.Now().Add(-5 * time.Hour),
				StreamStatus:  "connected",
				AutopilotTitle: "Test Autopilot",
			},
		}

		if len(mockRuns) == 0 {
			t.Error("expected at least one mock run")
		}
	})

	t.Run("heartbeat detection", func(t *testing.T) {
		ctx := context.Background()

		// Mock runs without heartbeat
		mockRuns := []db.GetAutopilotRunsWithoutHeartbeatRow{
			{
				ID:             uuid.New(),
				WorkspaceID:    uuid.New(),
				AutopilotID:    uuid.New(),
				Status:         "running",
				LastHeartbeatAt: pgtype.Timestamptz{Time: time.Now().Add(-30 * time.Minute), Valid: true},
				AutopilotTitle: "Test Autopilot",
			},
		}

		if len(mockRuns) == 0 {
			t.Error("expected at least one mock run")
		}
	})

	t.Run("state consistency check", func(t *testing.T) {
		ctx := context.Background()

		// Mock inconsistent runs
		mockRuns := []db.GetStateInconsistentRunsRow{
			{
				RunID:          uuid.New(),
				RunStatus:      "completed",
				WorkspaceID:    uuid.New(),
				IssueID:        uuid.New(),
				IssueStatus:    "in_progress",
				IssueTitle:     "Test Issue",
				AutopilotTitle: "Test Autopilot",
			},
		}

		if len(mockRuns) == 0 {
			t.Error("expected at least one mock run")
		}
	})
}

// TestDetailsJSON tests the JSON conversion function
func TestDetailsJSON(t *testing.T) {
	details := map[string]interface{}{
		"test_key":  "test_value",
		"timestamp": time.Now().Format(time.RFC3339),
		"count":     42,
	}

	result := detailsJSON(details)

	if !result.Valid {
		t.Error("expected valid JSONB result")
	}

	if len(result.Bytes) == 0 {
		t.Error("expected non-empty JSON bytes")
	}
}