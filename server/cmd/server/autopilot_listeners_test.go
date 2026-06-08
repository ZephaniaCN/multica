package main

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestAutopilotRunOnlyTaskTerminalEventsUpdateRun(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerAutopilotListeners(bus, autopilotSvc)

	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load fixture agent: %v", err)
	}

	tests := []struct {
		name       string
		finalize   func(task db.AgentTaskQueue)
		wantStatus string
		wantResult string
		wantReason string
	}{
		{
			name: "completed",
			finalize: func(task db.AgentTaskQueue) {
				if _, err := taskSvc.CompleteTask(ctx, task.ID, []byte(`{"output":"done"}`), "", ""); err != nil {
					t.Fatalf("CompleteTask: %v", err)
				}
			},
			wantStatus: "completed",
			wantResult: "done",
		},
		{
			name: "failed",
			finalize: func(task db.AgentTaskQueue) {
				if _, err := taskSvc.FailTask(ctx, task.ID, "boom", "", "", "agent_error"); err != nil {
					t.Fatalf("FailTask: %v", err)
				}
			},
			wantStatus: "failed",
			wantReason: "boom",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
				WorkspaceID:        parseUUID(testWorkspaceID),
				Title:              "Run-only listener " + tc.name,
				Description:        pgtype.Text{String: "Run listener regression test", Valid: true},
				AssigneeID:         parseUUID(agentID),
				Status:             "active",
				ExecutionMode:      "run_only",
				IssueTitleTemplate: pgtype.Text{},
				CreatedByType:      "member",
				CreatedByID:        parseUUID(testUserID),
			})
			if err != nil {
				t.Fatalf("CreateAutopilot: %v", err)
			}
			t.Cleanup(func() {
				if _, err := testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID); err != nil {
					t.Logf("cleanup autopilot: %v", err)
				}
			})

			run, err := autopilotSvc.DispatchAutopilot(ctx, ap, pgtype.UUID{}, "manual", nil)
			if err != nil {
				t.Fatalf("DispatchAutopilot: %v", err)
			}
			if !run.TaskID.Valid {
				t.Fatal("run_only dispatch did not link a task")
			}

			if _, err := testPool.Exec(ctx,
				`UPDATE agent_task_queue SET status = 'dispatched', dispatched_at = now() WHERE id = $1`,
				run.TaskID,
			); err != nil {
				t.Fatalf("mark task dispatched: %v", err)
			}
			task, err := queries.StartAgentTask(ctx, run.TaskID)
			if err != nil {
				t.Fatalf("StartAgentTask: %v", err)
			}

			tc.finalize(task)

			updatedRun, err := queries.GetAutopilotRun(ctx, run.ID)
			if err != nil {
				t.Fatalf("GetAutopilotRun: %v", err)
			}
			if updatedRun.Status != tc.wantStatus {
				t.Fatalf("expected run status %q, got %q", tc.wantStatus, updatedRun.Status)
			}
			if tc.wantResult != "" && !strings.Contains(string(updatedRun.Result), tc.wantResult) {
				t.Fatalf("expected run result to contain %q, got %s", tc.wantResult, string(updatedRun.Result))
			}
			if tc.wantReason != "" {
				if !updatedRun.FailureReason.Valid {
					t.Fatalf("expected failure reason %q, got invalid", tc.wantReason)
				}
				if updatedRun.FailureReason.String != tc.wantReason {
					t.Fatalf("expected failure reason %q, got %q", tc.wantReason, updatedRun.FailureReason.String)
				}
			}
		})
	}
}

// TestSyncRunFromIssueRecovery verifies that when a failed/skipped autopilot
// run's linked issue moves to done/in_review, the run is recovered to completed
// with previous_failure_reason preserved and failure_reason cleared.
func TestSyncRunFromIssueRecovery(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)

	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load fixture agent: %v", err)
	}

	t.Run("failed-run-recovered-on-issue-done", func(t *testing.T) {
		ap, issueID, runID := setupRecoveryTest(t, ctx, queries, autopilotSvc, agentID, "failed", "agent crashed")
		t.Cleanup(func() {
			testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
			testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID)
		})

		// Update issue to done
		_, err := testPool.Exec(ctx, `UPDATE issue SET status = 'done' WHERE id = $1`, issueID)
		if err != nil {
			t.Fatalf("update issue status: %v", err)
		}
		dbIssue, err := queries.GetIssue(ctx, parseUUID(issueID))
		if err != nil {
			t.Fatalf("GetIssue: %v", err)
		}
		autopilotSvc.SyncRunFromIssue(ctx, dbIssue)

		run, err := queries.GetAutopilotRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetAutopilotRun: %v", err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected run status 'completed', got %q", run.Status)
		}
		if !run.PreviousFailureReason.Valid || run.PreviousFailureReason.String != "agent crashed" {
			t.Fatalf("expected previous_failure_reason 'agent crashed', got %v", run.PreviousFailureReason)
		}
		if run.FailureReason.Valid {
			t.Fatalf("expected failure_reason to be cleared, got %q", run.FailureReason.String)
		}
		if !run.CompletedAt.Valid {
			t.Fatal("expected completed_at to be set")
		}
	})

	t.Run("skipped-run-recovered-on-issue-done", func(t *testing.T) {
		ap, issueID, runID := setupRecoveryTest(t, ctx, queries, autopilotSvc, agentID, "skipped", "")
		t.Cleanup(func() {
			testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
			testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID)
		})

		_, err := testPool.Exec(ctx, `UPDATE issue SET status = 'done' WHERE id = $1`, issueID)
		if err != nil {
			t.Fatalf("update issue status: %v", err)
		}
		dbIssue, err := queries.GetIssue(ctx, parseUUID(issueID))
		if err != nil {
			t.Fatalf("GetIssue: %v", err)
		}
		autopilotSvc.SyncRunFromIssue(ctx, dbIssue)

		run, err := queries.GetAutopilotRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetAutopilotRun: %v", err)
		}
		if run.Status != "completed" {
			t.Fatalf("expected run status 'completed', got %q", run.Status)
		}
		if run.FailureReason.Valid {
			t.Fatalf("expected failure_reason to be cleared after recovery, got %q", run.FailureReason.String)
		}
	})

	t.Run("cancel-does-not-overwrite-failed-run", func(t *testing.T) {
		ap, issueID, runID := setupRecoveryTest(t, ctx, queries, autopilotSvc, agentID, "failed", "original error")
		t.Cleanup(func() {
			testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
			testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID)
		})

		// Issue gets cancelled — must NOT reach the failed run via GetActiveAutopilotRunByIssue
		_, err := testPool.Exec(ctx, `UPDATE issue SET status = 'cancelled' WHERE id = $1`, issueID)
		if err != nil {
			t.Fatalf("update issue status: %v", err)
		}
		dbIssue, err := queries.GetIssue(ctx, parseUUID(issueID))
		if err != nil {
			t.Fatalf("GetIssue: %v", err)
		}
		autopilotSvc.SyncRunFromIssue(ctx, dbIssue)

		run, err := queries.GetAutopilotRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetAutopilotRun: %v", err)
		}
		// Run must remain failed with its original failure_reason
		if run.Status != "failed" {
			t.Fatalf("expected run to stay 'failed', got %q", run.Status)
		}
		if !run.FailureReason.Valid || run.FailureReason.String != "original error" {
			t.Fatalf("expected failure_reason 'original error', got %v", run.FailureReason)
		}
	})

	t.Run("previous-failure-reason-only-set-once", func(t *testing.T) {
		ap, issueID, runID := setupRecoveryTest(t, ctx, queries, autopilotSvc, agentID, "failed", "first error")
		t.Cleanup(func() {
			testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
			testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID)
		})

		// First recovery: done -> completed
		_, err := testPool.Exec(ctx, `UPDATE issue SET status = 'done' WHERE id = $1`, issueID)
		if err != nil {
			t.Fatalf("update issue: %v", err)
		}
		dbIssue, err := queries.GetIssue(ctx, parseUUID(issueID))
		if err != nil {
			t.Fatalf("GetIssue: %v", err)
		}
		autopilotSvc.SyncRunFromIssue(ctx, dbIssue)

		run, err := queries.GetAutopilotRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetAutopilotRun after recovery: %v", err)
		}
		if !run.PreviousFailureReason.Valid || run.PreviousFailureReason.String != "first error" {
			t.Fatalf("expected previous_failure_reason 'first error', got %v", run.PreviousFailureReason)
		}

		// UpdateCompleted again (e.g. in_review -> done) — previous_failure_reason must stay 'first error'
		if _, err := queries.UpdateAutopilotRunCompleted(ctx, db.UpdateAutopilotRunCompletedParams{
			ID: run.ID,
		}); err != nil {
			t.Fatalf("UpdateAutopilotRunCompleted: %v", err)
		}
		run2, err := queries.GetAutopilotRun(ctx, runID)
		if err != nil {
			t.Fatalf("GetAutopilotRun after second completion: %v", err)
		}
		if !run2.PreviousFailureReason.Valid || run2.PreviousFailureReason.String != "first error" {
			t.Fatalf("expected previous_failure_reason to stay 'first error', got %v", run2.PreviousFailureReason)
		}
	})
}

// setupRecoveryTest creates an autopilot, dispatches it, creates a linked
// issue, and manually sets the run to the given status. Returns the
// autopilot entity, issue ID string, and run ID.
func setupRecoveryTest(
	t *testing.T,
	ctx context.Context,
	queries *db.Queries,
	svc *service.AutopilotService,
	agentID string,
	runStatus string,
	failureReason string,
) (db.Autopilot, string, pgtype.UUID) {
	t.Helper()

	ap, err := queries.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Recovery test autopilot " + t.Name(),
		Description:        pgtype.Text{String: "Recovery regression test", Valid: true},
		AssigneeID:         parseUUID(agentID),
		Status:             "active",
		ExecutionMode:      "create_issue",
		IssueTitleTemplate: pgtype.Text{},
		CreatedByType:      "member",
		CreatedByID:        parseUUID(testUserID),
	})
	if err != nil {
		t.Fatalf("CreateAutopilot: %v", err)
	}

	// Create a run and link it to an issue
	run, err := queries.CreateAutopilotRun(ctx, db.CreateAutopilotRunParams{
		AutopilotID:    ap.ID,
		Source:         "manual",
		Status:         "issue_created",
		TriggerID:      pgtype.UUID{},
		TriggerPayload: nil,
	})
	if err != nil {
		t.Fatalf("CreateAutopilotRun: %v", err)
	}

	// Create an issue with autopilot origin
	var issueID string
	err = testPool.QueryRow(ctx, `
		INSERT INTO issue (workspace_id, title, status, priority, creator_type, creator_id, assignee_type, assignee_id, position, number, origin_type, origin_id)
		VALUES ($1, 'recovery test issue', 'todo', 'none', 'member', $2, 'agent', $3, 0,
		        (SELECT COALESCE(MAX(number), 0) + 1 FROM issue WHERE workspace_id = $1),
		        'autopilot', $4)
		RETURNING id::text
	`, testWorkspaceID, testUserID, agentID, ap.ID).Scan(&issueID)
	if err != nil {
		t.Fatalf("insert autopilot issue: %v", err)
	}

	// Link run to issue
	_, err = queries.UpdateAutopilotRunIssueCreated(ctx, db.UpdateAutopilotRunIssueCreatedParams{
		ID:      run.ID,
		IssueID: parseUUID(issueID),
	})
	if err != nil {
		t.Fatalf("UpdateAutopilotRunIssueCreated: %v", err)
	}

	// Manually set the run to the desired terminal status
	if runStatus == "failed" {
		_, err = queries.UpdateAutopilotRunFailed(ctx, db.UpdateAutopilotRunFailedParams{
			ID:            run.ID,
			FailureReason: pgtype.Text{String: failureReason, Valid: true},
		})
	} else if runStatus == "skipped" {
		_, err = testPool.Exec(ctx, `UPDATE autopilot_run SET status = 'skipped' WHERE id = $1`, run.ID)
	}
	if err != nil {
		t.Fatalf("set run status to %s: %v", runStatus, err)
	}

	return ap, issueID, run.ID
}
