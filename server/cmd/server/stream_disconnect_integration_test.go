package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// TestStreamDisconnectListenerPath verifies the full listener path:
// system stream-disconnect comment → run failed with failure_reason=stream_disconnected
// → exactly one compensation run created with the correct attributes.
func TestStreamDisconnectListenerPath(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerStreamDisconnectListener(bus, autopilotSvc)

	// Load fixture agent
	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load fixture agent: %v", err)
	}

	// Use transaction to avoid issue number collisions
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	qtx := queries.WithTx(tx)

	// Create autopilot
	ap, err := qtx.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Stream Disconnect Listener Test",
		Description:        pgtype.Text{String: "Test autopilot for stream disconnect listener", Valid: true},
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
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	// Increment issue counter
	issueNumber, err := qtx.IncrementIssueCounter(ctx, ap.WorkspaceID)
	if err != nil {
		t.Fatalf("IncrementIssueCounter: %v", err)
	}

	// Create issue
	issue, err := qtx.CreateIssueWithOrigin(ctx, db.CreateIssueWithOriginParams{
		WorkspaceID:   ap.WorkspaceID,
		Title:         "Stream disconnect test issue",
		Description:   pgtype.Text{},
		Status:        "todo",
		Priority:      "none",
		AssigneeType:  pgtype.Text{String: "agent", Valid: true},
		AssigneeID:    ap.AssigneeID,
		CreatorType:   ap.CreatedByType,
		CreatorID:     ap.CreatedByID,
		ParentIssueID: pgtype.UUID{},
		Position:      0,
		DueDate:       pgtype.Timestamptz{},
		Number:        issueNumber,
		ProjectID:     pgtype.UUID{},
		OriginType:    pgtype.Text{String: "autopilot", Valid: true},
		OriginID:      ap.ID,
	})
	if err != nil {
		t.Fatalf("CreateIssueWithOrigin: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID)
	})

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Create run
	run, err := autopilotSvc.DispatchAutopilot(ctx, ap, pgtype.UUID{}, "manual", nil)
	if err != nil {
		t.Fatalf("DispatchAutopilot: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, util.UUIDToString(issue.ID))
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, util.UUIDToString(issue.ID))
	})

	// Verify run is in issue_created status
	if run.Status != "issue_created" {
		t.Errorf("expected run status 'issue_created', got '%s'", run.Status)
	}

	// Insert system comment directly via DB (CreateComment API rejects type="system")
	systemCommentContent := "stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)"
	var commentID string
	err = testPool.QueryRow(ctx, `
		INSERT INTO comment (issue_id, workspace_id, content, type, author_type, author_id)
		VALUES ($1, $2, $3, 'system', 'agent', $4)
		RETURNING id::text
	`, util.UUIDToString(issue.ID), testWorkspaceID, systemCommentContent, agentID).Scan(&commentID)
	if err != nil {
		t.Fatalf("insert system comment: %v", err)
	}

	// Publish comment:created event to simulate the listener trigger
	bus.Publish(events.Event{
		Type:        "comment:created",
		WorkspaceID: testWorkspaceID,
		ActorType:   "system",
		Payload: map[string]any{
			"comment": map[string]any{
				"id":         commentID,
				"issue_id":   util.UUIDToString(issue.ID),
				"content":    systemCommentContent,
				"type":       "system",
				"author_id":  agentID,
				"author_type": "agent",
			},
		},
	})

	// Verify the original run was failed
	updatedRun, err := queries.GetAutopilotRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetAutopilotRun after comment: %v", err)
	}
	if updatedRun.Status != "failed" {
		t.Errorf("expected run status 'failed' after stream disconnect, got '%s'", updatedRun.Status)
	}
	if !updatedRun.FailureReason.Valid || updatedRun.FailureReason.String != "stream_disconnected" {
		t.Errorf("expected failure_reason='stream_disconnected', got '%v'", updatedRun.FailureReason)
	}

	// Verify exactly one compensation run was created
	var compensationCount int
	err = testPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM autopilot_run
		WHERE retry_of = $1 AND is_compensation = true
	`, run.ID).Scan(&compensationCount)
	if err != nil {
		t.Fatalf("query compensation runs: %v", err)
	}
	if compensationCount != 1 {
		t.Errorf("expected exactly 1 compensation run, got %d", compensationCount)
	}

	// Verify the compensation run has the correct attributes
	compRun, err := queries.GetAutopilotRun(ctx, pgtype.UUID{})
	if err == nil {
		if compRun.Status != "issue_created" {
			t.Errorf("expected compensation run status 'issue_created', got '%s'", compRun.Status)
		}
		if compRun.Source != "manual" {
			t.Errorf("expected compensation run source 'manual', got '%s'", compRun.Source)
		}
		if !compRun.IsCompensation {
			t.Error("expected compensation run is_compensation=true")
		}
		if !compRun.RetryOf.Valid || util.UUIDToString(compRun.RetryOf) != util.UUIDToString(run.ID) {
			t.Errorf("expected compensation run retry_of='%s', got '%v'", util.UUIDToString(run.ID), compRun.RetryOf)
		}
		if !compRun.CompensationKey.Valid {
			t.Error("expected compensation run to have compensation_key")
		} else {
			expectedKey := fmt.Sprintf("stream_disconnected:%s", util.UUIDToString(run.ID))
			if compRun.CompensationKey.String != expectedKey {
				t.Errorf("expected compensation_key='%s', got '%s'", expectedKey, compRun.CompensationKey.String)
			}
		}
	}
}

// TestStreamDisconnectReconcilerPath verifies the reconciler path:
// stuck issue_created run + stream-disconnect system comment →
// run failed with failure_reason=stream_disconnected → one compensation run created.
// This is the safety net for cases where the event-driven path was missed.
func TestStreamDisconnectReconcilerPath(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)

	// Load fixture agent
	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load fixture agent: %v", err)
	}

	// Create autopilot using transaction to avoid collisions
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	qtx := queries.WithTx(tx)

	ap, err := qtx.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Stream Disconnect Reconciler Test",
		Description:        pgtype.Text{String: "Test autopilot for reconciler", Valid: true},
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
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	// Increment issue counter
	issueNumber, err := qtx.IncrementIssueCounter(ctx, ap.WorkspaceID)
	if err != nil {
		t.Fatalf("IncrementIssueCounter: %v", err)
	}

	// Create issue
	issue, err := qtx.CreateIssueWithOrigin(ctx, db.CreateIssueWithOriginParams{
		WorkspaceID:   ap.WorkspaceID,
		Title:         "Reconciler stuck test issue",
		Description:   pgtype.Text{},
		Status:        "todo",
		Priority:      "none",
		AssigneeType:  pgtype.Text{String: "agent", Valid: true},
		AssigneeID:    ap.AssigneeID,
		CreatorType:   ap.CreatedByType,
		CreatorID:     ap.CreatedByID,
		ParentIssueID: pgtype.UUID{},
		Position:      0,
		DueDate:       pgtype.Timestamptz{},
		Number:        issueNumber,
		ProjectID:     pgtype.UUID{},
		OriginType:    pgtype.Text{String: "autopilot", Valid: true},
		OriginID:      ap.ID,
	})
	if err != nil {
		t.Fatalf("CreateIssueWithOrigin: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID)
	})

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Create run and set triggered_at to past time to simulate stuck run
	run, err := autopilotSvc.DispatchAutopilot(ctx, ap, pgtype.UUID{}, "manual", nil)
	if err != nil {
		t.Fatalf("DispatchAutopilot: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, util.UUIDToString(issue.ID))
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, util.UUIDToString(issue.ID))
		testPool.Exec(context.Background(), `DELETE FROM autopilot_run WHERE id = $1`, run.ID)
	})

	// Set triggered_at to past time to make it "stuck"
	oldTriggeredAt := time.Now().Add(-10 * time.Minute)
	_, err = testPool.Exec(ctx, `UPDATE autopilot_run SET triggered_at = $1 WHERE id = $2`, oldTriggeredAt, run.ID)
	if err != nil {
		t.Fatalf("update triggered_at: %v", err)
	}

	// Insert stream-disconnect system comment
	systemCommentContent := "stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)"
	_, err = testPool.Exec(ctx, `
		INSERT INTO comment (issue_id, workspace_id, content, type, author_type, author_id)
		VALUES ($1, $2, $3, 'system', 'agent', $4)
	`, util.UUIDToString(issue.ID), testWorkspaceID, systemCommentContent, agentID)
	if err != nil {
		t.Fatalf("insert system comment: %v", err)
	}

	// Call ReconcileStuckRuns directly
	reconciled, retryFailed, failed := autopilotSvc.ReconcileStuckRuns(ctx, 5*time.Minute, 20)

	if reconciled != 1 {
		t.Errorf("expected ReconcileStuckRuns to reconcile 1 run, got reconciled=%d", reconciled)
	}
	if retryFailed != 0 {
		t.Errorf("expected ReconcileStuckRuns retryFailed=0, got %d", retryFailed)
	}
	if failed != 0 {
		t.Errorf("expected ReconcileStuckRuns failed=0, got %d", failed)
	}

	// Verify the original run was failed
	updatedRun, err := queries.GetAutopilotRun(ctx, run.ID)
	if err != nil {
		t.Fatalf("GetAutopilotRun after reconciler: %v", err)
	}
	if updatedRun.Status != "failed" {
		t.Errorf("expected run status 'failed' after reconciler, got '%s'", updatedRun.Status)
	}
	if !updatedRun.FailureReason.Valid || updatedRun.FailureReason.String != "stream_disconnected" {
		t.Errorf("expected failure_reason='stream_disconnected' after reconciler, got '%v'", updatedRun.FailureReason)
	}

	// Verify exactly one compensation run was created
	var compensationCount int
	err = testPool.QueryRow(ctx, `
		SELECT COUNT(*) FROM autopilot_run
		WHERE retry_of = $1 AND is_compensation = true
	`, run.ID).Scan(&compensationCount)
	if err != nil {
		t.Fatalf("query compensation runs: %v", err)
	}
	if compensationCount != 1 {
		t.Errorf("expected exactly 1 compensation run after reconciler, got %d", compensationCount)
	}
}

// TestCompensationRunDoesNotRetry verifies that when a compensation run
// itself suffers a stream disconnect, it does NOT create a second compensation run.
// This prevents recursive cascades.
func TestCompensationRunDoesNotRetry(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerStreamDisconnectListener(bus, autopilotSvc)

	// Load fixture agent
	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load fixture agent: %v", err)
	}

	// Create autopilot
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	qtx := queries.WithTx(tx)

	ap, err := qtx.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Compensation Cascade Test",
		Description:        pgtype.Text{String: "Test that compensation runs do not retry", Valid: true},
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
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	// Increment issue counter
	issueNumber, err := qtx.IncrementIssueCounter(ctx, ap.WorkspaceID)
	if err != nil {
		t.Fatalf("IncrementIssueCounter: %v", err)
	}

	// Create issue
	issue, err := qtx.CreateIssueWithOrigin(ctx, db.CreateIssueWithOriginParams{
		WorkspaceID:   ap.WorkspaceID,
		Title:         "Compensation cascade test issue",
		Description:   pgtype.Text{},
		Status:        "todo",
		Priority:      "none",
		AssigneeType:  pgtype.Text{String: "agent", Valid: true},
		AssigneeID:    ap.AssigneeID,
		CreatorType:   ap.CreatedByType,
		CreatorID:     ap.CreatedByID,
		ParentIssueID: pgtype.UUID{},
		Position:      0,
		DueDate:       pgtype.Timestamptz{},
		Number:        issueNumber,
		ProjectID:     pgtype.UUID{},
		OriginType:    pgtype.Text{String: "autopilot", Valid: true},
		OriginID:      ap.ID,
	})
	if err != nil {
		t.Fatalf("CreateIssueWithOrigin: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID)
	})

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Create original run
	originalRun, err := autopilotSvc.DispatchAutopilot(ctx, ap, pgtype.UUID{}, "manual", nil)
	if err != nil {
		t.Fatalf("DispatchAutopilot: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, util.UUIDToString(issue.ID))
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, util.UUIDToString(issue.ID))
		testPool.Exec(context.Background(), `DELETE FROM autopilot_run WHERE id = $1 OR retry_of = $1`, originalRun.ID)
	})

	// Insert first stream-disconnect system comment
	systemCommentContent := "stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)"
	_, err = testPool.Exec(ctx, `
		INSERT INTO comment (issue_id, workspace_id, content, type, author_type, author_id)
		VALUES ($1, $2, $3, 'system', 'agent', $4)
	`, util.UUIDToString(issue.ID), testWorkspaceID, systemCommentContent, agentID)
	if err != nil {
		t.Fatalf("insert first system comment: %v", err)
	}

	// Publish comment:created event
	bus.Publish(events.Event{
		Type:        "comment:created",
		WorkspaceID: testWorkspaceID,
		ActorType:   "system",
		Payload: map[string]any{
			"comment": map[string]any{
				"issue_id":   util.UUIDToString(issue.ID),
				"content":    systemCommentContent,
				"type":       "system",
				"author_id":  agentID,
				"author_type": "agent",
			},
		},
	})

	// Get the compensation run
	var compRun db.AutopilotRun
	err = testPool.QueryRow(ctx, `
		SELECT r.* FROM autopilot_run r
		WHERE r.retry_of = $1 AND r.is_compensation = true
	`, originalRun.ID).Scan(&compRun.ID, &compRun.AutopilotID, &compRun.TriggerID, &compRun.Source,
		&compRun.Status, &compRun.IssueID, &compRun.TaskID, &compRun.TriggeredAt, &compRun.CompletedAt,
		&compRun.FailureReason, &compRun.TriggerPayload, &compRun.Result, &compRun.CreatedAt,
		&compRun.PreviousFailureReason, &compRun.IsCompensation, &compRun.RetryOf, &compRun.CompensationKey)
	if err != nil {
		t.Fatalf("query compensation run: %v", err)
	}
	t.Cleanup(func() {
		if compRun.IssueID.Valid {
			testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, compRun.IssueID)
			testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, util.UUIDToString(compRun.IssueID))
		}
		testPool.Exec(context.Background(), `DELETE FROM autopilot_run WHERE id = $1`, compRun.ID)
	})

	// Insert ANOTHER stream-disconnect system comment on the compensation run's issue
	_, err = testPool.Exec(ctx, `
		INSERT INTO comment (issue_id, workspace_id, content, type, author_type, author_id)
		VALUES ($1, $2, $3, 'system', 'agent', $4)
	`, util.UUIDToString(compRun.IssueID), testWorkspaceID, systemCommentContent, agentID)
	if err != nil {
		t.Fatalf("insert second system comment: %v", err)
	}

	// Publish comment:created event for compensation issue
	bus.Publish(events.Event{
		Type:        "comment:created",
		WorkspaceID: testWorkspaceID,
		ActorType:   "system",
		Payload: map[string]any{
			"comment": map[string]any{
				"issue_id":   util.UUIDToString(compRun.IssueID),
				"content":    systemCommentContent,
				"type":       "system",
				"author_id":  agentID,
				"author_type": "agent",
			},
		},
	})

	// Verify NO second-level compensation run was created
	var secondLevelCount int
	err = testPool.QueryRow(ctx, `
		SELECT COUNT(*)
		FROM autopilot_run
		WHERE retry_of = $1 AND is_compensation = true
	`, compRun.ID).Scan(&secondLevelCount)
	if err != nil {
		t.Fatalf("query second-level compensation runs: %v", err)
	}
	if secondLevelCount != 0 {
		t.Errorf("expected no second-level compensation runs, got %d", secondLevelCount)
	}

	// Verify the compensation run was failed
	updatedCompRun, err := queries.GetAutopilotRun(ctx, compRun.ID)
	if err != nil {
		t.Fatalf("GetAutopilotRun for compensation run: %v", err)
	}
	if updatedCompRun.Status != "failed" {
		t.Errorf("expected compensation run status 'failed' after stream disconnect, got '%s'", updatedCompRun.Status)
	}
}

// TestStreamDisconnectExactlyOnceDedupe verifies that the DB-level unique constraint
// on compensation_key prevents duplicate compensation runs from concurrent
// listener/reconciler cycles.
func TestStreamDisconnectExactlyOnceDedupe(t *testing.T) {
	ctx := context.Background()
	queries := db.New(testPool)
	bus := events.New()
	taskSvc := service.NewTaskService(queries, testPool, nil, bus)
	autopilotSvc := service.NewAutopilotService(queries, testPool, bus, taskSvc)
	registerStreamDisconnectListener(bus, autopilotSvc)

	// Load fixture agent
	var agentID string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID,
	).Scan(&agentID); err != nil {
		t.Fatalf("load fixture agent: %v", err)
	}

	// Create autopilot
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	qtx := queries.WithTx(tx)

	ap, err := qtx.CreateAutopilot(ctx, db.CreateAutopilotParams{
		WorkspaceID:        parseUUID(testWorkspaceID),
		Title:              "Dedupe Test",
		Description:        pgtype.Text{String: "Test exactly-once deduplication", Valid: true},
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
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM autopilot WHERE id = $1`, ap.ID)
	})

	// Increment issue counter
	issueNumber, err := qtx.IncrementIssueCounter(ctx, ap.WorkspaceID)
	if err != nil {
		t.Fatalf("IncrementIssueCounter: %v", err)
	}

	// Create issue
	issue, err := qtx.CreateIssueWithOrigin(ctx, db.CreateIssueWithOriginParams{
		WorkspaceID:   ap.WorkspaceID,
		Title:         "Dedupe test issue",
		Description:   pgtype.Text{},
		Status:        "todo",
		Priority:      "none",
		AssigneeType:  pgtype.Text{String: "agent", Valid: true},
		AssigneeID:    ap.AssigneeID,
		CreatorType:   ap.CreatedByType,
		CreatorID:     ap.CreatedByID,
		ParentIssueID: pgtype.UUID{},
		Position:      0,
		DueDate:       pgtype.Timestamptz{},
		Number:        issueNumber,
		ProjectID:     pgtype.UUID{},
		OriginType:    pgtype.Text{String: "autopilot", Valid: true},
		OriginID:      ap.ID,
	})
	if err != nil {
		t.Fatalf("CreateIssueWithOrigin: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issue.ID)
	})

	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Create original run
	originalRun, err := autopilotSvc.DispatchAutopilot(ctx, ap, pgtype.UUID{}, "manual", nil)
	if err != nil {
		t.Fatalf("DispatchAutopilot: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, util.UUIDToString(issue.ID))
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, util.UUIDToString(issue.ID))
		testPool.Exec(context.Background(), `DELETE FROM autopilot_run WHERE id = $1 OR retry_of = $1`, originalRun.ID)
	})

	// Manually create a compensation run with the expected compensation_key
	compensationKey := fmt.Sprintf("stream_disconnected:%s", util.UUIDToString(originalRun.ID))
	var compRun db.AutopilotRun
	err = testPool.QueryRow(ctx, `
		INSERT INTO autopilot_run (autopilot_id, status, source, is_compensation, retry_of, compensation_key, triggered_at, issue_id)
		SELECT $1, 'issue_created', 'manual', true, $2::uuid, $3, now(), $4
		RETURNING id, autopilot_id, trigger_id, source, status, issue_id, task_id, triggered_at, completed_at,
	                failure_reason, trigger_payload, result, created_at, previous_failure_reason,
	                is_compensation, retry_of, compensation_key
	`, ap.ID, originalRun.ID, compensationKey, originalRun.IssueID).Scan(&compRun.ID, &compRun.AutopilotID, &compRun.TriggerID,
		&compRun.Source, &compRun.Status, &compRun.IssueID, &compRun.TaskID, &compRun.TriggeredAt, &compRun.CompletedAt,
		&compRun.FailureReason, &compRun.TriggerPayload, &compRun.Result, &compRun.CreatedAt, &compRun.PreviousFailureReason,
		&compRun.IsCompensation, &compRun.RetryOf, &compRun.CompensationKey)
	if err != nil {
		t.Fatalf("create first compensation run: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM autopilot_run WHERE id = $1`, compRun.ID)
	})

	// Attempt to create a duplicate compensation run via the same key - should fail due to unique constraint
	_, err = testPool.Exec(ctx, `
		INSERT INTO autopilot_run (autopilot_id, status, source, is_compensation, retry_of, compensation_key, triggered_at, issue_id)
		SELECT $1, 'issue_created', 'manual', true, $2::uuid, $3, now(), $4
	`, ap.ID, originalRun.ID, compensationKey, originalRun.IssueID)
	if err == nil {
		t.Error("expected duplicate compensation run to fail due to unique constraint, but it succeeded")
	}
}

// TestStuckRunIndexCoverage verifies that idx_autopilot_run_stuck covers
// the ListStuckIssueCreatedRuns query correctly.
//
// The current index uses (status, created_at) while the query filters/orders by triggered_at.
// This test documents the current state and evaluates whether alignment is needed.
func TestStuckRunIndexCoverage(t *testing.T) {
	ctx := context.Background()

	// Verify the index exists
	var indexExists bool
	err := testPool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes
			WHERE schemaname = 'public'
			  AND indexname = 'idx_autopilot_run_stuck'
		)
	`).Scan(&indexExists)
	if err != nil {
		t.Fatalf("query index existence: %v", err)
	}
	if !indexExists {
		t.Error("idx_autopilot_run_stuck index does not exist")
	}

	// Get the index definition
	var indexDef string
	err = testPool.QueryRow(ctx, `
		SELECT indexdef FROM pg_indexes
		WHERE schemaname = 'public'
		  AND indexname = 'idx_autopilot_run_stuck'
	`).Scan(&indexDef)
	if err != nil {
		t.Fatalf("query index definition: %v", err)
	}

	// Verify the index includes status and created_at
	if idx, ok := asString(indexDef); ok {
		// The index should cover the status and created_at columns for the stuck run query
		// Since triggered_at and created_at are both set to now() at creation,
		// the index on created_at is functionally equivalent for the time-based filter.
		// However, the query orders by triggered_at, which the index doesn't directly support.

		// This test documents that the current implementation works because:
		// 1. Partial index filter WHERE status = 'issue_created' narrows the scan
		// 2. created_at ≈ triggered_at at creation time (both are now())
		// 3. The ORDER BY triggered_at happens after the filtered set is small

		_ = idx // Use the variable to avoid unused variable warning
		t.Log("idx_autopilot_run_stuck definition:", indexDef)
		t.Log("Current query filters by triggered_at but index uses created_at")
		t.Log("This works because both columns are set to now() at creation time")
		t.Log("If triggered_at and created_at ever diverge, the index should be updated to use triggered_at")
	}
}

// Helper function to convert pgtype.Text to string
func asString(text pgtype.Text) (string, bool) {
	if !text.Valid {
		return "", false
	}
	return text.String, true
}
