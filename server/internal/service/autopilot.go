package service

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// TxStarter abstracts transaction creation (satisfied by pgxpool.Pool).
type TxStarter interface {
	Begin(ctx context.Context) (pgx.Tx, error)
}

type AutopilotService struct {
	Queries   *db.Queries
	TxStarter TxStarter
	Bus       *events.Bus
	TaskSvc   *TaskService
}

func NewAutopilotService(q *db.Queries, tx TxStarter, bus *events.Bus, taskSvc *TaskService) *AutopilotService {
	return &AutopilotService{Queries: q, TxStarter: tx, Bus: bus, TaskSvc: taskSvc}
}

// DispatchAutopilot is the core execution entry point.
// It creates a run and either creates an issue or enqueues a direct agent task
// depending on execution_mode.
func (s *AutopilotService) DispatchAutopilot(
	ctx context.Context,
	autopilot db.Autopilot,
	triggerID pgtype.UUID,
	source string,
	payload []byte,
) (*db.AutopilotRun, error) {
	// Determine initial status based on execution mode.
	initialStatus := "issue_created"
	if autopilot.ExecutionMode == "run_only" {
		initialStatus = "running"
	}

	run, err := s.Queries.CreateAutopilotRun(ctx, db.CreateAutopilotRunParams{
		AutopilotID:    autopilot.ID,
		TriggerID:      triggerID,
		Source:         source,
		Status:         initialStatus,
		TriggerPayload: payload,
	})
	if err != nil {
		return nil, fmt.Errorf("create run: %w", err)
	}

	switch autopilot.ExecutionMode {
	case "create_issue":
		if err := s.dispatchCreateIssue(ctx, autopilot, &run); err != nil {
			s.failRun(ctx, run.ID, err.Error())
			return &run, fmt.Errorf("dispatch create_issue: %w", err)
		}
	case "run_only":
		if err := s.dispatchRunOnly(ctx, autopilot, &run); err != nil {
			s.failRun(ctx, run.ID, err.Error())
			return &run, fmt.Errorf("dispatch run_only: %w", err)
		}
	default:
		s.failRun(ctx, run.ID, "unknown execution_mode: "+autopilot.ExecutionMode)
		return &run, fmt.Errorf("unknown execution_mode: %s", autopilot.ExecutionMode)
	}

	// Update last_run_at on the autopilot.
	s.Queries.UpdateAutopilotLastRunAt(ctx, autopilot.ID)

	// Publish run start event.
	s.Bus.Publish(events.Event{
		Type:        protocol.EventAutopilotRunStart,
		WorkspaceID: util.UUIDToString(autopilot.WorkspaceID),
		ActorType:   "system",
		Payload: map[string]any{
			"run_id":       util.UUIDToString(run.ID),
			"autopilot_id": util.UUIDToString(autopilot.ID),
			"source":       source,
			"status":       run.Status,
		},
	})

	return &run, nil
}

// dispatchCreateIssue creates an issue and enqueues a task for the agent.
func (s *AutopilotService) dispatchCreateIssue(ctx context.Context, ap db.Autopilot, run *db.AutopilotRun) error {
	tx, err := s.TxStarter.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx)

	qtx := s.Queries.WithTx(tx)

	// Get next issue number.
	issueNumber, err := qtx.IncrementIssueCounter(ctx, ap.WorkspaceID)
	if err != nil {
		return fmt.Errorf("increment issue counter: %w", err)
	}

	title := s.interpolateTemplate(ap)
	description := s.buildIssueDescription(ap)

	issue, err := qtx.CreateIssueWithOrigin(ctx, db.CreateIssueWithOriginParams{
		WorkspaceID:   ap.WorkspaceID,
		Title:         title,
		Description:   description,
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
		return fmt.Errorf("create issue: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}

	// Update run with the linked issue.
	updatedRun, err := s.Queries.UpdateAutopilotRunIssueCreated(ctx, db.UpdateAutopilotRunIssueCreatedParams{
		ID:      run.ID,
		IssueID: issue.ID,
	})
	if err != nil {
		return fmt.Errorf("link run to issue: %w", err)
	}
	*run = updatedRun

	// Publish issue:created so the existing event chain fires
	// (subscriber listeners, activity listeners, notification listeners).
	prefix := s.getIssuePrefix(ap.WorkspaceID)
	s.Bus.Publish(events.Event{
		Type:        protocol.EventIssueCreated,
		WorkspaceID: util.UUIDToString(ap.WorkspaceID),
		ActorType:   ap.CreatedByType,
		ActorID:     util.UUIDToString(ap.CreatedByID),
		Payload: map[string]any{
			"issue": issueToMap(issue, prefix),
		},
	})

	// Enqueue agent task via the existing flow.
	if _, err := s.TaskSvc.EnqueueTaskForIssue(ctx, issue); err != nil {
		return fmt.Errorf("enqueue task for issue: %w", err)
	}

	slog.Info("autopilot dispatched (create_issue)",
		"autopilot_id", util.UUIDToString(ap.ID),
		"issue_id", util.UUIDToString(issue.ID),
		"run_id", util.UUIDToString(run.ID),
	)
	return nil
}

// dispatchRunOnly enqueues a direct agent task without creating an issue.
func (s *AutopilotService) dispatchRunOnly(ctx context.Context, ap db.Autopilot, run *db.AutopilotRun) error {
	agent, err := s.Queries.GetAgent(ctx, ap.AssigneeID)
	if err != nil {
		return fmt.Errorf("load agent: %w", err)
	}
	if agent.ArchivedAt.Valid {
		return fmt.Errorf("agent is archived")
	}
	if !agent.RuntimeID.Valid {
		return fmt.Errorf("agent has no runtime")
	}

	task, err := s.Queries.CreateAutopilotTask(ctx, db.CreateAutopilotTaskParams{
		AgentID:        ap.AssigneeID,
		RuntimeID:      agent.RuntimeID,
		Priority:       0,
		AutopilotRunID: run.ID,
	})
	if err != nil {
		return fmt.Errorf("create autopilot task: %w", err)
	}

	// Update run with task reference.
	updatedRun, err := s.Queries.UpdateAutopilotRunRunning(ctx, db.UpdateAutopilotRunRunningParams{
		ID:     run.ID,
		TaskID: task.ID,
	})
	if err != nil {
		slog.Warn("failed to update run with task_id", "run_id", util.UUIDToString(run.ID), "error", err)
	} else {
		*run = updatedRun
	}

	slog.Info("autopilot dispatched (run_only)",
		"autopilot_id", util.UUIDToString(ap.ID),
		"task_id", util.UUIDToString(task.ID),
		"run_id", util.UUIDToString(run.ID),
	)
	return nil
}

// SyncRunFromIssue updates the autopilot run when its linked issue reaches a terminal status.
func (s *AutopilotService) SyncRunFromIssue(ctx context.Context, issue db.Issue) {
	if !issue.OriginType.Valid || issue.OriginType.String != "autopilot" {
		return
	}

	wsID := util.UUIDToString(issue.WorkspaceID)

	switch issue.Status {
	case "done", "in_review":
		run, err := s.Queries.GetAutopilotRunByIssue(ctx, issue.ID)
		if err != nil {
			return // no run linked to this issue (any status)
		}
		prevStatus := run.Status
		if _, err := s.Queries.UpdateAutopilotRunCompleted(ctx, db.UpdateAutopilotRunCompletedParams{
			ID: run.ID,
		}); err != nil {
			slog.Warn("failed to complete autopilot run", "run_id", util.UUIDToString(run.ID), "error", err)
			return
		}
		if prevStatus == "failed" {
			slog.Info("autopilot run recovered from failed status",
				"run_id", util.UUIDToString(run.ID),
				"previous_status", prevStatus,
			)
		}
		s.publishRunDone(wsID, run, "completed")
	case "cancelled", "blocked":
		run, err := s.Queries.GetActiveAutopilotRunByIssue(ctx, issue.ID)
		if err != nil {
			return // no active run linked to this issue
		}
		reason := "issue " + issue.Status
		if _, err := s.Queries.UpdateAutopilotRunFailed(ctx, db.UpdateAutopilotRunFailedParams{
			ID:            run.ID,
			FailureReason: pgtype.Text{String: reason, Valid: true},
		}); err != nil {
			slog.Warn("failed to fail autopilot run", "run_id", util.UUIDToString(run.ID), "error", err)
			return
		}
		s.publishRunDone(wsID, run, "failed")
	}
}

// SyncRunFromTask updates the autopilot run when a run_only task completes or fails.
func (s *AutopilotService) SyncRunFromTask(ctx context.Context, task db.AgentTaskQueue) {
	if !task.AutopilotRunID.Valid {
		return
	}

	run, err := s.Queries.GetAutopilotRun(ctx, task.AutopilotRunID)
	if err != nil {
		return
	}

	autopilot, err := s.Queries.GetAutopilot(ctx, run.AutopilotID)
	if err != nil {
		return
	}
	wsID := util.UUIDToString(autopilot.WorkspaceID)

	switch task.Status {
	case "completed":
		if _, err := s.Queries.UpdateAutopilotRunCompleted(ctx, db.UpdateAutopilotRunCompletedParams{
			ID:     run.ID,
			Result: task.Result,
		}); err != nil {
			slog.Warn("failed to complete autopilot run from task", "run_id", util.UUIDToString(run.ID), "error", err)
			return
		}
		s.publishRunDone(wsID, run, "completed")
	case "failed", "cancelled":
		reason := "task " + task.Status
		if task.Error.Valid {
			reason = task.Error.String
		}
		if _, err := s.Queries.UpdateAutopilotRunFailed(ctx, db.UpdateAutopilotRunFailedParams{
			ID:            run.ID,
			FailureReason: pgtype.Text{String: reason, Valid: true},
		}); err != nil {
			slog.Warn("failed to fail autopilot run from task", "run_id", util.UUIDToString(run.ID), "error", err)
			return
		}
		s.publishRunDone(wsID, run, "failed")
	}
}


func (s *AutopilotService) failRun(ctx context.Context, runID pgtype.UUID, reason string) {
	if _, err := s.Queries.UpdateAutopilotRunFailed(ctx, db.UpdateAutopilotRunFailedParams{
		ID:            runID,
		FailureReason: pgtype.Text{String: reason, Valid: true},
	}); err != nil {
		slog.Warn("failed to mark autopilot run as failed", "run_id", util.UUIDToString(runID), "error", err)
	}
}

func (s *AutopilotService) publishRunDone(workspaceID string, run db.AutopilotRun, status string) {
	s.Bus.Publish(events.Event{
		Type:        protocol.EventAutopilotRunDone,
		WorkspaceID: workspaceID,
		ActorType:   "system",
		Payload: map[string]any{
			"run_id":       util.UUIDToString(run.ID),
			"autopilot_id": util.UUIDToString(run.AutopilotID),
			"status":       status,
		},
	})
}

// buildIssueDescription appends an autopilot system instruction to the
// user-provided description, asking the agent to rename the issue after
// it understands the actual work.
func (s *AutopilotService) buildIssueDescription(ap db.Autopilot) pgtype.Text {
	now := time.Now().UTC().Format("2006-01-02 15:04 UTC")
	note := fmt.Sprintf("\n\n---\n*Autopilot run triggered at %s. After starting work, rename this issue to accurately reflect what you are doing.*", now)
	base := ap.Description.String
	return pgtype.Text{String: base + note, Valid: true}
}

// interpolateTemplate replaces {{date}} in the issue title template.
func (s *AutopilotService) interpolateTemplate(ap db.Autopilot) string {
	tmpl := ap.Title
	if ap.IssueTitleTemplate.Valid && ap.IssueTitleTemplate.String != "" {
		tmpl = ap.IssueTitleTemplate.String
	}
	now := time.Now().UTC().Format("2006-01-02")
	return strings.ReplaceAll(tmpl, "{{date}}", now)
}

func (s *AutopilotService) getIssuePrefix(workspaceID pgtype.UUID) string {
	ws, err := s.Queries.GetWorkspace(context.Background(), workspaceID)
	if err != nil {
		return ""
	}
	return ws.IssuePrefix
}

// =============================================================================
// Stream Disconnect Reconciliation
// =============================================================================

// matchStreamDisconnected checks whether a comment content matches the
// specific stream-disconnected terminal failure signature from the agent
// harness. The matching requires BOTH the "stream disconnected" marker and
// the "codex/responses" endpoint URL to avoid misclassifying generic
// network errors as terminal stream disconnects.
func matchStreamDisconnected(content string) bool {
	lower := strings.ToLower(content)
	return strings.Contains(lower, "stream disconnected before completion") &&
		strings.Contains(lower, "codex/responses")
}

// HandleStreamDisconnectedComment checks whether a newly created system
// comment on an issue signals a terminal stream disconnect for the linked
// autopilot run, and if so fails the run and creates a compensation retry.
//
// The commentType parameter is the comment's Type field ("system" for task
// failure comments), not the author_type (which is "agent" for system comments).
//
// Returns the run (if any) that was failed, or nil.
func (s *AutopilotService) HandleStreamDisconnectedComment(ctx context.Context, issueID pgtype.UUID, commentContent, commentType string) (*db.AutopilotRun, error) {
	if commentType != "system" {
		return nil, nil
	}
	if !matchStreamDisconnected(commentContent) {
		return nil, nil
	}

	runs, err := s.Queries.GetAutopilotRunByIssueAndStatus(ctx, issueID)
	if err != nil {
		return nil, fmt.Errorf("lookup run by issue: %w", err)
	}
	if len(runs) == 0 {
		return nil, nil
	}

	run := runs[0]
	if run.Status != "issue_created" && run.Status != "running" {
		return nil, nil
	}

	issue, err := s.Queries.GetIssue(ctx, issueID)
	if err != nil {
		return nil, fmt.Errorf("load issue: %w", err)
	}
	if issue.Status == "done" || issue.Status == "cancelled" || issue.Status == "blocked" || issue.Status == "in_review" {
		slog.Info("stream disconnect: issue already in terminal state, skipping run failure",
			"run_id", util.UUIDToString(run.ID),
			"issue_id", util.UUIDToString(issueID),
			"issue_status", issue.Status,
		)
		return nil, nil
	}

	autopilot, err := s.Queries.GetAutopilot(ctx, run.AutopilotID)
	if err != nil {
		return nil, fmt.Errorf("load autopilot: %w", err)
	}

	wsID := util.UUIDToString(autopilot.WorkspaceID)
	slog.Info("stream disconnect detected: failing autopilot run",
		"run_id", util.UUIDToString(run.ID),
		"autopilot_id", util.UUIDToString(autopilot.ID),
		"issue_id", util.UUIDToString(issueID),
	)

	if _, err := s.Queries.UpdateAutopilotRunFailed(ctx, db.UpdateAutopilotRunFailedParams{
		ID:            run.ID,
		FailureReason: pgtype.Text{String: "stream_disconnected", Valid: true},
	}); err != nil {
		return &run, fmt.Errorf("fail run: %w", err)
	}

	s.publishRunDone(wsID, run, "failed")

	if err := s.CreateCompensationRun(ctx, autopilot, run); err != nil {
		slog.Warn("compensation retry failed",
			"run_id", util.UUIDToString(run.ID),
			"error", err,
		)
	}

	return &run, nil
}

// CreateCompensationRun creates exactly one compensation retry for a
// stream_disconnected terminal failure. The retry is deduped by a
// DB-level unique constraint on compensation_key, with an application-level
// existence check as a fast path. Compensation runs themselves are not
// retried to avoid recursive cascades.
func (s *AutopilotService) CreateCompensationRun(ctx context.Context, autopilot db.Autopilot, originalRun db.AutopilotRun) error {
	if originalRun.IsCompensation || originalRun.RetryOf.Valid {
		slog.Info("refusing to create compensation retry for a compensation run",
			"run_id", util.UUIDToString(originalRun.ID),
		)
		return nil
	}

	alreadyExists, err := s.Queries.CheckCompensationRetryExists(ctx, originalRun.ID)
	if err != nil {
		return fmt.Errorf("check existing retry: %w", err)
	}
	if alreadyExists {
		slog.Info("compensation retry already exists, skipping",
			"original_run_id", util.UUIDToString(originalRun.ID),
		)
		return nil
	}

	// Pre-retry guard: inspect the linked issue for partial work before
	// dispatching. If any meaningful agent comment (non-system) exists, or
	// the issue has been manually moved to a terminal status, skip the retry.
	if originalRun.IssueID.Valid {
		if skip, reason := s.shouldSkipCompensationRetry(ctx, originalRun.IssueID); skip {
			slog.Info("skipping compensation retry: partial work detected",
				"run_id", util.UUIDToString(originalRun.ID),
				"issue_id", util.UUIDToString(originalRun.IssueID),
				"reason", reason,
			)
			return nil
		}
	}

	compensationKey := fmt.Sprintf("stream_disconnected:%s", util.UUIDToString(originalRun.ID))

	compRun, err := s.Queries.CreateCompensationRun(ctx, db.CreateCompensationRunParams{
		AutopilotID:     autopilot.ID,
		TriggerID:       originalRun.TriggerID,
		RetryOf:         originalRun.ID,
		CompensationKey: pgtype.Text{String: compensationKey, Valid: true},
	})
	if err != nil {
		return fmt.Errorf("create compensation run: %w", err)
	}

	wsID := util.UUIDToString(autopilot.WorkspaceID)

	slog.Info("compensation retry run created",
		"compensation_run_id", util.UUIDToString(compRun.ID),
		"original_run_id", util.UUIDToString(originalRun.ID),
		"autopilot_id", util.UUIDToString(autopilot.ID),
		"compensation_key", compensationKey,
	)

	if err := s.dispatchCreateIssue(ctx, autopilot, &compRun); err != nil {
		s.failRun(ctx, compRun.ID, fmt.Sprintf("compensation dispatch failed: %v", err))
		return fmt.Errorf("dispatch compensation run: %w", err)
	}

	s.Queries.UpdateAutopilotLastRunAt(ctx, autopilot.ID)

	s.Bus.Publish(events.Event{
		Type:        protocol.EventAutopilotRunStart,
		WorkspaceID: wsID,
		ActorType:   "system",
		Payload: map[string]any{
			"run_id":          util.UUIDToString(compRun.ID),
			"autopilot_id":    util.UUIDToString(autopilot.ID),
			"source":          "compensation_retry",
			"status":          compRun.Status,
			"retry_of":        util.UUIDToString(originalRun.ID),
			"is_compensation": true,
		},
	})

	return nil
}

// shouldSkipCompensationRetry inspects the linked issue to decide whether
// a compensation retry should be skipped because the failed run produced
// partial work. Returns (skip, reason).
func (s *AutopilotService) shouldSkipCompensationRetry(ctx context.Context, issueID pgtype.UUID) (bool, string) {
	issue, err := s.Queries.GetIssue(ctx, issueID)
	if err != nil {
		return true, "failed to load issue"
	}

	if issue.Status == "done" || issue.Status == "cancelled" || issue.Status == "blocked" || issue.Status == "in_review" {
		return true, "issue already in terminal status: " + issue.Status
	}

	comments, err := s.Queries.ListComments(ctx, db.ListCommentsParams{
		IssueID:     issueID,
		WorkspaceID: issue.WorkspaceID,
	})
	if err != nil {
		return true, "failed to load comments"
	}

	agentCommentCount := 0
	for _, c := range comments {
		if c.Type != "system" {
			agentCommentCount++
		}
	}

	if agentCommentCount > 0 {
		return true, fmt.Sprintf("issue has %d non-system comments, indicating partial work", agentCommentCount)
	}

	return false, ""
}

// ReconcileStuckRuns scans for create_issue autopilot runs that have been
// stuck in issue_created for longer than the given threshold and have a
// stream_disconnected system comment on their linked issue. It fails those
// runs and creates compensation retries.
//
// This is the background safety net for cases where the event-driven path
// (HandleStreamDisconnectedComment) was missed due to a restart or transient
// failure.
func (s *AutopilotService) ReconcileStuckRuns(ctx context.Context, stuckThreshold time.Duration, limit int32) (reconciled int, retryFailed int, failed int) {
	runs, err := s.Queries.ListStuckIssueCreatedRuns(ctx, db.ListStuckIssueCreatedRunsParams{
		StuckInterval: pgtype.Interval{Microseconds: stuckThreshold.Microseconds(), Valid: true},
		Limit:          limit,
	})
	if err != nil {
		slog.Warn("stream disconnect reconciler: failed to list stuck runs", "error", err)
		return 0, 0, 0
	}

	for _, run := range runs {
		if !run.IssueID.Valid {
			continue
		}

		autopilot, err := s.Queries.GetAutopilot(ctx, run.AutopilotID)
		if err != nil {
			failed++
			continue
		}

		issue, err := s.Queries.GetIssue(ctx, run.IssueID)
		if err != nil {
			failed++
			continue
		}

		if issue.Status == "blocked" || issue.Status == "done" || issue.Status == "cancelled" || issue.Status == "in_review" {
			continue
		}

		comments, err := s.Queries.ListComments(ctx, db.ListCommentsParams{
			IssueID:     run.IssueID,
			WorkspaceID: autopilot.WorkspaceID,
		})
		if err != nil {
			failed++
			continue
		}

		disconnected := false
		for _, c := range comments {
			if c.Type == "system" && matchStreamDisconnected(c.Content) {
				disconnected = true
				break
			}
		}
		if !disconnected {
			continue
		}

		wsID := util.UUIDToString(autopilot.WorkspaceID)
		slog.Info("stream disconnect reconciler: failing stuck run",
			"run_id", util.UUIDToString(run.ID),
			"autopilot_id", util.UUIDToString(autopilot.ID),
			"issue_id", util.UUIDToString(run.IssueID),
		)

		if _, err := s.Queries.UpdateAutopilotRunFailed(ctx, db.UpdateAutopilotRunFailedParams{
			ID:            run.ID,
			FailureReason: pgtype.Text{String: "stream_disconnected", Valid: true},
		}); err != nil {
			failed++
			slog.Warn("stream disconnect reconciler: failed to fail run", "run_id", util.UUIDToString(run.ID), "error", err)
			continue
		}

		s.publishRunDone(wsID, run, "failed")

		if err := s.CreateCompensationRun(ctx, autopilot, run); err != nil {
			retryFailed++
			slog.Warn("stream disconnect reconciler: compensation retry failed",
				"run_id", util.UUIDToString(run.ID),
				"error", err,
			)
		}

		reconciled++
	}

	return reconciled, retryFailed, failed
}
