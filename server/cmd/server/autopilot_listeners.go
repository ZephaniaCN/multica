package main

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/handler"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// registerAutopilotListeners hooks into issue and task events to keep
// autopilot runs in sync with their linked issues and tasks.
func registerAutopilotListeners(bus *events.Bus, svc *service.AutopilotService) {
	ctx := context.Background()

	// When an issue with origin_type='autopilot' reaches a terminal status,
	// update the corresponding autopilot run.
	bus.Subscribe(protocol.EventIssueUpdated, func(e events.Event) {
		payload, ok := e.Payload.(map[string]any)
		if !ok {
			return
		}
		statusChanged, _ := payload["status_changed"].(bool)
		if !statusChanged {
			return
		}
		issue, ok := payload["issue"].(handler.IssueResponse)
		if !ok {
			return
		}
		// Only handle statuses that finalize an autopilot run.
		if issue.Status != "done" && issue.Status != "in_review" && issue.Status != "cancelled" && issue.Status != "blocked" {
			return
		}
		// Load the full issue from DB to check origin_type.
		dbIssue, err := svc.Queries.GetIssue(ctx, parseUUID(issue.ID))
		if err != nil {
			slog.Debug("autopilot listener: failed to load issue", "issue_id", issue.ID, "error", err)
			return
		}
		svc.SyncRunFromIssue(ctx, dbIssue)
	})

	// When a task completes or fails, check if it's an autopilot run_only task.
	bus.Subscribe(protocol.EventTaskCompleted, func(e events.Event) {
		syncRunFromTaskEvent(ctx, svc, e)
	})
	bus.Subscribe(protocol.EventTaskFailed, func(e events.Event) {
		syncRunFromTaskEvent(ctx, svc, e)
	})
	bus.Subscribe(protocol.EventTaskCancelled, func(e events.Event) {
		syncRunFromTaskEvent(ctx, svc, e)
	})
}

func syncRunFromTaskEvent(ctx context.Context, svc *service.AutopilotService, e events.Event) {
	payload, ok := e.Payload.(map[string]any)
	if !ok {
		return
	}
	taskID, ok := payload["task_id"].(string)
	if !ok || taskID == "" {
		return
	}
	task, err := svc.Queries.GetAgentTask(ctx, parseUUID(taskID))
	if err != nil {
		return
	}
	if !task.AutopilotRunID.Valid {
		return
	}
	svc.SyncRunFromTask(ctx, task)
}

// registerStreamDisconnectListener hooks into comment:created events to
// detect terminal stream_disconnected system comments and trigger run
// failure sync + compensation retry.
//
// Comment events come from two sources with different payload shapes:
//   - handler path: payload["comment"] = handler.CommentResponse
//   - task failure path: payload["comment"] = map[string]any
//
// We accept both shapes and extract the fields we need.
func registerStreamDisconnectListener(bus *events.Bus, svc *service.AutopilotService) {
	ctx := context.Background()

	bus.Subscribe(protocol.EventCommentCreated, func(e events.Event) {
		payload, ok := e.Payload.(map[string]any)
		if !ok {
			return
		}

		rawComment, ok := payload["comment"]
		if !ok {
			return
		}

		var (
			issueID     pgtype.UUID
			content     string
			commentType string
			ok1, ok2    bool
		)

		if cr, ok := rawComment.(handler.CommentResponse); ok {
			issueID = parseUUID(cr.IssueID)
			content = cr.Content
			commentType = cr.Type
			ok1, ok2 = true, true
		} else if cm, ok := rawComment.(map[string]any); ok {
			issueID, ok1 = parseUUIDFromMap(cm, "issue_id")
			content, ok2 = stringFromMap(cm, "content")
			commentType, _ = stringFromMap(cm, "type")
		} else {
			return
		}

		if !ok1 || !ok2 || content == "" {
			return
		}
		if !issueID.Valid {
			return
		}

		run, err := svc.HandleStreamDisconnectedComment(ctx, issueID, content, commentType)
		if err != nil {
			slog.Warn("stream disconnect listener: handle failed",
				"issue_id", util.UUIDToString(issueID),
				"error", err,
			)
			return
		}
		if run != nil {
			slog.Info("stream disconnect listener: run failed",
				"run_id", util.UUIDToString(run.ID),
				"issue_id", util.UUIDToString(issueID),
			)
		}
	})
}

func parseUUIDFromMap(m map[string]any, key string) (pgtype.UUID, bool) {
	v, ok := m[key]
	if !ok {
		return pgtype.UUID{}, false
	}
	s, ok := v.(string)
	if !ok {
		return pgtype.UUID{}, false
	}
	return parseUUID(s), true
}

func stringFromMap(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
