package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/multica-ai/multica/server/internal/service"
)

const (
	// streamDisconnectReconcilerInterval controls how often we scan
	// for autopilot runs that may be stuck due to stream disconnects.
	streamDisconnectReconcilerInterval = 30 * time.Second

	// stuckRunThreshold is how long a run must be in issue_created
	// before the reconciler considers it potentially stuck.
	stuckRunThreshold = 5 * time.Minute

	// maxStuckRunsPerCycle limits reconciliation throughput per cycle.
	maxStuckRunsPerCycle = 20
)

// runStreamDisconnectReconciler periodically scans for create_issue
// autopilot runs that have been stuck in issue_created with a
// stream_disconnected system comment. It runs as a safety net for
// cases where the event-driven path misses the comment:created event.
func runStreamDisconnectReconciler(ctx context.Context, svc *service.AutopilotService) {
	ticker := time.NewTicker(streamDisconnectReconcilerInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			reconciled, failed := svc.ReconcileStuckRuns(ctx, stuckRunThreshold, maxStuckRunsPerCycle)
			if reconciled > 0 || failed > 0 {
				slog.Info("stream disconnect reconciler: cycle complete",
					"reconciled", reconciled,
					"failed", failed,
				)
			}
		}
	}
}
