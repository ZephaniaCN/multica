package handler

import (
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/internal/util"
)

// HealthCheckEvent represents a detected health issue
type HealthCheckEvent struct {
	ID           uuid.UUID `json:"id"`
	WorkspaceID  uuid.UUID `json:"workspace_id"`
	CheckType    string    `json:"check_type"`
	Severity     string    `json:"severity"`
	ResourceType string    `json:"resource_type"`
	ResourceID   uuid.UUID `json:"resource_id"`
	Details      any       `json:"details"`
	DetectedAt   time.Time `json:"detected_at"`
	ResolvedAt   *time.Time `json:"resolved_at,omitempty"`
	CreatedAt    time.Time `json:"created_at"`
}

// HealthCheckStats represents overall health check statistics
type HealthCheckStats struct {
	TotalEvents        int64     `json:"total_events"`
	UnresolvedEvents   int64     `json:"unresolved_events"`
	CriticalEvents     int64     `json:"critical_events"`
	HeartbeatChecks    int64     `json:"heartbeat_checks"`
	StreamDisconnects  int64     `json:"stream_disconnects"`
	ExecutionTimeouts  int64     `json:"execution_timeouts"`
	StateInconsistencies int64    `json:"state_inconsistencies"`
	LastCheckAt        time.Time `json:"last_check_at"`
}

// GetHealthCheckEvents returns unresolved health check events for a workspace
func (h *Handler) GetHealthCheckEvents(w http.ResponseWriter, r *http.Request) {
	wsID, err := h.getWorkspaceID(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	// Get unresolved events
	events, err := h.Queries.GetUnresolvedHealthCheckEvents(r.Context())
	if err != nil {
		http.Error(w, "Failed to get health check events", http.StatusInternalServerError)
		return
	}

	// Filter by workspace and convert to response format
	result := make([]HealthCheckEvent, 0)
	for _, event := range events {
		if event.WorkspaceID != wsID {
			continue
		}

		var resolvedAt *time.Time
		if event.ResolvedAt.Valid {
			resolvedAt = &event.ResolvedAt.Time
		}

		var details any
		if err := event.ResolvedAt.Scanner(&details); err != nil {
			details = map[string]interface{}{}
		}

		result = append(result, HealthCheckEvent{
			ID:           event.ID,
			WorkspaceID:  event.WorkspaceID,
			CheckType:    event.CheckType,
			Severity:     event.Severity,
			ResourceType: event.ResourceType,
			ResourceID:   event.ResourceID,
			Details:      details,
			DetectedAt:   event.DetectedAt.Time,
			ResolvedAt:   resolvedAt,
			CreatedAt:    event.CreatedAt.Time,
		})
	}

	writeJSON(w, http.StatusOK, result)
}

// GetResourceHealthCheckEvents returns health check events for a specific resource
func (h *Handler) GetResourceHealthCheckEvents(w http.ResponseWriter, r *http.Request) {
	wsID, err := h.getWorkspaceID(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	resourceType := r.PathValue("resource_type")
	resourceIDStr := r.PathValue("resource_id")
	resourceID, err := uuid.Parse(resourceIDStr)
	if err != nil {
		http.Error(w, "Invalid resource ID", http.StatusBadRequest)
		return
	}

	// Parse limit parameter
	limit := 50
	if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
		if l, err := parseIntParam(limitStr, 1, 100); err == nil {
			limit = l
		}
	}

	events, err := h.Queries.GetHealthCheckEventsByResource(r.Context(), db.GetHealthCheckEventsByResourceParams{
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Limit:        int32(limit),
	})
	if err != nil {
		http.Error(w, "Failed to get health check events", http.StatusInternalServerError)
		return
	}

	// Filter by workspace and convert to response format
	result := make([]HealthCheckEvent, 0)
	for _, event := range events {
		if event.WorkspaceID != wsID {
			continue
		}

		var resolvedAt *time.Time
		if event.ResolvedAt.Valid {
			resolvedAt = &event.ResolvedAt.Time
		}

		result = append(result, HealthCheckEvent{
			ID:           event.ID,
			WorkspaceID:  event.WorkspaceID,
			CheckType:    event.CheckType,
			Severity:     event.Severity,
			ResourceType: event.ResourceType,
			ResourceID:   event.ResourceID,
			Details:      event.Details,
			DetectedAt:   event.DetectedAt.Time,
			ResolvedAt:   resolvedAt,
			CreatedAt:    event.CreatedAt.Time,
		})
	}

	writeJSON(w, http.StatusOK, result)
}

// GetHealthCheckStats returns overall health check statistics
func (h *Handler) GetHealthCheckStats(w http.ResponseWriter, r *http.Request) {
	wsID, err := h.getWorkspaceID(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	stats, err := h.Queries.GetHealthCheckStats(r.Context())
	if err != nil {
		http.Error(w, "Failed to get health check stats", http.StatusInternalServerError)
		return
	}

	result := HealthCheckStats{
		TotalEvents:         stats.TotalEvents,
		UnresolvedEvents:    stats.UnresolvedEvents,
		CriticalEvents:      stats.CriticalEvents,
		HeartbeatChecks:     stats.HeartbeatChecks,
		StreamDisconnects:   stats.StreamDisconnects,
		ExecutionTimeouts:   stats.ExecutionTimeouts,
		StateInconsistencies: stats.StateInconsistencies,
		LastCheckAt:         stats.LastCheckAt.Time,
	}

	writeJSON(w, http.StatusOK, result)
}

// ResolveHealthCheckEvent marks a health check event as resolved
func (h *Handler) ResolveHealthCheckEvent(w http.ResponseWriter, r *http.Request) {
	wsID, err := h.getWorkspaceID(r)
	if err != nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	eventIDStr := r.PathValue("id")
	eventID, err := uuid.Parse(eventIDStr)
	if err != nil {
		http.Error(w, "Invalid event ID", http.StatusBadRequest)
		return
	}

	// Verify the event belongs to this workspace
	event, err := h.Queries.GetHealthCheckEventByID(r.Context(), eventID)
	if err != nil {
		http.Error(w, "Event not found", http.StatusNotFound)
		return
	}

	if event.WorkspaceID != wsID {
		http.Error(w, "Forbidden", http.StatusForbidden)
		return
	}

	// Mark as resolved
	err = h.Queries.ResolveHealthCheckEvent(r.Context(), eventID)
	if err != nil {
		http.Error(w, "Failed to resolve event", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success": true,
		"message": "Health check event resolved",
	})
}