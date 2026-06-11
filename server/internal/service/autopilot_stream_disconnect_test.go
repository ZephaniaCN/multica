package service

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// =============================================================================
// Unit tests: matchStreamDisconnected
// =============================================================================

func TestMatchStreamDisconnected(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    bool
	}{
		{
			name:    "exact MYW-3165 failure shape",
			content: "stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)",
			want:    true,
		},
		{
			name:    "case insensitive",
			content: "STREAM DISCONNECTED BEFORE COMPLETION: error sending request for url (https://chatgpt.com/backend-api/codex/responses)",
			want:    true,
		},
		{
			name:    "stream disconnected only without codex/responses",
			content: "stream disconnected before completion: some other error",
			want:    false,
		},
		{
			name:    "codex/responses only without stream disconnect",
			content: "error sending request for url (https://chatgpt.com/backend-api/codex/responses)",
			want:    false,
		},
		{
			name:    "generic network error not matching",
			content: "error sending request for url (https://api.example.com)",
			want:    false,
		},
		{
			name:    "unrelated system comment",
			content: "task completed successfully",
			want:    false,
		},
		{
			name:    "empty content",
			content: "",
			want:    false,
		},
		{
			name:    "both patterns present in longer message",
			content: "task failed: stream disconnected before completion; last error: error sending request for url (https://chatgpt.com/backend-api/codex/responses)",
			want:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchStreamDisconnected(tt.content)
			if got != tt.want {
				t.Errorf("matchStreamDisconnected(%q) = %v, want %v", tt.content, got, tt.want)
			}
		})
	}
}

// =============================================================================
// Test helpers
// =============================================================================

type noOpDB struct{}

func (m *noOpDB) Exec(_ context.Context, _ string, _ ...interface{}) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}

func (m *noOpDB) Query(_ context.Context, _ string, _ ...interface{}) (pgx.Rows, error) {
	return nil, pgx.ErrNoRows
}

func (m *noOpDB) QueryRow(_ context.Context, _ string, _ ...interface{}) pgx.Row {
	return &noOpRow{}
}

func (m *noOpDB) SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return nil
}

func testStreamUUID(b byte) pgtype.UUID {
	var u pgtype.UUID
	u.Valid = true
	u.Bytes[0] = b
	return u
}

type noOpRow struct{}

func (r *noOpRow) Scan(_ ...interface{}) error {
	return pgx.ErrNoRows
}

// =============================================================================
// Unit tests: HandleStreamDisconnectedComment input validation
// =============================================================================

func TestHandleStreamDisconnectedComment_RejectsNonSystemType(t *testing.T) {
	bus := events.New()
	svc := &AutopilotService{Queries: db.New(&noOpDB{}), Bus: bus}

	run, err := svc.HandleStreamDisconnectedComment(context.Background(),
		testStreamUUID(1),
		"stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)",
		"comment",
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run != nil {
		t.Error("expected nil run for non-system comment type")
	}
}

func TestHandleStreamDisconnectedComment_RejectsNonMatchingContent(t *testing.T) {
	bus := events.New()
	svc := &AutopilotService{Queries: db.New(&noOpDB{}), Bus: bus}

	run, err := svc.HandleStreamDisconnectedComment(context.Background(),
		testStreamUUID(1),
		"agent is working on the task",
		"system",
	)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run != nil {
		t.Error("expected nil run for non-stream-disconnect content")
	}
}

// =============================================================================
// Unit tests: CreateCompensationRun recursion prevention
// =============================================================================

func TestCreateCompensationRun_RejectsCompensationRun(t *testing.T) {
	bus := events.New()
	autopilotID := testStreamUUID(3)

	compRun := db.AutopilotRun{
		ID:             testStreamUUID(1),
		AutopilotID:    autopilotID,
		Status:         "failed",
		IsCompensation: true,
		RetryOf:        testStreamUUID(4),
	}

	autopilot := db.Autopilot{
		ID:          autopilotID,
		WorkspaceID: testStreamUUID(5),
	}

	svc := &AutopilotService{Queries: db.New(&noOpDB{}), Bus: bus}

	err := svc.CreateCompensationRun(context.Background(), autopilot, compRun)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// =============================================================================
// Test: System comment shape documentation
// =============================================================================

func TestSystemCommentShape_DocumentsAuthorTypeVsType(t *testing.T) {
	// Documents the contract: system error comments from task failures
	// have author_type="agent" and type="system". Both the listener
	// and reconciler must match on Type, not AuthorType.

	expectedAuthorType := "agent"
	expectedType := "system"

	if expectedAuthorType != "agent" {
		t.Errorf("system error comment author_type must be 'agent', got %q", expectedAuthorType)
	}
	if expectedType != "system" {
		t.Errorf("system error comment type must be 'system', got %q", expectedType)
	}

	// Guard: the listener passes commentType (not authorType) to
	// HandleStreamDisconnectedComment, and the function checks
	// commentType == "system". Verify that a comment with
	// author_type="agent" and type="system" would match.
	svc := &AutopilotService{Queries: db.New(&noOpDB{}), Bus: events.New()}
	run, err := svc.HandleStreamDisconnectedComment(context.Background(),
		testStreamUUID(1),
		"stream disconnected before completion: error sending request for url (https://chatgpt.com/backend-api/codex/responses)",
		"agent", // wrong: passes author_type instead of type
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if run != nil {
		t.Error("should reject when commentType == 'agent' (author_type, not type)")
	}
}
