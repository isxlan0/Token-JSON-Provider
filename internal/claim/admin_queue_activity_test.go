package claim

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"
)

func TestAdminQueueActivitySnapshotTracksQueueLifecycle(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4010", "admin-queue-activity-user")
	result, err := service.ClaimTokens(ctx, userID, nil, 1)
	if err != nil {
		t.Fatalf("queue claim request: %v", err)
	}
	if result == nil || !result.Queued {
		t.Fatalf("expected queued claim request, got %+v", result)
	}

	insertTestToken(t, store, "admin-queue-activity.json", `{"access_token":"admin-queue-activity","refresh_token":"admin-queue-activity-r","account_id":"acct-admin-queue-activity"}`, 0, 1)
	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue: %v", err)
	}

	snapshot := service.GetAdminQueueActivitySnapshot(16)
	if snapshot.Total < 4 {
		t.Fatalf("expected lifecycle events to be recorded, got %+v", snapshot)
	}

	requiredStages := map[string]bool{
		"queue_enqueued":     false,
		"processing_started": false,
		"probe_started":      false,
		"queue_completed":    false,
	}
	for _, item := range snapshot.Items {
		if _, ok := requiredStages[item.Stage]; ok {
			requiredStages[item.Stage] = true
		}
	}
	for stage, seen := range requiredStages {
		if !seen {
			t.Fatalf("expected activity stage %q in snapshot, got %+v", stage, snapshot.Items)
		}
	}
}

func TestAdminQueueActivitySuppressesAdjacentDuplicateRetryPending(t *testing.T) {
	service, _ := newClaimTestService(t)
	retryAt := time.Now().Add(2 * time.Minute).Unix()
	entry := userQueueEntry{
		ID:            88,
		UserID:        4011,
		Remaining:     1,
		QueueRank:     1,
		RequestID:     "admin-queue-dup",
		Status:        queueStatusQueuedBlocked,
		BlockReason:   sql.NullString{String: queueBlockReasonHourlyQuotaExhausted, Valid: true},
		NextRetryAtTS: sql.NullInt64{Int64: retryAt, Valid: true},
	}

	service.publishAdminQueueRetryPending(entry)
	service.publishAdminQueueRetryPending(entry)

	snapshot := service.GetAdminQueueActivitySnapshot(10)
	if snapshot.Total != 1 {
		t.Fatalf("expected duplicate retry events to collapse, got %+v", snapshot)
	}
	if len(snapshot.Items) != 1 || snapshot.Items[0].Stage != "retry_pending" {
		t.Fatalf("unexpected retry snapshot: %+v", snapshot.Items)
	}
}

func TestAdminQueueStreamEmitsActivitySnapshot(t *testing.T) {
	service, _ := newClaimTestService(t)
	service.publishAdminQueueActivity(adminQueueActivityEvent{
		Kind:      "info",
		Stage:     "queue_enqueued",
		Message:   "开始排队",
		Detail:    "测试活动流",
		QueueID:   9,
		UserID:    4012,
		RequestID: "admin-stream-request",
	})

	e := echo.New()
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request := httptest.NewRequest(http.MethodGet, "/admin/queue/stream", nil).WithContext(reqCtx)
	recorder := httptest.NewRecorder()
	echoContext := e.NewContext(request, recorder)

	done := make(chan error, 1)
	go func() {
		done <- service.adminGetQueueStream(echoContext)
	}()

	waitForTestCondition(t, time.Second, func() bool {
		body := recorder.Body.String()
		return strings.Contains(body, "event: stream_status") && strings.Contains(body, "event: admin_queue_activity")
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("admin queue stream returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("admin queue stream did not exit after context cancellation")
	}

	body := recorder.Body.String()
	if !strings.Contains(body, "\"scope\":\"admin_queue_activity\"") {
		t.Fatalf("expected admin queue stream scope marker, got %s", body)
	}
	if !strings.Contains(body, "\"message\":\"开始排队\"") {
		t.Fatalf("expected activity payload in stream body, got %s", body)
	}
}
