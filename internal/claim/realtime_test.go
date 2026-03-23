package claim

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
	"token-atlas/internal/config"
	"token-atlas/internal/runtimecache"
)

func TestCreateClaimRequestPublishesDirectTerminalSnapshot(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4001", "realtime-direct-user")
	insertTestToken(t, store, "realtime-direct.json", `{"access_token":"realtime-direct","refresh_token":"realtime-direct-r","account_id":"acct-realtime-direct"}`, 0, 1)

	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-self",
		User: auth.UserPayload{
			ID:         "4001",
			Username:   "realtime-direct-user",
			Name:       "realtime-direct-user",
			TrustLevel: 2,
		},
	}

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 1, "tab-a")
	if err != nil {
		t.Fatalf("create claim request: %v", err)
	}
	if accepted == nil || accepted.RequestID == "" {
		t.Fatalf("expected accepted request payload, got %+v", accepted)
	}
	if accepted.Queued {
		t.Fatalf("expected direct claim to complete immediately, got queued payload %+v", accepted)
	}

	selfSnapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-self")
	if err != nil {
		t.Fatalf("get self snapshot: %v", err)
	}
	if len(selfSnapshot.Requests) != 1 {
		t.Fatalf("expected one realtime request, got %+v", selfSnapshot.Requests)
	}
	if selfSnapshot.Requests[0].RequestID != accepted.RequestID {
		t.Fatalf("unexpected request id in snapshot: %+v", selfSnapshot.Requests[0])
	}
	if selfSnapshot.Requests[0].Status != claimStatusSucceeded || !selfSnapshot.Requests[0].Terminal {
		t.Fatalf("expected succeeded terminal snapshot, got %+v", selfSnapshot.Requests[0])
	}
	if selfSnapshot.Requests[0].Source != claimSourceSelf {
		t.Fatalf("expected self source for same session, got %+v", selfSnapshot.Requests[0])
	}

	otherSnapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-other")
	if err != nil {
		t.Fatalf("get other session snapshot: %v", err)
	}
	if len(otherSnapshot.Requests) != 1 {
		t.Fatalf("expected one realtime request for other session, got %+v", otherSnapshot.Requests)
	}
	if otherSnapshot.Requests[0].Source != claimSourceOtherSession {
		t.Fatalf("expected other_session source, got %+v", otherSnapshot.Requests[0])
	}
}

func TestCreateClaimRequestQueuedSnapshotCompletesAfterAdvance(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4002", "realtime-queued-user")
	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-queued",
		User: auth.UserPayload{
			ID:         "4002",
			Username:   "realtime-queued-user",
			Name:       "realtime-queued-user",
			TrustLevel: 2,
		},
	}

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 1, "tab-queued")
	if err != nil {
		t.Fatalf("create queued claim request: %v", err)
	}
	if accepted == nil || !accepted.Queued {
		t.Fatalf("expected queued accepted payload, got %+v", accepted)
	}

	queuedSnapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-queued")
	if err != nil {
		t.Fatalf("get queued snapshot: %v", err)
	}
	if len(queuedSnapshot.Requests) != 1 {
		t.Fatalf("expected queued snapshot to contain one request, got %+v", queuedSnapshot.Requests)
	}
	if queuedSnapshot.Requests[0].Status != claimStatusQueued || queuedSnapshot.Requests[0].QueueID != accepted.QueueID {
		t.Fatalf("expected queued realtime request, got %+v", queuedSnapshot.Requests[0])
	}

	insertTestToken(t, store, "realtime-queued.json", `{"access_token":"realtime-queued","refresh_token":"realtime-queued-r","account_id":"acct-realtime-queued"}`, 0, 1)
	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue: %v", err)
	}

	finalSnapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-queued")
	if err != nil {
		t.Fatalf("get final snapshot: %v", err)
	}
	if len(finalSnapshot.Requests) != 1 {
		t.Fatalf("expected final snapshot to contain one request, got %+v", finalSnapshot.Requests)
	}
	if finalSnapshot.Requests[0].Status != claimStatusSucceeded || !finalSnapshot.Requests[0].Terminal {
		t.Fatalf("expected queued request to finish successfully, got %+v", finalSnapshot.Requests[0])
	}
	if finalSnapshot.Requests[0].Queued {
		t.Fatalf("expected queued request flag to be cleared, got %+v", finalSnapshot.Requests[0])
	}
}

func TestQueueStreamEmitsClaimSnapshotEvents(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4003", "stream-user")
	insertTestToken(t, store, "stream-direct.json", `{"access_token":"stream-direct","refresh_token":"stream-direct-r","account_id":"acct-stream-direct"}`, 0, 1)

	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-stream",
		User: auth.UserPayload{
			ID:         "4003",
			Username:   "stream-user",
			Name:       "stream-user",
			TrustLevel: 2,
		},
	}

	if _, err := service.CreateClaimRequest(ctx, requestContext, nil, 1, "tab-stream"); err != nil {
		t.Fatalf("create direct claim request: %v", err)
	}

	e := echo.New()
	reqCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	request := httptest.NewRequest(http.MethodGet, "/me/queue-stream", nil).WithContext(reqCtx)
	recorder := httptest.NewRecorder()
	echoContext := e.NewContext(request, recorder)
	echoContext.Set("auth.request_context", requestContext)

	done := make(chan error, 1)
	go func() {
		done <- service.getQueueStream(echoContext)
	}()

	waitForTestCondition(t, time.Second, func() bool {
		body := recorder.Body.String()
		return strings.Contains(body, "event: stream_status") && strings.Contains(body, "event: claim_snapshot")
	})

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("queue stream returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queue stream did not exit after context cancellation")
	}

	body := recorder.Body.String()
	if !strings.Contains(body, "\"transport\":\"sse\"") {
		t.Fatalf("expected SSE transport marker in stream body, got %s", body)
	}
	if !strings.Contains(body, "\"request_id\":\"") {
		t.Fatalf("expected request id in claim snapshot, got %s", body)
	}
}
