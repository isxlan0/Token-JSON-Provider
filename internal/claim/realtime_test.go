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

	"token-atlas/internal/auth"
	"token-atlas/internal/config"
	"token-atlas/internal/runtimecache"
)

func TestCreateClaimRequestPublishesTerminalSnapshotWhenInventoryIsReady(t *testing.T) {
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
		t.Fatalf("expected ready inventory request to complete directly, got %+v", accepted)
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
		t.Fatalf("expected terminal success snapshot, got %+v", selfSnapshot.Requests[0])
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

func TestCreateClaimRequestSnapshotRebuildsFromTerminalClaimQueueLedger(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4004", "realtime-direct-ledger")
	insertTestToken(t, store, "realtime-direct-ledger.json", `{"access_token":"realtime-direct-ledger","refresh_token":"realtime-direct-ledger-r","account_id":"acct-realtime-direct-ledger"}`, 0, 1)

	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-ledger",
		User: auth.UserPayload{
			ID:         "4004",
			Username:   "realtime-direct-ledger",
			Name:       "realtime-direct-ledger",
			TrustLevel: 2,
		},
	}

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 1, "tab-ledger")
	if err != nil {
		t.Fatalf("create direct claim request: %v", err)
	}

	var (
		status          string
		originSessionID sql.NullString
		originTabID     sql.NullString
	)
	if err := store.DB().QueryRow(`
		SELECT status, origin_session_id, origin_tab_id
		FROM claim_queue
		WHERE user_id = ? AND request_id = ?
	`, userID, accepted.RequestID).Scan(&status, &originSessionID, &originTabID); err != nil {
		t.Fatalf("load persisted claim request row: %v", err)
	}
	if status != queueStatusSucceeded {
		t.Fatalf("expected accepted request to persist as succeeded, got %q", status)
	}
	if !originSessionID.Valid || originSessionID.String != "session-ledger" {
		t.Fatalf("unexpected persisted origin session: %+v", originSessionID)
	}
	if !originTabID.Valid || originTabID.String != "tab-ledger" {
		t.Fatalf("unexpected persisted origin tab: %+v", originTabID)
	}

	rebuilt, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-ledger")
	if err != nil {
		t.Fatalf("rebuild direct claim snapshot: %v", err)
	}
	if len(rebuilt.Requests) != 1 {
		t.Fatalf("expected rebuilt snapshot to contain one request, got %+v", rebuilt.Requests)
	}
	if rebuilt.Requests[0].RequestID != accepted.RequestID || rebuilt.Requests[0].Status != claimStatusSucceeded || !rebuilt.Requests[0].Terminal {
		t.Fatalf("expected rebuilt direct request to complete successfully, got %+v", rebuilt.Requests[0])
	}
	if rebuilt.Requests[0].Source != claimSourceSelf {
		t.Fatalf("expected rebuilt request to preserve self source, got %+v", rebuilt.Requests[0])
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
	if accepted.Status != claimStatusQueuedBlocked || accepted.BlockReason != queueBlockReasonInventoryUnavailable {
		t.Fatalf("expected inventory-unavailable acceptance state, got %+v", accepted)
	}

	queuedSnapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-queued")
	if err != nil {
		t.Fatalf("get queued snapshot: %v", err)
	}
	if len(queuedSnapshot.Requests) != 1 {
		t.Fatalf("expected queued snapshot to contain one request, got %+v", queuedSnapshot.Requests)
	}
	if queuedSnapshot.Requests[0].Status != claimStatusQueuedBlocked || queuedSnapshot.Requests[0].QueueID != accepted.QueueID {
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

func TestQueuedRequestRebuildKeepsProgressNonTerminal(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4005", "realtime-progress-user")
	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-progress",
		User: auth.UserPayload{
			ID:         "4005",
			Username:   "realtime-progress-user",
			Name:       "realtime-progress-user",
			TrustLevel: 2,
		},
	}

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 2, "tab-progress")
	if err != nil {
		t.Fatalf("create queued progress request: %v", err)
	}
	if accepted == nil || !accepted.Queued {
		t.Fatalf("expected queued accepted payload, got %+v", accepted)
	}

	insertTestToken(t, store, "realtime-progress.json", `{"access_token":"realtime-progress","refresh_token":"realtime-progress-r","account_id":"acct-realtime-progress"}`, 0, 1)
	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue for progress: %v", err)
	}

	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	rebuilt, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-progress")
	if err != nil {
		t.Fatalf("rebuild queued progress snapshot: %v", err)
	}
	if len(rebuilt.Requests) != 1 {
		t.Fatalf("expected rebuilt progress snapshot to contain one request, got %+v", rebuilt.Requests)
	}
	request := rebuilt.Requests[0]
	if request.RequestID != accepted.RequestID {
		t.Fatalf("unexpected request after rebuild: %+v", request)
	}
	if request.Status != claimStatusQueuedWaiting || request.Terminal {
		t.Fatalf("expected partial progress to remain queued and non-terminal, got %+v", request)
	}
	if request.Granted != 1 || request.Remaining != 1 {
		t.Fatalf("expected queued progress counts to reflect partial grant, got %+v", request)
	}
	if !request.Queued {
		t.Fatalf("expected queued flag to remain true, got %+v", request)
	}
}

func TestQueuedBlockedRequestRebuildStaysNonTerminal(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4006", "realtime-blocked-user")
	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-blocked",
		User: auth.UserPayload{
			ID:         "4006",
			Username:   "realtime-blocked-user",
			Name:       "realtime-blocked-user",
			TrustLevel: 2,
		},
	}

	tokenID := insertTestToken(t, store, "realtime-blocked.json", `{"access_token":"realtime-blocked","refresh_token":"realtime-blocked-r","account_id":"acct-realtime-blocked"}`, 0, 2)
	claimID := insertTestClaimRecord(t, store, tokenID, userID, nil, time.Now().Add(-3*time.Minute).Unix(), "seed-blocked")
	insertTestUserTokenClaim(t, store, userID, tokenID, claimID, time.Now().Add(-3*time.Minute).Unix())

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 1, "tab-blocked")
	if err != nil {
		t.Fatalf("create blocked claim request: %v", err)
	}
	if accepted == nil || !accepted.Queued || accepted.Status != claimStatusQueuedBlocked {
		t.Fatalf("expected blocked accepted payload, got %+v", accepted)
	}

	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	rebuilt, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-blocked")
	if err != nil {
		t.Fatalf("rebuild blocked snapshot: %v", err)
	}
	requestsByID := make(map[string]claimRealtimeRequest, len(rebuilt.Requests))
	for _, item := range rebuilt.Requests {
		requestsByID[item.RequestID] = item
	}
	request, ok := requestsByID[accepted.RequestID]
	if !ok {
		t.Fatalf("expected blocked request %q in rebuilt snapshot, got %+v", accepted.RequestID, rebuilt.Requests)
	}
	if request.Status != claimStatusQueuedBlocked || request.Terminal || !request.Queued {
		t.Fatalf("expected blocked request to remain non-terminal queued state, got %+v", request)
	}
	if request.BlockReason != queueBlockReasonNoEligibleTokens {
		t.Fatalf("expected blocked request reason to persist, got %+v", request)
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

func TestUpsertClaimRealtimeRequestDropsStaleQueueTotalRollback(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4007", "realtime-monotonic-user")
	requestID := "realtime-monotonic-request"
	updatedAtTS := time.Now().Unix()

	if err := service.upsertClaimRealtimeRequest(ctx, userID, claimRealtimeRequest{
		RequestID:     requestID,
		Status:        claimStatusQueuedWaiting,
		Queued:        true,
		QueueID:       11,
		QueuePosition: 3,
		QueueTotal:    11,
		Requested:     3,
		Granted:       0,
		Remaining:     3,
		UpdatedAtTS:   updatedAtTS,
	}); err != nil {
		t.Fatalf("insert initial realtime request: %v", err)
	}

	if err := service.upsertClaimRealtimeRequest(ctx, userID, claimRealtimeRequest{
		RequestID:     requestID,
		Status:        claimStatusQueuedWaiting,
		Queued:        true,
		QueueID:       11,
		QueuePosition: 3,
		QueueTotal:    7,
		Requested:     3,
		Granted:       0,
		Remaining:     3,
		UpdatedAtTS:   updatedAtTS,
	}); err != nil {
		t.Fatalf("insert stale realtime rollback: %v", err)
	}

	snapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-monotonic")
	if err != nil {
		t.Fatalf("get realtime snapshot: %v", err)
	}
	if len(snapshot.Requests) != 1 {
		t.Fatalf("expected one realtime request, got %+v", snapshot.Requests)
	}
	request := snapshot.Requests[0]
	if request.RequestID != requestID {
		t.Fatalf("unexpected request in snapshot: %+v", request)
	}
	if request.QueueTotal != 11 {
		t.Fatalf("expected stale queue_total rollback to be dropped, got %+v", request)
	}
	if request.QueuePosition != 3 || request.Remaining != 3 || request.Status != claimStatusQueuedWaiting {
		t.Fatalf("expected realtime request to keep newer queue state, got %+v", request)
	}
}
