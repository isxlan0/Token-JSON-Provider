package claim

import (
	"context"
	"database/sql"
	"fmt"
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

func TestCreateClaimRequestQueuesThenPublishesTerminalSnapshotWhenInventoryIsReady(t *testing.T) {
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
	if !accepted.Queued {
		t.Fatalf("expected request acceptance to stay asynchronous, got %+v", accepted)
	}
	if accepted.Status != claimStatusQueuedWaiting || accepted.Terminal {
		t.Fatalf("expected accepted ack to remain queued until queue pump runs, got %+v", accepted)
	}
	if accepted.Granted != 0 || accepted.Remaining != 1 || len(accepted.Items) != 0 {
		t.Fatalf("expected accepted ack to avoid synchronous grants, got %+v", accepted)
	}
	if accepted.QueuePosition != 1 || accepted.QueueTotal != 1 {
		t.Fatalf("expected accepted ack to include queue placement, got %+v", accepted)
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
	if selfSnapshot.Requests[0].Status != claimStatusQueuedWaiting || selfSnapshot.Requests[0].Terminal {
		t.Fatalf("expected queued snapshot before queue advance, got %+v", selfSnapshot.Requests[0])
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

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue: %v", err)
	}

	finalSnapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-self")
	if err != nil {
		t.Fatalf("get final snapshot: %v", err)
	}
	if len(finalSnapshot.Requests) != 1 {
		t.Fatalf("expected one realtime request after advance, got %+v", finalSnapshot.Requests)
	}
	if finalSnapshot.Requests[0].Status != claimStatusSucceeded || !finalSnapshot.Requests[0].Terminal {
		t.Fatalf("expected terminal success snapshot after advance, got %+v", finalSnapshot.Requests[0])
	}
	if finalSnapshot.Requests[0].Queued {
		t.Fatalf("expected final request to leave queued state, got %+v", finalSnapshot.Requests[0])
	}
}

func TestCreateClaimRequestQueuesWhenQueueDisabled(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	service.cfg.Server.QueueEnabled = false
	ctx := context.Background()

	userID := insertTestUser(t, store, "4011", "realtime-queue-disabled-user")
	insertTestToken(t, store, "realtime-queue-disabled.json", `{"access_token":"realtime-queue-disabled","refresh_token":"realtime-queue-disabled-r","account_id":"acct-realtime-queue-disabled"}`, 0, 1)

	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-disabled",
		User: auth.UserPayload{
			ID:         "4011",
			Username:   "realtime-queue-disabled-user",
			Name:       "realtime-queue-disabled-user",
			TrustLevel: 2,
		},
	}

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 1, "tab-disabled")
	if err != nil {
		t.Fatalf("create queued claim request with queue disabled: %v", err)
	}
	if accepted == nil || !accepted.Queued {
		t.Fatalf("expected accepted payload to enqueue even when direct queueing is disabled, got %+v", accepted)
	}
	if accepted.Status != claimStatusQueuedWaiting || accepted.Terminal {
		t.Fatalf("expected queue-disabled accepted ack to remain queued, got %+v", accepted)
	}

	var queuedRows int
	if err := store.DB().QueryRow(`
		SELECT COUNT(*)
		FROM claim_queue
		WHERE user_id = ? AND status IN ('queued', 'queued_waiting', 'queued_blocked') AND remaining > 0
	`, userID).Scan(&queuedRows); err != nil {
		t.Fatalf("count queued rows: %v", err)
	}
	if queuedRows != 1 {
		t.Fatalf("expected queue-disabled request to persist one active queue row, got %d", queuedRows)
	}

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue with queue disabled: %v", err)
	}

	finalSnapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, "session-disabled")
	if err != nil {
		t.Fatalf("get final snapshot with queue disabled: %v", err)
	}
	if len(finalSnapshot.Requests) != 1 || finalSnapshot.Requests[0].Status != claimStatusSucceeded || !finalSnapshot.Requests[0].Terminal {
		t.Fatalf("expected queue-disabled accepted request to complete through queue pump, got %+v", finalSnapshot.Requests)
	}
}

func TestCreateClaimRequestQueuesBlockedWhenQueueDisabledAndInventoryUnavailable(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	service.cfg.Server.QueueEnabled = false
	ctx := context.Background()

	userID := insertTestUser(t, store, "4012", "realtime-queue-disabled-empty-user")
	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-disabled-empty",
		User: auth.UserPayload{
			ID:         "4012",
			Username:   "realtime-queue-disabled-empty-user",
			Name:       "realtime-queue-disabled-empty-user",
			TrustLevel: 2,
		},
	}

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 1, "tab-disabled-empty")
	if err != nil {
		t.Fatalf("expected blocked queued request instead of conflict, got %v", err)
	}
	if accepted == nil || !accepted.Queued {
		t.Fatalf("expected blocked accepted payload, got %+v", accepted)
	}
	if accepted.Status != claimStatusQueuedBlocked || accepted.BlockReason != queueBlockReasonInventoryUnavailable {
		t.Fatalf("expected inventory-unavailable blocked ack, got %+v", accepted)
	}

	var queuedRows int
	if err := store.DB().QueryRow(`
		SELECT COUNT(*)
		FROM claim_queue
		WHERE user_id = ? AND status IN ('queued', 'queued_waiting', 'queued_blocked') AND remaining > 0
	`, userID).Scan(&queuedRows); err != nil {
		t.Fatalf("count queued rows: %v", err)
	}
	if queuedRows != 1 {
		t.Fatalf("expected blocked request to keep one active queue row, got %d", queuedRows)
	}
}

func TestCreateClaimRequestReturnsTerminalPartialWhenHourlyQuotaIsExhausted(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "40121", "realtime-hourly-partial-user")
	tokenID := insertTestToken(t, store, "realtime-hourly-partial.json", `{"access_token":"realtime-hourly-partial","refresh_token":"realtime-hourly-partial-r","account_id":"acct-realtime-hourly-partial"}`, 0, 1)
	earliestClaimTS := time.Now().Add(-30 * time.Minute).Unix()
	for index := 0; index < service.cfg.Inventory.Limits.Healthy.Hourly; index++ {
		insertTestClaimRecord(t, store, tokenID, userID, nil, earliestClaimTS+int64(index), fmt.Sprintf("hourly-seed-%d", index))
	}

	requestContext := &auth.RequestContext{
		UserID:    userID,
		SessionID: "session-hourly-partial",
		User: auth.UserPayload{
			ID:         "40121",
			Username:   "realtime-hourly-partial-user",
			Name:       "realtime-hourly-partial-user",
			TrustLevel: 2,
		},
	}

	accepted, err := service.CreateClaimRequest(ctx, requestContext, nil, 6, "tab-hourly-partial")
	if err != nil {
		t.Fatalf("create hourly exhausted claim request: %v", err)
	}
	if accepted == nil {
		t.Fatal("expected accepted response for exhausted hourly quota request")
	}
	if accepted.Queued || !accepted.Terminal {
		t.Fatalf("expected hourly exhausted request to finish immediately, got %+v", accepted)
	}
	if accepted.Status != claimStatusPartial {
		t.Fatalf("expected hourly exhausted request to be partial, got %+v", accepted)
	}
	if accepted.Granted != 0 || accepted.Remaining != accepted.Requested || accepted.Requested != 6 {
		t.Fatalf("unexpected accepted counts for hourly exhausted request: %+v", accepted)
	}
	if !strings.Contains(accepted.ReasonMessage, "额度已用尽") {
		t.Fatalf("expected quota exhaustion reason message, got %+v", accepted)
	}

	var activeRows int
	if err := store.DB().QueryRow(`
		SELECT COUNT(*)
		FROM claim_queue
		WHERE user_id = ? AND status IN ('queued', 'queued_waiting', 'queued_blocked') AND remaining > 0
	`, userID).Scan(&activeRows); err != nil {
		t.Fatalf("count active queue rows: %v", err)
	}
	if activeRows != 0 {
		t.Fatalf("expected no active queue rows for exhausted hourly quota request, got %d", activeRows)
	}

	var (
		status       string
		remaining    int
		cancelReason sql.NullString
	)
	if err := store.DB().QueryRow(`
		SELECT status, remaining, cancel_reason
		FROM claim_queue
		WHERE user_id = ? AND request_id = ?
	`, userID, accepted.RequestID).Scan(&status, &remaining, &cancelReason); err != nil {
		t.Fatalf("load terminal claim ledger row: %v", err)
	}
	if status != queueStatusPartial || remaining != 6 {
		t.Fatalf("unexpected terminal claim ledger row: status=%q remaining=%d", status, remaining)
	}
	if !cancelReason.Valid || cancelReason.String != queueBlockReasonHourlyQuotaExhausted {
		t.Fatalf("unexpected terminal reason for exhausted hourly quota request: %+v", cancelReason)
	}

	snapshot, err := service.GetClaimRealtimeSnapshot(ctx, userID, requestContext.SessionID)
	if err != nil {
		t.Fatalf("get realtime snapshot for hourly exhausted request: %v", err)
	}
	var request *claimRealtimeRequest
	for index := range snapshot.Requests {
		if snapshot.Requests[index].RequestID == accepted.RequestID {
			request = &snapshot.Requests[index]
			break
		}
	}
	if request == nil {
		t.Fatalf("expected realtime snapshot to include hourly exhausted request %q, got %+v", accepted.RequestID, snapshot.Requests)
	}
	if request.Status != claimStatusPartial || request.Queued || !request.Terminal {
		t.Fatalf("unexpected realtime request for hourly exhausted claim: %+v", request)
	}
	if request.Granted != 0 || request.Remaining != 6 {
		t.Fatalf("unexpected realtime counts for hourly exhausted claim: %+v", request)
	}
	if request.ReasonCode != queueBlockReasonHourlyQuotaExhausted {
		t.Fatalf("unexpected realtime reason code for hourly exhausted claim: %+v", request)
	}
}

func TestClaimTokensQueuesLargeDirectRequestToAvoidBlockingResponse(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4013", "queued-large-direct-user")
	for index := 0; index < directClaimSyncMaxCount+2; index++ {
		insertTestToken(
			t,
			store,
			fmt.Sprintf("bulk-%d.json", index),
			fmt.Sprintf(`{"access_token":"bulk-%d","refresh_token":"bulk-%d-r","account_id":"acct-bulk-%d"}`, index, index, index),
			0,
			1,
		)
	}

	result, err := service.ClaimTokens(ctx, userID, nil, directClaimSyncMaxCount+2)
	if err != nil {
		t.Fatalf("claim large direct batch: %v", err)
	}
	if !result.Queued {
		t.Fatalf("expected large batch request to be queued for async processing, got %+v", result)
	}
	if result.Requested != directClaimSyncMaxCount+2 {
		t.Fatalf("unexpected requested count in queued large batch: %+v", result)
	}
	if result.QueueID <= 0 {
		t.Fatalf("expected queued large batch to have queue id, got %+v", result)
	}
}

func TestClaimTokensQueuesLargeOriginalRequestEvenWhenQuotaClampLowersTarget(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "4014", "queued-large-clamped-user")
	seedTokenID := insertTestToken(
		t,
		store,
		"clamped-seed.json",
		`{"access_token":"clamped-seed","refresh_token":"clamped-seed-r","account_id":"acct-clamped-seed"}`,
		0,
		100,
	)
	for index := 0; index < 4; index++ {
		insertTestToken(
			t,
			store,
			fmt.Sprintf("clamped-%d.json", index),
			fmt.Sprintf(`{"access_token":"clamped-%d","refresh_token":"clamped-%d-r","account_id":"acct-clamped-%d"}`, index, index, index),
			0,
			1,
		)
	}

	for index := 0; index < 26; index++ {
		requestID := fmt.Sprintf("seed-claim-%d", index)
		insertTestClaimRecord(t, store, seedTokenID, userID, nil, time.Now().Add(-5*time.Minute).Unix(), requestID)
	}

	result, err := service.ClaimTokens(ctx, userID, nil, 5)
	if err != nil {
		t.Fatalf("claim clamped large batch: %v", err)
	}
	if !result.Queued {
		t.Fatalf("expected clamped large request to be queued, got %+v", result)
	}
	if result.Requested != 4 {
		t.Fatalf("expected queued request to honor remaining quota 4, got %+v", result)
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
	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue for terminal ledger rebuild: %v", err)
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
		t.Fatalf("expected accepted request to persist as succeeded after queue advance, got %q", status)
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
