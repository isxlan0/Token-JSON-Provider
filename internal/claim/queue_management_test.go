package claim

import (
	"context"
	"testing"
	"time"

	"token-atlas/internal/database"
)

func TestAdvanceQueueCancelsInvalidRowAndContinues(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	validUserID := insertTestUser(t, store, "3001", "valid-user")
	insertTestToken(t, store, "queue-continue.json", `{"access_token":"queue-continue","refresh_token":"queue-continue-r","account_id":"acct-queue-continue"}`, 0, 1)

	insertTestQueuedEntry(t, store, 9999, nil, 1, 1, 1, "invalid-row", time.Now().Add(-2*time.Minute).Unix())
	insertTestQueuedEntry(t, store, validUserID, nil, 1, 1, 2, "valid-row", time.Now().Add(-time.Minute).Unix())

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue: %v", err)
	}

	var (
		status       string
		cancelReason string
	)
	if err := store.DB().QueryRow(`
		SELECT status, cancel_reason
		FROM claim_queue
		WHERE request_id = 'invalid-row'
	`).Scan(&status, &cancelReason); err != nil {
		t.Fatalf("load invalid queue row: %v", err)
	}
	if status != queueStatusCancelled || cancelReason != queueCancelReasonUserMissing {
		t.Fatalf("unexpected invalid queue row state: status=%q cancel_reason=%q", status, cancelReason)
	}

	queueStatus, err := service.GetQueueStatus(ctx, validUserID)
	if err != nil {
		t.Fatalf("get queue status: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected valid user queue to be drained, got %+v", queueStatus)
	}

	claims, err := service.ListClaims(ctx, validUserID)
	if err != nil {
		t.Fatalf("list claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("expected one fulfilled claim, got %d", len(claims))
	}
}

func TestRecordQueueFailureCancelsAfterThreshold(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "3002", "failure-user")
	queueID := insertTestQueuedEntry(t, store, userID, nil, 1, 1, 1, "failure-row", time.Now().Add(-time.Minute).Unix())

	entry, err := service.getUserQueueEntry(ctx, userID)
	if err != nil {
		t.Fatalf("load queue entry: %v", err)
	}
	if entry == nil {
		t.Fatal("expected active queue entry")
	}

	for attempt := 1; attempt <= queueFailureThreshold; attempt++ {
		result, err := service.recordQueueFailure(ctx, *entry, "probe exploded")
		if err != nil {
			t.Fatalf("record queue failure attempt %d: %v", attempt, err)
		}
		if !result.Changed {
			t.Fatalf("expected changed result on attempt %d", attempt)
		}
	}

	var (
		status          string
		cancelReason    string
		lastErrorReason string
		failureCount    int
		totalQueued     int
	)
	if err := store.DB().QueryRow(`
		SELECT status, cancel_reason, last_error_reason, failure_count
		FROM claim_queue
		WHERE id = ?
	`, queueID).Scan(&status, &cancelReason, &lastErrorReason, &failureCount); err != nil {
		t.Fatalf("load failed queue row: %v", err)
	}
	if status != queueStatusCancelled {
		t.Fatalf("unexpected queue status: %q", status)
	}
	if cancelReason != queueCancelReasonFailureThreshold {
		t.Fatalf("unexpected cancel reason: %q", cancelReason)
	}
	if lastErrorReason != "probe exploded" {
		t.Fatalf("unexpected last error reason: %q", lastErrorReason)
	}
	if failureCount != queueFailureThreshold {
		t.Fatalf("unexpected failure count: %d", failureCount)
	}
	if err := store.DB().QueryRow(`SELECT total_queued FROM queue_runtime WHERE id = 1`).Scan(&totalQueued); err != nil {
		t.Fatalf("load queue runtime: %v", err)
	}
	if totalQueued != 0 {
		t.Fatalf("expected queue runtime to be refreshed to 0, got %d", totalQueued)
	}
}

func TestListQueueForAdminFiltersTimeoutAndAbnormal(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	timeoutUserID := insertTestUser(t, store, "3003", "timeout-user")
	abnormalUserID := insertTestUser(t, store, "3004", "abnormal-user")

	timeoutQueueID := insertTestQueuedEntry(t, store, timeoutUserID, nil, 2, 2, 1, "timeout-row", time.Now().Add(-2*queueTimeoutWindow).Unix())
	insertTestQueuedEntry(t, store, abnormalUserID, nil, 1, 1, 2, "abnormal-row", time.Now().Add(-5*time.Minute).Unix())
	if _, err := store.DB().Exec(`
		UPDATE claim_queue
		SET failure_count = 2,
		    last_error_reason = 'network wobble',
		    last_error_at_ts = ?
		WHERE request_id = 'abnormal-row'
	`, time.Now().Add(-time.Minute).Unix()); err != nil {
		t.Fatalf("seed abnormal queue row: %v", err)
	}

	timeoutPayload, err := service.ListQueueForAdmin(ctx, "", "all", adminQueueOnlyTimeout, 50, 0)
	if err != nil {
		t.Fatalf("list timeout queue entries: %v", err)
	}
	timeoutItems := timeoutPayload["items"].([]map[string]any)
	if len(timeoutItems) != 1 {
		t.Fatalf("expected one timeout item, got %d", len(timeoutItems))
	}
	if got := int64(timeoutItems[0]["queue_id"].(int64)); got != timeoutQueueID {
		t.Fatalf("unexpected timeout queue id: %d", got)
	}
	if !timeoutItems[0]["is_timeout"].(bool) {
		t.Fatalf("expected timeout flag to be true: %+v", timeoutItems[0])
	}

	abnormalPayload, err := service.ListQueueForAdmin(ctx, "", "all", adminQueueOnlyAbnormal, 50, 0)
	if err != nil {
		t.Fatalf("list abnormal queue entries: %v", err)
	}
	abnormalItems := abnormalPayload["items"].([]map[string]any)
	if len(abnormalItems) != 1 {
		t.Fatalf("expected one abnormal item, got %d", len(abnormalItems))
	}
	if abnormalItems[0]["user_id"].(int64) != abnormalUserID {
		t.Fatalf("unexpected abnormal user id: %+v", abnormalItems[0])
	}
	if !abnormalItems[0]["is_abnormal"].(bool) {
		t.Fatalf("expected abnormal flag to be true: %+v", abnormalItems[0])
	}
}

func TestListQueueForAdminSkipsLegacyAndInvalidQueuedRows(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "3006", "legacy-queue-user")
	activeQueueID := insertTestQueuedEntry(t, store, userID, nil, 2, 2, 1, "active-row", time.Now().Add(-time.Minute).Unix())

	if _, err := store.DB().Exec(`
		INSERT INTO claim_queue (
			user_id,
			api_key_id,
			requested,
			remaining,
			queue_rank,
			enqueued_at_ts,
			request_id,
			status,
			cancel_reason,
			cancelled_at_ts
		) VALUES
			(?, NULL, 5, 0, 0, ?, 'invalid-queued-row', 'queued', NULL, NULL),
			(?, NULL, 1, 0, 0, ?, 'legacy-fulfilled-row', 'fulfilled', NULL, NULL),
			(?, NULL, 1, 1, 0, ?, 'cancelled-row', 'cancelled', 'admin:test', ?)
	`, userID, time.Now().Add(-2*time.Minute).Unix(), userID, time.Now().Add(-3*time.Minute).Unix(), userID, time.Now().Add(-4*time.Minute).Unix(), time.Now().Add(-2*time.Minute).Unix()); err != nil {
		t.Fatalf("seed legacy queue rows: %v", err)
	}

	payload, err := service.ListQueueForAdmin(ctx, "", "all", adminQueueOnlyAll, 50, 0)
	if err != nil {
		t.Fatalf("list all queue entries: %v", err)
	}
	items := payload["items"].([]map[string]any)
	if len(items) != 2 {
		t.Fatalf("expected two visible queue rows, got %d: %+v", len(items), items)
	}
	if payload["total"].(int) != 2 {
		t.Fatalf("expected total visible queue rows to be 2, got %+v", payload["total"])
	}
	if got := items[0]["queue_id"].(int64); got != activeQueueID {
		t.Fatalf("expected active queued row first, got queue id %d", got)
	}
	if items[0]["status"].(string) != queueStatusQueued {
		t.Fatalf("expected first row to be queued, got %+v", items[0])
	}
	if items[1]["status"].(string) != queueStatusCancelled {
		t.Fatalf("expected second row to be cancelled, got %+v", items[1])
	}
}

func TestStartupReconcileQueueClearsAllQueuedEntries(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	validUserID := insertTestUser(t, store, "3005", "startup-queue-user")
	insertTestToken(t, store, "startup-queue.json", `{"access_token":"startup-queue","refresh_token":"startup-queue-r","account_id":"acct-startup-queue"}`, 0, 1)

	insertTestQueuedEntry(t, store, 424242, nil, 1, 1, 1, "startup-invalid", time.Now().Add(-2*time.Minute).Unix())
	insertTestQueuedEntry(t, store, validUserID, nil, 1, 1, 2, "startup-valid", time.Now().Add(-time.Minute).Unix())
	insertTestQueuedEntry(t, store, validUserID, nil, 3, 0, 0, "startup-zero-remaining", time.Now().Add(-3*time.Minute).Unix())

	summary, err := service.startupReconcileQueue(ctx)
	if err != nil {
		t.Fatalf("startup reconcile queue: %v", err)
	}
	if summary["cancelled"] != 3 {
		t.Fatalf("expected all startup queue rows to be cancelled, got %+v", summary)
	}
	if summary["startup_reset"] != 3 {
		t.Fatalf("expected startup reset count to be 3, got %+v", summary)
	}
	if summary["queued_after"] != 0 {
		t.Fatalf("expected empty queue after startup clear, got %+v", summary)
	}

	var (
		status       string
		cancelReason string
		claimCount   int
	)
	if err := store.DB().QueryRow(`
		SELECT status, cancel_reason
		FROM claim_queue
		WHERE request_id = 'startup-invalid'
	`).Scan(&status, &cancelReason); err != nil {
		t.Fatalf("load startup invalid queue row: %v", err)
	}
	if status != queueStatusCancelled || cancelReason != queueCancelReasonStartupReset {
		t.Fatalf("unexpected startup invalid queue row state: status=%q cancel_reason=%q", status, cancelReason)
	}
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM token_claims WHERE user_id = ?`, validUserID).Scan(&claimCount); err != nil {
		t.Fatalf("count startup claims: %v", err)
	}
	if claimCount != 0 {
		t.Fatalf("expected startup clear to fulfill zero queued claims, got %d", claimCount)
	}
}

func insertTestQueuedEntry(t *testing.T, store *database.Store, userID int64, apiKeyID *int64, requested int, remaining int, queueRank int, requestID string, enqueuedAt int64) int64 {
	t.Helper()

	var apiKeyArg any
	if apiKeyID != nil {
		apiKeyArg = *apiKeyID
	}

	result, err := store.DB().Exec(`
		INSERT INTO claim_queue (
			user_id,
			api_key_id,
			requested,
			remaining,
			queue_rank,
			enqueued_at_ts,
			request_id,
			status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
	`, userID, apiKeyArg, requested, remaining, queueRank, enqueuedAt, requestID, queueStatusQueued)
	if err != nil {
		t.Fatalf("insert test queue row: %v", err)
	}

	queueID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted queue id: %v", err)
	}
	return queueID
}
