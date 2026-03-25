package claim

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"token-atlas/internal/auth"
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
	if status != queueStatusFailed {
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

func TestAdvanceQueueFinalizesHourlyQuotaAsPartial(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "30021", "hourly-block-user")
	queueID := insertTestQueuedEntry(t, store, userID, nil, 2, 2, 1, "hourly-block-row", time.Now().Add(-time.Minute).Unix())
	tokenID := insertTestToken(t, store, "hourly-block.json", `{"access_token":"hourly-block","refresh_token":"hourly-block-r","account_id":"acct-hourly-block"}`, 0, 1)
	earliestClaimTS := time.Now().Add(-30 * time.Minute).Unix()
	for index := 0; index < 30; index++ {
		insertTestClaimRecord(t, store, tokenID, userID, nil, earliestClaimTS+int64(index), "hourly-limit-seed")
	}

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue with hourly quota exhaustion: %v", err)
	}

	var (
		status       string
		blockReason  sql.NullString
		nextRetry    sql.NullInt64
		cancelReason sql.NullString
	)
	if err := store.DB().QueryRow(`
		SELECT status, block_reason, next_retry_at_ts, cancel_reason
		FROM claim_queue
		WHERE id = ?
	`, queueID).Scan(&status, &blockReason, &nextRetry, &cancelReason); err != nil {
		t.Fatalf("load hourly partial queue row: %v", err)
	}
	if status != queueStatusPartial {
		t.Fatalf("expected partial status, got %q", status)
	}
	if blockReason.Valid {
		t.Fatalf("expected block reason to be cleared, got %+v", blockReason)
	}
	if nextRetry.Valid {
		t.Fatalf("expected next retry to be cleared, got %+v", nextRetry)
	}
	if !cancelReason.Valid || cancelReason.String != queueBlockReasonHourlyQuotaExhausted {
		t.Fatalf("unexpected hourly terminal reason: %+v", cancelReason)
	}

	queueStatus, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get queue status after hourly terminalization: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected queue to be cleared after hourly exhaustion, got %+v", queueStatus)
	}
}

func TestAdvanceQueueFinalizesPartialAfterConsumingLastHourlyQuota(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	service.cfg.Inventory.Limits.Healthy.Hourly = 1
	service.cfg.Inventory.Limits.Warning.Hourly = 1
	service.cfg.Inventory.Limits.Critical.Hourly = 1

	userID := insertTestUser(t, store, "300211", "hourly-partial-user")
	queueID := insertTestQueuedEntry(t, store, userID, nil, 2, 2, 1, "hourly-partial-row", time.Now().Add(-time.Minute).Unix())
	insertTestToken(t, store, "hourly-partial.json", `{"access_token":"hourly-partial","refresh_token":"hourly-partial-r","account_id":"acct-hourly-partial"}`, 0, 1)

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue with remaining hourly quota: %v", err)
	}

	var (
		status       string
		remaining    int
		cancelReason sql.NullString
	)
	if err := store.DB().QueryRow(`
		SELECT status, remaining, cancel_reason
		FROM claim_queue
		WHERE id = ?
	`, queueID).Scan(&status, &remaining, &cancelReason); err != nil {
		t.Fatalf("load partial queue row: %v", err)
	}
	if status != queueStatusPartial {
		t.Fatalf("expected partial status after consuming last hourly quota, got %q", status)
	}
	if remaining != 1 {
		t.Fatalf("expected one request to remain unfulfilled, got %d", remaining)
	}
	if !cancelReason.Valid || cancelReason.String != queueBlockReasonHourlyQuotaExhausted {
		t.Fatalf("unexpected partial terminal reason: %+v", cancelReason)
	}

	queueStatus, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get queue status after partial completion: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected partial completion to leave no active queue row, got %+v", queueStatus)
	}

	var claimCount int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM token_claims WHERE user_id = ?`, userID).Scan(&claimCount); err != nil {
		t.Fatalf("count claims after partial completion: %v", err)
	}
	if claimCount != 1 {
		t.Fatalf("expected one successful grant before terminal partial, got %d", claimCount)
	}
}

func TestAdvanceQueueMarksAPIKeyRateLimitedBlocked(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()
	service.cfg.APIKeys.RatePerMinute = 2

	userID := insertTestUser(t, store, "30022", "api-key-block-user")
	apiKeyID := insertTestAPIKey(t, store, userID, "api-key-block")
	queueID := insertTestQueuedEntry(t, store, userID, &apiKeyID, 1, 1, 1, "api-key-block-row", time.Now().Add(-time.Minute).Unix())
	tokenID := insertTestToken(t, store, "api-key-block.json", `{"access_token":"api-key-block","refresh_token":"api-key-block-r","account_id":"acct-api-key-block"}`, 0, 1)
	earliestClaimTS := time.Now().Add(-40 * time.Second).Unix()
	for index := 0; index < service.cfg.APIKeys.RatePerMinute; index++ {
		insertTestClaimRecord(t, store, tokenID, userID, &apiKeyID, earliestClaimTS+int64(index), "api-key-limit-seed")
	}

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue with api key limit exhaustion: %v", err)
	}

	var (
		status      string
		blockReason sql.NullString
		nextRetry   sql.NullInt64
	)
	if err := store.DB().QueryRow(`
		SELECT status, block_reason, next_retry_at_ts
		FROM claim_queue
		WHERE id = ?
	`, queueID).Scan(&status, &blockReason, &nextRetry); err != nil {
		t.Fatalf("load api key blocked queue row: %v", err)
	}
	if status != queueStatusQueuedBlocked {
		t.Fatalf("expected queued_blocked status, got %q", status)
	}
	if !blockReason.Valid || blockReason.String != queueBlockReasonAPIKeyRateLimited {
		t.Fatalf("unexpected api key block reason: %+v", blockReason)
	}
	if !nextRetry.Valid || nextRetry.Int64 != earliestClaimTS+60 {
		t.Fatalf("unexpected api key next retry: %+v", nextRetry)
	}
}

func TestAdvanceQueueRetriesAfterProbeTimeoutWithoutMarkingBlocked(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "30023", "blocking-probe-user")
	queueID := insertTestQueuedEntry(t, store, userID, nil, 1, 1, 1, "blocking-probe-row", time.Now().Add(-time.Minute).Unix())
	insertTestToken(t, store, "blocking-probe.json", `{"access_token":"blocking-probe","refresh_token":"blocking-probe-r","account_id":"acct-blocking-probe"}`, 0, 1)

	probe := newBlockingProbe()
	service.probe = probe

	startedAt := time.Now()
	done := make(chan error, 1)
	go func() {
		done <- service.AdvanceQueue(ctx)
	}()

	probe.waitUntilStarted(t)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("advance queue with blocking probe: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("advance queue did not return within row timeout budget")
	}
	probe.release()

	if elapsed := time.Since(startedAt); elapsed > 5*time.Second {
		t.Fatalf("expected blocking row to time out quickly, got %v", elapsed)
	}

	var (
		status      string
		blockReason sql.NullString
		nextRetry   sql.NullInt64
	)
	if err := store.DB().QueryRow(`
		SELECT status, block_reason, next_retry_at_ts
		FROM claim_queue
		WHERE id = ?
	`, queueID).Scan(&status, &blockReason, &nextRetry); err != nil {
		t.Fatalf("load blocking queue row: %v", err)
	}
	if status != queueStatusQueuedWaiting {
		t.Fatalf("expected blocking queue row to stay waiting, got %q", status)
	}
	if blockReason.Valid {
		t.Fatalf("expected probe timeout retry to avoid blocked reason, got %+v", blockReason)
	}
	if !nextRetry.Valid || nextRetry.Int64 < startedAt.Unix() {
		t.Fatalf("expected short retry timestamp after probe timeout, got %+v", nextRetry)
	}
}

func TestAdvanceQueueReassessesInventoryRetryWindowWhenTokensBecomeAvailable(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "300231", "inventory-retry-user")
	queueID := insertTestQueuedEntry(t, store, userID, nil, 1, 1, 1, "inventory-retry-row", time.Now().Add(-time.Minute).Unix())
	nextRetryTS := time.Now().Add(10 * time.Minute).Unix()
	if _, err := store.DB().Exec(`
		UPDATE claim_queue
		SET status = ?,
		    block_reason = ?,
		    next_retry_at_ts = ?
		WHERE id = ?
	`, queueStatusQueuedBlocked, queueBlockReasonProbeLocksPending, nextRetryTS, queueID); err != nil {
		t.Fatalf("seed inventory retry queue row: %v", err)
	}

	insertTestToken(t, store, "inventory-retry-token.json", `{"access_token":"inventory-retry","refresh_token":"inventory-retry-r","account_id":"acct-inventory-retry"}`, 0, 1)

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue with stale inventory retry: %v", err)
	}

	queueStatus, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get queue status after inventory retry advance: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected stale inventory retry window to be reassessed, got %+v", queueStatus)
	}

	claims, err := service.ListClaims(ctx, userID)
	if err != nil {
		t.Fatalf("list claims after inventory retry advance: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("expected one fulfilled claim after inventory retry advance, got %d", len(claims))
	}
}

func TestAdvanceQueueConvertsHourlyRetryWindowToTerminalPartial(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "300232", "quota-retry-user")
	queueID := insertTestQueuedEntry(t, store, userID, nil, 1, 1, 1, "quota-retry-row", time.Now().Add(-time.Minute).Unix())
	tokenID := insertTestToken(t, store, "quota-retry-token.json", `{"access_token":"quota-retry","refresh_token":"quota-retry-r","account_id":"acct-quota-retry"}`, 0, 1)
	earliestClaimTS := time.Now().Add(-30 * time.Minute).Unix()
	for index := 0; index < service.cfg.Inventory.Limits.Healthy.Hourly; index++ {
		insertTestClaimRecord(t, store, tokenID, userID, nil, earliestClaimTS+int64(index), "quota-retry-seed")
	}
	nextRetryTS := time.Now().Add(10 * time.Minute).Unix()
	if _, err := store.DB().Exec(`
		UPDATE claim_queue
		SET status = ?,
		    block_reason = ?,
		    next_retry_at_ts = ?
		WHERE id = ?
	`, queueStatusQueuedBlocked, queueBlockReasonHourlyQuotaExhausted, nextRetryTS, queueID); err != nil {
		t.Fatalf("seed quota retry queue row: %v", err)
	}

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue with quota retry window: %v", err)
	}

	queueStatus, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get queue status after quota retry advance: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected hourly retry window to be closed immediately, got %+v", queueStatus)
	}

	var claimCount int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM token_claims WHERE user_id = ?`, userID).Scan(&claimCount); err != nil {
		t.Fatalf("count claims after quota retry advance: %v", err)
	}
	if claimCount != service.cfg.Inventory.Limits.Healthy.Hourly {
		t.Fatalf("expected no additional claims after hourly retry terminalization, got %d", claimCount)
	}

	var (
		status       string
		cancelReason sql.NullString
	)
	if err := store.DB().QueryRow(`
		SELECT status, cancel_reason
		FROM claim_queue
		WHERE id = ?
	`, queueID).Scan(&status, &cancelReason); err != nil {
		t.Fatalf("load terminalized quota retry row: %v", err)
	}
	if status != queueStatusPartial {
		t.Fatalf("expected blocked quota retry row to become partial, got %q", status)
	}
	if !cancelReason.Valid || cancelReason.String != queueBlockReasonHourlyQuotaExhausted {
		t.Fatalf("unexpected quota retry terminal reason: %+v", cancelReason)
	}
}

func TestAdvanceQueueUsesDedicatedClaimProbeWhileUploadProbeIsBlocked(t *testing.T) {
	service, store := newClaimTestService(t)
	service.probe = nil
	service.claimProbe = alwaysOKProbe{}
	uploadProbe := newBlockingProbe()
	service.uploadProbe = uploadProbe

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		uploadProbe.release()
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()
		if err := service.Stop(stopCtx); err != nil {
			t.Fatalf("stop service: %v", err)
		}
	}()
	service.Start(ctx)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	uploaderID := insertTestUser(t, store, "30024", "blocked-upload-user")
	uploadRequestContext := &auth.RequestContext{
		UserID: uploaderID,
		User: auth.UserPayload{
			ID:         "30024",
			Username:   "blocked-upload-user",
			Name:       "blocked-upload-user",
			TrustLevel: 2,
		},
	}
	files := []uploadFileInput{{
		Name:          "blocked-upload.json",
		ContentBase64: "eyJhY2NvdW50X2lkIjoiYWNjdC1ibG9ja2VkLXVwbG9hZCIsImFjY2Vzc190b2tlbiI6InRva2VuLWJsb2NrZWQtdXBsb2FkIiwicmVmcmVzaF90b2tlbiI6InJlZnJlc2gtYmxvY2tlZC11cGxvYWQifQ==",
	}}
	if _, err := service.QueueUploadBatch(context.Background(), uploadRequestContext, files); err != nil {
		t.Fatalf("queue blocked upload batch: %v", err)
	}
	uploadProbe.waitUntilStarted(t)

	claimerID := insertTestUser(t, store, "30025", "claim-queue-user")
	queued, err := service.ClaimTokens(context.Background(), claimerID, nil, 1)
	if err != nil {
		t.Fatalf("queue claim request: %v", err)
	}
	if !queued.Queued {
		t.Fatalf("expected claim request to enter queue, got %+v", queued)
	}

	insertTestToken(t, store, "separate-claim-probe.json", `{"access_token":"separate-claim-probe","refresh_token":"separate-claim-probe-r","account_id":"acct-separate-claim-probe"}`, 0, 1)
	if err := service.AdvanceQueue(context.Background()); err != nil {
		t.Fatalf("advance queue with blocked upload probe: %v", err)
	}

	queueStatus, err := service.GetQueueStatus(context.Background(), claimerID)
	if err != nil {
		t.Fatalf("get queue status after claim advance: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected dedicated claim probe to drain queue despite blocked upload probe, got %+v", queueStatus)
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

func TestListQueueForAdminIncludesBlockedMetadata(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "30041", "blocked-admin-user")
	queueID := insertTestQueuedEntry(t, store, userID, nil, 3, 3, 1, "blocked-admin-row", time.Now().Add(-5*time.Minute).Unix())
	lastProgressTS := time.Now().Add(-3 * time.Minute).Unix()
	nextRetryTS := time.Now().Add(2 * time.Minute).Unix()
	if _, err := store.DB().Exec(`
		UPDATE claim_queue
		SET status = ?,
		    block_reason = ?,
		    last_progress_at_ts = ?,
		    next_retry_at_ts = ?
		WHERE id = ?
	`, queueStatusQueuedBlocked, queueBlockReasonNoEligibleTokens, lastProgressTS, nextRetryTS, queueID); err != nil {
		t.Fatalf("seed blocked admin queue row: %v", err)
	}

	payload, err := service.ListQueueForAdmin(ctx, "", queueStatusQueued, adminQueueOnlyAll, 50, 0)
	if err != nil {
		t.Fatalf("list blocked queue entries: %v", err)
	}
	items := payload["items"].([]map[string]any)
	if len(items) != 1 {
		t.Fatalf("expected one blocked queue item, got %d: %+v", len(items), items)
	}
	item := items[0]
	if item["status"].(string) != queueStatusQueuedBlocked {
		t.Fatalf("expected blocked queue status, got %+v", item)
	}
	if item["block_reason"].(string) != queueBlockReasonNoEligibleTokens {
		t.Fatalf("expected blocked queue reason, got %+v", item)
	}
	if item["last_progress_at"].(string) != isoformatFromTS(lastProgressTS) {
		t.Fatalf("expected last progress timestamp, got %+v", item)
	}
	if item["next_retry_at"].(string) != isoformatFromTS(nextRetryTS) {
		t.Fatalf("expected next retry timestamp, got %+v", item)
	}
	if item["queue_position"].(int) != 1 {
		t.Fatalf("expected blocked queue position to be preserved, got %+v", item)
	}
}

func TestCancelAllQueuedEntriesCancelsEveryActiveRow(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	firstUserID := insertTestUser(t, store, "30042", "cancel-all-user-1")
	secondUserID := insertTestUser(t, store, "30043", "cancel-all-user-2")
	firstQueueID := insertTestQueuedEntry(t, store, firstUserID, nil, 1, 1, 1, "cancel-all-row-1", time.Now().Add(-3*time.Minute).Unix())
	secondQueueID := insertTestQueuedEntry(t, store, secondUserID, nil, 2, 2, 2, "cancel-all-row-2", time.Now().Add(-2*time.Minute).Unix())
	if _, err := store.DB().Exec(`
		UPDATE claim_queue
		SET status = ?,
		    block_reason = ?,
		    next_retry_at_ts = ?
		WHERE id = ?
	`, queueStatusQueuedBlocked, queueBlockReasonNoEligibleTokens, time.Now().Add(time.Minute).Unix(), secondQueueID); err != nil {
		t.Fatalf("seed blocked queue row for cancel all: %v", err)
	}

	items, err := service.cancelAllQueuedEntries(ctx, queueStatusCancelled, "admin:test-cancel-all", nil)
	if err != nil {
		t.Fatalf("cancel all queued entries: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected two cancelled queue entries, got %d", len(items))
	}

	var (
		firstStatus  string
		firstReason  string
		secondStatus string
		secondReason string
		totalQueued  int
	)
	if err := store.DB().QueryRow(`
		SELECT status, cancel_reason
		FROM claim_queue
		WHERE id = ?
	`, firstQueueID).Scan(&firstStatus, &firstReason); err != nil {
		t.Fatalf("load first cancelled queue row: %v", err)
	}
	if err := store.DB().QueryRow(`
		SELECT status, cancel_reason
		FROM claim_queue
		WHERE id = ?
	`, secondQueueID).Scan(&secondStatus, &secondReason); err != nil {
		t.Fatalf("load second cancelled queue row: %v", err)
	}
	if firstStatus != queueStatusCancelled || secondStatus != queueStatusCancelled {
		t.Fatalf("expected both queue rows to be cancelled, got first=%q second=%q", firstStatus, secondStatus)
	}
	if firstReason != "admin:test-cancel-all" || secondReason != "admin:test-cancel-all" {
		t.Fatalf("unexpected cancel reasons after cancel all: first=%q second=%q", firstReason, secondReason)
	}
	if err := store.DB().QueryRow(`SELECT total_queued FROM queue_runtime WHERE id = 1`).Scan(&totalQueued); err != nil {
		t.Fatalf("load queue runtime after cancel all: %v", err)
	}
	if totalQueued != 0 {
		t.Fatalf("expected queue runtime to be empty after cancel all, got %d", totalQueued)
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
