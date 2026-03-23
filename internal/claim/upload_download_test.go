package claim

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"token-atlas/internal/auth"
	"token-atlas/internal/config"
	proberuntime "token-atlas/internal/probe"
	"token-atlas/internal/runtimecache"

	_ "modernc.org/sqlite"
)

func TestQueueUploadBatchAcceptsAndPreservesExtraFields(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	userID := insertTestUser(t, store, "2001", "uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "2001",
			Username:   "uploader",
			Name:       "Uploader",
			TrustLevel: 2,
		},
	}

	rawJSON := `{"account_id":"acct-upload","access_token":"token-upload","refresh_token":"refresh-upload","extra":{"region":"hk"}}`
	files := []uploadFileInput{{
		Name:          "upload.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(rawJSON)),
	}}

	result, err := service.QueueUploadBatch(context.Background(), requestContext, files)
	if err != nil {
		t.Fatalf("queue upload batch: %v", err)
	}

	summary := result["summary"].(map[string]int)
	if summary["queued"] != 1 {
		t.Fatalf("expected initial queued summary, got %+v", summary)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		payload := service.GetUploadResults(userID)
		currentSummary := payload["summary"].(map[string]int)
		if currentSummary["accepted"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("upload worker did not accept task in time: %+v", currentSummary)
		}
		time.Sleep(20 * time.Millisecond)
	}

	var contentJSON string
	if err := store.DB().QueryRow(`SELECT content_json FROM tokens WHERE account_id = 'acct-upload'`).Scan(&contentJSON); err != nil {
		t.Fatalf("query uploaded token: %v", err)
	}

	var payload map[string]any
	if err := json.Unmarshal([]byte(contentJSON), &payload); err != nil {
		t.Fatalf("decode stored upload content: %v", err)
	}
	if payload["account_id"] != "acct-upload" {
		t.Fatalf("unexpected stored account_id: %#v", payload["account_id"])
	}
	extra, ok := payload["extra"].(map[string]any)
	if !ok || extra["region"] != "hk" {
		t.Fatalf("expected extra field to be preserved, got %#v", payload["extra"])
	}
}

func TestUploadWaitsForDatabaseAvailabilityBeforeAccepting(t *testing.T) {
	service, store := newClaimTestService(t)
	probe := newBlockingProbe()
	service.probe = probe

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	userID := insertTestUser(t, store, "20011", "db-wait-uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "20011",
			Username:   "db-wait-uploader",
			Name:       "DB Wait Uploader",
			TrustLevel: 2,
		},
	}

	files := []uploadFileInput{{
		Name:          "db-wait.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(`{"account_id":"acct-db-wait","access_token":"token-db-wait","refresh_token":"refresh-db-wait"}`)),
	}}

	result, err := service.QueueUploadBatch(context.Background(), requestContext, files)
	if err != nil {
		t.Fatalf("queue upload batch: %v", err)
	}
	if got := result["summary"].(map[string]int)["queued"]; got != 1 {
		t.Fatalf("expected initial queued summary, got %+v", result["summary"])
	}

	probe.waitUntilStarted(t)

	releaseLock := holdSQLiteWriteLock(t, store.DB())
	lockReleased := false
	defer func() {
		if !lockReleased {
			releaseLock()
		}
	}()

	probe.release()

	waitingItem := waitForUploadItem(t, service, userID, 15*time.Second, func(item map[string]any) bool {
		return item["status"] == "processing" && item["stage"] == "waiting_db"
	})
	if waitingItem["stage_label"] != "等待入库" {
		t.Fatalf("expected waiting_db stage label, got %#v", waitingItem)
	}

	waitingSummary := service.GetUploadResults(userID)["summary"].(map[string]int)
	if waitingSummary["processing"] != 1 {
		t.Fatalf("expected processing summary while database is locked, got %+v", waitingSummary)
	}
	if waitingSummary["db_busy"] != 0 {
		t.Fatalf("expected db_busy summary to remain 0 while task waits, got %+v", waitingSummary)
	}

	entries, err := os.ReadDir(service.cfg.Files.TokenDir)
	if err != nil {
		t.Fatalf("read token dir while waiting: %v", err)
	}
	tempCount := 0
	finalCount := 0
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, ".uploading"):
			tempCount++
		case strings.HasSuffix(name, ".json"):
			finalCount++
		}
	}
	if tempCount != 1 || finalCount != 0 {
		t.Fatalf("expected exactly one temp upload file and no final json while waiting, got temp=%d final=%d entries=%v", tempCount, finalCount, entries)
	}

	releaseLock()
	lockReleased = true

	acceptedItem := waitForUploadItem(t, service, userID, 15*time.Second, func(item map[string]any) bool {
		return item["status"] == "accepted"
	})
	if acceptedItem["stage"] != "accepted" {
		t.Fatalf("expected accepted upload item after lock release, got %#v", acceptedItem)
	}

	var storedCount int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM tokens WHERE account_id = 'acct-db-wait'`).Scan(&storedCount); err != nil {
		t.Fatalf("query stored upload count: %v", err)
	}
	if storedCount != 1 {
		t.Fatalf("expected uploaded token to be stored exactly once, got %d", storedCount)
	}

	entries, err = os.ReadDir(service.cfg.Files.TokenDir)
	if err != nil {
		t.Fatalf("read token dir after accept: %v", err)
	}
	tempCount = 0
	finalPath := ""
	for _, entry := range entries {
		name := entry.Name()
		switch {
		case strings.HasSuffix(name, ".uploading"):
			tempCount++
		case strings.HasSuffix(name, ".json"):
			finalPath = filepath.Join(service.cfg.Files.TokenDir, name)
		}
	}
	if tempCount != 0 {
		t.Fatalf("expected temp upload files to be cleaned after accept, got entries=%v", entries)
	}
	if finalPath == "" {
		t.Fatalf("expected promoted final upload file after accept, got entries=%v", entries)
	}
	if _, err := os.Stat(finalPath); err != nil {
		t.Fatalf("stat promoted upload file: %v", err)
	}
}

func TestUploadMarksDatabaseBusyWhenStoreWindowExpires(t *testing.T) {
	service, store := newClaimTestService(t)
	probe := newBlockingProbe()
	service.probe = probe

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
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

	userID := insertTestUser(t, store, "20012", "db-timeout-uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "20012",
			Username:   "db-timeout-uploader",
			Name:       "DB Timeout Uploader",
			TrustLevel: 2,
		},
	}

	files := []uploadFileInput{{
		Name:          "db-timeout.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(`{"account_id":"acct-db-timeout","access_token":"token-db-timeout","refresh_token":"refresh-db-timeout"}`)),
	}}

	if _, err := service.QueueUploadBatch(context.Background(), requestContext, files); err != nil {
		t.Fatalf("queue upload batch: %v", err)
	}

	probe.waitUntilStarted(t)

	releaseLock := holdSQLiteWriteLock(t, store.DB())
	defer releaseLock()
	probe.release()

	busyItem := waitForUploadItem(t, service, userID, 15*time.Second, func(item map[string]any) bool {
		return item["status"] == "db_busy"
	})
	if busyItem["stage"] != "waiting_db" {
		t.Fatalf("expected db_busy item to stay on waiting_db stage, got %#v", busyItem)
	}

	summary := service.GetUploadResults(userID)["summary"].(map[string]int)
	if summary["db_busy"] != 1 {
		t.Fatalf("expected db_busy summary to be 1 after timeout, got %+v", summary)
	}

	var storedCount int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM tokens WHERE account_id = 'acct-db-timeout'`).Scan(&storedCount); err != nil {
		t.Fatalf("query stored upload count after db busy timeout: %v", err)
	}
	if storedCount != 0 {
		t.Fatalf("expected no token record after db busy timeout, got %d", storedCount)
	}

	entries, err := os.ReadDir(service.cfg.Files.TokenDir)
	if err != nil {
		t.Fatalf("read token dir after db busy timeout: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".uploading") {
			t.Fatalf("expected temp upload file cleanup after db busy timeout, got entries=%v", entries)
		}
	}
}

func TestUploadPromoteFailureRollsBackRuntimeAndInvalidatesInventory(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	service.promoteTokenFile = func(string, string) error {
		return errors.New("disk full")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
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

	userID := insertTestUser(t, store, "20013", "promote-fail-uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "20013",
			Username:   "promote-fail-uploader",
			Name:       "Promote Fail Uploader",
			TrustLevel: 2,
		},
	}

	inventoryVersionBefore := service.cacheScopeVersion("inventory")

	files := []uploadFileInput{{
		Name:          "promote-fail.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(`{"account_id":"acct-promote-fail","access_token":"token-promote-fail","refresh_token":"refresh-promote-fail"}`)),
	}}

	if _, err := service.QueueUploadBatch(context.Background(), requestContext, files); err != nil {
		t.Fatalf("queue upload batch: %v", err)
	}

	failedItem := waitForUploadItem(t, service, userID, 5*time.Second, func(item map[string]any) bool {
		return item["status"] == "probe_failed" && item["stage"] == "processing_failed"
	})
	if failedItem["reason"] != "库存文件落盘失败，请稍后重试" {
		t.Fatalf("unexpected failed upload item: %#v", failedItem)
	}

	var storedCount int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM tokens WHERE account_id = 'acct-promote-fail'`).Scan(&storedCount); err != nil {
		t.Fatalf("query stored upload count after promote failure: %v", err)
	}
	if storedCount != 0 {
		t.Fatalf("expected token row to be rolled back after promote failure, got %d", storedCount)
	}

	var (
		status          string
		totalTokens     int
		availableTokens int
		unclaimedTokens int
	)
	if err := store.DB().QueryRow(`
		SELECT status, total_tokens, available_tokens, unclaimed_tokens
		FROM inventory_runtime
		WHERE id = 1
	`).Scan(&status, &totalTokens, &availableTokens, &unclaimedTokens); err != nil {
		t.Fatalf("query inventory runtime after promote failure: %v", err)
	}
	if totalTokens != 0 || availableTokens != 0 || unclaimedTokens != 0 {
		t.Fatalf("expected inventory runtime rollback after promote failure, got status=%q total=%d available=%d unclaimed=%d", status, totalTokens, availableTokens, unclaimedTokens)
	}

	if inventoryVersionAfter := service.cacheScopeVersion("inventory"); inventoryVersionAfter <= inventoryVersionBefore {
		t.Fatalf("expected inventory cache scope to be invalidated after promote rollback, before=%d after=%d", inventoryVersionBefore, inventoryVersionAfter)
	}

	entries, err := os.ReadDir(service.cfg.Files.TokenDir)
	if err != nil {
		t.Fatalf("read token dir after promote failure: %v", err)
	}
	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), ".json") || strings.HasSuffix(entry.Name(), ".uploading") {
			t.Fatalf("expected promote failure to leave no token files behind, got entries=%v", entries)
		}
	}
}

func TestDuplicateUploadSubmissionReceivesFinalStatusFromPendingTask(t *testing.T) {
	service, store := newClaimTestService(t)
	probe := newBlockingProbe()
	service.probe = probe

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	userID := insertTestUser(t, store, "2002", "duplicate-uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "2002",
			Username:   "duplicate-uploader",
			Name:       "Duplicate Uploader",
			TrustLevel: 2,
		},
	}

	rawJSON := `{"account_id":"acct-duplicate","access_token":"token-duplicate","refresh_token":"refresh-duplicate"}`
	files := []uploadFileInput{{
		Name:          "duplicate.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(rawJSON)),
	}}

	first, err := service.QueueUploadBatch(context.Background(), requestContext, files)
	if err != nil {
		t.Fatalf("queue first upload batch: %v", err)
	}
	if got := first["summary"].(map[string]int)["queued"]; got != 1 {
		t.Fatalf("expected first batch to queue 1 item, got %+v", first["summary"])
	}

	probe.waitUntilStarted(t)

	second, err := service.QueueUploadBatch(context.Background(), requestContext, files)
	if err != nil {
		t.Fatalf("queue duplicate upload batch: %v", err)
	}
	secondItems := second["items"].([]map[string]any)
	if got := secondItems[0]["status"]; got != "processing" && got != "queued" {
		t.Fatalf("expected duplicate batch to attach to pending task, got status=%#v item=%#v", got, secondItems[0])
	}

	probe.release()

	deadline := time.Now().Add(5 * time.Second)
	for {
		payload := service.GetUploadResults(userID)
		items := payload["items"].([]map[string]any)
		if len(items) == 1 && items[0]["status"] == "accepted" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("duplicate batch did not receive final accepted status: %#v", payload)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestQueueUploadBatchRefreshesQueuedPositionsWithinSameBatch(t *testing.T) {
	service, store := newClaimTestService(t)

	userID := insertTestUser(t, store, "2003", "queue-refresh")

	taskOne := uploadTask{
		BatchID:         "batch-queue-refresh",
		UserID:          userID,
		RequestIndex:    0,
		FileName:        "first.json",
		AccountID:       "acct-queue-1",
		AccessTokenHash: hashTokenValue("token-queue-1"),
	}
	taskTwo := uploadTask{
		BatchID:         "batch-queue-refresh",
		UserID:          userID,
		RequestIndex:    1,
		FileName:        "second.json",
		AccountID:       "acct-queue-2",
		AccessTokenHash: hashTokenValue("token-queue-2"),
	}

	if existing, _ := service.reserveUploadTask(taskOne); existing != nil {
		t.Fatalf("expected first reserved task to be unique, got %#v", existing)
	}
	if existing, _ := service.reserveUploadTask(taskTwo); existing != nil {
		t.Fatalf("expected second reserved task to be unique, got %#v", existing)
	}

	initialItems := service.refreshPendingUploadResultItems([]map[string]any{
		{
			"request_index": 0,
			"file_name":     "first.json",
			"account_id":    "acct-queue-1",
		},
		{
			"request_index": 1,
			"file_name":     "second.json",
			"account_id":    "acct-queue-2",
		},
	})
	service.setUploadSnapshot(userID, uploadSnapshot{
		BatchID:   taskOne.BatchID,
		CreatedAt: isoformatNow(),
		Items:     initialItems,
	})

	payload := service.GetUploadResults(userID)
	items := payload["items"].([]map[string]any)
	if got := intFromAny(items[0]["queue_position"]); got != 1 {
		t.Fatalf("expected first item queue position 1, got %#v", items[0])
	}
	if got := intFromAny(items[0]["queue_total"]); got != 2 {
		t.Fatalf("expected first item queue total 2, got %#v", items[0])
	}
	if got := intFromAny(items[1]["queue_position"]); got != 2 {
		t.Fatalf("expected second item queue position 2, got %#v", items[1])
	}
	if got := intFromAny(items[1]["queue_total"]); got != 2 {
		t.Fatalf("expected second item queue total 2, got %#v", items[1])
	}

	service.markUploadTaskProcessing(taskOne)

	payload = service.GetUploadResults(userID)
	items = payload["items"].([]map[string]any)
	if got := items[0]["status"]; got != "processing" {
		t.Fatalf("expected first item to enter processing, got %#v", items[0])
	}
	if got := items[1]["status"]; got != "queued" {
		t.Fatalf("expected second item to remain queued, got %#v", items[1])
	}
	if got := intFromAny(items[1]["queue_position"]); got != 1 {
		t.Fatalf("expected second item queue position to refresh to 1, got %#v", items[1])
	}
	if got := intFromAny(items[1]["queue_total"]); got != 1 {
		t.Fatalf("expected second item queue total to refresh to 1, got %#v", items[1])
	}
}

func TestUploadProgressIncludesTimelineEvents(t *testing.T) {
	service, store := newClaimTestService(t)
	probe := newBlockingProbe()
	service.probe = probe

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service.Start(ctx)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	userID := insertTestUser(t, store, "2004", "timeline-uploader")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "2004",
			Username:   "timeline-uploader",
			Name:       "Timeline Uploader",
			TrustLevel: 2,
		},
	}

	files := []uploadFileInput{{
		Name:          "timeline.json",
		ContentBase64: base64.StdEncoding.EncodeToString([]byte(`{"account_id":"acct-timeline","access_token":"token-timeline","refresh_token":"refresh-timeline"}`)),
	}}

	if _, err := service.QueueUploadBatch(context.Background(), requestContext, files); err != nil {
		t.Fatalf("queue upload batch: %v", err)
	}

	probe.waitUntilStarted(t)

	payload := service.GetUploadResults(userID)
	items := payload["items"].([]map[string]any)
	if len(items) != 1 {
		t.Fatalf("expected 1 upload item, got %#v", payload)
	}
	item := items[0]
	if got := item["stage"]; got != "probing" {
		t.Fatalf("expected stage probing while probe is running, got %#v", item)
	}
	events, ok := item["events"].([]map[string]any)
	if !ok || len(events) < 3 {
		t.Fatalf("expected upload timeline events, got %#v", item["events"])
	}
	last := events[len(events)-1]
	if got := last["label"]; got != "开始测试" {
		t.Fatalf("expected latest timeline event to be 开始测试, got %#v", last)
	}

	probe.release()
}

func TestClaimDownloadAccessSummaryHonorsHiddenAndOtherVisibleClaims(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userOneID := insertTestUser(t, store, "3001", "first")
	userTwoID := insertTestUser(t, store, "3002", "second")

	insertTestToken(t, store, "token-visible.json", `{"access_token":"visible","refresh_token":"visible-r","account_id":"acct-visible"}`, 0, 1)
	insertTestToken(t, store, "token-hidden.json", `{"access_token":"hidden","refresh_token":"hidden-r","account_id":"acct-hidden"}`, 0, 1)

	visibleResult, err := service.ClaimTokens(ctx, userOneID, nil, 1)
	if err != nil {
		t.Fatalf("claim visible token: %v", err)
	}
	hiddenResult, err := service.ClaimTokens(ctx, userOneID, nil, 1)
	if err != nil {
		t.Fatalf("claim hidden token: %v", err)
	}

	if err := service.HideClaims(ctx, userOneID, []int64{hiddenResult.Items[0].ClaimID}); err != nil {
		t.Fatalf("hide claim: %v", err)
	}

	hiddenAccess, err := service.GetClaimDownloadAccessSummary(ctx, hiddenResult.Items[0].TokenID, userOneID)
	if err != nil {
		t.Fatalf("get hidden access summary: %v", err)
	}
	if !hiddenAccess.UserHiddenClaim || hiddenAccess.OtherVisibleClaim {
		t.Fatalf("unexpected hidden access summary: %+v", hiddenAccess)
	}
	hiddenItem, err := service.GetClaimedTokenForDownload(ctx, hiddenResult.Items[0].TokenID, userOneID)
	if err != nil {
		t.Fatalf("get hidden claimed token: %v", err)
	}
	if hiddenItem != nil {
		t.Fatalf("expected hidden claimed token to be unavailable, got %+v", hiddenItem)
	}

	otherVisibleAccess, err := service.GetClaimDownloadAccessSummary(ctx, visibleResult.Items[0].TokenID, userTwoID)
	if err != nil {
		t.Fatalf("get other visible access summary: %v", err)
	}
	if otherVisibleAccess.UserHiddenClaim || !otherVisibleAccess.OtherVisibleClaim {
		t.Fatalf("unexpected other visible access summary: %+v", otherVisibleAccess)
	}
	otherVisibleItem, err := service.GetClaimedTokenForDownload(ctx, visibleResult.Items[0].TokenID, userTwoID)
	if err != nil {
		t.Fatalf("get other user claimed token: %v", err)
	}
	if otherVisibleItem != nil {
		t.Fatalf("expected other user's claimed token to be unavailable, got %+v", otherVisibleItem)
	}
}

func pushTempWorkingDir(t *testing.T) func() {
	t.Helper()

	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}
	tempDir := filepath.Join(t.TempDir(), "workspace")
	if err := os.MkdirAll(tempDir, 0o755); err != nil {
		t.Fatalf("create temp working directory: %v", err)
	}
	if err := os.Chdir(tempDir); err != nil {
		t.Fatalf("chdir to temp working directory: %v", err)
	}

	return func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Fatalf("restore working directory: %v", err)
		}
	}
}

func waitForUploadItem(t *testing.T, service *Service, userID int64, timeout time.Duration, predicate func(map[string]any) bool) map[string]any {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		payload := service.GetUploadResults(userID)
		items, _ := payload["items"].([]map[string]any)
		for _, item := range items {
			if predicate(item) {
				return item
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("upload item did not reach expected state in time: %#v", payload)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func holdSQLiteWriteLock(t *testing.T, db *sql.DB) func() {
	t.Helper()

	dbPath := sqliteDatabasePath(t, db)
	lockerDB, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open sqlite locker db: %v", err)
	}
	lockerDB.SetMaxOpenConns(1)
	lockerDB.SetMaxIdleConns(1)

	conn, err := lockerDB.Conn(context.Background())
	if err != nil {
		_ = lockerDB.Close()
		t.Fatalf("open sqlite locker connection: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `PRAGMA busy_timeout = 50`); err != nil {
		_ = conn.Close()
		_ = lockerDB.Close()
		t.Fatalf("set sqlite locker busy_timeout: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `PRAGMA journal_mode = WAL`); err != nil {
		_ = conn.Close()
		_ = lockerDB.Close()
		t.Fatalf("set sqlite locker journal mode: %v", err)
	}
	if _, err := conn.ExecContext(context.Background(), `BEGIN IMMEDIATE`); err != nil {
		_ = conn.Close()
		_ = lockerDB.Close()
		t.Fatalf("begin immediate sqlite locker transaction: %v", err)
	}

	return func() {
		if _, err := conn.ExecContext(context.Background(), `ROLLBACK`); err != nil && !strings.Contains(err.Error(), "no transaction is active") {
			t.Fatalf("rollback sqlite locker transaction: %v", err)
		}
		if err := conn.Close(); err != nil {
			t.Fatalf("close sqlite locker connection: %v", err)
		}
		if err := lockerDB.Close(); err != nil {
			t.Fatalf("close sqlite locker db: %v", err)
		}
	}
}

func sqliteDatabasePath(t *testing.T, db *sql.DB) string {
	t.Helper()

	rows, err := db.Query(`PRAGMA database_list`)
	if err != nil {
		t.Fatalf("query sqlite database_list: %v", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			seq  int
			name string
			path string
		)
		if err := rows.Scan(&seq, &name, &path); err != nil {
			t.Fatalf("scan sqlite database_list row: %v", err)
		}
		if name == "main" && strings.TrimSpace(path) != "" {
			return path
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sqlite database_list rows: %v", err)
	}
	t.Fatal("sqlite main database path not found")
	return ""
}

type blockingProbe struct {
	started   chan struct{}
	releaseCh chan struct{}
}

func newBlockingProbe() *blockingProbe {
	return &blockingProbe{
		started:   make(chan struct{}, 1),
		releaseCh: make(chan struct{}),
	}
}

func (p *blockingProbe) Start() {}

func (p *blockingProbe) Stop() {
	select {
	case <-p.releaseCh:
	default:
		close(p.releaseCh)
	}
}

func (p *blockingProbe) Submit(map[string]any, float64) proberuntime.Result {
	select {
	case p.started <- struct{}{}:
	default:
	}
	<-p.releaseCh
	return proberuntime.Result{Status: "ok"}
}

func (p *blockingProbe) waitUntilStarted(t *testing.T) {
	t.Helper()
	select {
	case <-p.started:
	case <-time.After(2 * time.Second):
		t.Fatalf("probe did not start in time")
	}
}

func (p *blockingProbe) release() {
	select {
	case <-p.releaseCh:
	default:
		close(p.releaseCh)
	}
}
