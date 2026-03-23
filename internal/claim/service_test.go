package claim

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"token-atlas/internal/auth"
	"token-atlas/internal/config"
	"token-atlas/internal/database"
	proberuntime "token-atlas/internal/probe"
	"token-atlas/internal/runtimecache"
)

func TestClaimTokensDirectGrant(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "1001", "tester")
	insertTestToken(t, store, "token-a.json", `{"access_token":"token-a","refresh_token":"refresh-a","account_id":"acct-a"}`, 0, 1)

	result, err := service.ClaimTokens(ctx, userID, nil, 1)
	if err != nil {
		t.Fatalf("claim tokens: %v", err)
	}

	if result.Queued {
		t.Fatalf("expected direct grant, got queued result")
	}
	if result.Granted != 1 || len(result.Items) != 1 {
		t.Fatalf("unexpected grant result: %+v", result)
	}
	if result.Items[0].FileName != "token-a.json" {
		t.Fatalf("unexpected claimed file name: %q", result.Items[0].FileName)
	}

	var (
		claimCount  int
		isAvailable int
	)
	if err := store.DB().QueryRow(`SELECT claim_count, is_available FROM tokens WHERE file_name = 'token-a.json'`).Scan(&claimCount, &isAvailable); err != nil {
		t.Fatalf("query claimed token state: %v", err)
	}
	if claimCount != 1 || isAvailable != 0 {
		t.Fatalf("unexpected token state after claim: claim_count=%d is_available=%d", claimCount, isAvailable)
	}
}

func TestQueuedClaimCanBeFulfilledByQueuePump(t *testing.T) {
	service, store := newClaimTestService(t)
	ctx := context.Background()

	userID := insertTestUser(t, store, "1002", "queued-user")

	result, err := service.ClaimTokens(ctx, userID, nil, 1)
	if err != nil {
		t.Fatalf("queue empty inventory claim: %v", err)
	}
	if !result.Queued {
		t.Fatalf("expected queued result, got %+v", result)
	}

	insertTestToken(t, store, "token-b.json", `{"access_token":"token-b","refresh_token":"refresh-b","account_id":"acct-b"}`, 0, 1)

	if err := service.AdvanceQueue(ctx); err != nil {
		t.Fatalf("advance queue: %v", err)
	}

	queueStatus, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get queue status: %v", err)
	}
	if queueStatus.Queued {
		t.Fatalf("expected queue to be drained, got %+v", queueStatus)
	}

	claims, err := service.ListClaims(ctx, userID)
	if err != nil {
		t.Fatalf("list claims: %v", err)
	}
	if len(claims) != 1 {
		t.Fatalf("expected one queued claim to be fulfilled, got %d", len(claims))
	}
	if claims[0].FileName != "token-b.json" {
		t.Fatalf("unexpected queued claim file: %q", claims[0].FileName)
	}
}

func TestGetQueueStatusCachesMissAndReusesSnapshot(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10021", "queue-cache-user")
	insertTestQueuedEntry(t, store, userID, nil, 2, 2, 1, "queue-cache-row", time.Now().Add(-time.Minute).Unix())

	first, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get first queue status: %v", err)
	}
	if !first.Queued || first.QueueID <= 0 {
		t.Fatalf("expected queued snapshot on first read, got %+v", first)
	}

	if _, err := store.DB().Exec(`DELETE FROM claim_queue WHERE id = ?`, first.QueueID); err != nil {
		t.Fatalf("delete queue row after first snapshot: %v", err)
	}

	second, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get second queue status: %v", err)
	}
	if !second.Queued || second.QueueID != first.QueueID {
		t.Fatalf("expected second queue status to be served from cache, got first=%+v second=%+v", first, second)
	}
}

func TestServiceStopWaitsForWorkersToExit(t *testing.T) {
	service, _ := newClaimTestService(t)

	restoreWD := pushTempWorkingDir(t)
	defer restoreWD()

	ctx, cancel := context.WithCancel(context.Background())
	service.Start(ctx)

	cancel()

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()
	if err := service.Stop(stopCtx); err != nil {
		t.Fatalf("stop service: %v", err)
	}
}

func TestBootstrapCacheRefreshesWhenUploadSnapshotChanges(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "1003", "bootstrap-user")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "1003",
			Username:   "bootstrap-user",
			Name:       "bootstrap-user",
			TrustLevel: 2,
		},
	}

	first, err := service.GetBootstrap(ctx, requestContext)
	if err != nil {
		t.Fatalf("get first bootstrap: %v", err)
	}
	firstUpload, ok := first["upload_results"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected first upload payload: %#v", first["upload_results"])
	}
	if firstUpload["batch_id"] != nil {
		t.Fatalf("expected empty batch id before upload snapshot, got %#v", firstUpload["batch_id"])
	}

	service.setUploadSnapshot(userID, uploadSnapshot{
		BatchID:   "batch-1",
		CreatedAt: "2026-03-22T00:00:00Z",
		Items: []map[string]any{
			{
				"request_index": 1,
				"file_name":     "demo.json",
				"status":        "queued",
				"reason":        "等待处理",
			},
		},
		History:     []map[string]any{},
		QueueStatus: map[string]any{"queued": 1},
	})

	second, err := service.GetBootstrap(ctx, requestContext)
	if err != nil {
		t.Fatalf("get second bootstrap: %v", err)
	}
	secondUpload, ok := second["upload_results"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected second upload payload: %#v", second["upload_results"])
	}
	if secondUpload["batch_id"] != "batch-1" {
		t.Fatalf("expected refreshed upload batch id, got %#v", secondUpload["batch_id"])
	}
}

func TestRuntimeSnapshotCacheRefreshesWhenUploadSnapshotChanges(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "1004", "runtime-user")

	first, err := service.GetRuntimeSnapshot(ctx, userID)
	if err != nil {
		t.Fatalf("get first runtime snapshot: %v", err)
	}
	if first.UploadResults["batch_id"] != nil {
		t.Fatalf("expected empty upload batch id before upload snapshot, got %#v", first.UploadResults["batch_id"])
	}

	service.setUploadSnapshot(userID, uploadSnapshot{
		BatchID:   "runtime-batch-1",
		CreatedAt: "2026-03-22T00:00:00Z",
		Items: []map[string]any{
			{
				"request_index": 1,
				"file_name":     "runtime.json",
				"status":        "accepted",
				"reason":        "已入库",
			},
		},
		History:     []map[string]any{},
		QueueStatus: map[string]any{"queued": 0},
	})

	second, err := service.GetRuntimeSnapshot(ctx, userID)
	if err != nil {
		t.Fatalf("get second runtime snapshot: %v", err)
	}
	if second.UploadResults["batch_id"] != "runtime-batch-1" {
		t.Fatalf("expected refreshed runtime upload batch id, got %#v", second.UploadResults["batch_id"])
	}
}

func TestAdminBootstrapIncludesDefaultDatasets(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	adminUserID := insertTestUser(t, store, "9001", "admin-user")
	insertTestUser(t, store, "9002", "normal-user")
	insertTestToken(t, store, "admin-bootstrap.json", `{"access_token":"admin-bootstrap","refresh_token":"admin-bootstrap-r","account_id":"acct-admin-bootstrap"}`, 0, 1)

	payload, err := service.GetAdminBootstrap(ctx, &auth.RequestContext{
		UserID: adminUserID,
		User: auth.UserPayload{
			ID:         "9001",
			Username:   "admin-user",
			Name:       "Admin User",
			TrustLevel: 4,
			IsAdmin:    true,
		},
		IsAdmin: true,
	})
	if err != nil {
		t.Fatalf("get admin bootstrap: %v", err)
	}

	me, ok := payload["me"].(map[string]any)
	if !ok || me["user"] == nil {
		t.Fatalf("admin bootstrap missing me payload: %#v", payload["me"])
	}

	users, ok := payload["users"].(map[string]any)
	if !ok || users["limit"] != defaultAdminUsersLimit {
		t.Fatalf("admin bootstrap users payload unexpected: %#v", payload["users"])
	}

	bans, ok := payload["bans"].(map[string]any)
	if !ok || bans["limit"] != defaultAdminBansLimit {
		t.Fatalf("admin bootstrap bans payload unexpected: %#v", payload["bans"])
	}

	tokens, ok := payload["tokens"].(map[string]any)
	if !ok || tokens["limit"] != defaultAdminTokensLimit {
		t.Fatalf("admin bootstrap tokens payload unexpected: %#v", payload["tokens"])
	}

	queue, ok := payload["queue"].(map[string]any)
	if !ok || queue["limit"] != defaultAdminQueueLimit {
		t.Fatalf("admin bootstrap queue payload unexpected: %#v", payload["queue"])
	}

	policy, ok := payload["policy"].(map[string]any)
	if !ok || policy["system"] == nil {
		t.Fatalf("admin bootstrap policy payload unexpected: %#v", payload["policy"])
	}
}

func TestAdminQueueCacheUsesDedicatedScope(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10022", "admin-queue-cache-user")
	insertTestQueuedEntry(t, store, userID, nil, 1, 1, 1, "admin-queue-cache-row-1", time.Now().Add(-time.Minute).Unix())

	first, err := service.cachedAdminQueuePage(ctx, "", "queued", adminQueueOnlyAll, 50, 0)
	if err != nil {
		t.Fatalf("load first cached admin queue page: %v", err)
	}
	if intFromAny(first["total"]) != 1 {
		t.Fatalf("expected first admin queue total to be 1, got %+v", first)
	}

	insertTestQueuedEntry(t, store, userID, nil, 1, 1, 2, "admin-queue-cache-row-2", time.Now().Unix())
	service.invalidateAdminCache()

	second, err := service.cachedAdminQueuePage(ctx, "", "queued", adminQueueOnlyAll, 50, 0)
	if err != nil {
		t.Fatalf("load second cached admin queue page: %v", err)
	}
	if intFromAny(second["total"]) != 1 {
		t.Fatalf("expected generic admin invalidation to keep queue cache warm, got %+v", second)
	}

	service.invalidateAdminQueueCache()
	third, err := service.cachedAdminQueuePage(ctx, "", "queued", adminQueueOnlyAll, 50, 0)
	if err != nil {
		t.Fatalf("load refreshed admin queue page: %v", err)
	}
	if intFromAny(third["total"]) != 2 {
		t.Fatalf("expected queue-specific invalidation to refresh cache, got %+v", third)
	}
}

func newClaimTestService(t *testing.T) (*Service, *database.Store) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "claim-test.db")
	store, err := database.Open(dbPath)
	if err != nil {
		t.Fatalf("open test database: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init test database: %v", err)
	}

	cfg := config.Config{
		Inventory: config.InventoryConfig{
			Thresholds: config.ThresholdConfig{
				Healthy:  1,
				Warning:  1,
				Critical: 1,
			},
			Limits: config.InventoryLimitConfig{
				Healthy: config.InventoryStatusLimit{
					Hourly:    30,
					MaxClaims: 1,
				},
				Warning: config.InventoryStatusLimit{
					Hourly:    20,
					MaxClaims: 2,
				},
				Critical: config.InventoryStatusLimit{
					Hourly:    15,
					MaxClaims: 3,
				},
			},
			NonHealthyMaxClaimsScope: "all_unfinished",
		},
		APIKeys: config.APIKeyConfig{
			MaxPerUser:    5,
			RatePerMinute: 60,
		},
		LinuxDO: config.LinuxDOConfig{
			MinTrustLevel: 0,
		},
		Files: config.FilesConfig{
			TokenDir: filepath.Join(t.TempDir(), "tokens"),
		},
		Upload: config.UploadConfig{
			MaxFilesPerRequest: 10,
			MaxFileSizeBytes:   10 * 1024,
			MaxSuccessPerHour:  20,
		},
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	service := NewService(cfg, store, nil, nil, logger)
	service.probe = alwaysOKProbe{}
	return service, store
}

func insertTestUser(t *testing.T, store *database.Store, linuxDOUserID string, username string) int64 {
	t.Helper()

	result, err := store.DB().Exec(`
		INSERT INTO users (
			linuxdo_user_id,
			linuxdo_username,
			linuxdo_name,
			trust_level,
			created_at_ts,
			last_login_at_ts
		) VALUES (?, ?, ?, 2, 1000, 1000)
	`, linuxDOUserID, username, username)
	if err != nil {
		t.Fatalf("insert test user: %v", err)
	}

	userID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted user id: %v", err)
	}
	return userID
}

func insertTestToken(t *testing.T, store *database.Store, fileName string, contentJSON string, claimCount int, maxClaims int) int64 {
	t.Helper()

	result, err := store.DB().Exec(`
		INSERT INTO tokens (
			file_name,
			file_path,
			file_hash,
			encoding,
			content_json,
			account_id,
			access_token_hash,
			is_active,
			is_cleaned,
			is_enabled,
			is_banned,
			is_available,
			claim_count,
			max_claims,
			created_at_ts,
			updated_at_ts,
			last_seen_at_ts
		) VALUES (?, ?, ?, 'utf-8', ?, ?, ?, 1, 0, 1, 0, ?, ?, ?, 1000, 1000, 1000)
	`, fileName, "./token/"+fileName, "file-hash-"+fileName, contentJSON, "acct-"+fileName, "hash-"+fileName, boolToInt(claimCount < maxClaims), claimCount, maxClaims)
	if err != nil {
		t.Fatalf("insert test token: %v", err)
	}

	tokenID, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("read inserted token id: %v", err)
	}
	return tokenID
}

func boolToInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

type alwaysOKProbe struct{}

func (alwaysOKProbe) Start() {}

func (alwaysOKProbe) Stop() {}

func (alwaysOKProbe) Submit(map[string]any, float64) proberuntime.Result {
	return proberuntime.Result{Status: "ok"}
}
