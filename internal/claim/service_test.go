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

func TestGetQueueStatusUsesStaleSnapshotWhenRefreshTimesOut(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "100211", "queue-stale-user")
	insertTestQueuedEntry(t, store, userID, nil, 2, 2, 1, "queue-stale-row", time.Now().Add(-time.Minute).Unix())

	first, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get first queue status: %v", err)
	}
	if first.DataSource != dataSourceLive {
		t.Fatalf("expected first queue status to be live, got %+v", first)
	}

	service.invalidateUserQueueCache(userID)

	expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	second, err := service.GetQueueStatus(expiredCtx, userID)
	if err != nil {
		t.Fatalf("get stale queue status: %v", err)
	}
	if second.DataSource != dataSourceStale {
		t.Fatalf("expected stale queue status data source, got %+v", second)
	}
	if second.QueueID != first.QueueID {
		t.Fatalf("expected stale queue status to preserve queue row, first=%+v second=%+v", first, second)
	}
	if !second.Degraded || second.DegradedReason != degradedReasonReadTimeout {
		t.Fatalf("expected degraded stale queue status, got %+v", second)
	}
}

func TestGetQueueStatusDefersClaimableNowOnMainPath(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10023", "queue-deferred-user")
	insertTestToken(t, store, "queue-deferred.json", `{"access_token":"queue-deferred","refresh_token":"queue-deferred-r","account_id":"acct-queue-deferred"}`, 0, 1)

	payload, err := service.GetQueueStatus(ctx, userID)
	if err != nil {
		t.Fatalf("get queue status: %v", err)
	}
	if payload.ClaimableNow != nil {
		t.Fatalf("expected queue status main path to defer claimable_now, got %+v", payload)
	}
	if payload.ClaimableNowState != dataSourceUnavailable {
		t.Fatalf("expected deferred claimable_now state to be unavailable, got %+v", payload)
	}
	if payload.DegradedReason != degradedReasonClaimableDeferred {
		t.Fatalf("expected deferred degraded reason, got %+v", payload)
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

func TestDashboardSummaryCachesColdAndHotReads(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10032", "dashboard-cache-user")
	insertTestToken(t, store, "dashboard-cache.json", `{"access_token":"dashboard-cache","refresh_token":"dashboard-cache-r","account_id":"acct-dashboard-cache"}`, 0, 1)

	options := dashboardSummaryOptions{
		Window:                 defaultDashboardWindow,
		Bucket:                 defaultDashboardBucket,
		LeaderboardWindow:      "24h",
		LeaderboardLimit:       10,
		RecentLimit:            10,
		ContributorLimit:       10,
		RecentContributorLimit: 10,
	}

	first, err := service.GetDashboardSummaryWithOptions(ctx, userID, options)
	if err != nil {
		t.Fatalf("get cold dashboard summary: %v", err)
	}
	if first["data_source"] != dataSourceLive {
		t.Fatalf("expected cold dashboard summary to be live, got %#v", first["data_source"])
	}

	second, err := service.GetDashboardSummaryWithOptions(ctx, userID, options)
	if err != nil {
		t.Fatalf("get hot dashboard summary: %v", err)
	}
	if second["data_source"] != dataSourceCache {
		t.Fatalf("expected hot dashboard summary to be cache, got %#v", second["data_source"])
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

func TestGetQuotaUsageRefreshesWhenInventoryPolicyChanges(t *testing.T) {
	service, store := newClaimTestServiceFromEnv(t, claimQuotaRefreshTestEnvBody())
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10041", "quota-refresh-user")
	insertTestToken(t, store, "quota-refresh-a.json", `{"access_token":"quota-refresh-a","refresh_token":"quota-refresh-a-r","account_id":"acct-quota-refresh-a"}`, 0, 1)
	disabledTokenID := insertTestToken(t, store, "quota-refresh-b.json", `{"access_token":"quota-refresh-b","refresh_token":"quota-refresh-b-r","account_id":"acct-quota-refresh-b"}`, 0, 1)
	setTestTokenEnabledState(t, store, disabledTokenID, false)

	first, err := service.GetQuotaUsage(ctx, userID)
	if err != nil {
		t.Fatalf("get first quota usage: %v", err)
	}
	if first.Limit != service.cfg.Inventory.Limits.Warning.Hourly {
		t.Fatalf("expected warning quota limit %d, got %d", service.cfg.Inventory.Limits.Warning.Hourly, first.Limit)
	}

	if _, err := service.SetTokenEnabled(ctx, disabledTokenID, true); err != nil {
		t.Fatalf("enable second token: %v", err)
	}

	second, err := service.GetQuotaUsage(ctx, userID)
	if err != nil {
		t.Fatalf("get refreshed quota usage: %v", err)
	}
	if second.Limit != service.cfg.Inventory.Limits.Healthy.Hourly {
		t.Fatalf("expected healthy quota limit %d after inventory refresh, got %d", service.cfg.Inventory.Limits.Healthy.Hourly, second.Limit)
	}
	if second.Remaining != service.cfg.Inventory.Limits.Healthy.Hourly {
		t.Fatalf("expected healthy remaining quota %d after inventory refresh, got %d", service.cfg.Inventory.Limits.Healthy.Hourly, second.Remaining)
	}
}

func TestGetRuntimeSnapshotRefreshesQuotaWhenInventoryPolicyChanges(t *testing.T) {
	service, store := newClaimTestServiceFromEnv(t, claimQuotaRefreshTestEnvBody())
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10042", "runtime-refresh-user")
	insertTestToken(t, store, "runtime-refresh-a.json", `{"access_token":"runtime-refresh-a","refresh_token":"runtime-refresh-a-r","account_id":"acct-runtime-refresh-a"}`, 0, 1)
	disabledTokenID := insertTestToken(t, store, "runtime-refresh-b.json", `{"access_token":"runtime-refresh-b","refresh_token":"runtime-refresh-b-r","account_id":"acct-runtime-refresh-b"}`, 0, 1)
	setTestTokenEnabledState(t, store, disabledTokenID, false)

	first, err := service.GetRuntimeSnapshot(ctx, userID)
	if err != nil {
		t.Fatalf("get first runtime snapshot: %v", err)
	}
	if first.Quota.Limit != service.cfg.Inventory.Limits.Warning.Hourly {
		t.Fatalf("expected warning runtime quota limit %d, got %d", service.cfg.Inventory.Limits.Warning.Hourly, first.Quota.Limit)
	}

	if _, err := service.SetTokenEnabled(ctx, disabledTokenID, true); err != nil {
		t.Fatalf("enable second token: %v", err)
	}

	second, err := service.GetRuntimeSnapshot(ctx, userID)
	if err != nil {
		t.Fatalf("get refreshed runtime snapshot: %v", err)
	}
	if second.DataSource != dataSourceLive {
		t.Fatalf("expected refreshed runtime snapshot to be live after inventory change, got %q", second.DataSource)
	}
	if second.Quota.Limit != service.cfg.Inventory.Limits.Healthy.Hourly {
		t.Fatalf("expected healthy runtime quota limit %d after inventory refresh, got %d", service.cfg.Inventory.Limits.Healthy.Hourly, second.Quota.Limit)
	}
	if second.Quota.Remaining != service.cfg.Inventory.Limits.Healthy.Hourly {
		t.Fatalf("expected healthy runtime remaining quota %d after inventory refresh, got %d", service.cfg.Inventory.Limits.Healthy.Hourly, second.Quota.Remaining)
	}
}

func TestGetRuntimeSnapshotUsesStaleSnapshotWhenRefreshTimesOut(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	userID := insertTestUser(t, store, "100421", "runtime-stale-user")

	first, err := service.GetRuntimeSnapshot(context.Background(), userID)
	if err != nil {
		t.Fatalf("get first runtime snapshot: %v", err)
	}
	if first.DataSource != dataSourceLive {
		t.Fatalf("expected first runtime snapshot to be live, got %+v", first)
	}

	service.invalidateUserQuotaCache(userID)

	expiredCtx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()

	second, err := service.GetRuntimeSnapshot(expiredCtx, userID)
	if err != nil {
		t.Fatalf("get stale runtime snapshot: %v", err)
	}
	if second.DataSource != dataSourceStale {
		t.Fatalf("expected stale runtime snapshot data source, got %+v", second)
	}
	if second.GeneratedAt != first.GeneratedAt {
		t.Fatalf("expected stale runtime snapshot to preserve generated_at, first=%q second=%q", first.GeneratedAt, second.GeneratedAt)
	}
	if !second.Degraded || second.DegradedReason != degradedReasonReadTimeout {
		t.Fatalf("expected degraded stale runtime snapshot, got %+v", second)
	}
}

func TestInventoryCacheInvalidationDoesNotBustOtherUsersRuntimeSnapshot(t *testing.T) {
	service, store := newClaimTestServiceFromEnv(t, claimPolicyTestEnvBody())
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	claimerID := insertTestUser(t, store, "100422", "claimer")
	observerID := insertTestUser(t, store, "100423", "observer")
	insertTestToken(t, store, "inventory-bump.json", `{"access_token":"inventory-bump","refresh_token":"inventory-bump-r","account_id":"acct-inventory-bump"}`, 0, 1)
	if _, err := service.ensureInventoryPolicy(ctx, true); err != nil {
		t.Fatalf("prime inventory policy runtime: %v", err)
	}
	service.syncInventoryPolicyScope(ctx)

	first, err := service.GetRuntimeSnapshot(ctx, observerID)
	if err != nil {
		t.Fatalf("get observer runtime snapshot: %v", err)
	}
	if first.DataSource != dataSourceLive {
		t.Fatalf("expected first observer runtime snapshot to be live, got %+v", first)
	}

	runtimeKeyBefore := service.userRuntimeSnapshotCacheKey(observerID)
	quotaKeyBefore := service.userQuotaCacheKey(observerID)
	profileKeyBefore := service.userProfileCacheKey(observerID, false)

	if _, err := service.ClaimTokens(ctx, claimerID, nil, 1); err != nil {
		t.Fatalf("claim token to bump inventory: %v", err)
	}

	if runtimeKeyAfter := service.userRuntimeSnapshotCacheKey(observerID); runtimeKeyAfter != runtimeKeyBefore {
		t.Fatalf("expected observer runtime snapshot key to remain stable across inventory-only bump, before=%q after=%q", runtimeKeyBefore, runtimeKeyAfter)
	}
	if quotaKeyAfter := service.userQuotaCacheKey(observerID); quotaKeyAfter != quotaKeyBefore {
		t.Fatalf("expected observer quota key to remain stable across inventory-only bump, before=%q after=%q", quotaKeyBefore, quotaKeyAfter)
	}
	if profileKeyAfter := service.userProfileCacheKey(observerID, false); profileKeyAfter != profileKeyBefore {
		t.Fatalf("expected observer profile key to remain stable across inventory-only bump, before=%q after=%q", profileKeyBefore, profileKeyAfter)
	}

	second, err := service.GetRuntimeSnapshot(ctx, observerID)
	if err != nil {
		t.Fatalf("get observer runtime snapshot after inventory bump: %v", err)
	}
	if second.DataSource != dataSourceCache {
		t.Fatalf("expected observer runtime snapshot to stay hot in cache after another user's claim, got %+v", second)
	}
}

func TestGetProfileRefreshesQuotaWhenInventoryPolicyChanges(t *testing.T) {
	service, store := newClaimTestServiceFromEnv(t, claimQuotaRefreshTestEnvBody())
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10043", "profile-refresh-user")
	insertTestToken(t, store, "profile-refresh-a.json", `{"access_token":"profile-refresh-a","refresh_token":"profile-refresh-a-r","account_id":"acct-profile-refresh-a"}`, 0, 1)
	disabledTokenID := insertTestToken(t, store, "profile-refresh-b.json", `{"access_token":"profile-refresh-b","refresh_token":"profile-refresh-b-r","account_id":"acct-profile-refresh-b"}`, 0, 1)
	setTestTokenEnabledState(t, store, disabledTokenID, false)

	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "10043",
			Username:   "profile-refresh-user",
			Name:       "profile-refresh-user",
			TrustLevel: 2,
		},
	}

	first, err := service.GetProfile(ctx, requestContext)
	if err != nil {
		t.Fatalf("get first profile: %v", err)
	}
	if first.Quota.Limit != service.cfg.Inventory.Limits.Warning.Hourly {
		t.Fatalf("expected warning profile quota limit %d, got %d", service.cfg.Inventory.Limits.Warning.Hourly, first.Quota.Limit)
	}

	if _, err := service.SetTokenEnabled(ctx, disabledTokenID, true); err != nil {
		t.Fatalf("enable second token: %v", err)
	}

	second, err := service.GetProfile(ctx, requestContext)
	if err != nil {
		t.Fatalf("get refreshed profile: %v", err)
	}
	if second.Quota.Limit != service.cfg.Inventory.Limits.Healthy.Hourly {
		t.Fatalf("expected healthy profile quota limit %d after inventory refresh, got %d", service.cfg.Inventory.Limits.Healthy.Hourly, second.Quota.Limit)
	}
	if second.Quota.Remaining != service.cfg.Inventory.Limits.Healthy.Hourly {
		t.Fatalf("expected healthy profile remaining quota %d after inventory refresh, got %d", service.cfg.Inventory.Limits.Healthy.Hourly, second.Quota.Remaining)
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

func TestDefaultDashboardSummaryPayloadMarksUnavailableValues(t *testing.T) {
	service, _ := newClaimTestService(t)

	payload := service.defaultDashboardSummaryPayload(dashboardSummaryOptions{
		Window: defaultDashboardWindow,
		Bucket: defaultDashboardBucket,
	})

	if payload["data_source"] != dataSourceUnavailable {
		t.Fatalf("expected top-level unavailable data source, got %#v", payload["data_source"])
	}
	if _, ok := payload["stale_at"]; ok {
		t.Fatalf("default unavailable payload should not include stale_at, got %#v", payload["stale_at"])
	}

	stats, ok := payload["stats"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected stats payload: %#v", payload["stats"])
	}
	if stats["data_source"] != dataSourceUnavailable {
		t.Fatalf("expected stats data source to be unavailable, got %#v", stats["data_source"])
	}
	if value, exists := stats["total_tokens"]; !exists || value != nil {
		t.Fatalf("expected unavailable total_tokens to be null, got exists=%v value=%#v", exists, value)
	}
}

func TestGetBootstrapReadsStaleDashboardEnvelope(t *testing.T) {
	service, store := newClaimTestService(t)
	service.cache = runtimecache.New(context.Background(), config.CacheConfig{Backend: "memory"}, nil)
	ctx := context.Background()

	userID := insertTestUser(t, store, "10031", "bootstrap-stale-user")
	requestContext := &auth.RequestContext{
		UserID: userID,
		User: auth.UserPayload{
			ID:         "10031",
			Username:   "bootstrap-stale-user",
			Name:       "bootstrap-stale-user",
			TrustLevel: 2,
		},
	}

	options := dashboardSummaryOptions{Window: defaultDashboardWindow, Bucket: defaultDashboardBucket}
	staleGeneratedAt := "2026-03-23T12:00:00Z"
	stalePayload := service.defaultDashboardSummaryPayload(options)
	stalePayload["data_source"] = dataSourceStale
	service.cache.SetJSON(
		service.dashboardSummaryStaleCacheKey(userID, options),
		staleReadEnvelope[map[string]any]{
			Value:       stalePayload,
			GeneratedAt: staleGeneratedAt,
		},
		service.cfg.Cache.DashboardTTL,
	)

	payload, err := service.GetBootstrap(ctx, requestContext)
	if err != nil {
		t.Fatalf("get bootstrap: %v", err)
	}

	dashboard, ok := payload["dashboard"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected dashboard payload: %#v", payload["dashboard"])
	}
	if dashboard["data_source"] != dataSourceStale {
		t.Fatalf("expected stale dashboard data source, got %#v", dashboard["data_source"])
	}
	if dashboard["stale_at"] != staleGeneratedAt {
		t.Fatalf("expected dashboard stale_at=%q, got %#v", staleGeneratedAt, dashboard["stale_at"])
	}
}

func TestBuildInventoryPolicyForStatusMapsConfiguredLimits(t *testing.T) {
	service, _ := newClaimTestServiceFromEnv(t, claimPolicyTestEnvBody())

	testCases := []struct {
		name   string
		status string
		want   config.InventoryStatusLimit
	}{
		{name: "healthy", status: "healthy", want: service.cfg.Inventory.Limits.Healthy},
		{name: "warning", status: "warning", want: service.cfg.Inventory.Limits.Warning},
		{name: "critical", status: "critical", want: service.cfg.Inventory.Limits.Critical},
	}

	snapshot := map[string]int{
		"unclaimed": 701,
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			policy := service.buildInventoryPolicyForStatus(snapshot, tc.status)
			if policy.Status != tc.status {
				t.Fatalf("expected status %q, got %q", tc.status, policy.Status)
			}
			if policy.HourlyLimit != tc.want.Hourly {
				t.Fatalf("expected hourly limit %d for status %q, got %d", tc.want.Hourly, tc.status, policy.HourlyLimit)
			}
			if policy.MaxClaims != tc.want.MaxClaims {
				t.Fatalf("expected max claims %d for status %q, got %d", tc.want.MaxClaims, tc.status, policy.MaxClaims)
			}
		})
	}
}

func TestGetInventoryPolicyUsesRuntimeStatusAsSingleSourceOfTruth(t *testing.T) {
	service, store := newClaimTestServiceFromEnv(t, claimPolicyTestEnvBody())
	ctx := context.Background()

	insertInventoryRuntimeState(t, store, "warning", 900, 900, 701, 99, 1000)
	if got := service.resolveInventoryStatus(701); got != "healthy" {
		t.Fatalf("expected setup snapshot to resolve healthy before runtime override, got %q", got)
	}

	policy, err := service.getInventoryPolicy(ctx)
	if err != nil {
		t.Fatalf("get inventory policy: %v", err)
	}

	expected := service.cfg.Inventory.Limits.Warning
	if policy.Status != "warning" {
		t.Fatalf("expected warning status from runtime state, got %q", policy.Status)
	}
	if policy.HourlyLimit != expected.Hourly {
		t.Fatalf("expected warning hourly limit %d from config, got %d", expected.Hourly, policy.HourlyLimit)
	}
	if policy.MaxClaims != expected.MaxClaims {
		t.Fatalf("expected warning max claims %d from config, got %d", expected.MaxClaims, policy.MaxClaims)
	}
}

func TestLoadSystemStatusUsesRuntimeStatusAsSingleSourceOfTruth(t *testing.T) {
	service, store := newClaimTestServiceFromEnv(t, claimPolicyTestEnvBody())
	ctx := context.Background()

	insertInventoryRuntimeState(t, store, "critical", 900, 900, 701, 77, 1000)
	if got := service.resolveInventoryStatus(701); got != "healthy" {
		t.Fatalf("expected setup snapshot to resolve healthy before runtime override, got %q", got)
	}

	payload, err := service.loadSystemStatus(ctx)
	if err != nil {
		t.Fatalf("load system status: %v", err)
	}

	health, ok := payload["health"].(map[string]any)
	if !ok {
		t.Fatalf("unexpected health payload: %#v", payload["health"])
	}
	expected := service.cfg.Inventory.Limits.Critical
	if health["status"] != "critical" {
		t.Fatalf("expected critical health status, got %#v", health["status"])
	}
	if intFromAny(health["hourly_limit"]) != expected.Hourly {
		t.Fatalf("expected critical hourly limit %d from config, got %#v", expected.Hourly, health["hourly_limit"])
	}
	if intFromAny(health["max_claims"]) != expected.MaxClaims {
		t.Fatalf("expected critical max claims %d from config, got %#v", expected.MaxClaims, health["max_claims"])
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

func newClaimTestServiceFromEnv(t *testing.T, envBody string) (*Service, *database.Store) {
	t.Helper()

	restoreWD := pushTempWorkingDir(t)
	t.Cleanup(restoreWD)
	unsetClaimTestEnvKeys(t)

	if err := os.WriteFile(".env", []byte(envBody), 0o644); err != nil {
		t.Fatalf("write test .env: %v", err)
	}

	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load config from .env: %v", err)
	}

	store, err := database.OpenWithOptions(cfg.Database.Path, database.OpenOptions{
		MaxOpenConns: cfg.Database.MaxOpenConns,
		MaxIdleConns: cfg.Database.MaxIdleConns,
	})
	if err != nil {
		t.Fatalf("open test database from env config: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init test database from env config: %v", err)
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	service := NewService(cfg, store, nil, nil, logger)
	service.probe = alwaysOKProbe{}
	return service, store
}

func claimPolicyTestEnvBody() string {
	return `TOKEN_DB_PATH=claim-policy-test.db
TOKEN_FILES_DIR=tokens
TOKEN_HEALTHY_THRESHOLD=1000
TOKEN_WARNING_THRESHOLD=500
TOKEN_CRITICAL_THRESHOLD=100
TOKEN_HOURLY_LIMIT_HEALTHY=41
TOKEN_HOURLY_LIMIT_WARNING=23
TOKEN_HOURLY_LIMIT_CRITICAL=17
TOKEN_MAX_CLAIMS_HEALTHY=4
TOKEN_MAX_CLAIMS_WARNING=7
TOKEN_MAX_CLAIMS_CRITICAL=9
TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE=all_unfinished
TOKEN_APIKEY_MAX_PER_USER=5
TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE=60
TOKEN_UPLOAD_MAX_FILES_PER_REQUEST=10
TOKEN_UPLOAD_MAX_FILE_SIZE_BYTES=10240
TOKEN_UPLOAD_MAX_SUCCESS_PER_HOUR=20
`
}

func claimQuotaRefreshTestEnvBody() string {
	return `TOKEN_DB_PATH=claim-quota-refresh.db
TOKEN_FILES_DIR=tokens
TOKEN_HEALTHY_THRESHOLD=2
TOKEN_WARNING_THRESHOLD=2
TOKEN_CRITICAL_THRESHOLD=1
TOKEN_HOURLY_LIMIT_HEALTHY=30
TOKEN_HOURLY_LIMIT_WARNING=20
TOKEN_HOURLY_LIMIT_CRITICAL=15
TOKEN_MAX_CLAIMS_HEALTHY=1
TOKEN_MAX_CLAIMS_WARNING=2
TOKEN_MAX_CLAIMS_CRITICAL=3
TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE=all_unfinished
TOKEN_APIKEY_MAX_PER_USER=5
TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE=60
TOKEN_UPLOAD_MAX_FILES_PER_REQUEST=10
TOKEN_UPLOAD_MAX_FILE_SIZE_BYTES=10240
TOKEN_UPLOAD_MAX_SUCCESS_PER_HOUR=20
`
}

func unsetClaimTestEnvKeys(t *testing.T) {
	t.Helper()

	keys := []string{
		"TOKEN_DB_PATH",
		"TOKEN_FILES_DIR",
		"TOKEN_HEALTHY_THRESHOLD",
		"TOKEN_WARNING_THRESHOLD",
		"TOKEN_CRITICAL_THRESHOLD",
		"TOKEN_HOURLY_LIMIT_HEALTHY",
		"TOKEN_HOURLY_LIMIT_WARNING",
		"TOKEN_HOURLY_LIMIT_CRITICAL",
		"TOKEN_MAX_CLAIMS_HEALTHY",
		"TOKEN_MAX_CLAIMS_WARNING",
		"TOKEN_MAX_CLAIMS_CRITICAL",
		"TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE",
		"TOKEN_APIKEY_MAX_PER_USER",
		"TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE",
		"TOKEN_UPLOAD_MAX_FILES_PER_REQUEST",
		"TOKEN_UPLOAD_MAX_FILE_SIZE_BYTES",
		"TOKEN_UPLOAD_MAX_SUCCESS_PER_HOUR",
	}

	snapshot := make(map[string]*string, len(keys))
	for _, key := range keys {
		if value, ok := os.LookupEnv(key); ok {
			copied := value
			snapshot[key] = &copied
		} else {
			snapshot[key] = nil
		}
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset env %s: %v", key, err)
		}
	}

	t.Cleanup(func() {
		for key, value := range snapshot {
			if value == nil {
				_ = os.Unsetenv(key)
				continue
			}
			_ = os.Setenv(key, *value)
		}
	})
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

func setTestTokenEnabledState(t *testing.T, store *database.Store, tokenID int64, enabled bool) {
	t.Helper()

	enabledValue := boolToInt(enabled)
	if _, err := store.DB().Exec(`
		UPDATE tokens
		SET is_enabled = ?,
		    is_available = ?,
		    updated_at_ts = 1000
		WHERE id = ?
	`, enabledValue, enabledValue, tokenID); err != nil {
		t.Fatalf("set test token enabled state: %v", err)
	}
}

func insertInventoryRuntimeState(t *testing.T, store *database.Store, status string, total int, available int, unclaimed int, maxClaims int, updatedAtTS int64) {
	t.Helper()

	if _, err := store.DB().Exec(`
		INSERT INTO inventory_runtime (
			id,
			status,
			total_tokens,
			available_tokens,
			unclaimed_tokens,
			max_claims,
			updated_at_ts
		) VALUES (1, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = excluded.status,
			total_tokens = excluded.total_tokens,
			available_tokens = excluded.available_tokens,
			unclaimed_tokens = excluded.unclaimed_tokens,
			max_claims = excluded.max_claims,
			updated_at_ts = excluded.updated_at_ts
	`, status, total, available, unclaimed, maxClaims, updatedAtTS); err != nil {
		t.Fatalf("insert inventory runtime state: %v", err)
	}
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
