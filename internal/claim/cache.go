package claim

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"time"

	"token-atlas/internal/runtimecache"
)

func (s *Service) userCacheKey(prefix string, userID int64, parts ...any) string {
	if s.cache == nil {
		return runtimecache.BuildCacheKey(prefix, append([]any{userID}, parts...)...)
	}
	version := s.cache.ScopeVersion("user", userID)
	arguments := []any{userID, fmt.Sprintf("v%d", version)}
	arguments = append(arguments, parts...)
	return runtimecache.BuildCacheKey(prefix, arguments...)
}

func (s *Service) snapshotCacheKey(prefix string, parts ...any) string {
	return runtimecache.BuildSnapshotCacheKey(prefix, parts...)
}

func cacheAliasKey(stableKey string) string {
	return runtimecache.BuildCacheKey("cache-alias", stableKey)
}

func stableDashboardCacheKey(prefix string, userID *int64, parts ...any) string {
	arguments := make([]any, 0, len(parts)+1)
	if userID != nil {
		arguments = append(arguments, *userID)
	}
	arguments = append(arguments, parts...)
	return runtimecache.BuildCacheKey(prefix, arguments...)
}

func (s *Service) dashboardCacheKey(prefix string, userID *int64, parts ...any) string {
	if s.cache == nil {
		return stableDashboardCacheKey(prefix, userID, parts...)
	}

	dashboardVersion := s.cache.ScopeVersion(prefix)
	arguments := []any{fmt.Sprintf("v%d", dashboardVersion)}
	if userID != nil {
		arguments = append([]any{*userID}, arguments...)
	}
	arguments = append(arguments, parts...)
	return runtimecache.BuildCacheKey(prefix, arguments...)
}

func (s *Service) dashboardStaleCacheKey(prefix string, userID *int64, parts ...any) string {
	arguments := []any{prefix}
	if userID != nil {
		arguments = append(arguments, *userID)
	}
	arguments = append(arguments, parts...)
	return s.snapshotCacheKey("dashboard-stale", arguments...)
}

func (s *Service) dashboardSummaryCacheKey(userID int64, options dashboardSummaryOptions) string {
	normalized := normalizeDashboardSummaryOptions(options)
	return s.snapshotCacheKey(
		"dashboard-summary",
		userID,
		normalized.WindowSeconds,
		normalized.BucketSeconds,
		normalized.LeaderboardWindow,
		normalized.LeaderboardLimit,
		normalized.RecentLimit,
		normalized.ContributorLimit,
		normalized.RecentContributorLimit,
		fmt.Sprintf("sumv%d", s.cacheScopeVersion("dashboard-summary")),
		fmt.Sprintf("sv%d", s.cacheScopeVersion("dashboard-stats")),
		fmt.Sprintf("sysv%d", s.cacheScopeVersion("dashboard-system")),
		fmt.Sprintf("lbv%d", s.cacheScopeVersion("dashboard-leaderboard")),
		fmt.Sprintf("rv%d", s.cacheScopeVersion("dashboard-recent")),
		fmt.Sprintf("cv%d", s.cacheScopeVersion("dashboard-contributors")),
		fmt.Sprintf("rcv%d", s.cacheScopeVersion("dashboard-recent-contributors")),
		fmt.Sprintf("tv%d", s.cacheScopeVersion("dashboard-trends")),
	)
}

func (s *Service) dashboardSummaryStaleCacheKey(userID int64, options dashboardSummaryOptions) string {
	normalized := normalizeDashboardSummaryOptions(options)
	return s.dashboardStaleCacheKey(
		"dashboard-summary",
		&userID,
		normalized.WindowSeconds,
		normalized.BucketSeconds,
		normalized.LeaderboardWindow,
		normalized.LeaderboardLimit,
		normalized.RecentLimit,
		normalized.ContributorLimit,
		normalized.RecentContributorLimit,
	)
}

func (s *Service) dashboardSummaryAliasKey(userID int64, options dashboardSummaryOptions) string {
	normalized := normalizeDashboardSummaryOptions(options)
	return cacheAliasKey(
		s.snapshotCacheKey(
			"dashboard-summary",
			userID,
			normalized.WindowSeconds,
			normalized.BucketSeconds,
			normalized.LeaderboardWindow,
			normalized.LeaderboardLimit,
			normalized.RecentLimit,
			normalized.ContributorLimit,
			normalized.RecentContributorLimit,
		),
	)
}

func (s *Service) dashboardSectionAliasKey(prefix string, userID *int64, parts ...any) string {
	return cacheAliasKey(stableDashboardCacheKey(prefix, userID, parts...))
}

func (s *Service) adminCacheKey(prefix string, parts ...any) string {
	if s.cache == nil {
		return runtimecache.BuildCacheKey(prefix, parts...)
	}
	version := s.cache.ScopeVersion("admin")
	return runtimecache.BuildCacheKey(prefix, append([]any{fmt.Sprintf("v%d", version)}, parts...)...)
}

func (s *Service) adminQueueCacheKey(prefix string, parts ...any) string {
	if s.cache == nil {
		return runtimecache.BuildCacheKey(prefix, parts...)
	}
	version := s.cache.ScopeVersion("admin-queue")
	return runtimecache.BuildCacheKey(prefix, append([]any{fmt.Sprintf("v%d", version)}, parts...)...)
}

func (s *Service) userBasicCacheKey(userID int64, isAdmin bool) string {
	return s.snapshotCacheKey("user-basic", userID, boolToFlag(isAdmin), fmt.Sprintf("v%d", s.cacheScopeVersion("user-basic", userID)))
}

func (s *Service) userQuotaCacheKey(userID int64) string {
	return s.snapshotCacheKey(
		"user-quota",
		userID,
		fmt.Sprintf("v%d", s.cacheScopeVersion("user-quota", userID)),
		fmt.Sprintf("ipv%d", s.inventoryPolicyScopeVersion()),
	)
}

func (s *Service) userClaimSummaryCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-claims-summary", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-claims-summary", userID)))
}

func (s *Service) userProfileCacheKey(userID int64, isAdmin bool) string {
	return s.snapshotCacheKey(
		"user-profile",
		userID,
		boolToFlag(isAdmin),
		fmt.Sprintf("v%d", s.cacheScopeVersion("user-profile", userID)),
		fmt.Sprintf("ipv%d", s.inventoryPolicyScopeVersion()),
	)
}

func (s *Service) userAPIKeySummaryCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-apikey-summary", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-apikey-summary", userID)))
}

func (s *Service) userRuntimeSnapshotCacheKey(userID int64) string {
	return s.snapshotCacheKey(
		"user-runtime-snapshot",
		userID,
		fmt.Sprintf("qv%d", s.cacheScopeVersion("user-quota", userID)),
		fmt.Sprintf("cv%d", s.cacheScopeVersion("user-claims-summary", userID)),
		fmt.Sprintf("akv%d", s.cacheScopeVersion("user-apikey-summary", userID)),
		fmt.Sprintf("urv%d", s.cacheScopeVersion("user-upload-results", userID)),
		fmt.Sprintf("ipv%d", s.inventoryPolicyScopeVersion()),
	)
}

func (s *Service) userRuntimeSnapshotAliasKey(userID int64) string {
	return cacheAliasKey(s.snapshotCacheKey("user-runtime-snapshot", userID))
}

func (s *Service) userRuntimeSnapshotStaleCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-runtime-snapshot-stale", userID)
}

func (s *Service) userClaimRealtimeCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-claim-realtime", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-claim-realtime", userID)))
}

func (s *Service) userQueueCacheKey(userID int64) string {
	return s.snapshotCacheKey(
		"user-queue",
		userID,
		fmt.Sprintf("uv%d", s.cacheScopeVersion("user-queue", userID)),
		fmt.Sprintf("qv%d", s.cacheScopeVersion("queue-runtime")),
		fmt.Sprintf("iv%d", s.cacheScopeVersion("inventory")),
	)
}

func (s *Service) userQueueStaleCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-queue-stale", userID)
}

func (s *Service) userClaimableNowCacheKey(userID int64) string {
	return s.snapshotCacheKey(
		"user-claimable-now",
		userID,
		fmt.Sprintf("cv%d", s.cacheScopeVersion("user-claims-summary", userID)),
		fmt.Sprintf("iv%d", s.cacheScopeVersion("inventory")),
	)
}

func (s *Service) userClaimableNowStaleCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-claimable-now-stale", userID)
}

func (s *Service) userUploadResultsCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-upload-results", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-upload-results", userID)))
}

func (s *Service) adminBootstrapCacheKey(userID int64) string {
	return s.snapshotCacheKey(
		"admin-bootstrap",
		userID,
		fmt.Sprintf("av%d", s.cacheScopeVersion("admin")),
		fmt.Sprintf("upv%d", s.cacheScopeVersion("user-profile", userID)),
		defaultAdminUsersLimit,
		defaultAdminBansLimit,
		defaultAdminTokensLimit,
		defaultAdminQueueLimit,
	)
}

func (s *Service) claimsListCacheKey(userID int64) string {
	return runtimecache.BuildCacheKey("me-claims", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-claims", userID)))
}

func (s *Service) claimFilesCacheKey(userID int64) string {
	return runtimecache.BuildCacheKey("me-claim-files", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-claims", userID)))
}

func (s *Service) claimedDocumentsCacheKey(userID int64) string {
	return s.userCacheKey("me-claimed-docs", userID)
}

func (s *Service) claimedTokenCacheKey(userID int64, tokenID int64) string {
	return runtimecache.BuildCacheKey("claimed-token", userID, tokenID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-claims", userID)))
}

func (s *Service) invalidateUserProfileCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user-basic", userID)
	s.cache.BumpScope("user-profile", userID)
}

func (s *Service) invalidateUserQuotaCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user-quota", userID)
	s.cache.BumpScope("user-profile", userID)
}

func (s *Service) invalidateUserAPIKeysCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user-apikeys", userID)
	s.cache.BumpScope("user-apikey-summary", userID)
	s.cache.BumpScope("user-profile", userID)
}

func (s *Service) invalidateUserClaimsCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user", userID)
	s.cache.BumpScope("user-claims", userID)
	s.cache.BumpScope("user-claims-summary", userID)
	s.cache.BumpScope("user-profile", userID)
}

func (s *Service) invalidateUserQueueCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.Delete(s.userQueueCacheKey(userID))
	s.cache.BumpScope("user-queue", userID)
}

func (s *Service) invalidateQueueRuntimeCache() {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("queue-runtime")
}

func (s *Service) invalidateUserUploadResultsCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.Delete(s.userUploadResultsCacheKey(userID))
	s.cache.BumpScope("user-upload-results", userID)
}

func (s *Service) invalidateInventoryCache() {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("inventory")
	s.syncInventoryPolicyScopeAsync()
}

func (s *Service) invalidateUserCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user", userID)
	s.invalidateUserProfileCache(userID)
	s.invalidateUserQuotaCache(userID)
	s.invalidateUserAPIKeysCache(userID)
	s.invalidateUserClaimsCache(userID)
}

func (s *Service) invalidateDashboardCache(userID *int64) {
	if s.cache == nil {
		return
	}
	for _, scope := range []string{
		"dashboard-summary",
		"dashboard-stats",
		"dashboard-system",
		"dashboard-leaderboard",
		"dashboard-recent",
		"dashboard-contributors",
		"dashboard-recent-contributors",
		"dashboard-trends",
	} {
		s.cache.BumpScope(scope)
	}
	if userID != nil {
		s.touchSystemIndexTimestamp()
	}
}

func (s *Service) bumpDashboardScope(scope string) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope(scope)
	s.cache.BumpScope("dashboard-summary")
}

func (s *Service) invalidateDashboardQueueCache() {
	s.bumpDashboardScope("dashboard-system")
	s.touchSystemIndexTimestamp()
}

func (s *Service) invalidateDashboardClaimCaches() {
	s.bumpDashboardScope("dashboard-system")
	s.bumpDashboardScope("dashboard-stats")
	s.bumpDashboardScope("dashboard-leaderboard")
	s.bumpDashboardScope("dashboard-recent")
	s.bumpDashboardScope("dashboard-trends")
	s.touchSystemIndexTimestamp()
}

func (s *Service) invalidateDashboardInventoryCache() {
	s.bumpDashboardScope("dashboard-system")
	s.bumpDashboardScope("dashboard-stats")
	s.touchSystemIndexTimestamp()
}

func (s *Service) invalidateDashboardContributorCaches() {
	s.bumpDashboardScope("dashboard-contributors")
	s.bumpDashboardScope("dashboard-recent-contributors")
	s.touchSystemIndexTimestamp()
}

func (s *Service) invalidateDashboardUploadCaches() {
	s.invalidateDashboardInventoryCache()
	s.bumpDashboardScope("dashboard-contributors")
	s.bumpDashboardScope("dashboard-recent-contributors")
}

func (s *Service) invalidateAdminCache() {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("admin")
}

func (s *Service) invalidateAdminQueueCache() {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("admin-queue")
}

func (s *Service) invalidateAllRuntimeCache(userID *int64, includeAdmin bool) {
	if userID == nil {
		s.touchSystemIndexTimestamp()
	}
	if userID != nil {
		s.invalidateUserCache(*userID)
	}
	s.invalidateDashboardCache(userID)
	if includeAdmin {
		s.invalidateAdminCache()
	}
}

func (s *Service) cacheScopeVersion(scope string, parts ...any) int64 {
	if s.cache == nil {
		return 1
	}
	return s.cache.ScopeVersion(scope, parts...)
}

func inventoryPolicySignature(state inventoryRuntimeState, exists bool) string {
	if !exists {
		return "missing"
	}
	return fmt.Sprintf("%s:%d", strings.TrimSpace(state.Status), state.MaxClaims)
}

func (s *Service) inventoryPolicyStateCacheKey() string {
	return runtimecache.BuildCacheKey("inventory-policy-state")
}

func (s *Service) inventoryPolicyScopeVersion() int64 {
	if s.cache == nil {
		return 1
	}
	if _, ok := s.cache.GetText(s.inventoryPolicyStateCacheKey()); !ok {
		s.syncInventoryPolicyScopeAsync()
	}
	return s.cache.ScopeVersion("inventory-policy")
}

func (s *Service) syncInventoryPolicyScope(ctx context.Context) {
	if s.cache == nil || s.store == nil || s.store.DB() == nil {
		return
	}

	state, exists, err := s.getInventoryRuntimeState(ctx, s.store.DB())
	if err != nil {
		s.logger.Warn("sync inventory policy scope", "error", err)
		return
	}

	key := s.inventoryPolicyStateCacheKey()
	nextSignature := inventoryPolicySignature(state, exists)
	if currentSignature, ok := s.cache.GetText(key); ok && strings.TrimSpace(currentSignature) == nextSignature {
		return
	}

	s.cache.SetText(key, nextSignature, 0)
	s.cache.BumpScope("inventory-policy")
}

func (s *Service) syncInventoryPolicyScopeAsync() {
	if s.cache == nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s.syncInventoryPolicyScope(ctx)
}

func (s *Service) notifyQueueUsers(ctx context.Context, extraUserIDs ...int64) {
	seen := make(map[int64]struct{}, len(extraUserIDs))
	for _, userID := range extraUserIDs {
		if userID <= 0 {
			continue
		}
		seen[userID] = struct{}{}
	}

	for userID := range seen {
		payload, err := s.loadQueueStatusSnapshot(ctx, userID)
		if err != nil {
			s.invalidateUserQueueCache(userID)
			s.logger.Warn("refresh queue snapshot", "user_id", userID, "error", err)
			continue
		}
		s.setQueueStatusSnapshot(userID, payload)
		if err := s.syncClaimRealtimeQueueStatus(ctx, userID, payload); err != nil {
			s.logger.Warn("sync claim realtime queue status", "user_id", userID, "error", err)
		}
	}
}

func (s *Service) scheduleQueueNotifications(userIDs ...int64) {
	if s == nil {
		return
	}
	ids := mapKeysInt64(uniquePositiveInt64(userIDs...))
	if len(ids) == 0 {
		return
	}
	if _, ok := s.serviceContext(); !ok {
		s.notifyQueueUsers(context.Background(), ids...)
		return
	}
	for _, userID := range ids {
		select {
		case s.queueNotifyCh <- userID:
		default:
			go s.notifyQueueUsers(context.Background(), userID)
		}
	}
}

func (s *Service) queueNotificationWorkerLoop(ctx context.Context) {
	var (
		timer  *time.Timer
		timerC <-chan time.Time
		batch  = make(map[int64]struct{})
	)
	stopTimer := func() {
		if timer == nil {
			return
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timerC = nil
	}
	flush := func() {
		if len(batch) == 0 {
			return
		}
		userIDs := mapKeysInt64(batch)
		clear(batch)
		notifyCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.notifyQueueUsers(notifyCtx, userIDs...)
	}
	for {
		select {
		case <-ctx.Done():
			stopTimer()
			flush()
			return
		case userID := <-s.queueNotifyCh:
			if userID <= 0 {
				continue
			}
			batch[userID] = struct{}{}
			if len(batch) >= queueNotifyBatchMaxUsers {
				stopTimer()
				flush()
				continue
			}
			if timer == nil {
				timer = time.NewTimer(queueNotifyBatchWindow)
			} else {
				stopTimer()
				timer.Reset(queueNotifyBatchWindow)
			}
			timerC = timer.C
		case <-timerC:
			stopTimer()
			flush()
		}
	}
}

func uniquePositiveInt64(values ...int64) map[int64]struct{} {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[int64]struct{}, len(values))
	for _, value := range values {
		if value > 0 {
			seen[value] = struct{}{}
		}
	}
	return seen
}

func (s *Service) queueSnapshotTTL() int {
	return maxInt(s.cfg.Cache.QueueTTL, 86400)
}

func (s *Service) uploadResultsTTL() int {
	return maxInt(s.cfg.Cache.MeTTL, 60)
}

func (s *Service) claimableNowSnapshotTTL() int {
	return maxInt(s.cfg.Cache.QueueTTL, 300)
}

func (s *Service) getCachedQueueStatus(userID int64) (queueStatusPayload, bool) {
	if s.cache == nil {
		return queueStatusPayload{}, false
	}
	var payload queueStatusPayload
	if !s.cache.GetJSON(s.userQueueCacheKey(userID), &payload) {
		return queueStatusPayload{}, false
	}
	return payload, true
}

func (s *Service) setQueueStatusSnapshot(userID int64, payload queueStatusPayload) queueStatusPayload {
	if s.cache == nil {
		return payload
	}

	payload = stripQueueStatusAuxiliaryFields(payload)
	if strings.TrimSpace(payload.GeneratedAt) == "" {
		payload.GeneratedAt = isoformatNow()
	}
	payload = withQueueStatusMetadata(payload, dataSourceLive, payload.GeneratedAt, "", "", false)

	key := s.userQueueCacheKey(userID)
	previousKey := key
	var previous queueStatusPayload
	hadPrevious := s.cache.GetJSON(key, &previous)
	changed := !hadPrevious || !reflect.DeepEqual(previous, payload)
	if changed {
		s.cache.BumpScope("user-queue", userID)
		key = s.userQueueCacheKey(userID)
	}
	s.cache.SetJSON(key, payload, s.queueSnapshotTTL())
	if changed && hadPrevious && previousKey != key {
		s.cache.Delete(previousKey)
	}
	s.cache.SetJSON(s.userQueueStaleCacheKey(userID), payload, s.queueSnapshotTTL())
	if changed {
		s.queueEvents.notify(userID)
	}
	return payload
}

func (s *Service) setClaimableNowSnapshot(userID int64, snapshot storedClaimableNowSnapshot) storedClaimableNowSnapshot {
	if s.cache == nil {
		return snapshot
	}
	if strings.TrimSpace(snapshot.GeneratedAt) == "" {
		snapshot.GeneratedAt = isoformatNow()
	}
	s.cache.SetJSON(s.userClaimableNowCacheKey(userID), snapshot, s.claimableNowSnapshotTTL())
	s.cache.SetJSON(s.userClaimableNowStaleCacheKey(userID), snapshot, s.claimableNowSnapshotTTL())
	return snapshot
}

func buildUploadResultsSummaryPayload(payload map[string]any) map[string]any {
	summaryPayload := map[string]any{
		"batch_id":   nil,
		"created_at": nil,
		"summary":    buildDefaultUploadResultsSummary(),
	}
	if payload == nil {
		return summaryPayload
	}

	summaryPayload["batch_id"] = payload["batch_id"]
	summaryPayload["created_at"] = payload["created_at"]
	summaryPayload["summary"] = normalizeUploadResultsSummary(payload["summary"])
	return summaryPayload
}

func normalizeUploadResultsSummary(value any) map[string]int {
	summary := buildDefaultUploadResultsSummary()
	switch typed := value.(type) {
	case map[string]int:
		for key, item := range typed {
			summary[key] = item
		}
	case map[string]any:
		for key, item := range typed {
			summary[key] = intFromAny(item)
		}
	}
	return summary
}

func boolToFlag(value bool) int {
	if value {
		return 1
	}
	return 0
}
