package claim

import (
	"context"
	"fmt"
	"reflect"

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

func (s *Service) dashboardCacheKey(prefix string, userID *int64, parts ...any) string {
	if s.cache == nil {
		arguments := make([]any, 0, len(parts)+1)
		if userID != nil {
			arguments = append(arguments, *userID)
		}
		arguments = append(arguments, parts...)
		return runtimecache.BuildCacheKey(prefix, arguments...)
	}

	dashboardVersion := s.cache.ScopeVersion("dashboard")
	arguments := []any{fmt.Sprintf("v%d", dashboardVersion)}
	if userID != nil {
		userVersion := s.cache.ScopeVersion("dashboard-user", *userID)
		arguments = append([]any{*userID, fmt.Sprintf("uv%d", userVersion)}, arguments...)
	}
	arguments = append(arguments, parts...)
	return runtimecache.BuildCacheKey(prefix, arguments...)
}

func (s *Service) adminCacheKey(prefix string, parts ...any) string {
	if s.cache == nil {
		return runtimecache.BuildCacheKey(prefix, parts...)
	}
	version := s.cache.ScopeVersion("admin")
	return runtimecache.BuildCacheKey(prefix, append([]any{fmt.Sprintf("v%d", version)}, parts...)...)
}

func (s *Service) userBasicCacheKey(userID int64, isAdmin bool) string {
	return s.snapshotCacheKey("user-basic", userID, boolToFlag(isAdmin), fmt.Sprintf("v%d", s.cacheScopeVersion("user-basic", userID)))
}

func (s *Service) userQuotaCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-quota", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-quota", userID)))
}

func (s *Service) userClaimSummaryCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-claims-summary", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-claims-summary", userID)))
}

func (s *Service) userProfileCacheKey(userID int64, isAdmin bool) string {
	return s.snapshotCacheKey("user-profile", userID, boolToFlag(isAdmin), fmt.Sprintf("v%d", s.cacheScopeVersion("user-profile", userID)))
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
	)
}

func (s *Service) userQueueCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-queue", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-queue", userID)))
}

func (s *Service) userUploadResultsCacheKey(userID int64) string {
	return s.snapshotCacheKey("user-upload-results", userID, fmt.Sprintf("v%d", s.cacheScopeVersion("user-upload-results", userID)))
}

func (s *Service) userBootstrapCacheKey(userID int64, isAdmin bool) string {
	return s.snapshotCacheKey(
		"user-bootstrap",
		userID,
		boolToFlag(isAdmin),
		fmt.Sprintf("pv%d", s.cacheScopeVersion("user-profile", userID)),
		fmt.Sprintf("dv%d", s.cacheScopeVersion("dashboard")),
		fmt.Sprintf("duv%d", s.cacheScopeVersion("dashboard-user", userID)),
		fmt.Sprintf("qv%d", s.cacheScopeVersion("user-queue", userID)),
		fmt.Sprintf("urv%d", s.cacheScopeVersion("user-upload-results", userID)),
	)
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
	s.cache.BumpScope("user-queue", userID)
}

func (s *Service) invalidateUserUploadResultsCache(userID int64) {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("user-upload-results", userID)
}

func (s *Service) invalidateInventoryCache() {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("inventory")
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
	s.cache.BumpScope("dashboard")
	if userID != nil {
		s.cache.BumpScope("dashboard-user", *userID)
	}
}

func (s *Service) invalidateAdminCache() {
	if s.cache == nil {
		return
	}
	s.cache.BumpScope("admin")
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

func (s *Service) notifyQueueUsers(ctx context.Context, extraUserIDs ...int64) {
	seen := make(map[int64]struct{}, len(extraUserIDs))
	for _, userID := range extraUserIDs {
		if userID <= 0 {
			continue
		}
		seen[userID] = struct{}{}
	}

	queuedUserIDs, err := s.listQueuedUserIDs(ctx)
	if err == nil {
		for _, userID := range queuedUserIDs {
			if userID <= 0 {
				continue
			}
			seen[userID] = struct{}{}
		}
	}

	for userID := range seen {
		payload, err := s.loadQueueStatusSnapshot(ctx, userID)
		if err != nil {
			s.invalidateUserQueueCache(userID)
			s.queueEvents.notify(userID)
			continue
		}
		s.setQueueStatusSnapshot(userID, payload)
	}
}

func (s *Service) queueSnapshotTTL() int {
	return maxInt(s.cfg.Cache.QueueTTL, 86400)
}

func (s *Service) uploadResultsTTL() int {
	return maxInt(s.cfg.Cache.MeTTL, 60)
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

	key := s.userQueueCacheKey(userID)
	var previous queueStatusPayload
	hadPrevious := s.cache.GetJSON(key, &previous)
	changed := !hadPrevious || !reflect.DeepEqual(previous, payload)
	if changed {
		s.cache.BumpScope("user-queue", userID)
		key = s.userQueueCacheKey(userID)
	}
	s.cache.SetJSON(key, payload, s.queueSnapshotTTL())
	if changed {
		s.queueEvents.notify(userID)
	}
	return payload
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
