package claim

import (
	"context"
	"strings"
	"time"

	"token-atlas/internal/auth"
	"token-atlas/internal/runtimecache"
)

const (
	defaultDashboardWindow = "7d"
	defaultDashboardBucket = "1h"

	defaultAdminUsersLimit  = 50
	defaultAdminBansLimit   = 50
	defaultAdminTokensLimit = 50
)

func (s *Service) GetBootstrap(ctx context.Context, requestContext *auth.RequestContext) (map[string]any, error) {
	startedAt := time.Now()
	userID := int64(0)
	var (
		profileDuration       time.Duration
		dashboardDuration     time.Duration
		queueStatusDuration   time.Duration
		claimRealtimeDuration time.Duration
		uploadResultsDuration time.Duration
		profileSource         = dataSourceUnavailable
		dashboardSource       = dataSourceUnavailable
		queueStatusSource     = dataSourceUnavailable
		claimRealtimeSource   = dataSourceUnavailable
		uploadResultsSource   = dataSourceLive
	)
	if requestContext != nil {
		userID = requestContext.UserID
	}

	profileStartedAt := time.Now()
	profile := s.defaultProfilePayload(requestContext)
	if requestContext != nil {
		if cached, ok := s.getCachedProfile(requestContext.UserID, requestContext.IsAdmin); ok {
			profile = cached
			profileSource = dataSourceCache
		} else if runtimeSnapshot, ok := s.getCachedRuntimeSnapshot(requestContext.UserID); ok {
			profile = s.defaultProfilePayload(requestContext)
			profile.Quota = runtimeSnapshot.Quota
			profile.Claims = runtimeSnapshot.Claims
			if summary, ok := runtimeSnapshot.APIKeys["summary"]; ok {
				profile.APIKeys = summary
			}
			if runtimeSnapshot.Uploads != nil {
				profile.Uploads = runtimeSnapshot.Uploads
			}
			profileSource = dataSourceCache
		} else if stale, ok := s.getCachedStaleRuntimeSnapshot(requestContext.UserID); ok {
			profile = s.defaultProfilePayload(requestContext)
			profile.Quota = stale.Value.Quota
			profile.Claims = stale.Value.Claims
			if summary, ok := stale.Value.APIKeys["summary"]; ok {
				profile.APIKeys = summary
			}
			if stale.Value.Uploads != nil {
				profile.Uploads = stale.Value.Uploads
			}
			profileSource = dataSourceStale
		}
	}
	profileDuration = time.Since(profileStartedAt)

	dashboardOptions := dashboardSummaryOptions{Window: defaultDashboardWindow, Bucket: defaultDashboardBucket}
	dashboardStartedAt := time.Now()
	dashboard := s.defaultDashboardSummaryPayload(dashboardOptions)
	if requestContext != nil {
		if cached, ok := s.getCachedDashboardSummary(requestContext.UserID, dashboardOptions); ok {
			dashboard = annotateDashboardPayload(cached, dataSourceCache, "", "", "", false)
			dashboardSource = dataSourceCache
		} else if stale, ok := s.getCachedStaleDashboardSummary(requestContext.UserID, dashboardOptions); ok {
			dashboard = annotateDashboardPayload(
				stale.Value,
				dataSourceStale,
				stale.GeneratedAt,
				staleTimestamp(stale.GeneratedAt),
				degradedReasonCompatibility,
				true,
			)
			dashboardSource = dataSourceStale
		}
	}
	dashboardDuration = time.Since(dashboardStartedAt)

	queueStatusStartedAt := time.Now()
	queueStatus := s.defaultQueueStatusPayload()
	if requestContext != nil {
		if cached, ok := s.getCachedQueueStatus(requestContext.UserID); ok {
			queueStatus = withQueueStatusMetadata(cached, dataSourceCache, cached.GeneratedAt, "", "", false)
			queueStatusSource = dataSourceCache
		} else if stale, ok := s.getCachedStaleQueueStatus(requestContext.UserID); ok {
			queueStatus = withQueueStatusMetadata(
				stale,
				dataSourceStale,
				stale.GeneratedAt,
				staleTimestamp(stale.GeneratedAt),
				degradedReasonCompatibility,
				true,
			)
			queueStatusSource = dataSourceStale
		}
		queueStatus = s.attachCachedClaimableNow(requestContext.UserID, queueStatus)
	}
	queueStatusDuration = time.Since(queueStatusStartedAt)

	claimRealtimeStartedAt := time.Now()
	claimRealtime := s.emptyClaimRealtimeSnapshot()
	if requestContext != nil {
		if cached, ok := s.getCachedClaimRealtime(requestContext.UserID); ok {
			claimRealtime = cached
			claimRealtimeSource = dataSourceCache
		}
	}
	claimRealtimeDuration = time.Since(claimRealtimeStartedAt)

	uploadResultsStartedAt := time.Now()
	uploadResults := buildUploadResultsSummaryPayload(nil)
	if requestContext != nil {
		uploadResults = buildUploadResultsSummaryPayload(s.GetUploadResults(requestContext.UserID))
	}
	uploadResultsDuration = time.Since(uploadResultsStartedAt)
	dashboardStaleAt, _ := dashboard["stale_at"].(string)

	bootstrapSource := dataSourceCache
	for _, source := range []string{profileSource, dashboardSource, queueStatusSource, claimRealtimeSource} {
		switch source {
		case dataSourceUnavailable:
			bootstrapSource = dataSourceUnavailable
		case dataSourceStale:
			if bootstrapSource != dataSourceUnavailable {
				bootstrapSource = dataSourceStale
			}
		}
	}

	payload := map[string]any{
		"profile":        profile,
		"dashboard":      dashboard,
		"claim_realtime": claimRealtime,
		"queue_status":   queueStatus,
		"upload_results": uploadResults,
		"sources": map[string]any{
			"profile":        profileSource,
			"dashboard":      dashboardSource,
			"queue_status":   queueStatusSource,
			"claim_realtime": claimRealtimeSource,
			"upload_results": uploadResultsSource,
		},
		"data_source":     bootstrapSource,
		"generated_at":    isoformatNow(),
		"degraded":        true,
		"degraded_reason": degradedReasonCompatibility,
	}
	if bootstrapSource == dataSourceStale {
		payload["stale_at"] = firstNonEmpty(
			strings.TrimSpace(queueStatus.StaleAt),
			strings.TrimSpace(dashboardStaleAt),
		)
	}
	s.logger.Info(
		"get bootstrap",
		"user_id", userID,
		"data_source", payload["data_source"],
		"degraded", payload["degraded"],
		"profile_source", profileSource,
		"dashboard_source", dashboardSource,
		"queue_status_source", queueStatusSource,
		"claim_realtime_source", claimRealtimeSource,
		"upload_results_source", uploadResultsSource,
		"total_ms", durationMillis(time.Since(startedAt)),
		"profile_ms", durationMillis(profileDuration),
		"dashboard_ms", durationMillis(dashboardDuration),
		"queue_status_ms", durationMillis(queueStatusDuration),
		"claim_realtime_ms", durationMillis(claimRealtimeDuration),
		"upload_results_ms", durationMillis(uploadResultsDuration),
	)
	return payload, nil
}

func (s *Service) cachedAdminUsersPage(ctx context.Context, search string, banStatus string, limit int, offset int) (map[string]any, error) {
	cacheKey := s.adminCacheKey("admin-users", strings.ToLower(strings.TrimSpace(search)), strings.ToLower(strings.TrimSpace(banStatus)), limit, offset)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.AdminTTL, func() (map[string]any, error) {
		return s.ListUsersForAdmin(ctx, search, banStatus, limit, offset)
	})
}

func (s *Service) cachedAdminBansPage(ctx context.Context, statusFilter string, search string, limit int, offset int) (map[string]any, error) {
	cacheKey := s.adminCacheKey("admin-bans", strings.ToLower(strings.TrimSpace(statusFilter)), strings.ToLower(strings.TrimSpace(search)), limit, offset)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.AdminTTL, func() (map[string]any, error) {
		return s.ListBans(ctx, statusFilter, search, limit, offset)
	})
}

func (s *Service) cachedAdminTokensPage(ctx context.Context, search string, statusFilter string, limit int, offset int) (map[string]any, error) {
	cacheKey := s.adminCacheKey("admin-tokens", strings.ToLower(strings.TrimSpace(search)), strings.ToLower(strings.TrimSpace(statusFilter)), limit, offset)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.AdminTTL, func() (map[string]any, error) {
		return s.ListTokensForAdmin(ctx, search, statusFilter, limit, offset)
	})
}

func (s *Service) cachedAdminQueuePage(ctx context.Context, search string, statusFilter string, onlyFilter string, limit int, offset int) (map[string]any, error) {
	cacheKey := s.adminQueueCacheKey(
		"admin-queue",
		strings.ToLower(strings.TrimSpace(search)),
		strings.ToLower(strings.TrimSpace(statusFilter)),
		strings.ToLower(strings.TrimSpace(onlyFilter)),
		limit,
		offset,
	)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.AdminTTL, func() (map[string]any, error) {
		return s.ListQueueForAdmin(ctx, search, statusFilter, onlyFilter, limit, offset)
	})
}

func (s *Service) buildAdminBootstrapPayload(ctx context.Context, requestContext *auth.RequestContext) (map[string]any, error) {
	me, err := s.GetAdminMe(ctx, requestContext)
	if err != nil {
		return nil, err
	}

	users, err := s.cachedAdminUsersPage(ctx, "", "", defaultAdminUsersLimit, 0)
	if err != nil {
		return nil, err
	}

	bans, err := s.cachedAdminBansPage(ctx, "", "", defaultAdminBansLimit, 0)
	if err != nil {
		return nil, err
	}

	tokens, err := s.cachedAdminTokensPage(ctx, "", "", defaultAdminTokensLimit, 0)
	if err != nil {
		return nil, err
	}

	queue, err := s.cachedAdminQueuePage(ctx, "", "queued", adminQueueOnlyAll, defaultAdminQueueLimit, 0)
	if err != nil {
		return nil, err
	}

	policy, err := s.GetAdminPolicy(ctx)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"me":     me,
		"users":  users,
		"bans":   bans,
		"tokens": tokens,
		"queue":  queue,
		"policy": policy,
	}, nil
}

func (s *Service) GetAdminBootstrap(ctx context.Context, requestContext *auth.RequestContext) (map[string]any, error) {
	return runtimecache.CacheJSON(
		s.cache,
		s.adminBootstrapCacheKey(requestContext.UserID),
		s.cfg.Cache.AdminTTL,
		func() (map[string]any, error) {
			return s.buildAdminBootstrapPayload(ctx, requestContext)
		},
	)
}

func (s *Service) warmReadCaches(ctx context.Context) {
	if ctx.Err() != nil || !s.waitForStartupReady(ctx) {
		return
	}

	if _, err := s.getSystemStatus(ctx); err != nil {
		s.logger.Warn("warm system cache", "error", err)
	}
	if _, err := s.getLeaderboard(ctx, 24*3600, 10); err != nil {
		s.logger.Warn("warm leaderboard cache", "error", err)
	}
	if _, err := s.getRecentClaims(ctx, 10); err != nil {
		s.logger.Warn("warm recent claims cache", "error", err)
	}
	if _, err := s.getContributorLeaderboard(ctx, 10); err != nil {
		s.logger.Warn("warm contributor leaderboard cache", "error", err)
	}
	if _, err := s.getRecentContributors(ctx, 10); err != nil {
		s.logger.Warn("warm recent contributors cache", "error", err)
	}
	if _, err := s.getClaimTrends(ctx, 7*24*3600, 3600); err != nil {
		s.logger.Warn("warm claim trends cache", "error", err)
	}
	s.primeAdminDefaultReadCaches(ctx)
}

func (s *Service) primeAdminDefaultReadCaches(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if _, err := s.cachedAdminUsersPage(ctx, "", "", defaultAdminUsersLimit, 0); err != nil {
		s.logger.Warn("warm admin users cache", "error", err)
	}
	if _, err := s.cachedAdminBansPage(ctx, "", "", defaultAdminBansLimit, 0); err != nil {
		s.logger.Warn("warm admin bans cache", "error", err)
	}
	if _, err := s.cachedAdminTokensPage(ctx, "", "", defaultAdminTokensLimit, 0); err != nil {
		s.logger.Warn("warm admin tokens cache", "error", err)
	}
	if _, err := s.cachedAdminQueuePage(ctx, "", "queued", adminQueueOnlyAll, defaultAdminQueueLimit, 0); err != nil {
		s.logger.Warn("warm admin queue cache", "error", err)
	}
	if _, err := s.GetAdminPolicy(ctx); err != nil {
		s.logger.Warn("warm admin policy cache", "error", err)
	}
}
