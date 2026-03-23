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

func (s *Service) buildBootstrapPayload(ctx context.Context, requestContext *auth.RequestContext) (map[string]any, error) {
	profile, err := s.GetProfile(ctx, requestContext)
	if err != nil {
		return nil, err
	}

	dashboard, err := s.GetDashboardSummary(ctx, requestContext.UserID, defaultDashboardWindow, defaultDashboardBucket)
	if err != nil {
		return nil, err
	}

	queueStatus, err := s.GetQueueStatus(ctx, requestContext.UserID)
	if err != nil {
		return nil, err
	}

	claimRealtime, err := s.GetClaimRealtimeSnapshot(ctx, requestContext.UserID, requestContext.SessionID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"profile":        profile,
		"dashboard":      dashboard,
		"claim_realtime": claimRealtime,
		"queue_status":   queueStatus,
		"upload_results": buildUploadResultsSummaryPayload(s.GetUploadResults(requestContext.UserID)),
	}, nil
}

func (s *Service) GetBootstrap(ctx context.Context, requestContext *auth.RequestContext) (map[string]any, error) {
	startedAt := time.Now()
	observedCtx, _ := withSQLMetrics(ctx)
	var (
		profileDuration       time.Duration
		dashboardDuration     time.Duration
		queueStatusDuration   time.Duration
		claimRealtimeDuration time.Duration
		uploadResultsDuration time.Duration
		loaderExecuted        bool
	)

	payload, err := runtimecache.CacheJSON(
		s.cache,
		s.userBootstrapCacheKey(requestContext.UserID, requestContext.IsAdmin),
		s.cfg.Cache.MeTTL,
		func() (map[string]any, error) {
			loaderExecuted = true

			stageStartedAt := time.Now()
			profile, err := s.GetProfile(observedCtx, requestContext)
			profileDuration = time.Since(stageStartedAt)
			if err != nil {
				return nil, err
			}

			stageStartedAt = time.Now()
			dashboard, err := s.GetDashboardSummary(observedCtx, requestContext.UserID, defaultDashboardWindow, defaultDashboardBucket)
			dashboardDuration = time.Since(stageStartedAt)
			if err != nil {
				return nil, err
			}

			stageStartedAt = time.Now()
			queueStatus, err := s.GetQueueStatus(observedCtx, requestContext.UserID)
			queueStatusDuration = time.Since(stageStartedAt)
			if err != nil {
				return nil, err
			}

			stageStartedAt = time.Now()
			claimRealtime, err := s.GetClaimRealtimeSnapshot(observedCtx, requestContext.UserID, requestContext.SessionID)
			claimRealtimeDuration = time.Since(stageStartedAt)
			if err != nil {
				return nil, err
			}

			stageStartedAt = time.Now()
			uploadResults := buildUploadResultsSummaryPayload(s.GetUploadResults(requestContext.UserID))
			uploadResultsDuration = time.Since(stageStartedAt)

			return map[string]any{
				"profile":        profile,
				"dashboard":      dashboard,
				"claim_realtime": claimRealtime,
				"queue_status":   queueStatus,
				"upload_results": uploadResults,
			}, nil
		},
	)
	s.logger.Info(
		"get bootstrap",
		"user_id", requestContext.UserID,
		"cache_hit", !loaderExecuted,
		"total_ms", durationMillis(time.Since(startedAt)),
		"profile_ms", durationMillis(profileDuration),
		"dashboard_ms", durationMillis(dashboardDuration),
		"queue_status_ms", durationMillis(queueStatusDuration),
		"claim_realtime_ms", durationMillis(claimRealtimeDuration),
		"upload_results_ms", durationMillis(uploadResultsDuration),
		"sql_count", sqlCount(observedCtx),
		"error", err,
	)
	return payload, err
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
