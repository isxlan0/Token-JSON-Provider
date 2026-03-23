package claim

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"token-atlas/internal/auth"
	"token-atlas/internal/database"
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
	return runtimecache.CacheJSON(
		s.cache,
		s.userBootstrapCacheKey(requestContext.UserID, requestContext.IsAdmin),
		s.cfg.Cache.MeTTL,
		func() (map[string]any, error) {
			return s.buildBootstrapPayload(ctx, requestContext)
		},
	)
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

	userIDs, err := s.listQueuedUserIDs(ctx)
	if err != nil {
		s.logger.Warn("list queued users for cache warmup", "error", err)
		return
	}
	for _, userID := range userIDs {
		if userID <= 0 {
			continue
		}
		if err := s.refreshUserBootstrapCacheByID(ctx, userID); err != nil {
			s.logger.Warn("warm queued user bootstrap cache", "user_id", userID, "error", err)
		}
	}
}

func (s *Service) primeUserReadCaches(ctx context.Context, userID int64) {
	if ctx.Err() != nil || userID <= 0 {
		return
	}
	if err := s.refreshUserBootstrapCacheByID(ctx, userID); err != nil {
		s.logger.Warn("refresh user bootstrap cache", "user_id", userID, "error", err)
	}
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

func (s *Service) refreshUserBootstrapCache(ctx context.Context, requestContext *auth.RequestContext) error {
	if requestContext == nil || requestContext.IsBanned {
		return nil
	}
	_, err := s.GetBootstrap(ctx, requestContext)
	return err
}

func (s *Service) refreshUserBootstrapCacheByID(ctx context.Context, userID int64) error {
	requestContext, err := s.buildRequestContextForUserID(ctx, userID)
	if err != nil || requestContext == nil {
		return err
	}
	return s.refreshUserBootstrapCache(ctx, requestContext)
}

func (s *Service) buildRequestContextForUserID(ctx context.Context, userID int64) (*auth.RequestContext, error) {
	user, err := s.getUserByID(ctx, userID)
	if err != nil || user == nil {
		return nil, err
	}

	username := user.LinuxDOUsername
	isAdmin := s.isAdminIdentity(user.LinuxDOUserID, username)
	userPayload := auth.UserPayload{
		ID:         user.LinuxDOUserID,
		Username:   username,
		Name:       firstNonEmpty(user.LinuxDOName.String, username),
		TrustLevel: user.TrustLevel,
		IsAdmin:    isAdmin,
	}

	ban, err := s.getActiveBanPayload(ctx, user.LinuxDOUserID)
	if err != nil {
		return nil, err
	}

	return &auth.RequestContext{
		UserID:   user.ID,
		DBUser:   user,
		User:     userPayload,
		IsAdmin:  isAdmin,
		Ban:      ban,
		IsBanned: ban != nil,
	}, nil
}

func (s *Service) getUserByID(ctx context.Context, userID int64) (*database.User, error) {
	return s.getUserByIDQueryer(ctx, s.store.DB(), userID)
}

func (s *Service) getUserByIDQueryer(ctx context.Context, queryer sqlQueryer, userID int64) (*database.User, error) {
	row := queryer.QueryRowContext(ctx, `
		SELECT id, linuxdo_user_id, linuxdo_username, linuxdo_name, trust_level, created_at_ts, last_login_at_ts
		FROM users
		WHERE id = ?
	`, userID)

	var user database.User
	if err := row.Scan(&user.ID, &user.LinuxDOUserID, &user.LinuxDOUsername, &user.LinuxDOName, &user.TrustLevel, &user.CreatedAtTS, &user.LastLoginAtTS); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query user by id %d: %w", userID, err)
	}
	return &user, nil
}

func (s *Service) isAdminIdentity(linuxDOUserID string, username string) bool {
	if _, ok := s.cfg.APIKeys.AdminIdentities.IDs[strings.TrimSpace(linuxDOUserID)]; ok {
		return true
	}
	normalized := strings.ToLower(strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(username), "@")))
	_, ok := s.cfg.APIKeys.AdminIdentities.Usernames[normalized]
	return ok
}
