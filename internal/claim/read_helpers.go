package claim

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"token-atlas/internal/auth"
	"token-atlas/internal/runtimecache"
)

const (
	runtimeSnapshotReadTimeout  = 1500 * time.Millisecond
	queueStatusReadTimeout      = 1500 * time.Millisecond
	dashboardSummaryReadTimeout = 2500 * time.Millisecond
	bootstrapReadTimeout        = 2500 * time.Millisecond
)

type sqlMetricsContextKey struct{}

type sqlMetricsCounter struct {
	count int64
}

func withSQLMetrics(ctx context.Context) (context.Context, *sqlMetricsCounter) {
	if existing := sqlMetricsFromContext(ctx); existing != nil {
		return ctx, existing
	}
	counter := &sqlMetricsCounter{}
	return context.WithValue(ctx, sqlMetricsContextKey{}, counter), counter
}

func sqlMetricsFromContext(ctx context.Context) *sqlMetricsCounter {
	if ctx == nil {
		return nil
	}
	counter, _ := ctx.Value(sqlMetricsContextKey{}).(*sqlMetricsCounter)
	return counter
}

func incrementSQLCount(ctx context.Context) {
	counter := sqlMetricsFromContext(ctx)
	if counter == nil {
		return
	}
	atomic.AddInt64(&counter.count, 1)
}

func sqlCount(ctx context.Context) int64 {
	counter := sqlMetricsFromContext(ctx)
	if counter == nil {
		return 0
	}
	return atomic.LoadInt64(&counter.count)
}

func queryContextCounted(ctx context.Context, queryer sqlQueryer, query string, args ...any) (*sql.Rows, error) {
	incrementSQLCount(ctx)
	return queryer.QueryContext(ctx, query, args...)
}

func queryRowContextCounted(ctx context.Context, queryer sqlQueryer, query string, args ...any) *sql.Row {
	incrementSQLCount(ctx)
	return queryer.QueryRowContext(ctx, query, args...)
}

func execContextCounted(ctx context.Context, queryer sqlQueryer, query string, args ...any) (sql.Result, error) {
	incrementSQLCount(ctx)
	return queryer.ExecContext(ctx, query, args...)
}

func durationMillis(duration time.Duration) int64 {
	return duration.Milliseconds()
}

func isReadDegradeError(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded) || isDatabaseBusyError(err)
}

func (s *Service) defaultUploadPolicyPayload() map[string]int {
	return map[string]int{
		"max_files_per_request": s.cfg.Upload.MaxFilesPerRequest,
		"max_file_size_bytes":   s.cfg.Upload.MaxFileSizeBytes,
		"max_success_per_hour":  s.cfg.Upload.MaxSuccessPerHour,
		"min_trust_level":       s.cfg.LinuxDO.MinTrustLevel,
	}
}

func profileUserPayloadFromRequestContext(requestContext *auth.RequestContext) *profileUserPayload {
	if requestContext == nil {
		return nil
	}

	userID := strings.TrimSpace(requestContext.User.ID)
	if userID == "" && requestContext.UserID > 0 {
		userID = strconv.FormatInt(requestContext.UserID, 10)
	}

	return &profileUserPayload{
		ID:         userID,
		Username:   requestContext.User.Username,
		Name:       requestContext.User.Name,
		TrustLevel: requestContext.User.TrustLevel,
		IsAdmin:    requestContext.IsAdmin || requestContext.User.IsAdmin,
		IsBanned:   false,
	}
}

func defaultClaimsSummary() map[string]int {
	return map[string]int{
		"total":  0,
		"unique": 0,
	}
}

func (s *Service) defaultProfilePayload(requestContext *auth.RequestContext) profilePayload {
	user := profileUserPayload{}
	if payload := profileUserPayloadFromRequestContext(requestContext); payload != nil {
		user = *payload
	}

	return profilePayload{
		User:   user,
		Quota:  quotaUsage{},
		Claims: defaultClaimsSummary(),
		APIKeys: apiKeySummaryPayload{
			Limit:  s.cfg.APIKeys.MaxPerUser,
			Active: 0,
		},
		Uploads: s.defaultUploadPolicyPayload(),
	}
}

func (s *Service) mergeRuntimeSnapshotWithRequestContext(payload runtimeSnapshotPayload, requestContext *auth.RequestContext) runtimeSnapshotPayload {
	if payload.User == nil {
		payload.User = profileUserPayloadFromRequestContext(requestContext)
	}
	if payload.Claims == nil {
		payload.Claims = defaultClaimsSummary()
	}
	if payload.APIKeys == nil {
		payload.APIKeys = map[string]apiKeySummaryPayload{
			"summary": {
				Limit:  s.cfg.APIKeys.MaxPerUser,
				Active: 0,
			},
		}
	}
	if payload.Uploads == nil {
		payload.Uploads = s.defaultUploadPolicyPayload()
	}
	if payload.UploadResults == nil {
		payload.UploadResults = buildUploadResultsSummaryPayload(nil)
	}
	return payload
}

func (s *Service) defaultRuntimeSnapshotPayload(requestContext *auth.RequestContext) runtimeSnapshotPayload {
	return s.mergeRuntimeSnapshotWithRequestContext(runtimeSnapshotPayload{
		Quota:         quotaUsage{},
		Claims:        defaultClaimsSummary(),
		UploadResults: buildUploadResultsSummaryPayload(nil),
		Degraded:      true,
	}, requestContext)
}

func (s *Service) defaultQueueStatusPayload() queueStatusPayload {
	return queueStatusPayload{
		Queued:          false,
		TotalQueued:     0,
		AvailableTokens: 0,
		ClaimableNow:    0,
	}
}

func (s *Service) defaultDashboardSummaryPayload(options dashboardSummaryOptions) map[string]any {
	normalized := normalizeDashboardSummaryOptions(options)
	policy := s.buildInventoryPolicy(map[string]int{
		"total":     0,
		"available": 0,
		"unclaimed": 0,
	})

	return map[string]any{
		"stats": map[string]int{
			"total_tokens":          0,
			"available_tokens":      0,
			"claimed_total":         0,
			"claimed_unique":        0,
			"others_claimed_total":  0,
			"others_claimed_unique": 0,
		},
		"leaderboard": map[string]any{
			"window": normalized.LeaderboardWindow,
			"items":  []map[string]any{},
		},
		"recent": map[string]any{
			"items": []map[string]any{},
		},
		"contributors": map[string]any{
			"items": []map[string]any{},
		},
		"recent_contributors": map[string]any{
			"items": []map[string]any{},
		},
		"trends": map[string]any{
			"window": normalized.WindowSeconds,
			"bucket": normalized.BucketSeconds,
			"series": []map[string]any{},
		},
		"system": map[string]any{
			"inventory": map[string]int{
				"total":     0,
				"available": 0,
				"unclaimed": 0,
			},
			"queue": map[string]any{
				"total": 0,
			},
			"health": map[string]any{
				"status":       policy.Status,
				"hourly_limit": policy.HourlyLimit,
				"max_claims":   policy.MaxClaims,
				"thresholds":   policy.Thresholds,
			},
			"index": map[string]any{
				"updated_at": s.systemIndexTimestamp(),
			},
		},
		"degraded": true,
	}
}

func (s *Service) defaultBootstrapPayload(requestContext *auth.RequestContext) map[string]any {
	queueStatus := s.defaultQueueStatusPayload()
	if requestContext != nil {
		if cached, ok := s.getCachedQueueStatus(requestContext.UserID); ok {
			queueStatus = cached
		}
	}
	queueStatus.Degraded = true

	return map[string]any{
		"profile":        s.defaultProfilePayload(requestContext),
		"dashboard":      s.defaultDashboardSummaryPayload(dashboardSummaryOptions{Window: defaultDashboardWindow, Bucket: defaultDashboardBucket}),
		"claim_realtime": s.emptyClaimRealtimeSnapshot(),
		"queue_status":   queueStatus,
		"upload_results": buildUploadResultsSummaryPayload(nil),
		"degraded":       true,
	}
}

func getCachedJSON[T any](cache *runtimecache.AppCache, key string) (T, bool) {
	var zero T
	if cache == nil {
		return zero, false
	}

	var payload T
	if !cache.GetJSON(key, &payload) {
		return zero, false
	}
	return payload, true
}

func (s *Service) getCachedRuntimeSnapshot(userID int64) (runtimeSnapshotPayload, bool) {
	return getCachedJSON[runtimeSnapshotPayload](s.cache, s.userRuntimeSnapshotCacheKey(userID))
}

func (s *Service) getCachedDashboardSummary(userID int64, options dashboardSummaryOptions) (map[string]any, bool) {
	return getCachedJSON[map[string]any](s.cache, s.dashboardSummaryCacheKey(userID, options))
}

func (s *Service) getCachedBootstrap(userID int64, isAdmin bool) (map[string]any, bool) {
	return getCachedJSON[map[string]any](s.cache, s.userBootstrapCacheKey(userID, isAdmin))
}
