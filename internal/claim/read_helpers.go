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

	dataSourceLive        = "live"
	dataSourceCache       = "cache"
	dataSourceStale       = "stale"
	dataSourceDeferred    = "deferred"
	dataSourceUnavailable = "unavailable"

	degradedReasonReadTimeout       = "read_timeout"
	degradedReasonDatabaseBusy      = "database_busy"
	degradedReasonPartialData       = "partial_data_unavailable"
	degradedReasonCompatibility     = "compatibility_cache_only"
	degradedReasonDataUnavailable   = "data_unavailable"
	degradedReasonClaimableDeferred = "claimable_now_deferred"
)

type sqlMetricsContextKey struct{}

type sqlMetricsCounter struct {
	count int64
}

func withSQLMetrics(ctx context.Context) (context.Context, *sqlMetricsCounter) {
	counter := &sqlMetricsCounter{}
	return context.WithValue(ctx, sqlMetricsContextKey{}, counter), counter
}

type observedReadStage struct {
	ctx     context.Context
	counter *sqlMetricsCounter
	started time.Time
}

type staleReadEnvelope[T any] struct {
	Value       T      `json:"value"`
	GeneratedAt string `json:"generated_at,omitempty"`
}

type cachedReadResult[T any] struct {
	Value          T
	CacheState     runtimecache.CacheState
	DataSource     string
	GeneratedAt    string
	StaleAt        string
	Degraded       bool
	DegradedReason string
	Duration       time.Duration
	SQLCount       int64
}

func startObservedReadStage(ctx context.Context) observedReadStage {
	stageCtx, counter := withSQLMetrics(ctx)
	return observedReadStage{
		ctx:     stageCtx,
		counter: counter,
		started: time.Now(),
	}
}

func (s observedReadStage) Context() context.Context {
	return s.ctx
}

func (s observedReadStage) Duration() time.Duration {
	if s.started.IsZero() {
		return 0
	}
	return time.Since(s.started)
}

func (s observedReadStage) SQLCount() int64 {
	if s.counter == nil {
		return 0
	}
	return atomic.LoadInt64(&s.counter.count)
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

func sumSQLCounts(values ...int64) int64 {
	total := int64(0)
	for _, value := range values {
		total += value
	}
	return total
}

func cacheStateLabel(state runtimecache.CacheState) string {
	trimmed := strings.TrimSpace(string(state))
	if trimmed == "" {
		return string(runtimecache.CacheStateMiss)
	}
	return trimmed
}

func degradedReasonForError(err error) string {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return degradedReasonReadTimeout
	case isDatabaseBusyError(err):
		return degradedReasonDatabaseBusy
	default:
		return degradedReasonDataUnavailable
	}
}

func loadCachedReadResult[T any](
	ctx context.Context,
	s *Service,
	primaryKey string,
	staleKey string,
	ttlSec int,
	loader func(context.Context) (T, error),
) (cachedReadResult[T], error) {
	var zero T
	stage := startObservedReadStage(ctx)
	liveGeneratedAt := ""
	value, cacheState, err := runtimecache.CacheJSONWithState(
		s.cache,
		primaryKey,
		ttlSec,
		func() (T, error) {
			loaded, loadErr := loader(stage.Context())
			if loadErr != nil {
				return zero, loadErr
			}
			liveGeneratedAt = isoformatNow()
			if s.cache != nil && strings.TrimSpace(staleKey) != "" {
				s.cache.SetJSON(staleKey, staleReadEnvelope[T]{
					Value:       loaded,
					GeneratedAt: liveGeneratedAt,
				}, ttlSec)
			}
			return loaded, nil
		},
	)
	result := cachedReadResult[T]{
		Value:      value,
		CacheState: cacheState,
		Duration:   stage.Duration(),
		SQLCount:   stage.SQLCount(),
	}
	if err != nil {
		if isReadDegradeError(err) && strings.TrimSpace(staleKey) != "" {
			if staleEnvelope, ok := getCachedJSON[staleReadEnvelope[T]](s.cache, staleKey); ok {
				result.Value = staleEnvelope.Value
				result.DataSource = dataSourceStale
				result.GeneratedAt = staleEnvelope.GeneratedAt
				result.StaleAt = staleTimestamp(staleEnvelope.GeneratedAt)
				result.Degraded = true
				result.DegradedReason = degradedReasonForError(err)
				return result, nil
			}
		}
		return result, err
	}

	if cacheState == runtimecache.CacheStateMiss {
		result.DataSource = dataSourceLive
		result.GeneratedAt = liveGeneratedAt
		return result, nil
	}
	result.DataSource = dataSourceCache
	return result, nil
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

func (s *Service) dbStatsLogArgs() []any {
	if s == nil || s.store == nil || s.store.DB() == nil {
		return nil
	}

	stats := s.store.DB().Stats()
	return []any{
		"db_open_connections", stats.OpenConnections,
		"db_in_use_connections", stats.InUse,
		"db_idle_connections", stats.Idle,
		"db_max_open_connections", stats.MaxOpenConnections,
		"db_config_max_idle_connections", s.cfg.Database.MaxIdleConns,
		"db_wait_count", stats.WaitCount,
		"db_wait_duration_ms", durationMillis(stats.WaitDuration),
	}
}

func (s *Service) databaseStatsPayload() map[string]any {
	if s == nil || s.store == nil || s.store.DB() == nil {
		return map[string]any{}
	}

	stats := s.store.DB().Stats()
	return map[string]any{
		"open_connections":     stats.OpenConnections,
		"in_use_connections":   stats.InUse,
		"idle_connections":     stats.Idle,
		"max_open_connections": stats.MaxOpenConnections,
		"max_idle_connections": s.cfg.Database.MaxIdleConns,
		"wait_count":           stats.WaitCount,
		"wait_duration_ms":     durationMillis(stats.WaitDuration),
		"max_idle_closed":      stats.MaxIdleClosed,
		"max_lifetime_closed":  stats.MaxLifetimeClosed,
		"max_idle_time_closed": stats.MaxIdleTimeClosed,
	}
}

func (s *Service) cacheStatsPayload() map[string]any {
	if s == nil || s.cache == nil {
		return map[string]any{
			"backend": "disabled",
		}
	}

	stats := s.cache.StatsSnapshot()
	return map[string]any{
		"backend":             stats.BackendName,
		"local_hits":          stats.LocalHits,
		"backend_hits":        stats.BackendHits,
		"misses":              stats.Misses,
		"sets":                stats.Sets,
		"deletes":             stats.Deletes,
		"scope_version_reads": stats.ScopeVersionReads,
		"scope_bumps":         stats.ScopeBumps,
	}
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
	payload.Debug = s.debugSettingsPayload()
	return payload
}

func (s *Service) defaultRuntimeSnapshotPayload(requestContext *auth.RequestContext) runtimeSnapshotPayload {
	now := isoformatNow()
	return s.mergeRuntimeSnapshotWithRequestContext(runtimeSnapshotPayload{
		Quota:          quotaUsage{},
		Claims:         defaultClaimsSummary(),
		UploadResults:  buildUploadResultsSummaryPayload(nil),
		DataSource:     dataSourceUnavailable,
		GeneratedAt:    now,
		Degraded:       true,
		DegradedReason: degradedReasonDataUnavailable,
	}, requestContext)
}

func intPtr(value int) *int {
	result := value
	return &result
}

func annotateDashboardSection(section map[string]any, dataSource string, generatedAt string, staleAt string, degradedReason string) map[string]any {
	if section == nil {
		section = map[string]any{}
	}
	if strings.TrimSpace(dataSource) != "" {
		section["data_source"] = dataSource
	}
	if strings.TrimSpace(generatedAt) != "" {
		section["generated_at"] = generatedAt
	}
	if strings.TrimSpace(staleAt) != "" {
		section["stale_at"] = staleAt
	}
	if strings.TrimSpace(degradedReason) != "" {
		section["degraded_reason"] = degradedReason
	}
	return section
}

func annotateDashboardPayload(payload map[string]any, dataSource string, generatedAt string, staleAt string, degradedReason string, degraded bool) map[string]any {
	if payload == nil {
		payload = map[string]any{}
	}
	if strings.TrimSpace(dataSource) != "" {
		payload["data_source"] = dataSource
	}
	if strings.TrimSpace(generatedAt) != "" {
		payload["generated_at"] = generatedAt
	}
	if strings.TrimSpace(staleAt) != "" {
		payload["stale_at"] = staleAt
	}
	if strings.TrimSpace(degradedReason) != "" {
		payload["degraded_reason"] = degradedReason
	}
	if degraded {
		payload["degraded"] = true
	}
	return payload
}

func staleTimestamp(generatedAt string) string {
	trimmed := strings.TrimSpace(generatedAt)
	if trimmed != "" {
		return trimmed
	}
	return isoformatNow()
}

func withQueueStatusMetadata(payload queueStatusPayload, dataSource string, generatedAt string, staleAt string, degradedReason string, degraded bool) queueStatusPayload {
	payload.DataSource = dataSource
	payload.GeneratedAt = generatedAt
	payload.StaleAt = staleAt
	payload.DegradedReason = degradedReason
	payload.Degraded = degraded
	if payload.ClaimableNow == nil && strings.TrimSpace(payload.ClaimableNowState) == "" {
		if strings.EqualFold(strings.TrimSpace(dataSource), dataSourceUnavailable) {
			payload.ClaimableNowState = dataSourceUnavailable
		} else {
			payload.ClaimableNowState = dataSourceDeferred
		}
	}
	return payload
}

func stripQueueStatusAuxiliaryFields(payload queueStatusPayload) queueStatusPayload {
	payload.ClaimableNow = nil
	payload.ClaimableNowState = ""
	payload.ClaimableNowUpdatedAt = ""
	return payload
}

func applyClaimableNowSnapshot(payload queueStatusPayload, snapshot storedClaimableNowSnapshot, dataSource string) queueStatusPayload {
	payload.ClaimableNow = intPtr(snapshot.ClaimableNow)
	payload.ClaimableNowState = dataSource
	payload.ClaimableNowUpdatedAt = snapshot.GeneratedAt
	return payload
}

func claimableNowPayloadFromSnapshot(snapshot storedClaimableNowSnapshot, dataSource string, staleAt string, degradedReason string, degraded bool) claimableNowPayload {
	count := snapshot.ClaimableNow
	return claimableNowPayload{
		ClaimableNow:          &count,
		ClaimableNowState:     dataSource,
		ClaimableNowUpdatedAt: snapshot.GeneratedAt,
		DataSource:            dataSource,
		GeneratedAt:           snapshot.GeneratedAt,
		StaleAt:               staleAt,
		DegradedReason:        degradedReason,
		Degraded:              degraded,
	}
}

func unavailableClaimableNowPayload(reason string) claimableNowPayload {
	now := isoformatNow()
	return claimableNowPayload{
		ClaimableNow:      nil,
		ClaimableNowState: dataSourceUnavailable,
		DataSource:        dataSourceUnavailable,
		GeneratedAt:       now,
		Degraded:          true,
		DegradedReason:    reason,
	}
}

func (s *Service) defaultQueueStatusPayload() queueStatusPayload {
	now := isoformatNow()
	return withQueueStatusMetadata(queueStatusPayload{
		Queued:          false,
		TotalQueued:     0,
		AvailableTokens: 0,
		ClaimableNow:    nil,
	}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable, true)
}

func (s *Service) defaultDashboardSummaryPayload(options dashboardSummaryOptions) map[string]any {
	normalized := normalizeDashboardSummaryOptions(options)
	policy := s.buildInventoryPolicy(map[string]int{
		"total":     0,
		"available": 0,
		"unclaimed": 0,
	})
	now := isoformatNow()

	return annotateDashboardPayload(map[string]any{
		"stats": annotateDashboardSection(map[string]any{
			"total_tokens":          nil,
			"available_tokens":      nil,
			"claimed_total":         nil,
			"claimed_unique":        nil,
			"others_claimed_total":  nil,
			"others_claimed_unique": nil,
		}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable),
		"leaderboard": annotateDashboardSection(map[string]any{
			"window": normalized.LeaderboardWindow,
			"items":  []map[string]any{},
		}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable),
		"recent": annotateDashboardSection(map[string]any{
			"items": []map[string]any{},
		}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable),
		"contributors": annotateDashboardSection(map[string]any{
			"items": []map[string]any{},
		}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable),
		"recent_contributors": annotateDashboardSection(map[string]any{
			"items": []map[string]any{},
		}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable),
		"trends": annotateDashboardSection(map[string]any{
			"window": normalized.WindowSeconds,
			"bucket": normalized.BucketSeconds,
			"series": []map[string]any{},
		}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable),
		"system": annotateDashboardSection(map[string]any{
			"inventory": map[string]any{
				"total":     nil,
				"available": nil,
				"unclaimed": nil,
			},
			"queue": map[string]any{
				"total": nil,
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
		}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable),
	}, dataSourceUnavailable, now, "", degradedReasonDataUnavailable, true)
}

func (s *Service) defaultBootstrapPayload(requestContext *auth.RequestContext) map[string]any {
	now := isoformatNow()

	return map[string]any{
		"profile":        s.defaultProfilePayload(requestContext),
		"dashboard":      s.defaultDashboardSummaryPayload(dashboardSummaryOptions{Window: defaultDashboardWindow, Bucket: defaultDashboardBucket}),
		"claim_realtime": s.emptyClaimRealtimeSnapshot(),
		"queue_status":   s.defaultQueueStatusPayload(),
		"upload_results": buildUploadResultsSummaryPayload(nil),
		"debug":          s.debugSettingsPayload(),
		"sources": map[string]any{
			"profile":        dataSourceUnavailable,
			"dashboard":      dataSourceUnavailable,
			"queue_status":   dataSourceUnavailable,
			"claim_realtime": dataSourceUnavailable,
			"upload_results": dataSourceUnavailable,
		},
		"data_source":     dataSourceUnavailable,
		"generated_at":    now,
		"degraded_reason": degradedReasonCompatibility,
		"degraded":        true,
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

func (s *Service) getCachedStaleRuntimeSnapshot(userID int64) (staleReadEnvelope[runtimeSnapshotPayload], bool) {
	return getCachedJSON[staleReadEnvelope[runtimeSnapshotPayload]](s.cache, s.userRuntimeSnapshotStaleCacheKey(userID))
}

func (s *Service) getCachedProfile(userID int64, isAdmin bool) (profilePayload, bool) {
	return getCachedJSON[profilePayload](s.cache, s.userProfileCacheKey(userID, isAdmin))
}

func (s *Service) getCachedDashboardSummary(userID int64, options dashboardSummaryOptions) (map[string]any, bool) {
	return getCachedJSON[map[string]any](s.cache, s.dashboardSummaryCacheKey(userID, options))
}

func (s *Service) getCachedStaleDashboardSummary(userID int64, options dashboardSummaryOptions) (staleReadEnvelope[map[string]any], bool) {
	return getCachedJSON[staleReadEnvelope[map[string]any]](s.cache, s.dashboardSummaryStaleCacheKey(userID, options))
}

func (s *Service) getCachedStaleQueueStatus(userID int64) (queueStatusPayload, bool) {
	return getCachedJSON[queueStatusPayload](s.cache, s.userQueueStaleCacheKey(userID))
}

func (s *Service) getCachedClaimRealtime(userID int64) (claimRealtimeSnapshot, bool) {
	return getCachedJSON[claimRealtimeSnapshot](s.cache, s.userClaimRealtimeCacheKey(userID))
}

func (s *Service) getCachedClaimableNow(userID int64) (storedClaimableNowSnapshot, bool) {
	return getCachedJSON[storedClaimableNowSnapshot](s.cache, s.userClaimableNowCacheKey(userID))
}

func (s *Service) getCachedStaleClaimableNow(userID int64) (storedClaimableNowSnapshot, bool) {
	return getCachedJSON[storedClaimableNowSnapshot](s.cache, s.userClaimableNowStaleCacheKey(userID))
}
