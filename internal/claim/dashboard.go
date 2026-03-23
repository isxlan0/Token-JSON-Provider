package claim

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"
)

type dashboardSummaryOptions struct {
	Window                 string
	Bucket                 string
	LeaderboardWindow      string
	LeaderboardLimit       int
	RecentLimit            int
	ContributorLimit       int
	RecentContributorLimit int
}

type normalizedDashboardSummaryOptions struct {
	WindowSeconds          int
	BucketSeconds          int
	LeaderboardWindow      int
	LeaderboardLimit       int
	RecentLimit            int
	ContributorLimit       int
	RecentContributorLimit int
}

func normalizeDashboardSummaryOptions(options dashboardSummaryOptions) normalizedDashboardSummaryOptions {
	return normalizedDashboardSummaryOptions{
		WindowSeconds:          parseWindowSeconds(options.Window, 7*24*3600, 14*24*3600),
		BucketSeconds:          parseBucketSeconds(options.Bucket, 3600),
		LeaderboardWindow:      parseWindowSeconds(options.LeaderboardWindow, 24*3600, 7*24*3600),
		LeaderboardLimit:       clampInt(options.LeaderboardLimit, 1, 10, 10),
		RecentLimit:            clampInt(options.RecentLimit, 1, 10, 10),
		ContributorLimit:       clampInt(options.ContributorLimit, 1, 10, 10),
		RecentContributorLimit: clampInt(options.RecentContributorLimit, 1, 10, 10),
	}
}

func (s *Service) GetDashboardSummaryWithOptions(ctx context.Context, userID int64, options dashboardSummaryOptions) (map[string]any, error) {
	normalized := normalizeDashboardSummaryOptions(options)
	startedAt := time.Now()
	var (
		statsDuration              time.Duration
		systemDuration             time.Duration
		leaderboardDuration        time.Duration
		recentDuration             time.Duration
		contributorsDuration       time.Duration
		recentContributorsDuration time.Duration
		trendsDuration             time.Duration
		statsSQLCount              int64
		systemSQLCount             int64
		leaderboardSQLCount        int64
		recentSQLCount             int64
		contributorsSQLCount       int64
		recentContributorsSQLCount int64
		trendsSQLCount             int64
		statsDataSource            string
		systemDataSource           string
		leaderboardDataSource      string
		recentDataSource           string
		contributorsDataSource     string
		recentContributorsSource   string
		trendsDataSource           string
		statsError                 error
		systemError                error
		leaderboardError           error
		recentError                error
		contributorsError          error
		recentContributorsError    error
		trendsError                error
	)

	payloadResult, err := loadCachedReadResult(
		ctx,
		s,
		s.dashboardSummaryCacheKey(userID, options),
		s.dashboardSummaryStaleCacheKey(userID, options),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) (map[string]any, error) {
			var (
				statsResult              cachedReadResult[map[string]int]
				systemResult             cachedReadResult[map[string]any]
				leaderboardResult        cachedReadResult[map[string]any]
				recentResult             cachedReadResult[[]map[string]any]
				contributorsResult       cachedReadResult[[]map[string]any]
				recentContributorsResult cachedReadResult[[]map[string]any]
				trendsResult             cachedReadResult[[]map[string]any]
			)
			var wg sync.WaitGroup
			wg.Add(7)

			go func() {
				defer wg.Done()
				result, sectionErr := s.getDashboardStatsObserved(loadCtx, userID)
				statsResult = result
				statsError = sectionErr
			}()
			go func() {
				defer wg.Done()
				result, sectionErr := s.getSystemStatusObserved(loadCtx)
				systemResult = result
				systemError = sectionErr
			}()
			go func() {
				defer wg.Done()
				result, sectionErr := s.getLeaderboardObserved(loadCtx, normalized.LeaderboardWindow, normalized.LeaderboardLimit)
				leaderboardResult = result
				leaderboardError = sectionErr
			}()
			go func() {
				defer wg.Done()
				result, sectionErr := s.getRecentClaimsObserved(loadCtx, normalized.RecentLimit)
				recentResult = result
				recentError = sectionErr
			}()
			go func() {
				defer wg.Done()
				result, sectionErr := s.getContributorLeaderboardObserved(loadCtx, normalized.ContributorLimit)
				contributorsResult = result
				contributorsError = sectionErr
			}()
			go func() {
				defer wg.Done()
				result, sectionErr := s.getRecentContributorsObserved(loadCtx, normalized.RecentContributorLimit)
				recentContributorsResult = result
				recentContributorsError = sectionErr
			}()
			go func() {
				defer wg.Done()
				result, sectionErr := s.getClaimTrendsObserved(loadCtx, normalized.WindowSeconds, normalized.BucketSeconds)
				trendsResult = result
				trendsError = sectionErr
			}()
			wg.Wait()

			statsDuration = statsResult.Duration
			systemDuration = systemResult.Duration
			leaderboardDuration = leaderboardResult.Duration
			recentDuration = recentResult.Duration
			contributorsDuration = contributorsResult.Duration
			recentContributorsDuration = recentContributorsResult.Duration
			trendsDuration = trendsResult.Duration
			statsSQLCount = statsResult.SQLCount
			systemSQLCount = systemResult.SQLCount
			leaderboardSQLCount = leaderboardResult.SQLCount
			recentSQLCount = recentResult.SQLCount
			contributorsSQLCount = contributorsResult.SQLCount
			recentContributorsSQLCount = recentContributorsResult.SQLCount
			trendsSQLCount = trendsResult.SQLCount
			statsDataSource = statsResult.DataSource
			systemDataSource = systemResult.DataSource
			leaderboardDataSource = leaderboardResult.DataSource
			recentDataSource = recentResult.DataSource
			contributorsDataSource = contributorsResult.DataSource
			recentContributorsSource = recentContributorsResult.DataSource
			trendsDataSource = trendsResult.DataSource

			if statsError != nil {
				statsResult = cachedReadResult[map[string]int]{
					Value:          map[string]int{},
					DataSource:     dataSourceUnavailable,
					Degraded:       true,
					DegradedReason: degradedReasonForError(statsError),
					Duration:       statsResult.Duration,
					SQLCount:       statsResult.SQLCount,
				}
				statsDataSource = statsResult.DataSource
			}
			if systemError != nil {
				systemResult = cachedReadResult[map[string]any]{
					Value:          map[string]any{},
					DataSource:     dataSourceUnavailable,
					Degraded:       true,
					DegradedReason: degradedReasonForError(systemError),
					Duration:       systemResult.Duration,
					SQLCount:       systemResult.SQLCount,
				}
				systemDataSource = systemResult.DataSource
			}
			if leaderboardError != nil {
				leaderboardResult = cachedReadResult[map[string]any]{
					Value:          map[string]any{},
					DataSource:     dataSourceUnavailable,
					Degraded:       true,
					DegradedReason: degradedReasonForError(leaderboardError),
					Duration:       leaderboardResult.Duration,
					SQLCount:       leaderboardResult.SQLCount,
				}
				leaderboardDataSource = leaderboardResult.DataSource
			}
			if recentError != nil {
				recentResult = cachedReadResult[[]map[string]any]{
					Value:          []map[string]any{},
					DataSource:     dataSourceUnavailable,
					Degraded:       true,
					DegradedReason: degradedReasonForError(recentError),
					Duration:       recentResult.Duration,
					SQLCount:       recentResult.SQLCount,
				}
				recentDataSource = recentResult.DataSource
			}
			if contributorsError != nil {
				contributorsResult = cachedReadResult[[]map[string]any]{
					Value:          []map[string]any{},
					DataSource:     dataSourceUnavailable,
					Degraded:       true,
					DegradedReason: degradedReasonForError(contributorsError),
					Duration:       contributorsResult.Duration,
					SQLCount:       contributorsResult.SQLCount,
				}
				contributorsDataSource = contributorsResult.DataSource
			}
			if recentContributorsError != nil {
				recentContributorsResult = cachedReadResult[[]map[string]any]{
					Value:          []map[string]any{},
					DataSource:     dataSourceUnavailable,
					Degraded:       true,
					DegradedReason: degradedReasonForError(recentContributorsError),
					Duration:       recentContributorsResult.Duration,
					SQLCount:       recentContributorsResult.SQLCount,
				}
				recentContributorsSource = recentContributorsResult.DataSource
			}
			if trendsError != nil {
				trendsResult = cachedReadResult[[]map[string]any]{
					Value:          []map[string]any{},
					DataSource:     dataSourceUnavailable,
					Degraded:       true,
					DegradedReason: degradedReasonForError(trendsError),
					Duration:       trendsResult.Duration,
					SQLCount:       trendsResult.SQLCount,
				}
				trendsDataSource = trendsResult.DataSource
			}

			statsPayload := annotateDashboardSection(map[string]any{
				"total_tokens":          statsResult.Value["total_tokens"],
				"available_tokens":      statsResult.Value["available_tokens"],
				"claimed_total":         statsResult.Value["claimed_total"],
				"claimed_unique":        statsResult.Value["claimed_unique"],
				"others_claimed_total":  statsResult.Value["others_claimed_total"],
				"others_claimed_unique": statsResult.Value["others_claimed_unique"],
			}, statsResult.DataSource, statsResult.GeneratedAt, statsResult.StaleAt, statsResult.DegradedReason)
			leaderboardPayload := annotateDashboardSection(leaderboardResult.Value, leaderboardResult.DataSource, leaderboardResult.GeneratedAt, leaderboardResult.StaleAt, leaderboardResult.DegradedReason)
			recentPayload := annotateDashboardSection(map[string]any{
				"items": recentResult.Value,
			}, recentResult.DataSource, recentResult.GeneratedAt, recentResult.StaleAt, recentResult.DegradedReason)
			contributorsPayload := annotateDashboardSection(map[string]any{
				"items": contributorsResult.Value,
			}, contributorsResult.DataSource, contributorsResult.GeneratedAt, contributorsResult.StaleAt, contributorsResult.DegradedReason)
			recentContributorsPayload := annotateDashboardSection(map[string]any{
				"items": recentContributorsResult.Value,
			}, recentContributorsResult.DataSource, recentContributorsResult.GeneratedAt, recentContributorsResult.StaleAt, recentContributorsResult.DegradedReason)
			trendsPayload := annotateDashboardSection(map[string]any{
				"window": normalized.WindowSeconds,
				"bucket": normalized.BucketSeconds,
				"series": trendsResult.Value,
			}, trendsResult.DataSource, trendsResult.GeneratedAt, trendsResult.StaleAt, trendsResult.DegradedReason)
			systemPayload := annotateDashboardSection(systemResult.Value, systemResult.DataSource, systemResult.GeneratedAt, systemResult.StaleAt, systemResult.DegradedReason)

			degraded := statsResult.Degraded ||
				systemResult.Degraded ||
				leaderboardResult.Degraded ||
				recentResult.Degraded ||
				contributorsResult.Degraded ||
				recentContributorsResult.Degraded ||
				trendsResult.Degraded
			degradedReason := ""
			if degraded {
				degradedReason = degradedReasonPartialData
			}

			return annotateDashboardPayload(map[string]any{
				"stats":               statsPayload,
				"leaderboard":         leaderboardPayload,
				"recent":              recentPayload,
				"contributors":        contributorsPayload,
				"recent_contributors": recentContributorsPayload,
				"trends":              trendsPayload,
				"system":              systemPayload,
			}, dataSourceLive, isoformatNow(), "", degradedReason, degraded), nil
		},
	)
	payload := payloadResult.Value
	if payload != nil {
		payloadDegraded, _ := payload["degraded"].(bool)
		payload = annotateDashboardPayload(
			payload,
			payloadResult.DataSource,
			payloadResult.GeneratedAt,
			payloadResult.StaleAt,
			payloadResult.DegradedReason,
			payloadDegraded || payloadResult.Degraded,
		)
	}

	s.logger.Info(
		"get dashboard summary",
		"user_id", userID,
		"cache_state", cacheStateLabel(payloadResult.CacheState),
		"data_source", payloadResult.DataSource,
		"total_ms", durationMillis(time.Since(startedAt)),
		"stats_ms", durationMillis(statsDuration),
		"system_ms", durationMillis(systemDuration),
		"leaderboard_ms", durationMillis(leaderboardDuration),
		"recent_ms", durationMillis(recentDuration),
		"contributors_ms", durationMillis(contributorsDuration),
		"recent_contributors_ms", durationMillis(recentContributorsDuration),
		"trends_ms", durationMillis(trendsDuration),
		"stats_data_source", statsDataSource,
		"system_data_source", systemDataSource,
		"leaderboard_data_source", leaderboardDataSource,
		"recent_data_source", recentDataSource,
		"contributors_data_source", contributorsDataSource,
		"recent_contributors_data_source", recentContributorsSource,
		"trends_data_source", trendsDataSource,
		"stats_sql_count", statsSQLCount,
		"system_sql_count", systemSQLCount,
		"leaderboard_sql_count", leaderboardSQLCount,
		"recent_sql_count", recentSQLCount,
		"contributors_sql_count", contributorsSQLCount,
		"recent_contributors_sql_count", recentContributorsSQLCount,
		"trends_sql_count", trendsSQLCount,
		"cumulative_sql_count", sumSQLCounts(
			statsSQLCount,
			systemSQLCount,
			leaderboardSQLCount,
			recentSQLCount,
			contributorsSQLCount,
			recentContributorsSQLCount,
			trendsSQLCount,
		),
		"stats_error", statsError,
		"system_error", systemError,
		"leaderboard_error", leaderboardError,
		"recent_error", recentError,
		"contributors_error", contributorsError,
		"recent_contributors_error", recentContributorsError,
		"trends_error", trendsError,
		"error", err,
	)
	return payload, err
}

func (s *Service) loadLeaderboard(ctx context.Context, windowSeconds int, limit int) (map[string]any, error) {
	rows, err := queryContextCounted(ctx, s.store.DB(), `
			SELECT users.linuxdo_user_id,
			       users.linuxdo_username,
			       users.linuxdo_name,
			       COUNT(*) AS cnt
			FROM token_claims
			JOIN users ON users.id = token_claims.user_id
			WHERE token_claims.claimed_at_ts >= ?
			GROUP BY users.id, users.linuxdo_user_id, users.linuxdo_username, users.linuxdo_name
			ORDER BY cnt DESC, users.linuxdo_username ASC, users.linuxdo_user_id ASC
			LIMIT ?
		`, time.Now().Unix()-int64(windowSeconds), limit)
	if err != nil {
		return nil, fmt.Errorf("query dashboard leaderboard: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			userID   string
			username string
			name     sql.NullString
			count    int
		)
		if err := rows.Scan(&userID, &username, &name, &count); err != nil {
			return nil, fmt.Errorf("scan dashboard leaderboard row: %w", err)
		}
		items = append(items, map[string]any{
			"user_id":  userID,
			"username": username,
			"name":     firstNonEmpty(name.String, username),
			"count":    count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dashboard leaderboard rows: %w", err)
	}

	return map[string]any{
		"window": windowSeconds,
		"items":  items,
	}, nil
}

func (s *Service) getLeaderboardObserved(ctx context.Context, windowSeconds int, limit int) (cachedReadResult[map[string]any], error) {
	return loadCachedReadResult(
		ctx,
		s,
		s.dashboardCacheKey("dashboard-leaderboard", nil, windowSeconds, limit),
		s.dashboardStaleCacheKey("dashboard-leaderboard", nil, windowSeconds, limit),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) (map[string]any, error) {
			return s.loadLeaderboard(loadCtx, windowSeconds, limit)
		},
	)
}

func (s *Service) getLeaderboard(ctx context.Context, windowSeconds int, limit int) (map[string]any, error) {
	result, err := s.getLeaderboardObserved(ctx, windowSeconds, limit)
	return result.Value, err
}

func (s *Service) loadRecentClaims(ctx context.Context, limit int) ([]map[string]any, error) {
	rows, err := queryContextCounted(ctx, s.store.DB(), `
			SELECT MIN(token_claims.id) AS first_claim_id,
			       token_claims.request_id,
			       MAX(token_claims.claimed_at_ts) AS claimed_at_ts,
			       users.linuxdo_username,
			       users.linuxdo_name,
			       COUNT(*) AS cnt
			FROM token_claims
			JOIN users ON users.id = token_claims.user_id
			GROUP BY token_claims.request_id, users.id, users.linuxdo_username, users.linuxdo_name
			ORDER BY claimed_at_ts DESC, first_claim_id DESC
			LIMIT ?
		`, limit)
	if err != nil {
		return nil, fmt.Errorf("query dashboard recent claims: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			firstClaimID int64
			requestID    string
			claimedAt    int64
			username     string
			name         sql.NullString
			count        int
		)
		if err := rows.Scan(&firstClaimID, &requestID, &claimedAt, &username, &name, &count); err != nil {
			return nil, fmt.Errorf("scan dashboard recent row: %w", err)
		}
		_ = firstClaimID
		_ = requestID
		items = append(items, map[string]any{
			"username":   username,
			"name":       firstNonEmpty(name.String, username),
			"count":      count,
			"claimed_at": isoformatFromTS(claimedAt),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate dashboard recent rows: %w", err)
	}
	return items, nil
}

func (s *Service) getRecentClaimsObserved(ctx context.Context, limit int) (cachedReadResult[[]map[string]any], error) {
	return loadCachedReadResult(
		ctx,
		s,
		s.dashboardCacheKey("dashboard-recent", nil, limit),
		s.dashboardStaleCacheKey("dashboard-recent", nil, limit),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) ([]map[string]any, error) {
			return s.loadRecentClaims(loadCtx, limit)
		},
	)
}

func (s *Service) getRecentClaims(ctx context.Context, limit int) ([]map[string]any, error) {
	result, err := s.getRecentClaimsObserved(ctx, limit)
	return result.Value, err
}

func (s *Service) loadContributorLeaderboard(ctx context.Context, limit int) ([]map[string]any, error) {
	rows, err := queryContextCounted(ctx, s.store.DB(), `
			SELECT provider_user_id,
			       provider_username,
			       provider_name,
			       COUNT(*) AS cnt
			FROM tokens
			WHERE provider_user_id IS NOT NULL
			  AND provider_user_id != ''
			GROUP BY provider_user_id, provider_username, provider_name
			ORDER BY cnt DESC, provider_username ASC, provider_user_id ASC
			LIMIT ?
		`, limit)
	if err != nil {
		return nil, fmt.Errorf("query contributor leaderboard: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			userID   string
			username sql.NullString
			name     sql.NullString
			count    int
		)
		if err := rows.Scan(&userID, &username, &name, &count); err != nil {
			return nil, fmt.Errorf("scan contributor leaderboard row: %w", err)
		}
		items = append(items, map[string]any{
			"user_id":  userID,
			"username": username.String,
			"name":     firstNonEmpty(name.String, username.String, userID),
			"count":    count,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contributor leaderboard rows: %w", err)
	}
	return items, nil
}

func (s *Service) getContributorLeaderboardObserved(ctx context.Context, limit int) (cachedReadResult[[]map[string]any], error) {
	return loadCachedReadResult(
		ctx,
		s,
		s.dashboardCacheKey("dashboard-contributors", nil, limit),
		s.dashboardStaleCacheKey("dashboard-contributors", nil, limit),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) ([]map[string]any, error) {
			return s.loadContributorLeaderboard(loadCtx, limit)
		},
	)
}

func (s *Service) getContributorLeaderboard(ctx context.Context, limit int) ([]map[string]any, error) {
	result, err := s.getContributorLeaderboardObserved(ctx, limit)
	return result.Value, err
}

func (s *Service) loadRecentContributors(ctx context.Context, limit int) ([]map[string]any, error) {
	rows, err := queryContextCounted(ctx, s.store.DB(), `
			SELECT provider_user_id,
			       provider_username,
			       provider_name,
			       COUNT(*) AS cnt,
			       MAX(uploaded_at_ts) AS uploaded_at_ts
			FROM tokens
			WHERE provider_user_id IS NOT NULL
			  AND provider_user_id != ''
			GROUP BY provider_user_id, provider_username, provider_name
			ORDER BY uploaded_at_ts DESC, provider_username ASC, provider_user_id ASC
			LIMIT ?
		`, limit)
	if err != nil {
		return nil, fmt.Errorf("query recent contributors: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			userID     string
			username   sql.NullString
			name       sql.NullString
			count      int
			uploadedAt sql.NullInt64
		)
		if err := rows.Scan(&userID, &username, &name, &count, &uploadedAt); err != nil {
			return nil, fmt.Errorf("scan recent contributor row: %w", err)
		}

		item := map[string]any{
			"user_id":  userID,
			"username": username.String,
			"name":     firstNonEmpty(name.String, username.String, userID),
			"count":    count,
		}
		if uploadedAt.Valid {
			item["uploaded_at"] = isoformatFromTS(uploadedAt.Int64)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate recent contributor rows: %w", err)
	}

	return items, nil
}

func (s *Service) getRecentContributorsObserved(ctx context.Context, limit int) (cachedReadResult[[]map[string]any], error) {
	return loadCachedReadResult(
		ctx,
		s,
		s.dashboardCacheKey("dashboard-recent-contributors", nil, limit),
		s.dashboardStaleCacheKey("dashboard-recent-contributors", nil, limit),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) ([]map[string]any, error) {
			return s.loadRecentContributors(loadCtx, limit)
		},
	)
}

func (s *Service) getRecentContributors(ctx context.Context, limit int) ([]map[string]any, error) {
	result, err := s.getRecentContributorsObserved(ctx, limit)
	return result.Value, err
}

func (s *Service) loadClaimTrends(ctx context.Context, windowSeconds int, bucketSeconds int) ([]map[string]any, error) {
	startTS := time.Now().Unix() - int64(windowSeconds)
	startBucket := (startTS / int64(bucketSeconds)) * int64(bucketSeconds)
	endBucket := (time.Now().Unix() / int64(bucketSeconds)) * int64(bucketSeconds)

	rows, err := queryContextCounted(ctx, s.store.DB(), `
			SELECT CAST(claimed_at_ts / ? AS INTEGER) * ? AS bucket_ts,
			       COUNT(*) AS cnt
			FROM token_claims
			WHERE claimed_at_ts >= ?
			GROUP BY bucket_ts
			ORDER BY bucket_ts ASC
		`, bucketSeconds, bucketSeconds, startTS)
	if err != nil {
		return nil, fmt.Errorf("query claim trends: %w", err)
	}
	defer rows.Close()

	counts := make(map[int64]int)
	for rows.Next() {
		var bucketTS int64
		var count int
		if err := rows.Scan(&bucketTS, &count); err != nil {
			return nil, fmt.Errorf("scan claim trend row: %w", err)
		}
		counts[bucketTS] = count
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate claim trend rows: %w", err)
	}

	series := make([]map[string]any, 0)
	for cursor := startBucket; cursor <= endBucket; cursor += int64(bucketSeconds) {
		series = append(series, map[string]any{
			"ts":    isoformatFromTS(cursor),
			"count": counts[cursor],
		})
	}
	return series, nil
}

func (s *Service) getClaimTrendsObserved(ctx context.Context, windowSeconds int, bucketSeconds int) (cachedReadResult[[]map[string]any], error) {
	return loadCachedReadResult(
		ctx,
		s,
		s.dashboardCacheKey("dashboard-trends", nil, windowSeconds, bucketSeconds),
		s.dashboardStaleCacheKey("dashboard-trends", nil, windowSeconds, bucketSeconds),
		s.cfg.Cache.DashboardTTL,
		func(loadCtx context.Context) ([]map[string]any, error) {
			return s.loadClaimTrends(loadCtx, windowSeconds, bucketSeconds)
		},
	)
}

func (s *Service) getClaimTrends(ctx context.Context, windowSeconds int, bucketSeconds int) ([]map[string]any, error) {
	result, err := s.getClaimTrendsObserved(ctx, windowSeconds, bucketSeconds)
	return result.Value, err
}

func clampInt(value int, minimum int, maximum int, fallback int) int {
	if value == 0 {
		value = fallback
	}
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}
