package claim

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"token-atlas/internal/runtimecache"
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

func (s *Service) GetDashboardSummaryWithOptions(ctx context.Context, userID int64, options dashboardSummaryOptions) (map[string]any, error) {
	leaderboardWindow := parseWindowSeconds(options.LeaderboardWindow, 24*3600, 7*24*3600)
	leaderboardLimit := clampInt(options.LeaderboardLimit, 1, 10, 10)
	recentLimit := clampInt(options.RecentLimit, 1, 10, 10)
	contributorLimit := clampInt(options.ContributorLimit, 1, 10, 10)
	recentContributorLimit := clampInt(options.RecentContributorLimit, 1, 10, 10)
	windowSeconds := parseWindowSeconds(options.Window, 7*24*3600, 14*24*3600)
	bucketSeconds := parseBucketSeconds(options.Bucket, 3600)

	cacheKey := s.dashboardCacheKey(
		"dashboard-summary",
		&userID,
		windowSeconds,
		bucketSeconds,
		leaderboardWindow,
		leaderboardLimit,
		recentLimit,
		contributorLimit,
		recentContributorLimit,
	)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.DashboardTTL, func() (map[string]any, error) {
		stats, err := s.getDashboardStats(ctx, userID)
		if err != nil {
			return nil, err
		}

		system, err := s.getSystemStatus(ctx)
		if err != nil {
			return nil, err
		}

		leaderboard, err := s.getLeaderboard(ctx, leaderboardWindow, leaderboardLimit)
		if err != nil {
			return nil, err
		}
		recentClaims, err := s.getRecentClaims(ctx, recentLimit)
		if err != nil {
			return nil, err
		}
		contributors, err := s.getContributorLeaderboard(ctx, contributorLimit)
		if err != nil {
			return nil, err
		}
		recentContributors, err := s.getRecentContributors(ctx, recentContributorLimit)
		if err != nil {
			return nil, err
		}
		trends, err := s.getClaimTrends(ctx, windowSeconds, bucketSeconds)
		if err != nil {
			return nil, err
		}

		return map[string]any{
			"stats":               stats,
			"leaderboard":         leaderboard,
			"recent":              map[string]any{"items": recentClaims},
			"contributors":        map[string]any{"items": contributors},
			"recent_contributors": map[string]any{"items": recentContributors},
			"trends": map[string]any{
				"window": windowSeconds,
				"bucket": bucketSeconds,
				"series": trends,
			},
			"system": system,
		}, nil
	})
}

func (s *Service) getLeaderboard(ctx context.Context, windowSeconds int, limit int) (map[string]any, error) {
	cacheKey := s.dashboardCacheKey("dashboard-leaderboard", nil, windowSeconds, limit)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.DashboardTTL, func() (map[string]any, error) {
		rows, err := s.store.DB().QueryContext(ctx, `
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
	})
}

func (s *Service) getRecentClaims(ctx context.Context, limit int) ([]map[string]any, error) {
	cacheKey := s.dashboardCacheKey("dashboard-recent", nil, limit)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.DashboardTTL, func() ([]map[string]any, error) {
		rows, err := s.store.DB().QueryContext(ctx, `
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
	})
}

func (s *Service) getContributorLeaderboard(ctx context.Context, limit int) ([]map[string]any, error) {
	cacheKey := s.dashboardCacheKey("dashboard-contributors", nil, limit)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.DashboardTTL, func() ([]map[string]any, error) {
		rows, err := s.store.DB().QueryContext(ctx, `
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
	})
}

func (s *Service) getRecentContributors(ctx context.Context, limit int) ([]map[string]any, error) {
	cacheKey := s.dashboardCacheKey("dashboard-recent-contributors", nil, limit)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.DashboardTTL, func() ([]map[string]any, error) {
		rows, err := s.store.DB().QueryContext(ctx, `
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
	})
}

func (s *Service) getClaimTrends(ctx context.Context, windowSeconds int, bucketSeconds int) ([]map[string]any, error) {
	cacheKey := s.dashboardCacheKey("dashboard-trends", nil, windowSeconds, bucketSeconds)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.DashboardTTL, func() ([]map[string]any, error) {
		startTS := time.Now().Unix() - int64(windowSeconds)
		startBucket := (startTS / int64(bucketSeconds)) * int64(bucketSeconds)
		endBucket := (time.Now().Unix() / int64(bucketSeconds)) * int64(bucketSeconds)

		rows, err := s.store.DB().QueryContext(ctx, `
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
	})
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
