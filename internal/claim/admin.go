package claim

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/labstack/echo/v4"

	"token-atlas/internal/auth"
	"token-atlas/internal/database"
	"token-atlas/internal/runtimecache"
)

func (s *Service) GetAdminMe(ctx context.Context, requestContext *auth.RequestContext) (map[string]any, error) {
	cacheKey := s.snapshotCacheKey(
		"admin-me",
		requestContext.UserID,
		fmt.Sprintf("av%d", s.cacheScopeVersion("admin")),
		fmt.Sprintf("upv%d", s.cacheScopeVersion("user-profile", requestContext.UserID)),
	)
	return runtimecache.CacheJSON(s.cache, cacheKey, s.cfg.Cache.AdminTTL, func() (map[string]any, error) {
		policy, err := s.getInventoryPolicy(ctx)
		if err != nil {
			return nil, err
		}

		system, err := s.getSystemStatus(ctx)
		if err != nil {
			return nil, err
		}

		userPayload := map[string]any{
			"id":          requestContext.User.ID,
			"username":    requestContext.User.Username,
			"name":        requestContext.User.Name,
			"trust_level": requestContext.User.TrustLevel,
			"is_admin":    requestContext.User.IsAdmin,
			"is_banned":   requestContext.IsBanned,
		}

		return map[string]any{
			"user":   userPayload,
			"ban":    requestContext.Ban,
			"policy": buildAdminPolicyPayload(policy),
			"system": system,
		}, nil
	})
}

func (s *Service) ListUsersForAdmin(ctx context.Context, search string, banStatus string, limit int, offset int) (map[string]any, error) {
	limit = clampInt(limit, 1, 200, 100)
	offset = maxInt(0, offset)

	nowTS := time.Now().Unix()
	whereParts := []string{"1 = 1"}
	params := []any{nowTS}
	if trimmed := strings.ToLower(strings.TrimSpace(search)); trimmed != "" {
		pattern := "%" + trimmed + "%"
		whereParts = append(whereParts, `
			(
				lower(users.linuxdo_user_id) LIKE ?
				OR lower(users.linuxdo_username) LIKE ?
				OR lower(COALESCE(users.linuxdo_name, '')) LIKE ?
			)
		`)
		params = append(params, pattern, pattern, pattern)
	}
	switch strings.ToLower(strings.TrimSpace(banStatus)) {
	case "banned":
		whereParts = append(whereParts, "ban.id IS NOT NULL")
	case "normal":
		whereParts = append(whereParts, "ban.id IS NULL")
	}

	totalQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM users
		LEFT JOIN (
			SELECT ub.*
			FROM user_bans AS ub
			INNER JOIN (
				SELECT linuxdo_user_id, MAX(id) AS max_id
				FROM user_bans
				WHERE unbanned_at_ts IS NULL
				  AND (expires_at_ts IS NULL OR expires_at_ts > ?)
				GROUP BY linuxdo_user_id
			) latest ON latest.max_id = ub.id
		) AS ban ON ban.linuxdo_user_id = users.linuxdo_user_id
		WHERE %s
	`, strings.Join(whereParts, " AND "))
	total, err := queryCount(ctx, s.store.DB(), totalQuery, params...)
	if err != nil {
		return nil, err
	}

	listQuery := fmt.Sprintf(`
		SELECT users.id,
		       users.linuxdo_user_id,
		       users.linuxdo_username,
		       users.linuxdo_name,
		       users.trust_level,
		       users.created_at_ts,
		       users.last_login_at_ts,
		       COALESCE(claim_totals.claim_count, 0) AS claim_count,
		       COALESCE(api_totals.active_keys, 0) AS active_keys,
		       ban.id AS active_ban_id,
		       ban.reason AS ban_reason,
		       ban.expires_at_ts AS ban_expires_at_ts
		FROM users
		LEFT JOIN (
			SELECT user_id, COUNT(*) AS claim_count
			FROM token_claims
			GROUP BY user_id
		) AS claim_totals ON claim_totals.user_id = users.id
		LEFT JOIN (
			SELECT user_id, COUNT(*) AS active_keys
			FROM api_keys
			WHERE status = 'active'
			GROUP BY user_id
		) AS api_totals ON api_totals.user_id = users.id
		LEFT JOIN (
			SELECT ub.*
			FROM user_bans AS ub
			INNER JOIN (
				SELECT linuxdo_user_id, MAX(id) AS max_id
				FROM user_bans
				WHERE unbanned_at_ts IS NULL
				  AND (expires_at_ts IS NULL OR expires_at_ts > ?)
				GROUP BY linuxdo_user_id
			) latest ON latest.max_id = ub.id
		) AS ban ON ban.linuxdo_user_id = users.linuxdo_user_id
		WHERE %s
		ORDER BY users.last_login_at_ts DESC, users.id DESC
		LIMIT ? OFFSET ?
	`, strings.Join(whereParts, " AND "))
	rows, err := s.store.DB().QueryContext(ctx, listQuery, append(params, limit, offset)...)
	if err != nil {
		return nil, fmt.Errorf("query admin users: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var (
			dbUserID       int64
			linuxDOUserID  string
			username       string
			name           sql.NullString
			trustLevel     int64
			createdAtTS    int64
			lastLoginAtTS  int64
			claimCount     int
			activeKeys     int
			activeBanID    sql.NullInt64
			banReason      sql.NullString
			banExpiresAtTS sql.NullInt64
		)
		if err := rows.Scan(&dbUserID, &linuxDOUserID, &username, &name, &trustLevel, &createdAtTS, &lastLoginAtTS, &claimCount, &activeKeys, &activeBanID, &banReason, &banExpiresAtTS); err != nil {
			return nil, fmt.Errorf("scan admin user row: %w", err)
		}

		item := map[string]any{
			"db_user_id":      dbUserID,
			"linuxdo_user_id": linuxDOUserID,
			"username":        username,
			"name":            firstNonEmpty(name.String, username),
			"trust_level":     trustLevel,
			"created_at":      isoformatFromTS(createdAtTS),
			"last_login_at":   isoformatFromTS(lastLoginAtTS),
			"claim_count":     claimCount,
			"active_api_keys": activeKeys,
			"is_banned":       activeBanID.Valid,
			"ban_reason":      nil,
			"ban_expires_at":  nil,
		}
		if banReason.Valid {
			item["ban_reason"] = banReason.String
		}
		if banExpiresAtTS.Valid {
			item["ban_expires_at"] = isoformatFromTS(banExpiresAtTS.Int64)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate admin user rows: %w", err)
	}

	return map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}, nil
}

func (s *Service) GetAdminUserDetail(ctx context.Context, linuxDOUserID string) (map[string]any, error) {
	user, err := s.getUserByLinuxDOID(ctx, linuxDOUserID)
	if err != nil {
		return nil, err
	}
	if user == nil {
		return nil, nil
	}

	totalsRow := s.store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*) AS total_claims,
		       COUNT(DISTINCT token_id) AS unique_claims
		FROM token_claims
		WHERE user_id = ?
	`, user.ID)
	var totalClaims int
	var uniqueClaims int
	if err := totalsRow.Scan(&totalClaims, &uniqueClaims); err != nil {
		return nil, fmt.Errorf("query admin user totals: %w", err)
	}

	apiRow := s.store.DB().QueryRowContext(ctx, `
		SELECT COUNT(*) AS total_keys,
		       COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_keys
		FROM api_keys
		WHERE user_id = ?
	`, user.ID)
	var totalKeys int
	var activeKeys int
	if err := apiRow.Scan(&totalKeys, &activeKeys); err != nil {
		return nil, fmt.Errorf("query admin user api key totals: %w", err)
	}

	recentRows, err := s.store.DB().QueryContext(ctx, `
		SELECT token_claims.claimed_at_ts,
		       COALESCE(token_claims.claim_file_name, tokens.file_name) AS file_name
		FROM token_claims
		LEFT JOIN tokens ON tokens.id = token_claims.token_id
		WHERE token_claims.user_id = ?
		ORDER BY token_claims.claimed_at_ts DESC, token_claims.id DESC
		LIMIT 20
	`, user.ID)
	if err != nil {
		return nil, fmt.Errorf("query admin user recent claims: %w", err)
	}
	defer recentRows.Close()

	recent := make([]map[string]any, 0)
	for recentRows.Next() {
		var claimedAtTS int64
		var fileName string
		if err := recentRows.Scan(&claimedAtTS, &fileName); err != nil {
			return nil, fmt.Errorf("scan admin user recent claim row: %w", err)
		}
		recent = append(recent, map[string]any{
			"claimed_at": isoformatFromTS(claimedAtTS),
			"file_name":  fileName,
		})
	}
	if err := recentRows.Err(); err != nil {
		return nil, fmt.Errorf("iterate admin user recent claim rows: %w", err)
	}

	ban, err := s.getActiveBanPayload(ctx, linuxDOUserID)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"user": map[string]any{
			"db_user_id":      user.ID,
			"linuxdo_user_id": user.LinuxDOUserID,
			"username":        user.LinuxDOUsername,
			"name":            firstNonEmpty(user.LinuxDOName.String, user.LinuxDOUsername),
			"trust_level":     user.TrustLevel,
			"created_at":      isoformatFromTS(user.CreatedAtTS),
			"last_login_at":   isoformatFromTS(user.LastLoginAtTS),
		},
		"claims": map[string]any{
			"total":  totalClaims,
			"unique": uniqueClaims,
			"recent": recent,
		},
		"api_keys": map[string]any{
			"total":  totalKeys,
			"active": activeKeys,
		},
		"ban": ban,
	}, nil
}

func (s *Service) BanUser(ctx context.Context, linuxDOUserID string, usernameSnapshot string, reason string, bannedByUserID int64, expiresAtTS *int64) (*auth.BanPayload, error) {
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		return nil, echo.NewHTTPError(400, "封禁原因必填")
	}

	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (*auth.BanPayload, error) {
		nowTS := time.Now().Unix()
		if _, err := tx.ExecContext(ctx, `
			UPDATE user_bans
			SET unbanned_at_ts = ?,
			    unbanned_by_user_id = ?
			WHERE linuxdo_user_id = ?
			  AND unbanned_at_ts IS NULL
		`, nowTS, bannedByUserID, linuxDOUserID); err != nil {
			return nil, fmt.Errorf("close previous user bans: %w", err)
		}

		result, err := tx.ExecContext(ctx, `
			INSERT INTO user_bans (
				linuxdo_user_id,
				username_snapshot,
				reason,
				banned_by_user_id,
				banned_at_ts,
				expires_at_ts
			) VALUES (?, ?, ?, ?, ?, ?)
		`, linuxDOUserID, nullableStringOrNil(usernameSnapshot), trimmedReason, bannedByUserID, nowTS, nullableInt64(expiresAtTS))
		if err != nil {
			return nil, fmt.Errorf("insert user ban: %w", err)
		}

		banID, err := result.LastInsertId()
		if err != nil {
			return nil, fmt.Errorf("read inserted ban id: %w", err)
		}

		return s.getBanByIDTx(ctx, tx, banID)
	})
}

func (s *Service) UnbanUser(ctx context.Context, linuxDOUserID string, unbannedByUserID int64) (bool, error) {
	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (bool, error) {
		nowTS := time.Now().Unix()
		result, err := tx.ExecContext(ctx, `
			UPDATE user_bans
			SET unbanned_at_ts = ?,
			    unbanned_by_user_id = ?
			WHERE linuxdo_user_id = ?
			  AND unbanned_at_ts IS NULL
			  AND (expires_at_ts IS NULL OR expires_at_ts > ?)
		`, nowTS, unbannedByUserID, linuxDOUserID, nowTS)
		if err != nil {
			return false, fmt.Errorf("unban user: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return false, fmt.Errorf("read unban rows affected: %w", err)
		}
		return affected > 0, nil
	})
}

func (s *Service) ListBans(ctx context.Context, statusFilter string, search string, limit int, offset int) (map[string]any, error) {
	limit = clampInt(limit, 1, 200, 100)
	offset = maxInt(0, offset)

	nowTS := time.Now().Unix()
	whereParts := []string{"1 = 1"}
	params := make([]any, 0)
	switch strings.ToLower(strings.TrimSpace(statusFilter)) {
	case "active":
		whereParts = append(whereParts, "user_bans.unbanned_at_ts IS NULL")
		whereParts = append(whereParts, "(user_bans.expires_at_ts IS NULL OR user_bans.expires_at_ts > ?)")
		params = append(params, nowTS)
	case "expired":
		whereParts = append(whereParts, "user_bans.unbanned_at_ts IS NULL")
		whereParts = append(whereParts, "user_bans.expires_at_ts IS NOT NULL")
		whereParts = append(whereParts, "user_bans.expires_at_ts <= ?")
		params = append(params, nowTS)
	case "unbanned":
		whereParts = append(whereParts, "user_bans.unbanned_at_ts IS NOT NULL")
	}
	if trimmed := strings.ToLower(strings.TrimSpace(search)); trimmed != "" {
		pattern := "%" + trimmed + "%"
		whereParts = append(whereParts, `
			(
				lower(user_bans.linuxdo_user_id) LIKE ?
				OR lower(COALESCE(user_bans.username_snapshot, '')) LIKE ?
				OR lower(COALESCE(target.linuxdo_username, '')) LIKE ?
				OR lower(COALESCE(target.linuxdo_name, '')) LIKE ?
			)
		`)
		params = append(params, pattern, pattern, pattern, pattern)
	}

	totalQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM user_bans
		LEFT JOIN users AS target ON target.linuxdo_user_id = user_bans.linuxdo_user_id
		WHERE %s
	`, strings.Join(whereParts, " AND "))
	total, err := queryCount(ctx, s.store.DB(), totalQuery, params...)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT user_bans.id,
		       user_bans.linuxdo_user_id,
		       user_bans.username_snapshot,
		       user_bans.reason,
		       user_bans.banned_at_ts,
		       user_bans.expires_at_ts,
		       user_bans.unbanned_by_user_id,
		       user_bans.unbanned_at_ts,
		       users.linuxdo_username AS banned_by_username,
		       users.linuxdo_name AS banned_by_name,
		       unbanners.linuxdo_username AS unbanned_by_username,
		       unbanners.linuxdo_name AS unbanned_by_name
		FROM user_bans
		LEFT JOIN users AS target ON target.linuxdo_user_id = user_bans.linuxdo_user_id
		LEFT JOIN users ON users.id = user_bans.banned_by_user_id
		LEFT JOIN users AS unbanners ON unbanners.id = user_bans.unbanned_by_user_id
		WHERE %s
		ORDER BY user_bans.banned_at_ts DESC, user_bans.id DESC
		LIMIT ? OFFSET ?
	`, strings.Join(whereParts, " AND "))
	rows, err := s.store.DB().QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, fmt.Errorf("query bans: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		banPayload, err := scanBanRow(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, banPayload)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate bans: %w", err)
	}

	return map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}, nil
}

func (s *Service) ListTokensForAdmin(ctx context.Context, search string, statusFilter string, limit int, offset int) (map[string]any, error) {
	limit = clampInt(limit, 1, 500, 200)
	offset = maxInt(0, offset)

	whereParts := []string{"1 = 1"}
	params := make([]any, 0)
	if trimmed := strings.ToLower(strings.TrimSpace(search)); trimmed != "" {
		pattern := "%" + trimmed + "%"
		whereParts = append(whereParts, `
			(
				lower(file_name) LIKE ?
				OR lower(file_path) LIKE ?
			)
		`)
		params = append(params, pattern, pattern)
	}
	switch strings.ToLower(strings.TrimSpace(statusFilter)) {
	case "enabled":
		whereParts = append(whereParts, "is_active = 1", "is_enabled = 1", "is_banned = 0")
	case "banned":
		whereParts = append(whereParts, "is_banned = 1")
	case "disabled":
		whereParts = append(whereParts, "(is_active = 0 OR is_enabled = 0 OR is_banned = 1)")
	}

	totalQuery := fmt.Sprintf(`SELECT COUNT(*) FROM tokens WHERE %s`, strings.Join(whereParts, " AND "))
	total, err := queryCount(ctx, s.store.DB(), totalQuery, params...)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf(`
		SELECT id,
		       file_name,
		       file_path,
		       encoding,
		       is_active,
		       is_enabled,
		       is_banned,
		       is_available,
		       is_cleaned,
		       claim_count,
		       max_claims,
		       banned_at_ts,
		       ban_reason,
		       cleaned_at_ts,
		       last_probe_at_ts,
		       last_probe_status,
		       created_at_ts,
		       updated_at_ts,
		       last_seen_at_ts
		FROM tokens
		WHERE %s
		ORDER BY is_banned ASC, is_active DESC, is_enabled DESC, updated_at_ts DESC, id DESC
		LIMIT ? OFFSET ?
	`, strings.Join(whereParts, " AND "))
	rows, err := s.store.DB().QueryContext(ctx, query, append(params, limit, offset)...)
	if err != nil {
		return nil, fmt.Errorf("query admin tokens: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		item, err := scanAdminTokenRow(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate admin tokens: %w", err)
	}

	return map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}, nil
}

func (s *Service) SetTokenEnabled(ctx context.Context, tokenID int64, enabled bool) (map[string]any, error) {
	item, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (map[string]any, error) {
		nowTS := time.Now().Unix()
		enabledValue := 0
		if enabled {
			enabledValue = 1
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE tokens
			SET is_enabled = ?,
			    is_available = CASE
					WHEN is_active = 1 AND is_banned = 0 AND ? = 1 AND claim_count < max_claims THEN 1
					ELSE 0
				END,
			    updated_at_ts = ?
			WHERE id = ?
			  AND (? = 0 OR is_banned = 0)
		`, enabledValue, enabledValue, nowTS, tokenID, enabledValue); err != nil {
			return nil, fmt.Errorf("update token enabled state: %w", err)
		}

		if _, err := s.ensureInventoryPolicyTx(ctx, tx, true); err != nil {
			return nil, err
		}

		row := tx.QueryRowContext(ctx, `
			SELECT id,
			       file_name,
			       file_path,
			       encoding,
			       is_active,
			       is_enabled,
			       is_banned,
			       is_available,
			       is_cleaned,
			       claim_count,
			       max_claims,
			       banned_at_ts,
			       ban_reason,
			       cleaned_at_ts,
			       last_probe_at_ts,
			       last_probe_status,
			       created_at_ts,
			       updated_at_ts,
			       last_seen_at_ts
			FROM tokens
			WHERE id = ?
		`, tokenID)
		item, err := scanAdminTokenRowFromRow(row)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, nil
			}
			return nil, err
		}
		return item, nil
	})
	if err != nil {
		return nil, err
	}
	if item != nil {
		s.invalidateInventoryCache()
		s.invalidateDashboardInventoryCache()
		s.invalidateAdminCache()
		s.wakeQueuePump()
	}
	return item, nil
}

func (s *Service) CleanupExhaustedTokens(ctx context.Context, mode string) (map[string]any, error) {
	normalizedMode := strings.ToLower(strings.TrimSpace(mode))
	if normalizedMode == "" {
		normalizedMode = "files_only"
	}
	if normalizedMode != "files_only" && normalizedMode != "files_and_db" {
		return nil, echo.NewHTTPError(400, "Invalid cleanup mode.")
	}

	rows, err := s.store.DB().QueryContext(ctx, `
		SELECT id, file_name, file_path
		FROM tokens
		WHERE is_active = 1 AND is_cleaned = 0 AND claim_count >= max_claims
		ORDER BY updated_at_ts ASC, id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("query exhausted tokens: %w", err)
	}
	defer rows.Close()

	type exhaustedToken struct {
		ID       int64
		FileName string
		FilePath string
	}
	exhausted := make([]exhaustedToken, 0)
	for rows.Next() {
		var item exhaustedToken
		if err := rows.Scan(&item.ID, &item.FileName, &item.FilePath); err != nil {
			return nil, fmt.Errorf("scan exhausted token row: %w", err)
		}
		exhausted = append(exhausted, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate exhausted token rows: %w", err)
	}

	cleanedIDs := make([]int64, 0, len(exhausted))
	failed := make([]map[string]any, 0)
	deletedFiles := 0
	missingFiles := 0
	for _, item := range exhausted {
		targetPath, resolveErr := s.resolveStoredTokenPath(item.FilePath, item.FileName)
		if resolveErr != nil {
			failed = append(failed, map[string]any{"file_name": item.FileName, "detail": resolveErr.Error()})
			continue
		}

		if err := os.Remove(targetPath); err != nil {
			if os.IsNotExist(err) {
				missingFiles++
				cleanedIDs = append(cleanedIDs, item.ID)
				continue
			}
			failed = append(failed, map[string]any{"file_name": item.FileName, "detail": err.Error()})
			continue
		}

		deletedFiles++
		cleanedIDs = append(cleanedIDs, item.ID)
	}

	compactedContent := 0
	if len(cleanedIDs) > 0 || normalizedMode == "files_and_db" {
		compacted, err := s.cleanupExhaustedTokenRows(ctx, cleanedIDs, normalizedMode)
		if err != nil {
			return nil, err
		}
		compactedContent = compacted
	}

	vacuumed := false
	if normalizedMode == "files_and_db" && compactedContent > 0 {
		if _, err := s.store.DB().ExecContext(ctx, `VACUUM`); err != nil {
			return nil, fmt.Errorf("vacuum database: %w", err)
		}
		vacuumed = true
	}

	result := map[string]any{
		"mode":              normalizedMode,
		"matched":           len(exhausted),
		"cleaned":           len(cleanedIDs),
		"deleted_files":     deletedFiles,
		"missing_files":     missingFiles,
		"compacted_content": compactedContent,
		"vacuumed":          vacuumed,
		"failed":            failed,
	}
	if len(cleanedIDs) > 0 || compactedContent > 0 {
		s.invalidateInventoryCache()
		s.invalidateDashboardInventoryCache()
		s.invalidateAdminCache()
		s.wakeQueuePump()
	}
	return result, nil
}

func (s *Service) GetAdminPolicy(ctx context.Context) (map[string]any, error) {
	return runtimecache.CacheJSON(s.cache, s.adminCacheKey("admin-policy"), s.cfg.Cache.AdminTTL, func() (map[string]any, error) {
		policy, err := s.getInventoryPolicy(ctx)
		if err != nil {
			return nil, err
		}

		system, err := s.getSystemStatus(ctx)
		if err != nil {
			return nil, err
		}

		payload := buildAdminPolicyPayload(policy)
		payload["system"] = system
		return payload, nil
	})
}

func buildAdminPolicyPayload(policy inventoryPolicy) map[string]any {
	return map[string]any{
		"source":                       "env",
		"status":                       policy.Status,
		"hourly_limit":                 policy.HourlyLimit,
		"max_claims":                   policy.MaxClaims,
		"thresholds":                   policy.Thresholds,
		"non_healthy_max_claims_scope": policy.NonHealthyMaxClaimsScope,
	}
}

func (s *Service) cleanupExhaustedTokenRows(ctx context.Context, cleanedIDs []int64, mode string) (int, error) {
	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (int, error) {
		nowTS := time.Now().Unix()
		if len(cleanedIDs) > 0 {
			placeholders := make([]string, 0, len(cleanedIDs))
			args := make([]any, 0, len(cleanedIDs)+3)
			args = append(args, nowTS, nowTS, nowTS)
			for _, id := range cleanedIDs {
				placeholders = append(placeholders, "?")
				args = append(args, id)
			}

			contentSQL := ""
			if mode == "files_and_db" {
				contentSQL = "content_json = '{}',"
			}
			query := fmt.Sprintf(`
				UPDATE tokens
				SET is_active = 0,
				    is_cleaned = 1,
				    is_enabled = 0,
				    is_available = 0,
				    %s
				    cleaned_at_ts = ?,
				    updated_at_ts = ?,
				    last_seen_at_ts = ?
				WHERE id IN (%s)
			`, contentSQL, strings.Join(placeholders, ","))
			if _, err := tx.ExecContext(ctx, query, args...); err != nil {
				return 0, fmt.Errorf("mark exhausted tokens cleaned: %w", err)
			}
		}

		compacted := 0
		if mode == "files_and_db" {
			result, err := tx.ExecContext(ctx, `
				UPDATE tokens
				SET content_json = '{}',
				    updated_at_ts = CASE
						WHEN is_cleaned = 1 AND content_json != '{}' THEN ?
						ELSE updated_at_ts
					END
				WHERE is_cleaned = 1 AND content_json != '{}'
			`, nowTS)
			if err != nil {
				return 0, fmt.Errorf("compact token content: %w", err)
			}
			if affected, err := result.RowsAffected(); err == nil {
				compacted = int(affected)
			}
		}

		if _, err := s.ensureInventoryPolicyTx(ctx, tx, true); err != nil {
			return 0, err
		}
		return compacted, nil
	})
}

func (s *Service) getUserByLinuxDOID(ctx context.Context, linuxDOUserID string) (*database.User, error) {
	row := s.store.DB().QueryRowContext(ctx, `
		SELECT id, linuxdo_user_id, linuxdo_username, linuxdo_name, trust_level, created_at_ts, last_login_at_ts
		FROM users
		WHERE linuxdo_user_id = ?
	`, linuxDOUserID)

	var user database.User
	if err := row.Scan(&user.ID, &user.LinuxDOUserID, &user.LinuxDOUsername, &user.LinuxDOName, &user.TrustLevel, &user.CreatedAtTS, &user.LastLoginAtTS); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("query user by linuxdo id %s: %w", linuxDOUserID, err)
	}
	return &user, nil
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

func (s *Service) getActiveBanPayload(ctx context.Context, linuxDOUserID string) (*auth.BanPayload, error) {
	return s.getActiveBanPayloadQueryer(ctx, s.store.DB(), linuxDOUserID)
}

func (s *Service) getActiveBanPayloadQueryer(ctx context.Context, queryer sqlQueryer, linuxDOUserID string) (*auth.BanPayload, error) {
	row := queryer.QueryRowContext(ctx, `
		SELECT user_bans.id,
		       user_bans.linuxdo_user_id,
		       user_bans.username_snapshot,
		       user_bans.reason,
		       user_bans.banned_at_ts,
		       user_bans.expires_at_ts,
		       user_bans.unbanned_by_user_id,
		       user_bans.unbanned_at_ts,
		       users.linuxdo_username AS banned_by_username,
		       users.linuxdo_name AS banned_by_name,
		       unbanners.linuxdo_username AS unbanned_by_username,
		       unbanners.linuxdo_name AS unbanned_by_name
		FROM user_bans
		LEFT JOIN users ON users.id = user_bans.banned_by_user_id
		LEFT JOIN users AS unbanners ON unbanners.id = user_bans.unbanned_by_user_id
		WHERE user_bans.linuxdo_user_id = ?
		  AND user_bans.unbanned_at_ts IS NULL
		  AND (user_bans.expires_at_ts IS NULL OR user_bans.expires_at_ts > ?)
		ORDER BY user_bans.banned_at_ts DESC, user_bans.id DESC
		LIMIT 1
	`, linuxDOUserID, time.Now().Unix())

	return scanBanPayloadFromRow(row)
}

func (s *Service) getBanByIDTx(ctx context.Context, tx *sql.Tx, banID int64) (*auth.BanPayload, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT user_bans.id,
		       user_bans.linuxdo_user_id,
		       user_bans.username_snapshot,
		       user_bans.reason,
		       user_bans.banned_at_ts,
		       user_bans.expires_at_ts,
		       user_bans.unbanned_by_user_id,
		       user_bans.unbanned_at_ts,
		       users.linuxdo_username AS banned_by_username,
		       users.linuxdo_name AS banned_by_name,
		       unbanners.linuxdo_username AS unbanned_by_username,
		       unbanners.linuxdo_name AS unbanned_by_name
		FROM user_bans
		LEFT JOIN users ON users.id = user_bans.banned_by_user_id
		LEFT JOIN users AS unbanners ON unbanners.id = user_bans.unbanned_by_user_id
		WHERE user_bans.id = ?
	`, banID)
	return scanBanPayloadFromRow(row)
}

func scanBanPayloadFromRow(row *sql.Row) (*auth.BanPayload, error) {
	var (
		ban                auth.BanPayload
		usernameSnapshot   sql.NullString
		bannedAtTS         int64
		expiresAtTS        sql.NullInt64
		unbannedByUserID   sql.NullInt64
		unbannedAtTS       sql.NullInt64
		bannedByUsername   sql.NullString
		bannedByName       sql.NullString
		unbannedByUsername sql.NullString
		unbannedByName     sql.NullString
	)
	if err := row.Scan(&ban.ID, &ban.LinuxDOUserID, &usernameSnapshot, &ban.Reason, &bannedAtTS, &expiresAtTS, &unbannedByUserID, &unbannedAtTS, &bannedByUsername, &bannedByName, &unbannedByUsername, &unbannedByName); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("scan ban payload: %w", err)
	}
	ban.BannedAt = isoformatFromTS(bannedAtTS)
	if usernameSnapshot.Valid {
		value := usernameSnapshot.String
		ban.UsernameSnapshot = &value
	}
	if expiresAtTS.Valid {
		value := isoformatFromTS(expiresAtTS.Int64)
		ban.ExpiresAt = &value
	}
	if unbannedAtTS.Valid {
		value := isoformatFromTS(unbannedAtTS.Int64)
		ban.UnbannedAt = &value
	}
	if bannedByUsername.Valid {
		ban.BannedBy = &auth.ActorPayload{
			Username: bannedByUsername.String,
			Name:     firstNonEmpty(bannedByName.String, bannedByUsername.String),
		}
	}
	if unbannedByUsername.Valid {
		ban.UnbannedBy = &auth.ActorPayload{
			Username: unbannedByUsername.String,
			Name:     firstNonEmpty(unbannedByName.String, unbannedByUsername.String),
		}
	}
	ban.IsActive = !unbannedAtTS.Valid && (!expiresAtTS.Valid || expiresAtTS.Int64 > time.Now().Unix())
	return &ban, nil
}

func scanBanRow(rows *sql.Rows) (map[string]any, error) {
	var (
		banID              int64
		linuxDOUserID      string
		usernameSnapshot   sql.NullString
		reason             string
		bannedAtTS         int64
		expiresAtTS        sql.NullInt64
		unbannedByUserID   sql.NullInt64
		unbannedAtTS       sql.NullInt64
		bannedByUsername   sql.NullString
		bannedByName       sql.NullString
		unbannedByUsername sql.NullString
		unbannedByName     sql.NullString
	)
	if err := rows.Scan(&banID, &linuxDOUserID, &usernameSnapshot, &reason, &bannedAtTS, &expiresAtTS, &unbannedByUserID, &unbannedAtTS, &bannedByUsername, &bannedByName, &unbannedByUsername, &unbannedByName); err != nil {
		return nil, fmt.Errorf("scan ban row: %w", err)
	}

	payload := map[string]any{
		"id":                banID,
		"linuxdo_user_id":   linuxDOUserID,
		"username_snapshot": nil,
		"reason":            reason,
		"banned_at":         isoformatFromTS(bannedAtTS),
		"expires_at":        nil,
		"unbanned_at":       nil,
		"banned_by":         nil,
		"unbanned_by":       nil,
		"is_active":         !unbannedAtTS.Valid && (!expiresAtTS.Valid || expiresAtTS.Int64 > time.Now().Unix()),
	}
	if usernameSnapshot.Valid {
		payload["username_snapshot"] = usernameSnapshot.String
	}
	if expiresAtTS.Valid {
		payload["expires_at"] = isoformatFromTS(expiresAtTS.Int64)
	}
	if unbannedAtTS.Valid {
		payload["unbanned_at"] = isoformatFromTS(unbannedAtTS.Int64)
	}
	if bannedByUsername.Valid {
		payload["banned_by"] = map[string]any{
			"username": bannedByUsername.String,
			"name":     firstNonEmpty(bannedByName.String, bannedByUsername.String),
		}
	}
	if unbannedByUsername.Valid {
		payload["unbanned_by"] = map[string]any{
			"username": unbannedByUsername.String,
			"name":     firstNonEmpty(unbannedByName.String, unbannedByUsername.String),
		}
	}
	return payload, nil
}

func scanAdminTokenRow(rows *sql.Rows) (map[string]any, error) {
	var (
		id              int64
		fileName        string
		filePath        string
		encoding        string
		isActive        int
		isEnabled       int
		isBanned        int
		isAvailable     int
		isCleaned       int
		claimCount      int
		maxClaims       int
		bannedAtTS      sql.NullInt64
		banReason       sql.NullString
		cleanedAtTS     sql.NullInt64
		lastProbeAtTS   sql.NullInt64
		lastProbeStatus sql.NullString
		createdAtTS     int64
		updatedAtTS     int64
		lastSeenAtTS    int64
	)
	if err := rows.Scan(&id, &fileName, &filePath, &encoding, &isActive, &isEnabled, &isBanned, &isAvailable, &isCleaned, &claimCount, &maxClaims, &bannedAtTS, &banReason, &cleanedAtTS, &lastProbeAtTS, &lastProbeStatus, &createdAtTS, &updatedAtTS, &lastSeenAtTS); err != nil {
		return nil, fmt.Errorf("scan admin token row: %w", err)
	}
	return buildAdminTokenPayload(id, fileName, filePath, encoding, isActive, isEnabled, isBanned, isAvailable, isCleaned, claimCount, maxClaims, bannedAtTS, banReason, cleanedAtTS, lastProbeAtTS, lastProbeStatus, createdAtTS, updatedAtTS, lastSeenAtTS), nil
}

func scanAdminTokenRowFromRow(row *sql.Row) (map[string]any, error) {
	var (
		id              int64
		fileName        string
		filePath        string
		encoding        string
		isActive        int
		isEnabled       int
		isBanned        int
		isAvailable     int
		isCleaned       int
		claimCount      int
		maxClaims       int
		bannedAtTS      sql.NullInt64
		banReason       sql.NullString
		cleanedAtTS     sql.NullInt64
		lastProbeAtTS   sql.NullInt64
		lastProbeStatus sql.NullString
		createdAtTS     int64
		updatedAtTS     int64
		lastSeenAtTS    int64
	)
	if err := row.Scan(&id, &fileName, &filePath, &encoding, &isActive, &isEnabled, &isBanned, &isAvailable, &isCleaned, &claimCount, &maxClaims, &bannedAtTS, &banReason, &cleanedAtTS, &lastProbeAtTS, &lastProbeStatus, &createdAtTS, &updatedAtTS, &lastSeenAtTS); err != nil {
		return nil, err
	}
	return buildAdminTokenPayload(id, fileName, filePath, encoding, isActive, isEnabled, isBanned, isAvailable, isCleaned, claimCount, maxClaims, bannedAtTS, banReason, cleanedAtTS, lastProbeAtTS, lastProbeStatus, createdAtTS, updatedAtTS, lastSeenAtTS), nil
}

func buildAdminTokenPayload(id int64, fileName string, filePath string, encoding string, isActive int, isEnabled int, isBanned int, isAvailable int, isCleaned int, claimCount int, maxClaims int, bannedAtTS sql.NullInt64, banReason sql.NullString, cleanedAtTS sql.NullInt64, lastProbeAtTS sql.NullInt64, lastProbeStatus sql.NullString, createdAtTS int64, updatedAtTS int64, lastSeenAtTS int64) map[string]any {
	payload := map[string]any{
		"id":                id,
		"file_name":         fileName,
		"file_path":         filePath,
		"encoding":          encoding,
		"is_active":         isActive == 1,
		"is_cleaned":        isCleaned == 1,
		"is_enabled":        isEnabled == 1,
		"is_banned":         isBanned == 1,
		"is_available":      isAvailable == 1,
		"claim_count":       claimCount,
		"max_claims":        maxClaims,
		"ban_reason":        nil,
		"banned_at":         nil,
		"last_probe_status": nil,
		"last_probe_at":     nil,
		"cleaned_at":        nil,
		"created_at":        isoformatFromTS(createdAtTS),
		"updated_at":        isoformatFromTS(updatedAtTS),
		"last_seen_at":      isoformatFromTS(lastSeenAtTS),
	}
	if banReason.Valid {
		payload["ban_reason"] = banReason.String
	}
	if bannedAtTS.Valid {
		payload["banned_at"] = isoformatFromTS(bannedAtTS.Int64)
	}
	if lastProbeStatus.Valid {
		payload["last_probe_status"] = lastProbeStatus.String
	}
	if lastProbeAtTS.Valid {
		payload["last_probe_at"] = isoformatFromTS(lastProbeAtTS.Int64)
	}
	if cleanedAtTS.Valid {
		payload["cleaned_at"] = isoformatFromTS(cleanedAtTS.Int64)
	}
	return payload
}

func (s *Service) resolveStoredTokenPath(relativePath string, fileName string) (string, error) {
	trimmedPath := strings.TrimSpace(relativePath)
	if filepath.IsAbs(trimmedPath) {
		return trimmedPath, nil
	}

	baseDir, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("resolve working directory: %w", err)
	}
	if trimmedPath != "" {
		return filepath.Join(baseDir, filepath.FromSlash(trimmedPath)), nil
	}

	trimmedName := filepath.Base(strings.TrimSpace(fileName))
	if trimmedName != "" && trimmedName != "." && trimmedName != string(filepath.Separator) {
		return s.tokenFileAbsolutePath(trimmedName), nil
	}
	return "", fmt.Errorf("resolve stored token path: missing file path and file name")
}

func nullableStringOrNil(value string) any {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	return trimmed
}

func nullableInt64(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}
