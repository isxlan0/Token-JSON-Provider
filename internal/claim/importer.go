package claim

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

func (s *Service) reconcileTokenFiles(ctx context.Context) (map[string]int, error) {
	if err := s.ensureTokenDir(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entries, err := s.listTokenDirEntries()
	if err != nil {
		return nil, err
	}

	existingNames := make(map[string]struct{}, len(entries))
	imported := 0
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		fileName := entry.Name()
		existingNames[fileName] = struct{}{}
		changed, err := s.importTokenFile(ctx, fileName)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return nil, err
			}
			s.logger.Error("reconcile token file", "error", err, "file_name", fileName)
			continue
		}
		if changed {
			imported++
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	deactivated, err := s.deactivateMissingTokenFiles(ctx, existingNames)
	if err != nil {
		return nil, err
	}

	if imported > 0 || deactivated > 0 {
		s.invalidateInventoryCache()
		s.invalidateDashboardUploadCaches()
		s.invalidateAdminCache()
		s.notifyQueueUsers(ctx)
		s.wakeQueuePump()
	}

	return map[string]int{
		"total":       len(entries),
		"imported":    imported,
		"deactivated": deactivated,
	}, nil
}

func (s *Service) importTokenFile(ctx context.Context, fileName string) (bool, error) {
	path := filepath.Join(s.tokenDirPath(), filepath.Base(strings.TrimSpace(fileName)))
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s.deactivateTokenFile(ctx, filepath.Base(path))
		}
		if isRetriableTokenFileReadError(err) {
			return false, wrapRetryableTokenImportError(fmt.Errorf("read token file %s: %w", fileName, err))
		}
		return false, fmt.Errorf("read token file %s: %w", fileName, err)
	}

	encoding, contentJSON, fileHash, normalized, err := loadTokenFilePayload(raw)
	if err != nil {
		return false, err
	}

	relativePath := s.tokenFileRelativePath(filepath.Base(path))
	now := time.Now().Unix()
	healthyMaxClaims := s.cfg.Inventory.Limits.Healthy.MaxClaims
	accountID := normalizedUploadString(normalized, "account_id")
	accessTokenHash := hashTokenValue(normalizedUploadString(normalized, "access_token"))

	changed, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (bool, error) {
		row := tx.QueryRowContext(ctx, `
			SELECT id, claim_count, max_claims, is_enabled, is_banned, file_hash, file_path, encoding, content_json, is_active
			FROM tokens
			WHERE file_name = ?
		`, filepath.Base(path))

		var (
			tokenID          int64
			claimCount       int
			maxClaims        int
			isEnabled        int
			isBanned         int
			existingHash     string
			existingPath     string
			existingEncoding string
			existingContent  string
			isActive         int
		)
		existing := true
		if err := row.Scan(&tokenID, &claimCount, &maxClaims, &isEnabled, &isBanned, &existingHash, &existingPath, &existingEncoding, &existingContent, &isActive); err != nil {
			if err != sql.ErrNoRows {
				return false, fmt.Errorf("select token by file name: %w", err)
			}
			existing = false
		}

		conflict, err := s.findSyncTokenConflictTx(ctx, tx, accountID, accessTokenHash, existing, tokenID)
		if err != nil {
			return false, err
		}
		if conflict {
			if existing {
				if _, err := tx.ExecContext(ctx, `
					UPDATE tokens
					SET is_active = 0,
					    is_available = 0,
					    updated_at_ts = ?,
					    last_seen_at_ts = ?
					WHERE id = ?
				`, now, now, tokenID); err != nil {
					return false, fmt.Errorf("deactivate conflicted token: %w", err)
				}
				if _, err := s.ensureInventoryPolicyTx(ctx, tx, true); err != nil {
					return false, err
				}
				return true, nil
			}
			return false, nil
		}

		if existing {
			effectiveMaxClaims := maxClaims
			if effectiveMaxClaims <= 0 {
				effectiveMaxClaims = healthyMaxClaims
			}
			nextIsActive := 1
			if isBanned == 1 {
				nextIsActive = 0
			}
			isAvailable := 0
			if isEnabled == 1 && isBanned == 0 && claimCount < effectiveMaxClaims {
				isAvailable = 1
			}

			changed := existingHash != fileHash ||
				existingPath != relativePath ||
				existingEncoding != encoding ||
				existingContent != contentJSON ||
				isActive != nextIsActive

			if _, err := tx.ExecContext(ctx, `
				UPDATE tokens
				SET file_path = ?,
				    file_hash = ?,
				    encoding = ?,
				    content_json = ?,
				    account_id = COALESCE(NULLIF(account_id, ''), ?),
				    access_token_hash = COALESCE(NULLIF(access_token_hash, ''), ?),
				    is_active = ?,
				    is_available = ?,
				    updated_at_ts = ?,
				    last_seen_at_ts = ?
				WHERE id = ?
			`, relativePath, fileHash, encoding, contentJSON, accountID, accessTokenHash, nextIsActive, isAvailable, now, now, tokenID); err != nil {
				return false, fmt.Errorf("update token file: %w", err)
			}
			if _, err := s.ensureInventoryPolicyTx(ctx, tx, true); err != nil {
				return false, err
			}
			return changed, nil
		}

		if _, err := tx.ExecContext(ctx, `
			INSERT INTO tokens (
				file_name,
				file_path,
				file_hash,
				encoding,
				content_json,
				account_id,
				access_token_hash,
				is_active,
				is_cleaned,
				is_enabled,
				is_banned,
				is_available,
				claim_count,
				max_claims,
				created_at_ts,
				cleaned_at_ts,
				updated_at_ts,
				last_seen_at_ts
			) VALUES (?, ?, ?, ?, ?, ?, ?, 1, 0, 1, 0, 1, 0, ?, ?, NULL, ?, ?)
		`, filepath.Base(path), relativePath, fileHash, encoding, contentJSON, accountID, accessTokenHash, healthyMaxClaims, now, now, now); err != nil {
			return false, fmt.Errorf("insert imported token: %w", err)
		}

		if _, err := s.ensureInventoryPolicyTx(ctx, tx, true); err != nil {
			return false, err
		}
		return true, nil
	})
	if err != nil {
		return false, err
	}
	if changed {
		s.invalidateInventoryCache()
		s.invalidateDashboardUploadCaches()
		s.invalidateAdminCache()
		s.notifyQueueUsers(ctx)
		s.wakeQueuePump()
	}
	return changed, nil
}

func (s *Service) findSyncTokenConflictTx(ctx context.Context, tx *sql.Tx, accountID string, accessTokenHash string, existing bool, currentTokenID int64) (bool, error) {
	params := []any{accountID, accessTokenHash}
	query := `
		SELECT 1
		FROM tokens
		WHERE (account_id = ? OR access_token_hash = ?)
	`
	if existing {
		query += ` AND id != ?`
		params = append(params, currentTokenID)
	}
	query += ` LIMIT 1`

	var marker int
	if err := tx.QueryRowContext(ctx, query, params...).Scan(&marker); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("query sync token conflict: %w", err)
	}
	return true, nil
}

func (s *Service) deactivateTokenFile(ctx context.Context, fileName string) (bool, error) {
	affected, err := runWithDatabaseBusyRetry(ctx, func() (int64, error) {
		now := time.Now().Unix()
		result, err := s.store.DB().ExecContext(ctx, `
			UPDATE tokens
			SET is_active = 0,
			    is_available = 0,
			    updated_at_ts = ?,
			    last_seen_at_ts = ?
			WHERE file_name = ? AND is_active != 0
		`, now, now, fileName)
		if err != nil {
			return 0, fmt.Errorf("deactivate token file %s: %w", fileName, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read deactivate token rows affected: %w", err)
		}
		return affected, nil
	})
	if err != nil {
		return false, err
	}
	if affected > 0 {
		s.invalidateInventoryCache()
		s.invalidateDashboardUploadCaches()
		s.invalidateAdminCache()
		s.notifyQueueUsers(ctx)
	}
	return affected > 0, nil
}

func (s *Service) deactivateMissingTokenFiles(ctx context.Context, existingNames map[string]struct{}) (int, error) {
	now := time.Now().Unix()
	query := `
		UPDATE tokens
		SET is_active = 0,
		    is_available = 0,
		    updated_at_ts = ?,
		    last_seen_at_ts = ?
		WHERE is_active != 0
	`
	args := []any{now, now}
	if len(existingNames) > 0 {
		names := make([]string, 0, len(existingNames))
		for name := range existingNames {
			names = append(names, name)
		}
		sort.Strings(names)

		placeholders := make([]string, 0, len(names))
		for _, name := range names {
			placeholders = append(placeholders, "?")
			args = append(args, name)
		}
		query += fmt.Sprintf(" AND file_name NOT IN (%s)", strings.Join(placeholders, ","))
	}

	affected, err := runWithDatabaseBusyRetry(ctx, func() (int64, error) {
		result, err := s.store.DB().ExecContext(ctx, query, args...)
		if err != nil {
			return 0, fmt.Errorf("deactivate missing token files: %w", err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return 0, fmt.Errorf("read deactivate missing token rows affected: %w", err)
		}
		return affected, nil
	})
	if err != nil {
		return 0, err
	}
	return int(affected), nil
}

func loadTokenFilePayload(raw []byte) (string, string, string, map[string]any, error) {
	encoding, decoded, err := detectAndDecodeText(raw)
	if err != nil {
		return "", "", "", nil, err
	}
	var payload any
	if err := json.Unmarshal([]byte(decoded), &payload); err != nil {
		wrapped := fmt.Errorf("decode token file json: %w", err)
		if isRetriableTokenFileJSONError(decoded, err) {
			return "", "", "", nil, wrapRetryableTokenImportError(wrapped)
		}
		return "", "", "", nil, wrapped
	}

	normalized, err := normalizeUploadContent(payload)
	if err != nil {
		return "", "", "", nil, err
	}

	contentJSONBytes, err := json.Marshal(payload)
	if err != nil {
		return "", "", "", nil, fmt.Errorf("encode token file json: %w", err)
	}

	fileHashBytes := sha256.Sum256(raw)
	return encoding, string(contentJSONBytes), hex.EncodeToString(fileHashBytes[:]), normalized, nil
}

var errRetryableTokenImport = errors.New("retryable token import")

type retryableTokenImportError struct {
	err error
}

func (e retryableTokenImportError) Error() string {
	return e.err.Error()
}

func (e retryableTokenImportError) Unwrap() error {
	return errRetryableTokenImport
}

func wrapRetryableTokenImportError(err error) error {
	if err == nil {
		return nil
	}
	return retryableTokenImportError{err: err}
}

func isRetriableTokenImportError(err error) bool {
	return errors.Is(err, errRetryableTokenImport)
}

func isRetriableTokenFileReadError(err error) bool {
	if err == nil {
		return false
	}
	if os.IsPermission(err) {
		return true
	}

	message := strings.ToLower(err.Error())
	return strings.Contains(message, "used by another process") ||
		strings.Contains(message, "sharing violation") ||
		strings.Contains(message, "access is denied")
}

func isRetriableTokenFileJSONError(decoded string, err error) bool {
	if err == nil {
		return false
	}

	trimmed := strings.TrimSpace(trimLeadingBOM(decoded))
	if trimmed == "" {
		return true
	}

	message := strings.ToLower(err.Error())
	if strings.Contains(message, "unexpected end of json input") || strings.Contains(message, "unexpected eof") {
		return true
	}

	var syntaxErr *json.SyntaxError
	return errors.As(err, &syntaxErr) && syntaxErr.Offset >= int64(len(trimmed))
}

func (s *Service) listQueuedUserIDs(ctx context.Context) ([]int64, error) {
	rows, err := s.store.DB().QueryContext(ctx, `
		SELECT DISTINCT user_id
		FROM claim_queue
		WHERE status = 'queued' AND remaining > 0
		ORDER BY queue_rank ASC, id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list queued user ids: %w", err)
	}
	defer rows.Close()

	userIDs := make([]int64, 0)
	for rows.Next() {
		var userID int64
		if err := rows.Scan(&userID); err != nil {
			return nil, fmt.Errorf("scan queued user id: %w", err)
		}
		userIDs = append(userIDs, userID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued user ids: %w", err)
	}
	return userIDs, nil
}
