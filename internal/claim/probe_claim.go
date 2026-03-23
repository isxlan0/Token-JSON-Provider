package claim

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	proberuntime "token-atlas/internal/probe"
)

type reservedClaimToken struct {
	TokenID int64
	Content map[string]any
}

func (s *Service) reserveClaimableTokenForUser(ctx context.Context, userID int64) (*reservedClaimToken, error) {
	reserveUntil := time.Now().Unix() + int64(maxInt(5, s.cfg.Probe.ReserveSec))
	now := time.Now().Unix()

	type reserveResult struct {
		tokenID     int64
		contentJSON string
	}

	result, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (reserveResult, error) {
		row := tx.QueryRowContext(ctx, `
			SELECT id, content_json
			FROM tokens
			WHERE is_active = 1
			  AND is_enabled = 1
			  AND is_banned = 0
			  AND is_available = 1
			  AND claim_count < max_claims
			  AND (probe_lock_until_ts IS NULL OR probe_lock_until_ts < ?)
			  AND NOT EXISTS (
				  SELECT 1
				  FROM user_token_claims
				  WHERE user_token_claims.user_id = ?
					AND user_token_claims.token_id = tokens.id
			  )
			ORDER BY
			  CASE WHEN claim_count > 0 THEN 0 ELSE 1 END ASC,
			  created_at_ts ASC,
			  id ASC
			LIMIT 1
		`, now, userID)

		var reserved reserveResult
		if err := row.Scan(&reserved.tokenID, &reserved.contentJSON); err != nil {
			if err == sql.ErrNoRows {
				return reserveResult{}, nil
			}
			return reserveResult{}, fmt.Errorf("select reservable token: %w", err)
		}

		result, err := tx.ExecContext(ctx, `
			UPDATE tokens
			SET probe_lock_until_ts = ?,
			    updated_at_ts = ?
			WHERE id = ?
			  AND is_active = 1
			  AND is_enabled = 1
			  AND is_banned = 0
			  AND is_available = 1
			  AND claim_count < max_claims
			  AND (probe_lock_until_ts IS NULL OR probe_lock_until_ts < ?)
		`, reserveUntil, now, reserved.tokenID, now)
		if err != nil {
			return reserveResult{}, fmt.Errorf("reserve token %d: %w", reserved.tokenID, err)
		}
		affected, err := result.RowsAffected()
		if err != nil {
			return reserveResult{}, fmt.Errorf("read reserve token rows affected: %w", err)
		}
		if affected <= 0 {
			return reserveResult{}, nil
		}

		return reserved, nil
	})
	if err != nil {
		return nil, err
	}
	if result.tokenID == 0 {
		return nil, nil
	}

	content, err := proberuntime.DecodeContent(result.contentJSON)
	if err != nil {
		if releaseErr := s.recordProbeStatus(ctx, result.tokenID, "non_401_error", true); releaseErr != nil {
			return nil, releaseErr
		}
		return nil, nil
	}

	return &reservedClaimToken{
		TokenID: result.tokenID,
		Content: content,
	}, nil
}

func (s *Service) recordProbeStatus(ctx context.Context, tokenID int64, statusText string, clearLock bool) error {
	now := time.Now().Unix()
	clearValue := 0
	if clearLock {
		clearValue = 1
	}
	if _, err := s.store.DB().ExecContext(ctx, `
		UPDATE tokens
		SET last_probe_at_ts = ?,
		    last_probe_status = ?,
		    probe_lock_until_ts = CASE WHEN ? = 1 THEN NULL ELSE probe_lock_until_ts END,
		    updated_at_ts = ?
		WHERE id = ?
	`, now, statusText, clearValue, now, tokenID); err != nil {
		return fmt.Errorf("record probe status for token %d: %w", tokenID, err)
	}
	return nil
}

func (s *Service) markTokenBanned(ctx context.Context, tokenID int64, reason string) error {
	now := time.Now().Unix()
	fileName, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (string, error) {
		row := tx.QueryRowContext(ctx, `SELECT file_name FROM tokens WHERE id = ?`, tokenID)
		var fileName string
		if err := row.Scan(&fileName); err != nil {
			if err == sql.ErrNoRows {
				return "", fmt.Errorf("load token %d for ban: %w", tokenID, err)
			}
			return "", fmt.Errorf("load token %d file name for ban: %w", tokenID, err)
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE tokens
			SET is_banned = 1,
			    is_enabled = 0,
			    is_available = 0,
			    is_active = 0,
			    banned_at_ts = COALESCE(banned_at_ts, ?),
			    ban_reason = ?,
			    last_probe_at_ts = ?,
			    last_probe_status = 'banned_401',
			    probe_lock_until_ts = NULL,
			    updated_at_ts = ?
			WHERE id = ?
		`, now, reason, now, now, tokenID); err != nil {
			return "", fmt.Errorf("mark token banned %d: %w", tokenID, err)
		}
		if _, err := s.ensureInventoryPolicyTx(ctx, tx, true); err != nil {
			return "", err
		}
		return fileName, nil
	})
	if err != nil {
		return err
	}

	if deleteErr := s.removeTokenFileNow(fileName); deleteErr != nil {
		s.scheduleTokenFileDeleteRetry(fileName, reason, deleteErr)
	}

	s.invalidateInventoryCache()
	s.invalidateDashboardInventoryCache()
	s.invalidateAdminCache()
	s.notifyQueueUsers(ctx)
	return nil
}

func (s *Service) finalizeClaimReservedToken(ctx context.Context, tokenID int64, userID int64, apiKeyID *int64, requestID string, hourlyLimit int, apiKeyLimit int) (*claimAllocation, error) {
	for attempt := 0; attempt < 8; attempt++ {
		item, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (*claimAllocation, error) {
			now := time.Now().Unix()

			row := tx.QueryRowContext(ctx, `
				SELECT id,
				       file_name,
				       file_path,
				       encoding,
				       content_json,
				       claim_count,
				       max_claims,
				       provider_user_id,
				       provider_username,
				       provider_name,
				       is_active,
				       is_enabled,
				       is_banned,
				       is_available
				FROM tokens
				WHERE id = ?
			`, tokenID)

			var (
				item             claimAllocation
				contentJSON      string
				claimCount       int
				maxClaims        int
				providerUserID   sql.NullString
				providerUsername sql.NullString
				providerName     sql.NullString
				isActive         int
				isEnabled        int
				isBanned         int
				isAvailable      int
			)
			if err := row.Scan(
				&item.TokenID,
				&item.FileName,
				&item.FilePath,
				&item.Encoding,
				&contentJSON,
				&claimCount,
				&maxClaims,
				&providerUserID,
				&providerUsername,
				&providerName,
				&isActive,
				&isEnabled,
				&isBanned,
				&isAvailable,
			); err != nil {
				if err == sql.ErrNoRows {
					return nil, nil
				}
				return nil, fmt.Errorf("load reserved token %d: %w", tokenID, err)
			}

			if hasExisting, err := s.userAlreadyClaimedTokenTx(ctx, tx, userID, tokenID); err != nil {
				return nil, err
			} else if hasExisting || isActive != 1 || isEnabled != 1 || isBanned != 0 || isAvailable != 1 || claimCount >= maxClaims {
				if _, err := tx.ExecContext(ctx, `
					UPDATE tokens
					SET probe_lock_until_ts = NULL,
					    updated_at_ts = ?
					WHERE id = ?
				`, now, tokenID); err != nil {
					return nil, fmt.Errorf("release token probe lock %d: %w", tokenID, err)
				}
				return nil, nil
			}

			if hourlyLimit > 0 {
				used, err := s.countUserClaimsSince(ctx, tx, userID, now-3600)
				if err != nil {
					return nil, err
				}
				if used >= hourlyLimit {
					if _, err := tx.ExecContext(ctx, `
						UPDATE tokens
						SET probe_lock_until_ts = NULL,
						    updated_at_ts = ?
						WHERE id = ?
					`, now, tokenID); err != nil {
						return nil, fmt.Errorf("release token probe lock %d after hourly limit: %w", tokenID, err)
					}
					return nil, nil
				}
			}

			if apiKeyID != nil && apiKeyLimit > 0 {
				minuteUsed, err := s.countAPIKeyClaimsSince(ctx, tx, *apiKeyID, now-60)
				if err != nil {
					return nil, err
				}
				if minuteUsed >= apiKeyLimit {
					if _, err := tx.ExecContext(ctx, `
						UPDATE tokens
						SET probe_lock_until_ts = NULL,
						    updated_at_ts = ?
						WHERE id = ?
					`, now, tokenID); err != nil {
						return nil, fmt.Errorf("release token probe lock %d after api key limit: %w", tokenID, err)
					}
					return nil, nil
				}
			}

			newCount := claimCount + 1
			newAvailable := 0
			if newCount < maxClaims {
				newAvailable = 1
			}

			if _, err := tx.ExecContext(ctx, `
				UPDATE tokens
				SET claim_count = ?,
				    is_available = ?,
				    probe_lock_until_ts = NULL,
				    last_probe_at_ts = COALESCE(last_probe_at_ts, ?),
				    last_probe_status = CASE
						WHEN last_probe_status IS NULL OR last_probe_status = '' THEN 'ok'
						ELSE last_probe_status
					END,
				    updated_at_ts = ?
				WHERE id = ?
				  AND is_banned = 0
			`, newCount, newAvailable, now, now, tokenID); err != nil {
				return nil, fmt.Errorf("finalize token claim %d: %w", tokenID, err)
			}

			var apiKeyArg any
			if apiKeyID != nil {
				apiKeyArg = *apiKeyID
			}

			claimInsert, err := tx.ExecContext(ctx, `
				INSERT INTO token_claims (
					token_id,
					user_id,
					api_key_id,
					claimed_at_ts,
					is_hidden,
					claim_file_name,
					claim_file_path,
					claim_encoding,
					claim_content_json,
					provider_user_id,
					provider_username,
					provider_name,
					request_id
				) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
			`, tokenID, userID, apiKeyArg, now, item.FileName, item.FilePath, item.Encoding, contentJSON, providerUserID, providerUsername, providerName, requestID)
			if err != nil {
				return nil, fmt.Errorf("insert finalized token claim: %w", err)
			}

			claimID, err := claimInsert.LastInsertId()
			if err != nil {
				return nil, fmt.Errorf("read finalized token claim id: %w", err)
			}

			if _, err := tx.ExecContext(ctx, `
				INSERT INTO user_token_claims (user_id, token_id, first_claim_id, created_at_ts)
				VALUES (?, ?, ?, ?)
			`, userID, tokenID, claimID, now); err != nil {
				if isUniqueConstraintError(err) {
					return nil, errRetryAllocation
				}
				return nil, fmt.Errorf("insert user token claim: %w", err)
			}
			if err := s.refreshInventoryRuntimeTx(ctx, tx); err != nil {
				return nil, err
			}

			content, err := decodeJSONContent(contentJSON)
			if err != nil {
				return nil, err
			}
			item.ClaimID = claimID
			item.Content = content
			item.FirstClaim = claimCount == 0
			return &item, nil
		})
		if err != nil {
			if errors.Is(err, errRetryAllocation) {
				continue
			}
			return nil, err
		}
		if item != nil {
			return item, nil
		}
	}
	return nil, nil
}

func (s *Service) userAlreadyClaimedTokenTx(ctx context.Context, tx *sql.Tx, userID int64, tokenID int64) (bool, error) {
	var marker int
	if err := tx.QueryRowContext(ctx, `
		SELECT 1
		FROM user_token_claims
		WHERE user_id = ? AND token_id = ?
		LIMIT 1
	`, userID, tokenID).Scan(&marker); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("check existing user token claim: %w", err)
	}
	return true, nil
}
