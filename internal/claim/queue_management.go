package claim

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	defaultAdminQueueLimit = 50

	queueFailureThreshold = 3
	queueTimeoutWindow    = 60 * time.Minute

	queueStatusQueued    = "queued"
	queueStatusCancelled = "cancelled"
	queueStatusExpired   = "expired"

	queueCancelReasonUserMissing       = "system_user_missing"
	queueCancelReasonUserBanned        = "system_user_banned"
	queueCancelReasonAPIKeyUnavailable = "system_api_key_unavailable"
	queueCancelReasonTimeout           = "system_queue_timeout"
	queueCancelReasonFailureThreshold  = "system_failure_threshold"
	queueCancelReasonInvalidState      = "system_invalid_queue_state"
	queueCancelReasonStartupReset      = "system_startup_reset"

	adminQueueOnlyAll      = "all"
	adminQueueOnlyAbnormal = "abnormal"
	adminQueueOnlyTimeout  = "timeout"
)

type queueAdvanceResult struct {
	Changed bool
	Claimed bool
}

type queueFailureResult struct {
	Changed      bool
	Cancelled    bool
	FailureCount int
}

type queueValidationResult struct {
	Status string
	Reason string
	Valid  bool
}

type adminQueueListRow struct {
	userQueueEntry
	LinuxDOUserID sql.NullString
	Username      sql.NullString
	Name          sql.NullString
}

func (s *Service) queueTimeoutSeconds() int64 {
	return int64(queueTimeoutWindow / time.Second)
}

func mapKeysInt64(values map[int64]struct{}) []int64 {
	if len(values) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(values))
	for value := range values {
		keys = append(keys, value)
	}
	sort.Slice(keys, func(i int, j int) bool {
		return keys[i] < keys[j]
	})
	return keys
}

func (s *Service) afterQueueMutation(ctx context.Context, userIDs ...int64) {
	s.invalidateQueueRuntimeCache()
	s.invalidateDashboardQueueCache()
	s.invalidateAdminQueueCache()
	s.invalidateAdminCache()

	seen := make(map[int64]struct{}, len(userIDs))
	for _, userID := range userIDs {
		if userID <= 0 {
			continue
		}
		if _, ok := seen[userID]; ok {
			continue
		}
		seen[userID] = struct{}{}
		s.invalidateUserQueueCache(userID)
	}

	s.notifyQueueUsers(ctx, userIDs...)
}

func (s *Service) isQueueEntryTimedOut(entry userQueueEntry, nowTS int64) bool {
	if entry.Status != queueStatusQueued || entry.Remaining <= 0 || entry.EnqueuedAtTS <= 0 {
		return false
	}
	return nowTS-entry.EnqueuedAtTS >= s.queueTimeoutSeconds()
}

func (s *Service) queueWaitDurationSeconds(entry userQueueEntry, nowTS int64) int64 {
	if entry.EnqueuedAtTS <= 0 {
		return 0
	}
	endTS := nowTS
	if entry.CancelledAtTS.Valid && entry.CancelledAtTS.Int64 > 0 && entry.CancelledAtTS.Int64 < endTS {
		endTS = entry.CancelledAtTS.Int64
	}
	if endTS <= entry.EnqueuedAtTS {
		return 0
	}
	return endTS - entry.EnqueuedAtTS
}

func adminQueueVisibilityWhereClause() string {
	return `
		(
			(claim_queue.status = 'queued' AND claim_queue.remaining > 0)
			OR claim_queue.status = 'cancelled'
			OR claim_queue.status = 'expired'
		)
	`
}

func adminQueueOrderByClause(statusFilter string) string {
	switch statusFilter {
	case queueStatusQueued:
		return `
			claim_queue.queue_rank ASC,
			claim_queue.id ASC
		`
	case queueStatusCancelled, queueStatusExpired:
		return `
			COALESCE(claim_queue.cancelled_at_ts, claim_queue.enqueued_at_ts) DESC,
			claim_queue.id DESC
		`
	default:
		return `
			CASE WHEN claim_queue.status = 'queued' THEN 0 ELSE 1 END ASC,
			CASE WHEN claim_queue.status = 'queued' THEN claim_queue.queue_rank ELSE 0 END ASC,
			CASE WHEN claim_queue.status = 'queued' THEN 0 ELSE COALESCE(claim_queue.cancelled_at_ts, claim_queue.enqueued_at_ts) END DESC,
			claim_queue.id DESC
		`
	}
}

func (s *Service) validateQueueEntry(ctx context.Context, entry userQueueEntry) (queueValidationResult, error) {
	return s.validateQueueEntryQueryer(ctx, s.store.DB(), entry)
}

func (s *Service) validateQueueEntryQueryer(ctx context.Context, queryer sqlQueryer, entry userQueueEntry) (queueValidationResult, error) {
	user, err := s.getUserByIDQueryer(ctx, queryer, entry.UserID)
	if err != nil {
		return queueValidationResult{}, err
	}
	if user == nil {
		return queueValidationResult{
			Status: queueStatusCancelled,
			Reason: queueCancelReasonUserMissing,
			Valid:  false,
		}, nil
	}

	ban, err := s.getActiveBanPayloadQueryer(ctx, queryer, user.LinuxDOUserID)
	if err != nil {
		return queueValidationResult{}, err
	}
	if ban != nil {
		return queueValidationResult{
			Status: queueStatusCancelled,
			Reason: queueCancelReasonUserBanned,
			Valid:  false,
		}, nil
	}

	if entry.APIKeyID.Valid {
		active, err := s.isQueueAPIKeyActiveQueryer(ctx, queryer, entry.APIKeyID.Int64)
		if err != nil {
			return queueValidationResult{}, err
		}
		if !active {
			return queueValidationResult{
				Status: queueStatusCancelled,
				Reason: queueCancelReasonAPIKeyUnavailable,
				Valid:  false,
			}, nil
		}
	}

	return queueValidationResult{Valid: true}, nil
}

func (s *Service) isQueueAPIKeyActive(ctx context.Context, apiKeyID int64) (bool, error) {
	return s.isQueueAPIKeyActiveQueryer(ctx, s.store.DB(), apiKeyID)
}

func (s *Service) isQueueAPIKeyActiveQueryer(ctx context.Context, queryer sqlQueryer, apiKeyID int64) (bool, error) {
	var status string
	if err := queryer.QueryRowContext(ctx, `
		SELECT status
		FROM api_keys
		WHERE id = ?
	`, apiKeyID).Scan(&status); err != nil {
		if err == sql.ErrNoRows {
			return false, nil
		}
		return false, fmt.Errorf("query queue api key %d: %w", apiKeyID, err)
	}
	return strings.EqualFold(strings.TrimSpace(status), "active"), nil
}

func (s *Service) advanceQueueRow(ctx context.Context, row userQueueEntry, policy inventoryPolicy) (queueAdvanceResult, error) {
	validation, err := s.validateQueueEntry(ctx, row)
	if err != nil {
		return queueAdvanceResult{}, err
	}
	if !validation.Valid {
		changed, err := s.cancelQueueEntryWithStatus(ctx, row.ID, validation.Status, validation.Reason, nil)
		if err != nil {
			return queueAdvanceResult{}, err
		}
		if changed {
			if publishErr := s.publishQueueTerminalState(ctx, row, validation.Status, validation.Reason); publishErr != nil {
				s.logger.Warn("publish invalid queue terminal state", "queue_id", row.ID, "user_id", row.UserID, "request_id", row.RequestID, "error", publishErr)
			}
		}
		return queueAdvanceResult{Changed: changed}, nil
	}

	nowTS := time.Now().Unix()
	if s.isQueueEntryTimedOut(row, nowTS) {
		changed, err := s.cancelQueueEntryWithStatus(ctx, row.ID, queueStatusExpired, queueCancelReasonTimeout, nil)
		if err != nil {
			return queueAdvanceResult{}, err
		}
		if changed {
			if publishErr := s.publishQueueTerminalState(ctx, row, queueStatusExpired, queueCancelReasonTimeout); publishErr != nil {
				s.logger.Warn("publish timed out queue terminal state", "queue_id", row.ID, "user_id", row.UserID, "request_id", row.RequestID, "error", publishErr)
			}
		}
		return queueAdvanceResult{Changed: changed}, nil
	}

	remainingQuota, err := s.remainingHourlyQuota(ctx, row.UserID, policy.HourlyLimit)
	if err != nil {
		return queueAdvanceResult{}, err
	}
	if remainingQuota <= 0 {
		return queueAdvanceResult{}, nil
	}

	allowed := minInt(row.Remaining, remainingQuota)
	if row.APIKeyID.Valid && s.cfg.APIKeys.RatePerMinute > 0 {
		remainingMinute, err := s.remainingMinuteQuota(ctx, row.APIKeyID.Int64, s.cfg.APIKeys.RatePerMinute)
		if err != nil {
			return queueAdvanceResult{}, err
		}
		if remainingMinute <= 0 {
			return queueAdvanceResult{}, nil
		}
		allowed = minInt(allowed, remainingMinute)
	}
	if allowed <= 0 {
		return queueAdvanceResult{}, nil
	}

	granted := 0
	for granted < allowed {
		var queueAPIKeyID *int64
		if row.APIKeyID.Valid {
			value := row.APIKeyID.Int64
			queueAPIKeyID = &value
		}

		item, err := s.allocateClaimableToken(ctx, row.UserID, queueAPIKeyID, row.RequestID, policy.HourlyLimit, s.cfg.APIKeys.RatePerMinute)
		if err != nil {
			return queueAdvanceResult{}, err
		}
		if item == nil {
			break
		}
		granted++
	}

	if granted <= 0 {
		return queueAdvanceResult{}, nil
	}
	completed, err := s.consumeQueueGrant(ctx, row.ID, granted)
	if err != nil {
		return queueAdvanceResult{}, err
	}
	if completed {
		if publishErr := s.publishQueueCompletion(ctx, row); publishErr != nil {
			s.logger.Warn(
				"publish queue completion state",
				"queue_id",
				row.ID,
				"user_id",
				row.UserID,
				"request_id",
				row.RequestID,
				"error",
				publishErr,
			)
		}
	}
	return queueAdvanceResult{Changed: true, Claimed: true}, nil
}

func (s *Service) recordQueueFailure(ctx context.Context, entry userQueueEntry, reason string) (queueFailureResult, error) {
	trimmedReason := strings.TrimSpace(reason)
	if trimmedReason == "" {
		trimmedReason = "queue_processing_error"
	}

	return withTx(ctx, s.store.DB(), func(tx *sql.Tx) (queueFailureResult, error) {
		row := tx.QueryRowContext(ctx, `
			SELECT failure_count
			FROM claim_queue
			WHERE id = ? AND status = 'queued' AND remaining > 0
		`, entry.ID)

		var failureCount int
		if err := row.Scan(&failureCount); err != nil {
			if err == sql.ErrNoRows {
				return queueFailureResult{}, nil
			}
			return queueFailureResult{}, fmt.Errorf("load queue failure count for id %d: %w", entry.ID, err)
		}

		nowTS := time.Now().Unix()
		failureCount++
		if failureCount >= queueFailureThreshold {
			if _, err := tx.ExecContext(ctx, `
				UPDATE claim_queue
				SET status = ?,
				    cancel_reason = ?,
				    cancelled_at_ts = ?,
				    cancelled_by_user_id = NULL,
				    queue_rank = 0,
				    last_error_reason = ?,
				    last_error_at_ts = ?,
				    failure_count = ?
				WHERE id = ? AND status = 'queued' AND remaining > 0
			`, queueStatusCancelled, queueCancelReasonFailureThreshold, nowTS, trimmedReason, nowTS, failureCount, entry.ID); err != nil {
				return queueFailureResult{}, fmt.Errorf("cancel failed queue row %d: %w", entry.ID, err)
			}
			if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
				return queueFailureResult{}, err
			}
			return queueFailureResult{
				Changed:      true,
				Cancelled:    true,
				FailureCount: failureCount,
			}, nil
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE claim_queue
			SET last_error_reason = ?,
			    last_error_at_ts = ?,
			    failure_count = ?
			WHERE id = ? AND status = 'queued' AND remaining > 0
		`, trimmedReason, nowTS, failureCount, entry.ID); err != nil {
			return queueFailureResult{}, fmt.Errorf("update queue failure metadata for id %d: %w", entry.ID, err)
		}
		return queueFailureResult{
			Changed:      true,
			FailureCount: failureCount,
		}, nil
	})
}

func (s *Service) normalizeActiveQueueStateTx(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE claim_queue
		SET queue_rank = 0
		WHERE status != 'queued' OR remaining <= 0
	`); err != nil {
		return fmt.Errorf("reset inactive queue ranks: %w", err)
	}

	rows, err := tx.QueryContext(ctx, `
		SELECT id
		FROM claim_queue
		WHERE status = 'queued' AND remaining > 0
		ORDER BY enqueued_at_ts ASC, id ASC
	`)
	if err != nil {
		return fmt.Errorf("list active queue ids: %w", err)
	}
	defer rows.Close()

	ids := make([]int64, 0)
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("scan active queue id: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate active queue ids: %w", err)
	}

	for index, id := range ids {
		if _, err := tx.ExecContext(ctx, `
			UPDATE claim_queue
			SET queue_rank = ?
			WHERE id = ?
		`, index+1, id); err != nil {
			return fmt.Errorf("update queue rank for id %d: %w", id, err)
		}
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO queue_runtime (id, total_queued, updated_at_ts)
		VALUES (1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			total_queued = excluded.total_queued,
			updated_at_ts = excluded.updated_at_ts
	`, len(ids), time.Now().Unix()); err != nil {
		return fmt.Errorf("refresh queue runtime: %w", err)
	}

	return nil
}

func (s *Service) loadQueuedEntryByIDTx(ctx context.Context, tx *sql.Tx, queueID int64) (*userQueueEntry, error) {
	row := tx.QueryRowContext(ctx, `
		SELECT id,
		       user_id,
		       api_key_id,
		       requested,
		       remaining,
		       queue_rank,
		       enqueued_at_ts,
		       request_id,
		       status,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE id = ? AND status = 'queued' AND remaining > 0
	`, queueID)

	var item userQueueEntry
	if err := row.Scan(
		&item.ID,
		&item.UserID,
		&item.APIKeyID,
		&item.Requested,
		&item.Remaining,
		&item.QueueRank,
		&item.EnqueuedAtTS,
		&item.RequestID,
		&item.Status,
		&item.CancelReason,
		&item.CancelledAtTS,
		&item.CancelledByUserID,
		&item.LastErrorReason,
		&item.LastErrorAtTS,
		&item.FailureCount,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("load queued entry %d: %w", queueID, err)
	}
	return &item, nil
}

func (s *Service) listQueuedEntriesTx(ctx context.Context, tx *sql.Tx) ([]userQueueEntry, error) {
	rows, err := tx.QueryContext(ctx, `
		SELECT id,
		       user_id,
		       api_key_id,
		       requested,
		       remaining,
		       queue_rank,
		       enqueued_at_ts,
		       request_id,
		       status,
		       cancel_reason,
		       cancelled_at_ts,
		       cancelled_by_user_id,
		       last_error_reason,
		       last_error_at_ts,
		       failure_count
		FROM claim_queue
		WHERE status = 'queued' AND remaining > 0
		ORDER BY queue_rank ASC, id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list queued entries in tx: %w", err)
	}
	defer rows.Close()

	items := make([]userQueueEntry, 0)
	for rows.Next() {
		var item userQueueEntry
		if err := rows.Scan(
			&item.ID,
			&item.UserID,
			&item.APIKeyID,
			&item.Requested,
			&item.Remaining,
			&item.QueueRank,
			&item.EnqueuedAtTS,
			&item.RequestID,
			&item.Status,
			&item.CancelReason,
			&item.CancelledAtTS,
			&item.CancelledByUserID,
			&item.LastErrorReason,
			&item.LastErrorAtTS,
			&item.FailureCount,
		); err != nil {
			return nil, fmt.Errorf("scan queued entry in tx: %w", err)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate queued entries in tx: %w", err)
	}
	return items, nil
}

func (s *Service) cancelQueueEntryWithStatus(ctx context.Context, queueID int64, status string, reason string, cancelledByUserID *int64) (bool, error) {
	_, changed, _, err := s.cancelQueueEntryInternal(ctx, queueID, status, reason, cancelledByUserID)
	return changed, err
}

func (s *Service) cancelQueueEntryInternal(ctx context.Context, queueID int64, status string, reason string, cancelledByUserID *int64) (*userQueueEntry, bool, []int64, error) {
	trimmedReason := strings.TrimSpace(reason)
	if queueID <= 0 {
		return nil, false, nil, nil
	}
	if trimmedReason == "" {
		return nil, false, nil, fmt.Errorf("queue cancel reason is empty")
	}

	type cancelResult struct {
		entry       *userQueueEntry
		changed     bool
		affectedIDs []int64
	}

	result, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (cancelResult, error) {
		entry, err := s.loadQueuedEntryByIDTx(ctx, tx, queueID)
		if err != nil {
			return cancelResult{}, err
		}
		if entry == nil {
			return cancelResult{}, nil
		}

		nowTS := time.Now().Unix()
		var cancelledBy any
		if cancelledByUserID != nil {
			cancelledBy = *cancelledByUserID
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE claim_queue
			SET status = ?,
			    cancel_reason = ?,
			    cancelled_at_ts = ?,
			    cancelled_by_user_id = ?,
			    queue_rank = 0
			WHERE id = ? AND status = 'queued' AND remaining > 0
		`, status, trimmedReason, nowTS, cancelledBy, queueID); err != nil {
			return cancelResult{}, fmt.Errorf("cancel queue row %d: %w", queueID, err)
		}
		if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
			return cancelResult{}, err
		}
		return cancelResult{
			entry:       entry,
			changed:     true,
			affectedIDs: []int64{entry.UserID},
		}, nil
	})
	if err != nil {
		return nil, false, nil, err
	}
	return result.entry, result.changed, result.affectedIDs, nil
}

func (s *Service) cancelQueueEntry(ctx context.Context, queueID int64, status string, reason string, cancelledByUserID *int64) (*userQueueEntry, error) {
	entry, changed, affectedIDs, err := s.cancelQueueEntryInternal(ctx, queueID, status, reason, cancelledByUserID)
	if err != nil {
		return nil, err
	}
	if changed {
		if entry != nil {
			if publishErr := s.publishQueueTerminalState(ctx, *entry, status, reason); publishErr != nil {
				s.logger.Warn("publish queue cancel terminal state", "queue_id", entry.ID, "user_id", entry.UserID, "request_id", entry.RequestID, "error", publishErr)
			}
		}
		s.afterQueueMutation(ctx, affectedIDs...)
		s.wakeQueuePump()
	}
	return entry, nil
}

func (s *Service) cancelQueuedEntriesByUser(ctx context.Context, userID int64, status string, reason string, cancelledByUserID *int64) ([]userQueueEntry, error) {
	trimmedReason := strings.TrimSpace(reason)
	if userID <= 0 {
		return nil, nil
	}
	if trimmedReason == "" {
		return nil, fmt.Errorf("queue cancel reason is empty")
	}

	type cancelResult struct {
		items       []userQueueEntry
		affectedIDs []int64
	}

	result, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (cancelResult, error) {
		rows, err := tx.QueryContext(ctx, `
			SELECT id,
			       user_id,
			       api_key_id,
			       requested,
			       remaining,
			       queue_rank,
			       enqueued_at_ts,
			       request_id,
			       status,
			       cancel_reason,
			       cancelled_at_ts,
			       cancelled_by_user_id,
			       last_error_reason,
			       last_error_at_ts,
			       failure_count
			FROM claim_queue
			WHERE user_id = ? AND status = 'queued' AND remaining > 0
			ORDER BY queue_rank ASC, id ASC
		`, userID)
		if err != nil {
			return cancelResult{}, fmt.Errorf("list queued entries for user %d: %w", userID, err)
		}
		defer rows.Close()

		items := make([]userQueueEntry, 0)
		for rows.Next() {
			var item userQueueEntry
			if err := rows.Scan(
				&item.ID,
				&item.UserID,
				&item.APIKeyID,
				&item.Requested,
				&item.Remaining,
				&item.QueueRank,
				&item.EnqueuedAtTS,
				&item.RequestID,
				&item.Status,
				&item.CancelReason,
				&item.CancelledAtTS,
				&item.CancelledByUserID,
				&item.LastErrorReason,
				&item.LastErrorAtTS,
				&item.FailureCount,
			); err != nil {
				return cancelResult{}, fmt.Errorf("scan user queue entry: %w", err)
			}
			items = append(items, item)
		}
		if err := rows.Err(); err != nil {
			return cancelResult{}, fmt.Errorf("iterate user queue entries: %w", err)
		}
		if len(items) == 0 {
			return cancelResult{}, nil
		}

		nowTS := time.Now().Unix()
		var cancelledBy any
		if cancelledByUserID != nil {
			cancelledBy = *cancelledByUserID
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE claim_queue
			SET status = ?,
			    cancel_reason = ?,
			    cancelled_at_ts = ?,
			    cancelled_by_user_id = ?,
			    queue_rank = 0
			WHERE user_id = ? AND status = 'queued' AND remaining > 0
		`, status, trimmedReason, nowTS, cancelledBy, userID); err != nil {
			return cancelResult{}, fmt.Errorf("cancel queued entries for user %d: %w", userID, err)
		}
		if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
			return cancelResult{}, err
		}
		return cancelResult{
			items:       items,
			affectedIDs: []int64{userID},
		}, nil
	})
	if err != nil {
		return nil, err
	}
	if len(result.affectedIDs) > 0 {
		for _, item := range result.items {
			if publishErr := s.publishQueueTerminalState(ctx, item, status, reason); publishErr != nil {
				s.logger.Warn("publish user queue cancel terminal state", "queue_id", item.ID, "user_id", item.UserID, "request_id", item.RequestID, "error", publishErr)
			}
		}
		s.afterQueueMutation(ctx, result.affectedIDs...)
		s.wakeQueuePump()
	}
	return result.items, nil
}

func (s *Service) reconcileQueue(ctx context.Context, expireTimedOut bool) (map[string]int, error) {
	summary := map[string]int{
		"scanned":              0,
		"kept":                 0,
		"cancelled":            0,
		"expired":              0,
		"invalid_user_missing": 0,
		"invalid_user_banned":  0,
		"invalid_api_key":      0,
		"timed_out":            0,
	}
	affectedUsers := make(map[int64]struct{})
	terminalEntries := make([]userQueueEntry, 0)
	nowTS := time.Now().Unix()

	if _, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (struct{}, error) {
		queueRows, err := s.listQueuedEntriesTx(ctx, tx)
		if err != nil {
			return struct{}{}, err
		}

		for _, row := range queueRows {
			summary["scanned"]++

			validation, err := s.validateQueueEntryQueryer(ctx, tx, row)
			if err != nil {
				return struct{}{}, err
			}
			if !validation.Valid {
				if _, err := tx.ExecContext(ctx, `
					UPDATE claim_queue
					SET status = ?,
					    cancel_reason = ?,
					    cancelled_at_ts = ?,
					    cancelled_by_user_id = NULL,
					    queue_rank = 0
					WHERE id = ? AND status = 'queued' AND remaining > 0
				`, validation.Status, validation.Reason, nowTS, row.ID); err != nil {
					return struct{}{}, fmt.Errorf("reconcile cancel queue row %d: %w", row.ID, err)
				}
				terminalEntries = append(terminalEntries, row)
				affectedUsers[row.UserID] = struct{}{}
				summary["cancelled"]++
				switch validation.Reason {
				case queueCancelReasonUserMissing:
					summary["invalid_user_missing"]++
				case queueCancelReasonUserBanned:
					summary["invalid_user_banned"]++
				case queueCancelReasonAPIKeyUnavailable:
					summary["invalid_api_key"]++
				}
				continue
			}

			if expireTimedOut && s.isQueueEntryTimedOut(row, nowTS) {
				if _, err := tx.ExecContext(ctx, `
					UPDATE claim_queue
					SET status = ?,
					    cancel_reason = ?,
					    cancelled_at_ts = ?,
					    cancelled_by_user_id = NULL,
					    queue_rank = 0
					WHERE id = ? AND status = 'queued' AND remaining > 0
				`, queueStatusExpired, queueCancelReasonTimeout, nowTS, row.ID); err != nil {
					return struct{}{}, fmt.Errorf("reconcile expire queue row %d: %w", row.ID, err)
				}
				terminalEntries = append(terminalEntries, row)
				affectedUsers[row.UserID] = struct{}{}
				summary["expired"]++
				summary["timed_out"]++
				continue
			}

			summary["kept"]++
		}

		if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
			return struct{}{}, err
		}
		totalQueued, err := s.getTotalQueued(ctx, tx)
		if err != nil {
			return struct{}{}, err
		}
		summary["queued_after"] = totalQueued
		return struct{}{}, nil
	}); err != nil {
		return nil, err
	}

	for _, item := range terminalEntries {
		status := item.Status
		reason := strings.TrimSpace(item.CancelReason.String)
		if validation, err := s.validateQueueEntry(ctx, item); err == nil && !validation.Valid {
			status = validation.Status
			reason = validation.Reason
		}
		if s.isQueueEntryTimedOut(item, nowTS) {
			status = queueStatusExpired
			reason = queueCancelReasonTimeout
		}
		if publishErr := s.publishQueueTerminalState(ctx, item, status, reason); publishErr != nil {
			s.logger.Warn("publish reconciled queue terminal state", "queue_id", item.ID, "user_id", item.UserID, "request_id", item.RequestID, "error", publishErr)
		}
	}
	if len(affectedUsers) > 0 {
		s.afterQueueMutation(ctx, mapKeysInt64(affectedUsers)...)
	}
	return summary, nil
}

func (s *Service) clearQueuedEntriesOnStartup(ctx context.Context) (map[string]int, error) {
	summary := map[string]int{
		"cancelled":     0,
		"startup_reset": 0,
		"queued_after":  0,
	}
	affectedUsers := make(map[int64]struct{})
	cancelledEntries := make([]userQueueEntry, 0)
	nowTS := time.Now().Unix()

	if _, err := withTx(ctx, s.store.DB(), func(tx *sql.Tx) (struct{}, error) {
		rows, err := tx.QueryContext(ctx, `
			SELECT id,
			       user_id,
			       api_key_id,
			       requested,
			       remaining,
			       queue_rank,
			       enqueued_at_ts,
			       request_id,
			       status,
			       cancel_reason,
			       cancelled_at_ts,
			       cancelled_by_user_id,
			       last_error_reason,
			       last_error_at_ts,
			       failure_count
			FROM claim_queue
			WHERE status = 'queued'
		`)
		if err != nil {
			return struct{}{}, fmt.Errorf("list startup queued entries: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var item userQueueEntry
			if err := rows.Scan(
				&item.ID,
				&item.UserID,
				&item.APIKeyID,
				&item.Requested,
				&item.Remaining,
				&item.QueueRank,
				&item.EnqueuedAtTS,
				&item.RequestID,
				&item.Status,
				&item.CancelReason,
				&item.CancelledAtTS,
				&item.CancelledByUserID,
				&item.LastErrorReason,
				&item.LastErrorAtTS,
				&item.FailureCount,
			); err != nil {
				return struct{}{}, fmt.Errorf("scan startup queued entry: %w", err)
			}
			cancelledEntries = append(cancelledEntries, item)
			if item.UserID > 0 {
				affectedUsers[item.UserID] = struct{}{}
			}
		}
		if err := rows.Err(); err != nil {
			return struct{}{}, fmt.Errorf("iterate startup queued entries: %w", err)
		}

		result, err := tx.ExecContext(ctx, `
			UPDATE claim_queue
			SET status = ?,
			    cancel_reason = ?,
			    cancelled_at_ts = ?,
			    cancelled_by_user_id = NULL,
			    queue_rank = 0
			WHERE status = 'queued'
		`, queueStatusCancelled, queueCancelReasonStartupReset, nowTS)
		if err != nil {
			return struct{}{}, fmt.Errorf("clear queued entries on startup: %w", err)
		}
		if err := s.normalizeActiveQueueStateTx(ctx, tx); err != nil {
			return struct{}{}, err
		}

		cancelled := 0
		if affected, err := result.RowsAffected(); err == nil {
			cancelled = int(affected)
		}
		summary["cancelled"] = cancelled
		summary["startup_reset"] = cancelled

		totalQueued, err := s.getTotalQueued(ctx, tx)
		if err != nil {
			return struct{}{}, err
		}
		summary["queued_after"] = totalQueued
		return struct{}{}, nil
	}); err != nil {
		return nil, err
	}

	for _, item := range cancelledEntries {
		if publishErr := s.publishQueueTerminalState(ctx, item, queueStatusCancelled, queueCancelReasonStartupReset); publishErr != nil {
			s.logger.Warn("publish startup reset queue terminal state", "queue_id", item.ID, "user_id", item.UserID, "request_id", item.RequestID, "error", publishErr)
		}
	}
	if len(affectedUsers) > 0 {
		s.afterQueueMutation(ctx, mapKeysInt64(affectedUsers)...)
	}
	return summary, nil
}

func (s *Service) startupReconcileQueue(ctx context.Context) (map[string]int, error) {
	return s.clearQueuedEntriesOnStartup(ctx)
}

func (s *Service) RefreshQueue(ctx context.Context) (map[string]any, error) {
	summary, err := s.reconcileQueue(ctx, true)
	if err != nil {
		return nil, err
	}
	if err := s.AdvanceQueue(ctx); err != nil {
		return nil, err
	}
	totalQueued, err := s.getTotalQueued(ctx, s.store.DB())
	if err != nil {
		return nil, err
	}

	result := make(map[string]any, len(summary)+2)
	for key, value := range summary {
		result[key] = value
	}
	result["total_queued"] = totalQueued
	result["ok"] = true
	return result, nil
}

func (s *Service) ListQueueForAdmin(ctx context.Context, search string, statusFilter string, onlyFilter string, limit int, offset int) (map[string]any, error) {
	limit = clampInt(limit, 1, 200, 100)
	offset = maxInt(0, offset)

	nowTS := time.Now().Unix()
	timeoutCutoff := nowTS - s.queueTimeoutSeconds()

	whereParts := []string{adminQueueVisibilityWhereClause()}
	params := make([]any, 0)
	normalizedStatusFilter := strings.ToLower(strings.TrimSpace(statusFilter))

	switch normalizedStatusFilter {
	case queueStatusQueued:
		whereParts = append(whereParts, "claim_queue.status = ?")
		whereParts = append(whereParts, "claim_queue.remaining > 0")
		params = append(params, normalizedStatusFilter)
	case queueStatusCancelled, queueStatusExpired:
		whereParts = append(whereParts, "claim_queue.status = ?")
		params = append(params, normalizedStatusFilter)
	default:
		normalizedStatusFilter = "all"
	}

	switch normalized := strings.ToLower(strings.TrimSpace(onlyFilter)); normalized {
	case adminQueueOnlyAbnormal:
		whereParts = append(whereParts, `
			(
				claim_queue.failure_count > 0
				OR COALESCE(TRIM(claim_queue.last_error_reason), '') != ''
				OR claim_queue.cancel_reason = ?
			)
		`)
		params = append(params, queueCancelReasonFailureThreshold)
	case adminQueueOnlyTimeout:
		whereParts = append(whereParts, `
			(
				(claim_queue.status = 'queued' AND claim_queue.remaining > 0 AND claim_queue.enqueued_at_ts <= ?)
				OR (claim_queue.status = 'expired' AND claim_queue.cancel_reason = ?)
			)
		`)
		params = append(params, timeoutCutoff, queueCancelReasonTimeout)
	}

	if trimmed := strings.ToLower(strings.TrimSpace(search)); trimmed != "" {
		pattern := "%" + trimmed + "%"
		whereParts = append(whereParts, `
			(
				CAST(claim_queue.id AS TEXT) LIKE ?
				OR CAST(claim_queue.user_id AS TEXT) LIKE ?
				OR lower(COALESCE(users.linuxdo_user_id, '')) LIKE ?
				OR lower(COALESCE(users.linuxdo_username, '')) LIKE ?
				OR lower(claim_queue.request_id) LIKE ?
			)
		`)
		params = append(params, pattern, pattern, pattern, pattern, pattern)
	}

	totalQuery := fmt.Sprintf(`
		SELECT COUNT(*)
		FROM claim_queue
		LEFT JOIN users ON users.id = claim_queue.user_id
		WHERE %s
	`, strings.Join(whereParts, " AND "))
	total, err := queryCount(ctx, s.store.DB(), totalQuery, params...)
	if err != nil {
		return nil, err
	}

	listQuery := fmt.Sprintf(`
		SELECT claim_queue.id,
		       claim_queue.user_id,
		       claim_queue.api_key_id,
		       claim_queue.requested,
		       claim_queue.remaining,
		       claim_queue.queue_rank,
		       claim_queue.enqueued_at_ts,
		       claim_queue.request_id,
		       claim_queue.status,
		       claim_queue.cancel_reason,
		       claim_queue.cancelled_at_ts,
		       claim_queue.cancelled_by_user_id,
		       claim_queue.last_error_reason,
		       claim_queue.last_error_at_ts,
		       claim_queue.failure_count,
		       users.linuxdo_user_id,
		       users.linuxdo_username,
		       users.linuxdo_name
		FROM claim_queue
		LEFT JOIN users ON users.id = claim_queue.user_id
		WHERE %s
		ORDER BY %s
		LIMIT ? OFFSET ?
	`, strings.Join(whereParts, " AND "), adminQueueOrderByClause(normalizedStatusFilter))
	rows, err := s.store.DB().QueryContext(ctx, listQuery, append(params, limit, offset)...)
	if err != nil {
		return nil, fmt.Errorf("query admin queue: %w", err)
	}
	defer rows.Close()

	items := make([]map[string]any, 0)
	for rows.Next() {
		var item adminQueueListRow
		if err := rows.Scan(
			&item.ID,
			&item.UserID,
			&item.APIKeyID,
			&item.Requested,
			&item.Remaining,
			&item.QueueRank,
			&item.EnqueuedAtTS,
			&item.RequestID,
			&item.Status,
			&item.CancelReason,
			&item.CancelledAtTS,
			&item.CancelledByUserID,
			&item.LastErrorReason,
			&item.LastErrorAtTS,
			&item.FailureCount,
			&item.LinuxDOUserID,
			&item.Username,
			&item.Name,
		); err != nil {
			return nil, fmt.Errorf("scan admin queue row: %w", err)
		}
		items = append(items, buildAdminQueuePayload(item, nowTS, timeoutCutoff))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate admin queue rows: %w", err)
	}

	return map[string]any{
		"items":  items,
		"total":  total,
		"limit":  limit,
		"offset": offset,
	}, nil
}

func buildAdminQueuePayload(item adminQueueListRow, nowTS int64, timeoutCutoff int64) map[string]any {
	payload := map[string]any{
		"queue_id":              item.ID,
		"user_id":               item.UserID,
		"linuxdo_user_id":       nil,
		"username":              nil,
		"name":                  nil,
		"api_key_id":            nil,
		"requested":             item.Requested,
		"remaining":             item.Remaining,
		"queue_position":        nil,
		"enqueued_at":           isoformatFromTS(item.EnqueuedAtTS),
		"wait_duration_seconds": queueWaitDurationSecondsForAdmin(item.userQueueEntry, nowTS),
		"request_id":            item.RequestID,
		"status":                item.Status,
		"last_error_reason":     nil,
		"last_error_at":         nil,
		"failure_count":         item.FailureCount,
		"cancel_reason":         nil,
		"cancelled_at":          nil,
		"cancelled_by_user_id":  nil,
		"is_timeout":            false,
		"is_abnormal":           false,
	}
	if item.LinuxDOUserID.Valid {
		payload["linuxdo_user_id"] = item.LinuxDOUserID.String
	}
	if item.Username.Valid {
		payload["username"] = item.Username.String
	}
	if item.Name.Valid {
		payload["name"] = firstNonEmpty(item.Name.String, item.Username.String, item.LinuxDOUserID.String)
	} else if item.Username.Valid || item.LinuxDOUserID.Valid {
		payload["name"] = firstNonEmpty(item.Username.String, item.LinuxDOUserID.String)
	}
	if item.APIKeyID.Valid {
		payload["api_key_id"] = item.APIKeyID.Int64
	}
	if item.Status == queueStatusQueued && item.Remaining > 0 {
		payload["queue_position"] = item.QueueRank
	}
	if item.LastErrorReason.Valid {
		payload["last_error_reason"] = item.LastErrorReason.String
	}
	if item.LastErrorAtTS.Valid {
		payload["last_error_at"] = isoformatFromTS(item.LastErrorAtTS.Int64)
	}
	if item.CancelReason.Valid {
		payload["cancel_reason"] = item.CancelReason.String
	}
	if item.CancelledAtTS.Valid {
		payload["cancelled_at"] = isoformatFromTS(item.CancelledAtTS.Int64)
	}
	if item.CancelledByUserID.Valid {
		payload["cancelled_by_user_id"] = item.CancelledByUserID.Int64
	}

	isTimedOut := (item.Status == queueStatusQueued && item.Remaining > 0 && item.EnqueuedAtTS <= timeoutCutoff) ||
		(item.Status == queueStatusExpired && item.CancelReason.Valid && item.CancelReason.String == queueCancelReasonTimeout)
	isAbnormal := item.FailureCount > 0 ||
		(item.LastErrorReason.Valid && strings.TrimSpace(item.LastErrorReason.String) != "") ||
		(item.CancelReason.Valid && item.CancelReason.String == queueCancelReasonFailureThreshold)

	payload["is_timeout"] = isTimedOut
	payload["is_abnormal"] = isAbnormal
	return payload
}

func queueWaitDurationSecondsForAdmin(entry userQueueEntry, nowTS int64) int64 {
	if entry.EnqueuedAtTS <= 0 {
		return 0
	}
	endTS := nowTS
	if entry.CancelledAtTS.Valid && entry.CancelledAtTS.Int64 > 0 && entry.CancelledAtTS.Int64 < endTS {
		endTS = entry.CancelledAtTS.Int64
	}
	if endTS <= entry.EnqueuedAtTS {
		return 0
	}
	return endTS - entry.EnqueuedAtTS
}
