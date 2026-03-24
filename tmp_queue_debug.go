package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"time"

	dbstore "token-atlas/internal/database"

	_ "modernc.org/sqlite"
)

func main() {
	dbPath := "token_atlas.db"
	if len(os.Args) > 1 && os.Args[1] != "" {
		dbPath = os.Args[1]
	}
	queueID := int64(1931)
	if len(os.Args) > 2 && os.Args[2] != "" {
		if parsed, err := strconv.ParseInt(os.Args[2], 10, 64); err == nil && parsed > 0 {
			queueID = parsed
		}
	}

	store, err := dbstore.OpenWithOptions(dbPath, dbstore.OpenOptions{})
	if err != nil {
		panic(err)
	}
	defer store.Close()
	if err := store.Init(context.Background()); err != nil {
		panic(err)
	}
	db := store.DB()

	printQueueSchema(db)

	type queueRow struct {
		ID             int64
		UserID         int64
		RequestID      string
		Status         string
		Remaining      int
		QueueRank      int
		BlockReason    sql.NullString
		NextRetryAtTS  sql.NullInt64
		LastProgressTS sql.NullInt64
		LastError      sql.NullString
		LastErrorTS    sql.NullInt64
		EnqueuedAtTS   int64
	}

	var row queueRow
	err = db.QueryRow(`
		SELECT id,
		       user_id,
		       request_id,
		       status,
		       remaining,
		       queue_rank,
		       block_reason,
		       next_retry_at_ts,
		       last_progress_at_ts,
		       last_error_reason,
		       last_error_at_ts,
		       enqueued_at_ts
		FROM claim_queue
		WHERE id = ?
	`, queueID).Scan(
		&row.ID,
		&row.UserID,
		&row.RequestID,
		&row.Status,
		&row.Remaining,
		&row.QueueRank,
		&row.BlockReason,
		&row.NextRetryAtTS,
		&row.LastProgressTS,
		&row.LastError,
		&row.LastErrorTS,
		&row.EnqueuedAtTS,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			fmt.Printf("database=%s\n", dbPath)
			fmt.Printf("queue #%d not found; recent rows:\n", queueID)
			printRecentQueueRows(db)
			return
		}
		fmt.Fprintln(os.Stderr, "queue row:", err)
		os.Exit(1)
	}

	fmt.Printf("database=%s\n", dbPath)
	fmt.Printf("queue #%d user=%d request=%s status=%s remaining=%d rank=%d enqueued=%s\n",
		row.ID, row.UserID, row.RequestID, row.Status, row.Remaining, row.QueueRank, formatTS(row.EnqueuedAtTS))
	fmt.Printf("block_reason=%q next_retry=%s last_progress=%s last_error=%q last_error_at=%s now=%s\n",
		nullString(row.BlockReason), formatNullTS(row.NextRetryAtTS), formatNullTS(row.LastProgressTS), nullString(row.LastError), formatNullTS(row.LastErrorTS), time.Now().Format(time.RFC3339))

	printRuntimeRows(db)
	printCount(db, "active queue", `SELECT COUNT(*) FROM claim_queue WHERE status IN ('queued','queued_waiting','queued_blocked') AND remaining > 0`)
	printCount(db, "available tokens", `SELECT COUNT(*) FROM tokens WHERE is_active=1 AND is_enabled=1 AND is_banned=0 AND is_available=1 AND claim_count < max_claims`)
	printCount(db, "available unlocked tokens", `SELECT COUNT(*) FROM tokens WHERE is_active=1 AND is_enabled=1 AND is_banned=0 AND is_available=1 AND claim_count < max_claims AND (probe_lock_until_ts IS NULL OR probe_lock_until_ts < ?)`, time.Now().Unix())
	printCount(db, "user eligible unlocked", `SELECT COUNT(*) FROM tokens WHERE is_active=1 AND is_enabled=1 AND is_banned=0 AND is_available=1 AND claim_count < max_claims AND (probe_lock_until_ts IS NULL OR probe_lock_until_ts < ?) AND NOT EXISTS (SELECT 1 FROM user_token_claims WHERE user_token_claims.user_id = ? AND user_token_claims.token_id = tokens.id)`, time.Now().Unix(), row.UserID)
	printCount(db, "user eligible potential", `SELECT COUNT(*) FROM tokens WHERE is_active=1 AND is_enabled=1 AND is_banned=0 AND is_available=1 AND claim_count < max_claims AND NOT EXISTS (SELECT 1 FROM user_token_claims WHERE user_token_claims.user_id = ? AND user_token_claims.token_id = tokens.id)`, row.UserID)
	printActiveQueueRows(db)

	rows, err := db.Query(`
		SELECT id, claim_count, max_claims, probe_lock_until_ts, last_probe_status, file_name
		FROM tokens
		WHERE is_active=1 AND is_enabled=1 AND is_banned=0 AND is_available=1 AND claim_count < max_claims
		ORDER BY created_at_ts ASC, id ASC
		LIMIT 10
	`)
	if err != nil {
		fmt.Fprintln(os.Stderr, "tokens:", err)
		os.Exit(1)
	}
	defer rows.Close()

	fmt.Println("tokens:")
	for rows.Next() {
		var (
			id             int64
			claimCount     int
			maxClaims      int
			lockTS         sql.NullInt64
			lastProbe      sql.NullString
			fileName       string
		)
		if err := rows.Scan(&id, &claimCount, &maxClaims, &lockTS, &lastProbe, &fileName); err != nil {
			panic(err)
		}
		fmt.Printf("  token=%d file=%s claim=%d/%d lock=%s last_probe=%q\n", id, fileName, claimCount, maxClaims, formatNullTS(lockTS), nullString(lastProbe))
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func printRuntimeRows(db *sql.DB) {
	var (
		totalQueued int
		queueUpdated sql.NullInt64
	)
	if err := db.QueryRow(`SELECT total_queued, updated_at_ts FROM queue_runtime WHERE id = 1`).Scan(&totalQueued, &queueUpdated); err == nil {
		fmt.Printf("queue_runtime: total_queued=%d updated_at=%s\n", totalQueued, formatNullTS(queueUpdated))
	}

	var (
		status    string
		maxClaims int
		total     int
		available int
		unclaimed int
		updated   sql.NullInt64
	)
	if err := db.QueryRow(`
		SELECT status, max_claims, total_tokens, available_tokens, unclaimed_tokens, updated_at_ts
		FROM inventory_runtime
		WHERE id = 1
	`).Scan(&status, &maxClaims, &total, &available, &unclaimed, &updated); err == nil {
		fmt.Printf("inventory_runtime: status=%s max_claims=%d total=%d available=%d unclaimed=%d updated_at=%s\n",
			status, maxClaims, total, available, unclaimed, formatNullTS(updated))
	}
}

func printActiveQueueRows(db *sql.DB) {
	rows, err := db.Query(`
		SELECT id, user_id, request_id, status, remaining, block_reason, next_retry_at_ts, last_progress_at_ts, last_error_reason, enqueued_at_ts
		FROM claim_queue
		WHERE status IN ('queued','queued_waiting','queued_blocked') AND remaining > 0
		ORDER BY enqueued_at_ts ASC, id ASC
	`)
	if err != nil {
		fmt.Printf("active queue rows: %v\n", err)
		return
	}
	defer rows.Close()
	fmt.Println("active queue rows:")
	for rows.Next() {
		var (
			id            int64
			userID        int64
			requestID     string
			status        string
			remaining     int
			blockReason   sql.NullString
			nextRetry     sql.NullInt64
			lastProgress  sql.NullInt64
			lastError     sql.NullString
			enqueued      int64
		)
		if err := rows.Scan(&id, &userID, &requestID, &status, &remaining, &blockReason, &nextRetry, &lastProgress, &lastError, &enqueued); err != nil {
			panic(err)
		}
		fmt.Printf("  id=%d user=%d request=%s status=%s remaining=%d block=%q next_retry=%s last_progress=%s last_error=%q enqueued=%s\n",
			id, userID, requestID, status, remaining, nullString(blockReason), formatNullTS(nextRetry), formatNullTS(lastProgress), nullString(lastError), formatTS(enqueued))
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func printQueueSchema(db *sql.DB) {
	rows, err := db.Query(`PRAGMA table_info(claim_queue)`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "claim_queue schema: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	fmt.Println("claim_queue columns:")
	for rows.Next() {
		var (
			cid       int
			name      string
			dataType  string
			notNull   int
			defaultV  sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultV, &primaryKey); err != nil {
			panic(err)
		}
		fmt.Printf("  %d: %s %s notnull=%d default=%q pk=%d\n", cid, name, dataType, notNull, nullString(defaultV), primaryKey)
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func printRecentQueueRows(db *sql.DB) {
	rows, err := db.Query(`
		SELECT id, user_id, request_id, status, remaining, enqueued_at_ts
		FROM claim_queue
		ORDER BY id DESC
		LIMIT 10
	`)
	if err != nil {
		fmt.Fprintf(os.Stderr, "recent queue rows: %v\n", err)
		os.Exit(1)
	}
	defer rows.Close()
	for rows.Next() {
		var (
			id        int64
			userID    int64
			requestID string
			status    string
			remaining int
			enqueued  int64
		)
		if err := rows.Scan(&id, &userID, &requestID, &status, &remaining, &enqueued); err != nil {
			panic(err)
		}
		fmt.Printf("  id=%d user=%d request=%s status=%s remaining=%d enqueued=%s\n", id, userID, requestID, status, remaining, formatTS(enqueued))
	}
	if err := rows.Err(); err != nil {
		panic(err)
	}
}

func printCount(db *sql.DB, label string, query string, args ...any) {
	var count int
	if err := db.QueryRow(query, args...).Scan(&count); err != nil {
		fmt.Printf("%s: error: %v\n", label, err)
		return
	}
	fmt.Printf("%s: %d\n", label, count)
}

func formatTS(ts int64) string {
	if ts <= 0 {
		return "-"
	}
	return time.Unix(ts, 0).Format(time.RFC3339)
}

func formatNullTS(ts sql.NullInt64) string {
	if !ts.Valid || ts.Int64 <= 0 {
		return "-"
	}
	return formatTS(ts.Int64)
}

func nullString(value sql.NullString) string {
	if !value.Valid {
		return ""
	}
	return value.String
}
