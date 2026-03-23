package database

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"

	_ "modernc.org/sqlite"
)

func TestInitCreatesSchemaAndIndexes(t *testing.T) {
	store := openTempStore(t)
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	for _, tableName := range []string{
		"users",
		"api_keys",
		"tokens",
		"token_claims",
		"user_token_claims",
		"claim_queue",
		"user_bans",
		"inventory_runtime",
		"queue_runtime",
	} {
		if !sqliteObjectExists(t, store.DB(), "table", tableName) {
			t.Fatalf("missing table %s", tableName)
		}
	}

	for _, indexName := range []string{
		"idx_token_claims_user_time",
		"idx_token_claims_api_time",
		"idx_token_claims_user_hidden_time",
		"idx_token_claims_user_token_hidden_time",
		"idx_token_claims_request_user_time",
		"idx_token_claims_claimed_at",
		"idx_token_claims_claimed_at_request_user",
		"idx_user_token_claims_user",
		"idx_claim_queue_status_time",
		"idx_claim_queue_user_status_remaining_time",
		"idx_claim_queue_status_remaining_time",
		"idx_claim_queue_status_rank",
		"idx_tokens_account_id_unique",
		"idx_tokens_access_token_hash_unique",
		"idx_token_claims_token_user",
		"idx_tokens_claimable_candidates",
		"idx_api_keys_user_status",
		"idx_user_bans_target_time",
		"idx_user_bans_active_lookup",
	} {
		if !sqliteObjectExists(t, store.DB(), "index", indexName) {
			t.Fatalf("missing index %s", indexName)
		}
	}
}

func TestInitMigratesLegacySchemaAndBackfillsDerivedData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "legacy.db")
	db := openSQLite(t, dbPath)
	createLegacySchema(t, db)
	seedLegacyData(t, db)
	if err := db.Close(); err != nil {
		t.Fatalf("close legacy db: %v", err)
	}

	store, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	tokenColumns := tableColumnSet(t, store.DB(), "tokens")
	for _, columnName := range []string{
		"is_active",
		"is_cleaned",
		"is_enabled",
		"is_banned",
		"banned_at_ts",
		"ban_reason",
		"cleaned_at_ts",
		"last_probe_at_ts",
		"last_probe_status",
		"probe_lock_until_ts",
		"account_id",
		"access_token_hash",
		"provider_user_id",
		"provider_username",
		"provider_name",
		"uploaded_at_ts",
	} {
		if _, ok := tokenColumns[columnName]; !ok {
			t.Fatalf("missing migrated tokens column %s", columnName)
		}
	}

	claimColumns := tableColumnSet(t, store.DB(), "token_claims")
	for _, columnName := range []string{
		"is_hidden",
		"claim_file_name",
		"claim_file_path",
		"claim_encoding",
		"claim_content_json",
		"provider_user_id",
		"provider_username",
		"provider_name",
	} {
		if _, ok := claimColumns[columnName]; !ok {
			t.Fatalf("missing migrated token_claims column %s", columnName)
		}
	}

	queueColumns := tableColumnSet(t, store.DB(), "claim_queue")
	for _, columnName := range []string{
		"queue_rank",
		"cancel_reason",
		"cancelled_at_ts",
		"cancelled_by_user_id",
		"last_error_reason",
		"last_error_at_ts",
		"failure_count",
	} {
		if _, ok := queueColumns[columnName]; !ok {
			t.Fatalf("missing migrated claim_queue column %s", columnName)
		}
	}

	inventoryColumns := tableColumnSet(t, store.DB(), "inventory_runtime")
	for _, columnName := range []string{
		"status",
		"total_tokens",
		"available_tokens",
		"unclaimed_tokens",
		"max_claims",
		"updated_at_ts",
	} {
		if _, ok := inventoryColumns[columnName]; !ok {
			t.Fatalf("missing migrated inventory_runtime column %s", columnName)
		}
	}

	var (
		accountID       string
		accessTokenHash string
		isActive        int
		isEnabled       int
	)
	if err := store.DB().QueryRow(`
		SELECT account_id, access_token_hash, is_active, is_enabled
		FROM tokens
		WHERE id = 10
	`).Scan(&accountID, &accessTokenHash, &isActive, &isEnabled); err != nil {
		t.Fatalf("query migrated token: %v", err)
	}
	if accountID != "acct-1" {
		t.Fatalf("unexpected account_id: %q", accountID)
	}
	if accessTokenHash != hashTokenValue("access-1") {
		t.Fatalf("unexpected access token hash: %q", accessTokenHash)
	}
	if isActive != 1 || isEnabled != 1 {
		t.Fatalf("unexpected migrated defaults: is_active=%d is_enabled=%d", isActive, isEnabled)
	}

	var queueRank1, queueRank2, queueRank3 int
	if err := store.DB().QueryRow(`SELECT queue_rank FROM claim_queue WHERE id = 1`).Scan(&queueRank1); err != nil {
		t.Fatalf("query queue rank 1: %v", err)
	}
	if err := store.DB().QueryRow(`SELECT queue_rank FROM claim_queue WHERE id = 2`).Scan(&queueRank2); err != nil {
		t.Fatalf("query queue rank 2: %v", err)
	}
	if err := store.DB().QueryRow(`SELECT queue_rank FROM claim_queue WHERE id = 3`).Scan(&queueRank3); err != nil {
		t.Fatalf("query queue rank 3: %v", err)
	}
	if queueRank1 != 1 || queueRank2 != 2 || queueRank3 != 0 {
		t.Fatalf("unexpected queue ranks: %d %d %d", queueRank1, queueRank2, queueRank3)
	}

	var totalQueued int
	if err := store.DB().QueryRow(`SELECT total_queued FROM queue_runtime WHERE id = 1`).Scan(&totalQueued); err != nil {
		t.Fatalf("query queue runtime: %v", err)
	}
	if totalQueued != 2 {
		t.Fatalf("unexpected total queued: %d", totalQueued)
	}

	var (
		firstHidden   int
		secondHidden  int
		claimName     string
		claimPath     string
		claimEncoding string
	)
	if err := store.DB().QueryRow(`
		SELECT is_hidden, claim_file_name, claim_file_path, claim_encoding
		FROM token_claims
		WHERE id = 100
	`).Scan(&firstHidden, &claimName, &claimPath, &claimEncoding); err != nil {
		t.Fatalf("query first claim: %v", err)
	}
	if err := store.DB().QueryRow(`SELECT is_hidden FROM token_claims WHERE id = 101`).Scan(&secondHidden); err != nil {
		t.Fatalf("query second claim: %v", err)
	}
	if firstHidden != 0 || secondHidden != 1 {
		t.Fatalf("unexpected hidden flags: first=%d second=%d", firstHidden, secondHidden)
	}
	if claimName != "sample.json" || claimPath != "./token/sample.json" || claimEncoding != "utf-8" {
		t.Fatalf("unexpected claim file backfill: %q %q %q", claimName, claimPath, claimEncoding)
	}

	var userTokenClaims int
	if err := store.DB().QueryRow(`SELECT COUNT(*) FROM user_token_claims`).Scan(&userTokenClaims); err != nil {
		t.Fatalf("count user_token_claims: %v", err)
	}
	if userTokenClaims != 1 {
		t.Fatalf("unexpected user_token_claims count: %d", userTokenClaims)
	}
}

func TestDashboardTimeRangeQueriesUseClaimTimeIndexes(t *testing.T) {
	store := openTempStore(t)
	t.Cleanup(func() {
		_ = store.Close()
	})

	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("init store: %v", err)
	}

	testCases := []struct {
		name        string
		query       string
		args        []any
		wantDetails []string
	}{
		{
			name: "leaderboard",
			query: `
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
			`,
			args: []any{0, 10},
			wantDetails: []string{
				"idx_token_claims_claimed_at",
				"idx_token_claims_claimed_at_request_user",
			},
		},
		{
			name: "trends",
			query: `
				SELECT CAST(claimed_at_ts / ? AS INTEGER) * ? AS bucket_ts,
				       COUNT(*) AS cnt
				FROM token_claims
				WHERE claimed_at_ts >= ?
				GROUP BY bucket_ts
				ORDER BY bucket_ts ASC
			`,
			args: []any{3600, 3600, 0},
			wantDetails: []string{
				"idx_token_claims_claimed_at",
				"idx_token_claims_claimed_at_request_user",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			details := sqliteQueryPlanDetails(t, store.DB(), tc.query, tc.args...)
			if !queryPlanContainsAny(details, tc.wantDetails...) {
				t.Fatalf("query plan for %s did not mention expected indexes; details=%v", tc.name, details)
			}
		})
	}
}

func openTempStore(t *testing.T) *Store {
	t.Helper()

	store, err := Open(filepath.Join(t.TempDir(), "token_atlas.db"))
	if err != nil {
		t.Fatalf("open temp store: %v", err)
	}
	return store
}

func openSQLite(t *testing.T, path string) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open sqlite db: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	return db
}

func sqliteObjectExists(t *testing.T, db *sql.DB, objectType string, name string) bool {
	t.Helper()

	var count int
	if err := db.QueryRow(`
		SELECT COUNT(*)
		FROM sqlite_master
		WHERE type = ? AND name = ?
	`, objectType, name).Scan(&count); err != nil {
		t.Fatalf("query sqlite_master for %s %s: %v", objectType, name, err)
	}

	return count == 1
}

func tableColumnSet(t *testing.T, db *sql.DB, tableName string) map[string]struct{} {
	t.Helper()

	rows, err := db.Query(`PRAGMA table_info(` + tableName + `)`)
	if err != nil {
		t.Fatalf("query table_info for %s: %v", tableName, err)
	}
	defer rows.Close()

	columns := map[string]struct{}{}
	for rows.Next() {
		var (
			cid        int
			name       string
			typ        string
			notNull    int
			defaultVal sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultVal, &primaryKey); err != nil {
			t.Fatalf("scan table_info for %s: %v", tableName, err)
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate table_info for %s: %v", tableName, err)
	}

	return columns
}

func sqliteQueryPlanDetails(t *testing.T, db *sql.DB, query string, args ...any) []string {
	t.Helper()

	rows, err := db.Query(`EXPLAIN QUERY PLAN `+query, args...)
	if err != nil {
		t.Fatalf("explain query plan: %v", err)
	}
	defer rows.Close()

	details := make([]string, 0)
	for rows.Next() {
		var (
			id     int
			parent int
			unused int
			detail string
		)
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatalf("scan query plan row: %v", err)
		}
		details = append(details, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate query plan rows: %v", err)
	}
	return details
}

func queryPlanContainsAny(details []string, wantDetails ...string) bool {
	for _, detail := range details {
		lowerDetail := strings.ToLower(detail)
		for _, want := range wantDetails {
			if strings.Contains(lowerDetail, strings.ToLower(want)) {
				return true
			}
		}
	}
	return false
}

func createLegacySchema(t *testing.T, db *sql.DB) {
	t.Helper()

	legacyStatements := []string{
		`CREATE TABLE users (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			linuxdo_user_id TEXT NOT NULL UNIQUE,
			linuxdo_username TEXT NOT NULL,
			linuxdo_name TEXT,
			trust_level INTEGER NOT NULL,
			created_at_ts INTEGER NOT NULL,
			last_login_at_ts INTEGER NOT NULL
		)`,
		`CREATE TABLE api_keys (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			name TEXT,
			key_hash TEXT NOT NULL UNIQUE,
			key_prefix TEXT NOT NULL,
			status TEXT NOT NULL,
			created_at_ts INTEGER NOT NULL,
			last_used_at_ts INTEGER,
			revoked_at_ts INTEGER
		)`,
		`CREATE TABLE tokens (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			file_name TEXT NOT NULL UNIQUE,
			file_path TEXT NOT NULL,
			file_hash TEXT NOT NULL,
			encoding TEXT NOT NULL,
			content_json TEXT NOT NULL,
			is_available INTEGER NOT NULL,
			claim_count INTEGER NOT NULL,
			max_claims INTEGER NOT NULL,
			created_at_ts INTEGER NOT NULL,
			updated_at_ts INTEGER NOT NULL,
			last_seen_at_ts INTEGER NOT NULL
		)`,
		`CREATE TABLE token_claims (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			token_id INTEGER NOT NULL,
			user_id INTEGER NOT NULL,
			api_key_id INTEGER,
			claimed_at_ts INTEGER NOT NULL,
			request_id TEXT NOT NULL
		)`,
		`CREATE TABLE user_token_claims (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			token_id INTEGER NOT NULL,
			first_claim_id INTEGER NOT NULL,
			created_at_ts INTEGER NOT NULL,
			UNIQUE(user_id, token_id)
		)`,
		`CREATE TABLE claim_queue (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			user_id INTEGER NOT NULL,
			api_key_id INTEGER,
			requested INTEGER NOT NULL,
			remaining INTEGER NOT NULL,
			enqueued_at_ts INTEGER NOT NULL,
			request_id TEXT NOT NULL,
			status TEXT NOT NULL
		)`,
		`CREATE TABLE inventory_runtime (
			id INTEGER PRIMARY KEY CHECK(id = 1),
			status TEXT NOT NULL,
			max_claims INTEGER NOT NULL,
			updated_at_ts INTEGER NOT NULL
		)`,
		`CREATE TABLE queue_runtime (
			id INTEGER PRIMARY KEY CHECK(id = 1),
			total_queued INTEGER NOT NULL,
			updated_at_ts INTEGER NOT NULL
		)`,
		`CREATE TABLE user_bans (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			linuxdo_user_id TEXT NOT NULL,
			username_snapshot TEXT,
			reason TEXT NOT NULL,
			banned_by_user_id INTEGER NOT NULL,
			banned_at_ts INTEGER NOT NULL,
			expires_at_ts INTEGER,
			unbanned_by_user_id INTEGER,
			unbanned_at_ts INTEGER
		)`,
	}

	for _, statement := range legacyStatements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatalf("apply legacy schema: %v", err)
		}
	}
}

func seedLegacyData(t *testing.T, db *sql.DB) {
	t.Helper()

	if _, err := db.Exec(`
		INSERT INTO users (id, linuxdo_user_id, linuxdo_username, linuxdo_name, trust_level, created_at_ts, last_login_at_ts)
		VALUES (1, 'linuxdo-1', 'demo-user', 'Demo User', 2, 1000, 1000)
	`); err != nil {
		t.Fatalf("insert user: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO tokens (
			id, file_name, file_path, file_hash, encoding, content_json,
			is_available, claim_count, max_claims, created_at_ts, updated_at_ts, last_seen_at_ts
		) VALUES (
			10, 'sample.json', './token/sample.json', 'hash-1', 'utf-8',
			'{"storage":{"access_token":"access-1","refresh_token":"refresh-1","account_id":"acct-1"}}',
			1, 0, 1, 1000, 1000, 1000
		)
	`); err != nil {
		t.Fatalf("insert token: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO token_claims (id, token_id, user_id, api_key_id, claimed_at_ts, request_id)
		VALUES
			(100, 10, 1, NULL, 1100, 'req-1'),
			(101, 10, 1, NULL, 1200, 'req-2')
	`); err != nil {
		t.Fatalf("insert token claims: %v", err)
	}

	if _, err := db.Exec(`
		INSERT INTO claim_queue (id, user_id, api_key_id, requested, remaining, enqueued_at_ts, request_id, status)
		VALUES
			(1, 1, NULL, 1, 1, 1500, 'queue-1', 'queued'),
			(2, 1, NULL, 1, 1, 1600, 'queue-2', 'queued'),
			(3, 1, NULL, 1, 0, 1700, 'queue-3', 'done')
	`); err != nil {
		t.Fatalf("insert claim queue rows: %v", err)
	}
}
