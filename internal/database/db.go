package database

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"runtime"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

const busyTimeoutMS = 1_500

var schemaStatements = []string{
	`CREATE TABLE IF NOT EXISTS users (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		linuxdo_user_id TEXT NOT NULL UNIQUE,
		linuxdo_username TEXT NOT NULL,
		linuxdo_name TEXT,
		trust_level INTEGER NOT NULL,
		created_at_ts INTEGER NOT NULL,
		last_login_at_ts INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS api_keys (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		name TEXT,
		key_hash TEXT NOT NULL UNIQUE,
		key_prefix TEXT NOT NULL,
		key_value TEXT,
		status TEXT NOT NULL,
		created_at_ts INTEGER NOT NULL,
		last_used_at_ts INTEGER,
		revoked_at_ts INTEGER,
		FOREIGN KEY(user_id) REFERENCES users(id)
	)`,
	`CREATE TABLE IF NOT EXISTS tokens (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		file_name TEXT NOT NULL UNIQUE,
		file_path TEXT NOT NULL,
		file_hash TEXT NOT NULL,
		encoding TEXT NOT NULL,
		content_json TEXT NOT NULL,
		account_id TEXT,
		access_token_hash TEXT,
		provider_user_id TEXT,
		provider_username TEXT,
		provider_name TEXT,
		uploaded_at_ts INTEGER,
		is_active INTEGER NOT NULL,
		is_cleaned INTEGER NOT NULL DEFAULT 0,
		is_enabled INTEGER NOT NULL DEFAULT 1,
		is_banned INTEGER NOT NULL DEFAULT 0,
		is_available INTEGER NOT NULL,
		claim_count INTEGER NOT NULL,
		max_claims INTEGER NOT NULL,
		created_at_ts INTEGER NOT NULL,
		banned_at_ts INTEGER,
		ban_reason TEXT,
		cleaned_at_ts INTEGER,
		last_probe_at_ts INTEGER,
		last_probe_status TEXT,
		probe_lock_until_ts INTEGER,
		updated_at_ts INTEGER NOT NULL,
		last_seen_at_ts INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS token_claims (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		token_id INTEGER NOT NULL,
		user_id INTEGER NOT NULL,
		api_key_id INTEGER,
		claimed_at_ts INTEGER NOT NULL,
		is_hidden INTEGER NOT NULL DEFAULT 0,
		claim_file_name TEXT,
		claim_file_path TEXT,
		claim_encoding TEXT,
		claim_content_json TEXT,
		provider_user_id TEXT,
		provider_username TEXT,
		provider_name TEXT,
		request_id TEXT NOT NULL,
		FOREIGN KEY(token_id) REFERENCES tokens(id),
		FOREIGN KEY(user_id) REFERENCES users(id),
		FOREIGN KEY(api_key_id) REFERENCES api_keys(id)
	)`,
	`CREATE TABLE IF NOT EXISTS user_token_claims (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		token_id INTEGER NOT NULL,
		first_claim_id INTEGER NOT NULL,
		created_at_ts INTEGER NOT NULL,
		UNIQUE(user_id, token_id),
		FOREIGN KEY(user_id) REFERENCES users(id),
		FOREIGN KEY(token_id) REFERENCES tokens(id),
		FOREIGN KEY(first_claim_id) REFERENCES token_claims(id)
	)`,
	`CREATE TABLE IF NOT EXISTS claim_queue (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		user_id INTEGER NOT NULL,
		api_key_id INTEGER,
		requested INTEGER NOT NULL,
		remaining INTEGER NOT NULL,
		queue_rank INTEGER NOT NULL,
		enqueued_at_ts INTEGER NOT NULL,
		request_id TEXT NOT NULL,
		status TEXT NOT NULL,
		cancel_reason TEXT,
		cancelled_at_ts INTEGER,
		cancelled_by_user_id INTEGER,
		last_error_reason TEXT,
		last_error_at_ts INTEGER,
		failure_count INTEGER NOT NULL DEFAULT 0
	)`,
	`CREATE TABLE IF NOT EXISTS inventory_runtime (
		id INTEGER PRIMARY KEY CHECK(id = 1),
		status TEXT NOT NULL,
		total_tokens INTEGER NOT NULL DEFAULT 0,
		available_tokens INTEGER NOT NULL DEFAULT 0,
		unclaimed_tokens INTEGER NOT NULL DEFAULT 0,
		max_claims INTEGER NOT NULL,
		updated_at_ts INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS queue_runtime (
		id INTEGER PRIMARY KEY CHECK(id = 1),
		total_queued INTEGER NOT NULL,
		updated_at_ts INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS claim_stats_runtime (
		id INTEGER PRIMARY KEY CHECK(id = 1),
		claimed_total INTEGER NOT NULL DEFAULT 0,
		claimed_unique INTEGER NOT NULL DEFAULT 0,
		updated_at_ts INTEGER NOT NULL
	)`,
	`CREATE TABLE IF NOT EXISTS user_claim_stats_runtime (
		user_id INTEGER PRIMARY KEY,
		claimed_total INTEGER NOT NULL DEFAULT 0,
		claimed_unique INTEGER NOT NULL DEFAULT 0,
		exclusive_claimed_unique INTEGER NOT NULL DEFAULT 0,
		updated_at_ts INTEGER NOT NULL,
		FOREIGN KEY(user_id) REFERENCES users(id)
	)`,
	`CREATE TABLE IF NOT EXISTS user_bans (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		linuxdo_user_id TEXT NOT NULL,
		username_snapshot TEXT,
		reason TEXT NOT NULL,
		banned_by_user_id INTEGER NOT NULL,
		banned_at_ts INTEGER NOT NULL,
		expires_at_ts INTEGER,
		unbanned_by_user_id INTEGER,
		unbanned_at_ts INTEGER,
		FOREIGN KEY(banned_by_user_id) REFERENCES users(id),
		FOREIGN KEY(unbanned_by_user_id) REFERENCES users(id)
	)`,
}

var indexStatements = []string{
	`CREATE INDEX IF NOT EXISTS idx_token_claims_user_time
		ON token_claims(user_id, claimed_at_ts)`,
	`CREATE INDEX IF NOT EXISTS idx_token_claims_api_time
		ON token_claims(api_key_id, claimed_at_ts)`,
	`CREATE INDEX IF NOT EXISTS idx_token_claims_user_hidden_time
		ON token_claims(user_id, is_hidden, claimed_at_ts DESC, id DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_token_claims_user_token_hidden_time
		ON token_claims(user_id, token_id, is_hidden, claimed_at_ts DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_token_claims_request_user_time
		ON token_claims(request_id, user_id, claimed_at_ts)`,
	`CREATE INDEX IF NOT EXISTS idx_token_claims_claimed_at
		ON token_claims(claimed_at_ts DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_token_claims_claimed_at_request_user
		ON token_claims(claimed_at_ts DESC, request_id, user_id)`,
	`CREATE INDEX IF NOT EXISTS idx_user_token_claims_user
		ON user_token_claims(user_id, token_id)`,
	`CREATE INDEX IF NOT EXISTS idx_user_token_claims_token
		ON user_token_claims(token_id, user_id)`,
	`CREATE INDEX IF NOT EXISTS idx_claim_queue_status_time
		ON claim_queue(status, enqueued_at_ts, id)`,
	`CREATE INDEX IF NOT EXISTS idx_claim_queue_user_status_remaining_time
		ON claim_queue(user_id, status, remaining, enqueued_at_ts, id)`,
	`CREATE INDEX IF NOT EXISTS idx_claim_queue_status_remaining_time
		ON claim_queue(status, remaining, enqueued_at_ts, id)`,
	`CREATE INDEX IF NOT EXISTS idx_claim_queue_status_rank
		ON claim_queue(status, queue_rank)`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_tokens_account_id_unique
		ON tokens(account_id)
		WHERE account_id IS NOT NULL AND account_id != ''`,
	`CREATE UNIQUE INDEX IF NOT EXISTS idx_tokens_access_token_hash_unique
		ON tokens(access_token_hash)
		WHERE access_token_hash IS NOT NULL AND access_token_hash != ''`,
	`CREATE INDEX IF NOT EXISTS idx_token_claims_token_user
		ON token_claims(token_id, user_id)`,
	`CREATE INDEX IF NOT EXISTS idx_tokens_claimable_candidates
		ON tokens(is_active, is_enabled, is_banned, is_available, probe_lock_until_ts, claim_count, max_claims, id)`,
	`CREATE INDEX IF NOT EXISTS idx_tokens_provider_identity_uploaded
		ON tokens(provider_user_id, provider_username, provider_name, uploaded_at_ts DESC)
		WHERE provider_user_id IS NOT NULL AND provider_user_id != ''`,
	`CREATE INDEX IF NOT EXISTS idx_api_keys_user_status
		ON api_keys(user_id, status)`,
	`CREATE INDEX IF NOT EXISTS idx_user_bans_target_time
		ON user_bans(linuxdo_user_id, banned_at_ts DESC)`,
	`CREATE INDEX IF NOT EXISTS idx_user_bans_active_lookup
		ON user_bans(linuxdo_user_id, unbanned_at_ts, expires_at_ts, id DESC)`,
}

type Store struct {
	path   string
	db     *sql.DB
	initMu sync.Mutex
}

func Open(path string) (*Store, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("database path is empty")
	}

	db, err := sql.Open("sqlite", sqliteDSN(path))
	if err != nil {
		return nil, fmt.Errorf("open sqlite database %q: %w", path, err)
	}

	maxOpen := sqliteMaxOpenConns()
	maxIdle := sqliteMaxIdleConns(maxOpen)
	db.SetMaxOpenConns(maxOpen)
	db.SetMaxIdleConns(maxIdle)
	db.SetConnMaxLifetime(0)

	store := &Store{
		path: path,
		db:   db,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := store.applyPragmas(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}

	return store, nil
}

func (s *Store) DB() *sql.DB {
	return s.db
}

func (s *Store) Close() error {
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) Init(ctx context.Context) error {
	s.initMu.Lock()
	defer s.initMu.Unlock()

	if err := s.applyPragmas(ctx); err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin init transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback()
	}()

	for _, statement := range schemaStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply schema: %w", err)
		}
	}

	if err := ensureClaimColumns(ctx, tx); err != nil {
		return err
	}
	if err := ensureAPIKeyColumns(ctx, tx); err != nil {
		return err
	}
	if err := ensureQueueColumns(ctx, tx); err != nil {
		return err
	}
	if err := ensureTokenColumns(ctx, tx); err != nil {
		return err
	}
	if err := ensureInventoryRuntimeColumns(ctx, tx); err != nil {
		return err
	}
	if err := ensureClaimStatsRuntimeColumns(ctx, tx); err != nil {
		return err
	}
	if err := ensureUserClaimStatsRuntimeColumns(ctx, tx); err != nil {
		return err
	}
	if err := refreshQueueRuntime(ctx, tx); err != nil {
		return err
	}
	if err := backfillClaimContentColumns(ctx, tx); err != nil {
		return err
	}
	if err := backfillClaimProviderColumns(ctx, tx); err != nil {
		return err
	}
	if err := backfillTokenIdentityColumns(ctx, tx); err != nil {
		return err
	}
	if err := rebuildUserTokenClaims(ctx, tx); err != nil {
		return err
	}
	if err := hideDuplicateClaims(ctx, tx); err != nil {
		return err
	}
	if err := refreshClaimStatsRuntime(ctx, tx); err != nil {
		return err
	}

	for _, statement := range indexStatements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("create indexes: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit init transaction: %w", err)
	}

	return nil
}

func (s *Store) applyPragmas(ctx context.Context) error {
	for _, statement := range []string{
		`PRAGMA foreign_keys = ON`,
		`PRAGMA journal_mode = WAL`,
		fmt.Sprintf(`PRAGMA busy_timeout = %d`, busyTimeoutMS),
	} {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("apply pragma %q: %w", statement, err)
		}
	}
	return nil
}

func sqliteDSN(path string) string {
	query := url.Values{}
	query.Add("_pragma", fmt.Sprintf("busy_timeout(%d)", busyTimeoutMS))
	query.Add("_pragma", "foreign_keys(ON)")
	query.Add("_pragma", "journal_mode(WAL)")
	return path + "?" + query.Encode()
}

func sqliteMaxOpenConns() int {
	workers := runtime.GOMAXPROCS(0)
	if workers < 4 {
		return 4
	}
	if workers > 8 {
		return 8
	}
	return workers
}

func sqliteMaxIdleConns(maxOpen int) int {
	if maxOpen <= 4 {
		return maxOpen
	}
	return 4
}

func ensureClaimColumns(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "token_claims")
	if err != nil {
		return err
	}

	definitions := []struct {
		name       string
		definition string
	}{
		{name: "is_hidden", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "claim_file_name", definition: "TEXT"},
		{name: "claim_file_path", definition: "TEXT"},
		{name: "claim_encoding", definition: "TEXT"},
		{name: "claim_content_json", definition: "TEXT"},
		{name: "provider_user_id", definition: "TEXT"},
		{name: "provider_username", definition: "TEXT"},
		{name: "provider_name", definition: "TEXT"},
	}

	for _, definition := range definitions {
		if _, ok := columns[definition.name]; ok {
			continue
		}
		statement := fmt.Sprintf(`ALTER TABLE token_claims ADD COLUMN %s %s`, definition.name, definition.definition)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add token_claims.%s: %w", definition.name, err)
		}
	}

	return nil
}

func ensureAPIKeyColumns(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "api_keys")
	if err != nil {
		return err
	}

	if _, ok := columns["key_value"]; ok {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `ALTER TABLE api_keys ADD COLUMN key_value TEXT`); err != nil {
		return fmt.Errorf("add api_keys.key_value: %w", err)
	}

	return nil
}

func ensureQueueColumns(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "claim_queue")
	if err != nil {
		return err
	}

	if _, ok := columns["queue_rank"]; !ok {
		if _, err := tx.ExecContext(ctx, `ALTER TABLE claim_queue ADD COLUMN queue_rank INTEGER NOT NULL DEFAULT 0`); err != nil {
			return fmt.Errorf("add claim_queue.queue_rank: %w", err)
		}
		rows, err := tx.QueryContext(ctx, `
			SELECT id
			FROM claim_queue
			WHERE status = 'queued' AND remaining > 0
			ORDER BY enqueued_at_ts ASC, id ASC
		`)
		if err != nil {
			return fmt.Errorf("select queued rows: %w", err)
		}
		defer rows.Close()

		var ids []int64
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				return fmt.Errorf("scan queued row id: %w", err)
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			return fmt.Errorf("iterate queued rows: %w", err)
		}

		for index, id := range ids {
			if _, err := tx.ExecContext(ctx, `UPDATE claim_queue SET queue_rank = ? WHERE id = ?`, index+1, id); err != nil {
				return fmt.Errorf("backfill claim_queue.queue_rank for id %d: %w", id, err)
			}
		}
		columns["queue_rank"] = struct{}{}
	}

	definitions := []struct {
		name       string
		definition string
	}{
		{name: "cancel_reason", definition: "TEXT"},
		{name: "cancelled_at_ts", definition: "INTEGER"},
		{name: "cancelled_by_user_id", definition: "INTEGER"},
		{name: "last_error_reason", definition: "TEXT"},
		{name: "last_error_at_ts", definition: "INTEGER"},
		{name: "failure_count", definition: "INTEGER NOT NULL DEFAULT 0"},
	}
	for _, definition := range definitions {
		if _, ok := columns[definition.name]; ok {
			continue
		}
		statement := fmt.Sprintf(`ALTER TABLE claim_queue ADD COLUMN %s %s`, definition.name, definition.definition)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add claim_queue.%s: %w", definition.name, err)
		}
	}

	return nil
}

func ensureTokenColumns(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "tokens")
	if err != nil {
		return err
	}

	definitions := []struct {
		name       string
		definition string
	}{
		{name: "is_active", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "is_cleaned", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "is_enabled", definition: "INTEGER NOT NULL DEFAULT 1"},
		{name: "is_banned", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "banned_at_ts", definition: "INTEGER"},
		{name: "ban_reason", definition: "TEXT"},
		{name: "cleaned_at_ts", definition: "INTEGER"},
		{name: "last_probe_at_ts", definition: "INTEGER"},
		{name: "last_probe_status", definition: "TEXT"},
		{name: "probe_lock_until_ts", definition: "INTEGER"},
		{name: "account_id", definition: "TEXT"},
		{name: "access_token_hash", definition: "TEXT"},
		{name: "provider_user_id", definition: "TEXT"},
		{name: "provider_username", definition: "TEXT"},
		{name: "provider_name", definition: "TEXT"},
		{name: "uploaded_at_ts", definition: "INTEGER"},
	}

	for _, definition := range definitions {
		if _, ok := columns[definition.name]; ok {
			continue
		}
		statement := fmt.Sprintf(`ALTER TABLE tokens ADD COLUMN %s %s`, definition.name, definition.definition)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add tokens.%s: %w", definition.name, err)
		}
	}

	return nil
}

func ensureInventoryRuntimeColumns(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "inventory_runtime")
	if err != nil {
		return err
	}

	definitions := []struct {
		name       string
		definition string
	}{
		{name: "total_tokens", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "available_tokens", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "unclaimed_tokens", definition: "INTEGER NOT NULL DEFAULT 0"},
	}

	for _, definition := range definitions {
		if _, ok := columns[definition.name]; ok {
			continue
		}
		statement := fmt.Sprintf(`ALTER TABLE inventory_runtime ADD COLUMN %s %s`, definition.name, definition.definition)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add inventory_runtime.%s: %w", definition.name, err)
		}
	}

	return nil
}

func ensureClaimStatsRuntimeColumns(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "claim_stats_runtime")
	if err != nil {
		return err
	}

	definitions := []struct {
		name       string
		definition string
	}{
		{name: "claimed_total", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "claimed_unique", definition: "INTEGER NOT NULL DEFAULT 0"},
	}

	for _, definition := range definitions {
		if _, ok := columns[definition.name]; ok {
			continue
		}
		statement := fmt.Sprintf(`ALTER TABLE claim_stats_runtime ADD COLUMN %s %s`, definition.name, definition.definition)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add claim_stats_runtime.%s: %w", definition.name, err)
		}
	}

	return nil
}

func ensureUserClaimStatsRuntimeColumns(ctx context.Context, tx *sql.Tx) error {
	columns, err := tableColumns(ctx, tx, "user_claim_stats_runtime")
	if err != nil {
		return err
	}

	definitions := []struct {
		name       string
		definition string
	}{
		{name: "claimed_total", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "claimed_unique", definition: "INTEGER NOT NULL DEFAULT 0"},
		{name: "exclusive_claimed_unique", definition: "INTEGER NOT NULL DEFAULT 0"},
	}

	for _, definition := range definitions {
		if _, ok := columns[definition.name]; ok {
			continue
		}
		statement := fmt.Sprintf(`ALTER TABLE user_claim_stats_runtime ADD COLUMN %s %s`, definition.name, definition.definition)
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("add user_claim_stats_runtime.%s: %w", definition.name, err)
		}
	}

	return nil
}

func refreshQueueRuntime(ctx context.Context, tx *sql.Tx) error {
	var totalQueued int64
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM claim_queue
		WHERE status = 'queued' AND remaining > 0
	`).Scan(&totalQueued); err != nil {
		return fmt.Errorf("count queued rows: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO queue_runtime (id, total_queued, updated_at_ts)
		VALUES (1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			total_queued = excluded.total_queued,
			updated_at_ts = excluded.updated_at_ts
	`, totalQueued, nowUnix()); err != nil {
		return fmt.Errorf("refresh queue_runtime: %w", err)
	}

	return nil
}

func backfillClaimContentColumns(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE token_claims
		SET claim_file_name = COALESCE(
				claim_file_name,
				(SELECT tokens.file_name FROM tokens WHERE tokens.id = token_claims.token_id)
			),
			claim_file_path = COALESCE(
				claim_file_path,
				(SELECT tokens.file_path FROM tokens WHERE tokens.id = token_claims.token_id)
			),
			claim_encoding = COALESCE(
				claim_encoding,
				(SELECT tokens.encoding FROM tokens WHERE tokens.id = token_claims.token_id)
			),
			claim_content_json = COALESCE(
				claim_content_json,
				(SELECT tokens.content_json FROM tokens WHERE tokens.id = token_claims.token_id)
			)
		WHERE claim_file_name IS NULL
		   OR claim_file_path IS NULL
		   OR claim_encoding IS NULL
		   OR claim_content_json IS NULL
	`); err != nil {
		return fmt.Errorf("backfill token_claims content columns: %w", err)
	}

	return nil
}

func backfillClaimProviderColumns(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE token_claims
		SET provider_user_id = COALESCE(
				provider_user_id,
				(SELECT tokens.provider_user_id FROM tokens WHERE tokens.id = token_claims.token_id)
			),
			provider_username = COALESCE(
				provider_username,
				(SELECT tokens.provider_username FROM tokens WHERE tokens.id = token_claims.token_id)
			),
			provider_name = COALESCE(
				provider_name,
				(SELECT tokens.provider_name FROM tokens WHERE tokens.id = token_claims.token_id)
			)
		WHERE provider_user_id IS NULL
		   OR provider_username IS NULL
		   OR provider_name IS NULL
	`); err != nil {
		return fmt.Errorf("backfill token_claims provider columns: %w", err)
	}

	return nil
}

func backfillTokenIdentityColumns(ctx context.Context, tx *sql.Tx) error {
	rows, err := tx.QueryContext(ctx, `
		SELECT id, content_json
		FROM tokens
		WHERE (account_id IS NULL OR account_id = '')
		   OR (access_token_hash IS NULL OR access_token_hash = '')
	`)
	if err != nil {
		return fmt.Errorf("select tokens for backfill: %w", err)
	}
	defer rows.Close()

	type pendingToken struct {
		id          int64
		contentJSON string
	}

	var pending []pendingToken
	for rows.Next() {
		var item pendingToken
		if err := rows.Scan(&item.id, &item.contentJSON); err != nil {
			return fmt.Errorf("scan token backfill row: %w", err)
		}
		pending = append(pending, item)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate token backfill rows: %w", err)
	}

	for _, item := range pending {
		accountID, accessToken, err := extractUploadIdentity(item.contentJSON)
		if err != nil {
			continue
		}

		if _, err := tx.ExecContext(ctx, `
			UPDATE tokens
			SET account_id = COALESCE(NULLIF(account_id, ''), ?),
				access_token_hash = COALESCE(NULLIF(access_token_hash, ''), ?)
			WHERE id = ?
		`, accountID, hashTokenValue(accessToken), item.id); err != nil {
			return fmt.Errorf("backfill token identity for id %d: %w", item.id, err)
		}
	}

	return nil
}

func rebuildUserTokenClaims(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		INSERT OR IGNORE INTO user_token_claims (user_id, token_id, first_claim_id, created_at_ts)
		SELECT claims.user_id,
		       claims.token_id,
		       claims.id,
		       claims.claimed_at_ts
		FROM token_claims AS claims
		JOIN (
			SELECT user_id, token_id, MIN(id) AS first_claim_id
			FROM token_claims
			GROUP BY user_id, token_id
		) AS firsts
		  ON firsts.user_id = claims.user_id
		 AND firsts.token_id = claims.token_id
		 AND firsts.first_claim_id = claims.id
	`); err != nil {
		return fmt.Errorf("rebuild user_token_claims: %w", err)
	}

	return nil
}

func hideDuplicateClaims(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		UPDATE token_claims
		SET is_hidden = 1
		WHERE EXISTS (
			SELECT 1
			FROM user_token_claims
			WHERE user_token_claims.user_id = token_claims.user_id
			  AND user_token_claims.token_id = token_claims.token_id
			  AND user_token_claims.first_claim_id <> token_claims.id
		)
	`); err != nil {
		return fmt.Errorf("hide duplicate claims: %w", err)
	}

	return nil
}

func refreshClaimStatsRuntime(ctx context.Context, tx *sql.Tx) error {
	var claimedTotal int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM token_claims`).Scan(&claimedTotal); err != nil {
		return fmt.Errorf("count total claim stats runtime: %w", err)
	}

	var claimedUnique int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT token_id) FROM user_token_claims`).Scan(&claimedUnique); err != nil {
		return fmt.Errorf("count unique claim stats runtime: %w", err)
	}

	now := nowUnix()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO claim_stats_runtime (id, claimed_total, claimed_unique, updated_at_ts)
		VALUES (1, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			claimed_total = excluded.claimed_total,
			claimed_unique = excluded.claimed_unique,
			updated_at_ts = excluded.updated_at_ts
	`, claimedTotal, claimedUnique, now); err != nil {
		return fmt.Errorf("upsert claim_stats_runtime: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM user_claim_stats_runtime`); err != nil {
		return fmt.Errorf("clear user_claim_stats_runtime: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `
		INSERT INTO user_claim_stats_runtime (
			user_id,
			claimed_total,
			claimed_unique,
			exclusive_claimed_unique,
			updated_at_ts
		)
		WITH claim_totals AS (
			SELECT user_id, COUNT(*) AS claimed_total
			FROM token_claims
			GROUP BY user_id
		),
		unique_totals AS (
			SELECT user_id, COUNT(*) AS claimed_unique
			FROM user_token_claims
			GROUP BY user_id
		),
		exclusive_totals AS (
			SELECT utc.user_id, COUNT(*) AS exclusive_claimed_unique
			FROM user_token_claims AS utc
			JOIN (
				SELECT token_id
				FROM user_token_claims
				GROUP BY token_id
				HAVING COUNT(*) = 1
			) AS exclusive_tokens
			  ON exclusive_tokens.token_id = utc.token_id
			GROUP BY utc.user_id
		)
		SELECT users.id,
		       COALESCE(claim_totals.claimed_total, 0),
		       COALESCE(unique_totals.claimed_unique, 0),
		       COALESCE(exclusive_totals.exclusive_claimed_unique, 0),
		       ?
		FROM users
		LEFT JOIN claim_totals
		  ON claim_totals.user_id = users.id
		LEFT JOIN unique_totals
		  ON unique_totals.user_id = users.id
		LEFT JOIN exclusive_totals
		  ON exclusive_totals.user_id = users.id
		WHERE COALESCE(claim_totals.claimed_total, 0) > 0
		   OR COALESCE(unique_totals.claimed_unique, 0) > 0
		   OR COALESCE(exclusive_totals.exclusive_claimed_unique, 0) > 0
	`, now); err != nil {
		return fmt.Errorf("rebuild user_claim_stats_runtime: %w", err)
	}

	return nil
}

func tableColumns(ctx context.Context, tx *sql.Tx, tableName string) (map[string]struct{}, error) {
	rows, err := tx.QueryContext(ctx, fmt.Sprintf(`PRAGMA table_info(%s)`, tableName))
	if err != nil {
		return nil, fmt.Errorf("inspect columns for %s: %w", tableName, err)
	}
	defer rows.Close()

	columns := map[string]struct{}{}
	for rows.Next() {
		var (
			cid        int
			name       string
			columnType string
			notNull    int
			defaultVal sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &columnType, &notNull, &defaultVal, &primaryKey); err != nil {
			return nil, fmt.Errorf("scan columns for %s: %w", tableName, err)
		}
		columns[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate columns for %s: %w", tableName, err)
	}

	return columns, nil
}

func extractUploadIdentity(contentJSON string) (string, string, error) {
	var payload any
	if err := json.Unmarshal([]byte(contentJSON), &payload); err != nil {
		return "", "", err
	}

	root, ok := payload.(map[string]any)
	if !ok {
		return "", "", fmt.Errorf("upload payload is not an object")
	}

	candidate := root
	if storage, ok := root["storage"].(map[string]any); ok {
		candidate = storage
	}

	accountID := normalizeUploadedValue(candidate["account_id"])
	accessToken := normalizeUploadedValue(candidate["access_token"])
	refreshToken := normalizeUploadedValue(candidate["refresh_token"])
	if accountID == "" || accessToken == "" || refreshToken == "" {
		return "", "", fmt.Errorf("upload payload is missing required fields")
	}

	return accountID, accessToken, nil
}

func normalizeUploadedValue(value any) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(fmt.Sprint(value))
}

func hashTokenValue(value string) string {
	sum := sha256.Sum256([]byte(value))
	return hex.EncodeToString(sum[:])
}

func nowUnix() int64 {
	return time.Now().Unix()
}
