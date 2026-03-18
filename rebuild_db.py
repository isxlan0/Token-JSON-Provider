from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Any


TABLE_ORDER = [
    "users",
    "api_keys",
    "tokens",
    "token_claims",
    "claim_queue",
    "inventory_runtime",
    "user_bans",
]


CREATE_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        linuxdo_user_id TEXT NOT NULL UNIQUE,
        linuxdo_username TEXT NOT NULL,
        linuxdo_name TEXT,
        avatar_url TEXT,
        trust_level INTEGER NOT NULL,
        created_at_ts INTEGER NOT NULL,
        last_login_at_ts INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS api_keys (
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
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_name TEXT NOT NULL UNIQUE,
        file_path TEXT NOT NULL,
        file_hash TEXT NOT NULL,
        encoding TEXT NOT NULL,
        content_json TEXT NOT NULL,
        is_active INTEGER NOT NULL,
        is_cleaned INTEGER NOT NULL DEFAULT 0,
        is_enabled INTEGER NOT NULL DEFAULT 1,
        is_available INTEGER NOT NULL,
        claim_count INTEGER NOT NULL,
        max_claims INTEGER NOT NULL,
        created_at_ts INTEGER NOT NULL,
        cleaned_at_ts INTEGER,
        updated_at_ts INTEGER NOT NULL,
        last_seen_at_ts INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS token_claims (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        token_id INTEGER NOT NULL,
        user_id INTEGER NOT NULL,
        api_key_id INTEGER,
        claimed_at_ts INTEGER NOT NULL,
        is_hidden INTEGER NOT NULL DEFAULT 0,
        request_id TEXT NOT NULL,
        FOREIGN KEY(token_id) REFERENCES tokens(id),
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(api_key_id) REFERENCES api_keys(id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS claim_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        api_key_id INTEGER,
        requested INTEGER NOT NULL,
        remaining INTEGER NOT NULL,
        enqueued_at_ts INTEGER NOT NULL,
        request_id TEXT NOT NULL,
        status TEXT NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS inventory_runtime (
        id INTEGER PRIMARY KEY CHECK(id = 1),
        status TEXT NOT NULL,
        max_claims INTEGER NOT NULL,
        updated_at_ts INTEGER NOT NULL
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS user_bans (
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
    );
    """,
]


INDEX_STATEMENTS = [
    """
    CREATE INDEX IF NOT EXISTS idx_token_claims_user_time
        ON token_claims(user_id, claimed_at_ts);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_token_claims_api_time
        ON token_claims(api_key_id, claimed_at_ts);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_claim_queue_status_time
        ON claim_queue(status, enqueued_at_ts, id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_user_bans_target_time
        ON user_bans(linuxdo_user_id, banned_at_ts DESC);
    """,
]


def connect_source(path: Path) -> sqlite3.Connection:
    uri = f"file:{path.as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def connect_dest(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def create_schema(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA foreign_keys = OFF")
    for sql in CREATE_STATEMENTS:
        conn.execute(sql)
    for sql in INDEX_STATEMENTS:
        conn.execute(sql)
    conn.commit()


def source_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row["name"]) for row in rows]


def dest_columns(conn: sqlite3.Connection, table: str) -> list[str]:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return [str(row["name"]) for row in rows]


def default_value_for_missing(table: str, column: str) -> Any:
    if table == "tokens":
        if column == "is_cleaned":
            return 0
        if column == "is_enabled":
            return 1
        if column == "cleaned_at_ts":
            return None
        if column == "is_active":
            return 1
    if table == "token_claims" and column == "is_hidden":
        return 0
    if table == "api_keys" and column == "key_value":
        return None
    return None


def copy_table(src: sqlite3.Connection, dst: sqlite3.Connection, table: str) -> dict[str, int]:
    src_cols = source_columns(src, table)
    dst_cols = dest_columns(dst, table)
    select_cols = ", ".join(src_cols)
    insert_cols = ", ".join(dst_cols)
    placeholders = ", ".join("?" for _ in dst_cols)
    insert_sql = f"INSERT OR IGNORE INTO {table} ({insert_cols}) VALUES ({placeholders})"
    copied = 0
    skipped = 0
    failed = 0
    last_rowid = 0
    single_row_fallback = False

    try:
        max_rowid_row = src.execute(f"SELECT COALESCE(MAX(rowid), 0) FROM {table}").fetchone()
        max_rowid = int(max_rowid_row[0]) if max_rowid_row and max_rowid_row[0] is not None else 0
    except sqlite3.DatabaseError:
        max_rowid = 0

    while True:
        if single_row_fallback:
            if last_rowid >= max_rowid:
                break
            next_rowid = last_rowid + 1
            try:
                rows = src.execute(
                    f"SELECT rowid AS __rowid__, {select_cols} FROM {table} WHERE rowid = ?",
                    (next_rowid,),
                ).fetchall()
                last_rowid = next_rowid
            except sqlite3.DatabaseError as exc:
                failed += 1
                print(f"[{table}] skip unreadable rowid {next_rowid}: {exc}")
                last_rowid = next_rowid
                continue
        else:
            try:
                rows = src.execute(
                    f"SELECT rowid AS __rowid__, {select_cols} FROM {table} WHERE rowid > ? ORDER BY rowid LIMIT 500",
                    (last_rowid,),
                ).fetchall()
            except sqlite3.DatabaseError as exc:
                print(f"[{table}] batch read failed after rowid {last_rowid}: {exc}")
                print(f"[{table}] switching to single-row recovery mode...")
                single_row_fallback = True
                continue

        if not rows:
            break

        for row in rows:
            try:
                last_rowid = max(last_rowid, int(row["__rowid__"]))
                values: list[Any] = []
                for col in dst_cols:
                    if col in row.keys():
                        values.append(row[col])
                    else:
                        values.append(default_value_for_missing(table, col))
                cursor = dst.execute(insert_sql, values)
                if int(cursor.rowcount or 0) > 0:
                    copied += 1
                else:
                    skipped += 1
            except sqlite3.DatabaseError as exc:
                failed += 1
                print(f"[{table}] skip rowid {last_rowid}: {exc}")
        dst.commit()

    return {"copied": copied, "skipped": skipped, "failed": failed}


def align_sequences(dst: sqlite3.Connection) -> None:
    dst.execute(
        "INSERT OR IGNORE INTO sqlite_sequence(name, seq) VALUES ('users', 0), ('api_keys', 0), ('tokens', 0), ('token_claims', 0), ('claim_queue', 0), ('user_bans', 0)"
    )
    for table in ("users", "api_keys", "tokens", "token_claims", "claim_queue", "user_bans"):
        row = dst.execute(f"SELECT COALESCE(MAX(id), 0) FROM {table}").fetchone()
        dst.execute("UPDATE sqlite_sequence SET seq = ? WHERE name = ?", (int(row[0]), table))
    dst.commit()


def main() -> int:
    parser = argparse.ArgumentParser(description="Rebuild a SQLite database into a clean file.")
    parser.add_argument("source", nargs="?", default="token_atlas.db", help="Source SQLite database path")
    parser.add_argument("dest", nargs="?", help="Destination SQLite database path")
    args = parser.parse_args()

    source_path = Path(args.source).resolve()
    if not source_path.exists():
        raise SystemExit(f"Source database not found: {source_path}")

    if args.dest:
        dest_path = Path(args.dest).resolve()
    else:
        default_dest = source_path.with_name("token_atlas.db")
        fallback_dest = source_path.with_name("token_atlas.rebuilt.db")
        dest_path = fallback_dest if default_dest.exists() else default_dest
    if dest_path.exists():
        raise SystemExit(f"Destination already exists: {dest_path}")

    src = connect_source(source_path)
    dst = connect_dest(dest_path)
    try:
        create_schema(dst)
        summary: dict[str, dict[str, int]] = {}
        for table in TABLE_ORDER:
            print(f"Rebuilding {table}...")
            summary[table] = copy_table(src, dst, table)
        align_sequences(dst)
        print("\nSummary:")
        for table in TABLE_ORDER:
            stats = summary[table]
            print(
                f"- {table}: copied={stats['copied']} skipped={stats['skipped']} failed={stats['failed']}"
            )
        print(f"\nCreated: {dest_path}")
    finally:
        src.close()
        dst.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
