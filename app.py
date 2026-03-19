from __future__ import annotations

import base64
from contextlib import asynccontextmanager
import hashlib
import json
import io
import os
import secrets
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Body, Depends, FastAPI, Header, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware
import claim_queue

BASE_DIR = Path(__file__).resolve().parent
TOKEN_DIR = BASE_DIR / "token"
STATIC_DIR = BASE_DIR / "static"
ENV_FILE = BASE_DIR / ".env"

API_KEY_HEADER = "X-API-Key"
LEGACY_ACCESS_KEY_HEADER = "X-Access-Key"
SESSION_SECRET_ENV = "TOKEN_INDEX_SESSION_SECRET"
LINUXDO_CLIENT_ID_ENV = "TOKEN_INDEX_LINUXDO_CLIENT_ID"
LINUXDO_CLIENT_SECRET_ENV = "TOKEN_INDEX_LINUXDO_CLIENT_SECRET"
LINUXDO_REDIRECT_URI_ENV = "TOKEN_INDEX_LINUXDO_REDIRECT_URI"
LINUXDO_SCOPE_ENV = "TOKEN_INDEX_LINUXDO_SCOPE"
LINUXDO_MIN_TRUST_LEVEL_ENV = "TOKEN_INDEX_LINUXDO_MIN_TRUST_LEVEL"
LINUXDO_ALLOWED_IDS_ENV = "TOKEN_INDEX_LINUXDO_ALLOWED_IDS"
DB_PATH_ENV = "TOKEN_DB_PATH"
ADMIN_IDENTITIES_ENV = "TOKEN_INDEX_ADMIN_IDENTITIES"
TOKEN_HEALTHY_THRESHOLD_ENV = "TOKEN_HEALTHY_THRESHOLD"
TOKEN_WARNING_THRESHOLD_ENV = "TOKEN_WARNING_THRESHOLD"
TOKEN_CRITICAL_THRESHOLD_ENV = "TOKEN_CRITICAL_THRESHOLD"
TOKEN_HOURLY_LIMIT_HEALTHY_ENV = "TOKEN_HOURLY_LIMIT_HEALTHY"
TOKEN_HOURLY_LIMIT_WARNING_ENV = "TOKEN_HOURLY_LIMIT_WARNING"
TOKEN_HOURLY_LIMIT_CRITICAL_ENV = "TOKEN_HOURLY_LIMIT_CRITICAL"
TOKEN_MAX_CLAIMS_HEALTHY_ENV = "TOKEN_MAX_CLAIMS_HEALTHY"
TOKEN_MAX_CLAIMS_WARNING_ENV = "TOKEN_MAX_CLAIMS_WARNING"
TOKEN_MAX_CLAIMS_CRITICAL_ENV = "TOKEN_MAX_CLAIMS_CRITICAL"
TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE_ENV = "TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE"
APIKEY_MAX_PER_USER_ENV = "TOKEN_APIKEY_MAX_PER_USER"
APIKEY_RATE_PER_MIN_ENV = "TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE"
PROVIDER_BASE_URL_ENV = "TOKEN_PROVIDER_BASE_URL"
SESSION_COOKIE_NAME = "token_atlas_session"
SESSION_AUTH_KEY = "auth"
SESSION_OAUTH_STATE_KEY = "linuxdo_oauth_state"
SESSION_POST_LOGIN_REDIRECT_KEY = "post_login_redirect"
LINUXDO_AUTHORIZE_URL = "https://connect.linux.do/oauth2/authorize"
LINUXDO_TOKEN_URL = "https://connect.linux.do/oauth2/token"
LINUXDO_USER_URL = "https://connect.linux.do/api/user"


def isoformat_timestamp(timestamp: float) -> str:
    return (
        datetime.fromtimestamp(timestamp, tz=timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )


def isoformat_now() -> str:
    return datetime.now(tz=timezone.utc).astimezone().isoformat(timespec="seconds")


def now_ts() -> int:
    return int(time.time())


def isoformat_from_ts(value: int) -> str:
    return datetime.fromtimestamp(value, tz=timezone.utc).astimezone().isoformat(timespec="seconds")


def env_value(name: str, default: str = "") -> str:
    raw = os.getenv(name, "")
    if not raw:
        return default
    cleaned = raw.split("#", 1)[0].strip()
    return cleaned or default


def env_int(name: str, default: int) -> int:
    raw = env_value(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def get_non_healthy_max_claims_scope() -> str:
    raw = env_value(TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE_ENV, "all_unfinished").strip().lower()
    if raw in {"all_unfinished", "new_only", "unclaimed_only"}:
        return raw
    return "all_unfinished"


def normalize_username(value: str) -> str:
    return value.strip().lstrip("@").strip().lower()


def parse_admin_identities(raw: str) -> dict[str, set[str]]:
    ids: set[str] = set()
    usernames: set[str] = set()
    for part in raw.split(","):
        candidate = part.strip()
        if not candidate:
            continue
        if candidate.startswith("@") or not candidate.isdigit():
            normalized = normalize_username(candidate)
            if normalized:
                usernames.add(normalized)
            continue
        ids.add(candidate)
    return {"ids": ids, "usernames": usernames}


def detect_encoding(raw: bytes) -> str:
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if raw.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if raw.startswith(b"\xfe\xff"):
        return "utf-16-be"

    candidates = (
        "utf-8",
        "utf-16",
        "utf-16-le",
        "utf-16-be",
        "gbk",
        "big5",
        "latin-1",
    )

    for encoding in candidates:
        try:
            decoded = raw.decode(encoding)
            json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        return encoding

    for encoding in candidates:
        try:
            raw.decode(encoding)
        except UnicodeDecodeError:
            continue
        return encoding

    raise UnicodeDecodeError("unknown", raw, 0, len(raw), "Unable to determine encoding")


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing required config file: {path}")

    raw = path.read_bytes()
    encoding = detect_encoding(raw)
    text = raw.decode(encoding)

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue

        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip()

        if value[:1] == value[-1:] and value[:1] in {'"', "'"}:
            value = value[1:-1]

        if key:
            os.environ.setdefault(key, value)


try:
    load_dotenv_file(ENV_FILE)
except FileNotFoundError as exc:
    raise SystemExit(f"{exc}\nRefusing to start without the required config file.") from None


@dataclass
class TokenDocument:
    index: int
    id: str
    name: str
    path: str
    size: int
    mtime: str
    encoding: str
    keys: list[str]
    error: str | None
    content: Any | None

    def to_index_payload(self) -> dict[str, Any]:
        return {
            "index": self.index,
            "id": self.id,
            "name": self.name,
            "path": self.path,
            "size": self.size,
            "mtime": self.mtime,
            "encoding": self.encoding,
            "keys": self.keys,
            "error": self.error,
        }

    def to_detail_payload(self) -> dict[str, Any]:
        return {
            "item": self.to_index_payload(),
            "content": self.content,
        }


class ArchivePayload(BaseModel):
    names: list[str] | None = None


class ApiKeyCreatePayload(BaseModel):
    name: str | None = None


class ClaimPayload(BaseModel):
    count: int = Field(default=1, ge=1)


class ClaimHidePayload(BaseModel):
    claim_ids: list[int] = Field(default_factory=list)


class AdminBanPayload(BaseModel):
    reason: str = Field(min_length=1, max_length=500)
    expires_at: str | None = None


class LinuxDOUser(BaseModel):
    id: int
    username: str
    name: str | None = None
    active: bool = True
    trust_level: int = 0
    silenced: bool = False
    avatar_template: str | None = None


class RateLimitError(Exception):
    pass


class AccessDeniedError(Exception):
    pass


class BannedUserError(Exception):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(payload.get("detail") or "User is banned")
        self.payload = payload


class DashboardMemoryCache:
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._inventory = {"total": 0, "available": 0, "unclaimed": 0}
        self._queue = {"total": 0}
        self._policy = {
            "status": "healthy",
            "unclaimed": 0,
            "thresholds": get_inventory_thresholds(),
            "hourly_limit": get_inventory_limits()["healthy"]["hourly"],
            "max_claims": get_inventory_limits()["healthy"]["max_claims"],
        }
        self._index_updated_at = isoformat_now()
        self._claim_events: list[dict[str, Any]] = []
        self._total_claims = 0
        self._token_claimers: dict[int, set[int]] = {}
        self._user_total_claims: dict[int, int] = {}
        self._user_unique_tokens: dict[int, set[int]] = {}
        self._leaderboard_cache: dict[str, dict[str, Any]] = {}
        self._recent_cache: dict[str, dict[str, Any]] = {}
        self._trends_cache: dict[str, dict[str, Any]] = {}
        self._stats_cache: dict[int, dict[str, int]] = {}

    def refresh_from_db(self, db_handle: "TokenDb", index_updated_at: str | None = None) -> None:
        cutoff = now_ts() - _CLAIM_EVENT_WINDOW_SEC
        with db_handle.connect() as conn:
            inventory = db_handle.get_inventory_snapshot(conn=conn)
            queue_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM claim_queue WHERE status = 'queued' AND remaining > 0"
            ).fetchone()
            events_rows = conn.execute(
                """
                SELECT MIN(token_claims.id) as first_claim_id,
                       token_claims.request_id as request_id,
                       token_claims.user_id as user_id,
                       token_claims.claimed_at_ts as claimed_at_ts,
                       users.linuxdo_user_id as public_user_id,
                       users.linuxdo_username as username,
                       users.linuxdo_name as name,
                       COUNT(*) as cnt
                FROM token_claims
                JOIN users ON users.id = token_claims.user_id
                WHERE token_claims.claimed_at_ts >= ?
                GROUP BY token_claims.request_id,
                         token_claims.user_id,
                         token_claims.claimed_at_ts,
                         users.linuxdo_user_id,
                         users.linuxdo_username,
                         users.linuxdo_name
                ORDER BY token_claims.claimed_at_ts ASC, first_claim_id ASC
                """,
                (cutoff,),
            ).fetchall()
            total_row = conn.execute("SELECT COUNT(*) as cnt FROM token_claims").fetchone()
            user_total_rows = conn.execute(
                """
                SELECT user_id, COUNT(*) as cnt
                FROM token_claims
                GROUP BY user_id
                """
            ).fetchall()
            unique_rows = conn.execute(
                """
                SELECT user_id, token_id
                FROM token_claims
                GROUP BY user_id, token_id
                """
            ).fetchall()

        policy = build_inventory_policy_from_snapshot(inventory)
        claim_events = [
            {
                "request_id": row["request_id"],
                "user_id": int(row["user_id"]),
                "public_user_id": row["public_user_id"],
                "username": row["username"],
                "name": row["name"] or row["username"],
                "claimed_at_ts": int(row["claimed_at_ts"]),
                "count": int(row["cnt"]),
            }
            for row in events_rows
        ]
        user_total_claims = {int(row["user_id"]): int(row["cnt"]) for row in user_total_rows}
        user_unique_tokens: dict[int, set[int]] = {}
        token_claimers: dict[int, set[int]] = {}
        for row in unique_rows:
            user_id = int(row["user_id"])
            token_id = int(row["token_id"])
            user_unique_tokens.setdefault(user_id, set()).add(token_id)
            token_claimers.setdefault(token_id, set()).add(user_id)

        with self._lock:
            self._inventory = inventory
            self._queue = {"total": int(queue_row["cnt"]) if queue_row else 0}
            self._policy = policy
            self._index_updated_at = index_updated_at or isoformat_now()
            self._claim_events = claim_events
            self._total_claims = int(total_row["cnt"]) if total_row else 0
            self._user_total_claims = user_total_claims
            self._user_unique_tokens = user_unique_tokens
            self._token_claimers = token_claimers
            self._invalidate_derived_locked()

        _POLICY_CACHE["value"] = policy
        _POLICY_CACHE["ts"] = time.time()
        _POLICY_STATE["status"] = policy["status"]
        _POLICY_STATE["max_claims"] = policy["max_claims"]

    def get_system_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "inventory": dict(self._inventory),
                "queue": dict(self._queue),
                "health": {
                    "status": self._policy["status"],
                    "hourly_limit": self._policy["hourly_limit"],
                    "max_claims": self._policy["max_claims"],
                    "thresholds": dict(self._policy["thresholds"]),
                },
                "index": {"updated_at": self._index_updated_at},
            }

    def get_stats(self, user_id: int) -> dict[str, int]:
        with self._lock:
            cached = self._stats_cache.get(user_id)
            if cached is not None:
                return dict(cached)

            user_total = self._user_total_claims.get(user_id, 0)
            user_unique = len(self._user_unique_tokens.get(user_id, set()))
            others_unique = sum(1 for claimers in self._token_claimers.values() if any(uid != user_id for uid in claimers))
            stats = {
                "total_tokens": int(self._inventory["total"]),
                "available_tokens": int(self._inventory["available"]),
                "claimed_total": int(self._total_claims),
                "claimed_unique": len(self._token_claimers),
                "others_claimed_total": max(0, int(self._total_claims) - user_total),
                "others_claimed_unique": others_unique,
                "user_claimed_total": user_total,
                "user_claimed_unique": user_unique,
            }
            self._stats_cache[user_id] = stats
            return dict(stats)

    def get_leaderboard(self, window_sec: int, limit: int) -> dict[str, Any]:
        cache_key = f"{window_sec}:{limit}"
        cutoff = now_ts() - max(0, int(window_sec))
        limit = max(1, int(limit))
        with self._lock:
            cached = self._leaderboard_cache.get(cache_key)
            if cached is not None:
                return cached

            aggregates: dict[int, dict[str, Any]] = {}
            for event in self._claim_events:
                if event["claimed_at_ts"] < cutoff:
                    continue
                user_id = int(event["user_id"])
                current = aggregates.setdefault(
                    user_id,
                    {
                        "user_id": event["public_user_id"],
                        "username": event["username"],
                        "name": event["name"],
                        "count": 0,
                    },
                )
                current["count"] += int(event["count"])

            items = sorted(
                aggregates.values(),
                key=lambda item: (-int(item["count"]), str(item["username"]), str(item["user_id"])),
            )[:limit]
            payload = {"window": window_sec, "items": items}
            self._leaderboard_cache[cache_key] = payload
            return payload

    def get_recent(self, limit: int) -> dict[str, Any]:
        cache_key = str(limit)
        limit = max(1, int(limit))
        with self._lock:
            cached = self._recent_cache.get(cache_key)
            if cached is not None:
                return cached

            items = [
                {
                    "name": event["name"],
                    "username": event["username"],
                    "count": int(event["count"]),
                    "claimed_at": isoformat_from_ts(int(event["claimed_at_ts"])),
                }
                for event in reversed(self._claim_events[-limit:])
            ]
            payload = {"items": items}
            self._recent_cache[cache_key] = payload
            return payload

    def get_trends(self, window_sec: int, bucket_sec: int) -> dict[str, Any]:
        cache_key = f"{window_sec}:{bucket_sec}"
        window_sec = max(1, int(window_sec))
        bucket_sec = max(60, int(bucket_sec))
        now = now_ts()
        start_ts = now - window_sec
        start_bucket = (start_ts // bucket_sec) * bucket_sec
        end_bucket = (now // bucket_sec) * bucket_sec
        with self._lock:
            cached = self._trends_cache.get(cache_key)
            if cached is not None:
                return cached

            counts: dict[int, int] = {}
            for event in self._claim_events:
                claimed_at_ts = int(event["claimed_at_ts"])
                if claimed_at_ts < start_ts:
                    continue
                bucket_ts = (claimed_at_ts // bucket_sec) * bucket_sec
                counts[bucket_ts] = counts.get(bucket_ts, 0) + int(event["count"])

            series: list[dict[str, Any]] = []
            cursor = start_bucket
            while cursor <= end_bucket:
                series.append({"ts": isoformat_from_ts(int(cursor)), "count": counts.get(int(cursor), 0)})
                cursor += bucket_sec

            payload = {"window": window_sec, "bucket": bucket_sec, "series": series}
            self._trends_cache[cache_key] = payload
            return payload

    def _invalidate_derived_locked(self) -> None:
        self._leaderboard_cache = {}
        self._recent_cache = {}
        self._trends_cache = {}
        self._stats_cache = {}


class TokenDb:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def connect(self, timeout: float = 30.0) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False, timeout=timeout)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute(f"PRAGMA busy_timeout = {max(1, int(timeout * 1000))}")
        return conn

    def init_db(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    linuxdo_user_id TEXT NOT NULL UNIQUE,
                    linuxdo_username TEXT NOT NULL,
                    linuxdo_name TEXT,
                    trust_level INTEGER NOT NULL,
                    created_at_ts INTEGER NOT NULL,
                    last_login_at_ts INTEGER NOT NULL
                );

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

                CREATE TABLE IF NOT EXISTS inventory_runtime (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    status TEXT NOT NULL,
                    max_claims INTEGER NOT NULL,
                    updated_at_ts INTEGER NOT NULL
                );

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

                CREATE INDEX IF NOT EXISTS idx_token_claims_user_time
                    ON token_claims(user_id, claimed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_token_claims_api_time
                    ON token_claims(api_key_id, claimed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_claim_queue_status_time
                    ON claim_queue(status, enqueued_at_ts, id);
                CREATE INDEX IF NOT EXISTS idx_user_bans_target_time
                    ON user_bans(linuxdo_user_id, banned_at_ts DESC);
                """
            )
            claims_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(token_claims)").fetchall()
            }
            if "is_hidden" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN is_hidden INTEGER NOT NULL DEFAULT 0")
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(api_keys)").fetchall()
            }
            if "key_value" not in columns:
                conn.execute("ALTER TABLE api_keys ADD COLUMN key_value TEXT")
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(tokens)").fetchall()
            }
            if "is_active" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1")
            if "is_cleaned" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN is_cleaned INTEGER NOT NULL DEFAULT 0")
            if "is_enabled" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN is_enabled INTEGER NOT NULL DEFAULT 1")
            if "cleaned_at_ts" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN cleaned_at_ts INTEGER")

    def _get_runtime_policy_state(self, conn: sqlite3.Connection) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT status, max_claims, updated_at_ts FROM inventory_runtime WHERE id = 1"
        ).fetchone()
        if not row:
            return None
        return {
            "status": str(row["status"]),
            "max_claims": int(row["max_claims"]),
            "updated_at_ts": int(row["updated_at_ts"]),
        }

    def _set_runtime_policy_state(
        self,
        conn: sqlite3.Connection,
        *,
        status: str,
        max_claims: int,
        updated_at_ts: int,
    ) -> None:
        conn.execute(
            """
            INSERT INTO inventory_runtime (id, status, max_claims, updated_at_ts)
            VALUES (1, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                status = excluded.status,
                max_claims = excluded.max_claims,
                updated_at_ts = excluded.updated_at_ts
            """,
            (status, max_claims, updated_at_ts),
        )

    def _load_token_file(self, path: Path) -> tuple[str, str, str]:
        raw = path.read_bytes()
        encoding = detect_encoding(raw)
        decoded = raw.decode(encoding)
        content = json.loads(decoded)
        file_hash = hashlib.sha256(raw).hexdigest()
        content_json = json.dumps(content, ensure_ascii=False)
        return encoding, content_json, file_hash

    def sync_tokens(self, token_dir: Path) -> None:
        files = sorted(token_dir.glob("*.json"), key=lambda path: path.name.lower())
        seen_names: set[str] = set()
        now = now_ts()
        healthy_max_claims = get_inventory_limits()["healthy"]["max_claims"]

        with self._lock, self.connect() as conn:
            for path in files:
                try:
                    encoding, content_json, file_hash = self._load_token_file(path)
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    continue

                file_name = path.name
                file_path = path.relative_to(BASE_DIR).as_posix()
                seen_names.add(file_name)

                row = conn.execute(
                    "SELECT id, claim_count, max_claims, is_enabled FROM tokens WHERE file_name = ?",
                    (file_name,),
                ).fetchone()
                if row:
                    claim_count = int(row["claim_count"])
                    effective_max_claims = int(row["max_claims"])
                    is_enabled = int(row["is_enabled"]) if "is_enabled" in row.keys() else 1
                    is_available = 1 if is_enabled and claim_count < effective_max_claims else 0
                    conn.execute(
                        """
                        UPDATE tokens
                        SET file_path = ?,
                            file_hash = ?,
                            encoding = ?,
                            content_json = ?,
                            is_active = 1,
                            is_available = ?,
                            updated_at_ts = ?,
                            last_seen_at_ts = ?
                        WHERE id = ?
                        """,
                        (
                            file_path,
                            file_hash,
                            encoding,
                            content_json,
                            is_available,
                            now,
                            now,
                            row["id"],
                        ),
                    )
                else:
                    conn.execute(
                        """
                        INSERT INTO tokens (
                            file_name,
                            file_path,
                            file_hash,
                            encoding,
                            content_json,
                            is_active,
                            is_enabled,
                            is_available,
                            claim_count,
                            max_claims,
                            created_at_ts,
                            updated_at_ts,
                            last_seen_at_ts
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            file_name,
                            file_path,
                            file_hash,
                            encoding,
                            content_json,
                            1,
                            1,
                            1,
                            0,
                            healthy_max_claims,
                            now,
                            now,
                            now,
                        ),
                    )

            if seen_names:
                placeholders = ",".join("?" for _ in seen_names)
                conn.execute(
                    f"""
                    UPDATE tokens
                    SET is_active = 0,
                        is_available = 0,
                        last_seen_at_ts = ?
                    WHERE file_name NOT IN ({placeholders})
                    """,
                    (now, *seen_names),
                )
            else:
                conn.execute(
                    "UPDATE tokens SET is_active = 0, is_available = 0, last_seen_at_ts = ?",
                    (now,),
                )
            self.ensure_inventory_policy(conn=conn)
        _refresh_dashboard_memory()

    def sync_new_tokens(self, token_dir: Path) -> dict[str, int]:
        now = now_ts()
        healthy_max_claims = get_inventory_limits()["healthy"]["max_claims"]
        imported = 0
        skipped = 0
        errors = 0
        files: list[Path] = []

        try:
            with os.scandir(token_dir) as entries:
                for entry in entries:
                    if not entry.is_file() or not entry.name.lower().endswith(".json"):
                        continue
                    files.append(Path(entry.path))
        except OSError:
            return {"imported": 0, "skipped": 0, "errors": 1}

        with self._lock, self.connect(timeout=3.0) as conn:
            existing_names = {
                str(row["file_name"])
                for row in conn.execute("SELECT file_name FROM tokens").fetchall()
            }
            for path in files:
                file_name = path.name
                if file_name in existing_names:
                    skipped += 1
                    continue
                try:
                    encoding, content_json, file_hash = self._load_token_file(path)
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    errors += 1
                    continue
                file_path = path.relative_to(BASE_DIR).as_posix()
                conn.execute(
                    """
                    INSERT INTO tokens (
                        file_name,
                        file_path,
                        file_hash,
                        encoding,
                        content_json,
                        is_active,
                        is_cleaned,
                        is_enabled,
                        is_available,
                        claim_count,
                        max_claims,
                        created_at_ts,
                        cleaned_at_ts,
                        updated_at_ts,
                        last_seen_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_name,
                        file_path,
                        file_hash,
                        encoding,
                        content_json,
                        1,
                        0,
                        1,
                        1,
                        0,
                        healthy_max_claims,
                        now,
                        None,
                        now,
                        now,
                    ),
                )
                existing_names.add(file_name)
                imported += 1
            self.ensure_inventory_policy(conn=conn)

        if imported:
            _refresh_dashboard_memory()
        return {"imported": imported, "skipped": skipped, "errors": errors}

    def upsert_user(self, user: LinuxDOUser) -> dict[str, Any]:
        now = now_ts()
        with self._lock, self.connect() as conn:
            row = conn.execute(
                "SELECT id FROM users WHERE linuxdo_user_id = ?",
                (str(user.id),),
            ).fetchone()
            if row:
                conn.execute(
                    """
                    UPDATE users
                    SET linuxdo_username = ?,
                        linuxdo_name = ?,
                        trust_level = ?,
                        last_login_at_ts = ?
                    WHERE id = ?
                    """,
                    (user.username, user.name, user.trust_level, now, row["id"]),
                )
                user_id = int(row["id"])
            else:
                cursor = conn.execute(
                    """
                    INSERT INTO users (
                        linuxdo_user_id,
                        linuxdo_username,
                        linuxdo_name,
                        trust_level,
                        created_at_ts,
                        last_login_at_ts
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(user.id),
                        user.username,
                        user.name,
                        user.trust_level,
                        now,
                        now,
                    ),
                )
                user_id = int(cursor.lastrowid)

        result = {
            "id": user_id,
            "linuxdo_user_id": str(user.id),
            "username": user.username,
            "name": user.name or user.username,
            "trust_level": user.trust_level,
        }
        _refresh_dashboard_memory()
        return result

    def get_user(self, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if not row:
                return None
            return dict(row)

    def get_user_by_linuxdo_id(self, linuxdo_user_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE linuxdo_user_id = ?",
                (str(linuxdo_user_id),),
            ).fetchone()
            if not row:
                return None
            return dict(row)

    def get_active_ban(self, linuxdo_user_id: str, *, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
        target_conn = conn or self.connect()
        should_close = conn is None
        try:
            row = target_conn.execute(
                """
                SELECT user_bans.*,
                       users.linuxdo_username as banned_by_username,
                       users.linuxdo_name as banned_by_name,
                       unbanners.linuxdo_username as unbanned_by_username,
                       unbanners.linuxdo_name as unbanned_by_name
                FROM user_bans
                LEFT JOIN users ON users.id = user_bans.banned_by_user_id
                LEFT JOIN users AS unbanners ON unbanners.id = user_bans.unbanned_by_user_id
                WHERE user_bans.linuxdo_user_id = ?
                  AND user_bans.unbanned_at_ts IS NULL
                  AND (user_bans.expires_at_ts IS NULL OR user_bans.expires_at_ts > ?)
                ORDER BY user_bans.banned_at_ts DESC, user_bans.id DESC
                LIMIT 1
                """,
                (str(linuxdo_user_id), now_ts()),
            ).fetchone()
            if not row:
                return None
            return self._format_ban_row(row)
        finally:
            if should_close:
                target_conn.close()

    def list_bans(
        self,
        *,
        status_filter: str = "active",
        search: str = "",
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        now = now_ts()
        where_parts = ["1 = 1"]
        params: list[Any] = []
        if status_filter == "active":
            where_parts.append("user_bans.unbanned_at_ts IS NULL")
            where_parts.append("(user_bans.expires_at_ts IS NULL OR user_bans.expires_at_ts > ?)")
            params.append(now)
        elif status_filter == "expired":
            where_parts.append("user_bans.unbanned_at_ts IS NULL")
            where_parts.append("user_bans.expires_at_ts IS NOT NULL")
            where_parts.append("user_bans.expires_at_ts <= ?")
            params.append(now)
        elif status_filter == "unbanned":
            where_parts.append("user_bans.unbanned_at_ts IS NOT NULL")
        if search:
            pattern = f"%{search.strip().lower()}%"
            where_parts.append(
                """
                (
                    lower(user_bans.linuxdo_user_id) LIKE ?
                    OR lower(COALESCE(user_bans.username_snapshot, '')) LIKE ?
                    OR lower(COALESCE(target.linuxdo_username, '')) LIKE ?
                    OR lower(COALESCE(target.linuxdo_name, '')) LIKE ?
                )
                """
            )
            params.extend([pattern, pattern, pattern, pattern])
        with self.connect() as conn:
            total_row = conn.execute(
                f"""
                SELECT COUNT(*) as cnt
                FROM user_bans
                LEFT JOIN users AS target ON target.linuxdo_user_id = user_bans.linuxdo_user_id
                WHERE {' AND '.join(where_parts)}
                """,
                params,
            ).fetchone()
            rows = conn.execute(
                f"""
                SELECT user_bans.*,
                       target.linuxdo_username as target_username,
                       target.linuxdo_name as target_name,
                       users.linuxdo_username as banned_by_username,
                       users.linuxdo_name as banned_by_name,
                       unbanners.linuxdo_username as unbanned_by_username,
                       unbanners.linuxdo_name as unbanned_by_name
                FROM user_bans
                LEFT JOIN users AS target ON target.linuxdo_user_id = user_bans.linuxdo_user_id
                LEFT JOIN users ON users.id = user_bans.banned_by_user_id
                LEFT JOIN users AS unbanners ON unbanners.id = user_bans.unbanned_by_user_id
                WHERE {' AND '.join(where_parts)}
                ORDER BY user_bans.banned_at_ts DESC, user_bans.id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
        return {
            "items": [self._format_ban_row(row) for row in rows],
            "total": int(total_row["cnt"]) if total_row else 0,
            "limit": limit,
            "offset": offset,
        }

    def ban_user(
        self,
        *,
        linuxdo_user_id: str,
        username_snapshot: str | None,
        reason: str,
        banned_by_user_id: int,
        expires_at_ts: int | None,
        timeout_s: float = 30.0,
    ) -> dict[str, Any]:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute(
                """
                UPDATE user_bans
                SET unbanned_at_ts = ?,
                    unbanned_by_user_id = ?
                WHERE linuxdo_user_id = ?
                  AND unbanned_at_ts IS NULL
                """,
                (now, banned_by_user_id, str(linuxdo_user_id)),
            )
            cursor = conn.execute(
                """
                INSERT INTO user_bans (
                    linuxdo_user_id,
                    username_snapshot,
                    reason,
                    banned_by_user_id,
                    banned_at_ts,
                    expires_at_ts
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (str(linuxdo_user_id), username_snapshot, reason.strip(), banned_by_user_id, now, expires_at_ts),
            )
            row = conn.execute(
                """
                SELECT user_bans.*,
                       users.linuxdo_username as banned_by_username,
                       users.linuxdo_name as banned_by_name
                FROM user_bans
                LEFT JOIN users ON users.id = user_bans.banned_by_user_id
                WHERE user_bans.id = ?
                """,
                (int(cursor.lastrowid),),
            ).fetchone()
        return self._format_ban_row(row) if row else {}

    def unban_user(self, linuxdo_user_id: str, *, unbanned_by_user_id: int, timeout_s: float = 30.0) -> bool:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout_s) as conn:
            result = conn.execute(
                """
                UPDATE user_bans
                SET unbanned_at_ts = ?,
                    unbanned_by_user_id = ?
                WHERE linuxdo_user_id = ?
                  AND unbanned_at_ts IS NULL
                  AND (expires_at_ts IS NULL OR expires_at_ts > ?)
                """,
                (now, unbanned_by_user_id, str(linuxdo_user_id), now),
            )
        return int(result.rowcount or 0) > 0

    def list_users_for_admin(
        self,
        *,
        search: str = "",
        ban_status: str = "all",
        limit: int = 100,
        offset: int = 0,
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        offset = max(0, int(offset))
        now = now_ts()
        where_parts = ["1 = 1"]
        params: list[Any] = [now]
        if search:
            pattern = f"%{search.strip().lower()}%"
            where_parts.append(
                """
                (
                    lower(users.linuxdo_user_id) LIKE ?
                    OR lower(users.linuxdo_username) LIKE ?
                    OR lower(COALESCE(users.linuxdo_name, '')) LIKE ?
                )
                """
            )
            params.extend([pattern, pattern, pattern])
        if ban_status == "banned":
            where_parts.append("ban.id IS NOT NULL")
        elif ban_status == "normal":
            where_parts.append("ban.id IS NULL")
        with self.connect() as conn:
            total_row = conn.execute(
                f"""
                SELECT COUNT(*) as cnt
                FROM users
                LEFT JOIN (
                    SELECT ub.*
                    FROM user_bans AS ub
                    INNER JOIN (
                        SELECT linuxdo_user_id, MAX(id) as max_id
                        FROM user_bans
                        WHERE unbanned_at_ts IS NULL
                          AND (expires_at_ts IS NULL OR expires_at_ts > ?)
                        GROUP BY linuxdo_user_id
                    ) latest ON latest.max_id = ub.id
                ) AS ban ON ban.linuxdo_user_id = users.linuxdo_user_id
                WHERE {' AND '.join(where_parts)}
                """,
                params,
            ).fetchone()
            rows = conn.execute(
                f"""
                SELECT users.*,
                       COALESCE(claim_totals.claim_count, 0) as claim_count,
                       COALESCE(api_totals.active_keys, 0) as active_keys,
                       ban.id as active_ban_id,
                       ban.reason as ban_reason,
                       ban.banned_at_ts as ban_banned_at_ts,
                       ban.expires_at_ts as ban_expires_at_ts
                FROM users
                LEFT JOIN (
                    SELECT user_id, COUNT(*) as claim_count
                    FROM token_claims
                    GROUP BY user_id
                ) AS claim_totals ON claim_totals.user_id = users.id
                LEFT JOIN (
                    SELECT user_id, COUNT(*) as active_keys
                    FROM api_keys
                    WHERE status = 'active'
                    GROUP BY user_id
                ) AS api_totals ON api_totals.user_id = users.id
                LEFT JOIN (
                    SELECT ub.*
                    FROM user_bans AS ub
                    INNER JOIN (
                        SELECT linuxdo_user_id, MAX(id) as max_id
                        FROM user_bans
                        WHERE unbanned_at_ts IS NULL
                          AND (expires_at_ts IS NULL OR expires_at_ts > ?)
                        GROUP BY linuxdo_user_id
                    ) latest ON latest.max_id = ub.id
                ) AS ban ON ban.linuxdo_user_id = users.linuxdo_user_id
                WHERE {' AND '.join(where_parts)}
                ORDER BY users.last_login_at_ts DESC, users.id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "db_user_id": int(row["id"]),
                    "linuxdo_user_id": str(row["linuxdo_user_id"]),
                    "username": row["linuxdo_username"],
                    "name": row["linuxdo_name"] or row["linuxdo_username"],
                    "trust_level": int(row["trust_level"]),
                    "created_at": isoformat_from_ts(int(row["created_at_ts"])),
                    "last_login_at": isoformat_from_ts(int(row["last_login_at_ts"])),
                    "claim_count": int(row["claim_count"]),
                    "active_api_keys": int(row["active_keys"]),
                    "is_banned": row["active_ban_id"] is not None,
                    "ban_reason": row["ban_reason"],
                    "ban_expires_at": isoformat_from_ts(int(row["ban_expires_at_ts"]))
                    if row["ban_expires_at_ts"]
                    else None,
                }
            )
        return {"items": items, "total": int(total_row["cnt"]) if total_row else 0, "limit": limit, "offset": offset}

    def get_admin_user_detail(self, linuxdo_user_id: str) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE linuxdo_user_id = ?",
                (str(linuxdo_user_id),),
            ).fetchone()
            if not row:
                return None
            user = dict(row)
            totals = conn.execute(
                """
                SELECT COUNT(*) as total_claims,
                       COUNT(DISTINCT token_id) as unique_claims
                FROM token_claims
                WHERE user_id = ?
                """,
                (int(row["id"]),),
            ).fetchone()
            api_totals = conn.execute(
                """
                SELECT COUNT(*) as total_keys,
                       COUNT(CASE WHEN status = 'active' THEN 1 END) as active_keys
                FROM api_keys
                WHERE user_id = ?
                """,
                (int(row["id"]),),
            ).fetchone()
            recent_claim_rows = conn.execute(
                """
                SELECT token_claims.claimed_at_ts, tokens.file_name
                FROM token_claims
                JOIN tokens ON tokens.id = token_claims.token_id
                WHERE token_claims.user_id = ?
                ORDER BY token_claims.claimed_at_ts DESC, token_claims.id DESC
                LIMIT 20
                """,
                (int(row["id"]),),
            ).fetchall()
            ban = self.get_active_ban(str(linuxdo_user_id), conn=conn)
        return {
            "user": {
                "db_user_id": int(user["id"]),
                "linuxdo_user_id": str(user["linuxdo_user_id"]),
                "username": user["linuxdo_username"],
                "name": user["linuxdo_name"] or user["linuxdo_username"],
                "trust_level": int(user["trust_level"]),
                "created_at": isoformat_from_ts(int(user["created_at_ts"])),
                "last_login_at": isoformat_from_ts(int(user["last_login_at_ts"])),
            },
            "claims": {
                "total": int(totals["total_claims"]) if totals else 0,
                "unique": int(totals["unique_claims"]) if totals else 0,
                "recent": [
                    {
                        "claimed_at": isoformat_from_ts(int(item["claimed_at_ts"])),
                        "file_name": item["file_name"],
                    }
                    for item in recent_claim_rows
                ],
            },
            "api_keys": {
                "total": int(api_totals["total_keys"]) if api_totals else 0,
                "active": int(api_totals["active_keys"]) if api_totals else 0,
            },
            "ban": ban,
        }

    def list_tokens_for_admin(
        self,
        *,
        search: str = "",
        status_filter: str = "all",
        limit: int = 200,
        offset: int = 0,
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 500))
        offset = max(0, int(offset))
        where_parts = ["1 = 1"]
        params: list[Any] = []
        if search:
            pattern = f"%{search.strip().lower()}%"
            where_parts.append(
                """
                (
                    lower(file_name) LIKE ?
                    OR lower(file_path) LIKE ?
                )
                """
            )
            params.extend([pattern, pattern])
        if status_filter == "enabled":
            where_parts.append("is_active = 1")
            where_parts.append("is_enabled = 1")
        elif status_filter == "disabled":
            where_parts.append("(is_active = 0 OR is_enabled = 0)")
        with self.connect() as conn:
            total_row = conn.execute(
                f"""
                SELECT COUNT(*) as cnt
                FROM tokens
                WHERE {' AND '.join(where_parts)}
                """,
                params,
            ).fetchone()
            rows = conn.execute(
                f"""
                SELECT id, file_name, file_path, encoding, is_active, is_enabled, is_available,
                       is_cleaned, claim_count, max_claims, cleaned_at_ts, created_at_ts, updated_at_ts, last_seen_at_ts
                FROM tokens
                WHERE {' AND '.join(where_parts)}
                ORDER BY is_active DESC, is_enabled DESC, updated_at_ts DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
        return {
            "items": [
            {
                "id": int(row["id"]),
                "file_name": row["file_name"],
                "file_path": row["file_path"],
                "encoding": row["encoding"],
                "is_active": bool(row["is_active"]),
                "is_cleaned": bool(row["is_cleaned"]),
                "is_enabled": bool(row["is_enabled"]),
                "is_available": bool(row["is_available"]),
                "claim_count": int(row["claim_count"]),
                "max_claims": int(row["max_claims"]),
                "cleaned_at": isoformat_from_ts(int(row["cleaned_at_ts"])) if row["cleaned_at_ts"] else None,
                "created_at": isoformat_from_ts(int(row["created_at_ts"])),
                "updated_at": isoformat_from_ts(int(row["updated_at_ts"])),
                "last_seen_at": isoformat_from_ts(int(row["last_seen_at_ts"])),
            }
            for row in rows
            ],
            "total": int(total_row["cnt"]) if total_row else 0,
            "limit": limit,
            "offset": offset,
        }

    def set_token_enabled(self, token_id: int, enabled: bool, *, timeout_s: float = 30.0) -> dict[str, Any] | None:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute(
                """
                UPDATE tokens
                SET is_enabled = ?,
                    is_available = CASE
                        WHEN is_active = 1 AND ? = 1 AND claim_count < max_claims THEN 1
                        ELSE 0
                    END,
                    updated_at_ts = ?
                WHERE id = ?
                """,
                (1 if enabled else 0, 1 if enabled else 0, now, token_id),
            )
            row = conn.execute(
                """
                SELECT id, file_name, file_path, encoding, is_active, is_enabled, is_available,
                       is_cleaned, claim_count, max_claims, cleaned_at_ts, created_at_ts, updated_at_ts, last_seen_at_ts
                FROM tokens
                WHERE id = ?
                """,
                (token_id,),
            ).fetchone()
            self.ensure_inventory_policy(conn=conn)
        _refresh_dashboard_memory()
        if not row:
            return None
        return {
            "id": int(row["id"]),
            "file_name": row["file_name"],
            "file_path": row["file_path"],
            "encoding": row["encoding"],
            "is_active": bool(row["is_active"]),
            "is_cleaned": bool(row["is_cleaned"]),
            "is_enabled": bool(row["is_enabled"]),
            "is_available": bool(row["is_available"]),
            "claim_count": int(row["claim_count"]),
            "max_claims": int(row["max_claims"]),
            "cleaned_at": isoformat_from_ts(int(row["cleaned_at_ts"])) if row["cleaned_at_ts"] else None,
            "created_at": isoformat_from_ts(int(row["created_at_ts"])),
            "updated_at": isoformat_from_ts(int(row["updated_at_ts"])),
            "last_seen_at": isoformat_from_ts(int(row["last_seen_at_ts"])),
        }

    def cleanup_exhausted_tokens(self, token_dir: Path) -> dict[str, Any]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, file_name, file_path
                FROM tokens
                WHERE is_active = 1 AND is_cleaned = 0 AND claim_count >= max_claims
                ORDER BY updated_at_ts ASC, id ASC
                """
            ).fetchall()

        cleaned_ids: list[int] = []
        failed: list[dict[str, str]] = []
        deleted_files = 0
        missing_files = 0
        for row in rows:
            token_id = int(row["id"])
            file_name = str(row["file_name"])
            relative_path = str(row["file_path"] or "")
            target_path = BASE_DIR / relative_path if relative_path else (token_dir / file_name)
            try:
                if target_path.exists():
                    target_path.unlink()
                    deleted_files += 1
                else:
                    missing_files += 1
                cleaned_ids.append(token_id)
            except OSError as exc:
                failed.append({"file_name": file_name, "detail": str(exc)})

        compacted_content = 0
        now = now_ts()
        with self._lock, self.connect(timeout=3.0) as conn:
            if cleaned_ids:
                placeholders = ",".join("?" for _ in cleaned_ids)
                conn.execute(
                    f"""
                    UPDATE tokens
                    SET is_active = 0,
                        is_cleaned = 1,
                        is_enabled = 0,
                        is_available = 0,
                        content_json = '{{}}',
                        cleaned_at_ts = ?,
                        updated_at_ts = ?,
                        last_seen_at_ts = ?
                    WHERE id IN ({placeholders})
                    """,
                    (now, now, now, *cleaned_ids),
                )
            compacted_cursor = conn.execute(
                """
                UPDATE tokens
                SET content_json = '{}',
                    updated_at_ts = CASE
                        WHEN is_cleaned = 1 AND content_json != '{}' THEN ?
                        ELSE updated_at_ts
                    END
                WHERE is_cleaned = 1 AND content_json != '{}'
                """,
                (now,),
            )
            compacted_content = int(compacted_cursor.rowcount or 0)
            self.ensure_inventory_policy(conn=conn)
        if cleaned_ids or compacted_content:
            _refresh_dashboard_memory()

        return {
            "matched": len(rows),
            "cleaned": len(cleaned_ids),
            "deleted_files": deleted_files,
            "missing_files": missing_files,
            "compacted_content": compacted_content,
            "failed": failed,
        }

    def _format_ban_row(self, row: sqlite3.Row | dict[str, Any]) -> dict[str, Any]:
        def _value(key: str) -> Any:
            try:
                return row[key]
            except (KeyError, IndexError):
                return None

        expires_at_ts = row["expires_at_ts"]
        unbanned_at_ts = row["unbanned_at_ts"]
        banned_by_username = _value("banned_by_username")
        banned_by_name = _value("banned_by_name")
        unbanned_by_username = _value("unbanned_by_username")
        unbanned_by_name = _value("unbanned_by_name")
        return {
            "id": int(row["id"]),
            "linuxdo_user_id": str(row["linuxdo_user_id"]),
            "username_snapshot": row["username_snapshot"],
            "reason": row["reason"],
            "banned_at": isoformat_from_ts(int(row["banned_at_ts"])),
            "expires_at": isoformat_from_ts(int(expires_at_ts)) if expires_at_ts else None,
            "unbanned_at": isoformat_from_ts(int(unbanned_at_ts)) if unbanned_at_ts else None,
            "banned_by": {
                "username": banned_by_username,
                "name": banned_by_name or banned_by_username,
            }
            if banned_by_username
            else None,
            "unbanned_by": {
                "username": unbanned_by_username,
                "name": unbanned_by_name or unbanned_by_username,
            }
            if unbanned_by_username
            else None,
            "is_active": unbanned_at_ts is None and (expires_at_ts is None or int(expires_at_ts) > now_ts()),
        }

    def list_api_keys(self, user_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, name, key_prefix, key_value, status, created_at_ts, last_used_at_ts
                FROM api_keys
                WHERE user_id = ?
                ORDER BY id DESC
                """,
                (user_id,),
            ).fetchall()
        return [
            {
                "id": int(row["id"]),
                "name": row["name"],
                "prefix": row["key_prefix"],
                "token": row["key_value"],
                "status": row["status"],
                "created_at": isoformat_from_ts(int(row["created_at_ts"])),
                "last_used_at": isoformat_from_ts(int(row["last_used_at_ts"]))
                if row["last_used_at_ts"]
                else None,
            }
            for row in rows
        ]

    def create_api_key(self, user_id: int, name: str | None) -> dict[str, Any]:
        now = now_ts()
        api_key = generate_api_key()
        key_hash = hash_api_key(api_key)
        prefix = api_key[:8]

        with self._lock, self.connect() as conn:
            existing = conn.execute(
                "SELECT COUNT(*) as cnt FROM api_keys WHERE user_id = ? AND status = 'active'",
                (user_id,),
            ).fetchone()
            if existing and int(existing["cnt"]) >= get_apikey_max_per_user():
                raise PermissionError("API key limit reached.")

            cursor = conn.execute(
                """
                INSERT INTO api_keys (
                    user_id,
                    name,
                    key_hash,
                    key_prefix,
                    key_value,
                    status,
                    created_at_ts
                ) VALUES (?, ?, ?, ?, ?, 'active', ?)
                """,
                (user_id, name, key_hash, prefix, api_key, now),
            )
            api_key_id = int(cursor.lastrowid)

        created = {
            "id": api_key_id,
            "name": name,
            "prefix": prefix,
            "token": api_key,
            "created_at": isoformat_from_ts(now),
        }
        with _API_KEY_CACHE_LOCK:
            _API_KEY_CACHE_BY_HASH[key_hash] = {
                "api_key_id": api_key_id,
                "user_id": user_id,
                "status": "active",
            }
        return created

    def revoke_api_key(self, user_id: int, api_key_id: int) -> None:
        now = now_ts()
        with self._lock, self.connect() as conn:
            row = conn.execute(
                "SELECT key_hash FROM api_keys WHERE id = ? AND user_id = ?",
                (api_key_id, user_id),
            ).fetchone()
            conn.execute(
                """
                UPDATE api_keys
                SET status = 'revoked', revoked_at_ts = ?
                WHERE id = ? AND user_id = ?
                """,
                (now, api_key_id, user_id),
            )
        if row:
            with _API_KEY_CACHE_LOCK:
                _API_KEY_CACHE_BY_HASH.pop(str(row["key_hash"]), None)

    def resolve_api_key(self, api_key: str) -> dict[str, Any] | None:
        key_hash = hash_api_key(api_key)
        with _API_KEY_CACHE_LOCK:
            cached = _API_KEY_CACHE_BY_HASH.get(key_hash)

        if cached and cached.get("status") == "active":
            return {"api_key_id": int(cached["api_key_id"]), "user_id": int(cached["user_id"])}

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT api_keys.id as api_key_id, api_keys.user_id as user_id
                FROM api_keys
                WHERE api_keys.key_hash = ? AND api_keys.status = 'active'
                """,
                (key_hash,),
            ).fetchone()
        if not row:
            return None
        record = {"api_key_id": int(row["api_key_id"]), "user_id": int(row["user_id"])}
        with _API_KEY_CACHE_LOCK:
            _API_KEY_CACHE_BY_HASH[key_hash] = {**record, "status": "active"}
        return record

    def get_quota_usage(self, user_id: int) -> dict[str, int]:
        now = now_ts()
        cutoff = now - 3600
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM token_claims
                WHERE user_id = ? AND claimed_at_ts >= ?
                """
                ,
                (user_id, cutoff),
            ).fetchone()
            used = int(row["cnt"]) if row else 0
            limit = get_claim_hourly_limit(self, conn=conn)
        remaining = max(0, limit - used)
        return {"used": used, "limit": limit, "remaining": remaining}

    def get_user_claim_totals(self, user_id: int) -> dict[str, int]:
        with self.connect() as conn:
            total = conn.execute(
                "SELECT COUNT(*) as cnt FROM token_claims WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            unique = conn.execute(
                "SELECT COUNT(DISTINCT token_id) as cnt FROM token_claims WHERE user_id = ?",
                (user_id,),
            ).fetchone()
        return {
            "total": int(total["cnt"]) if total else 0,
            "unique": int(unique["cnt"]) if unique else 0,
        }

    def get_dashboard_stats(self, user_id: int) -> dict[str, int]:
        with self.connect() as conn:
            total_tokens = conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1"
            ).fetchone()
            available_tokens = conn.execute(
                """
                SELECT COALESCE(SUM(
                    CASE
                        WHEN claim_count < max_claims THEN max_claims - claim_count
                        ELSE 0
                    END
                ), 0) as cnt
                FROM tokens
                WHERE is_active = 1 AND is_enabled = 1
                """
            ).fetchone()
            claimed_total = conn.execute("SELECT COUNT(*) as cnt FROM token_claims").fetchone()
            claimed_unique = conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1 AND claim_count > 0"
            ).fetchone()
            others_claimed_total = conn.execute(
                "SELECT COUNT(*) as cnt FROM token_claims WHERE user_id != ?",
                (user_id,),
            ).fetchone()
            others_claimed_unique = conn.execute(
                "SELECT COUNT(DISTINCT token_id) as cnt FROM token_claims WHERE user_id != ?",
                (user_id,),
            ).fetchone()

        return {
            "total_tokens": int(total_tokens["cnt"]) if total_tokens else 0,
            "available_tokens": int(available_tokens["cnt"]) if available_tokens else 0,
            "claimed_total": int(claimed_total["cnt"]) if claimed_total else 0,
            "claimed_unique": int(claimed_unique["cnt"]) if claimed_unique else 0,
            "others_claimed_total": int(others_claimed_total["cnt"]) if others_claimed_total else 0,
            "others_claimed_unique": int(others_claimed_unique["cnt"]) if others_claimed_unique else 0,
        }

    def get_inventory_snapshot(self, *, conn=None) -> dict[str, int]:
        def _fetch(target_conn):
            total_row = target_conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1"
            ).fetchone()
            available_row = target_conn.execute(
                """
                SELECT COALESCE(SUM(
                    CASE
                        WHEN claim_count < max_claims THEN max_claims - claim_count
                        ELSE 0
                    END
                ), 0) as cnt
                FROM tokens
                WHERE is_active = 1 AND is_enabled = 1
                """
            ).fetchone()
            unclaimed_row = target_conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1 AND claim_count = 0"
            ).fetchone()
            return {
                "total": int(total_row["cnt"]) if total_row else 0,
                "available": int(available_row["cnt"]) if available_row else 0,
                "unclaimed": int(unclaimed_row["cnt"]) if unclaimed_row else 0,
            }

        if conn is None:
            with self.connect() as target_conn:
                return _fetch(target_conn)
        return _fetch(conn)

    def ensure_inventory_policy(self, *, conn=None) -> dict[str, Any]:
        def _ensure(target_conn):
            policy = get_inventory_policy(self, conn=target_conn, force=True)
            status = policy["status"]
            max_claims = int(policy["max_claims"])
            non_healthy_scope = str(policy.get("non_healthy_max_claims_scope") or "all_unfinished")
            runtime_state = self._get_runtime_policy_state(target_conn)
            runtime_status = runtime_state["status"] if runtime_state else None
            runtime_max_claims = int(runtime_state["max_claims"]) if runtime_state else None
            healthy_max_claims = get_inventory_limits()["healthy"]["max_claims"]

            def _apply_non_healthy_scope(now: int) -> None:
                if max_claims <= healthy_max_claims:
                    return
                if non_healthy_scope == "all_unfinished":
                    target_conn.execute(
                        """
                        UPDATE tokens
                        SET max_claims = ?,
                            is_available = CASE WHEN claim_count < ? THEN 1 ELSE 0 END,
                            updated_at_ts = ?
                        WHERE is_active = 1
                          AND is_enabled = 1
                          AND claim_count < ?
                          AND max_claims < ?
                        """
                        ,
                        (max_claims, max_claims, now, max_claims, max_claims),
                    )
                    return
                target_conn.execute(
                    """
                    UPDATE tokens
                    SET max_claims = ?,
                        is_available = CASE WHEN claim_count < ? THEN 1 ELSE 0 END,
                        updated_at_ts = ?
                    WHERE is_active = 1
                      AND is_enabled = 1
                      AND claim_count = 0
                      AND max_claims < ?
                    """
                    ,
                    (max_claims, max_claims, now, max_claims),
                )

            if runtime_status != status or runtime_max_claims != max_claims:
                now = now_ts()
                target_conn.execute(
                    """
                    UPDATE tokens
                    SET max_claims = ?,
                        is_available = CASE WHEN claim_count < ? THEN 1 ELSE 0 END,
                        updated_at_ts = ?
                    WHERE is_active = 1 AND is_enabled = 1
                    """
                    ,
                    (healthy_max_claims, healthy_max_claims, now),
                )
                _apply_non_healthy_scope(now)
                self._set_runtime_policy_state(
                    target_conn,
                    status=status,
                    max_claims=max_claims,
                    updated_at_ts=now,
                )
                _POLICY_STATE["status"] = status
                _POLICY_STATE["max_claims"] = max_claims
            elif runtime_state:
                now = now_ts()
                _apply_non_healthy_scope(now)
                _POLICY_STATE["status"] = runtime_state["status"]
                _POLICY_STATE["max_claims"] = int(runtime_state["max_claims"])
            return policy

        if conn is None:
            with self._lock, self.connect() as target_conn:
                target_conn.execute("BEGIN IMMEDIATE")
                return _ensure(target_conn)
        return _ensure(conn)

    def get_leaderboard(self, window_sec: int, limit: int) -> list[dict[str, Any]]:
        cutoff = now_ts() - max(0, int(window_sec))
        limit = max(1, int(limit))
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT users.linuxdo_user_id as user_id,
                       users.linuxdo_username as username,
                       users.linuxdo_name as name,
                       COUNT(*) as cnt
                FROM token_claims
                JOIN users ON users.id = token_claims.user_id
                WHERE token_claims.claimed_at_ts >= ?
                GROUP BY users.id
                ORDER BY cnt DESC, users.id ASC
                LIMIT ?
                """
                ,
                (cutoff, limit),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            name = row["name"] or row["username"]
            items.append(
                {
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "name": name,
                    "count": int(row["cnt"]),
                }
            )
        return items

    def list_recent_claims(self, limit: int) -> list[dict[str, Any]]:
        limit = max(1, int(limit))
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT token_claims.request_id as request_id,
                       MAX(token_claims.claimed_at_ts) as claimed_at_ts,
                       users.linuxdo_username as username,
                       users.linuxdo_name as name,
                       COUNT(*) as cnt
                FROM token_claims
                JOIN users ON users.id = token_claims.user_id
                GROUP BY token_claims.request_id, users.id
                ORDER BY claimed_at_ts DESC
                LIMIT ?
                """
                ,
                (limit,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            name = row["name"] or row["username"]
            items.append(
                {
                    "name": name,
                    "username": row["username"],
                    "count": int(row["cnt"]),
                    "claimed_at": isoformat_from_ts(int(row["claimed_at_ts"])),
                }
            )
        return items

    def get_claim_trends(self, window_sec: int, bucket_sec: int) -> list[dict[str, Any]]:
        window_sec = max(1, int(window_sec))
        bucket_sec = max(60, int(bucket_sec))
        now = now_ts()
        start_ts = now - window_sec
        start_bucket = (start_ts // bucket_sec) * bucket_sec
        end_bucket = (now // bucket_sec) * bucket_sec
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT CAST(claimed_at_ts / ? AS INTEGER) * ? as bucket_ts,
                       COUNT(*) as cnt
                FROM token_claims
                WHERE claimed_at_ts >= ?
                GROUP BY bucket_ts
                ORDER BY bucket_ts ASC
                """
                ,
                (bucket_sec, bucket_sec, start_ts),
            ).fetchall()
        counts = {int(row["bucket_ts"]): int(row["cnt"]) for row in rows}
        series: list[dict[str, Any]] = []
        cursor = start_bucket
        while cursor <= end_bucket:
            series.append(
                {
                    "ts": isoformat_from_ts(int(cursor)),
                    "count": counts.get(int(cursor), 0),
                }
            )
            cursor += bucket_sec
        return series

    def get_queue_overview(self) -> dict[str, int]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM claim_queue WHERE status = 'queued' AND remaining > 0"
            ).fetchone()
        total = int(row["cnt"]) if row else 0
        return {"total": total}

    def claim_tokens(
        self,
        user_id: int,
        api_key_id: int | None,
        count: int,
    ) -> dict[str, Any]:
        try:
            try_fulfill_queue(self)
        except Exception:
            pass
        requested = max(1, count)
        now = now_ts()
        cutoff = now - 3600
        request_id = secrets.token_hex(8)
        result: dict[str, Any] | None = None

        with self._lock, self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self.ensure_inventory_policy(conn=conn)
            batch_limit = get_claim_batch_limit(self, conn=conn)
            requested = min(requested, batch_limit)
            used_row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM token_claims
                WHERE user_id = ? AND claimed_at_ts >= ?
                """,
                (user_id, cutoff),
            ).fetchone()
            used = int(used_row["cnt"]) if used_row else 0
            limit = get_claim_hourly_limit(self, conn=conn)
            remaining = max(0, limit - used)
            if remaining <= 0:
                raise RateLimitError("您当前小时内的兑换额度已用完")

            if api_key_id is not None:
                per_minute = get_apikey_rate_per_minute()
                if per_minute > 0:
                    minute_cutoff = now - 60
                    minute_row = conn.execute(
                        """
                        SELECT COUNT(*) as cnt
                        FROM token_claims
                        WHERE api_key_id = ? AND claimed_at_ts >= ?
                        """,
                        (api_key_id, minute_cutoff),
                    ).fetchone()
                    minute_used = int(minute_row["cnt"]) if minute_row else 0
                    remaining_minute = max(0, per_minute - minute_used)
                    if remaining_minute <= 0:
                        raise RateLimitError("API key rate limit exceeded.")
                    requested = min(requested, remaining_minute)

            target = min(requested, remaining)
            if target <= 0:
                raise RateLimitError("您当前小时内的兑换额度已用完")

            if claim_queue.has_pending_queue(self, conn=conn):
                queued = claim_queue.enqueue_claim(self, user_id, api_key_id, target, conn=conn)
                result = {
                    "request_id": queued["request_id"],
                    "items": [],
                    "requested": target,
                    "granted": 0,
                    "queued": True,
                    "queue_id": queued["queue_id"],
                    "queue_position": queued["position"],
                    "queue_remaining": queued["remaining"],
                    "quota": {"used": used, "limit": limit, "remaining": remaining},
                }
            else:
                available_row = conn.execute(
                    """
                    SELECT COUNT(*) as cnt
                    FROM tokens
                    WHERE is_active = 1 AND is_enabled = 1 AND is_available = 1 AND claim_count < max_claims
                    """
                ).fetchone()
                available = int(available_row["cnt"]) if available_row else 0
                if available < target:
                    queued = claim_queue.enqueue_claim(self, user_id, api_key_id, target, conn=conn)
                    result = {
                        "request_id": queued["request_id"],
                        "items": [],
                        "requested": target,
                        "granted": 0,
                        "queued": True,
                        "queue_id": queued["queue_id"],
                        "queue_position": queued["position"],
                        "queue_remaining": queued["remaining"],
                        "quota": {"used": used, "limit": limit, "remaining": remaining},
                    }
                else:
                    rows = conn.execute(
                        """
                        SELECT id, file_name, file_path, encoding, content_json, claim_count, max_claims
                        FROM tokens
                        WHERE is_active = 1 AND is_enabled = 1 AND is_available = 1 AND claim_count < max_claims
                        ORDER BY
                            CASE WHEN claim_count > 0 THEN 0 ELSE 1 END ASC,
                            created_at_ts ASC,
                            id ASC
                        LIMIT ?
                        """,
                        (target,),
                    ).fetchall()

                    if not rows:
                        queued = claim_queue.enqueue_claim(self, user_id, api_key_id, target, conn=conn)
                        result = {
                            "request_id": queued["request_id"],
                            "items": [],
                            "requested": target,
                            "granted": 0,
                            "queued": True,
                            "queue_id": queued["queue_id"],
                            "queue_position": queued["position"],
                            "queue_remaining": queued["remaining"],
                            "quota": {"used": used, "limit": limit, "remaining": remaining},
                        }
                    else:
                        items: list[dict[str, Any]] = []
                        for row in rows:
                            token_id = int(row["id"])
                            new_count = int(row["claim_count"]) + 1
                            new_available = 1 if new_count < int(row["max_claims"]) else 0
                            conn.execute(
                                """
                                UPDATE tokens
                                SET claim_count = ?,
                                    is_available = ?,
                                    updated_at_ts = ?
                                WHERE id = ?
                                """,
                                (new_count, new_available, now, token_id),
                            )
                            conn.execute(
                                """
                                INSERT INTO token_claims (
                                    token_id, user_id, api_key_id, claimed_at_ts, is_hidden, request_id
                                ) VALUES (?, ?, ?, ?, 0, ?)
                                """,
                                (token_id, user_id, api_key_id, now, request_id),
                            )
                            claim_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

                            items.append(
                                {
                                    "claim_id": claim_id,
                                    "token_id": token_id,
                                    "file_name": row["file_name"],
                                    "file_path": row["file_path"],
                                    "encoding": row["encoding"],
                                    "content": json.loads(row["content_json"]),
                                }
                            )

                        new_used = used + len(items)
                        new_remaining = max(0, limit - new_used)
                        result = {
                            "request_id": request_id,
                            "items": items,
                            "requested": target,
                            "granted": len(items),
                            "queued": False,
                            "quota": {"used": new_used, "limit": limit, "remaining": new_remaining},
                        }

        if result is None:
            raise RuntimeError("claim_tokens completed without a result")
        _refresh_dashboard_memory()
        return result

    def get_claimed_token(self, token_id: int, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT tokens.id, tokens.file_name, tokens.file_path, tokens.encoding, tokens.content_json
                FROM tokens
                JOIN token_claims ON token_claims.token_id = tokens.id
                WHERE tokens.id = ? AND token_claims.user_id = ? AND token_claims.is_hidden = 0
                LIMIT 1
                """,
                (token_id, user_id),
            ).fetchone()
        if not row:
            return None
        return {
            "token_id": int(row["id"]),
            "file_name": row["file_name"],
            "file_path": row["file_path"],
            "encoding": row["encoding"],
            "content": json.loads(row["content_json"]),
        }

    def list_claims(self, user_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT token_claims.id as claim_id,
                       token_claims.claimed_at_ts,
                       token_claims.request_id,
                       tokens.id as token_id,
                       tokens.file_name,
                       tokens.file_path,
                       tokens.encoding,
                       tokens.content_json
                FROM token_claims
                JOIN tokens ON tokens.id = token_claims.token_id
                WHERE token_claims.user_id = ? AND token_claims.is_hidden = 0
                ORDER BY token_claims.claimed_at_ts DESC, token_claims.id DESC
                """,
                (user_id,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "claim_id": int(row["claim_id"]),
                    "token_id": int(row["token_id"]),
                    "request_id": row["request_id"],
                    "file_name": row["file_name"],
                    "file_path": row["file_path"],
                    "encoding": row["encoding"],
                    "claimed_at": isoformat_from_ts(int(row["claimed_at_ts"])),
                    "content": json.loads(row["content_json"]),
                }
            )
        return items

    def get_queue_status(self, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id, requested, remaining, enqueued_at_ts, request_id
                FROM claim_queue
                WHERE user_id = ? AND status = 'queued' AND remaining > 0
                ORDER BY enqueued_at_ts ASC, id ASC
                LIMIT 1
                """,
                (user_id,),
            ).fetchone()
            if not row:
                return None
            queue_id = int(row["id"])
            ahead_row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM claim_queue
                WHERE status = 'queued'
                  AND (enqueued_at_ts < ? OR (enqueued_at_ts = ? AND id < ?))
                """,
                (int(row["enqueued_at_ts"]), int(row["enqueued_at_ts"]), queue_id),
            ).fetchone()
            ahead = int(ahead_row["cnt"]) if ahead_row else 0
            total_row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM claim_queue
                WHERE status = 'queued' AND remaining > 0
                """
            ).fetchone()
            total_queued = int(total_row["cnt"]) if total_row else 0
            available_row = conn.execute(
                """
                SELECT COALESCE(SUM(
                    CASE
                        WHEN claim_count < max_claims THEN max_claims - claim_count
                        ELSE 0
                    END
                ), 0) as cnt
                FROM tokens
                WHERE is_active = 1 AND is_enabled = 1
                """
            ).fetchone()
            available = int(available_row["cnt"]) if available_row else 0
        return {
            "queue_id": queue_id,
            "position": ahead + 1,
            "ahead": ahead,
            "total_queued": total_queued,
            "available_tokens": available,
            "requested": int(row["requested"]),
            "remaining": int(row["remaining"]),
            "enqueued_at": isoformat_from_ts(int(row["enqueued_at_ts"])),
            "request_id": row["request_id"],
        }

    def hide_claims(self, user_id: int, claim_ids: list[int]) -> int:
        if not claim_ids:
            return 0
        placeholders = ",".join("?" for _ in claim_ids)
        with self._lock, self.connect() as conn:
            cursor = conn.execute(
                f"""
                UPDATE token_claims
                SET is_hidden = 1
                WHERE user_id = ? AND id IN ({placeholders})
                """,
                (user_id, *claim_ids),
            )
            return int(cursor.rowcount or 0)

    def list_claim_files(self, user_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT token_claims.id as claim_id,
                       tokens.file_name,
                       tokens.file_path,
                       tokens.encoding,
                       tokens.content_json
                FROM token_claims
                JOIN tokens ON tokens.id = token_claims.token_id
                WHERE token_claims.user_id = ? AND token_claims.is_hidden = 0
                ORDER BY token_claims.claimed_at_ts DESC, token_claims.id DESC
                """,
                (user_id,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "claim_id": int(row["claim_id"]),
                    "file_name": row["file_name"],
                    "file_path": row["file_path"],
                    "encoding": row["encoding"],
                    "content": json.loads(row["content_json"]),
                }
            )
        return items

    def list_claimed_tokens(self, user_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT tokens.id as token_id,
                       tokens.file_name,
                       tokens.file_path,
                       tokens.encoding,
                       tokens.content_json,
                       MAX(token_claims.claimed_at_ts) as last_claimed_ts
                FROM token_claims
                JOIN tokens ON tokens.id = token_claims.token_id
                WHERE token_claims.user_id = ? AND token_claims.is_hidden = 0
                GROUP BY tokens.id, tokens.file_name, tokens.file_path, tokens.encoding, tokens.content_json
                ORDER BY last_claimed_ts DESC, tokens.id DESC
                """,
                (user_id,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "token_id": int(row["token_id"]),
                    "file_name": row["file_name"],
                    "file_path": row["file_path"],
                    "encoding": row["encoding"],
                    "content": json.loads(row["content_json"]),
                }
            )
        return items

def sync_tokens_with_retry(db_handle: TokenDb, token_dir: Path, retries: int = 5, delay_sec: float = 1.0) -> None:
    for attempt in range(1, retries + 1):
        try:
            db_handle.sync_tokens(token_dir)
            return
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= retries:
                raise
            time.sleep(delay_sec)


def sync_new_tokens_with_retry(
    db_handle: TokenDb,
    token_dir: Path,
    retries: int = 3,
    delay_sec: float = 0.5,
) -> dict[str, int]:
    for attempt in range(1, retries + 1):
        try:
            return db_handle.sync_new_tokens(token_dir)
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= retries:
                raise
            time.sleep(delay_sec)
    return {"imported": 0, "skipped": 0, "errors": 0}


def run_db_write_with_retry(func, retries: int = 4, delay_sec: float = 0.35):
    for attempt in range(1, retries + 1):
        try:
            return func()
        except sqlite3.OperationalError as exc:
            if "database is locked" not in str(exc).lower() or attempt >= retries:
                raise
            time.sleep(delay_sec)


def make_db_busy_response() -> Response:
    error_json = json.dumps(
        {"detail": "数据库正忙，请稍后重试。"},
        ensure_ascii=False,
    ).encode("utf-8")
    return Response(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content=error_json,
        media_type="application/json",
        headers={"Retry-After": "3"},
    )


def get_session_secret() -> str:
    return os.getenv(SESSION_SECRET_ENV, "change-me-session-secret")


def get_linuxdo_client_id() -> str:
    return os.getenv(LINUXDO_CLIENT_ID_ENV, "").strip()


def get_linuxdo_client_secret() -> str:
    return os.getenv(LINUXDO_CLIENT_SECRET_ENV, "").strip()


def get_linuxdo_scope() -> str:
    return os.getenv(LINUXDO_SCOPE_ENV, "read").strip() or "read"


def get_linuxdo_min_trust_level() -> int:
    raw = os.getenv(LINUXDO_MIN_TRUST_LEVEL_ENV, "0").strip()
    try:
        return max(0, int(raw))
    except ValueError:
        return 0


def get_linuxdo_allowed_ids() -> set[str]:
    raw = os.getenv(LINUXDO_ALLOWED_IDS_ENV, "")
    return {item.strip() for item in raw.split(",") if item.strip()}


def get_db_path() -> Path:
    raw = os.getenv(DB_PATH_ENV, "").strip()
    if raw:
        return Path(raw)
    return BASE_DIR / "token_atlas.db"



def parse_window_to_seconds(raw: str | None, default_sec: int, *, max_seconds: int | None = None) -> int:
    if not raw:
        value = default_sec
    else:
        cleaned = raw.strip().lower()
        value = default_sec
        if cleaned.endswith("h") and cleaned[:-1].isdigit():
            value = int(cleaned[:-1]) * 3600
        elif cleaned.endswith("d") and cleaned[:-1].isdigit():
            value = int(cleaned[:-1]) * 86400
        elif cleaned.isdigit():
            value = int(cleaned)
    if max_seconds is not None:
        value = min(value, max_seconds)
    return max(1, value)


def parse_bucket_seconds(raw: str | None, default_sec: int) -> int:
    if not raw:
        return max(60, default_sec)
    cleaned = raw.strip().lower()
    if cleaned.endswith("h") and cleaned[:-1].isdigit():
        return max(60, int(cleaned[:-1]) * 3600)
    if cleaned.endswith("m") and cleaned[:-1].isdigit():
        return max(60, int(cleaned[:-1]) * 60)
    if cleaned.isdigit():
        return max(60, int(cleaned))
    return max(60, default_sec)


def get_provider_base_url() -> str | None:

    raw = os.getenv(PROVIDER_BASE_URL_ENV, "").strip()
    if not raw:
        return None
    return raw.rstrip("/")


def build_download_url(request: Request, token_id: int) -> str:
    url = str(request.url_for("download_claimed_token", token_id=token_id))
    base_url = get_provider_base_url()
    if not base_url:
        return url
    parsed = urllib.parse.urlparse(url)
    query = f"?{parsed.query}" if parsed.query else ""
    return f"{base_url}{parsed.path}{query}"


def get_inventory_thresholds() -> dict[str, int]:
    return {
        "healthy": max(1, env_int(TOKEN_HEALTHY_THRESHOLD_ENV, 1000)),
        "warning": max(1, env_int(TOKEN_WARNING_THRESHOLD_ENV, 500)),
        "critical": max(1, env_int(TOKEN_CRITICAL_THRESHOLD_ENV, 100)),
    }


def get_inventory_limits() -> dict[str, dict[str, int]]:
    return {
        "healthy": {
            "hourly": max(1, env_int(TOKEN_HOURLY_LIMIT_HEALTHY_ENV, 30)),
            "max_claims": max(1, env_int(TOKEN_MAX_CLAIMS_HEALTHY_ENV, 1)),
        },
        "warning": {
            "hourly": max(1, env_int(TOKEN_HOURLY_LIMIT_WARNING_ENV, 20)),
            "max_claims": max(1, env_int(TOKEN_MAX_CLAIMS_WARNING_ENV, 2)),
        },
        "critical": {
            "hourly": max(1, env_int(TOKEN_HOURLY_LIMIT_CRITICAL_ENV, 15)),
            "max_claims": max(1, env_int(TOKEN_MAX_CLAIMS_CRITICAL_ENV, 3)),
        },
    }


def build_inventory_policy_from_snapshot(snapshot: dict[str, int]) -> dict[str, Any]:
    thresholds = get_inventory_thresholds()
    limits = get_inventory_limits()
    unclaimed = int(snapshot["unclaimed"])
    status = resolve_inventory_status(unclaimed, thresholds)
    chosen = limits.get(status, limits["healthy"])
    return {
        "status": status,
        "unclaimed": unclaimed,
        "thresholds": thresholds,
        "hourly_limit": chosen["hourly"],
        "max_claims": chosen["max_claims"],
        "non_healthy_max_claims_scope": get_non_healthy_max_claims_scope(),
    }


def resolve_inventory_status(unclaimed: int, thresholds: dict[str, int]) -> str:
    if unclaimed < thresholds["critical"]:
        return "critical"
    if unclaimed < thresholds["warning"]:
        return "warning"
    return "healthy"


def get_inventory_policy(db_handle: "TokenDb", *, conn=None, force: bool = False) -> dict[str, Any]:
    cached = _POLICY_CACHE.get("value")
    ts = float(_POLICY_CACHE.get("ts") or 0.0)
    if cached and not force and _cache_fresh(ts):
        return cached
    snapshot = db_handle.get_inventory_snapshot(conn=conn)
    policy = build_inventory_policy_from_snapshot(snapshot)
    _POLICY_CACHE["value"] = policy
    _POLICY_CACHE["ts"] = time.time()
    return policy


def get_claim_hourly_limit(db_handle: "TokenDb" | None = None, *, conn=None) -> int:
    policy = get_inventory_policy(db_handle or db, conn=conn)
    return policy["hourly_limit"]


def get_claim_batch_limit(db_handle: "TokenDb" | None = None, *, conn=None) -> int:
    return get_claim_hourly_limit(db_handle, conn=conn)


def get_max_claims_per_token(db_handle: "TokenDb" | None = None, *, conn=None) -> int:
    policy = get_inventory_policy(db_handle or db, conn=conn)
    return policy["max_claims"]


def get_apikey_max_per_user() -> int:
    return max(1, env_int(APIKEY_MAX_PER_USER_ENV, 5))


def get_apikey_rate_per_minute() -> int:
    return max(0, env_int(APIKEY_RATE_PER_MIN_ENV, 60))


def get_admin_identities() -> dict[str, set[str]]:
    return parse_admin_identities(env_value(ADMIN_IDENTITIES_ENV))


def is_admin_identity(linuxdo_user_id: str | None, username: str | None) -> bool:
    identities = get_admin_identities()
    if linuxdo_user_id and str(linuxdo_user_id) in identities["ids"]:
        return True
    if username and normalize_username(username) in identities["usernames"]:
        return True
    return False


def is_safe_relative_redirect(value: str | None) -> bool:
    if not value:
        return False
    return value.startswith("/") and not value.startswith("//")


def parse_expires_at_to_ts(value: str | None) -> int | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    normalized = cleaned.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError("Invalid expires_at format") from exc
    if parsed.tzinfo is None:
        parsed = parsed.astimezone()
    return int(parsed.timestamp())


def generate_api_key() -> str:
    return f"tk_{secrets.token_urlsafe(24)}"


def hash_api_key(api_key: str) -> str:
    return hashlib.sha256(api_key.encode("utf-8")).hexdigest()


def is_linuxdo_enabled() -> bool:
    return bool(get_linuxdo_client_id() and get_linuxdo_client_secret())


def build_linuxdo_redirect_uri(request: Request) -> str:
    configured = os.getenv(LINUXDO_REDIRECT_URI_ENV, "").strip()
    if configured:
        return configured
    return str(request.url_for("linuxdo_callback"))


def build_linuxdo_authorize_url(request: Request, state: str) -> str:
    params = urllib.parse.urlencode(
        {
            "client_id": get_linuxdo_client_id(),
            "response_type": "code",
            "redirect_uri": build_linuxdo_redirect_uri(request),
            "scope": get_linuxdo_scope(),
            "state": state,
        }
    )
    return f"{LINUXDO_AUTHORIZE_URL}?{params}"


def decode_json_bytes(raw: bytes) -> Any:
    return json.loads(raw.decode("utf-8"))


def post_form_json(url: str, data: dict[str, str], headers: dict[str, str]) -> dict[str, Any]:
    body = urllib.parse.urlencode(data).encode("utf-8")
    request = urllib.request.Request(url, data=body, headers=headers, method="POST")
    with urllib.request.urlopen(request, timeout=15) as response:
        return decode_json_bytes(response.read())


def get_json(url: str, headers: dict[str, str]) -> dict[str, Any]:
    request = urllib.request.Request(url, headers=headers, method="GET")
    with urllib.request.urlopen(request, timeout=15) as response:
        return decode_json_bytes(response.read())


def exchange_linuxdo_code(request: Request, code: str) -> str:
    credentials = f"{get_linuxdo_client_id()}:{get_linuxdo_client_secret()}".encode("utf-8")
    basic_auth = base64.b64encode(credentials).decode("ascii")
    payload = post_form_json(
        LINUXDO_TOKEN_URL,
        {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": build_linuxdo_redirect_uri(request),
        },
        {
            "Accept": "application/json",
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    access_token = str(payload.get("access_token", "")).strip()
    if not access_token:
        raise ValueError("Linux.do token response did not include access_token")
    return access_token


def fetch_linuxdo_user(access_token: str) -> LinuxDOUser:
    payload = get_json(
        LINUXDO_USER_URL,
        {
            "Accept": "application/json",
            "Authorization": f"Bearer {access_token}",
        },
    )
    return LinuxDOUser.model_validate(payload)


def validate_linuxdo_user(user: LinuxDOUser) -> None:
    if not user.active:
        raise PermissionError("Linux.do user is not active")
    if user.silenced:
        raise PermissionError("Linux.do user is silenced")
    if user.trust_level < get_linuxdo_min_trust_level():
        raise PermissionError("Linux.do trust level is below the configured minimum")

    allowed_ids = get_linuxdo_allowed_ids()
    if allowed_ids and str(user.id) not in allowed_ids:
        raise PermissionError("Linux.do user is not in the allowlist")


def set_linuxdo_session(request: Request, user: LinuxDOUser, db_user_id: int) -> None:
    request.session[SESSION_AUTH_KEY] = {
        "method": "linuxdo",
        "user_id": db_user_id,
        "logged_in_at": isoformat_now(),
        "user": {
            "id": user.id,
            "username": user.username,
            "name": user.name or user.username,
            "trust_level": user.trust_level,
            "avatar_template": user.avatar_template,
            "is_admin": is_admin_identity(str(user.id), user.username),
        },
    }


def clear_auth_session(request: Request) -> None:
    request.session.pop(SESSION_AUTH_KEY, None)
    request.session.pop(SESSION_OAUTH_STATE_KEY, None)
    request.session.pop(SESSION_POST_LOGIN_REDIRECT_KEY, None)


def clear_auth_cookie(request: Request, response: Response) -> None:
    clear_auth_session(request)
    response.delete_cookie(SESSION_COOKIE_NAME)


def get_session_auth(request: Request) -> dict[str, Any] | None:
    session_auth = request.session.get(SESSION_AUTH_KEY)
    if isinstance(session_auth, dict):
        return session_auth
    return None


def build_ban_error_payload(context: dict[str, Any]) -> dict[str, Any]:
    ban = context["ban"]
    return {
        "detail": "当前账号已被封禁",
        "ban": ban,
        "user": context["user"],
    }


def get_request_context(request: Request) -> dict[str, Any]:
    auth = get_session_auth(request)
    if not auth or auth.get("method") != "linuxdo":
        raise PermissionError
    user_id = auth.get("user_id")
    if user_id is None:
        raise PermissionError
    db_user = db.get_user(int(user_id))
    if not db_user:
        raise PermissionError
    username = db_user["linuxdo_username"]
    public_user = {
        "id": db_user["linuxdo_user_id"],
        "username": username,
        "name": db_user["linuxdo_name"] or username,
        "trust_level": int(db_user["trust_level"]),
        "is_admin": is_admin_identity(db_user["linuxdo_user_id"], username),
    }
    ban = db.get_active_ban(db_user["linuxdo_user_id"])
    return {
        "user_id": int(db_user["id"]),
        "db_user": db_user,
        "user": public_user,
        "is_admin": bool(public_user["is_admin"]),
        "ban": ban,
        "is_banned": ban is not None,
    }


def require_session_user(request: Request) -> dict[str, Any]:
    context = get_request_context(request)
    if context["is_banned"]:
        raise BannedUserError(build_ban_error_payload(context))
    return context


def require_admin_user(request: Request) -> dict[str, Any]:
    context = get_request_context(request)
    if not context["is_admin"]:
        raise AccessDeniedError("Admin access required.")
    return context


def extract_api_key(
    x_api_key: str | None = Header(default=None, alias=API_KEY_HEADER),
    legacy_access_key: str | None = Header(default=None, alias=LEGACY_ACCESS_KEY_HEADER),
    key: str | None = Query(default=None),
) -> str | None:
    return x_api_key or legacy_access_key or key


def require_api_key(
    api_key: str | None = Depends(extract_api_key),
) -> dict[str, Any]:
    if not api_key:
        raise PermissionError
    record = db.resolve_api_key(api_key)
    if not record:
        raise PermissionError
    owner = db.get_user(int(record["user_id"]))
    if not owner:
        raise PermissionError
    ban = db.get_active_ban(owner["linuxdo_user_id"])
    if ban:
        raise BannedUserError(
            {
                "detail": "当前账号已被封禁",
                "ban": ban,
                "user": {
                    "id": owner["linuxdo_user_id"],
                    "username": owner["linuxdo_username"],
                    "name": owner["linuxdo_name"] or owner["linuxdo_username"],
                },
            }
        )
    return record


def verify_api_key(
    api_key: str | None = Depends(extract_api_key),
) -> None:
    if not api_key:
        raise PermissionError
    record = db.resolve_api_key(api_key)
    if not record:
        raise PermissionError


def build_claimed_documents(user_id: int) -> list[TokenDocument]:
    items = db.list_claimed_tokens(user_id)
    documents: list[TokenDocument] = []

    for index, item in enumerate(items):
        file_name = item["file_name"]
        file_path = item["file_path"]
        encoding = item["encoding"]
        content = item["content"]
        keys = sorted(content.keys()) if isinstance(content, dict) else []
        error = None

        full_path = BASE_DIR / file_path
        if full_path.exists():
            stat = full_path.stat()
            size = stat.st_size
            mtime = isoformat_timestamp(stat.st_mtime)
        else:
            size = len(json.dumps(content, ensure_ascii=False).encode("utf-8"))
            mtime = isoformat_now()

        documents.append(
            TokenDocument(
                index=index,
                id=Path(file_name).stem,
                name=file_name,
                path=file_path,
                size=size,
                mtime=mtime,
                encoding=encoding,
                keys=keys,
                error=error,
                content=content,
            )
        )

    return documents


db = TokenDb(get_db_path())
db.init_db()
_QUEUE_FULFILL_LOCK = threading.Lock()
_QUEUE_LAST_FULFILL_TS = 0.0
_QUEUE_FULFILL_THROTTLE_SEC = 5.0
_CLAIM_EVENT_WINDOW_SEC = 14 * 24 * 3600
_STATS_CACHE: dict[str, Any] = {"ts": 0.0, "value": None}
_POLICY_CACHE: dict[str, Any] = {"ts": 0.0, "value": None}
_POLICY_STATE: dict[str, Any] = {"status": None, "max_claims": None}
_LEADERBOARD_CACHE: dict[str, Any] = {"ts": 0.0, "value": None, "key": None}
_RECENT_CLAIMS_CACHE: dict[str, Any] = {"ts": 0.0, "value": None, "key": None}
_TRENDS_CACHE: dict[str, Any] = {"ts": 0.0, "value": None, "key": None}
_SYSTEM_STATUS_CACHE: dict[str, Any] = {"ts": 0.0, "value": None}
_QUEUE_STATUS_CACHE: dict[int, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 5.0
_QUEUE_PUMP_THREAD: threading.Thread | None = None
_QUEUE_PUMP_STOP = threading.Event()
_QUEUE_PUMP_INTERVAL_SEC = 20.0
_TOKEN_IMPORT_THREAD: threading.Thread | None = None
_TOKEN_IMPORT_STOP = threading.Event()
_TOKEN_IMPORT_INTERVAL_SEC = 60.0
_API_KEY_CACHE_LOCK = threading.RLock()
_API_KEY_CACHE_BY_HASH: dict[str, dict[str, Any]] = {}
_DASHBOARD_CACHE = DashboardMemoryCache()


def _refresh_dashboard_memory() -> None:
    _DASHBOARD_CACHE.refresh_from_db(db)
    _QUEUE_STATUS_CACHE.clear()


def try_fulfill_queue(db_handle: TokenDb) -> None:
    global _QUEUE_LAST_FULFILL_TS
    now = time.time()
    with _QUEUE_FULFILL_LOCK:
        if now - _QUEUE_LAST_FULFILL_TS < _QUEUE_FULFILL_THROTTLE_SEC:
            return
        _QUEUE_LAST_FULFILL_TS = now
    result = claim_queue.fulfill_queue(
        db_handle,
        hourly_limit=get_claim_hourly_limit(db_handle),
        apikey_rate_limit=get_apikey_rate_per_minute(),
    )
    if result.get("fulfilled") or result.get("updated"):
        _refresh_dashboard_memory()


def _cache_fresh(ts: float) -> bool:
    return time.time() - ts < _CACHE_TTL_SEC


def _queue_pump_loop() -> None:
    while not _QUEUE_PUMP_STOP.is_set():
        try:
            if claim_queue.has_pending_queue(db):
                try_fulfill_queue(db)
        except Exception:
            pass
        _QUEUE_PUMP_STOP.wait(timeout=_QUEUE_PUMP_INTERVAL_SEC)


def _token_import_loop() -> None:
    while not _TOKEN_IMPORT_STOP.wait(timeout=_TOKEN_IMPORT_INTERVAL_SEC):
        try:
            result = sync_new_tokens_with_retry(db, TOKEN_DIR)
            if result.get("imported"):
                try_fulfill_queue(db)
        except Exception:
            pass


@asynccontextmanager
async def lifespan(_: FastAPI):
    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    db.init_db()
    _refresh_dashboard_memory()

    _QUEUE_PUMP_STOP.clear()
    _TOKEN_IMPORT_STOP.clear()
    global _QUEUE_PUMP_THREAD
    global _TOKEN_IMPORT_THREAD
    _QUEUE_PUMP_THREAD = threading.Thread(
        target=_queue_pump_loop,
        daemon=True,
        name="claim-queue-pump",
    )
    _QUEUE_PUMP_THREAD.start()
    _TOKEN_IMPORT_THREAD = threading.Thread(
        target=_token_import_loop,
        daemon=True,
        name="token-import-pump",
    )
    _TOKEN_IMPORT_THREAD.start()

    try:
        yield
    finally:
        _QUEUE_PUMP_STOP.set()
        _TOKEN_IMPORT_STOP.set()
        if _QUEUE_PUMP_THREAD is not None:
            _QUEUE_PUMP_THREAD.join(timeout=5)
            _QUEUE_PUMP_THREAD = None
        if _TOKEN_IMPORT_THREAD is not None:
            _TOKEN_IMPORT_THREAD.join(timeout=5)
            _TOKEN_IMPORT_THREAD = None


app = FastAPI(title="Token Atlas", version="1.1.0", lifespan=lifespan)
app.add_middleware(
    SessionMiddleware,
    secret_key=get_session_secret(),
    session_cookie=SESSION_COOKIE_NAME,
    same_site="lax",
    max_age=60 * 60 * 24 * 7,
)
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")




@app.exception_handler(PermissionError)
def permission_error_handler(_: Any, __: PermissionError) -> Response:
    return Response(status_code=status.HTTP_401_UNAUTHORIZED)


@app.exception_handler(AccessDeniedError)
def access_denied_handler(_: Any, exc: AccessDeniedError) -> Response:
    payload = json.dumps({"detail": str(exc)}, ensure_ascii=False).encode("utf-8")
    return Response(status_code=status.HTTP_403_FORBIDDEN, content=payload, media_type="application/json")


@app.exception_handler(BannedUserError)
def banned_user_handler(_: Any, exc: BannedUserError) -> Response:
    payload = json.dumps(exc.payload, ensure_ascii=False).encode("utf-8")
    return Response(status_code=status.HTTP_403_FORBIDDEN, content=payload, media_type="application/json")


@app.exception_handler(RateLimitError)
def rate_limit_error_handler(_: Any, exc: RateLimitError) -> Response:
    payload = json.dumps({"detail": str(exc)}).encode("utf-8")
    return Response(status_code=status.HTTP_429_TOO_MANY_REQUESTS, content=payload, media_type="application/json")


@app.get("/", response_class=FileResponse)
def get_home() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "index.html",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/admin", response_class=FileResponse)
def get_admin_home() -> FileResponse:
    return FileResponse(
        STATIC_DIR / "admin.html",
        headers={"Cache-Control": "no-store"},
    )


@app.post("/auth/login")
def login(_: Request) -> Response:
    error_json = json.dumps({"detail": "Access key login has been removed."}).encode("utf-8")
    return Response(
        status_code=status.HTTP_410_GONE,
        content=error_json,
        media_type="application/json",
    )


@app.post("/auth/logout")
def logout(request: Request) -> Response:
    response = Response(status_code=status.HTTP_204_NO_CONTENT)
    clear_auth_cookie(request, response)
    return response


@app.get("/auth/status")
def get_auth_status(request: Request) -> dict[str, Any]:
    auth = get_session_auth(request)
    user_payload = None
    if auth is not None:
        try:
            context = get_request_context(request)
            user_payload = {
                **context["user"],
                "is_banned": context["is_banned"],
                "ban_reason": context["ban"]["reason"] if context["ban"] else None,
                "ban_expires_at": context["ban"]["expires_at"] if context["ban"] else None,
            }
        except PermissionError:
            clear_auth_session(request)
            auth = None
    payload = {
        "authenticated": auth is not None,
        "method": auth.get("method") if auth else None,
        "user": user_payload,
        "oauth": {
            "linuxdo_enabled": is_linuxdo_enabled(),
            "linuxdo_login_url": "/auth/linuxdo/login" if is_linuxdo_enabled() else None,
        },
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


@app.get("/admin/me")
def get_admin_me(request: Request) -> dict[str, Any]:
    context = require_admin_user(request)
    policy = get_inventory_policy(db)
    return {
        "user": context["user"],
        "ban": context["ban"],
        "policy": {
            "status": policy["status"],
            "hourly_limit": policy["hourly_limit"],
            "max_claims": policy["max_claims"],
            "thresholds": policy["thresholds"],
            "non_healthy_max_claims_scope": policy["non_healthy_max_claims_scope"],
            "source": "env",
        },
        "system": _DASHBOARD_CACHE.get_system_status(),
    }


@app.get("/admin/users")
def admin_list_users(
    request: Request,
    search: str = Query(default=""),
    ban_status: str = Query(default="all"),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    require_admin_user(request)
    return db.list_users_for_admin(search=search, ban_status=ban_status, limit=limit, offset=offset)


@app.get("/admin/users/{linuxdo_user_id}")
def admin_get_user_detail(request: Request, linuxdo_user_id: str) -> dict[str, Any]:
    require_admin_user(request)
    detail = db.get_admin_user_detail(linuxdo_user_id)
    if not detail:
        error_json = json.dumps({"detail": "User not found."}, ensure_ascii=False).encode("utf-8")
        return Response(status_code=status.HTTP_404_NOT_FOUND, content=error_json, media_type="application/json")
    return detail


@app.post("/admin/users/{linuxdo_user_id}/ban")
def admin_ban_user(request: Request, linuxdo_user_id: str, payload: AdminBanPayload) -> dict[str, Any]:
    context = require_admin_user(request)
    target = db.get_user_by_linuxdo_id(linuxdo_user_id)
    if not target:
        error_json = json.dumps({"detail": "User not found."}, ensure_ascii=False).encode("utf-8")
        return Response(status_code=status.HTTP_404_NOT_FOUND, content=error_json, media_type="application/json")
    try:
        expires_at_ts = parse_expires_at_to_ts(payload.expires_at)
    except ValueError:
        error_json = json.dumps(
            {"detail": "Invalid expires_at. Use ISO datetime or leave empty."},
            ensure_ascii=False,
        ).encode("utf-8")
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content=error_json, media_type="application/json")
    if expires_at_ts is not None and expires_at_ts <= now_ts():
        error_json = json.dumps(
            {"detail": "expires_at must be in the future."},
            ensure_ascii=False,
        ).encode("utf-8")
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content=error_json, media_type="application/json")
    try:
        ban = run_db_write_with_retry(
            lambda: db.ban_user(
                linuxdo_user_id=target["linuxdo_user_id"],
                username_snapshot=target["linuxdo_username"],
                reason=payload.reason,
                banned_by_user_id=context["user_id"],
                expires_at_ts=expires_at_ts,
                timeout_s=3.0,
            ),
            retries=2,
            delay_sec=0.2,
        )
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            return make_db_busy_response()
        raise
    return {"ok": True, "ban": ban}


@app.post("/admin/users/{linuxdo_user_id}/unban")
def admin_unban_user(request: Request, linuxdo_user_id: str) -> dict[str, Any]:
    context = require_admin_user(request)
    try:
        changed = run_db_write_with_retry(
            lambda: db.unban_user(
                linuxdo_user_id,
                unbanned_by_user_id=context["user_id"],
                timeout_s=3.0,
            ),
            retries=2,
            delay_sec=0.2,
        )
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            return make_db_busy_response()
        raise
    return {"ok": changed}


@app.get("/admin/bans")
def admin_list_bans(
    request: Request,
    status_filter: str = Query(default="active", alias="status"),
    search: str = Query(default=""),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    require_admin_user(request)
    return db.list_bans(status_filter=status_filter, search=search, limit=limit, offset=offset)


@app.get("/admin/tokens")
def admin_list_tokens(
    request: Request,
    search: str = Query(default=""),
    status_filter: str = Query(default="all", alias="status"),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    require_admin_user(request)
    return db.list_tokens_for_admin(search=search, status_filter=status_filter, limit=limit, offset=offset)


@app.post("/admin/tokens/{token_id}/activate")
def admin_activate_token(request: Request, token_id: int) -> dict[str, Any]:
    require_admin_user(request)
    try:
        token = run_db_write_with_retry(
            lambda: db.set_token_enabled(token_id, True, timeout_s=3.0),
            retries=2,
            delay_sec=0.2,
        )
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            return make_db_busy_response()
        raise
    if not token:
        error_json = json.dumps({"detail": "Token not found."}, ensure_ascii=False).encode("utf-8")
        return Response(status_code=status.HTTP_404_NOT_FOUND, content=error_json, media_type="application/json")
    return {"ok": True, "item": token}


@app.post("/admin/tokens/{token_id}/deactivate")
def admin_deactivate_token(request: Request, token_id: int) -> dict[str, Any]:
    require_admin_user(request)
    try:
        token = run_db_write_with_retry(
            lambda: db.set_token_enabled(token_id, False, timeout_s=3.0),
            retries=2,
            delay_sec=0.2,
        )
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            return make_db_busy_response()
        raise
    if not token:
        error_json = json.dumps({"detail": "Token not found."}, ensure_ascii=False).encode("utf-8")
        return Response(status_code=status.HTTP_404_NOT_FOUND, content=error_json, media_type="application/json")
    return {"ok": True, "item": token}


@app.post("/admin/tokens/cleanup-exhausted")
def admin_cleanup_exhausted_tokens(request: Request) -> dict[str, Any]:
    require_admin_user(request)
    try:
        result = run_db_write_with_retry(
            lambda: db.cleanup_exhausted_tokens(TOKEN_DIR),
            retries=2,
            delay_sec=0.2,
        )
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            return make_db_busy_response()
        raise
    return {"ok": True, **result}


@app.get("/admin/policy")
def admin_get_policy(request: Request) -> dict[str, Any]:
    require_admin_user(request)
    policy = get_inventory_policy(db)
    return {
        "source": "env",
        "status": policy["status"],
        "hourly_limit": policy["hourly_limit"],
        "max_claims": policy["max_claims"],
        "thresholds": policy["thresholds"],
        "non_healthy_max_claims_scope": policy["non_healthy_max_claims_scope"],
        "system": _DASHBOARD_CACHE.get_system_status(),
    }


@app.get("/me")
def get_me(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    user = db.get_user(user_id)
    if not user:
        raise PermissionError
    quota = db.get_quota_usage(user_id)
    totals = db.get_user_claim_totals(user_id)
    api_keys = db.list_api_keys(user_id)
    return {
        "user": {
            "id": user["linuxdo_user_id"],
            "username": user["linuxdo_username"],
            "name": user["linuxdo_name"] or user["linuxdo_username"],
            "trust_level": user["trust_level"],
            "is_admin": session["is_admin"],
            "is_banned": False,
        },
        "quota": quota,
        "claims": totals,
        "api_keys": {
            "limit": get_apikey_max_per_user(),
            "active": len([key for key in api_keys if key["status"] == "active"]),
        },
    }


@app.get("/dashboard/leaderboard")
def get_dashboard_leaderboard(
    request: Request,
    window: str | None = Query(default="24h"),
    limit: int = Query(default=50, ge=1, le=50),
) -> dict[str, Any]:
    require_session_user(request)
    window_sec = parse_window_to_seconds(window, 24 * 3600, max_seconds=7 * 24 * 3600)
    limit = min(limit, 50)
    return _DASHBOARD_CACHE.get_leaderboard(window_sec, limit)


@app.get("/dashboard/summary")
def get_dashboard_summary(
    request: Request,
    window: str | None = Query(default="7d"),
    bucket: str | None = Query(default="1h"),
    leaderboard_window: str | None = Query(default="24h"),
    leaderboard_limit: int = Query(default=50, ge=1, le=50),
    recent_limit: int = Query(default=50, ge=1, le=50),
) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    full_stats = _DASHBOARD_CACHE.get_stats(user_id)
    stats = {
        "total_tokens": full_stats["total_tokens"],
        "available_tokens": full_stats["available_tokens"],
        "claimed_total": full_stats["claimed_total"],
        "claimed_unique": full_stats["claimed_unique"],
        "others_claimed_total": full_stats["others_claimed_total"],
        "others_claimed_unique": full_stats["others_claimed_unique"],
    }

    leaderboard_window_sec = parse_window_to_seconds(
        leaderboard_window, 24 * 3600, max_seconds=7 * 24 * 3600
    )
    leaderboard_limit = min(leaderboard_limit, 50)
    leaderboard = _DASHBOARD_CACHE.get_leaderboard(leaderboard_window_sec, leaderboard_limit)

    recent_limit = min(recent_limit, 50)
    recent = _DASHBOARD_CACHE.get_recent(recent_limit)

    window_sec = parse_window_to_seconds(window, 7 * 24 * 3600, max_seconds=14 * 24 * 3600)
    bucket_sec = parse_bucket_seconds(bucket, 3600)
    trends = _DASHBOARD_CACHE.get_trends(window_sec, bucket_sec)
    system = _DASHBOARD_CACHE.get_system_status()

    return {
        "stats": stats,
        "leaderboard": leaderboard,
        "recent": recent,
        "trends": trends,
        "system": system,
    }


@app.get("/dashboard/recent-claims")
def get_dashboard_recent_claims(
    request: Request,
    limit: int = Query(default=20, ge=1, le=50),
) -> dict[str, Any]:
    require_session_user(request)
    return _DASHBOARD_CACHE.get_recent(limit)


@app.get("/dashboard/trends")
def get_dashboard_trends(
    request: Request,
    window: str | None = Query(default="7d"),
    bucket: str | None = Query(default="1h"),
) -> dict[str, Any]:
    require_session_user(request)
    window_sec = parse_window_to_seconds(window, 7 * 24 * 3600, max_seconds=14 * 24 * 3600)
    bucket_sec = parse_bucket_seconds(bucket, 3600)
    return _DASHBOARD_CACHE.get_trends(window_sec, bucket_sec)


@app.get("/dashboard/system-status")
def get_dashboard_system_status(request: Request) -> dict[str, Any]:
    require_session_user(request)
    return _DASHBOARD_CACHE.get_system_status()


@app.get("/dashboard/stats")
def get_dashboard_stats(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    stats = _DASHBOARD_CACHE.get_stats(user_id)
    return {
        "total_tokens": stats["total_tokens"],
        "available_tokens": stats["available_tokens"],
        "claimed_total": stats["claimed_total"],
        "claimed_unique": stats["claimed_unique"],
        "others_claimed_total": stats["others_claimed_total"],
        "others_claimed_unique": stats["others_claimed_unique"],
    }


@app.get("/me/api-keys")
def list_api_keys(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    keys = db.list_api_keys(user_id)
    return {"items": keys, "limit": get_apikey_max_per_user()}


@app.post("/me/api-keys")
def create_api_key(request: Request, payload: ApiKeyCreatePayload | None = Body(default=None)) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    name = payload.name if payload else None
    created = db.create_api_key(user_id, name)
    return created


@app.post("/me/api-keys/{api_key_id}/revoke")
def revoke_api_key(request: Request, api_key_id: int) -> Response:
    session = require_session_user(request)
    user_id = session["user_id"]
    db.revoke_api_key(user_id, api_key_id)
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/me/claims")
def list_claims(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    items = db.list_claims(user_id)
    for item in items:
        item["download_url"] = build_download_url(request, item["token_id"])
    return {"items": items}


@app.get("/me/queue-status")
def get_queue_status(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    cached = _QUEUE_STATUS_CACHE.get(user_id)
    if cached and _cache_fresh(cached[0]):
        return cached[1]
    status = db.get_queue_status(user_id)
    if not status:
        payload = {"queued": False}
        _QUEUE_STATUS_CACHE[user_id] = (time.time(), payload)
        return payload
    payload = {"queued": True, **status}
    _QUEUE_STATUS_CACHE[user_id] = (time.time(), payload)
    return payload


@app.post("/me/claims/hide")
def hide_claims(request: Request, payload: ClaimHidePayload) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    claim_ids = [int(cid) for cid in payload.claim_ids if isinstance(cid, int) or str(cid).isdigit()]
    updated = db.hide_claims(user_id, claim_ids)
    return {"hidden": updated}


@app.get("/me/claims/archive")
def download_claims_archive(request: Request) -> StreamingResponse:
    session = require_session_user(request)
    user_id = session["user_id"]
    items = db.list_claim_files(user_id)
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in items:
            filename = item["file_name"]
            content = json.dumps(item["content"], ensure_ascii=False).encode("utf-8")
            archive.writestr(filename, content)

    buffer.seek(0)
    filename = f"claimed-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


@app.post("/me/claim")
def claim_tokens_session(request: Request, payload: ClaimPayload) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    result = db.claim_tokens(user_id, None, payload.count)
    for item in result["items"]:
        item["download_url"] = build_download_url(request, item["token_id"])
    return result


@app.post("/api/claim")
def claim_tokens_api(
    request: Request,
    payload: ClaimPayload,
    api_record: dict[str, Any] = Depends(require_api_key),
) -> dict[str, Any]:
    result = db.claim_tokens(api_record["user_id"], api_record["api_key_id"], payload.count)
    for item in result["items"]:
        item["download_url"] = build_download_url(request, item["token_id"])
    return result


@app.get("/api/download/{token_id}", name="download_claimed_token")
def download_claimed_token(
    request: Request,
    token_id: int,
    api_key: str | None = Depends(extract_api_key),
) -> Response:
    user_id: int | None = None
    if api_key:
        resolved = db.resolve_api_key(api_key)
        if not resolved:
            raise PermissionError
        user_id = resolved["user_id"]
    else:
        session = require_session_user(request)
        user_id = int(session.get("user_id"))

    if not user_id:
        raise PermissionError

    token = db.get_claimed_token(token_id, user_id)
    if not token:
        raise PermissionError

    content_json = json.dumps(token["content"], ensure_ascii=False).encode("utf-8")
    filename = token["file_name"]
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return Response(content=content_json, media_type="application/json", headers=headers)


@app.get("/auth/linuxdo/login")
def start_linuxdo_login(request: Request, next: str | None = Query(default="/")) -> Response:
    if not is_linuxdo_enabled():
        error_json = json.dumps({"detail": "Linux.do OAuth is not configured."}).encode("utf-8")
        return Response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=error_json,
            media_type="application/json",
        )

    if is_safe_relative_redirect(next):
        request.session[SESSION_POST_LOGIN_REDIRECT_KEY] = next
    else:
        request.session[SESSION_POST_LOGIN_REDIRECT_KEY] = "/"
    state = secrets.token_urlsafe(32)
    request.session[SESSION_OAUTH_STATE_KEY] = state
    return RedirectResponse(
        build_linuxdo_authorize_url(request, state),
        status_code=status.HTTP_302_FOUND,
    )


@app.get("/auth/linuxdo/callback", name="linuxdo_callback")
def linuxdo_callback(
    request: Request,
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
) -> Response:
    expected_state = request.session.get(SESSION_OAUTH_STATE_KEY)
    request.session.pop(SESSION_OAUTH_STATE_KEY, None)
    post_login_redirect = request.session.pop(SESSION_POST_LOGIN_REDIRECT_KEY, "/")
    if not is_safe_relative_redirect(post_login_redirect):
        post_login_redirect = "/"

    if error:
        response = RedirectResponse(
            url=f"{post_login_redirect}?auth_error={urllib.parse.quote(error)}",
            status_code=status.HTTP_302_FOUND,
        )
        clear_auth_cookie(request, response)
        return response

    if not is_linuxdo_enabled():
        response = RedirectResponse(
            url=f"{post_login_redirect}?auth_error=linuxdo_not_configured",
            status_code=status.HTTP_302_FOUND,
        )
        clear_auth_cookie(request, response)
        return response

    if not code or not state or state != expected_state:
        response = RedirectResponse(
            url=f"{post_login_redirect}?auth_error=invalid_oauth_state",
            status_code=status.HTTP_302_FOUND,
        )
        clear_auth_cookie(request, response)
        return response

    try:
        access_token = exchange_linuxdo_code(request, code)
        user = fetch_linuxdo_user(access_token)
        validate_linuxdo_user(user)
        db_user = db.upsert_user(user)
        set_linuxdo_session(request, user, db_user["id"])
    except (PermissionError, ValueError, urllib.error.HTTPError, urllib.error.URLError, json.JSONDecodeError):
        response = RedirectResponse(
            url=f"{post_login_redirect}?auth_error=linuxdo_login_failed",
            status_code=status.HTTP_302_FOUND,
        )
        clear_auth_cookie(request, response)
        return response

    base_url = get_provider_base_url()
    if base_url:
        return RedirectResponse(
            url=f"{base_url}{post_login_redirect}?auth=linuxdo",
            status_code=status.HTTP_302_FOUND,
        )
    return RedirectResponse(url=f"{post_login_redirect}?auth=linuxdo", status_code=status.HTTP_302_FOUND)


@app.get("/json")
def get_index(api_record: dict[str, Any] = Depends(require_api_key)) -> dict[str, Any]:
    documents = build_claimed_documents(api_record["user_id"])
    return {
        "count": len(documents),
        "updated_at": isoformat_now(),
        "items": [item.to_index_payload() for item in documents],
    }


@app.get("/json/item")
def get_item(
    name: str | None = Query(default=None),
    id: str | None = Query(default=None),
    index: int | None = Query(default=None, ge=0),
    api_record: dict[str, Any] = Depends(require_api_key),
) -> dict[str, Any]:
    if name is None and id is None and index is None:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=b'{"detail":"Provide one of: name, id, index."}',
            media_type="application/json",
        )

    documents = build_claimed_documents(api_record["user_id"])
    item: TokenDocument | None = None
    if name:
        for doc in documents:
            if doc.name == name:
                item = doc
                break
    elif id:
        for doc in documents:
            if doc.id == id:
                item = doc
                break
    elif index is not None and 0 <= index < len(documents):
        item = documents[index]

    if item is None:
        return Response(
            status_code=status.HTTP_404_NOT_FOUND,
            content=b'{"detail":"Token file not found."}',
            media_type="application/json",
        )

    if item.error:
        error_json = json.dumps({"detail": item.error}, ensure_ascii=False).encode("utf-8")
        return Response(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content=error_json,
            media_type="application/json",
        )

    return item.to_detail_payload()


@app.get("/health")
def get_health() -> dict[str, Any]:
    system = _DASHBOARD_CACHE.get_system_status()
    return {
        "status": "ok",
        "token_count": int(system["inventory"]["total"]),
        "updated_at": system["index"]["updated_at"],
    }


@app.post("/json/archive")
def download_archive(
    payload: ArchivePayload | None = Body(default=None),
    api_record: dict[str, Any] = Depends(require_api_key),
) -> StreamingResponse:
    documents = build_claimed_documents(api_record["user_id"])
    if payload and payload.names:
        wanted = set(payload.names)
        documents = [doc for doc in documents if doc.name in wanted]

    if not documents:
        error_json = json.dumps({"detail": "No token files available for archive."}).encode("utf-8")
        return Response(
            status_code=status.HTTP_404_NOT_FOUND,
            content=error_json,
            media_type="application/json",
        )

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in documents:
            content = json.dumps(item.content, ensure_ascii=False).encode("utf-8")
            archive.writestr(item.name, content)

    buffer.seek(0)
    filename = f"token-atlas-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buffer, media_type="application/zip", headers=headers)


@app.get("/zip")
def download_all_archive(api_record: dict[str, Any] = Depends(require_api_key)) -> StreamingResponse:
    return download_archive(api_record=api_record)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=env_int("PORT", 8000),
        reload=False,
    )
