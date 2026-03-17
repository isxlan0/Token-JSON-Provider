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
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

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
CLAIM_HOURLY_LIMIT_ENV = "TOKEN_CLAIM_HOURLY_LIMIT"
CLAIM_BATCH_LIMIT_ENV = "TOKEN_CLAIM_BATCH_LIMIT"
TOKEN_MAX_CLAIMS_ENV = "TOKEN_MAX_CLAIMS_PER_TOKEN"
APIKEY_MAX_PER_USER_ENV = "TOKEN_APIKEY_MAX_PER_USER"
APIKEY_RATE_PER_MIN_ENV = "TOKEN_APIKEY_RATE_LIMIT_PER_MINUTE"
PROVIDER_BASE_URL_ENV = "TOKEN_PROVIDER_BASE_URL"
SESSION_COOKIE_NAME = "token_atlas_session"
SESSION_AUTH_KEY = "auth"
SESSION_OAUTH_STATE_KEY = "linuxdo_oauth_state"
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


class TokenDb:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.RLock()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
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
                    is_available INTEGER NOT NULL,
                    claim_count INTEGER NOT NULL,
                    max_claims INTEGER NOT NULL,
                    created_at_ts INTEGER NOT NULL,
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

                CREATE INDEX IF NOT EXISTS idx_token_claims_user_time
                    ON token_claims(user_id, claimed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_token_claims_api_time
                    ON token_claims(api_key_id, claimed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_claim_queue_status_time
                    ON claim_queue(status, enqueued_at_ts, id);
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
        max_claims = get_max_claims_per_token()

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
                    "SELECT id, claim_count FROM tokens WHERE file_name = ?",
                    (file_name,),
                ).fetchone()
                if row:
                    claim_count = int(row["claim_count"])
                    is_available = 1 if claim_count < max_claims else 0
                    conn.execute(
                        """
                        UPDATE tokens
                        SET file_path = ?,
                            file_hash = ?,
                            encoding = ?,
                            content_json = ?,
                            max_claims = ?,
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
                            max_claims,
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
                            is_available,
                            claim_count,
                            max_claims,
                            created_at_ts,
                            updated_at_ts,
                            last_seen_at_ts
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            file_name,
                            file_path,
                            file_hash,
                            encoding,
                            content_json,
                            1,
                            0,
                            max_claims,
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
                    SET is_available = 0,
                        last_seen_at_ts = ?
                    WHERE file_name NOT IN ({placeholders})
                    """,
                    (now, *seen_names),
                )
            else:
                conn.execute(
                    "UPDATE tokens SET is_available = 0, last_seen_at_ts = ?",
                    (now,),
                )

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

        return {
            "id": user_id,
            "linuxdo_user_id": str(user.id),
            "username": user.username,
            "name": user.name or user.username,
            "trust_level": user.trust_level,
        }

    def get_user(self, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT * FROM users WHERE id = ?",
                (user_id,),
            ).fetchone()
            if not row:
                return None
            return dict(row)

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

        return {
            "id": api_key_id,
            "name": name,
            "prefix": prefix,
            "token": api_key,
            "created_at": isoformat_from_ts(now),
        }

    def revoke_api_key(self, user_id: int, api_key_id: int) -> None:
        now = now_ts()
        with self._lock, self.connect() as conn:
            conn.execute(
                """
                UPDATE api_keys
                SET status = 'revoked', revoked_at_ts = ?
                WHERE id = ? AND user_id = ?
                """,
                (now, api_key_id, user_id),
            )

    def resolve_api_key(self, api_key: str) -> dict[str, Any] | None:
        key_hash = hash_api_key(api_key)
        now = now_ts()
        with self._lock, self.connect() as conn:
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
            conn.execute(
                "UPDATE api_keys SET last_used_at_ts = ? WHERE id = ?",
                (now, row["api_key_id"]),
            )
        return {"api_key_id": int(row["api_key_id"]), "user_id": int(row["user_id"])}

    def get_quota_usage(self, user_id: int) -> dict[str, int]:
        now = now_ts()
        cutoff = now - 3600
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM token_claims
                WHERE user_id = ? AND claimed_at_ts >= ?
                """,
                (user_id, cutoff),
            ).fetchone()
        used = int(row["cnt"]) if row else 0
        limit = get_claim_hourly_limit()
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
            total_tokens = conn.execute("SELECT COUNT(*) as cnt FROM tokens").fetchone()
            available_tokens = conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_available = 1 AND claim_count < max_claims"
            ).fetchone()
            claimed_total = conn.execute("SELECT COUNT(*) as cnt FROM token_claims").fetchone()
            claimed_unique = conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE claim_count > 0"
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
        batch_limit = get_claim_batch_limit()
        requested = min(requested, batch_limit)
        now = now_ts()
        cutoff = now - 3600
        request_id = secrets.token_hex(8)

        with self._lock, self.connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            used_row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM token_claims
                WHERE user_id = ? AND claimed_at_ts >= ?
                """,
                (user_id, cutoff),
            ).fetchone()
            used = int(used_row["cnt"]) if used_row else 0
            limit = get_claim_hourly_limit()
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
                _QUEUE_STATUS_CACHE.pop(user_id, None)
                return {
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

            available_row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM tokens
                WHERE is_available = 1 AND claim_count < max_claims
                """
            ).fetchone()
            available = int(available_row["cnt"]) if available_row else 0
            if available < target:
                queued = claim_queue.enqueue_claim(self, user_id, api_key_id, target, conn=conn)
                _QUEUE_STATUS_CACHE.pop(user_id, None)
                return {
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

            rows = conn.execute(
                """
                SELECT id, file_name, file_path, encoding, content_json, claim_count, max_claims
                FROM tokens
                WHERE is_available = 1 AND claim_count < max_claims
                ORDER BY created_at_ts ASC, id ASC
                LIMIT ?
                """,
                (target,),
            ).fetchall()

            if not rows:
                queued = claim_queue.enqueue_claim(self, user_id, api_key_id, target, conn=conn)
                _QUEUE_STATUS_CACHE.pop(user_id, None)
                return {
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

        return {
            "request_id": request_id,
            "items": items,
            "requested": target,
            "granted": len(items),
            "queued": False,
            "quota": {"used": new_used, "limit": limit, "remaining": new_remaining},
        }

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
                SELECT COUNT(*) as cnt
                FROM tokens
                WHERE is_available = 1 AND claim_count < max_claims
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

class TokenIndexStore:
    def __init__(self, token_dir: Path) -> None:
        self.token_dir = token_dir
        self._lock = threading.RLock()
        self._items: list[TokenDocument] = []
        self._updated_at = isoformat_now()

    def refresh_all(self) -> None:
        files = sorted(self.token_dir.glob("*.json"), key=lambda path: path.name.lower())
        documents: list[TokenDocument] = []

        for path in files:
            document = self._load_document(path, len(documents))
            if document is not None:
                documents.append(document)

        with self._lock:
            self._items = documents
            self._updated_at = isoformat_now()

    def list_index(self) -> dict[str, Any]:
        with self._lock:
            return {
                "count": len(self._items),
                "updated_at": self._updated_at,
                "items": [item.to_index_payload() for item in self._items],
            }

    def resolve(
        self,
        *,
        name: str | None = None,
        item_id: str | None = None,
        index: int | None = None,
    ) -> TokenDocument | None:
        with self._lock:
            if name:
                for item in self._items:
                    if item.name == name:
                        return item
                return None

            if item_id:
                for item in self._items:
                    if item.id == item_id:
                        return item
                return None

            if index is None:
                return None

            if 0 <= index < len(self._items):
                return self._items[index]

            return None

    def list_documents(self, names: list[str] | None = None) -> list[TokenDocument]:
        with self._lock:
            if not names:
                return list(self._items)

            wanted = set(names)
            return [item for item in self._items if item.name in wanted]

    def _load_document(self, path: Path, index: int) -> TokenDocument | None:
        try:
            raw = path.read_bytes()
            stat = path.stat()
        except OSError:
            return None

        encoding = detect_encoding(raw)
        relative_path = path.relative_to(BASE_DIR).as_posix()
        item_id = path.stem

        try:
            decoded = raw.decode(encoding)
            content = json.loads(decoded)
            keys = sorted(content.keys()) if isinstance(content, dict) else []
            error = None
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            content = None
            keys = []
            error = str(exc)

        return TokenDocument(
            index=index,
            id=item_id,
            name=path.name,
            path=relative_path,
            size=stat.st_size,
            mtime=isoformat_timestamp(stat.st_mtime),
            encoding=encoding,
            keys=keys,
            error=error,
            content=content,
        )


class TokenDirectoryEventHandler(FileSystemEventHandler):
    _DEBOUNCE_SECONDS = 2.0

    def __init__(self, store: TokenIndexStore, db: TokenDb) -> None:
        self.store = store
        self.db = db
        self._timer: threading.Timer | None = None
        self._lock = threading.Lock()

    def _do_sync(self) -> None:
        self.store.refresh_all()
        self.db.sync_tokens(TOKEN_DIR)
        try:
            try_fulfill_queue(self.db)
        except Exception:
            pass

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        paths = [getattr(event, "src_path", ""), getattr(event, "dest_path", "")]
        if not any(path.lower().endswith(".json") for path in paths if path):
            return

        with self._lock:
            if self._timer is not None:
                self._timer.cancel()
            self._timer = threading.Timer(self._DEBOUNCE_SECONDS, self._do_sync)
            self._timer.daemon = True
            self._timer.start()


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


def get_claim_hourly_limit() -> int:
    return max(1, env_int(CLAIM_HOURLY_LIMIT_ENV, 50))


def get_claim_batch_limit() -> int:
    return max(1, env_int(CLAIM_BATCH_LIMIT_ENV, 50))


def get_max_claims_per_token() -> int:
    return max(1, env_int(TOKEN_MAX_CLAIMS_ENV, 1))


def get_apikey_max_per_user() -> int:
    return max(1, env_int(APIKEY_MAX_PER_USER_ENV, 5))


def get_apikey_rate_per_minute() -> int:
    return max(0, env_int(APIKEY_RATE_PER_MIN_ENV, 60))


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
        },
    }


def clear_auth_session(request: Request) -> None:
    request.session.pop(SESSION_AUTH_KEY, None)
    request.session.pop(SESSION_OAUTH_STATE_KEY, None)


def clear_auth_cookie(request: Request, response: Response) -> None:
    clear_auth_session(request)
    response.delete_cookie(SESSION_COOKIE_NAME)


def get_session_auth(request: Request) -> dict[str, Any] | None:
    session_auth = request.session.get(SESSION_AUTH_KEY)
    if isinstance(session_auth, dict):
        return session_auth
    return None


def require_session_user(request: Request) -> dict[str, Any]:
    auth = get_session_auth(request)
    if not auth or auth.get("method") != "linuxdo":
        raise PermissionError
    user_id = auth.get("user_id")
    if user_id is None:
        raise PermissionError
    return {"user_id": int(user_id), "user": auth.get("user")}


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
store = TokenIndexStore(TOKEN_DIR)
observer: Observer | None = None
_QUEUE_FULFILL_LOCK = threading.Lock()
_QUEUE_LAST_FULFILL_TS = 0.0
_QUEUE_FULFILL_THROTTLE_SEC = 5.0
_STATS_CACHE: dict[str, Any] = {"ts": 0.0, "value": None}
_QUEUE_STATUS_CACHE: dict[int, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL_SEC = 5.0
_QUEUE_PUMP_THREAD: threading.Thread | None = None
_QUEUE_PUMP_STOP = threading.Event()
_QUEUE_PUMP_INTERVAL_SEC = 20.0


def try_fulfill_queue(db_handle: TokenDb) -> None:
    global _QUEUE_LAST_FULFILL_TS
    now = time.time()
    with _QUEUE_FULFILL_LOCK:
        if now - _QUEUE_LAST_FULFILL_TS < _QUEUE_FULFILL_THROTTLE_SEC:
            return
        _QUEUE_LAST_FULFILL_TS = now
    claim_queue.fulfill_queue(
        db_handle,
        hourly_limit=get_claim_hourly_limit(),
        apikey_rate_limit=get_apikey_rate_per_minute(),
    )


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


@asynccontextmanager
async def lifespan(_: FastAPI):
    global observer

    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    db.init_db()
    db.sync_tokens(TOKEN_DIR)
    store.refresh_all()

    event_handler = TokenDirectoryEventHandler(store, db)
    observer = Observer()
    observer.schedule(event_handler, str(TOKEN_DIR), recursive=False)
    observer.start()

    _QUEUE_PUMP_STOP.clear()
    global _QUEUE_PUMP_THREAD
    _QUEUE_PUMP_THREAD = threading.Thread(
        target=_queue_pump_loop,
        daemon=True,
        name="claim-queue-pump",
    )
    _QUEUE_PUMP_THREAD.start()

    try:
        yield
    finally:

        if observer is None:
            return

        observer.stop()
        observer.join(timeout=5)
        observer = None
        _QUEUE_PUMP_STOP.set()
        if _QUEUE_PUMP_THREAD is not None:
            _QUEUE_PUMP_THREAD.join(timeout=5)
            _QUEUE_PUMP_THREAD = None


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
    payload = {
        "authenticated": auth is not None,
        "method": auth.get("method") if auth else None,
        "user": auth.get("user") if auth else None,
        "oauth": {
            "linuxdo_enabled": is_linuxdo_enabled(),
            "linuxdo_login_url": "/auth/linuxdo/login" if is_linuxdo_enabled() else None,
        },
    }
    return JSONResponse(payload, headers={"Cache-Control": "no-store"})


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
        },
        "quota": quota,
        "claims": totals,
        "api_keys": {
            "limit": get_apikey_max_per_user(),
            "active": len([key for key in api_keys if key["status"] == "active"]),
        },
    }


@app.get("/dashboard/stats")
def get_dashboard_stats(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    cached = _STATS_CACHE.get("value")
    ts = float(_STATS_CACHE.get("ts") or 0.0)
    if cached is not None and _cache_fresh(ts):
        return cached
    stats = db.get_dashboard_stats(user_id)
    _STATS_CACHE["value"] = stats
    _STATS_CACHE["ts"] = time.time()
    return stats


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
        session = get_session_auth(request)
        if session and session.get("method") == "linuxdo":
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
def start_linuxdo_login(request: Request) -> Response:
    if not is_linuxdo_enabled():
        error_json = json.dumps({"detail": "Linux.do OAuth is not configured."}).encode("utf-8")
        return Response(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content=error_json,
            media_type="application/json",
        )

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

    if error:
        response = RedirectResponse(
            url=f"/?auth_error={urllib.parse.quote(error)}",
            status_code=status.HTTP_302_FOUND,
        )
        clear_auth_cookie(request, response)
        return response

    if not is_linuxdo_enabled():
        response = RedirectResponse(
            url="/?auth_error=linuxdo_not_configured",
            status_code=status.HTTP_302_FOUND,
        )
        clear_auth_cookie(request, response)
        return response

    if not code or not state or state != expected_state:
        response = RedirectResponse(
            url="/?auth_error=invalid_oauth_state",
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
            url="/?auth_error=linuxdo_login_failed",
            status_code=status.HTTP_302_FOUND,
        )
        clear_auth_cookie(request, response)
        return response

    base_url = get_provider_base_url()
    if base_url:
        return RedirectResponse(url=f"{base_url}/?auth=linuxdo", status_code=status.HTTP_302_FOUND)
    return RedirectResponse(url="/?auth=linuxdo", status_code=status.HTTP_302_FOUND)


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
    snapshot = store.list_index()
    return {
        "status": "ok",
        "token_count": snapshot["count"],
        "updated_at": snapshot["updated_at"],
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
