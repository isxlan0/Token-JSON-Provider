from __future__ import annotations

import base64
import binascii
from contextlib import asynccontextmanager
import hashlib
import heapq
import itertools
import json
import io
import os
import queue
import secrets
import sqlite3
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

from fastapi import Body, Depends, FastAPI, Header, Query, Request, Response, status
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from starlette.middleware.sessions import SessionMiddleware
import claim_queue
import codex_probe
try:
    from watchdog.events import FileSystemEvent, FileSystemEventHandler, FileMovedEvent
    from watchdog.observers import Observer
except Exception:
    FileSystemEvent = Any  # type: ignore[assignment]
    FileMovedEvent = Any  # type: ignore[assignment]
    FileSystemEventHandler = object  # type: ignore[assignment]
    Observer = None

try:
    import redis
except Exception:
    redis = None

BASE_DIR = Path(__file__).resolve().parent
TOKEN_DIR = BASE_DIR / "token"
STATIC_DIR = BASE_DIR / "static"
ENV_FILE = BASE_DIR / ".env"


def _format_runtime_log(tag: str, message: str) -> str:
    return f"[{time.strftime('%H:%M:%S')}] [{tag}] {message}"


def startup_log(message: str) -> None:
    print(_format_runtime_log("startup", message), flush=True)


def token_import_log(message: str) -> None:
    print(_format_runtime_log("token-import", message), flush=True)


def upload_log(message: str) -> None:
    print(_format_runtime_log("upload", message), flush=True)


def count_token_files(token_dir: Path) -> int | None:
    try:
        with os.scandir(token_dir) as entries:
            return sum(1 for entry in entries if entry.is_file() and entry.name.lower().endswith(".json"))
    except OSError:
        return None


def list_token_json_names(token_dir: Path) -> list[str]:
    try:
        with os.scandir(token_dir) as entries:
            return sorted(
                entry.name
                for entry in entries
                if entry.is_file() and entry.name.lower().endswith(".json")
            )
    except OSError:
        return []


def list_token_json_stats(token_dir: Path) -> dict[str, tuple[int, int]] | None:
    try:
        with os.scandir(token_dir) as entries:
            stats: dict[str, tuple[int, int]] = {}
            for entry in entries:
                if not entry.is_file() or not entry.name.lower().endswith(".json"):
                    continue
                stat = entry.stat()
                stats[entry.name] = (int(stat.st_mtime_ns), int(stat.st_size))
            return stats
    except OSError:
        return None

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
CACHE_BACKEND_ENV = "TOKEN_CACHE_BACKEND"
REDIS_URL_ENV = "TOKEN_REDIS_URL"
REDIS_USERNAME_ENV = "TOKEN_REDIS_USERNAME"
REDIS_PASSWORD_ENV = "TOKEN_REDIS_PASSWORD"
REDIS_PREFIX_ENV = "TOKEN_REDIS_PREFIX"
CACHE_DEFAULT_TTL_ENV = "TOKEN_CACHE_DEFAULT_TTL_SEC"
CACHE_ME_TTL_ENV = "TOKEN_CACHE_ME_TTL_SEC"
CACHE_CLAIMS_TTL_ENV = "TOKEN_CACHE_CLAIMS_TTL_SEC"
CACHE_ADMIN_TTL_ENV = "TOKEN_CACHE_ADMIN_TTL_SEC"
CACHE_QUEUE_TTL_ENV = "TOKEN_CACHE_QUEUE_TTL_SEC"
CACHE_DASHBOARD_TTL_ENV = "TOKEN_CACHE_DASHBOARD_TTL_SEC"
TOKEN_CODEX_PROBE_DELAY_SEC_ENV = "TOKEN_CODEX_PROBE_DELAY_SEC"
TOKEN_CODEX_PROBE_TIMEOUT_SEC_ENV = "TOKEN_CODEX_PROBE_TIMEOUT_SEC"
TOKEN_CODEX_PROBE_RESERVE_SEC_ENV = "TOKEN_CODEX_PROBE_RESERVE_SEC"
TOKEN_UPLOAD_MAX_FILES_ENV = "TOKEN_UPLOAD_MAX_FILES_PER_REQUEST"
TOKEN_UPLOAD_MAX_FILE_SIZE_ENV = "TOKEN_UPLOAD_MAX_FILE_SIZE_BYTES"
TOKEN_UPLOAD_MAX_SUCCESS_PER_HOUR_ENV = "TOKEN_UPLOAD_MAX_SUCCESS_PER_HOUR"
SESSION_COOKIE_NAME = "token_atlas_session"
SESSION_AUTH_KEY = "auth"
SESSION_OAUTH_STATE_KEY = "linuxdo_oauth_state"
SESSION_POST_LOGIN_REDIRECT_KEY = "post_login_redirect"
LINUXDO_AUTHORIZE_URL = "https://connect.linux.do/oauth2/authorize"
LINUXDO_TOKEN_URL = "https://connect.linux.do/oauth2/token"
LINUXDO_USER_URL = "https://connect.linux.do/api/user"
REQUIRED_UPLOAD_FIELDS = ("access_token", "refresh_token", "account_id")


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


def env_float(name: str, default: float) -> float:
    raw = env_value(name)
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def get_non_healthy_max_claims_scope() -> str:
    raw = env_value(TOKEN_NON_HEALTHY_MAX_CLAIMS_SCOPE_ENV, "all_unfinished").strip().lower()
    if raw in {"all_unfinished", "new_only", "unclaimed_only"}:
        return raw
    return "all_unfinished"


def normalize_username(value: str) -> str:
    return value.strip().lstrip("@").strip().lower()


def normalize_uploaded_value(value: Any) -> str:
    return str(value or "").strip()


def hash_token_value(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


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


class TokenCleanupPayload(BaseModel):
    mode: str = "files_only"


class UploadResultItem(BaseModel):
    request_index: int | None = None
    file_name: str
    status: str
    reason: str | None = None
    token_id: int | None = None
    account_id: str | None = None


class UploadTokenFilePayload(BaseModel):
    name: str
    content_base64: str


class UploadTokensPayload(BaseModel):
    files: list[UploadTokenFilePayload] = Field(default_factory=list)


@dataclass
class QueuedUploadTask:
    batch_id: str
    user_id: int
    provider_username: str
    provider_name: str
    hourly_limit: int
    request_index: int
    file_name: str
    content_json: str
    account_id: str
    access_token: str
    refresh_token: str
    access_token_hash: str


@dataclass
class HideClaimsTask:
    user_id: int
    claim_ids: list[int]


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


class UploadValidationError(ValueError):
    pass


def _extract_upload_candidate(content: Any) -> dict[str, Any]:
    if not isinstance(content, dict):
        raise UploadValidationError("JSON 根节点必须是对象")
    storage = content.get("storage")
    if isinstance(storage, dict):
        return storage
    return content


def normalize_upload_content(content: Any) -> dict[str, Any]:
    candidate = _extract_upload_candidate(content)
    normalized_required = {
        key: normalize_uploaded_value(candidate.get(key))
        for key in REQUIRED_UPLOAD_FIELDS
    }
    missing = [key for key, value in normalized_required.items() if not value]
    if missing:
        raise UploadValidationError(f"缺少必填字段：{', '.join(missing)}")
    normalized = dict(candidate)
    normalized.update(normalized_required)
    return normalized


def load_uploaded_json(raw: bytes) -> tuple[str, dict[str, Any], str]:
    encoding = detect_encoding(raw)
    decoded = raw.decode(encoding)
    parsed = json.loads(decoded.lstrip("\ufeff"))
    normalized = normalize_upload_content(parsed)
    content_json = json.dumps(normalized, ensure_ascii=False, separators=(",", ":"))
    return "utf-8", normalized, content_json


def build_uploaded_filename(*, account_id: str, uploaded_at_ts: int) -> str:
    hash_prefix = hashlib.sha256(account_id.encode("utf-8")).hexdigest()[:8]
    random_suffix = secrets.token_hex(3)
    return f"upload-{uploaded_at_ts}-{hash_prefix}-{random_suffix}.json"


class CacheBackend(ABC):
    @abstractmethod
    def get_text(self, key: str) -> str | None:
        raise NotImplementedError

    @abstractmethod
    def set_text(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    def delete(self, key: str) -> None:
        raise NotImplementedError

    @abstractmethod
    def incr(self, key: str) -> int:
        raise NotImplementedError

    def get_json(self, key: str) -> Any | None:
        raw = self.get_text(key)
        if raw is None:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            self.delete(key)
            return None

    def set_json(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        self.set_text(key, json.dumps(value, ensure_ascii=False), ttl_sec=ttl_sec)


class MemoryCacheBackend(CacheBackend):
    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._entries: dict[str, tuple[float | None, str]] = {}

    def get_text(self, key: str) -> str | None:
        now = time.time()
        with self._lock:
            entry = self._entries.get(key)
            if entry is None:
                return None
            expires_at, value = entry
            if expires_at is not None and expires_at <= now:
                self._entries.pop(key, None)
                return None
            return value

    def set_text(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        expires_at = None if ttl_sec is None else time.time() + max(1, int(ttl_sec))
        with self._lock:
            self._entries[key] = (expires_at, value)

    def delete(self, key: str) -> None:
        with self._lock:
            self._entries.pop(key, None)

    def incr(self, key: str) -> int:
        with self._lock:
            current_raw = self._entries.get(key)
            current = 0
            if current_raw is not None:
                try:
                    current = int(current_raw[1])
                except ValueError:
                    current = 0
            current += 1
            self._entries[key] = (None, str(current))
            return current


class RedisCacheBackend(CacheBackend):
    def __init__(self, url: str, prefix: str, *, username: str = "", password: str = "") -> None:
        if redis is None:
            raise RuntimeError("redis package is not installed")
        self._prefix = prefix
        client_kwargs: dict[str, Any] = {"decode_responses": True}
        if username:
            client_kwargs["username"] = username
        if password:
            client_kwargs["password"] = password
        self._client = redis.Redis.from_url(url, **client_kwargs)
        self._client.ping()

    def _full_key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def get_text(self, key: str) -> str | None:
        value = self._client.get(self._full_key(key))
        return str(value) if value is not None else None

    def set_text(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        full_key = self._full_key(key)
        if ttl_sec is None:
            self._client.set(full_key, value)
            return
        self._client.setex(full_key, max(1, int(ttl_sec)), value)

    def delete(self, key: str) -> None:
        self._client.delete(self._full_key(key))

    def incr(self, key: str) -> int:
        return int(self._client.incr(self._full_key(key)))


class AppCache:
    def __init__(self) -> None:
        self._backend_name = "memory"
        self._backend: CacheBackend = MemoryCacheBackend()
        self._lock = threading.RLock()

    @property
    def backend_name(self) -> str:
        return self._backend_name

    def configure(self, *, emit_log: bool = False) -> None:
        mode = env_value(CACHE_BACKEND_ENV, "auto").strip().lower() or "auto"
        prefix = env_value(REDIS_PREFIX_ENV, "token_index:")
        redis_url = env_value(REDIS_URL_ENV, "")
        redis_username = env_value(REDIS_USERNAME_ENV, "")
        redis_password = env_value(REDIS_PASSWORD_ENV, "")
        backend_name = "memory"
        backend: CacheBackend = MemoryCacheBackend()
        log_message = "[cache] using in-memory cache backend"

        if mode in {"auto", "redis"} and redis_url:
            try:
                backend = RedisCacheBackend(
                    redis_url,
                    prefix,
                    username=redis_username,
                    password=redis_password,
                )
                backend_name = "redis"
                log_message = (
                    f"[cache] Redis connected successfully: backend=redis url={redis_url} "
                    f"prefix={prefix} username={'<set>' if redis_username else '<empty>'}"
                )
            except Exception as exc:
                log_message = (
                    f"[cache] Redis connection failed: backend={mode} url={redis_url} "
                    f"reason={exc}. Falling back to in-memory cache."
                )
                if mode == "redis":
                    backend_name = "memory"
        elif mode == "redis":
            log_message = "[cache] Redis backend requested but TOKEN_REDIS_URL is empty. Falling back to in-memory cache."
        elif mode == "memory":
            log_message = "[cache] using in-memory cache backend (forced by TOKEN_CACHE_BACKEND=memory)"

        with self._lock:
            self._backend = backend
            self._backend_name = backend_name
        if emit_log:
            print(log_message, flush=True)

    def get_json(self, key: str) -> Any | None:
        with self._lock:
            return self._backend.get_json(key)

    def set_json(self, key: str, value: Any, ttl_sec: int | None = None) -> None:
        with self._lock:
            self._backend.set_json(key, value, ttl_sec=ttl_sec)

    def get_text(self, key: str) -> str | None:
        with self._lock:
            return self._backend.get_text(key)

    def set_text(self, key: str, value: str, ttl_sec: int | None = None) -> None:
        with self._lock:
            self._backend.set_text(key, value, ttl_sec=ttl_sec)

    def delete(self, key: str) -> None:
        with self._lock:
            self._backend.delete(key)

    def incr(self, key: str) -> int:
        with self._lock:
            return self._backend.incr(key)


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
        self._user_other_unique_tokens: dict[int, int] = {}
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
        user_other_unique_tokens = self._build_user_other_unique_tokens(token_claimers)

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
            self._user_other_unique_tokens = user_other_unique_tokens
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
            others_unique = self._user_other_unique_tokens.get(user_id)
            if others_unique is None:
                self._user_other_unique_tokens = self._build_user_other_unique_tokens(self._token_claimers)
                others_unique = self._user_other_unique_tokens.get(user_id, len(self._token_claimers))
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

    def _build_user_other_unique_tokens(self, token_claimers: dict[int, set[int]]) -> dict[int, int]:
        user_other_unique_tokens: dict[int, int] = {}
        for claimers in token_claimers.values():
            if not claimers:
                continue
            if len(claimers) == 1:
                only_user = next(iter(claimers))
                user_other_unique_tokens.setdefault(int(only_user), 0)
                continue
            for claimer_id in claimers:
                user_other_unique_tokens[int(claimer_id)] = user_other_unique_tokens.get(int(claimer_id), 0) + 1
        return user_other_unique_tokens

    def record_claim(
        self,
        *,
        user: dict[str, Any],
        request_id: str,
        claimed_at_ts: int,
        token_ids: list[int],
        first_claim_count: int,
        granted: int,
    ) -> None:
        if granted <= 0:
            return
        user_id = int(user["id"])
        event = {
            "request_id": request_id,
            "user_id": user_id,
            "public_user_id": user["linuxdo_user_id"],
            "username": user["linuxdo_username"],
            "name": user["linuxdo_name"] or user["linuxdo_username"],
            "claimed_at_ts": claimed_at_ts,
            "count": granted,
        }
        cutoff = now_ts() - _CLAIM_EVENT_WINDOW_SEC
        with self._lock:
            self._claim_events = [item for item in self._claim_events if int(item["claimed_at_ts"]) >= cutoff]
            self._claim_events.append(event)
            self._total_claims += granted
            self._inventory["available"] = max(0, int(self._inventory["available"]) - granted)
            self._inventory["unclaimed"] = max(0, int(self._inventory["unclaimed"]) - first_claim_count)
            self._user_total_claims[user_id] = self._user_total_claims.get(user_id, 0) + granted
            claimed_tokens = self._user_unique_tokens.setdefault(user_id, set())
            for raw_token_id in token_ids:
                token_id = int(raw_token_id)
                before_claimers = self._token_claimers.setdefault(token_id, set())
                was_new_for_user = user_id not in before_claimers
                if was_new_for_user:
                    claimed_tokens.add(token_id)
                before_claimers.add(user_id)
            # Safer than incrementally maintaining another derived projection across every write path.
            self._user_other_unique_tokens = {}
            self._invalidate_derived_locked()

    def set_queue_total(self, total: int) -> None:
        with self._lock:
            self._queue = {"total": max(0, int(total))}

    def adjust_queue_total(self, delta: int) -> None:
        with self._lock:
            current = int(self._queue.get("total", 0))
            self._queue = {"total": max(0, current + int(delta))}


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
                );

                CREATE TABLE IF NOT EXISTS token_claims (
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
                );

                CREATE TABLE IF NOT EXISTS user_token_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    token_id INTEGER NOT NULL,
                    first_claim_id INTEGER NOT NULL,
                    created_at_ts INTEGER NOT NULL,
                    UNIQUE(user_id, token_id),
                    FOREIGN KEY(user_id) REFERENCES users(id),
                    FOREIGN KEY(token_id) REFERENCES tokens(id),
                    FOREIGN KEY(first_claim_id) REFERENCES token_claims(id)
                );

                CREATE TABLE IF NOT EXISTS claim_queue (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    api_key_id INTEGER,
                    requested INTEGER NOT NULL,
                    remaining INTEGER NOT NULL,
                    queue_rank INTEGER NOT NULL,
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

                CREATE TABLE IF NOT EXISTS queue_runtime (
                    id INTEGER PRIMARY KEY CHECK(id = 1),
                    total_queued INTEGER NOT NULL,
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
                """
            )
            claims_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(token_claims)").fetchall()
            }
            if "is_hidden" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN is_hidden INTEGER NOT NULL DEFAULT 0")
            if "claim_file_name" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN claim_file_name TEXT")
            if "claim_file_path" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN claim_file_path TEXT")
            if "claim_encoding" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN claim_encoding TEXT")
            if "claim_content_json" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN claim_content_json TEXT")
            if "provider_user_id" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN provider_user_id TEXT")
            if "provider_username" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN provider_username TEXT")
            if "provider_name" not in claims_columns:
                conn.execute("ALTER TABLE token_claims ADD COLUMN provider_name TEXT")
            columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(api_keys)").fetchall()
            }
            if "key_value" not in columns:
                conn.execute("ALTER TABLE api_keys ADD COLUMN key_value TEXT")
            queue_columns = {
                row["name"]
                for row in conn.execute("PRAGMA table_info(claim_queue)").fetchall()
            }
            if "queue_rank" not in queue_columns:
                conn.execute("ALTER TABLE claim_queue ADD COLUMN queue_rank INTEGER NOT NULL DEFAULT 0")
                queue_rows = conn.execute(
                    """
                    SELECT id
                    FROM claim_queue
                    WHERE status = 'queued' AND remaining > 0
                    ORDER BY enqueued_at_ts ASC, id ASC
                    """
                ).fetchall()
                for rank, row in enumerate(queue_rows, start=1):
                    conn.execute(
                        "UPDATE claim_queue SET queue_rank = ? WHERE id = ?",
                        (rank, int(row["id"])),
                    )
            queue_total_row = conn.execute(
                "SELECT COUNT(*) as cnt FROM claim_queue WHERE status = 'queued' AND remaining > 0"
            ).fetchone()
            conn.execute(
                """
                INSERT INTO queue_runtime (id, total_queued, updated_at_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    total_queued = excluded.total_queued,
                    updated_at_ts = excluded.updated_at_ts
                """,
                (int(queue_total_row["cnt"]) if queue_total_row else 0, now_ts()),
            )
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
            if "is_banned" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN is_banned INTEGER NOT NULL DEFAULT 0")
            if "banned_at_ts" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN banned_at_ts INTEGER")
            if "ban_reason" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN ban_reason TEXT")
            if "cleaned_at_ts" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN cleaned_at_ts INTEGER")
            if "last_probe_at_ts" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN last_probe_at_ts INTEGER")
            if "last_probe_status" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN last_probe_status TEXT")
            if "probe_lock_until_ts" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN probe_lock_until_ts INTEGER")
            if "account_id" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN account_id TEXT")
            if "access_token_hash" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN access_token_hash TEXT")
            if "provider_user_id" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN provider_user_id TEXT")
            if "provider_username" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN provider_username TEXT")
            if "provider_name" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN provider_name TEXT")
            if "uploaded_at_ts" not in columns:
                conn.execute("ALTER TABLE tokens ADD COLUMN uploaded_at_ts INTEGER")
            conn.execute(
                """
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
                """
            )
            conn.execute(
                """
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
                """
            )
            token_rows = conn.execute(
                """
                SELECT id, content_json
                FROM tokens
                WHERE (account_id IS NULL OR account_id = '')
                   OR (access_token_hash IS NULL OR access_token_hash = '')
                """
            ).fetchall()
            for row in token_rows:
                try:
                    content = json.loads(str(row["content_json"]))
                    normalized = normalize_upload_content(content)
                except (TypeError, ValueError, json.JSONDecodeError):
                    continue
                conn.execute(
                    """
                    UPDATE tokens
                    SET account_id = COALESCE(NULLIF(account_id, ''), ?),
                        access_token_hash = COALESCE(NULLIF(access_token_hash, ''), ?)
                    WHERE id = ?
                    """,
                    (
                        normalized["account_id"],
                        hash_token_value(normalized["access_token"]),
                        int(row["id"]),
                    ),
                )
            conn.execute(
                """
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
                """
            )
            conn.execute(
                """
                UPDATE token_claims
                SET is_hidden = 1
                WHERE EXISTS (
                    SELECT 1
                    FROM user_token_claims
                    WHERE user_token_claims.user_id = token_claims.user_id
                      AND user_token_claims.token_id = token_claims.token_id
                      AND user_token_claims.first_claim_id <> token_claims.id
                )
                """
            )
            # Create indexes only after schema backfill so legacy databases
            # without the newer columns can migrate successfully.
            conn.executescript(
                """
                CREATE INDEX IF NOT EXISTS idx_token_claims_user_time
                    ON token_claims(user_id, claimed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_token_claims_api_time
                    ON token_claims(api_key_id, claimed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_token_claims_user_hidden_time
                    ON token_claims(user_id, is_hidden, claimed_at_ts DESC, id DESC);
                CREATE INDEX IF NOT EXISTS idx_token_claims_user_token_hidden_time
                    ON token_claims(user_id, token_id, is_hidden, claimed_at_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_token_claims_request_user_time
                    ON token_claims(request_id, user_id, claimed_at_ts);
                CREATE INDEX IF NOT EXISTS idx_user_token_claims_user
                    ON user_token_claims(user_id, token_id);
                CREATE INDEX IF NOT EXISTS idx_claim_queue_status_time
                    ON claim_queue(status, enqueued_at_ts, id);
                CREATE INDEX IF NOT EXISTS idx_claim_queue_user_status_remaining_time
                    ON claim_queue(user_id, status, remaining, enqueued_at_ts, id);
                CREATE INDEX IF NOT EXISTS idx_claim_queue_status_remaining_time
                    ON claim_queue(status, remaining, enqueued_at_ts, id);
                CREATE INDEX IF NOT EXISTS idx_claim_queue_status_rank
                    ON claim_queue(status, queue_rank);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tokens_account_id_unique
                    ON tokens(account_id)
                    WHERE account_id IS NOT NULL AND account_id != '';
                CREATE UNIQUE INDEX IF NOT EXISTS idx_tokens_access_token_hash_unique
                    ON tokens(access_token_hash)
                    WHERE access_token_hash IS NOT NULL AND access_token_hash != '';
                CREATE INDEX IF NOT EXISTS idx_token_claims_token_user
                    ON token_claims(token_id, user_id);
                CREATE INDEX IF NOT EXISTS idx_tokens_claimable_candidates
                    ON tokens(is_active, is_enabled, is_banned, is_available, probe_lock_until_ts, claim_count, max_claims, id);
                CREATE INDEX IF NOT EXISTS idx_api_keys_user_status
                    ON api_keys(user_id, status);
                CREATE INDEX IF NOT EXISTS idx_user_bans_target_time
                    ON user_bans(linuxdo_user_id, banned_at_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_user_bans_active_lookup
                    ON user_bans(linuxdo_user_id, unbanned_at_ts, expires_at_ts, id DESC);
                """
            )

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

    def get_uploaded_token_conflict(
        self,
        *,
        account_id: str,
        access_token_hash: str,
        exclude_token_id: int | None = None,
        conn: sqlite3.Connection | None = None,
    ) -> dict[str, Any] | None:
        target_conn = conn or self.connect()
        should_close = conn is None
        try:
            params: list[Any] = [account_id, access_token_hash]
            extra_where = ""
            if exclude_token_id is not None:
                extra_where = " AND id != ?"
                params.append(int(exclude_token_id))
            row = target_conn.execute(
                f"""
                SELECT id, account_id, access_token_hash, file_name
                FROM tokens
                WHERE account_id = ?
                   OR access_token_hash = ?
                  {extra_where}
                ORDER BY id ASC
                LIMIT 1
                """,
                params,
            ).fetchone()
            return dict(row) if row else None
        finally:
            if should_close:
                target_conn.close()

    def count_recent_successful_uploads(
        self,
        provider_user_id: int,
        *,
        within_sec: int = 3600,
        conn: sqlite3.Connection | None = None,
    ) -> int:
        target_conn = conn or self.connect()
        should_close = conn is None
        try:
            row = target_conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM tokens
                WHERE provider_user_id = ?
                  AND uploaded_at_ts IS NOT NULL
                  AND uploaded_at_ts >= ?
                """,
                (str(provider_user_id), now_ts() - within_sec),
            ).fetchone()
            return int(row["cnt"]) if row else 0
        finally:
            if should_close:
                target_conn.close()

    def create_uploaded_token(
        self,
        *,
        file_name: str,
        file_path: str,
        content_json: str,
        provider: dict[str, Any],
        uploaded_at_ts: int,
        hourly_success_limit: int,
        timeout_s: float = 30.0,
    ) -> dict[str, Any]:
        normalized = normalize_upload_content(json.loads(content_json))
        file_hash = hashlib.sha256(content_json.encode("utf-8")).hexdigest()
        account_id = normalized["account_id"]
        access_token_hash = hash_token_value(normalized["access_token"])
        healthy_max_claims = get_inventory_limits()["healthy"]["max_claims"]

        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute("BEGIN IMMEDIATE")
            uploaded_count = self.count_recent_successful_uploads(
                int(provider["id"]),
                conn=conn,
            )
            if uploaded_count >= hourly_success_limit:
                raise RateLimitError("当前小时上传额度不足")
            conflict = self.get_uploaded_token_conflict(
                account_id=account_id,
                access_token_hash=access_token_hash,
                conn=conn,
            )
            if conflict:
                raise UploadValidationError("重复账号")
            cursor = conn.execute(
                """
                INSERT INTO tokens (
                    file_name,
                    file_path,
                    file_hash,
                    encoding,
                    content_json,
                    account_id,
                    access_token_hash,
                    provider_user_id,
                    provider_username,
                    provider_name,
                    uploaded_at_ts,
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
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_name,
                    file_path,
                    file_hash,
                    "utf-8",
                    content_json,
                    account_id,
                    access_token_hash,
                    str(provider["id"]),
                    provider["username"],
                    provider["name"],
                    uploaded_at_ts,
                    1,
                    0,
                    1,
                    0,
                    1,
                    0,
                    healthy_max_claims,
                    uploaded_at_ts,
                    None,
                    uploaded_at_ts,
                    uploaded_at_ts,
                ),
            )
            token_id = int(cursor.lastrowid)
            self.ensure_inventory_policy(conn=conn)
        _refresh_dashboard_memory()
        invalidate_all_runtime_cache(include_admin=True)
        return {"token_id": token_id, "account_id": account_id, "file_name": file_name}

    def _find_sync_token_conflict(
        self,
        *,
        current_token_id: int | None,
        normalized: dict[str, str],
        conn: sqlite3.Connection,
    ) -> dict[str, Any] | None:
        return self.get_uploaded_token_conflict(
            account_id=normalized["account_id"],
            access_token_hash=hash_token_value(normalized["access_token"]),
            exclude_token_id=current_token_id,
            conn=conn,
        )

    def sync_tokens(self, token_dir: Path) -> None:
        files = sorted(token_dir.glob("*.json"), key=lambda path: path.name.lower())
        seen_names: set[str] = set()
        now = now_ts()
        healthy_max_claims = get_inventory_limits()["healthy"]["max_claims"]

        with self._lock, self.connect() as conn:
            for path in files:
                try:
                    encoding, content_json, file_hash = self._load_token_file(path)
                    normalized = normalize_upload_content(json.loads(content_json))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    continue
                except UploadValidationError:
                    continue

                file_name = path.name
                file_path = path.relative_to(BASE_DIR).as_posix()
                seen_names.add(file_name)

                row = conn.execute(
                    "SELECT id, claim_count, max_claims, is_enabled, is_banned FROM tokens WHERE file_name = ?",
                    (file_name,),
                ).fetchone()
                if row:
                    conflict = self._find_sync_token_conflict(
                        current_token_id=int(row["id"]),
                        normalized=normalized,
                        conn=conn,
                    )
                    if conflict:
                        conn.execute(
                            """
                            UPDATE tokens
                            SET is_active = 0,
                                is_available = 0,
                                updated_at_ts = ?,
                                last_seen_at_ts = ?
                            WHERE id = ?
                            """,
                            (now, now, int(row["id"])),
                        )
                        continue
                    claim_count = int(row["claim_count"])
                    effective_max_claims = int(row["max_claims"])
                    is_enabled = int(row["is_enabled"]) if "is_enabled" in row.keys() else 1
                    is_banned = int(row["is_banned"]) if "is_banned" in row.keys() else 0
                    is_available = 1 if is_enabled and not is_banned and claim_count < effective_max_claims else 0
                    conn.execute(
                        """
                        UPDATE tokens
                        SET file_path = ?,
                            file_hash = ?,
                            encoding = ?,
                            content_json = ?,
                            account_id = COALESCE(NULLIF(account_id, ''), ?),
                            access_token_hash = COALESCE(NULLIF(access_token_hash, ''), ?),
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
                            normalized["account_id"],
                            hash_token_value(normalized["access_token"]),
                            is_available,
                            now,
                            now,
                            row["id"],
                        ),
                    )
                else:
                    conflict = self._find_sync_token_conflict(
                        current_token_id=None,
                        normalized=normalized,
                        conn=conn,
                    )
                    if conflict:
                        continue
                    conn.execute(
                        """
                        INSERT INTO tokens (
                            file_name,
                            file_path,
                            file_hash,
                            encoding,
                            content_json,
                            account_id,
                            access_token_hash,
                            is_active,
                            is_enabled,
                            is_banned,
                            is_available,
                            claim_count,
                            max_claims,
                            created_at_ts,
                            updated_at_ts,
                            last_seen_at_ts
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            file_name,
                            file_path,
                            file_hash,
                            encoding,
                            content_json,
                            normalized["account_id"],
                            hash_token_value(normalized["access_token"]),
                            1,
                            1,
                            0,
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
        invalidate_all_runtime_cache(include_admin=True)

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
                    normalized = normalize_upload_content(json.loads(content_json))
                except (OSError, UnicodeDecodeError, json.JSONDecodeError):
                    errors += 1
                    continue
                except UploadValidationError:
                    errors += 1
                    continue
                file_path = path.relative_to(BASE_DIR).as_posix()
                conflict = self._find_sync_token_conflict(
                    current_token_id=None,
                    normalized=normalized,
                    conn=conn,
                )
                if conflict:
                    skipped += 1
                    continue
                conn.execute(
                    """
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
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        file_name,
                        file_path,
                        file_hash,
                        encoding,
                        content_json,
                        normalized["account_id"],
                        hash_token_value(normalized["access_token"]),
                        1,
                        0,
                        1,
                        0,
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
            invalidate_all_runtime_cache(include_admin=True)
        return {"imported": imported, "skipped": skipped, "errors": errors}

    def list_token_file_names(self) -> set[str]:
        with self.connect(timeout=3.0) as conn:
            return {
                str(row["file_name"])
                for row in conn.execute("SELECT file_name FROM tokens").fetchall()
            }

    def list_token_file_states(self) -> dict[str, dict[str, int]]:
        with self.connect(timeout=3.0) as conn:
            rows = conn.execute(
                "SELECT file_name, updated_at_ts, is_active FROM tokens"
            ).fetchall()
        return {
            str(row["file_name"]): {
                "updated_at_ts": int(row["updated_at_ts"] or 0),
                "is_active": int(row["is_active"] or 0),
            }
            for row in rows
        }

    def deactivate_token_file(self, file_name: str, *, timeout: float = 3.0) -> bool:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout) as conn:
            row = conn.execute(
                "SELECT id FROM tokens WHERE file_name = ? AND is_active != 0",
                (file_name,),
            ).fetchone()
            if not row:
                return False
            conn.execute(
                """
                UPDATE tokens
                SET is_active = 0,
                    is_available = 0,
                    updated_at_ts = ?,
                    last_seen_at_ts = ?
                WHERE id = ?
                """,
                (now, now, int(row["id"])),
            )
        _refresh_dashboard_memory()
        invalidate_all_runtime_cache(include_admin=True)
        return True

    def deactivate_missing_token_files(self, existing_file_names: set[str], *, timeout: float = 3.0) -> int:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout) as conn:
            if existing_file_names:
                placeholders = ",".join("?" for _ in existing_file_names)
                cursor = conn.execute(
                    f"""
                    UPDATE tokens
                    SET is_active = 0,
                        is_available = 0,
                        updated_at_ts = ?,
                        last_seen_at_ts = ?
                    WHERE file_name NOT IN ({placeholders})
                      AND is_active != 0
                    """,
                    (now, now, *sorted(existing_file_names)),
                )
            else:
                cursor = conn.execute(
                    """
                    UPDATE tokens
                    SET is_active = 0,
                        is_available = 0,
                        updated_at_ts = ?,
                        last_seen_at_ts = ?
                    WHERE is_active != 0
                    """,
                    (now, now),
                )
            changed = int(cursor.rowcount or 0)
        if changed:
            _refresh_dashboard_memory()
            invalidate_all_runtime_cache(include_admin=True)
        return changed

    def _sync_token_file_in_conn(
        self,
        path: Path,
        *,
        conn: sqlite3.Connection,
        now: int,
    ) -> dict[str, Any]:
        file_name = path.name
        if not file_name.lower().endswith(".json"):
            return {"status": "ignored", "file_name": file_name}

        try:
            encoding, content_json, file_hash = self._load_token_file(path)
            normalized = normalize_upload_content(json.loads(content_json))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            return {"status": "error", "file_name": file_name, "reason": type(exc).__name__}
        except UploadValidationError as exc:
            return {"status": "error", "file_name": file_name, "reason": str(exc)}

        healthy_max_claims = get_inventory_limits()["healthy"]["max_claims"]
        file_path = path.relative_to(BASE_DIR).as_posix()
        existing = conn.execute(
            "SELECT id, claim_count, max_claims, is_enabled, is_banned FROM tokens WHERE file_name = ?",
            (file_name,),
        ).fetchone()
        current_token_id = int(existing["id"]) if existing else None
        conflict = self._find_sync_token_conflict(
            current_token_id=current_token_id,
            normalized=normalized,
            conn=conn,
        )
        if conflict:
            if existing:
                conn.execute(
                    """
                    UPDATE tokens
                    SET is_active = 0,
                        is_available = 0,
                        updated_at_ts = ?,
                        last_seen_at_ts = ?
                    WHERE id = ?
                    """,
                    (now, now, current_token_id),
                )
                return {"status": "deactivated", "file_name": file_name, "reason": "token_conflict"}
            return {"status": "skipped", "file_name": file_name, "reason": "token_conflict"}

        if existing:
            claim_count = int(existing["claim_count"])
            effective_max_claims = int(existing["max_claims"])
            is_enabled = int(existing["is_enabled"]) if "is_enabled" in existing.keys() else 1
            is_banned = int(existing["is_banned"]) if "is_banned" in existing.keys() else 0
            is_available = 1 if is_enabled and not is_banned and claim_count < effective_max_claims else 0
            conn.execute(
                """
                UPDATE tokens
                SET file_path = ?,
                    file_hash = ?,
                    encoding = ?,
                    content_json = ?,
                    account_id = ?,
                    access_token_hash = ?,
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
                    normalized["account_id"],
                    hash_token_value(normalized["access_token"]),
                    is_available,
                    now,
                    now,
                    current_token_id,
                ),
            )
            return {"status": "updated", "file_name": file_name}

        conn.execute(
            """
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_name,
                file_path,
                file_hash,
                encoding,
                content_json,
                normalized["account_id"],
                hash_token_value(normalized["access_token"]),
                1,
                0,
                1,
                0,
                1,
                0,
                healthy_max_claims,
                now,
                None,
                now,
                now,
            ),
        )
        return {"status": "imported", "file_name": file_name}

    def import_token_files(self, paths: list[Path], *, timeout: float = 3.0) -> list[dict[str, Any]]:
        if not paths:
            return []

        now = now_ts()
        results: list[dict[str, Any]] = []
        changed = False
        with self._lock, self.connect(timeout=timeout) as conn:
            for path in paths:
                result = self._sync_token_file_in_conn(path, conn=conn, now=now)
                results.append(result)
                if result.get("status") != "skipped":
                    changed = True
            self.ensure_inventory_policy(conn=conn)
        if changed:
            _refresh_dashboard_memory()
            invalidate_all_runtime_cache(include_admin=True)
        return results

    def import_token_file(self, path: Path, *, timeout: float = 3.0) -> dict[str, Any]:
        results = self.import_token_files([path], timeout=timeout)
        if not results:
            return {"status": "ignored", "file_name": path.name}
        return results[0]

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
        invalidate_user_profile_cache(user_id)
        invalidate_user_queue_cache(user_id)
        invalidate_dashboard_cache(user_id=user_id)
        invalidate_admin_cache()
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
        target_user = self.get_user_by_linuxdo_id(linuxdo_user_id)
        if target_user:
            target_user_id = int(target_user["id"])
            invalidate_user_cache(target_user_id)
            invalidate_user_queue_cache(target_user_id)
        invalidate_admin_cache()
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
        changed = int(result.rowcount or 0) > 0
        if changed:
            target_user = self.get_user_by_linuxdo_id(linuxdo_user_id)
            if target_user:
                target_user_id = int(target_user["id"])
                invalidate_user_cache(target_user_id)
                invalidate_user_queue_cache(target_user_id)
            invalidate_admin_cache()
        return changed

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
                SELECT token_claims.claimed_at_ts,
                       COALESCE(token_claims.claim_file_name, tokens.file_name) as file_name
                FROM token_claims
                LEFT JOIN tokens ON tokens.id = token_claims.token_id
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

    def _token_row_to_admin_payload(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": int(row["id"]),
            "file_name": row["file_name"],
            "file_path": row["file_path"],
            "encoding": row["encoding"],
            "is_active": bool(row["is_active"]),
            "is_cleaned": bool(row["is_cleaned"]),
            "is_enabled": bool(row["is_enabled"]),
            "is_banned": bool(row["is_banned"]) if "is_banned" in row.keys() else False,
            "is_available": bool(row["is_available"]),
            "claim_count": int(row["claim_count"]),
            "max_claims": int(row["max_claims"]),
            "ban_reason": row["ban_reason"] if "ban_reason" in row.keys() else None,
            "banned_at": (
                isoformat_from_ts(int(row["banned_at_ts"]))
                if "banned_at_ts" in row.keys() and row["banned_at_ts"]
                else None
            ),
            "last_probe_status": row["last_probe_status"] if "last_probe_status" in row.keys() else None,
            "last_probe_at": (
                isoformat_from_ts(int(row["last_probe_at_ts"]))
                if "last_probe_at_ts" in row.keys() and row["last_probe_at_ts"]
                else None
            ),
            "cleaned_at": isoformat_from_ts(int(row["cleaned_at_ts"])) if row["cleaned_at_ts"] else None,
            "created_at": isoformat_from_ts(int(row["created_at_ts"])),
            "updated_at": isoformat_from_ts(int(row["updated_at_ts"])),
            "last_seen_at": isoformat_from_ts(int(row["last_seen_at_ts"])),
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
            where_parts.append("is_banned = 0")
        elif status_filter == "banned":
            where_parts.append("is_banned = 1")
        elif status_filter == "disabled":
            where_parts.append("(is_active = 0 OR is_enabled = 0 OR is_banned = 1)")
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
                SELECT id, file_name, file_path, encoding, is_active, is_enabled, is_banned, is_available,
                       is_cleaned, claim_count, max_claims, banned_at_ts, ban_reason, cleaned_at_ts,
                       last_probe_at_ts, last_probe_status, created_at_ts, updated_at_ts, last_seen_at_ts
                FROM tokens
                WHERE {' AND '.join(where_parts)}
                ORDER BY is_banned ASC, is_active DESC, is_enabled DESC, updated_at_ts DESC, id DESC
                LIMIT ? OFFSET ?
                """,
                (*params, limit, offset),
            ).fetchall()
        return {
            "items": [self._token_row_to_admin_payload(row) for row in rows],
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
                        WHEN is_active = 1 AND is_banned = 0 AND ? = 1 AND claim_count < max_claims THEN 1
                        ELSE 0
                    END,
                    updated_at_ts = ?
                WHERE id = ?
                  AND (? = 0 OR is_banned = 0)
                """,
                (1 if enabled else 0, 1 if enabled else 0, now, token_id, 1 if enabled else 0),
            )
            row = conn.execute(
                """
                SELECT id, file_name, file_path, encoding, is_active, is_enabled, is_banned, is_available,
                       is_cleaned, claim_count, max_claims, banned_at_ts, ban_reason, cleaned_at_ts,
                       last_probe_at_ts, last_probe_status, created_at_ts, updated_at_ts, last_seen_at_ts
                FROM tokens
                WHERE id = ?
                """,
                (token_id,),
            ).fetchone()
            self.ensure_inventory_policy(conn=conn)
        _refresh_dashboard_memory()
        invalidate_all_runtime_cache(include_admin=True)
        if not row:
            return None
        return self._token_row_to_admin_payload(row)

    def reserve_claimable_token_for_user(self, user_id: int, *, timeout_s: float = 30.0) -> dict[str, Any] | None:
        now = now_ts()
        reserve_until = now + get_codex_probe_reserve_sec()
        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id, file_name, file_path, encoding, content_json, claim_count, max_claims
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
                """,
                (now, user_id),
            ).fetchone()
            if not row:
                return None
            cursor = conn.execute(
                """
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
                """,
                (reserve_until, now, int(row["id"]), now),
            )
            if int(cursor.rowcount or 0) <= 0:
                return None
        payload = dict(row)
        payload["content"] = json.loads(str(row["content_json"]))
        return payload

    def record_probe_status(
        self,
        token_id: int,
        status_text: str,
        *,
        clear_lock: bool = True,
        timeout_s: float = 30.0,
    ) -> None:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute(
                """
                UPDATE tokens
                SET last_probe_at_ts = ?,
                    last_probe_status = ?,
                    probe_lock_until_ts = CASE WHEN ? = 1 THEN NULL ELSE probe_lock_until_ts END,
                    updated_at_ts = ?
                WHERE id = ?
                """,
                (now, status_text, 1 if clear_lock else 0, now, token_id),
            )

    def mark_token_banned(
        self,
        token_id: int,
        *,
        reason: str = "upstream_401",
        timeout_s: float = 30.0,
    ) -> None:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                UPDATE tokens
                SET is_banned = 1,
                    is_enabled = 0,
                    is_available = 0,
                    banned_at_ts = COALESCE(banned_at_ts, ?),
                    ban_reason = ?,
                    last_probe_at_ts = ?,
                    last_probe_status = 'banned_401',
                    probe_lock_until_ts = NULL,
                    updated_at_ts = ?
                WHERE id = ?
                """,
                (now, reason, now, now, token_id),
            )
            self.ensure_inventory_policy(conn=conn)
        _refresh_dashboard_memory()
        invalidate_all_runtime_cache(include_admin=True)

    def finalize_claim_reserved_token(
        self,
        token_id: int,
        user_id: int,
        api_key_id: int | None,
        request_id: str,
        *,
        hourly_limit: int | None = None,
        apikey_rate_limit: int | None = None,
        timeout_s: float = 30.0,
    ) -> dict[str, Any] | None:
        now = now_ts()
        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                """
                SELECT id, file_name, file_path, encoding, content_json, claim_count, max_claims,
                       provider_user_id, provider_username, provider_name,
                       is_active, is_enabled, is_banned, is_available
                FROM tokens
                WHERE id = ?
                """,
                (token_id,),
            ).fetchone()
            if not row:
                return None
            already_claimed = conn.execute(
                """
                SELECT 1
                FROM user_token_claims
                WHERE user_id = ? AND token_id = ?
                LIMIT 1
                """,
                (user_id, token_id),
            ).fetchone()
            if (
                already_claimed
                or int(row["is_active"]) != 1
                or int(row["is_enabled"]) != 1
                or int(row["is_banned"]) != 0
                or int(row["is_available"]) != 1
                or int(row["claim_count"]) >= int(row["max_claims"])
            ):
                conn.execute(
                    """
                    UPDATE tokens
                    SET probe_lock_until_ts = NULL,
                        updated_at_ts = ?
                    WHERE id = ?
                    """,
                    (now, token_id),
                )
                return None
            if hourly_limit is not None and hourly_limit > 0:
                used_row = conn.execute(
                    """
                    SELECT COUNT(*) as cnt
                    FROM token_claims
                    WHERE user_id = ? AND claimed_at_ts >= ?
                    """,
                    (user_id, now - 3600),
                ).fetchone()
                used = int(used_row["cnt"]) if used_row else 0
                if used >= hourly_limit:
                    conn.execute(
                        """
                        UPDATE tokens
                        SET probe_lock_until_ts = NULL,
                            updated_at_ts = ?
                        WHERE id = ?
                        """,
                        (now, token_id),
                    )
                    return None
            if api_key_id is not None and apikey_rate_limit is not None and apikey_rate_limit > 0:
                minute_row = conn.execute(
                    """
                    SELECT COUNT(*) as cnt
                    FROM token_claims
                    WHERE api_key_id = ? AND claimed_at_ts >= ?
                    """,
                    (api_key_id, now - 60),
                ).fetchone()
                minute_used = int(minute_row["cnt"]) if minute_row else 0
                if minute_used >= apikey_rate_limit:
                    conn.execute(
                        """
                        UPDATE tokens
                        SET probe_lock_until_ts = NULL,
                            updated_at_ts = ?
                        WHERE id = ?
                        """,
                        (now, token_id),
                    )
                    return None
               

            first_claim = int(row["claim_count"]) <= 0
            new_count = int(row["claim_count"]) + 1
            new_available = 1 if new_count < int(row["max_claims"]) else 0
            savepoint = f"claim_token_{token_id}"
            try:
                conn.execute(f"SAVEPOINT {savepoint}")
                conn.execute(
                    """
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
                    """,
                    (new_count, new_available, now, now, token_id),
                )
                conn.execute(
                    """
                    INSERT INTO token_claims (
                        token_id, user_id, api_key_id, claimed_at_ts, is_hidden,
                        claim_file_name, claim_file_path, claim_encoding, claim_content_json,
                        provider_user_id, provider_username, provider_name,
                        request_id
                    ) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        token_id,
                        user_id,
                        api_key_id,
                        now,
                        row["file_name"],
                        row["file_path"],
                        row["encoding"],
                        row["content_json"],
                        row["provider_user_id"],
                        row["provider_username"],
                        row["provider_name"],
                        request_id,
                    ),
                )
                claim_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
                conn.execute(
                    """
                    INSERT INTO user_token_claims (user_id, token_id, first_claim_id, created_at_ts)
                    VALUES (?, ?, ?, ?)
                    """,
                    (user_id, token_id, claim_id, now),
                )
                conn.execute(f"RELEASE SAVEPOINT {savepoint}")
            except sqlite3.IntegrityError:
                conn.execute(f"ROLLBACK TO SAVEPOINT {savepoint}")
                conn.execute(f"RELEASE SAVEPOINT {savepoint}")
                conn.execute(
                    """
                    UPDATE tokens
                    SET probe_lock_until_ts = NULL,
                        updated_at_ts = ?
                    WHERE id = ?
                    """,
                    (now, token_id),
                )
                return None

            return {
                "claim_id": claim_id,
                "token_id": token_id,
                "file_name": row["file_name"],
                "file_path": row["file_path"],
                "encoding": row["encoding"],
                "content": json.loads(str(row["content_json"])),
                "first_claim": first_claim,
            }

    def allocate_claimable_token(
        self,
        user_id: int,
        api_key_id: int | None,
        request_id: str,
        *,
        hourly_limit: int | None = None,
        apikey_rate_limit: int | None = None,
        timeout_s: float = 30.0,
    ) -> dict[str, Any] | None:
        while True:
            candidate = self.reserve_claimable_token_for_user(user_id, timeout_s=timeout_s)
            if not candidate:
                return None
            probe_result = _CODEX_PROBE.submit(
                candidate["content"],
                wait_timeout_sec=get_codex_probe_timeout_sec() + get_codex_probe_delay_sec() + 5.0,
            )
            token_id = int(candidate["id"])
            if probe_result.is_banned:
                self.mark_token_banned(token_id, reason="upstream_401", timeout_s=timeout_s)
                continue
            if probe_result.status != "ok":
                self.record_probe_status(token_id, probe_result.status, clear_lock=False, timeout_s=timeout_s)
            item = self.finalize_claim_reserved_token(
                token_id,
                user_id,
                api_key_id,
                request_id,
                hourly_limit=hourly_limit,
                apikey_rate_limit=apikey_rate_limit,
                timeout_s=timeout_s,
            )
            if item is None:
                continue
            item["probe_status"] = probe_result.status
            return item

    def cleanup_exhausted_tokens(self, token_dir: Path) -> dict[str, Any]:
        return self.cleanup_exhausted_tokens_with_mode(token_dir, mode="files_and_db")

    def vacuum_database(self, *, timeout_s: float = 60.0) -> None:
        with self._lock:
            with self.connect(timeout=timeout_s) as conn:
                conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            with self.connect(timeout=timeout_s) as conn:
                conn.execute("VACUUM")

    def cleanup_exhausted_tokens_with_mode(self, token_dir: Path, *, mode: str) -> dict[str, Any]:
        normalized_mode = (mode or "").strip().lower()
        if normalized_mode not in {"files_only", "files_and_db"}:
            raise ValueError("Invalid cleanup mode.")
        compact_db_content = normalized_mode == "files_and_db"
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
                content_sql = "content_json = '{}'," if compact_db_content else ""
                conn.execute(
                    f"""
                    UPDATE tokens
                    SET is_active = 0,
                        is_cleaned = 1,
                        is_enabled = 0,
                        is_available = 0,
                        {content_sql}
                        cleaned_at_ts = ?,
                        updated_at_ts = ?,
                        last_seen_at_ts = ?
                    WHERE id IN ({placeholders})
                    """,
                    (now, now, now, *cleaned_ids),
                )
            if compact_db_content:
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
            invalidate_all_runtime_cache(include_admin=True)
        vacuumed = False
        if compact_db_content and compacted_content > 0:
            self.vacuum_database()
            vacuumed = True

        return {
            "mode": normalized_mode,
            "matched": len(rows),
            "cleaned": len(cleaned_ids),
            "deleted_files": deleted_files,
            "missing_files": missing_files,
            "compacted_content": compacted_content,
            "vacuumed": vacuumed,
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

    def get_api_keys_payload(self, user_id: int) -> dict[str, Any]:
        items = self.list_api_keys(user_id)
        return {
            "items": items,
            "limit": get_apikey_max_per_user(),
            "active": sum(1 for item in items if str(item.get("status")) == "active"),
        }

    def get_api_key_summary(self, user_id: int) -> dict[str, int]:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM api_keys
                WHERE user_id = ? AND status = 'active'
                """,
                (user_id,),
            ).fetchone()
        return {
            "limit": get_apikey_max_per_user(),
            "active": int(row["cnt"]) if row else 0,
        }

    def list_queued_user_ids(self) -> list[int]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT user_id
                FROM claim_queue
                WHERE status = 'queued' AND remaining > 0
                ORDER BY queue_rank ASC, id ASC
                """
            ).fetchall()
        return [int(row["user_id"]) for row in rows]

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

        created_item = {
            "id": api_key_id,
            "name": name,
            "prefix": prefix,
            "token": api_key,
            "status": "active",
            "created_at": isoformat_from_ts(now),
            "last_used_at": None,
        }
        with _API_KEY_CACHE_LOCK:
            _API_KEY_CACHE_BY_HASH[key_hash] = {
                "api_key_id": api_key_id,
                "user_id": user_id,
                "status": "active",
            }
        _APP_CACHE.set_json(
            build_cache_key("apikey-resolve", key_hash),
            {"api_key_id": api_key_id, "user_id": user_id, "status": "active"},
            ttl_sec=get_cache_me_ttl(),
        )
        cached_payload = get_user_api_keys_snapshot(user_id)
        payload = set_user_api_keys_snapshot(
            user_id,
            {
                "items": [created_item, *(cached_payload.get("items") or [])],
                "limit": int(cached_payload.get("limit") or get_apikey_max_per_user()),
                "active": int(cached_payload.get("active") or 0) + 1,
            },
        )
        invalidate_admin_cache()
        return payload

    def revoke_api_key(self, user_id: int, api_key_id: int) -> dict[str, Any]:
        now = now_ts()
        with self._lock, self.connect() as conn:
            row = conn.execute(
                "SELECT key_hash FROM api_keys WHERE id = ? AND user_id = ?",
                (api_key_id, user_id),
            ).fetchone()
            cursor = conn.execute(
                """
                UPDATE api_keys
                SET status = 'revoked', revoked_at_ts = ?
                WHERE id = ? AND user_id = ?
                """,
                (now, api_key_id, user_id),
            )
        if not cursor.rowcount:
            return get_user_api_keys_snapshot(user_id)
        if row:
            with _API_KEY_CACHE_LOCK:
                _API_KEY_CACHE_BY_HASH.pop(str(row["key_hash"]), None)
            _APP_CACHE.delete(build_cache_key("apikey-resolve", str(row["key_hash"])))
        cached_payload = get_user_api_keys_snapshot(user_id)
        updated_items: list[dict[str, Any]] = []
        active = 0
        for item in cached_payload.get("items") or []:
            if int(item.get("id") or 0) == int(api_key_id):
                updated = dict(item)
                updated["status"] = "revoked"
                updated_items.append(updated)
            else:
                updated_items.append(item)
        for item in updated_items:
            if str(item.get("status")) == "active":
                active += 1
        payload = set_user_api_keys_snapshot(
            user_id,
            {
                "items": updated_items,
                "limit": int(cached_payload.get("limit") or get_apikey_max_per_user()),
                "active": active,
            },
        )
        invalidate_admin_cache()
        return payload

    def resolve_api_key(self, api_key: str) -> dict[str, Any] | None:
        key_hash = hash_api_key(api_key)
        cache_key = build_cache_key("apikey-resolve", key_hash)
        cached_payload = _APP_CACHE.get_json(cache_key)
        if isinstance(cached_payload, dict) and cached_payload.get("status") == "active":
            return {
                "api_key_id": int(cached_payload["api_key_id"]),
                "user_id": int(cached_payload["user_id"]),
            }
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
        _APP_CACHE.set_json(cache_key, {**record, "status": "active"}, ttl_sec=get_cache_me_ttl())
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
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0"
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
                WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0
                """
            ).fetchone()
            claimed_total = conn.execute("SELECT COUNT(*) as cnt FROM token_claims").fetchone()
            claimed_unique = conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0 AND claim_count > 0"
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
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0"
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
                WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0
                """
            ).fetchone()
            unclaimed_row = target_conn.execute(
                "SELECT COUNT(*) as cnt FROM tokens WHERE is_active = 1 AND is_enabled = 1 AND is_banned = 0 AND claim_count = 0"
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

    def ensure_inventory_policy(self, *, conn=None, refresh_existing_scope: bool = True) -> dict[str, Any]:
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
                _POLICY_STATE["status"] = runtime_state["status"]
                _POLICY_STATE["max_claims"] = int(runtime_state["max_claims"])
                if refresh_existing_scope:
                    now = now_ts()
                    _apply_non_healthy_scope(now)
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

    def get_contributor_leaderboard(self, limit: int) -> list[dict[str, Any]]:
        limit = max(1, int(limit))
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT provider_user_id as user_id,
                       COALESCE(provider_name, provider_username) as name,
                       provider_username as username,
                       COUNT(*) as cnt
                FROM tokens
                WHERE provider_user_id IS NOT NULL
                  AND provider_user_id != ''
                GROUP BY provider_user_id, provider_username, provider_name
                ORDER BY cnt DESC, provider_username ASC, provider_user_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "name": row["name"] or row["username"],
                    "count": int(row["cnt"] or 0),
                }
            )
        return items

    def list_recent_contributors(self, limit: int) -> list[dict[str, Any]]:
        limit = max(1, int(limit))
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT provider_user_id as user_id,
                       COALESCE(provider_name, provider_username) as name,
                       provider_username as username,
                       COUNT(*) as cnt,
                       MAX(uploaded_at_ts) as uploaded_at_ts
                FROM tokens
                WHERE provider_user_id IS NOT NULL
                  AND provider_user_id != ''
                GROUP BY provider_user_id, provider_username, provider_name
                ORDER BY uploaded_at_ts DESC, provider_username ASC, provider_user_id ASC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        items: list[dict[str, Any]] = []
        for row in rows:
            items.append(
                {
                    "user_id": row["user_id"],
                    "username": row["username"],
                    "name": row["name"] or row["username"],
                    "count": int(row["cnt"] or 0),
                    "uploaded_at": isoformat_from_ts(int(row["uploaded_at_ts"])) if row["uploaded_at_ts"] else None,
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
        return {"total": self.get_queue_total_runtime()}

    def get_queue_total_runtime(self, conn: sqlite3.Connection | None = None) -> int:
        def _read(target_conn: sqlite3.Connection) -> int:
            row = target_conn.execute(
                "SELECT total_queued FROM queue_runtime WHERE id = 1"
            ).fetchone()
            return int(row["total_queued"]) if row else 0

        if conn is not None:
            return _read(conn)
        with self.connect() as target_conn:
            return _read(target_conn)

    def list_claimable_tokens_for_user(
        self,
        conn: sqlite3.Connection,
        user_id: int,
        limit: int,
    ) -> list[sqlite3.Row]:
        now = now_ts()
        return conn.execute(
            """
            SELECT id, file_name, file_path, encoding, content_json, claim_count, max_claims
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
            LIMIT ?
            """,
            (now, user_id, max(0, int(limit))),
        ).fetchall()

    def claim_tokens(
        self,
        user_id: int,
        api_key_id: int | None,
        count: int,
    ) -> dict[str, Any]:
        requested = max(1, count)
        request_id = secrets.token_hex(8)
        now = now_ts()
        granted_token_ids: list[int] = []
        first_claim_count = 0
        queued_created = False
        queued_result: dict[str, Any] | None = None
        with self.connect() as conn:
            self.ensure_inventory_policy(conn=conn, refresh_existing_scope=False)
            batch_limit = get_claim_batch_limit(self, conn=conn)
            requested = min(requested, batch_limit)
            used_row = conn.execute(
                """
                SELECT COUNT(*) as cnt
                FROM token_claims
                WHERE user_id = ? AND claimed_at_ts >= ?
                """,
                (user_id, now - 3600),
            ).fetchone()
            used = int(used_row["cnt"]) if used_row else 0
            limit = get_claim_hourly_limit(self, conn=conn)
            remaining = max(0, limit - used)
            if remaining <= 0:
                raise RateLimitError("您当前小时内的兑换额度已用完")
            per_minute = get_apikey_rate_per_minute() if api_key_id is not None else 0
            if api_key_id is not None and per_minute > 0:
                minute_row = conn.execute(
                    """
                    SELECT COUNT(*) as cnt
                    FROM token_claims
                    WHERE api_key_id = ? AND claimed_at_ts >= ?
                    """,
                    (api_key_id, now - 60),
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
                queued_created = not bool(queued.get("existing"))
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
                if queued_created:
                    _DASHBOARD_CACHE.adjust_queue_total(1)
                queued_result = result
        if queued_result is not None:
            set_user_queue_snapshot(
                user_id,
                build_user_queue_snapshot_payload(
                    user_id,
                    {
                        "queue_id": queued_result["queue_id"],
                        "position": queued_result["queue_position"],
                        "ahead": max(0, int(queued_result["queue_position"]) - 1),
                        "requested": queued_result["requested"],
                        "remaining": queued_result["queue_remaining"],
                        "enqueued_at": isoformat_from_ts(int(queued["enqueued_at_ts"])),
                        "request_id": queued_result["request_id"],
                    },
                    available_tokens=get_global_available_tokens_estimate(),
                ),
            )
            if queued_created:
                refresh_queue_meta_runtime(self)
            invalidate_dashboard_cache(user_id=user_id)
            invalidate_admin_cache()
            return queued_result

        items: list[dict[str, Any]] = []
        while len(items) < target:
            item = self.allocate_claimable_token(
                user_id,
                api_key_id,
                request_id,
                hourly_limit=limit,
                apikey_rate_limit=per_minute,
            )
            if item is None:
                break
            if bool(item.get("first_claim")):
                first_claim_count += 1
            granted_token_ids.append(int(item["token_id"]))
            items.append(
                {
                    "claim_id": item["claim_id"],
                    "token_id": item["token_id"],
                    "file_name": item["file_name"],
                    "file_path": item["file_path"],
                    "encoding": item["encoding"],
                    "content": item["content"],
                }
            )

        if not items:
            queued = claim_queue.enqueue_claim(self, user_id, api_key_id, target)
            queued_created = not bool(queued.get("existing"))
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

        if queued_created:
            _DASHBOARD_CACHE.adjust_queue_total(1)
        if result.get("queued"):
            set_user_queue_snapshot(
                user_id,
                build_user_queue_snapshot_payload(
                    user_id,
                    {
                        "queue_id": result["queue_id"],
                        "position": result["queue_position"],
                        "ahead": max(0, int(result["queue_position"]) - 1),
                        "requested": result["requested"],
                        "remaining": result["queue_remaining"],
                        "enqueued_at": isoformat_from_ts(int(queued["enqueued_at_ts"])),
                        "request_id": result["request_id"],
                    },
                    available_tokens=get_global_available_tokens_estimate(),
                ),
            )
            if queued_created:
                refresh_queue_meta_runtime(self)
        if result.get("granted"):
            user = self.get_user(user_id)
            if user:
                _DASHBOARD_CACHE.record_claim(
                    user=user,
                    request_id=str(result["request_id"]),
                    claimed_at_ts=now,
                    token_ids=granted_token_ids,
                    first_claim_count=first_claim_count,
                    granted=int(result["granted"]),
                )
            set_user_queue_snapshot(user_id, build_default_user_queue_snapshot(queued=False))
        if result.get("granted"):
            # Keep the aggregated /me payload fresh even when child snapshots are maintained separately.
            invalidate_user_profile_cache(user_id)
            invalidate_user_quota_cache(user_id)
            invalidate_user_claims_cache(user_id)
        invalidate_dashboard_cache(user_id=user_id)
        invalidate_admin_cache()
        return result

    def get_claimed_token(self, token_id: int, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT token_claims.token_id as token_id,
                       COALESCE(token_claims.claim_file_name, tokens.file_name) as file_name,
                       COALESCE(token_claims.claim_file_path, tokens.file_path) as file_path,
                       COALESCE(token_claims.claim_encoding, tokens.encoding) as encoding,
                       COALESCE(token_claims.claim_content_json, tokens.content_json) as content_json
                FROM token_claims
                LEFT JOIN tokens ON tokens.id = token_claims.token_id
                WHERE token_claims.token_id = ? AND token_claims.user_id = ? AND token_claims.is_hidden = 0
                LIMIT 1
                """,
                (token_id, user_id),
            ).fetchone()
        if not row:
            return None
        return {
            "token_id": int(row["token_id"]),
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
                       token_claims.token_id as token_id,
                       COALESCE(token_claims.claim_file_name, tokens.file_name) as file_name,
                       COALESCE(token_claims.claim_file_path, tokens.file_path) as file_path,
                       COALESCE(token_claims.claim_encoding, tokens.encoding) as encoding,
                       COALESCE(token_claims.claim_content_json, tokens.content_json) as content_json,
                       token_claims.provider_user_id as provider_user_id,
                       token_claims.provider_username as provider_username,
                       token_claims.provider_name as provider_name
                FROM token_claims
                LEFT JOIN tokens ON tokens.id = token_claims.token_id
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
                    "provider": (
                        {
                            "user_id": row["provider_user_id"],
                            "username": row["provider_username"],
                            "name": row["provider_name"],
                        }
                        if row["provider_user_id"] or row["provider_username"] or row["provider_name"]
                        else None
                    ),
                }
            )
        return items

    def get_queue_status(self, user_id: int) -> dict[str, Any] | None:
        return self.list_queue_statuses([user_id]).get(int(user_id))

    def list_queue_statuses(self, user_ids: list[int] | None = None) -> dict[int, dict[str, Any]]:
        unique_user_ids = sorted({int(user_id) for user_id in (user_ids or [])})
        with self.connect() as conn:
            if unique_user_ids:
                placeholders = ",".join("?" for _ in unique_user_ids)
                rows = conn.execute(
                    f"""
                    SELECT id, user_id, requested, remaining, queue_rank, enqueued_at_ts, request_id
                    FROM claim_queue
                    WHERE status = 'queued' AND remaining > 0 AND user_id IN ({placeholders})
                    ORDER BY queue_rank ASC, id ASC
                    """,
                    tuple(unique_user_ids),
                ).fetchall()
            else:
                rows = conn.execute(
                    """
                    SELECT id, user_id, requested, remaining, queue_rank, enqueued_at_ts, request_id
                    FROM claim_queue
                    WHERE status = 'queued' AND remaining > 0
                    ORDER BY queue_rank ASC, id ASC
                    """
                ).fetchall()
        payloads: dict[int, dict[str, Any]] = {}
        for row in rows:
            user_id = int(row["user_id"])
            queue_rank = max(1, int(row["queue_rank"]))
            payloads[user_id] = {
                "queue_id": int(row["id"]),
                "position": queue_rank,
                "ahead": max(0, queue_rank - 1),
                "requested": int(row["requested"]),
                "remaining": int(row["remaining"]),
                "enqueued_at": isoformat_from_ts(int(row["enqueued_at_ts"])),
                "request_id": row["request_id"],
            }
        return payloads

    def consume_queue_grant(self, queue_id: int, granted: int, *, timeout_s: float = 30.0) -> dict[str, Any]:
        granted = max(0, int(granted))
        if granted <= 0:
            return {"removed": False, "remaining": None}
        with self._lock, self.connect(timeout=timeout_s) as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT remaining, queue_rank FROM claim_queue WHERE id = ? AND status = 'queued'",
                (queue_id,),
            ).fetchone()
            if not row:
                return {"removed": False, "remaining": None}
            remaining_after = max(0, int(row["remaining"]) - granted)
            if remaining_after <= 0:
                removed_rank = int(row["queue_rank"])
                conn.execute("DELETE FROM claim_queue WHERE id = ?", (queue_id,))
                conn.execute(
                    """
                    UPDATE claim_queue
                    SET queue_rank = queue_rank - 1
                    WHERE status = 'queued' AND remaining > 0 AND queue_rank > ?
                    """,
                    (removed_rank,),
                )
                current_total = self.get_queue_total_runtime(conn)
                conn.execute(
                    """
                    UPDATE queue_runtime
                    SET total_queued = ?, updated_at_ts = ?
                    WHERE id = 1
                    """,
                    (max(0, current_total - 1), now_ts()),
                )
                return {"removed": True, "remaining": 0}
            conn.execute(
                """
                UPDATE claim_queue
                SET remaining = ?, status = 'queued'
                WHERE id = ?
                """,
                (remaining_after, queue_id),
            )
            return {"removed": False, "remaining": remaining_after}

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
            changed = int(cursor.rowcount or 0)
        if changed:
            invalidate_user_claims_cache(user_id)
        return changed

    def list_claim_files(self, user_id: int) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT token_claims.id as claim_id,
                       COALESCE(token_claims.claim_file_name, tokens.file_name) as file_name,
                       COALESCE(token_claims.claim_file_path, tokens.file_path) as file_path,
                       COALESCE(token_claims.claim_encoding, tokens.encoding) as encoding,
                       COALESCE(token_claims.claim_content_json, tokens.content_json) as content_json
                FROM token_claims
                LEFT JOIN tokens ON tokens.id = token_claims.token_id
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
                SELECT token_claims.token_id as token_id,
                       COALESCE(token_claims.claim_file_name, tokens.file_name) as file_name,
                       COALESCE(token_claims.claim_file_path, tokens.file_path) as file_path,
                       COALESCE(token_claims.claim_encoding, tokens.encoding) as encoding,
                       COALESCE(token_claims.claim_content_json, tokens.content_json) as content_json,
                       MAX(token_claims.claimed_at_ts) as last_claimed_ts
                FROM token_claims
                LEFT JOIN tokens ON tokens.id = token_claims.token_id
                WHERE token_claims.user_id = ? AND token_claims.is_hidden = 0
                GROUP BY token_claims.token_id, file_name, file_path, encoding, content_json
                ORDER BY last_claimed_ts DESC, token_claims.token_id DESC
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


def get_cache_default_ttl() -> int:
    return max(1, env_int(CACHE_DEFAULT_TTL_ENV, 15))


def get_cache_me_ttl() -> int:
    return max(1, env_int(CACHE_ME_TTL_ENV, 10))


def get_cache_claims_ttl() -> int:
    return max(1, env_int(CACHE_CLAIMS_TTL_ENV, 15))


def get_cache_admin_ttl() -> int:
    return max(1, env_int(CACHE_ADMIN_TTL_ENV, 20))


def get_cache_queue_ttl() -> int:
    return max(1, env_int(CACHE_QUEUE_TTL_ENV, 5))


def get_cache_queue_snapshot_ttl() -> int:
    return max(get_cache_queue_ttl(), 86400)


def get_cache_upload_results_ttl() -> int:
    return max(get_cache_me_ttl(), 7 * 24 * 3600)


def get_cache_dashboard_ttl() -> int:
    return max(1, env_int(CACHE_DASHBOARD_TTL_ENV, 10))


def _cache_version_key(scope: str, *parts: Any) -> str:
    clean_parts = [str(part).strip() for part in parts if str(part).strip()]
    suffix = ":".join(clean_parts)
    if not suffix:
        return f"ns:{scope}"
    return f"ns:{scope}:{suffix}"


def get_cache_scope_version(scope: str, *parts: Any) -> int:
    key = _cache_version_key(scope, *parts)
    raw = _APP_CACHE.get_text(key)
    if raw is None:
        _APP_CACHE.set_text(key, "1")
        return 1
    try:
        value = int(raw)
    except ValueError:
        _APP_CACHE.set_text(key, "1")
        return 1
    return max(1, value)


def bump_cache_scope(scope: str, *parts: Any) -> int:
    return max(1, _APP_CACHE.incr(_cache_version_key(scope, *parts)))


def build_cache_key(prefix: str, *parts: Any) -> str:
    values = [prefix]
    for part in parts:
        values.append(str(part))
    return ":".join(values)


def cache_json(key: str, ttl_sec: int, loader: Callable[[], Any]) -> Any:
    cached = _APP_CACHE.get_json(key)
    if cached is not None:
        return cached
    payload = loader()
    _APP_CACHE.set_json(key, payload, ttl_sec=ttl_sec)
    return payload


def build_snapshot_cache_key(prefix: str, *parts: Any) -> str:
    return build_cache_key("snapshot", prefix, *parts)


def build_user_apikeys_cache_key(user_id: int) -> str:
    version = get_cache_scope_version("user-apikeys", user_id)
    return build_snapshot_cache_key("user-apikeys", user_id, f"v{version}")


def build_user_apikey_summary_cache_key(user_id: int) -> str:
    version = get_cache_scope_version("user-apikey-summary", user_id)
    return build_snapshot_cache_key("user-apikey-summary", user_id, f"v{version}")


def build_user_queue_cache_key(user_id: int) -> str:
    version = get_cache_scope_version("user-queue", user_id)
    return build_snapshot_cache_key(
        "user-queue",
        user_id,
        f"v{version}",
    )


def build_user_profile_cache_key(user_id: int, *, is_admin: bool) -> str:
    version = get_cache_scope_version("user-profile", user_id)
    return build_snapshot_cache_key("user-profile", user_id, int(bool(is_admin)), f"v{version}")


def build_user_upload_results_cache_key(user_id: int) -> str:
    version = get_cache_scope_version("user-upload-results", user_id)
    return build_snapshot_cache_key("user-upload-results", user_id, f"v{version}")


def invalidate_user_profile_cache(user_id: int) -> None:
    bump_cache_scope("user-basic", user_id)
    bump_cache_scope("user-profile", user_id)


def invalidate_user_quota_cache(user_id: int) -> None:
    bump_cache_scope("user-quota", user_id)
    bump_cache_scope("user-profile", user_id)


def invalidate_user_apikeys_cache(user_id: int) -> None:
    bump_cache_scope("user-apikeys", user_id)
    bump_cache_scope("user-apikey-summary", user_id)
    bump_cache_scope("user-profile", user_id)


def invalidate_user_claims_cache(user_id: int) -> None:
    bump_cache_scope("user-claims", user_id)
    bump_cache_scope("user-claims-summary", user_id)
    bump_cache_scope("user-profile", user_id)


def invalidate_user_queue_cache(user_id: int) -> None:
    bump_cache_scope("user-queue", user_id)


def invalidate_inventory_cache() -> None:
    bump_cache_scope("inventory")


def invalidate_user_cache(user_id: int) -> None:
    bump_cache_scope("user", user_id)
    invalidate_user_profile_cache(user_id)
    invalidate_user_quota_cache(user_id)
    invalidate_user_apikeys_cache(user_id)
    invalidate_user_claims_cache(user_id)


def invalidate_dashboard_cache(*, user_id: int | None = None) -> None:
    bump_cache_scope("dashboard")
    if user_id is not None:
        bump_cache_scope("dashboard-user", user_id)


def invalidate_admin_cache() -> None:
    bump_cache_scope("admin")


def invalidate_all_runtime_cache(*, user_id: int | None = None, include_admin: bool = False) -> None:
    if user_id is not None:
        invalidate_user_cache(user_id)
    invalidate_dashboard_cache(user_id=user_id)
    if include_admin:
        invalidate_admin_cache()


def _get_cached_snapshot(key: str) -> dict[str, Any] | None:
    payload = _APP_CACHE.get_json(key)
    if isinstance(payload, dict):
        return payload
    return None


def _set_cached_snapshot(key: str, payload: dict[str, Any], ttl_sec: int) -> dict[str, Any]:
    _APP_CACHE.set_json(key, payload, ttl_sec=ttl_sec)
    return payload


def build_user_cache_key(prefix: str, user_id: int, *parts: Any) -> str:
    version = get_cache_scope_version("user", user_id)
    return build_cache_key(prefix, user_id, f"v{version}", *parts)


def build_dashboard_cache_key(prefix: str, *parts: Any, user_id: int | None = None) -> str:
    dashboard_version = get_cache_scope_version("dashboard")
    if user_id is None:
        return build_cache_key(prefix, f"v{dashboard_version}", *parts)
    user_version = get_cache_scope_version("dashboard-user", user_id)
    return build_cache_key(prefix, user_id, f"v{dashboard_version}", f"uv{user_version}", *parts)


def build_admin_cache_key(prefix: str, *parts: Any) -> str:
    version = get_cache_scope_version("admin")
    return build_cache_key(prefix, f"v{version}", *parts)


def get_queue_total_snapshot(db_handle: "TokenDb") -> int:
    return db_handle.get_queue_total_runtime()


def refresh_queue_total_snapshot(db_handle: "TokenDb") -> int:
    before = _DASHBOARD_CACHE.get_system_status()["queue"].get("total", 0)
    total = get_queue_total_snapshot(db_handle)
    _DASHBOARD_CACHE.set_queue_total(total)
    token_import_log(f"[queue-refresh] total={before}->{total}")
    return total


def get_global_available_tokens_estimate() -> int:
    return max(0, int(_DASHBOARD_CACHE.get_system_status().get("inventory", {}).get("available", 0)))


def build_default_user_queue_snapshot(*, queued: bool = False) -> dict[str, Any]:
    payload = {
        "queued": bool(queued),
        "total_queued": int(_DASHBOARD_CACHE.get_system_status().get("queue", {}).get("total", 0)),
        "available_tokens": get_global_available_tokens_estimate(),
    }
    if not queued:
        return payload
    return payload


def build_user_basic_cache_key(user_id: int, *, is_admin: bool) -> str:
    version = get_cache_scope_version("user-basic", user_id)
    return build_snapshot_cache_key("user-basic", user_id, int(bool(is_admin)), f"v{version}")


def build_user_quota_cache_key(user_id: int) -> str:
    version = get_cache_scope_version("user-quota", user_id)
    return build_snapshot_cache_key("user-quota", user_id, f"v{version}")


def build_user_claim_summary_cache_key(user_id: int) -> str:
    version = get_cache_scope_version("user-claims-summary", user_id)
    return build_snapshot_cache_key("user-claims-summary", user_id, f"v{version}")


def get_user_basic_snapshot(user_id: int, *, is_admin: bool) -> dict[str, Any]:
    key = build_user_basic_cache_key(user_id, is_admin=is_admin)
    cached = _get_cached_snapshot(key)
    if cached is not None:
        return cached
    user = db.get_user(user_id)
    if not user:
        raise PermissionError
    payload = {
        "id": user["linuxdo_user_id"],
        "username": user["linuxdo_username"],
        "name": user["linuxdo_name"] or user["linuxdo_username"],
        "trust_level": user["trust_level"],
        "is_admin": is_admin,
        "is_banned": False,
    }
    return _set_cached_snapshot(key, payload, get_cache_me_ttl())


def get_user_quota_snapshot(user_id: int) -> dict[str, Any]:
    key = build_user_quota_cache_key(user_id)
    cached = _get_cached_snapshot(key)
    if cached is not None:
        return cached
    return _set_cached_snapshot(key, db.get_quota_usage(user_id), get_cache_me_ttl())


def get_user_claim_summary_snapshot(user_id: int) -> dict[str, int]:
    key = build_user_claim_summary_cache_key(user_id)
    cached = _get_cached_snapshot(key)
    if cached is not None:
        return {
            "total": int(cached.get("total") or 0),
            "unique": int(cached.get("unique") or 0),
        }
    return _set_cached_snapshot(key, db.get_user_claim_totals(user_id), get_cache_me_ttl())


def _hydrate_user_profile_snapshot(user_id: int, *, is_admin: bool) -> dict[str, Any]:
    user = get_user_basic_snapshot(user_id, is_admin=is_admin)
    quota = get_user_quota_snapshot(user_id)
    totals = get_user_claim_summary_snapshot(user_id)
    api_keys = get_user_api_key_summary_snapshot(user_id)
    return {
        "user": user,
        "quota": quota,
        "claims": totals,
        "api_keys": {
            "limit": int(api_keys.get("limit") or get_apikey_max_per_user()),
            "active": int(api_keys.get("active") or 0),
        },
        "uploads": {
            "max_files_per_request": get_upload_max_files_per_request(),
            "max_file_size_bytes": get_upload_max_file_size_bytes(),
            "max_success_per_hour": get_upload_max_success_per_hour(),
            "min_trust_level": get_linuxdo_min_trust_level(),
        },
    }


def get_user_profile_snapshot(user_id: int, *, is_admin: bool) -> dict[str, Any]:
    key = build_user_profile_cache_key(user_id, is_admin=is_admin)
    cached = _get_cached_snapshot(key)
    if cached is not None:
        return cached
    payload = _hydrate_user_profile_snapshot(user_id, is_admin=is_admin)
    return _set_cached_snapshot(key, payload, get_cache_me_ttl())


def get_user_api_keys_snapshot(user_id: int) -> dict[str, Any]:
    key = build_user_apikeys_cache_key(user_id)
    cached = _APP_CACHE.get_json(key)
    if isinstance(cached, dict):
        return {
            "items": list(cached.get("items") or []),
            "limit": int(cached.get("limit") or get_apikey_max_per_user()),
            "active": int(cached.get("active") or 0),
        }
    api_keys = db.get_api_keys_payload(user_id)
    _APP_CACHE.set_json(key, api_keys, ttl_sec=get_cache_me_ttl())
    return api_keys


def set_user_api_keys_snapshot(user_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    key = build_user_apikeys_cache_key(user_id)
    normalized = {
        "items": list(payload.get("items") or []),
        "limit": int(payload.get("limit") or get_apikey_max_per_user()),
        "active": int(payload.get("active") or 0),
    }
    _APP_CACHE.set_json(key, normalized, ttl_sec=get_cache_me_ttl())
    set_user_api_key_summary_snapshot(
        user_id,
        {"limit": normalized["limit"], "active": normalized["active"]},
    )
    # /me aggregates api key summary, so any direct payload rewrite must also evict the top-level profile snapshot.
    bump_cache_scope("user-profile", user_id)
    return normalized


def get_user_api_key_summary_snapshot(user_id: int) -> dict[str, int]:
    key = build_user_apikey_summary_cache_key(user_id)
    cached = _get_cached_snapshot(key)
    if cached is not None:
        return {
            "limit": int(cached.get("limit") or get_apikey_max_per_user()),
            "active": int(cached.get("active") or 0),
        }
    return set_user_api_key_summary_snapshot(
        user_id,
        db.get_api_key_summary(user_id),
    )


def set_user_api_key_summary_snapshot(user_id: int, payload: dict[str, Any]) -> dict[str, int]:
    key = build_user_apikey_summary_cache_key(user_id)
    normalized = {
        "limit": int(payload.get("limit") or get_apikey_max_per_user()),
        "active": int(payload.get("active") or 0),
    }
    _set_cached_snapshot(key, normalized, get_cache_me_ttl())
    return normalized


def get_default_upload_results_snapshot() -> dict[str, Any]:
    return {
        "batch_id": None,
        "created_at": None,
        "summary": {
            "total": 0,
            "accepted": 0,
            "duplicates": 0,
            "invalid": 0,
            "rejected": 0,
            "db_busy": 0,
            "queued": 0,
            "processing": 0,
        },
        "items": [],
        "history": [],
        "queue_status": None,
    }


def get_user_upload_results_snapshot(user_id: int) -> dict[str, Any]:
    key = build_user_upload_results_cache_key(user_id)
    cached = _get_cached_snapshot(key)
    if cached is not None:
        history = list(cached.get("history") or [])
        if not history and cached.get("items"):
            history = [
                build_upload_history_entry(
                    str(cached.get("batch_id") or "legacy"),
                    list(cached.get("items") or []),
                    created_at=str(cached.get("created_at") or isoformat_now()),
                )
            ]
        return {
            "batch_id": cached.get("batch_id"),
            "created_at": cached.get("created_at"),
            "summary": dict(cached.get("summary") or get_default_upload_results_snapshot()["summary"]),
            "items": list(cached.get("items") or []),
            "history": history,
            "queue_status": dict(cached.get("queue_status") or {}) if isinstance(cached.get("queue_status"), dict) else None,
        }
    return get_default_upload_results_snapshot()


def set_user_upload_results_snapshot(user_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    key = build_user_upload_results_cache_key(user_id)
    history = list(payload.get("history") or [])
    if not history and payload.get("items"):
        history = [
            build_upload_history_entry(
                str(payload.get("batch_id") or "latest"),
                list(payload.get("items") or []),
                created_at=str(payload.get("created_at") or isoformat_now()),
            )
        ]
    normalized = {
        "batch_id": payload.get("batch_id"),
        "created_at": payload.get("created_at"),
        "summary": dict(payload.get("summary") or get_default_upload_results_snapshot()["summary"]),
        "items": list(payload.get("items") or []),
        "history": history,
        "queue_status": dict(payload.get("queue_status") or {}) if isinstance(payload.get("queue_status"), dict) else None,
    }
    return _set_cached_snapshot(key, normalized, get_cache_upload_results_ttl())


def get_cached_user_queue_snapshot(user_id: int) -> dict[str, Any] | None:
    key = build_user_queue_cache_key(user_id)
    return _get_cached_snapshot(key)


def build_user_queue_snapshot_payload(
    user_id: int,
    status: dict[str, Any] | None,
    *,
    available_tokens: int | None,
) -> dict[str, Any]:
    if not status:
        return build_default_user_queue_snapshot(queued=False)
    total_queued = int(_DASHBOARD_CACHE.get_system_status().get("queue", {}).get("total", 0))
    claimable = get_global_available_tokens_estimate()
    if available_tokens is not None:
        claimable = max(0, int(available_tokens))
    return {
        "queued": True,
        **status,
        "total_queued": total_queued,
        "available_tokens": claimable,
    }


def set_user_queue_snapshot(user_id: int, payload: dict[str, Any]) -> dict[str, Any]:
    key = build_user_queue_cache_key(user_id)
    return _set_cached_snapshot(key, payload, get_cache_queue_snapshot_ttl())


def refresh_queue_meta_runtime(db_handle: "TokenDb") -> None:
    refresh_queue_total_snapshot(db_handle)


def refresh_user_queue_snapshots(user_ids: list[int]) -> None:
    unique_user_ids = sorted({int(user_id) for user_id in user_ids})
    if not unique_user_ids:
        return
    status_map = db.list_queue_statuses(unique_user_ids)
    for user_id in unique_user_ids:
        payload = build_user_queue_snapshot_payload(
            user_id,
            status_map.get(user_id),
            available_tokens=None,
        )
        set_user_queue_snapshot(user_id, payload)


def rebuild_user_queue_snapshots_from_db(user_ids: list[int]) -> None:
    unique_user_ids = sorted({int(user_id) for user_id in user_ids})
    if not unique_user_ids:
        return
    refresh_user_queue_snapshots(unique_user_ids)


def rebuild_all_queue_snapshots_from_db() -> None:
    queued_user_ids = db.list_queued_user_ids()
    refresh_queue_meta_runtime(db)
    if not queued_user_ids:
        return
    rebuild_user_queue_snapshots_from_db(queued_user_ids)



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


def get_expected_request_origin(request: Request) -> str:
    provider_base_url = get_provider_base_url()
    if provider_base_url:
        parsed = urllib.parse.urlparse(provider_base_url)
        if parsed.scheme and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"

    forwarded_proto = (request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip()
    forwarded_host = (request.headers.get("x-forwarded-host") or "").split(",", 1)[0].strip()
    if forwarded_proto and forwarded_host:
        return f"{forwarded_proto}://{forwarded_host}".rstrip("/")

    return str(request.base_url).rstrip("/")


def require_web_upload_request(request: Request) -> None:
    origin = (request.headers.get("origin") or "").rstrip("/")
    referer = request.headers.get("referer") or ""
    fetch_site = (request.headers.get("sec-fetch-site") or "").strip().lower()
    upload_source = (request.headers.get("x-upload-source") or "").strip().lower()
    expected_origin = get_expected_request_origin(request)

    if upload_source != "web":
        raise AccessDeniedError("上传仅允许从网页端发起。")
    if origin != expected_origin:
        raise AccessDeniedError("上传来源无效。")
    if fetch_site not in {"same-origin", "same-site"}:
        raise AccessDeniedError("仅允许网页端同源上传。")
    if referer and not referer.startswith(f"{expected_origin}/"):
        raise AccessDeniedError("上传来源无效。")


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


def get_codex_probe_delay_sec() -> float:
    return max(0.0, env_float(TOKEN_CODEX_PROBE_DELAY_SEC_ENV, 0.1))


def get_codex_probe_timeout_sec() -> float:
    return max(1.0, env_float(TOKEN_CODEX_PROBE_TIMEOUT_SEC_ENV, 20.0))


def get_codex_probe_reserve_sec() -> int:
    return max(5, int(env_float(TOKEN_CODEX_PROBE_RESERVE_SEC_ENV, 30.0)))


def get_upload_max_files_per_request() -> int:
    return max(1, env_int(TOKEN_UPLOAD_MAX_FILES_ENV, 10))


def get_upload_max_file_size_bytes() -> int:
    return max(1024, env_int(TOKEN_UPLOAD_MAX_FILE_SIZE_ENV, 10 * 1024))


def get_upload_max_success_per_hour() -> int:
    return max(1, env_int(TOKEN_UPLOAD_MAX_SUCCESS_PER_HOUR_ENV, 20))


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
    key = build_user_cache_key("me-claimed-docs", user_id)
    items = cache_json(key, get_cache_claims_ttl(), lambda: db.list_claimed_tokens(user_id))
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


def build_upload_response(
    items: list[dict[str, Any]],
    *,
    batch_id: str | None = None,
    created_at: str | None = None,
    history: list[dict[str, Any]] | None = None,
    queue_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = {
        "total": len(items),
        "accepted": sum(1 for item in items if item["status"] == "accepted"),
        "duplicates": sum(1 for item in items if item["status"] == "duplicate"),
        "invalid": sum(1 for item in items if item["status"] in {"invalid_json", "missing_fields", "invalid_file", "file_too_large"}),
        "rejected": sum(
            1
            for item in items
            if item["status"] not in {"accepted", "duplicate", "invalid_json", "missing_fields", "invalid_file", "file_too_large", "queued", "processing"}
        ),
        "db_busy": sum(1 for item in items if item["status"] == "db_busy"),
        "queued": sum(1 for item in items if item["status"] == "queued"),
        "processing": sum(1 for item in items if item["status"] == "processing"),
    }
    return {
        "batch_id": batch_id,
        "created_at": created_at,
        "summary": summary,
        "items": items,
        "history": list(history or []),
        "queue_status": dict(queue_status) if isinstance(queue_status, dict) else None,
    }


def build_upload_history_entry(
    batch_id: str,
    items: list[dict[str, Any]],
    *,
    created_at: str,
) -> dict[str, Any]:
    payload = build_upload_response(items, batch_id=batch_id, created_at=created_at)
    return {
        "batch_id": payload["batch_id"],
        "created_at": payload["created_at"],
        "summary": payload["summary"],
        "items": payload["items"],
    }


def get_current_user_queue_status_snapshot(user_id: int) -> dict[str, Any]:
    cached_payload = get_cached_user_queue_snapshot(user_id)
    if cached_payload is not None:
        return dict(cached_payload)
    status = db.get_queue_status(user_id)
    return build_user_queue_snapshot_payload(
        user_id,
        status,
        available_tokens=None,
    )


def update_user_upload_results_snapshot(
    user_id: int,
    batch_id: str,
    request_index: int,
    item_payload: dict[str, Any],
) -> dict[str, Any] | None:
    current = get_user_upload_results_snapshot(user_id)
    history = list(current.get("history") or [])
    batch_position = next(
        (index for index, batch in enumerate(history) if batch.get("batch_id") == batch_id),
        None,
    )
    if batch_position is None:
        return None
    batch_entry = dict(history[batch_position])
    items = list(batch_entry.get("items") or [])
    target_position = next(
        (
            index
            for index, existing in enumerate(items)
            if int(existing["request_index"]) == request_index
        ),
        None,
    )
    if target_position is None:
        return None
    merged = dict(items[target_position])
    merged.update(item_payload)
    items[target_position] = merged
    history[batch_position] = build_upload_history_entry(
        batch_id,
        items,
        created_at=str(batch_entry.get("created_at") or isoformat_now()),
    )
    latest = history[0] if history else get_default_upload_results_snapshot()
    payload = build_upload_response(
        list(latest.get("items") or []),
        batch_id=latest.get("batch_id"),
        created_at=latest.get("created_at"),
        history=history,
        queue_status=get_current_user_queue_status_snapshot(user_id),
    )
    return set_user_upload_results_snapshot(user_id, payload)


def write_uploaded_token_file(file_name: str, content_json: str) -> tuple[Path, str]:
    path = TOKEN_DIR / file_name
    _mark_internal_token_write(file_name)
    path.write_text(content_json, encoding="utf-8", newline="\n")
    return path, path.relative_to(BASE_DIR).as_posix()


def append_db_busy_results(
    results: list[dict[str, Any]],
    files: list[UploadTokenFilePayload],
    start_index: int,
) -> None:
    for request_index in range(start_index, len(files)):
        upload = files[request_index]
        results.append(
            {
                "request_index": request_index,
                "file_name": os.path.basename(upload.name or "upload.json"),
                "status": "db_busy",
                "reason": "数据库正忙，本文件尚未处理，请稍后重试。",
            }
        )


db = TokenDb(get_db_path())
db.init_db()
_CODEX_PROBE = codex_probe.CodexProbeQueue(
    delay_sec=get_codex_probe_delay_sec(),
    timeout_sec=get_codex_probe_timeout_sec(),
)
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
_CACHE_TTL_SEC = 5.0
_QUEUE_PUMP_THREAD: threading.Thread | None = None
_QUEUE_PUMP_STOP = threading.Event()
_QUEUE_PUMP_INTERVAL_SEC = 5.0
_TOKEN_IMPORT_THREAD: threading.Thread | None = None
_TOKEN_IMPORT_STOP = threading.Event()
_TOKEN_IMPORT_QUEUE: queue.Queue[tuple[str, int, str]] = queue.Queue()
_UPLOAD_TASK_THREAD: threading.Thread | None = None
_UPLOAD_TASK_STOP = threading.Event()
_UPLOAD_TASK_QUEUE: queue.Queue[QueuedUploadTask] = queue.Queue()
_UPLOAD_TASK_STATE_LOCK = threading.RLock()
_UPLOAD_TASK_ACTIVE_COUNT = 0
_HIDE_CLAIMS_THREAD: threading.Thread | None = None
_HIDE_CLAIMS_STOP = threading.Event()
_HIDE_CLAIMS_QUEUE: queue.Queue[HideClaimsTask] = queue.Queue()
_TOKEN_IMPORT_DELAYED_LOCK = threading.RLock()
_TOKEN_IMPORT_DELAYED_QUEUE: list[tuple[float, int, str, int, str]] = []
_TOKEN_IMPORT_DELAYED_SEQUENCE = itertools.count()
_TOKEN_IMPORT_PENDING_LOCK = threading.RLock()
_TOKEN_IMPORT_PENDING_FILES: set[str] = set()
_TOKEN_IMPORT_DIRTY_FILES: set[str] = set()
_TOKEN_IMPORT_INTERNAL_WRITES: dict[str, float] = {}
_TOKEN_IMPORT_RETRY_DELAY_SEC = 0.2
_TOKEN_IMPORT_MAX_RETRIES = 8
_TOKEN_IMPORT_INTERNAL_WRITE_TTL_SEC = 30.0
_TOKEN_IMPORT_FALLBACK_SCAN_INTERVAL_SEC = 30.0
_TOKEN_WATCHDOG_OBSERVER: Any = None
_API_KEY_CACHE_LOCK = threading.RLock()
_API_KEY_CACHE_BY_HASH: dict[str, dict[str, Any]] = {}
_APP_CACHE = AppCache()
_APP_CACHE.configure(emit_log=False)
_DASHBOARD_CACHE = DashboardMemoryCache()


def _refresh_dashboard_memory() -> None:
    before = _DASHBOARD_CACHE.get_system_status()
    _DASHBOARD_CACHE.refresh_from_db(db)
    after = _DASHBOARD_CACHE.get_system_status()
    invalidate_inventory_cache()
    invalidate_dashboard_cache()
    token_import_log(
        "[dashboard-refresh] "
        f"queue={before['queue'].get('total', 0)}->{after['queue'].get('total', 0)} "
        f"updated_at={before['index'].get('updated_at')}->{after['index'].get('updated_at')}"
    )


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
        queue_removed = bool(result.get("queue_removed"))
        refresh_queue_meta_runtime(db_handle)
        for event in result.get("events", []):
            user_id = int(event["user_id"])
            user = db_handle.get_user(int(event["user_id"]))
            if user:
                _DASHBOARD_CACHE.record_claim(
                    user=user,
                    request_id=str(event["request_id"]),
                    claimed_at_ts=int(event["claimed_at_ts"]),
                    token_ids=[int(token_id) for token_id in event.get("token_ids", [])],
                    first_claim_count=int(event.get("first_claim_count", 0)),
                    granted=int(event.get("granted", 0)),
                )
            if bool(event.get("queue_removed")):
                set_user_queue_snapshot(user_id, build_default_user_queue_snapshot(queued=False))
            else:
                set_user_queue_snapshot(
                    user_id,
                    build_user_queue_snapshot_payload(
                        user_id,
                        {
                            "queue_id": int(event.get("queue_id") or 0),
                            "position": int(event.get("queue_position") or 1),
                            "ahead": max(0, int(event.get("queue_position") or 1) - 1),
                            "requested": int(event.get("queue_requested") or 0),
                            "remaining": int(event.get("queue_remaining") or 0),
                            "enqueued_at": isoformat_from_ts(int(event.get("queue_enqueued_at_ts") or now_ts())),
                            "request_id": str(event["request_id"]),
                        },
                        available_tokens=get_global_available_tokens_estimate(),
                    ),
                )
            invalidate_user_profile_cache(user_id)
            invalidate_user_quota_cache(user_id)
            invalidate_user_claims_cache(user_id)
            invalidate_dashboard_cache(user_id=user_id)
            invalidate_admin_cache()
        if queue_removed:
            refresh_user_queue_snapshots(db_handle.list_queued_user_ids())
        invalidate_dashboard_cache()


def _cache_fresh(ts: float, ttl_sec: float | None = None) -> bool:
    return time.time() - ts < (ttl_sec if ttl_sec is not None else _CACHE_TTL_SEC)


def _queue_pump_loop() -> None:
    first_iteration = True
    while not _QUEUE_PUMP_STOP.is_set():
        try:
            if first_iteration or claim_queue.has_pending_queue(db):
                try_fulfill_queue(db)
        except Exception:
            pass
        first_iteration = False
        _QUEUE_PUMP_STOP.wait(timeout=_QUEUE_PUMP_INTERVAL_SEC)


def _cleanup_internal_token_writes(now: float | None = None) -> None:
    moment = time.time() if now is None else now
    expired = [
        file_name
        for file_name, expires_at in _TOKEN_IMPORT_INTERNAL_WRITES.items()
        if expires_at <= moment
    ]
    for file_name in expired:
        _TOKEN_IMPORT_INTERNAL_WRITES.pop(file_name, None)


def _get_token_import_state_counts() -> dict[str, int]:
    with _TOKEN_IMPORT_PENDING_LOCK:
        pending = len(_TOKEN_IMPORT_PENDING_FILES)
        dirty = len(_TOKEN_IMPORT_DIRTY_FILES)
    with _TOKEN_IMPORT_DELAYED_LOCK:
        delayed = len(_TOKEN_IMPORT_DELAYED_QUEUE)
    return {
        "ready": _TOKEN_IMPORT_QUEUE.qsize(),
        "pending": pending,
        "dirty": dirty,
        "delayed": delayed,
    }


def _log_token_import_state(prefix: str) -> None:
    counts = _get_token_import_state_counts()
    token_import_log(
        f"[token-state] {prefix} ready={counts['ready']} pending={counts['pending']} "
        f"dirty={counts['dirty']} delayed={counts['delayed']}"
    )


def _mark_internal_token_write(file_name: str) -> None:
    with _TOKEN_IMPORT_PENDING_LOCK:
        _cleanup_internal_token_writes()
        _TOKEN_IMPORT_INTERNAL_WRITES[file_name] = time.time() + _TOKEN_IMPORT_INTERNAL_WRITE_TTL_SEC


def _should_ignore_internal_token_write(file_name: str) -> bool:
    with _TOKEN_IMPORT_PENDING_LOCK:
        _cleanup_internal_token_writes()
        expires_at = _TOKEN_IMPORT_INTERNAL_WRITES.get(file_name)
        return expires_at is not None and expires_at > time.time()


def _enqueue_token_import(
    file_name: str,
    *,
    reason: str,
    attempt: int = 0,
    emit_log: bool = True,
) -> bool:
    if not file_name.lower().endswith(".json"):
        return False
    with _TOKEN_IMPORT_PENDING_LOCK:
        if attempt == 0:
            _cleanup_internal_token_writes()
            if file_name in _TOKEN_IMPORT_INTERNAL_WRITES:
                return False
        if file_name in _TOKEN_IMPORT_PENDING_FILES:
            if attempt == 0:
                _TOKEN_IMPORT_DIRTY_FILES.add(file_name)
                if emit_log:
                    _log_token_import_state(f"marked dirty file={file_name} reason={reason}")
            return False
        _TOKEN_IMPORT_PENDING_FILES.add(file_name)
        _TOKEN_IMPORT_DIRTY_FILES.discard(file_name)
    _TOKEN_IMPORT_QUEUE.put((file_name, attempt, reason))
    if emit_log:
        _log_token_import_state(f"queued file={file_name} reason={reason} attempt={attempt}")
    return True


def _compute_token_import_retry_delay(attempt: int) -> float:
    capped_attempt = max(0, min(attempt, 6))
    return _TOKEN_IMPORT_RETRY_DELAY_SEC * (2**capped_attempt)


def _schedule_delayed_token_import(
    file_name: str,
    *,
    reason: str,
    attempt: int,
    delay_sec: float,
) -> None:
    ready_at = time.monotonic() + max(0.0, delay_sec)
    with _TOKEN_IMPORT_DELAYED_LOCK:
        heapq.heappush(
            _TOKEN_IMPORT_DELAYED_QUEUE,
            (ready_at, next(_TOKEN_IMPORT_DELAYED_SEQUENCE), file_name, attempt, reason),
        )
    _log_token_import_state(
        f"scheduled retry file={file_name} reason={reason} attempt={attempt} delay={delay_sec:.2f}s"
    )


def _promote_ready_delayed_token_imports(now: float | None = None) -> int:
    promoted: list[tuple[str, int, str]] = []
    moment = time.monotonic() if now is None else now
    with _TOKEN_IMPORT_DELAYED_LOCK:
        while _TOKEN_IMPORT_DELAYED_QUEUE and _TOKEN_IMPORT_DELAYED_QUEUE[0][0] <= moment:
            _, _, file_name, attempt, reason = heapq.heappop(_TOKEN_IMPORT_DELAYED_QUEUE)
            promoted.append((file_name, attempt, reason))
    for task in promoted:
        _TOKEN_IMPORT_QUEUE.put(task)
    if promoted:
        _log_token_import_state(f"promoted delayed retries count={len(promoted)}")
    return len(promoted)


def _next_delayed_token_import_delay(now: float | None = None) -> float | None:
    moment = time.monotonic() if now is None else now
    with _TOKEN_IMPORT_DELAYED_LOCK:
        if not _TOKEN_IMPORT_DELAYED_QUEUE:
            return None
        return max(0.0, _TOKEN_IMPORT_DELAYED_QUEUE[0][0] - moment)


def _requeue_token_import(file_name: str, *, reason: str, attempt: int) -> None:
    delay_sec = _compute_token_import_retry_delay(attempt)
    _schedule_delayed_token_import(file_name, reason=reason, attempt=attempt, delay_sec=delay_sec)


def _finalize_token_import(file_name: str, *, rerun_reason: str) -> None:
    should_rerun = False
    with _TOKEN_IMPORT_PENDING_LOCK:
        _TOKEN_IMPORT_PENDING_FILES.discard(file_name)
        if file_name in _TOKEN_IMPORT_DIRTY_FILES:
            _TOKEN_IMPORT_DIRTY_FILES.discard(file_name)
            should_rerun = True
    if should_rerun and not _TOKEN_IMPORT_STOP.is_set():
        _enqueue_token_import(file_name, reason=rerun_reason)
        _log_token_import_state(f"rerun requested file={file_name} reason={rerun_reason}")
        return
    _log_token_import_state(f"released file={file_name} rerun={int(should_rerun)}")


def _release_token_import(file_name: str) -> None:
    _finalize_token_import(file_name, rerun_reason="dirty_rerun")


def _clear_delayed_token_imports() -> int:
    with _TOKEN_IMPORT_DELAYED_LOCK:
        cleared = len(_TOKEN_IMPORT_DELAYED_QUEUE)
        _TOKEN_IMPORT_DELAYED_QUEUE.clear()
    if cleared:
        _log_token_import_state(f"cleared delayed retries count={cleared}")
    return cleared


def _clear_token_import_queue() -> None:
    cleared = 0
    while True:
        try:
            _TOKEN_IMPORT_QUEUE.get_nowait()
        except queue.Empty:
            break
        else:
            cleared += 1
            _TOKEN_IMPORT_QUEUE.task_done()
    if cleared:
        _log_token_import_state(f"cleared ready queue count={cleared}")


def _is_token_file_present(path: Path) -> bool:
    try:
        return path.exists() and path.stat().st_size > 0
    except OSError:
        return False


def _startup_reconcile_token_files() -> dict[str, int]:
    file_stats = list_token_json_stats(TOKEN_DIR)
    if file_stats is None:
        token_import_log("startup reconcile skipped: unable to read token directory")
        return {"queued": 0, "total": 0, "deactivated": 0}
    if not file_stats:
        deactivated = db.deactivate_missing_token_files(set())
        return {"queued": 0, "total": 0, "deactivated": deactivated}
    try:
        known_states = db.list_token_file_states()
    except sqlite3.OperationalError as exc:
        token_import_log(f"startup reconcile skipped: database error: {exc}")
        return {"queued": 0, "total": len(file_stats), "deactivated": 0}
    queued = 0
    for file_name, (mtime_ns, _size) in file_stats.items():
        state = known_states.get(file_name)
        should_queue = False
        if state is None:
            should_queue = True
        elif state["is_active"] == 0:
            should_queue = True
        elif int(mtime_ns / 1_000_000_000) > state["updated_at_ts"]:
            should_queue = True
        if should_queue and _enqueue_token_import(file_name, reason="startup_reconcile", emit_log=False):
            queued += 1
    deactivated = db.deactivate_missing_token_files(set(file_stats))
    _log_token_import_state(
        f"startup reconcile queued={queued} total={len(file_stats)} deactivated={deactivated}"
    )
    return {"queued": queued, "total": len(file_stats), "deactivated": deactivated}


def _poll_token_directory_changes(previous_stats: dict[str, tuple[int, int]]) -> dict[str, tuple[int, int]]:
    current_stats = list_token_json_stats(TOKEN_DIR)
    if current_stats is None:
        token_import_log("fallback scan skipped: unable to read token directory")
        return previous_stats
    previous_names = set(previous_stats)
    current_names = set(current_stats)

    removed_names = sorted(previous_names - current_names)
    for file_name in removed_names:
        if db.deactivate_token_file(file_name, timeout=3.0):
            token_import_log(f"deactivated file={file_name} reason=poll_deleted")

    changed_count = 0
    for file_name, stat_pair in current_stats.items():
        previous_pair = previous_stats.get(file_name)
        if previous_pair != stat_pair:
            _enqueue_token_import(file_name, reason="poll_changed", emit_log=False)
            changed_count += 1

    token_import_log(
        "[token-scan] fallback scan finished "
        f"previous={len(previous_stats)} current={len(current_stats)} "
        f"changed={changed_count} removed={len(removed_names)}"
    )
    _log_token_import_state("fallback scan state")

    return current_stats


def _drain_token_import_batch(
    first_task: tuple[str, int, str],
    *,
    max_batch_size: int = 1024,
) -> list[tuple[str, int, str]]:
    tasks = [first_task]
    while len(tasks) < max_batch_size:
        try:
            tasks.append(_TOKEN_IMPORT_QUEUE.get_nowait())
        except queue.Empty:
            break
    return tasks


def _handle_token_import_results(
    tasks: list[tuple[str, int, str]],
    results: list[dict[str, Any]],
) -> bool:
    imported_any = False
    summary = {"imported": 0, "updated": 0, "deactivated": 0, "skipped": 0, "ignored": 0, "failed": 0}
    for (file_name, attempt, reason), result in zip(tasks, results):
        status = str(result.get("status", "unknown"))
        if status in {"imported", "updated"}:
            imported_any = True
            summary[status] += 1
            token_import_log(f"imported file={file_name} reason={reason}")
            _release_token_import(file_name)
            continue
        if status == "deactivated":
            summary["deactivated"] += 1
            token_import_log(f"deactivated file={file_name} reason={result.get('reason', reason)}")
            _release_token_import(file_name)
            continue
        if status == "skipped":
            summary["skipped"] += 1
            token_import_log(f"skipped file={file_name} reason={result.get('reason', 'unknown')}")
            _release_token_import(file_name)
            continue
        if status == "ignored":
            summary["ignored"] += 1
            _release_token_import(file_name)
            continue

        retryable = str(result.get("reason", "")).lower() in {
            "oserror",
            "unicodedecodeerror",
            "jsondecodeerror",
        }
        if retryable and attempt + 1 < _TOKEN_IMPORT_MAX_RETRIES:
            _requeue_token_import(
                file_name,
                reason=f"{reason}:{result.get('reason', 'retry')}",
                attempt=attempt + 1,
            )
            continue
        summary["failed"] += 1
        token_import_log(f"failed file={file_name} reason={result.get('reason', 'unknown')} attempts={attempt + 1}")
        _release_token_import(file_name)
    token_import_log(
        "batch result "
        f"size={len(tasks)} imported={summary['imported']} updated={summary['updated']} "
        f"deactivated={summary['deactivated']} skipped={summary['skipped']} "
        f"failed={summary['failed']}"
    )
    _log_token_import_state("batch result state")
    return imported_any


def _process_token_import_batch(tasks: list[tuple[str, int, str]]) -> None:
    ready_tasks: list[tuple[str, int, str]] = []
    ready_paths: list[Path] = []

    for file_name, attempt, reason in tasks:
        path = TOKEN_DIR / file_name
        if _is_token_file_present(path):
            ready_tasks.append((file_name, attempt, reason))
            ready_paths.append(path)
            continue
        if _TOKEN_IMPORT_STOP.is_set():
            continue
        if attempt + 1 < _TOKEN_IMPORT_MAX_RETRIES:
            _requeue_token_import(
                file_name,
                reason=f"{reason}:waiting_for_ready",
                attempt=attempt + 1,
            )
        else:
            token_import_log(f"failed file={file_name} reason=file_not_ready attempts={attempt + 1}")
            _release_token_import(file_name)

    if not ready_paths:
        return

    token_import_log(f"processing batch size={len(ready_paths)}")
    try:
        results = db.import_token_files(ready_paths, timeout=3.0)
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            for file_name, attempt, reason in ready_tasks:
                if attempt + 1 < _TOKEN_IMPORT_MAX_RETRIES:
                    _requeue_token_import(
                        file_name,
                        reason=f"{reason}:db_locked",
                        attempt=attempt + 1,
                    )
                else:
                    token_import_log(
                        f"failed file={file_name} reason=database_locked attempts={attempt + 1}"
                    )
                    _release_token_import(file_name)
            return
        for file_name, _attempt, _reason in ready_tasks:
            _release_token_import(file_name)
        token_import_log(f"batch failed reason=database_error error={type(exc).__name__}:{exc}")
        return

    imported_any = _handle_token_import_results(ready_tasks, results)
    if imported_any:
        try:
            try_fulfill_queue(db)
        except Exception as exc:
            token_import_log(f"queue fulfill after batch import failed error={type(exc).__name__}:{exc}")


class _TokenImportEventHandler(FileSystemEventHandler):
    def _is_token_path(self, raw_path: str) -> bool:
        try:
            return Path(raw_path).resolve().parent == TOKEN_DIR.resolve()
        except OSError:
            return False

    def _queue_event_path(self, raw_path: str, *, reason: str) -> None:
        if not self._is_token_path(raw_path):
            return
        file_name = Path(raw_path).name
        if not file_name.lower().endswith(".json"):
            return
        if _should_ignore_internal_token_write(file_name):
            token_import_log(f"ignored internal write file={file_name}")
            return
        _enqueue_token_import(file_name, reason=reason)

    def _deactivate_event_path(self, raw_path: str, *, reason: str) -> None:
        if not self._is_token_path(raw_path):
            return
        file_name = Path(raw_path).name
        if not file_name.lower().endswith(".json"):
            return
        if db.deactivate_token_file(file_name, timeout=3.0):
            token_import_log(f"deactivated file={file_name} reason={reason}")

    def on_created(self, event: FileSystemEvent) -> None:
        if getattr(event, "is_directory", False):
            return
        self._queue_event_path(str(event.src_path), reason="watch_created")

    def on_modified(self, event: FileSystemEvent) -> None:
        if getattr(event, "is_directory", False):
            return
        self._queue_event_path(str(event.src_path), reason="watch_modified")

    def on_deleted(self, event: FileSystemEvent) -> None:
        if getattr(event, "is_directory", False):
            return
        self._deactivate_event_path(str(event.src_path), reason="watch_deleted")

    def on_moved(self, event: FileMovedEvent) -> None:
        if getattr(event, "is_directory", False):
            return
        self._deactivate_event_path(str(event.src_path), reason="watch_moved_out")
        self._queue_event_path(str(event.dest_path), reason="watch_moved_in")


def _start_token_watchdog() -> Any:
    if Observer is None:
        token_import_log("watchdog unavailable; using fallback polling scanner")
        return None
    observer = Observer()
    observer.schedule(_TokenImportEventHandler(), str(TOKEN_DIR), recursive=False)
    observer.start()
    token_import_log(f"watcher started dir={TOKEN_DIR}")
    return observer


def _token_import_loop() -> None:
    global _TOKEN_WATCHDOG_OBSERVER
    token_import_log("background worker started")
    reconcile_result = _startup_reconcile_token_files()
    token_import_log(
        "startup reconcile finished "
        f"queued={reconcile_result['queued']} total={reconcile_result['total']} "
        f"deactivated={reconcile_result['deactivated']}"
    )
    observer = _start_token_watchdog()
    _TOKEN_WATCHDOG_OBSERVER = observer
    fallback_snapshot: dict[str, tuple[int, int]] = list_token_json_stats(TOKEN_DIR) or {}
    next_fallback_scan_at = time.time() + _TOKEN_IMPORT_FALLBACK_SCAN_INTERVAL_SEC
    try:
        while not _TOKEN_IMPORT_STOP.is_set():
            _promote_ready_delayed_token_imports()
            try:
                fallback_wait = max(0.0, next_fallback_scan_at - time.time())
                delayed_wait = _next_delayed_token_import_delay()
                wait_timeout = 0.5
                if delayed_wait is not None:
                    wait_timeout = min(wait_timeout, delayed_wait)
                wait_timeout = min(wait_timeout, fallback_wait)
                first_task = _TOKEN_IMPORT_QUEUE.get(timeout=wait_timeout)
            except queue.Empty:
                if time.time() >= next_fallback_scan_at:
                    fallback_snapshot = _poll_token_directory_changes(fallback_snapshot)
                    next_fallback_scan_at = time.time() + _TOKEN_IMPORT_FALLBACK_SCAN_INTERVAL_SEC
                continue
            batch_tasks = _drain_token_import_batch(first_task)
            _log_token_import_state(f"processing dequeued batch size={len(batch_tasks)}")
            try:
                _process_token_import_batch(batch_tasks)
            finally:
                for _ in batch_tasks:
                    _TOKEN_IMPORT_QUEUE.task_done()
            if time.time() >= next_fallback_scan_at:
                fallback_snapshot = _poll_token_directory_changes(fallback_snapshot)
                next_fallback_scan_at = time.time() + _TOKEN_IMPORT_FALLBACK_SCAN_INTERVAL_SEC
    finally:
        if observer is not None:
            observer.stop()
            observer.join(timeout=5)
            token_import_log("watcher stopped")
        _TOKEN_WATCHDOG_OBSERVER = None


def _clear_upload_task_queue() -> None:
    cleared = 0
    while True:
        try:
            _UPLOAD_TASK_QUEUE.get_nowait()
        except queue.Empty:
            break
        else:
            cleared += 1
            _UPLOAD_TASK_QUEUE.task_done()
    if cleared:
        upload_log(f"[upload-worker] cleared queued tasks count={cleared}")


def _clear_hide_claims_queue() -> None:
    cleared = 0
    while True:
        try:
            _HIDE_CLAIMS_QUEUE.get_nowait()
        except queue.Empty:
            break
        else:
            cleared += 1
            _HIDE_CLAIMS_QUEUE.task_done()
    if cleared:
        upload_log(f"[hide-claims] cleared queued tasks count={cleared}")


def get_upload_task_queue_position(offset: int = 0) -> int:
    with _UPLOAD_TASK_STATE_LOCK:
        active = _UPLOAD_TASK_ACTIVE_COUNT
    return max(1, int(active) + _UPLOAD_TASK_QUEUE.qsize() + int(offset) + 1)


def _process_upload_task(task: QueuedUploadTask) -> None:
    update_user_upload_results_snapshot(
        task.user_id,
        task.batch_id,
        task.request_index,
        {
            "status": "processing",
            "reason": "正在处理，请稍候刷新结果。",
        },
    )
    upload_log(
        f"[upload-worker] start user_id={task.user_id} batch={task.batch_id} "
        f"index={task.request_index} file={task.file_name} account_id={task.account_id}"
    )
    result_item: dict[str, Any]
    try:
        try:
            conflict = db.get_uploaded_token_conflict(
                account_id=task.account_id,
                access_token_hash=task.access_token_hash,
            )
        except sqlite3.OperationalError as exc:
            if "database is locked" in str(exc).lower():
                result_item = {
                    "status": "db_busy",
                    "reason": "数据库正忙，本文件尚未处理，请稍后重试。",
                }
            else:
                raise
        else:
            if conflict:
                result_item = {
                    "status": "duplicate",
                    "reason": "账号已存在于历史库存中",
                    "account_id": task.account_id,
                }
            else:
                probe_result = _CODEX_PROBE.submit(
                    {
                        "account_id": task.account_id,
                        "access_token": task.access_token,
                        "refresh_token": task.refresh_token,
                    },
                    wait_timeout_sec=get_codex_probe_timeout_sec() + get_codex_probe_delay_sec() + 5.0,
                )
                if probe_result.is_banned:
                    result_item = {
                        "status": "banned_401",
                        "reason": "账号已失效或被上游封禁",
                    }
                elif probe_result.detail == "probe_queue_timeout":
                    result_item = {
                        "status": "probe_timeout",
                        "reason": "账号探活超时，请稍后重试",
                    }
                elif probe_result.status != "ok":
                    reason = "账号探活失败"
                    if probe_result.http_status is not None:
                        reason = f"账号探活请求异常（HTTP {probe_result.http_status}）"
                    elif probe_result.detail:
                        reason = f"账号探活请求异常（{probe_result.detail}）"
                    result_item = {
                        "status": "probe_failed",
                        "reason": reason,
                    }
                else:
                    uploaded_at_ts = now_ts()
                    target_name = build_uploaded_filename(
                        account_id=task.account_id,
                        uploaded_at_ts=uploaded_at_ts,
                    )
                    file_path_obj, relative_path = write_uploaded_token_file(target_name, task.content_json)
                    try:
                        created = run_db_write_with_retry(
                            lambda: db.create_uploaded_token(
                                file_name=target_name,
                                file_path=relative_path,
                                content_json=task.content_json,
                                provider={
                                    "id": task.user_id,
                                    "username": task.provider_username,
                                    "name": task.provider_name,
                                },
                                uploaded_at_ts=uploaded_at_ts,
                                hourly_success_limit=task.hourly_limit,
                                timeout_s=3.0,
                            ),
                            retries=2,
                            delay_sec=0.2,
                        )
                    except UploadValidationError:
                        file_path_obj.unlink(missing_ok=True)
                        result_item = {
                            "status": "duplicate",
                            "reason": "账号已存在于历史库存中",
                            "account_id": task.account_id,
                        }
                    except RateLimitError:
                        file_path_obj.unlink(missing_ok=True)
                        result_item = {
                            "status": "rate_limited",
                            "reason": "当前小时上传额度不足",
                        }
                    except sqlite3.OperationalError as exc:
                        file_path_obj.unlink(missing_ok=True)
                        if "database is locked" in str(exc).lower():
                            result_item = {
                                "status": "db_busy",
                                "reason": "数据库正忙，本文件尚未处理，请稍后重试。",
                            }
                        else:
                            raise
                    except Exception:
                        file_path_obj.unlink(missing_ok=True)
                        raise
                    else:
                        invalidate_user_cache(task.user_id)
                        result_item = {
                            "status": "accepted",
                            "reason": "校验通过，已加入可领取库存",
                            "token_id": created["token_id"],
                            "account_id": created["account_id"],
                        }
    except Exception as exc:
        upload_log(
            f"[upload-worker] failed user_id={task.user_id} batch={task.batch_id} "
            f"index={task.request_index} file={task.file_name} "
            f"error={type(exc).__name__}:{exc}"
        )
        result_item = {
            "status": "probe_failed",
            "reason": "后台处理失败，请稍后重试",
        }
    update_user_upload_results_snapshot(task.user_id, task.batch_id, task.request_index, result_item)
    upload_log(
        f"[upload-worker] finished user_id={task.user_id} batch={task.batch_id} "
        f"index={task.request_index} file={task.file_name} status={result_item['status']}"
    )


def _upload_task_loop() -> None:
    global _UPLOAD_TASK_ACTIVE_COUNT
    upload_log("[upload-worker] background worker started")
    while not _UPLOAD_TASK_STOP.is_set():
        try:
            task = _UPLOAD_TASK_QUEUE.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            with _UPLOAD_TASK_STATE_LOCK:
                _UPLOAD_TASK_ACTIVE_COUNT += 1
            _process_upload_task(task)
        finally:
            with _UPLOAD_TASK_STATE_LOCK:
                _UPLOAD_TASK_ACTIVE_COUNT = max(0, _UPLOAD_TASK_ACTIVE_COUNT - 1)
            _UPLOAD_TASK_QUEUE.task_done()
    upload_log("[upload-worker] background worker stopped")


def _hide_claims_loop() -> None:
    upload_log("[hide-claims] background worker started")
    while not _HIDE_CLAIMS_STOP.is_set():
        try:
            task = _HIDE_CLAIMS_QUEUE.get(timeout=0.5)
        except queue.Empty:
            continue
        try:
            hidden = db.hide_claims(task.user_id, task.claim_ids)
            upload_log(
                f"[hide-claims] finished user_id={task.user_id} "
                f"requested={len(task.claim_ids)} hidden={hidden}"
            )
        except Exception as exc:
            upload_log(
                f"[hide-claims] failed user_id={task.user_id} "
                f"claims={len(task.claim_ids)} error={type(exc).__name__}:{exc}"
            )
        finally:
            _HIDE_CLAIMS_QUEUE.task_done()
    upload_log("[hide-claims] background worker stopped")


@asynccontextmanager
async def lifespan(_: FastAPI):
    startup_begin = time.perf_counter()

    def run_startup_step(name: str, func: Callable[[], None]) -> None:
        step_begin = time.perf_counter()
        startup_log(f"{name} started")
        try:
            func()
        except Exception as exc:
            startup_log(
                f"{name} failed after {time.perf_counter() - step_begin:.3f}s: "
                f"{type(exc).__name__}: {exc}"
            )
            raise
        startup_log(f"{name} finished in {time.perf_counter() - step_begin:.3f}s")

    startup_log(f"application startup begin pid={os.getpid()}")
    startup_log(f"token directory={TOKEN_DIR}")
    token_file_count = count_token_files(TOKEN_DIR)
    if token_file_count is None:
        startup_log("token file count unavailable before sync")
    else:
        startup_log(f"token files detected before sync={token_file_count}")

    run_startup_step("ensure token directory", lambda: TOKEN_DIR.mkdir(parents=True, exist_ok=True))
    run_startup_step("configure cache backend", lambda: _APP_CACHE.configure(emit_log=True))
    run_startup_step("start codex probe", _CODEX_PROBE.start)
    run_startup_step("initialize database", db.init_db)
    run_startup_step("refresh dashboard memory", _refresh_dashboard_memory)
    run_startup_step("rebuild queue snapshots", rebuild_all_queue_snapshots_from_db)
    startup_log(f"application startup finished in {time.perf_counter() - startup_begin:.3f}s")

    _QUEUE_PUMP_STOP.clear()
    _TOKEN_IMPORT_STOP.clear()
    _UPLOAD_TASK_STOP.clear()
    _HIDE_CLAIMS_STOP.clear()
    _clear_token_import_queue()
    _clear_delayed_token_imports()
    _clear_upload_task_queue()
    _clear_hide_claims_queue()
    with _UPLOAD_TASK_STATE_LOCK:
        global _UPLOAD_TASK_ACTIVE_COUNT
        _UPLOAD_TASK_ACTIVE_COUNT = 0
    with _TOKEN_IMPORT_PENDING_LOCK:
        _TOKEN_IMPORT_PENDING_FILES.clear()
        _TOKEN_IMPORT_DIRTY_FILES.clear()
        _TOKEN_IMPORT_INTERNAL_WRITES.clear()
    global _QUEUE_PUMP_THREAD
    global _TOKEN_IMPORT_THREAD
    global _UPLOAD_TASK_THREAD
    global _HIDE_CLAIMS_THREAD
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
    _UPLOAD_TASK_THREAD = threading.Thread(
        target=_upload_task_loop,
        daemon=True,
        name="upload-task-pump",
    )
    _UPLOAD_TASK_THREAD.start()
    _HIDE_CLAIMS_THREAD = threading.Thread(
        target=_hide_claims_loop,
        daemon=True,
        name="hide-claims-pump",
    )
    _HIDE_CLAIMS_THREAD.start()

    try:
        yield
    finally:
        _QUEUE_PUMP_STOP.set()
        _TOKEN_IMPORT_STOP.set()
        _UPLOAD_TASK_STOP.set()
        _HIDE_CLAIMS_STOP.set()
        _CODEX_PROBE.stop()
        if _QUEUE_PUMP_THREAD is not None:
            _QUEUE_PUMP_THREAD.join(timeout=5)
            _QUEUE_PUMP_THREAD = None
        if _TOKEN_IMPORT_THREAD is not None:
            _TOKEN_IMPORT_THREAD.join(timeout=5)
            _TOKEN_IMPORT_THREAD = None
        if _UPLOAD_TASK_THREAD is not None:
            _UPLOAD_TASK_THREAD.join(timeout=5)
            _UPLOAD_TASK_THREAD = None
        if _HIDE_CLAIMS_THREAD is not None:
            _HIDE_CLAIMS_THREAD.join(timeout=5)
            _HIDE_CLAIMS_THREAD = None
        _clear_token_import_queue()
        _clear_delayed_token_imports()
        _clear_upload_task_queue()
        _clear_hide_claims_queue()
        with _UPLOAD_TASK_STATE_LOCK:
            _UPLOAD_TASK_ACTIVE_COUNT = 0
        with _TOKEN_IMPORT_PENDING_LOCK:
            _TOKEN_IMPORT_PENDING_FILES.clear()
            _TOKEN_IMPORT_DIRTY_FILES.clear()
            _TOKEN_IMPORT_INTERNAL_WRITES.clear()


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


def _build_admin_me_payload(context: dict[str, Any]) -> dict[str, Any]:
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


@app.get("/admin/me")
def get_admin_me(request: Request) -> dict[str, Any]:
    context = require_admin_user(request)
    key = build_admin_cache_key("admin-me", context["user_id"])
    return cache_json(
        key,
        get_cache_admin_ttl(),
        lambda: _build_admin_me_payload(context),
    )


@app.get("/admin/users")
def admin_list_users(
    request: Request,
    search: str = Query(default=""),
    ban_status: str = Query(default="all"),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    require_admin_user(request)
    key = build_admin_cache_key("admin-users", search.strip().lower(), ban_status, limit, offset)
    return cache_json(
        key,
        get_cache_admin_ttl(),
        lambda: db.list_users_for_admin(search=search, ban_status=ban_status, limit=limit, offset=offset),
    )


@app.get("/admin/users/{linuxdo_user_id}")
def admin_get_user_detail(request: Request, linuxdo_user_id: str) -> dict[str, Any]:
    require_admin_user(request)
    key = build_admin_cache_key("admin-user-detail", linuxdo_user_id)
    detail = cache_json(key, get_cache_admin_ttl(), lambda: db.get_admin_user_detail(linuxdo_user_id))
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
    key = build_admin_cache_key("admin-bans", status_filter, search.strip().lower(), limit, offset)
    return cache_json(
        key,
        get_cache_admin_ttl(),
        lambda: db.list_bans(status_filter=status_filter, search=search, limit=limit, offset=offset),
    )


@app.get("/admin/tokens")
def admin_list_tokens(
    request: Request,
    search: str = Query(default=""),
    status_filter: str = Query(default="all", alias="status"),
    limit: int = Query(default=200, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    require_admin_user(request)
    key = build_admin_cache_key("admin-tokens", search.strip().lower(), status_filter, limit, offset)
    return cache_json(
        key,
        get_cache_admin_ttl(),
        lambda: db.list_tokens_for_admin(search=search, status_filter=status_filter, limit=limit, offset=offset),
    )


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
def admin_cleanup_exhausted_tokens(
    request: Request,
    payload: TokenCleanupPayload | None = Body(default=None),
) -> dict[str, Any]:
    require_admin_user(request)
    mode = (payload.mode if payload else "files_only").strip().lower()
    if mode not in {"files_only", "files_and_db"}:
        error_json = json.dumps({"detail": "Invalid cleanup mode."}, ensure_ascii=False).encode("utf-8")
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content=error_json, media_type="application/json")
    try:
        result = run_db_write_with_retry(
            lambda: db.cleanup_exhausted_tokens_with_mode(TOKEN_DIR, mode=mode),
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
    return get_user_profile_snapshot(user_id, is_admin=bool(session["is_admin"]))


@app.get("/me/quota")
def get_me_quota(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    return get_user_quota_snapshot(session["user_id"])


@app.get("/me/claims-summary")
def get_me_claims_summary(request: Request) -> dict[str, int]:
    session = require_session_user(request)
    return get_user_claim_summary_snapshot(session["user_id"])


@app.get("/me/api-key-summary")
def get_me_apikey_summary(request: Request) -> dict[str, int]:
    session = require_session_user(request)
    return get_user_api_key_summary_snapshot(session["user_id"])


@app.get("/me/uploads/results")
def get_me_upload_results(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    payload = get_user_upload_results_snapshot(session["user_id"])
    payload["queue_status"] = get_current_user_queue_status_snapshot(session["user_id"])
    return payload


@app.get("/dashboard/leaderboard")
def get_dashboard_leaderboard(
    request: Request,
    window: str | None = Query(default="24h"),
    limit: int = Query(default=10, ge=1, le=10),
) -> dict[str, Any]:
    require_session_user(request)
    window_sec = parse_window_to_seconds(window, 24 * 3600, max_seconds=7 * 24 * 3600)
    limit = min(limit, 10)
    key = build_dashboard_cache_key("dashboard-leaderboard", window_sec, limit)
    return cache_json(key, get_cache_dashboard_ttl(), lambda: _DASHBOARD_CACHE.get_leaderboard(window_sec, limit))


@app.get("/dashboard/summary")
def get_dashboard_summary(
    request: Request,
    window: str | None = Query(default="7d"),
    bucket: str | None = Query(default="1h"),
    leaderboard_window: str | None = Query(default="24h"),
    leaderboard_limit: int = Query(default=10, ge=1, le=10),
    recent_limit: int = Query(default=10, ge=1, le=10),
    contributor_limit: int = Query(default=10, ge=1, le=10),
    recent_contributor_limit: int = Query(default=10, ge=1, le=10),
) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    leaderboard_window_sec = parse_window_to_seconds(
        leaderboard_window, 24 * 3600, max_seconds=7 * 24 * 3600
    )
    leaderboard_limit = min(leaderboard_limit, 10)
    recent_limit = min(recent_limit, 10)
    contributor_limit = min(contributor_limit, 10)
    recent_contributor_limit = min(recent_contributor_limit, 10)
    window_sec = parse_window_to_seconds(window, 7 * 24 * 3600, max_seconds=14 * 24 * 3600)
    bucket_sec = parse_bucket_seconds(bucket, 3600)
    key = build_dashboard_cache_key(
        "dashboard-summary",
        window_sec,
        bucket_sec,
        leaderboard_window_sec,
        leaderboard_limit,
        recent_limit,
        contributor_limit,
        recent_contributor_limit,
        user_id=user_id,
    )
    def _load_summary() -> dict[str, Any]:
        full_stats = _DASHBOARD_CACHE.get_stats(user_id)
        stats = {
            "total_tokens": full_stats["total_tokens"],
            "available_tokens": full_stats["available_tokens"],
            "claimed_total": full_stats["claimed_total"],
            "claimed_unique": full_stats["claimed_unique"],
            "others_claimed_total": full_stats["others_claimed_total"],
            "others_claimed_unique": full_stats["others_claimed_unique"],
        }
        leaderboard = _DASHBOARD_CACHE.get_leaderboard(leaderboard_window_sec, leaderboard_limit)
        recent = _DASHBOARD_CACHE.get_recent(recent_limit)
        contributors = {
            "items": db.get_contributor_leaderboard(contributor_limit),
        }
        recent_contributors = {
            "items": db.list_recent_contributors(recent_contributor_limit),
        }
        trends = _DASHBOARD_CACHE.get_trends(window_sec, bucket_sec)
        system = _DASHBOARD_CACHE.get_system_status()
        return {
            "stats": stats,
            "leaderboard": leaderboard,
            "recent": recent,
            "contributors": contributors,
            "recent_contributors": recent_contributors,
            "trends": trends,
            "system": system,
        }
    return cache_json(key, get_cache_dashboard_ttl(), _load_summary)


@app.get("/dashboard/recent-claims")
def get_dashboard_recent_claims(
    request: Request,
    limit: int = Query(default=10, ge=1, le=10),
) -> dict[str, Any]:
    require_session_user(request)
    key = build_dashboard_cache_key("dashboard-recent", limit)
    return cache_json(key, get_cache_dashboard_ttl(), lambda: _DASHBOARD_CACHE.get_recent(limit))


@app.get("/dashboard/trends")
def get_dashboard_trends(
    request: Request,
    window: str | None = Query(default="7d"),
    bucket: str | None = Query(default="1h"),
) -> dict[str, Any]:
    require_session_user(request)
    window_sec = parse_window_to_seconds(window, 7 * 24 * 3600, max_seconds=14 * 24 * 3600)
    bucket_sec = parse_bucket_seconds(bucket, 3600)
    key = build_dashboard_cache_key("dashboard-trends", window_sec, bucket_sec)
    return cache_json(key, get_cache_dashboard_ttl(), lambda: _DASHBOARD_CACHE.get_trends(window_sec, bucket_sec))


@app.get("/dashboard/system-status")
def get_dashboard_system_status(request: Request) -> dict[str, Any]:
    require_session_user(request)
    key = build_dashboard_cache_key("dashboard-system")
    return cache_json(key, get_cache_dashboard_ttl(), lambda: _DASHBOARD_CACHE.get_system_status())


@app.get("/dashboard/stats")
def get_dashboard_stats(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    key = build_dashboard_cache_key("dashboard-stats", user_id=user_id)
    def _load_stats() -> dict[str, Any]:
        stats = _DASHBOARD_CACHE.get_stats(user_id)
        return {
            "total_tokens": stats["total_tokens"],
            "available_tokens": stats["available_tokens"],
            "claimed_total": stats["claimed_total"],
            "claimed_unique": stats["claimed_unique"],
            "others_claimed_total": stats["others_claimed_total"],
            "others_claimed_unique": stats["others_claimed_unique"],
        }
    return cache_json(key, get_cache_dashboard_ttl(), _load_stats)


@app.get("/me/api-keys")
def list_api_keys(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    return get_user_api_keys_snapshot(user_id)


@app.post("/me/api-keys")
def create_api_key(request: Request, payload: ApiKeyCreatePayload | None = Body(default=None)) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    name = payload.name if payload else None
    created = db.create_api_key(user_id, name)
    return created


@app.post("/me/api-keys/{api_key_id}/revoke")
def revoke_api_key(request: Request, api_key_id: int) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    return db.revoke_api_key(user_id, api_key_id)


@app.post("/me/uploads/tokens")
def upload_tokens_session(
    request: Request,
    payload: UploadTokensPayload | None = Body(default=None),
) -> dict[str, Any]:
    require_web_upload_request(request)
    session = require_session_user(request)
    user = session["user"]
    max_files = get_upload_max_files_per_request()
    max_file_size = get_upload_max_file_size_bytes()
    hourly_limit = get_upload_max_success_per_hour()
    files = payload.files if payload else []
    upload_log(
        f"[upload-request] user_id={user['id']} username={user['username']} "
        f"files={len(files)} max_files={max_files} max_size={max_file_size}"
    )

    if not files:
        upload_log(f"[upload-request] rejected user_id={user['id']} reason=no_files")
        error_json = json.dumps({"detail": "请至少上传一个 JSON 文件。"}, ensure_ascii=False).encode("utf-8")
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content=error_json, media_type="application/json")
    if len(files) > max_files:
        upload_log(
            f"[upload-request] rejected user_id={user['id']} reason=too_many_files "
            f"files={len(files)} max_files={max_files}"
        )
        error_json = json.dumps(
            {"detail": f"单次最多上传 {max_files} 个文件。"},
            ensure_ascii=False,
        ).encode("utf-8")
        return Response(status_code=status.HTTP_400_BAD_REQUEST, content=error_json, media_type="application/json")

    try:
        uploaded_count = db.count_recent_successful_uploads(user["id"])
    except sqlite3.OperationalError as exc:
        if "database is locked" in str(exc).lower():
            upload_log(f"[upload-request] db_busy_on_count user_id={user['id']}")
            return make_db_busy_response()
        raise
    remaining_success = max(0, hourly_limit - uploaded_count)
    upload_log(
        f"[upload-request] quota user_id={user['id']} uploaded_last_hour={uploaded_count} "
        f"remaining={remaining_success} hourly_limit={hourly_limit}"
    )
    if remaining_success <= 0:
        upload_log(f"[upload-request] rate_limited user_id={user['id']} remaining=0")
        raise RateLimitError("当前小时内可成功上传的账号数量已用完")

    results: list[dict[str, Any]] = []
    queued_tasks: list[QueuedUploadTask] = []
    batch_id = secrets.token_hex(8)
    created_at = isoformat_now()
    for request_index, upload in enumerate(files):
        original_name = os.path.basename(upload.name or "upload.json")
        upload_log(
            f"[upload-file] start user_id={user['id']} index={request_index} file={original_name}"
        )
        if not original_name.lower().endswith(".json"):
            upload_log(
                f"[upload-file] reject user_id={user['id']} index={request_index} "
                f"file={original_name} reason=invalid_extension"
            )
            results.append({"request_index": request_index, "file_name": original_name, "status": "invalid_file", "reason": "仅支持上传 .json 文件"})
            continue
        try:
            raw = base64.b64decode(upload.content_base64 or "", validate=True)
        except (ValueError, TypeError, binascii.Error):
            upload_log(
                f"[upload-file] reject user_id={user['id']} index={request_index} "
                f"file={original_name} reason=invalid_base64"
            )
            results.append({"request_index": request_index, "file_name": original_name, "status": "invalid_file", "reason": "文件内容编码无效"})
            continue
        if not raw:
            upload_log(
                f"[upload-file] reject user_id={user['id']} index={request_index} "
                f"file={original_name} reason=empty_file"
            )
            results.append({"request_index": request_index, "file_name": original_name, "status": "invalid_json", "reason": "文件为空"})
            continue
        if len(raw) > max_file_size:
            upload_log(
                f"[upload-file] reject user_id={user['id']} index={request_index} "
                f"file={original_name} reason=file_too_large size={len(raw)} max_size={max_file_size}"
            )
            results.append(
                {
                    "request_index": request_index,
                    "file_name": original_name,
                    "status": "file_too_large",
                    "reason": f"单文件不能超过 {max_file_size} 字节",
                }
            )
            continue
        try:
            _, normalized, content_json = load_uploaded_json(raw)
        except json.JSONDecodeError:
            upload_log(
                f"[upload-file] reject user_id={user['id']} index={request_index} "
                f"file={original_name} reason=json_decode_error"
            )
            results.append({"request_index": request_index, "file_name": original_name, "status": "invalid_json", "reason": "JSON 解析失败"})
            continue
        except UploadValidationError as exc:
            upload_log(
                f"[upload-file] reject user_id={user['id']} index={request_index} "
                f"file={original_name} reason=validation_error detail={exc}"
            )
            results.append({"request_index": request_index, "file_name": original_name, "status": "missing_fields", "reason": str(exc)})
            continue

        access_token_hash = hash_token_value(normalized["access_token"])
        queue_position = get_upload_task_queue_position(len(queued_tasks))
        upload_log(
            f"[upload-queue] queued user_id={user['id']} index={request_index} "
            f"file={original_name} account_id={normalized['account_id']} position={queue_position}"
        )
        results.append(
            {
                "request_index": request_index,
                "file_name": original_name,
                "status": "queued",
                "reason": "已进入上传队列。",
                "account_id": normalized["account_id"],
                "queue_position": queue_position,
            }
        )
        queued_tasks.append(
            QueuedUploadTask(
                batch_id=batch_id,
                user_id=int(user["id"]),
                provider_username=str(user["username"]),
                provider_name=str(user["name"] or user["username"]),
                hourly_limit=hourly_limit,
                request_index=request_index,
                file_name=original_name,
                content_json=content_json,
                account_id=normalized["account_id"],
                access_token=normalized["access_token"],
                refresh_token=normalized["refresh_token"],
                access_token_hash=access_token_hash,
            )
        )

    queue_status_payload = get_current_user_queue_status_snapshot(session["user_id"])
    previous_snapshot = get_user_upload_results_snapshot(session["user_id"])
    history = list(previous_snapshot.get("history") or [])
    history.insert(
        0,
        build_upload_history_entry(
            batch_id,
            results,
            created_at=created_at,
        ),
    )
    history = history[:20]
    response_payload = build_upload_response(
        results,
        batch_id=batch_id,
        created_at=created_at,
        history=history,
        queue_status=queue_status_payload,
    )
    set_user_upload_results_snapshot(session["user_id"], response_payload)
    for task in queued_tasks:
        _UPLOAD_TASK_QUEUE.put(task)
    success_count = sum(1 for item in results if item.get("status") == "accepted")
    upload_log(
        f"[upload-request] finished user_id={user['id']} total={len(results)} "
        f"accepted={success_count} queued={len(queued_tasks)} batch={batch_id}"
    )
    return response_payload


@app.get("/me/claims")
def list_claims(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    key = build_cache_key("me-claims", user_id, f"v{get_cache_scope_version('user-claims', user_id)}")
    def _load_claims() -> dict[str, Any]:
        items = db.list_claims(user_id)
        for item in items:
            item["download_url"] = build_download_url(request, item["token_id"])
        return {"items": items}
    return cache_json(key, get_cache_claims_ttl(), _load_claims)


@app.get("/me/queue-status")
def get_queue_status(request: Request) -> dict[str, Any]:
    session = require_session_user(request)
    payload = get_current_user_queue_status_snapshot(session["user_id"])
    if get_cached_user_queue_snapshot(session["user_id"]) is None:
        return set_user_queue_snapshot(session["user_id"], payload)
    return payload


@app.post("/me/claims/hide")
def hide_claims(request: Request, payload: ClaimHidePayload) -> dict[str, Any]:
    session = require_session_user(request)
    user_id = session["user_id"]
    claim_ids = [int(cid) for cid in payload.claim_ids if isinstance(cid, int) or str(cid).isdigit()]
    if not claim_ids:
        return {"queued": 0, "accepted": 0}
    unique_ids = sorted({int(claim_id) for claim_id in claim_ids})
    invalidate_user_claims_cache(user_id)
    invalidate_user_profile_cache(user_id)
    _HIDE_CLAIMS_QUEUE.put(HideClaimsTask(user_id=user_id, claim_ids=unique_ids))
    return {"queued": len(unique_ids), "accepted": len(unique_ids)}


@app.get("/me/claims/archive")
def download_claims_archive(request: Request) -> StreamingResponse:
    session = require_session_user(request)
    user_id = session["user_id"]
    key = build_cache_key("me-claim-files", user_id, f"v{get_cache_scope_version('user-claims', user_id)}")
    items = cache_json(key, get_cache_claims_ttl(), lambda: db.list_claim_files(user_id))
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

    key = build_cache_key("claimed-token", user_id, token_id, f"v{get_cache_scope_version('user-claims', user_id)}")
    token = cache_json(key, get_cache_claims_ttl(), lambda: db.get_claimed_token(token_id, user_id))
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
        access_log=False,
        log_level="warning",
    )
