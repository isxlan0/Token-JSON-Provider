from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import Body, Depends, FastAPI, Header, Query, Response, status
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

BASE_DIR = Path(__file__).resolve().parent
TOKEN_DIR = BASE_DIR / "token"
STATIC_DIR = BASE_DIR / "static"
ENV_FILE = BASE_DIR / ".env"

ACCESS_KEY_HEADER = "X-Access-Key"
ACCESS_KEY_ENV = "TOKEN_INDEX_ACCESS_KEY"
DEFAULT_ACCESS_KEY = "change-me-token-index"


def isoformat_timestamp(timestamp: float) -> str:
    return (
        datetime.fromtimestamp(timestamp, tz=timezone.utc)
        .astimezone()
        .isoformat(timespec="seconds")
    )


def isoformat_now() -> str:
    return datetime.now(tz=timezone.utc).astimezone().isoformat(timespec="seconds")


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
        return

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


load_dotenv_file(ENV_FILE)


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


class LoginPayload(BaseModel):
    password: str


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
    def __init__(self, store: TokenIndexStore) -> None:
        self.store = store

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return

        paths = [getattr(event, "src_path", ""), getattr(event, "dest_path", "")]
        if any(path.lower().endswith(".json") for path in paths if path):
            self.store.refresh_all()


def get_expected_access_key() -> str:
    return os.getenv(ACCESS_KEY_ENV, DEFAULT_ACCESS_KEY)


def verify_access_key(
    x_access_key: str | None = Header(default=None, alias=ACCESS_KEY_HEADER),
) -> None:
    if x_access_key != get_expected_access_key():
        raise PermissionError


store = TokenIndexStore(TOKEN_DIR)
observer: Observer | None = None
app = FastAPI(title="Token Atlas", version="1.0.0")
app.mount("/assets", StaticFiles(directory=STATIC_DIR), name="assets")


@app.exception_handler(PermissionError)
def permission_error_handler(_: Any, __: PermissionError) -> Response:
    return Response(status_code=status.HTTP_401_UNAUTHORIZED)


@app.on_event("startup")
def on_startup() -> None:
    global observer

    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    store.refresh_all()

    event_handler = TokenDirectoryEventHandler(store)
    observer = Observer()
    observer.schedule(event_handler, str(TOKEN_DIR), recursive=False)
    observer.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    global observer

    if observer is None:
        return

    observer.stop()
    observer.join(timeout=5)
    observer = None


@app.get("/", response_class=FileResponse)
def get_home() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.post("/auth/login")
def login(payload: LoginPayload = Body(...)) -> Response:
    if payload.password != get_expected_access_key():
        raise PermissionError

    return Response(status_code=status.HTTP_204_NO_CONTENT)


@app.get("/json", dependencies=[Depends(verify_access_key)])
def get_index() -> dict[str, Any]:
    return store.list_index()


@app.get("/json/item", dependencies=[Depends(verify_access_key)])
def get_item(
    name: str | None = Query(default=None),
    id: str | None = Query(default=None),
    index: int | None = Query(default=None, ge=0),
) -> dict[str, Any]:
    if name is None and id is None and index is None:
        return Response(
            status_code=status.HTTP_400_BAD_REQUEST,
            content=b'{"detail":"Provide one of: name, id, index."}',
            media_type="application/json",
        )

    item = store.resolve(name=name, item_id=id, index=index)
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


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", "8000")),
        reload=False,
    )