from __future__ import annotations

import json
import queue
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any

API_BASE_URL = "https://chatgpt.com/backend-api/codex"
CODEX_CLIENT_VERSION = "0.101.0"
CODEX_USER_AGENT = "codex_cli_rs/0.101.0 (Mac OS 26.0.1; arm64) Apple_Terminal/464"


def _probe_log(message: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] [probe] {message}", flush=True)


@dataclass(frozen=True)
class ProbeResult:
    status: str
    http_status: int | None = None
    detail: str = ""

    @property
    def is_banned(self) -> bool:
        return self.status == "banned_401"


@dataclass
class _ProbeTask:
    token_content: dict[str, Any]
    response_queue: queue.Queue[ProbeResult]


def _extract_storage(token_content: dict[str, Any]) -> dict[str, Any]:
    storage = token_content.get("storage")
    if isinstance(storage, dict):
        return storage
    return token_content


def _extract_credentials(token_content: dict[str, Any]) -> tuple[str, str]:
    storage = _extract_storage(token_content)
    access_token = str(storage.get("access_token", "")).strip()
    account_id = str(storage.get("account_id", "")).strip()
    return access_token, account_id


def _apply_codex_headers(
    request: urllib.request.Request,
    access_token: str,
    account_id: str,
    *,
    stream: bool,
) -> None:
    request.add_header("Content-Type", "application/json")
    request.add_header("Authorization", f"Bearer {access_token}")
    request.add_header("Version", CODEX_CLIENT_VERSION)
    request.add_header("Openai-Beta", "responses=experimental")
    request.add_header("Session_id", str(uuid.uuid4()))
    request.add_header("User-Agent", CODEX_USER_AGENT)
    if stream:
        request.add_header("Accept", "text/event-stream")
    else:
        request.add_header("Accept", "application/json")
    request.add_header("Connection", "Keep-Alive")
    request.add_header("Originator", "codex_cli_rs")
    if account_id:
        request.add_header("Chatgpt-Account-Id", account_id)


def probe_token(token_content: dict[str, Any], *, timeout_sec: float = 20.0) -> ProbeResult:
    access_token, account_id = _extract_credentials(token_content)
    if not access_token:
        _probe_log(f"[probe-request] missing_access_token account_id={account_id or '-'}")
        return ProbeResult(status="non_401_error", detail="missing_access_token")

    _probe_log(
        f"[probe-request] start account_id={account_id or '-'} timeout_sec={float(timeout_sec):.1f}"
    )
    stream = True
    body = {
        "model": "gpt-5",
        "stream": stream,
        "store": False,
        "instructions": "",
        "input": [
            {
                "type": "message",
                "role": "user",
                "content": [{"type": "input_text", "text": "ping"}],
            }
        ],
        "parallel_tool_calls": True,
        "include": ["reasoning.encrypted_content"],
    }
    req = urllib.request.Request(
        f"{API_BASE_URL}/responses",
        data=json.dumps(body).encode("utf-8"),
        method="POST",
    )
    _apply_codex_headers(req, access_token, account_id, stream=stream)
    _probe_log(
        f"[probe-request] headers account_id_header={int(bool(account_id))} "
        f"accept={req.headers.get('Accept', '-')}"
    )
    try:
        with urllib.request.urlopen(req, timeout=max(1.0, float(timeout_sec))) as resp:
            resp.read()
        result = ProbeResult(status="ok", http_status=200)
        _probe_log(f"[probe-request] success account_id={account_id or '-'} http_status=200")
        return result
    except urllib.error.HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except Exception:
            detail = ""
        if exc.code == 401:
            result = ProbeResult(status="banned_401", http_status=401, detail=detail)
            _probe_log(
                f"[probe-request] http_error account_id={account_id or '-'} "
                f"status=banned_401 http_status=401 detail={detail or '-'}"
            )
            return result
        result = ProbeResult(status="non_401_error", http_status=exc.code, detail=detail)
        _probe_log(
            f"[probe-request] http_error account_id={account_id or '-'} "
            f"status=non_401_error http_status={exc.code} detail={detail or '-'}"
        )
        return result
    except Exception as exc:
        result = ProbeResult(status="non_401_error", detail=str(exc))
        _probe_log(
            f"[probe-request] exception account_id={account_id or '-'} "
            f"status=non_401_error detail={exc}"
        )
        return result


class CodexProbeQueue:
    def __init__(self, *, delay_sec: float = 1.5, timeout_sec: float = 20.0) -> None:
        self._delay_sec = max(0.0, float(delay_sec))
        self._timeout_sec = max(1.0, float(timeout_sec))
        self._tasks: queue.Queue[_ProbeTask | None] = queue.Queue()
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._last_probe_monotonic = 0.0

    def start(self) -> None:
        with self._lock:
            if self._thread is not None and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._worker_loop, name="codex-probe-worker", daemon=True)
            self._thread.start()
            _probe_log("[probe-worker] started")

    def stop(self) -> None:
        self._stop.set()
        self._tasks.put(None)
        with self._lock:
            thread = self._thread
            self._thread = None
        if thread is not None:
            thread.join(timeout=5)
            _probe_log("[probe-worker] stopped")

    def submit(self, token_content: dict[str, Any], *, wait_timeout_sec: float | None = None) -> ProbeResult:
        self.start()
        response_queue: queue.Queue[ProbeResult] = queue.Queue(maxsize=1)
        access_token, account_id = _extract_credentials(token_content)
        self._tasks.put(_ProbeTask(token_content=token_content, response_queue=response_queue))
        timeout = self._timeout_sec + self._delay_sec + 5.0
        if wait_timeout_sec is not None:
            timeout = max(timeout, float(wait_timeout_sec))
        _probe_log(
            f"[probe-submit] queued account_id={account_id or '-'} "
            f"queue_size={self._tasks.qsize()} wait_timeout_sec={timeout:.1f}"
        )
        try:
            result = response_queue.get(timeout=timeout)
            _probe_log(
                f"[probe-submit] completed account_id={account_id or '-'} "
                f"status={result.status} http_status={result.http_status} detail={result.detail or '-'}"
            )
            return result
        except queue.Empty:
            _probe_log(
                f"[probe-submit] timeout account_id={account_id or '-'} "
                f"queue_size={self._tasks.qsize()} wait_timeout_sec={timeout:.1f}"
            )
            return ProbeResult(status="non_401_error", detail="probe_queue_timeout")

    def _worker_loop(self) -> None:
        while not self._stop.is_set():
            task = self._tasks.get()
            if task is None:
                continue
            _, account_id = _extract_credentials(task.token_content)
            _probe_log(
                f"[probe-worker] picked account_id={account_id or '-'} queue_size={self._tasks.qsize()}"
            )
            wait_for = self._delay_sec - (time.monotonic() - self._last_probe_monotonic)
            if wait_for > 0:
                _probe_log(
                    f"[probe-worker] throttling account_id={account_id or '-'} wait_for={wait_for:.2f}s"
                )
                time.sleep(wait_for)
            result = probe_token(task.token_content, timeout_sec=self._timeout_sec)
            self._last_probe_monotonic = time.monotonic()
            try:
                task.response_queue.put_nowait(result)
                _probe_log(
                    f"[probe-worker] delivered account_id={account_id or '-'} "
                    f"status={result.status} http_status={result.http_status}"
                )
            except queue.Full:
                _probe_log(f"[probe-worker] response_queue_full account_id={account_id or '-'}")
