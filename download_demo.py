from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, parse, request

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
TOKEN_DIR = BASE_DIR / "token"

API_KEY_ENV = "TOKEN_PROVIDER_API_KEY"
BASE_URL_ENV = "TOKEN_PROVIDER_BASE_URL"
SESSION_COOKIE_ENV = "TOKEN_ATLAS_SESSION"
SESSION_COOKIE_NAME = "token_atlas_session"
DEFAULT_BASE_URL = "http://127.0.0.1:8000"


def detect_text_encoding(raw: bytes) -> str:
    if raw.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    if raw.startswith(b"\xff\xfe"):
        return "utf-16-le"
    if raw.startswith(b"\xfe\xff"):
        return "utf-16-be"

    for encoding in ("utf-8", "utf-16", "utf-16-le", "utf-16-be", "gbk", "big5", "latin-1"):
        try:
            raw.decode(encoding)
            return encoding
        except UnicodeDecodeError:
            continue

    raise UnicodeDecodeError("unknown", raw, 0, len(raw), "Unable to determine encoding")


def load_dotenv_file(path: Path) -> None:
    if not path.exists():
        return

    raw = path.read_bytes()
    encoding = detect_text_encoding(raw)
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


def get_session_cookie() -> str:
    return os.getenv(SESSION_COOKIE_ENV, "")


def build_session_headers(session_cookie: str) -> dict[str, str]:
    if not session_cookie:
        return {}
    return {"Cookie": f"{SESSION_COOKIE_NAME}={session_cookie}"}

def build_json_headers(api_key: str, session_cookie: str) -> dict[str, str]:
    headers: dict[str, str] = {}
    if api_key:
        headers["X-API-Key"] = api_key
    if session_cookie:
        headers.update(build_session_headers(session_cookie))
    return headers

def build_json_params() -> dict[str, Any]:
    return {}


def request_json(
    method: str,
    path: str,
    *,
    base_url: str,
    headers: dict[str, str] | None = None,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    url = f"{base_url}{path}"
    if params:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{url}?{query}"

    payload = None
    final_headers: dict[str, str] = {"User-Agent": "Token-Atlas-Downloader"}
    if headers:
        final_headers.update(headers)

    if body is not None:
        final_headers["Content-Type"] = "application/json"
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = request.Request(url, data=payload, headers=final_headers, method=method)

    try:
        with request.urlopen(req, timeout=30) as resp:
            status = resp.status
            raw = resp.read()
    except error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
    except error.URLError as exc:
        return 0, {"detail": str(exc.reason)}

    if not raw:
        return status, None

    text = raw.decode("utf-8", errors="replace")
    try:
        return status, json.loads(text)
    except json.JSONDecodeError:
        return status, text


def request_bytes(
    method: str,
    path: str,
    *,
    base_url: str,
    headers: dict[str, str] | None = None,
) -> tuple[int, bytes, dict[str, str]]:
    url = f"{base_url}{path}"
    final_headers: dict[str, str] = {"User-Agent": "Token-Atlas-Downloader"}
    if headers:
        final_headers.update(headers)

    req = request.Request(url, headers=final_headers, method=method)

    try:
        with request.urlopen(req, timeout=30) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)
    except error.URLError as exc:
        return 0, str(exc.reason).encode("utf-8"), {}


def download_index(base_url: str, api_key: str, session_cookie: str) -> None:
    print(f"Base URL: {base_url}")
    print(f"API Key: {'set' if api_key else 'empty'}")
    print(f"Session Cookie: {'set' if session_cookie else 'empty'}")
    print()

    print("Fetching index...")
    headers = build_json_headers(api_key, session_cookie)
    params = build_json_params()
    status, payload = request_json("GET", "/json", base_url=base_url, headers=headers, params=params)

    if status != 200 or not isinstance(payload, dict):
        print(f"Index fetch failed (status={status})")
        if payload:
            print(json.dumps(payload, ensure_ascii=False, indent=2) if isinstance(payload, (dict, list)) else payload)
        return

    items = payload.get("items", [])
    if not items:
        print("Index empty. Nothing to download.")
        return

    print(f"Found {len(items)} files")

    TOKEN_DIR.mkdir(parents=True, exist_ok=True)

    success = 0
    failed = 0

    for i, item in enumerate(items, 1):
        name = item.get("name", "")
        print(f"  [{i}/{len(items)}] Download {name} ...", end=" ")

        item_status, item_payload = request_json(
            "GET",
            "/json/item",
            base_url=base_url,
            headers=headers,
            params={"name": name, **params},
        )

        if item_status != 200 or not isinstance(item_payload, dict):
            print(f"Failed (status={item_status})")
            failed += 1
            continue

        content = item_payload.get("content")
        if content is None:
            print("Failed (content empty)")
            failed += 1
            continue

        file_path = TOKEN_DIR / name
        if isinstance(content, (dict, list)):
            file_path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            file_path.write_text(str(content), encoding="utf-8")

        print("Done")
        success += 1

    print()
    print(f"Download complete: success {success}, failed {failed}, total {len(items)}")
    print(f"Saved to: {TOKEN_DIR}")


def download_claimed_api(base_url: str, api_key: str, count: int) -> None:
    print(f"Base URL: {base_url}")
    print(f"API Key: {'set' if api_key else 'empty'}")
    print()

    if not api_key:
        print("API Key is required for /api/claim.")
        return

    print("Claiming accounts...")
    headers = {"X-API-Key": api_key}
    claim_status, claim_payload = request_json(
        "POST",
        "/api/claim",
        base_url=base_url,
        headers=headers,
        body={"count": count},
    )

    if claim_status != 200 or not isinstance(claim_payload, dict):
        print(f"Claim failed (status={claim_status})")
        print(claim_payload)
        return

    items = claim_payload.get("items", [])
    if not items:
        print("No claimed items returned.")
        return

    TOKEN_DIR.mkdir(parents=True, exist_ok=True)

    for item in items:
        token_id = item.get("token_id")
        if token_id is None:
            continue
        print(f"Downloading token_id={token_id} ...", end=" ")
        download_status, download_payload = request_json(
            "GET",
            f"/api/download/{token_id}",
            base_url=base_url,
            headers={"X-API-Key": api_key},
        )
        if download_status != 200 or not isinstance(download_payload, dict):
            print("Failed")
            continue
        file_name = item.get("file_name") or f"token-{token_id}.json"
        file_path = TOKEN_DIR / file_name
        file_path.write_text(json.dumps(download_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Done")


def download_claimed_session(base_url: str, session_cookie: str, count: int) -> None:
    print(f"Base URL: {base_url}")
    print(f"Session Cookie: {'set' if session_cookie else 'empty'}")
    print()

    if not session_cookie:
        print("Session cookie is required for /me/claim.")
        return

    print("Claiming accounts (session)...")
    headers = build_session_headers(session_cookie)
    claim_status, claim_payload = request_json(
        "POST",
        "/me/claim",
        base_url=base_url,
        headers=headers,
        body={"count": count},
    )

    if claim_status != 200 or not isinstance(claim_payload, dict):
        print(f"Claim failed (status={claim_status})")
        print(claim_payload)
        return

    items = claim_payload.get("items", [])
    if not items:
        print("No claimed items returned.")
        return

    TOKEN_DIR.mkdir(parents=True, exist_ok=True)

    for item in items:
        token_id = item.get("token_id")
        if token_id is None:
            continue
        print(f"Downloading token_id={token_id} ...", end=" ")
        download_status, download_payload = request_json(
            "GET",
            f"/api/download/{token_id}",
            base_url=base_url,
            headers=build_session_headers(session_cookie),
        )
        if download_status != 200 or not isinstance(download_payload, dict):
            print("Failed")
            continue
        file_name = item.get("file_name") or f"token-{token_id}.json"
        file_path = TOKEN_DIR / file_name
        file_path.write_text(json.dumps(download_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        print("Done")


def download_claims_archive(base_url: str, session_cookie: str) -> None:
    print(f"Base URL: {base_url}")
    print(f"Session Cookie: {'set' if session_cookie else 'empty'}")
    print()

    if not session_cookie:
        print("Session cookie is required for /me/claims/archive.")
        return

    status, raw, _ = request_bytes(
        "GET",
        "/me/claims/archive",
        base_url=base_url,
        headers=build_session_headers(session_cookie),
    )

    if status != 200:
        print(f"Archive download failed (status={status})")
        print(raw.decode("utf-8", errors="replace"))
        return

    TOKEN_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"claimed-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
    file_path = TOKEN_DIR / filename
    file_path.write_bytes(raw)
    print(f"Saved to: {file_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Download account JSON data")
    parser.add_argument("--url", default=None, help="Base URL")
    parser.add_argument("--api-key", default=None, help="API key for /api and /json endpoints")
    parser.add_argument("--session-cookie", default=None, help="token_atlas_session value")
    parser.add_argument("--mode", choices=["index", "api", "session", "archive"], default="index")
    parser.add_argument("--count", type=int, default=1, help="Claim count when mode=api or session")
    args = parser.parse_args()

    base_url = (args.url or os.getenv(BASE_URL_ENV, DEFAULT_BASE_URL)).rstrip("/")
    api_key = args.api_key or os.getenv(API_KEY_ENV, "")
    session_cookie = args.session_cookie or get_session_cookie()

    if args.mode == "api":
        download_claimed_api(base_url, api_key, args.count)
    elif args.mode == "session":
        download_claimed_session(base_url, session_cookie, args.count)
    elif args.mode == "archive":
        download_claims_archive(base_url, session_cookie)
    else:
        download_index(base_url, api_key, session_cookie)


if __name__ == "__main__":
    main()
