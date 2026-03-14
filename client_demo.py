from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, parse, request

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"

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


def get_base_url() -> str:
    return os.getenv(BASE_URL_ENV, DEFAULT_BASE_URL).rstrip("/")


def get_api_key() -> str:
    return os.getenv(API_KEY_ENV, "")


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
    final_headers: dict[str, str] = {"User-Agent": "Token-Atlas-Client"}
    if headers:
        final_headers.update(headers)

    if body is not None:
        final_headers["Content-Type"] = "application/json"
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = request.Request(url, data=payload, headers=final_headers, method=method)

    try:
        with request.urlopen(req, timeout=20) as resp:
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
    final_headers: dict[str, str] = {"User-Agent": "Token-Atlas-Client"}
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


def print_result(title: str, status: int, payload: Any) -> None:
    print(f"\n[{title}] status={status}")
    if payload is None:
        print("<empty response>")
        return

    if isinstance(payload, (dict, list)):
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return

    print(payload)


def parse_actions(raw: str) -> list[int]:
    normalized = raw.replace(",", " ").strip()
    actions: list[int] = []

    for token in normalized.split():
        if token.isdigit():
            actions.append(int(token))

    return actions


def action_index(base_url: str, api_key: str, session_cookie: str) -> None:
    headers = build_json_headers(api_key, session_cookie)
    params = build_json_params()
    status, payload = request_json("GET", "/json", base_url=base_url, headers=headers, params=params)
    print_result("Index", status, payload)


def action_index_with_content(base_url: str, api_key: str, session_cookie: str) -> None:
    headers = build_json_headers(api_key, session_cookie)
    params = build_json_params()
    status, payload = request_json("GET", "/json", base_url=base_url, headers=headers, params=params)
    if status != 200 or not isinstance(payload, dict):
        print_result("Index + Content", status, payload)
        return

    items = payload.get("items", [])
    merged: list[dict[str, Any]] = []
    for item in items:
        item_status, item_payload = request_json(
            "GET",
            "/json/item",
            base_url=base_url,
            headers=headers,
            params={"name": item.get("name"), **params},
        )
        merged.append(
            {
                "status": item_status,
                "item": item,
                "detail": item_payload,
            }
        )

    print_result("Index + Content", 200, merged)


def action_by_id(base_url: str, api_key: str, session_cookie: str) -> None:
    item_id = input("Enter id: ").strip()
    headers = build_json_headers(api_key, session_cookie)
    params = build_json_params()
    status, payload = request_json(
        "GET",
        "/json/item",
        base_url=base_url,
        headers=headers,
        params={"id": item_id, **params},
    )
    print_result("Fetch by id", status, payload)


def action_by_index(base_url: str, api_key: str, session_cookie: str) -> None:
    index = input("Enter index: ").strip()
    headers = build_json_headers(api_key, session_cookie)
    params = build_json_params()
    status, payload = request_json(
        "GET",
        "/json/item",
        base_url=base_url,
        headers=headers,
        params={"index": index, **params},
    )
    print_result("Fetch by index", status, payload)


def action_by_name(base_url: str, api_key: str, session_cookie: str) -> None:
    name = input("Enter filename: ").strip()
    headers = build_json_headers(api_key, session_cookie)
    params = build_json_params()
    status, payload = request_json(
        "GET",
        "/json/item",
        base_url=base_url,
        headers=headers,
        params={"name": name, **params},
    )
    print_result("Fetch by name", status, payload)


def action_claim(base_url: str, api_key: str) -> None:
    count = input("How many to claim: ").strip()
    count_value = int(count) if count.isdigit() else 1
    headers = {"X-API-Key": api_key} if api_key else {}
    status, payload = request_json(
        "POST",
        "/api/claim",
        base_url=base_url,
        headers=headers,
        body={"count": count_value},
    )
    print_result("API Claim", status, payload)


def action_download(base_url: str, api_key: str) -> None:
    token_id = input("Enter token_id to download: ").strip()
    headers = {"X-API-Key": api_key} if api_key else {}
    status, payload = request_json(
        "GET",
        f"/api/download/{token_id}",
        base_url=base_url,
        headers=headers,
    )
    print_result("Download JSON", status, payload)


def action_me(base_url: str, session_cookie: str) -> None:
    headers = build_session_headers(session_cookie)
    status, payload = request_json("GET", "/me", base_url=base_url, headers=headers)
    print_result("Me", status, payload)


def action_dashboard_stats(base_url: str, session_cookie: str) -> None:
    headers = build_session_headers(session_cookie)
    status, payload = request_json("GET", "/dashboard/stats", base_url=base_url, headers=headers)
    print_result("Dashboard Stats", status, payload)


def action_claim_session(base_url: str, session_cookie: str) -> None:
    count = input("How many to claim (session): ").strip()
    count_value = int(count) if count.isdigit() else 1
    headers = build_session_headers(session_cookie)
    status, payload = request_json(
        "POST",
        "/me/claim",
        base_url=base_url,
        headers=headers,
        body={"count": count_value},
    )
    print_result("Session Claim", status, payload)


def action_list_claims(base_url: str, session_cookie: str) -> None:
    headers = build_session_headers(session_cookie)
    status, payload = request_json("GET", "/me/claims", base_url=base_url, headers=headers)
    print_result("Session Claims", status, payload)


def action_claims_archive(base_url: str, session_cookie: str) -> None:
    headers = build_session_headers(session_cookie)
    status, raw, _ = request_bytes("GET", "/me/claims/archive", base_url=base_url, headers=headers)
    if status != 200:
        print_result("Claims Archive", status, raw.decode("utf-8", errors="replace"))
        return
    filename = f"claimed-{datetime.now().strftime('%Y%m%d-%H%M%S')}.zip"
    file_path = BASE_DIR / filename
    file_path.write_bytes(raw)
    print(f"\n[Claims Archive] status={status}")
    print(f"Saved to {file_path}")


def action_set_base_url() -> str:
    value = input(f"Base URL [{get_base_url()}]: ").strip()
    return value.rstrip("/") if value else get_base_url()


def action_set_api_key() -> str:
    value = input("API key (X-API-Key): ").strip()
    return value if value else get_api_key()


def action_set_session_cookie() -> str:
    value = input("Session cookie (token_atlas_session): ").strip()
    return value if value else get_session_cookie()


def print_menu(base_url: str, api_key: str, session_cookie: str) -> None:
    print("\n================ Token Atlas Client Demo ================")
    print(f"Base URL: {base_url}")
    print(f"API Key: {'set' if api_key else 'empty'}")
    print(f"Session Cookie: {'set' if session_cookie else 'empty'}")
    print("1. List index (/json)")
    print("2. List index + content")
    print("3. Fetch by id")
    print("4. Fetch by index")
    print("5. Fetch by filename")
    print("6. Claim accounts (/api/claim)")
    print("7. Download claimed JSON (/api/download/{token_id})")
    print("8. Set base URL")
    print("9. Set API key")
    print("10. Get /me (session)")
    print("11. Get /dashboard/stats (session)")
    print("12. Claim accounts (/me/claim)")
    print("13. List /me/claims")
    print("14. Download /me/claims/archive")
    print("15. Set session cookie")
    print("0. Exit")


def main() -> None:
    base_url = get_base_url()
    api_key = get_api_key()
    session_cookie = get_session_cookie()

    while True:
        print_menu(base_url, api_key, session_cookie)
        actions = parse_actions(input("Select actions: "))
        if not actions:
            print("No valid actions.")
            continue

        should_exit = False
        for action in actions:
            if action == 0:
                should_exit = True
                break
            if action == 1:
                action_index(base_url, api_key, session_cookie)
            elif action == 2:
                action_index_with_content(base_url, api_key, session_cookie)
            elif action == 3:
                action_by_id(base_url, api_key, session_cookie)
            elif action == 4:
                action_by_index(base_url, api_key, session_cookie)
            elif action == 5:
                action_by_name(base_url, api_key, session_cookie)
            elif action == 6:
                action_claim(base_url, api_key)
            elif action == 7:
                action_download(base_url, api_key)
            elif action == 8:
                base_url = action_set_base_url()
            elif action == 9:
                api_key = action_set_api_key()
            elif action == 10:
                action_me(base_url, session_cookie)
            elif action == 11:
                action_dashboard_stats(base_url, session_cookie)
            elif action == 12:
                action_claim_session(base_url, session_cookie)
            elif action == 13:
                action_list_claims(base_url, session_cookie)
            elif action == 14:
                action_claims_archive(base_url, session_cookie)
            elif action == 15:
                session_cookie = action_set_session_cookie()
            else:
                print(f"Unsupported action: {action}")

        if should_exit:
            print("Bye.")
            return


if __name__ == "__main__":
    main()
