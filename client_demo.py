from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib import error, parse, request

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"

ACCESS_KEY_ENV = "TOKEN_INDEX_ACCESS_KEY"
BASE_URL_ENV = "TOKEN_PROVIDER_BASE_URL"
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


def get_access_key() -> str:
    return os.getenv(ACCESS_KEY_ENV, "")


def request_json(
    method: str,
    path: str,
    *,
    base_url: str,
    access_key: str,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    use_access_key: bool = True,
) -> tuple[int, Any]:
    url = f"{base_url}{path}"
    if params:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{url}?{query}"

    headers = {}
    payload = None

    if use_access_key and access_key:
        headers["X-Access-Key"] = access_key

    if body is not None:
        headers["Content-Type"] = "application/json"
        payload = json.dumps(body, ensure_ascii=False).encode("utf-8")

    req = request.Request(url, data=payload, headers=headers, method=method)

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
    normalized = raw.replace("，", " ").replace(",", " ").strip()
    actions: list[int] = []

    for token in normalized.split():
        if token.isdigit():
            actions.extend(int(char) for char in token)

    return actions


def action_login(base_url: str, access_key: str) -> None:
    status, payload = request_json(
        "POST",
        "/auth/login",
        base_url=base_url,
        access_key=access_key,
        body={"password": access_key},
        use_access_key=False,
    )
    print_result("登录校验", status, payload)


def action_index(base_url: str, access_key: str) -> None:
    status, payload = request_json("GET", "/json", base_url=base_url, access_key=access_key)
    print_result("全部索引", status, payload)


def action_index_with_content(base_url: str, access_key: str) -> None:
    status, payload = request_json("GET", "/json", base_url=base_url, access_key=access_key)
    if status != 200 or not isinstance(payload, dict):
        print_result("全部索引+内容", status, payload)
        return

    items = payload.get("items", [])
    merged: list[dict[str, Any]] = []
    for item in items:
        item_status, item_payload = request_json(
            "GET",
            "/json/item",
            base_url=base_url,
            access_key=access_key,
            params={"name": item.get("name")},
        )
        merged.append(
            {
                "status": item_status,
                "item": item,
                "detail": item_payload,
            }
        )

    print_result("全部索引+内容", 200, merged)


def action_by_id(base_url: str, access_key: str) -> None:
    item_id = input("请输入 id: ").strip()
    status, payload = request_json(
        "GET",
        "/json/item",
        base_url=base_url,
        access_key=access_key,
        params={"id": item_id},
    )
    print_result("按 id 获取内容", status, payload)


def action_by_index(base_url: str, access_key: str) -> None:
    index = input("请输入 index: ").strip()
    status, payload = request_json(
        "GET",
        "/json/item",
        base_url=base_url,
        access_key=access_key,
        params={"index": index},
    )
    print_result("按 index 获取内容", status, payload)


def action_health(base_url: str) -> None:
    status, payload = request_json(
        "GET",
        "/health",
        base_url=base_url,
        access_key="",
        use_access_key=False,
    )
    print_result("健康检测", status, payload)


def action_by_name(base_url: str, access_key: str) -> None:
    name = input("请输入文件名: ").strip()
    status, payload = request_json(
        "GET",
        "/json/item",
        base_url=base_url,
        access_key=access_key,
        params={"name": name},
    )
    print_result("按文件名获取内容", status, payload)


def action_set_base_url() -> str:
    value = input(f"请输入服务地址，回车保持当前值 [{get_base_url()}]: ").strip()
    return value.rstrip("/") if value else get_base_url()


def action_set_access_key() -> str:
    value = input("请输入访问密码，回车保持当前值: ").strip()
    return value if value else get_access_key()


def print_menu(base_url: str, access_key: str) -> None:
    print("\n================ Token Atlas Client Demo ================")
    print(f"服务地址: {base_url}")
    print(f"访问密码: {'已设置' if access_key else '未设置'}")
    print("1. 登录校验")
    print("2. 获取全部索引")
    print("3. 获取全部索引 + 内容")
    print("4. 获取指定 ID 内容")
    print("5. 获取指定 Index 内容")
    print("6. 健康检测")
    print("7. 按文件名获取内容")
    print("8. 修改服务地址")
    print("9. 修改访问密码")
    print("0. 退出")
    print("可一次输入多个编号，例如: 1 2 34")


def main() -> None:
    base_url = get_base_url()
    access_key = get_access_key()

    while True:
        print_menu(base_url, access_key)
        actions = parse_actions(input("请输入操作编号: "))
        if not actions:
            print("未识别到有效操作。")
            continue

        should_exit = False
        for action in actions:
            if action == 0:
                should_exit = True
                break
            if action == 1:
                action_login(base_url, access_key)
            elif action == 2:
                action_index(base_url, access_key)
            elif action == 3:
                action_index_with_content(base_url, access_key)
            elif action == 4:
                action_by_id(base_url, access_key)
            elif action == 5:
                action_by_index(base_url, access_key)
            elif action == 6:
                action_health(base_url)
            elif action == 7:
                action_by_name(base_url, access_key)
            elif action == 8:
                base_url = action_set_base_url()
                print(f"已切换服务地址: {base_url}")
            elif action == 9:
                access_key = action_set_access_key()
                print(f"访问密码状态: {'已设置' if access_key else '未设置'}")
            else:
                print(f"不支持的操作: {action}")

        if should_exit:
            print("已退出。")
            return


if __name__ == "__main__":
    main()