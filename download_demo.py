from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any
from urllib import error, parse, request

BASE_DIR = Path(__file__).resolve().parent
ENV_FILE = BASE_DIR / ".env"
TOKEN_DIR = BASE_DIR / "token"

ACCESS_KEY_ENV = "TOKEN_INDEX_ACCESS_KEY"
BASE_URL_ENV = "TOKEN_PROVIDER_BASE_URL"
DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_ACCESS_KEY = "your-access-key"


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


def request_json(
    method: str,
    path: str,
    *,
    base_url: str,
    access_key: str,
    params: dict[str, Any] | None = None,
) -> tuple[int, Any]:
    url = f"{base_url}{path}"
    if params:
        query = parse.urlencode({k: v for k, v in params.items() if v is not None})
        url = f"{url}?{query}"

    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
    if access_key:
        headers["X-Access-Key"] = access_key

    req = request.Request(url, headers=headers, method=method)

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


def download_all(base_url: str, access_key: str) -> None:
    print(f"服务地址: {base_url}")
    print(f"访问密码: {'已设置' if access_key else '未设置'}")
    print()

    # 获取文件索引
    print("正在获取文件索引...")
    status, payload = request_json("GET", "/json", base_url=base_url, access_key=access_key)

    if status != 200 or not isinstance(payload, dict):
        print(f"获取索引失败 (status={status})")
        if payload:
            print(json.dumps(payload, ensure_ascii=False, indent=2) if isinstance(payload, (dict, list)) else payload)
        return

    items = payload.get("items", [])
    if not items:
        print("索引为空，没有可下载的文件。")
        return

    print(f"发现 {len(items)} 个文件")

    # 创建输出目录
    TOKEN_DIR.mkdir(parents=True, exist_ok=True)

    # 逐个下载
    success = 0
    failed = 0

    for i, item in enumerate(items, 1):
        name = item.get("name", "")
        print(f"  [{i}/{len(items)}] 下载 {name} ...", end=" ")

        item_status, item_payload = request_json(
            "GET",
            "/json/item",
            base_url=base_url,
            access_key=access_key,
            params={"name": name},
        )

        if item_status != 200 or not isinstance(item_payload, dict):
            print(f"失败 (status={item_status})")
            failed += 1
            continue

        content = item_payload.get("content")
        if content is None:
            print("失败 (content 为空)")
            failed += 1
            continue

        # 写入文件
        file_path = TOKEN_DIR / name
        if isinstance(content, (dict, list)):
            file_path.write_text(json.dumps(content, ensure_ascii=False, indent=2), encoding="utf-8")
        else:
            file_path.write_text(str(content), encoding="utf-8")

        print("完成")
        success += 1

    # 结果摘要
    print()
    print(f"下载完成: 成功 {success}, 失败 {failed}, 共 {len(items)} 个文件")
    print(f"保存目录: {TOKEN_DIR}")


def main() -> None:
    parser = argparse.ArgumentParser(description="下载全部账号 JSON 数据")
    parser.add_argument("--url", default=None, help="服务地址")
    parser.add_argument("--key", default=None, help="访问密码")
    args = parser.parse_args()

    base_url = (args.url or os.getenv(BASE_URL_ENV, DEFAULT_BASE_URL)).rstrip("/")
    access_key = args.key or os.getenv(ACCESS_KEY_ENV, DEFAULT_ACCESS_KEY)

    download_all(base_url, access_key)


if __name__ == "__main__":
    main()
