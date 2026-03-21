import asyncio
import io
import json
import unittest
import zipfile
from unittest.mock import patch

from fastapi.responses import StreamingResponse
from starlette.requests import Request

import app as app_module


def make_request(path: str) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": path,
            "headers": [],
            "query_string": b"",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 12345),
        }
    )


async def collect_streaming_body(response: StreamingResponse) -> bytes:
    chunks: list[bytes] = []
    async for chunk in response.body_iterator:
        if isinstance(chunk, str):
            chunks.append(chunk.encode("utf-8"))
        else:
            chunks.append(chunk)
    return b"".join(chunks)


def render_exception(exc: Exception):
    for exc_type, handler in app_module.app.exception_handlers.items():
        if isinstance(exc, exc_type):
            return handler(None, exc)
    raise exc


def call_with_handlers(callback):
    try:
        response = callback()
    except Exception as exc:  # noqa: BLE001
        response = render_exception(exc)
    if isinstance(response, StreamingResponse):
        body = asyncio.run(collect_streaming_body(response))
    else:
        body = bytes(response.body or b"")
    return response, body


class DownloadEndpointTests(unittest.TestCase):
    def test_claims_archive_returns_zip_and_logs_success(self):
        items = [
            {
                "claim_id": 1,
                "file_name": "account-1.json",
                "content": {"account": "demo"},
            }
        ]
        with (
            patch.object(app_module, "require_session_user", return_value={"user_id": 42}),
            patch.object(app_module, "cache_json", side_effect=lambda _k, _t, loader: loader()),
            patch.object(app_module.db, "list_claim_files", return_value=items),
            patch.object(app_module, "download_log") as download_log,
        ):
            response, body = call_with_handlers(
                lambda: app_module.download_claims_archive(make_request("/me/claims/archive"))
            )

        self.assertEqual(response.status_code, 200)
        self.assertIn("application/zip", response.headers["content-type"])
        with zipfile.ZipFile(io.BytesIO(body), "r") as archive:
            self.assertEqual(archive.namelist(), ["account-1.json"])
            self.assertEqual(json.loads(archive.read("account-1.json").decode("utf-8")), {"account": "demo"})
        self.assertTrue(download_log.called)
        self.assertIn("status=success", download_log.call_args.args[0])
        self.assertIn("claims_count=1", download_log.call_args.args[0])

    def test_claims_archive_returns_404_when_no_claims_exist(self):
        with (
            patch.object(app_module, "require_session_user", return_value={"user_id": 42}),
            patch.object(app_module, "cache_json", side_effect=lambda _k, _t, loader: loader()),
            patch.object(app_module.db, "list_claim_files", return_value=[]),
        ):
            response, body = call_with_handlers(
                lambda: app_module.download_claims_archive(make_request("/me/claims/archive"))
            )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(json.loads(body.decode("utf-8")), {"detail": "No claimed token files available for archive."})

    def test_claims_archive_returns_401_when_session_is_expired(self):
        with patch.object(
            app_module,
            "require_session_user",
            side_effect=app_module.AuthenticationRequiredError("Authentication required."),
        ):
            response, body = call_with_handlers(
                lambda: app_module.download_claims_archive(make_request("/me/claims/archive"))
            )

        self.assertEqual(response.status_code, 401)
        self.assertEqual(json.loads(body.decode("utf-8")), {"detail": "Authentication required."})

    def test_claims_archive_returns_500_when_claim_content_is_corrupted(self):
        with (
            patch.object(app_module, "require_session_user", return_value={"user_id": 42}),
            patch.object(app_module, "cache_json", side_effect=lambda _k, _t, loader: loader()),
            patch.object(
                app_module.db,
                "list_claim_files",
                side_effect=json.JSONDecodeError("Expecting value", "broken", 0),
            ),
        ):
            response, body = call_with_handlers(
                lambda: app_module.download_claims_archive(make_request("/me/claims/archive"))
            )

        self.assertEqual(response.status_code, 500)
        self.assertEqual(json.loads(body.decode("utf-8")), {"detail": "Stored claim content is corrupted."})

    def test_single_claim_download_returns_403_for_other_users_claim(self):
        with (
            patch.object(app_module, "cache_json", side_effect=lambda _k, _t, loader: loader()),
            patch.object(app_module.db, "resolve_api_key", return_value={"user_id": 7}),
            patch.object(app_module.db, "get_claimed_token", return_value=None),
            patch.object(
                app_module.db,
                "get_claim_download_access_summary",
                return_value={"user_hidden_claim": False, "other_visible_claim": True},
            ),
        ):
            response, body = call_with_handlers(
                lambda: app_module.download_claimed_token(
                    make_request("/api/download/99"),
                    99,
                    api_key="demo-key",
                )
            )

        self.assertEqual(response.status_code, 403)
        self.assertEqual(json.loads(body.decode("utf-8")), {"detail": "You do not have access to this claimed token."})

    def test_single_claim_download_returns_404_when_claim_does_not_exist(self):
        with (
            patch.object(app_module, "cache_json", side_effect=lambda _k, _t, loader: loader()),
            patch.object(app_module.db, "resolve_api_key", return_value={"user_id": 7}),
            patch.object(app_module.db, "get_claimed_token", return_value=None),
            patch.object(
                app_module.db,
                "get_claim_download_access_summary",
                return_value={"user_hidden_claim": False, "other_visible_claim": False},
            ),
        ):
            response, body = call_with_handlers(
                lambda: app_module.download_claimed_token(
                    make_request("/api/download/100"),
                    100,
                    api_key="demo-key",
                )
            )

        self.assertEqual(response.status_code, 404)
        self.assertEqual(json.loads(body.decode("utf-8")), {"detail": "Claimed token download not found."})
