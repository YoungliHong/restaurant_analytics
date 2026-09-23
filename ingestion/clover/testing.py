"""Fake HTTP transport for exercising the Clover client without network
or credentials.

Shared by tests/ingestion/ (pytest) and scripts/spike/pull.py --fixture,
so "does the scaffold wire together end to end" has one implementation
instead of two that can drift apart.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent / "tests" / "fixtures" / "clover"


class FakeResponse:
    """Just enough of requests.Response for CloverClient.get() to work with."""

    def __init__(self, status_code: int, json_body: Any, headers: dict[str, str] | None = None):
        self.status_code = status_code
        self._json = json_body
        self.headers = headers or {}
        self.ok = 200 <= status_code < 300
        self.reason = json_body.get("message", "error") if isinstance(json_body, dict) else "error"
        self.text = json.dumps(json_body)

    def json(self) -> Any:
        return self._json


class FakeSession:
    """Drop-in for requests.Session that always returns one canned response.

    Records every call so a test can assert on the URL/headers/params the
    client actually sent (e.g. that the Authorization header carries the
    stored access token).
    """

    def __init__(self, response: FakeResponse):
        self.response = response
        self.calls: list[tuple[str, dict, dict]] = []

    def get(self, url: str, headers: dict | None = None, params: dict | None = None) -> FakeResponse:
        self.calls.append((url, headers or {}, params or {}))
        return self.response


def load_fixture(name: str) -> Any:
    """Read one JSON fixture from tests/fixtures/clover/ by filename."""
    return json.loads((FIXTURES_DIR / name).read_text())
