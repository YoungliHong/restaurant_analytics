"""Thin REST client over the Clover Orders/Inventory APIs.

Deliberately generic (get/paginate over any path) rather than one method
per endpoint: the sandbox spike (A1) is still discovering which
endpoints and `expand` params are actually needed. Endpoint-specific
convenience methods (e.g. get_orders(since=...)) can be layered on once
that's settled, without changing this module.
"""

from __future__ import annotations

from typing import Any, Iterator

import requests

from .auth import get_valid_access_token
from .config import CloverConfig
from .exceptions import CloverAPIError, CloverAuthError, CloverRateLimitError
from .token_store import TokenStore


class CloverClient:
    def __init__(
        self,
        config: CloverConfig,
        token_store: TokenStore,
        session: requests.Session | None = None,
    ):
        self.config = config
        self.token_store = token_store
        # Injectable transport so tests (and scripts/spike/pull.py
        # --fixture) can substitute canned responses without hitting the
        # network — see ingestion/clover/testing.py.
        self.session = session or requests.Session()

    def _headers(self) -> dict[str, str]:
        token = get_valid_access_token(self.config, self.token_store)
        return {"Authorization": f"Bearer {token}"}

    def get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """One GET request against the Clover API.

        Raises a typed CloverAPIError subclass on a non-2xx response, so
        callers can catch CloverAuthError / CloverRateLimitError
        specifically instead of re-checking status codes everywhere.
        """
        url = f"{self.config.api_base_url}{path}"
        response = self.session.get(url, headers=self._headers(), params=params or {})
        if response.status_code == 401:
            raise CloverAuthError(401, "unauthorized", response.text)
        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")
            raise CloverRateLimitError(
                429,
                "rate limited",
                float(retry_after) if retry_after else None,
                response.text,
            )
        if not response.ok:
            raise CloverAPIError(response.status_code, response.reason, response.text)
        return response.json()

    def paginate(
        self, path: str, params: dict[str, Any] | None = None, limit: int = 100
    ) -> Iterator[dict[str, Any]]:
        """Yield every element across all pages of a list endpoint.

        Stub: currently fetches ONE page (offset 0) and stops — enough to
        wire the rest of the pipeline against a fixture while A1 is still
        confirming offset/limit semantics (max page size? does offset
        shift if rows are written mid-pagination? see docs/clover/NOTES.md
        "Open" section).

        TODO(youngli) — the pagination loop reserved for you:
          - increment offset by the number of elements actually returned
            (not blindly by `limit`, in case the API returns fewer)
          - stop when a page returns zero elements
          - on CloverAuthError, call get_valid_access_token() to refresh
            and retry the SAME page once, rather than restarting the pull
          - on CloverRateLimitError, sleep `retry_after` (falling back to
            a fixed backoff if the header was absent) and retry the same
            page
        """
        page = self.get(path, {**(params or {}), "limit": limit, "offset": 0})
        yield from page.get("elements", [])
