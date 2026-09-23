"""Typed errors for the Clover client.

Callers (the pagination loop, the extractor's retry logic) need to branch
on failure kind — "refresh and retry" vs "back off and retry" vs "give
up" are different responses — so status codes are surfaced as distinct
exception classes rather than left for every caller to re-parse.
"""

from __future__ import annotations


class CloverAPIError(Exception):
    """Base class for any non-2xx response from the Clover API."""

    def __init__(self, status_code: int, message: str, response_body: str | None = None):
        super().__init__(f"Clover API error {status_code}: {message}")
        self.status_code = status_code
        self.response_body = response_body


class CloverAuthError(CloverAPIError):
    """401 — the access token is missing, invalid, or expired.

    Per docs/clover/NOTES.md, production access tokens live 30 minutes,
    so this is an expected, routine failure mode, not an edge case —
    TODO(youngli): the pagination loop (client.py) should catch this,
    call get_valid_access_token() to refresh, and retry the same page
    once, not restart the whole pull.
    """


class CloverRateLimitError(CloverAPIError):
    """429 — per-token or per-app rate limit hit.

    Per docs/clover/NOTES.md: 16 req/s and 5 concurrent per token, 50
    req/s and 10 concurrent per app. retry_after is read from the
    Retry-After header (seconds) when the API supplies one.
    """

    def __init__(
        self,
        status_code: int,
        message: str,
        retry_after: float | None = None,
        response_body: str | None = None,
    ):
        super().__init__(status_code, message, response_body)
        self.retry_after = retry_after
