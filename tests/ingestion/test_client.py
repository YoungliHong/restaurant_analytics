"""Tests for CloverClient against fixture data — no network, no credentials.

Confirms the scaffold's wiring (config -> token store -> client -> HTTP)
runs end to end, per docs/phase7-backlog.md A1's acceptance criteria.
Does NOT test the real pagination loop, 401-retry, or 429-backoff, since
those are TODO in ingestion/clover/client.py — this file should grow
alongside that TODO being filled in, not before.
"""

from __future__ import annotations

import pytest

from ingestion.clover.client import CloverClient
from ingestion.clover.config import CloverConfig
from ingestion.clover.exceptions import CloverAuthError, CloverRateLimitError
from ingestion.clover.testing import FakeResponse, FakeSession, load_fixture
from ingestion.clover.token_store import TokenSet, TokenStore


@pytest.fixture
def config() -> CloverConfig:
    return CloverConfig(
        environment="sandbox",
        client_id="test-client-id",
        client_secret="test-client-secret",
        redirect_uri="https://localhost/callback",
        merchant_id="TESTMERCHANT",
    )


@pytest.fixture
def token_store(tmp_path) -> TokenStore:
    store = TokenStore(tmp_path / "token.json")
    store.save(
        TokenSet(
            access_token="fake-access-token",
            refresh_token="fake-refresh-token",
            access_token_expiration=9_999_999_999,
            refresh_token_expiration=9_999_999_999,
        )
    )
    return store


def test_get_returns_fixture_page(config, token_store):
    body = load_fixture("orders_page1.json")
    session = FakeSession(FakeResponse(200, body))
    client = CloverClient(config, token_store, session=session)

    result = client.get(f"/v3/merchants/{config.merchant_id}/orders")

    assert result == body
    _url, headers, _params = session.calls[0]
    assert headers["Authorization"] == "Bearer fake-access-token"


def test_paginate_yields_elements_from_one_page(config, token_store):
    body = load_fixture("orders_page1.json")
    session = FakeSession(FakeResponse(200, body))
    client = CloverClient(config, token_store, session=session)

    elements = list(client.paginate(f"/v3/merchants/{config.merchant_id}/orders"))

    assert elements == body["elements"]
    assert len(elements) == 2


def test_paginate_handles_empty_page(config, token_store):
    body = load_fixture("orders_page_empty.json")
    session = FakeSession(FakeResponse(200, body))
    client = CloverClient(config, token_store, session=session)

    elements = list(client.paginate(f"/v3/merchants/{config.merchant_id}/orders"))

    assert elements == []


def test_get_raises_clover_auth_error_on_401(config, token_store):
    body = load_fixture("error_401.json")
    session = FakeSession(FakeResponse(401, body))
    client = CloverClient(config, token_store, session=session)

    with pytest.raises(CloverAuthError):
        client.get(f"/v3/merchants/{config.merchant_id}/orders")


def test_get_raises_rate_limit_error_on_429_with_retry_after(config, token_store):
    body = load_fixture("error_429.json")
    session = FakeSession(FakeResponse(429, body, headers={"Retry-After": "2"}))
    client = CloverClient(config, token_store, session=session)

    with pytest.raises(CloverRateLimitError) as exc_info:
        client.get(f"/v3/merchants/{config.merchant_id}/orders")

    assert exc_info.value.retry_after == 2.0


def test_get_valid_access_token_raises_clearly_when_store_is_empty(config, tmp_path):
    from ingestion.clover.auth import get_valid_access_token

    empty_store = TokenStore(tmp_path / "missing.json")

    with pytest.raises(CloverAuthError):
        get_valid_access_token(config, empty_store)
