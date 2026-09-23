"""OAuth 2.0 (high-trust / auth-code flow) for the Clover API.

Why OAuth and not a static token: static test API tokens work only in
sandbox (docs/clover/NOTES.md); production access requires the full
authorize -> exchange -> refresh cycle, with tokens that expire (access
tokens: 30 minutes). scripts/spike/oauth_flow.py drives this by hand for
the sandbox spike (A1) — that hands-on run is what settles refresh-token
lifetime and rotation behavior, which the TODOs below depend on.
"""

from __future__ import annotations

import time
from urllib.parse import urlencode

import requests

from .config import CloverConfig
from .exceptions import CloverAuthError
from .token_store import TokenSet, TokenStore


def build_authorize_url(config: CloverConfig) -> str:
    """Build the URL to send a merchant to, to authorize this app.

    Boilerplate string formatting — see
    docs/clover/NOTES.md (high-trust-app-auth-flow) for the flow this
    kicks off.
    """
    params = {"client_id": config.client_id, "redirect_uri": config.redirect_uri}
    return f"{config.oauth_base_url}/oauth/v2/authorize?{urlencode(params)}"


def exchange_code(config: CloverConfig, code: str) -> TokenSet:
    """Trade a one-time authorization code (from the redirect after
    build_authorize_url) for an access/refresh token pair.

    TODO(youngli): POST to f"{config.oauth_base_url}/oauth/v2/token" with
    client_id, client_secret, code (see docs/clover/NOTES.md — confirm
    form-encoded vs JSON body against the live sandbox response, A1 step
    6). Raise CloverAuthError on a non-2xx response rather than letting a
    raw requests exception surface.
    """
    raise NotImplementedError(
        "exchange_code: run the sandbox OAuth flow by hand first (A1 step 6), "
        "confirm the request/response shape, then implement this."
    )


def refresh_tokens(config: CloverConfig, refresh_token: str) -> TokenSet:
    """Trade a refresh token for a new access/refresh token pair.

    TODO(youngli): this is the "token refresh" logic reserved for you in
    docs/phase7-backlog.md (A1). Before implementing, settle in the
    sandbox spike: does the refresh token rotate (is a new one returned,
    and does the old one stop working)? That determines whether every
    caller of get_valid_access_token() below needs to persist a new
    refresh token on every single call, or only when Clover chooses to
    rotate it. Record the answer in docs/clover/NOTES.md as
    sandbox-verified before relying on it in production.
    """
    raise NotImplementedError(
        "refresh_tokens: settle rotation behavior in the A1 sandbox spike first."
    )


def get_valid_access_token(config: CloverConfig, store: TokenStore, leeway_seconds: int = 60) -> str:
    """Return a currently-valid access token for `config`, refreshing and
    persisting a new one if the stored one is expired or close to it.

    Stub: currently returns whatever is in the token store without
    checking expiry, so the rest of the client (get/paginate) can be
    wired and tested end-to-end before this is finished.

    TODO(youngli): compare tokens.access_token_expiration (Unix SECONDS —
    see token_store.py) against time.time() + leeway_seconds. If expired
    or missing, call refresh_tokens(), store.save() the result, and
    return the new access_token. Until this is done, a real run will
    start failing with 401s the first time a token actually expires
    (30 minutes in, per docs/clover/NOTES.md) — client.py's 401-retry
    TODO also depends on this being correct.
    """
    tokens = store.load()
    if tokens is None:
        raise CloverAuthError(
            401,
            "no token in the token store — run scripts/spike/oauth_flow.py "
            "(or seed a sandbox test token) before calling the API",
        )
    return tokens.access_token
