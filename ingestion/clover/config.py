"""Environment configuration for the Clover client.

Two Clover environments exist (sandbox, production) with different hosts
for OAuth and for the REST API (see docs/clover/NOTES.md). This module is
the single place that decides which one we're pointed at, so nothing else
in the client hardcodes a host. No secrets live here — client_id,
client_secret, and merchant_id are read from the environment, and tokens
live in token_store.py, never in this file or the repo.
"""

from __future__ import annotations

import os
from dataclasses import dataclass

SANDBOX = "sandbox"
PRODUCTION = "production"

# OAuth authorize/token/refresh endpoints. Confirmed in
# docs/clover/NOTES.md (generate-expiring-tokens-using-v2-oauth-flow).
_OAUTH_BASE_URLS = {
    SANDBOX: "https://sandbox.dev.clover.com",
    PRODUCTION: "https://www.clover.com",
}

# REST resource endpoints (orders, inventory, ...). TODO(youngli): confirm
# this host against the sandbox spike (A1) the first time client.get()
# actually runs — docs.clover.com's reference pages use a bare
# {serverUrl}/v3/merchants/{mId}/... path and don't always spell out
# whether sandbox uses a distinct API host from the OAuth host. Record
# whatever you find as sandbox-verified in docs/clover/NOTES.md.
_API_BASE_URLS = {
    SANDBOX: "https://apisandbox.dev.clover.com",
    PRODUCTION: "https://api.clover.com",
}


@dataclass(frozen=True)
class CloverConfig:
    """Everything the client needs to know about which Clover it's talking to."""

    environment: str  # SANDBOX or PRODUCTION
    client_id: str
    client_secret: str
    redirect_uri: str
    merchant_id: str

    @property
    def oauth_base_url(self) -> str:
        return _OAUTH_BASE_URLS[self.environment]

    @property
    def api_base_url(self) -> str:
        return _API_BASE_URLS[self.environment]


def load_config(environment: str = SANDBOX) -> CloverConfig:
    """Build a CloverConfig from environment variables.

    Expected env vars (see .env.example): CLOVER_CLIENT_ID,
    CLOVER_CLIENT_SECRET, CLOVER_REDIRECT_URI, CLOVER_MERCHANT_ID.
    Raises a clear error if any are missing rather than silently running
    with an empty credential.
    """
    missing = [
        name
        for name in (
            "CLOVER_CLIENT_ID",
            "CLOVER_CLIENT_SECRET",
            "CLOVER_REDIRECT_URI",
            "CLOVER_MERCHANT_ID",
        )
        if name not in os.environ
    ]
    if missing:
        raise RuntimeError(
            f"Missing required Clover env var(s): {', '.join(missing)}. "
            "See .env.example."
        )
    return CloverConfig(
        environment=environment,
        client_id=os.environ["CLOVER_CLIENT_ID"],
        client_secret=os.environ["CLOVER_CLIENT_SECRET"],
        redirect_uri=os.environ["CLOVER_REDIRECT_URI"],
        merchant_id=os.environ["CLOVER_MERCHANT_ID"],
    )
