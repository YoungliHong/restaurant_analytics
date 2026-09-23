"""Spike script: drive the sandbox OAuth authorize -> exchange -> refresh
cycle by hand (docs/phase7-backlog.md, A1 step 6).

This is meant to be run interactively, step by step, not as one
unattended script — the authorize step needs a human in a browser.

    python -m scripts.spike.oauth_flow authorize
        prints the URL to open in a browser; after authorizing, Clover
        redirects to CLOVER_REDIRECT_URI with ?code=... in the query
        string — copy that code for the next step.

    python -m scripts.spike.oauth_flow exchange <code>
        trades the code for an access/refresh token pair, saves it to
        the token store, and prints the expiration.

    python -m scripts.spike.oauth_flow refresh
        uses the stored refresh token to get a new pair. Run this once
        right away (to see the response shape) and once again after the
        access token has actually expired (~30 min, per
        docs/clover/NOTES.md) to confirm the 401 -> refresh -> retry path
        the client.py TODO describes.

Not production code. What this run needs to settle, and where it goes:
  - refresh token lifetime, and whether it rotates on use -> NOTES.md,
    then auth.refresh_tokens()'s TODO
  - exact request/response shape for /oauth/v2/token and /oauth/v2/refresh
    -> NOTES.md, then auth.exchange_code() / auth.refresh_tokens()
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from ingestion.clover.auth import build_authorize_url, exchange_code, refresh_tokens
from ingestion.clover.config import load_config
from ingestion.clover.token_store import TokenStore

DEFAULT_TOKEN_PATH = Path.home() / ".clover" / "sandbox_token.json"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("step", choices=["authorize", "exchange", "refresh"])
    parser.add_argument("code", nargs="?", help="authorization code, for the 'exchange' step")
    args = parser.parse_args()

    config = load_config()
    token_store = TokenStore(DEFAULT_TOKEN_PATH)

    if args.step == "authorize":
        print(build_authorize_url(config))
        return

    if args.step == "exchange":
        if not args.code:
            sys.exit("usage: python -m scripts.spike.oauth_flow exchange <code>")
        tokens = exchange_code(config, args.code)  # TODO(youngli): implement in auth.py
        token_store.save(tokens)
        print(f"saved token, access_token_expiration={tokens.access_token_expiration}")
        return

    if args.step == "refresh":
        existing = token_store.load()
        if existing is None:
            sys.exit("no token saved yet — run the 'exchange' step first")
        tokens = refresh_tokens(config, existing.refresh_token)  # TODO(youngli): implement in auth.py
        token_store.save(tokens)
        print(f"refreshed token, access_token_expiration={tokens.access_token_expiration}")
        # TODO(youngli): after this, try the OLD refresh_token again (e.g.
        # by keeping a copy before overwriting) to see whether it still
        # works — that's the rotation question in the module docstring.


if __name__ == "__main__":
    main()
