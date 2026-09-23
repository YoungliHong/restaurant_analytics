"""Spike script: pull orders from Clover and print them.

Run against canned fixture data (no network, no account needed) — this
is what proves the scaffold is wired end to end:
    python -m scripts.spike.pull --fixture

Run against a real sandbox test merchant, once A1 has one (needs
CLOVER_CLIENT_ID / CLOVER_CLIENT_SECRET / CLOVER_REDIRECT_URI /
CLOVER_MERCHANT_ID in the environment, and a token already saved via
scripts/spike/oauth_flow.py or a sandbox test token dropped into the
token store):
    python -m scripts.spike.pull

Not production code — this is throwaway exploration for the sandbox
spike (docs/phase7-backlog.md, A1). The real extractor lives in
ingestion/clover/extract.py once L2 starts.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from ingestion.clover.client import CloverClient
from ingestion.clover.config import CloverConfig, load_config
from ingestion.clover.testing import FakeResponse, FakeSession, load_fixture
from ingestion.clover.token_store import TokenSet, TokenStore

DEFAULT_TOKEN_PATH = Path.home() / ".clover" / "sandbox_token.json"


def _fixture_config() -> CloverConfig:
    return CloverConfig(
        environment="sandbox",
        client_id="fixture-client-id",
        client_secret="fixture-client-secret",
        redirect_uri="https://localhost/callback",
        merchant_id="FIXTUREMERCHANT",
    )


def _fixture_token_store(tmp_dir: Path) -> TokenStore:
    store = TokenStore(tmp_dir / "fixture_token.json")
    store.save(
        TokenSet(
            access_token="fixture-access-token",
            refresh_token="fixture-refresh-token",
            access_token_expiration=9_999_999_999,
            refresh_token_expiration=9_999_999_999,
        )
    )
    return store


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fixture", action="store_true", help="use canned fixture data instead of the network"
    )
    # TODO(youngli): once A1 settles the timestamp field/timezone question
    # (docs/clover/NOTES.md "Open"), add --since / --until here to test
    # the modifiedTime incremental filter, not just a full pull.
    args = parser.parse_args()

    if args.fixture:
        import tempfile

        with tempfile.TemporaryDirectory() as tmp_dir:
            config = _fixture_config()
            token_store = _fixture_token_store(Path(tmp_dir))
            session = FakeSession(FakeResponse(200, load_fixture("orders_page1.json")))
            client = CloverClient(config, token_store, session=session)
            orders = list(
                client.paginate(f"/v3/merchants/{config.merchant_id}/orders")
            )
    else:
        config = load_config()
        token_store = TokenStore(DEFAULT_TOKEN_PATH)
        client = CloverClient(config, token_store)
        orders = list(
            client.paginate(
                f"/v3/merchants/{config.merchant_id}/orders",
                params={"expand": "lineItems,payments,discounts"},
            )
        )

    print(json.dumps(orders, indent=2))


if __name__ == "__main__":
    main()
