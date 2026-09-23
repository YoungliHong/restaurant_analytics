"""Spike script: deliberately trigger error responses against the sandbox
(docs/phase7-backlog.md, A1 step 5) and save the raw bodies as fixtures.

Not production code. Each function below should, once run for real,
overwrite the matching hand-built fixture in tests/fixtures/clover/ with
what the sandbox actually returned, and get a line in docs/clover/NOTES.md
tagged sandbox-verified.

TODO(youngli):
  - trigger_401(): call client.get() with a token store pointed at a
    file holding a garbage access_token, confirm the 401 body shape,
    save it over tests/fixtures/clover/error_401.json.
  - trigger_404(): GET a nonexistent order id, save the body shape (not
    currently in the fixture set — add tests/fixtures/clover/error_404.json
    if the shape differs meaningfully from 401/429).
  - trigger_400(): send a malformed filter (e.g. a non-numeric
    modifiedTime) and save the body shape.
  - trigger_429(): fire a burst of concurrent requests above the
    documented per-token limit (16 req/s, 5 concurrent — see
    docs/clover/NOTES.md) and see whether the sandbox actually enforces
    it. If it doesn't, say so in NOTES.md instead of guessing — the
    committed error_429.json fixture stays labeled "hand-built from
    docs" until this is confirmed either way.
"""

from __future__ import annotations


def trigger_401() -> None:
    raise NotImplementedError("see module docstring")


def trigger_404() -> None:
    raise NotImplementedError("see module docstring")


def trigger_400() -> None:
    raise NotImplementedError("see module docstring")


def trigger_429() -> None:
    raise NotImplementedError("see module docstring")


if __name__ == "__main__":
    raise SystemExit(
        "errors.py has no CLI yet — call the trigger_* functions from a "
        "REPL/notebook once a sandbox merchant + token exist (A1)."
    )
