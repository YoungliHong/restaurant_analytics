"""Persists OAuth token pairs between runs.

Why this exists as its own module: production access tokens expire (30
minutes, per docs/clover/NOTES.md) and the refresh token obtained to
replace them must survive between scheduled runs — there's no login
prompt to fall back on in an unattended job. Sandbox test tokens don't
need this, but routing everything through the same TokenStore interface
means swapping sandbox for production later doesn't touch calling code.

File-backed by default; the directory this points at should be
git-ignored (see .gitignore: CLOVER_TOKEN_DIR / data/private/) so a token
never lands in the repo.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class TokenSet:
    """One OAuth token pair, as returned by /oauth/v2/token or /oauth/v2/refresh.

    access_token_expiration / refresh_token_expiration are Unix SECONDS —
    docs/clover/NOTES.md flags that the OAuth endpoints use seconds while
    the rest of the Clover API uses milliseconds. That mismatch is the
    most likely source of an off-by-1000x expiry bug here.
    """

    access_token: str
    refresh_token: str
    access_token_expiration: int
    refresh_token_expiration: int


class TokenStore:
    """Loads/saves a TokenSet to a local JSON file.

    One TokenStore instance per merchant_id (see config.py) rather than
    one file holding multiple merchants — keeps a corrupted or lost file
    scoped to a single merchant instead of taking down every location at
    once.
    """

    def __init__(self, path: Path):
        self.path = path

    def load(self) -> TokenSet | None:
        """Return the stored TokenSet, or None if nothing has been saved yet."""
        if not self.path.exists():
            return None
        data = json.loads(self.path.read_text())
        return TokenSet(**data)

    def save(self, tokens: TokenSet) -> None:
        """Persist a TokenSet, replacing whatever was stored before.

        TODO(youngli): make this atomic — write to a sibling temp file
        and `os.replace()` it into place — so a crash mid-write can't
        leave a truncated or empty token file. That's the "rotation
        handling" piece in docs/phase7-backlog.md (A1): if a run dies
        right after Clover issues a new refresh token but before it's
        durably saved, the job is stranded until someone re-runs the
        OAuth flow by hand. Also decide: does a failed save mean we
        should NOT have used the refresh token in the first place (i.e.
        save-then-use), given rotation may invalidate the old refresh
        token on first use — settle that in the A1 sandbox spike.
        """
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(json.dumps(asdict(tokens)))
