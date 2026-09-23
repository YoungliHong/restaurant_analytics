"""Guard: scan tests/fixtures/clover/ for anything that looks like it
wasn't scrubbed before committing (docs/phase7-backlog.md, A1 step 7 and
CLAUDE.md's "raw real exports never go in a public repo" rule).

Deliberately simple pattern matching, not a substitute for reading the
file before committing it — run this, then look at the diff yourself.

Usage:
    python -m scripts.spike.scrub_check
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

FIXTURES_DIR = Path(__file__).resolve().parent.parent.parent / "tests" / "fixtures" / "clover"

# Patterns that suggest a real (not FAKE_-prefixed) token, secret, or card
# fragment slipped into a fixture. Deliberately broad — a false positive
# just means a human looks at the line, a false negative ships a secret.
SUSPECT_PATTERNS = [
    re.compile(r'"access_token"\s*:\s*"(?!FAKE_)[^"]{20,}"'),
    re.compile(r'"refresh_token"\s*:\s*"(?!FAKE_)[^"]{20,}"'),
    re.compile(r'"client_secret"\s*:\s*"[^"]+"'),
    # Looks like a card number (15-19 digits). Deliberately starts at 15,
    # not 13: Clover epoch-millisecond timestamps (createdTime,
    # modifiedTime, ...) are legitimately 13 digits and would otherwise
    # false-positive on every fixture.
    re.compile(r"\b\d{15,19}\b"),
]


def scan_file(path: Path) -> list[str]:
    text = path.read_text()
    hits = []
    for pattern in SUSPECT_PATTERNS:
        for match in pattern.finditer(text):
            hits.append(f"{path.name}: {match.group(0)[:60]}")
    return hits


def main() -> int:
    all_hits: list[str] = []
    for path in sorted(FIXTURES_DIR.glob("*.json")):
        all_hits.extend(scan_file(path))

    if all_hits:
        print("scrub_check: possible unscrubbed data found:", file=sys.stderr)
        for hit in all_hits:
            print(f"  {hit}", file=sys.stderr)
        return 1

    print(f"scrub_check: clean ({len(list(FIXTURES_DIR.glob('*.json')))} fixture files checked)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
