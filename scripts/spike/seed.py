"""Spike script: seed the sandbox test merchant with orders (docs/phase7-backlog.md, A1 step 3).

Not production code, and deliberately left mostly TODO: seeding requires
a real test merchant and a write-capable sandbox test token (see A1 step
2 — this is a *different* credential from the read-only OAuth app the
rest of ingestion/clover/ uses, since read-only scopes are the
production rule per CLAUDE.md and seeding is a sandbox-only exception).

Known constraint to design around (from CLAUDE.md's Clover API section):
REST-created orders may need complete data and valid inventory items to
show up in the dashboard, so this needs to create/confirm inventory
items before creating orders that reference them. Refunds likely can't
be done via this script at all — the docs say the Platform API doesn't
process refunds — so that step is a manual dashboard action, not code
here.

TODO(youngli):
  1. Create (or confirm) a few inventory items via the Inventory API —
     find the actual endpoint (see docs/clover/NOTES.md "still to read"
     list) and call it with the write-capable test token.
  2. Create orders referencing those items, with line items, at least
     one modifier, a line-item discount, and an order-level discount.
  3. Print each created order's id so it can be looked up in the sandbox
     dashboard and in scripts/spike/pull.py output.
  4. Refund one order — by hand, in the sandbox dashboard, per the note
     above — and note its id here or in docs/clover/NOTES.md so
     pull.py's next run can be checked against it.
"""

from __future__ import annotations

import argparse


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args()
    raise NotImplementedError(
        "seed.py: needs a real sandbox test merchant + write-capable test "
        "token first (A1 step 2). See the module docstring for the seeding "
        "order (inventory items before orders, refund via dashboard)."
    )


if __name__ == "__main__":
    main()
