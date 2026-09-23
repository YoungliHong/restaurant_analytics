# Clover fixtures — status

**As of 2026-09-23: every file in this directory is hand-built from
docs.clover.com's documented response shapes, not yet captured from a
real sandbox call.** The sandbox developer account, test merchant, and
app in backlog item A1 haven't been created yet. These fixtures exist so
the client scaffold (`ingestion/clover/`) and its tests have something to
run against in the meantime.

**Once A1's sandbox pull happens, replace these files with real sandbox
responses** (scrub any real secret first — see `scripts/spike/scrub_check.py`)
and update `docs/clover/NOTES.md` to move each fact from `docs` to
`sandbox-verified`. A fixture that hasn't been replaced yet is not
evidence of real API behavior, only of documented behavior.

Rules (from CLAUDE.md, unchanged going forward):
- Sandbox JSON is fake data and may be committed here.
- Real exports (real orders, real customers, anything from the family's
  actual account) never go here — they go in `data/private/` or
  `docs/private/`, both gitignored, never committed.
- No card data, no PII, ever, even in sandbox fixtures — the sandbox test
  merchant should be seeded with fake data (Faker-style), not real
  people's information.

| File | What it represents | Status |
|---|---|---|
| `orders_page1.json` | A full page of orders (list endpoint) | hand-built from docs |
| `orders_page_empty.json` | A page past the end of results (0 elements) | hand-built from docs |
| `order_with_modifiers_discount.json` | One order, expanded: line items, modifiers, a line-item discount, an order-level discount | hand-built from docs |
| `order_refunded.json` | One order after a refund | hand-built from docs — refund representation is on the A1 "Open" list; this fixture is a guess to be corrected |
| `error_401.json` | Expired/invalid token response | hand-built from docs |
| `error_429.json` | Rate-limit response | hand-built from docs — A1 notes whether sandbox actually enforces this |
| `token_exchange_response.json` | Response from POST /oauth/v2/token | hand-built from docs |
| `token_refresh_response.json` | Response from POST /oauth/v2/refresh | hand-built from docs — whether the refresh_token value actually changes (rotation) is on the A1 "Open" list |
