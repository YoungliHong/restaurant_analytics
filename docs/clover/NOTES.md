# Clover facts

One line per fact: claim (own words) | source | date | tag.
Tags: `docs` = read on docs.clover.com, not yet exercised. `sandbox-verified` = observed in sandbox (may differ in production).
Facts below were gathered via summarized page fetches on 2026-09-21; re-read the source page in full before coding against any of them.
Snapshots: docs/clover/snapshots/ (gitignored).

## Auth and tokens
| Claim | Source | Date | Tag |
|---|---|---|---|
| Static test API tokens are sandbox-only; production needs OAuth expiring access+refresh tokens | https://docs.clover.com/dev/docs/generate-a-test-api-token.md | 2026-09-21 | docs |
| Test token is created in the test merchant's dashboard under Settings > API tokens, with selectable permissions | same | 2026-09-21 | docs |
| OAuth v2 has three endpoints: authorize, token (code exchange), refresh | https://docs.clover.com/dev/docs/generate-expiring-tokens-using-v2-oauth-flow.md | 2026-09-21 | docs |
| Token exchange for a backend app takes client_id, client_secret, code; response carries access token, refresh token, and an expiration for each | same | 2026-09-21 | docs |
| Access tokens live 30 minutes; an expired one gets a 401 | https://docs.clover.com/dev/docs/oauth-and-tokens-faqs.md | 2026-09-21 | docs |
| OAuth expiration fields are Unix seconds; the rest of the API uses milliseconds | same | 2026-09-21 | docs |
| Sandbox and production use the same OAuth flow on different hosts (sandbox.dev.clover.com vs www.clover.com) | same | 2026-09-21 | docs |
| Permissions (read/write per category) are set per app in the Global Developer Dashboard; changing them after install requires the merchant to reinstall | https://docs.clover.com/dev/docs/gdp-set-app-permissions | 2026-09-21 | docs |
| A private app is not in the App Market and is installed through an OAuth authorize link, but it still goes through Clover's approval | https://docs.clover.com/dev/docs/private-apps.md | 2026-09-21 | docs |
| Submitting a private app for production approval has no stated turnaround, and requires the production Developer Dashboard — separate from the sandbox dashboard used for A1, so submission doesn't block sandbox work | https://docs.clover.com/dev/docs/private-apps.md | 2026-09-23 | docs (confirmed by Youngli re-reading the live page) |

## Limits and errors
| Claim | Source | Date | Tag |
|---|---|---|---|
| Rate limits: 16 req/s and 5 concurrent per token; 50 req/s and 10 concurrent per app | https://docs.clover.com/dev/docs/api-usage-rate-limits.md | 2026-09-21 | docs |
| Over the limit returns 429 with a retry-after header; X-RateLimit-* headers say which limit tripped | same | 2026-09-21 | docs |
| Docs recommend filtering by modifiedTime and using the Export API for historical backfill | same | 2026-09-21 | docs |

## Orders
| Claim | Source | Date | Tag |
|---|---|---|---|
| createdTime filters take epoch milliseconds | https://docs.clover.com/dev/docs/orders-faqs | 2026-09-21 | docs |
| Max 1000 records per request; page with limit/offset; split large historical pulls by time range | same | 2026-09-21 | docs |
| Modifier amounts are in cents | same | 2026-09-21 | docs |
| The platform orders API does not process refunds (dashboard or Ecommerce API do); paymentState "Credited" indicates refunded | same | 2026-09-21 | docs |
| The expand parameter pulls nested objects in one call | same | 2026-09-21 | docs |
| Test merchants are non-billable (isBillable=false) | https://docs.clover.com/clover-platform/docs/faqs | 2026-09-21 | docs |

## Open: the sandbox spike must settle these (see docs/phase7-backlog.md, A1)
- Which timestamp is "order date" (createdTime vs clientCreatedTime vs modifiedTime), and the timezone it means.
- modifiedTime filter syntax and whether it catches refunds and voids.
- Can REST-seeded orders set their own timestamps (needed for daypart tests)?
- How refunds, voids, discounts and modifiers appear in the expanded JSON.
- Refresh token lifetime; whether refresh rotates the refresh token; what an old refresh token does after use.
- Whether sandbox enforces the 429 limits.
- Whether REST-created orders show in the sandbox dashboard.
- Production only (cannot be sandbox-verified): whether Clover approves a private app for one merchant, and how long it takes.
