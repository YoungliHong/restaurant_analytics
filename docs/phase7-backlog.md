# Phase 7 backlog: real Clover data + AWS landing zone

Branch: `phase-7-clover`. Last updated: 2026-09-21 (backlog created; nothing started).

**Legend.** Mode: `scaffold` (Claude builds the skeleton, Youngli fills in the core and owns it, Claude then reviews) or `review` (Youngli writes, Claude reviews). Blocked: `PEOPLE` (needs a human outside this repo) or `TECH` (needs another item first). Status: `todo`, `doing`, `done`, `blocked`.

**Ground rules that apply to every item** (from CLAUDE.md): read-only scopes only (orders, inventory); no card data, customer PII or wages; no secrets in git; real exports never in the public repo; sandbox JSON is fake and may be committed; every Clover fact goes in `docs/clover/NOTES.md` tagged `docs` or `sandbox-verified`; AWS is described as familiarity-level, never production.

## Milestone order and critical path

```
A1 sandbox spike ──┬─► D2 raw contract ─► L2 extractor ─► L4 stage ─► M1 load ─► M2..M4 models ─► Q* ─► Doc*
A2 rep email ──► A3 access path ──┘            ▲
D1 business questions (PEOPLE) ────────────────┘ (shapes marts, not landing)
```

A1 and A2 start immediately and in parallel. A1 needs no one. Everything downstream of A3 can be built and tested against sandbox fixtures, so a slow rep does not stall the build; it only stalls real data.

---

## 1. Access and discovery

### A1. SANDBOX SPIKE  ·  status: todo  ·  mode: scaffold  ·  blocked: none
**Goal.** Learn Clover's real API behavior (pagination, timestamps, refunds, tokens) in the sandbox, and leave behind a tested client skeleton, fixtures and a complete NOTES.md.

**Steps (in order).**
1. **Scaffold (Claude).** Clover client module for Youngli to fill in. Proposed layout:
   - `ingestion/clover/config.py`: env/base-URL selection (sandbox vs prod), no secrets in code. Complete.
   - `ingestion/clover/auth.py`: OAuth authorize URL builder, `exchange_code()`, `refresh_tokens()`, `get_valid_access_token()`. Signatures + docstrings + TODOs. **Youngli writes:** expiry check, refresh, rotation handling.
   - `ingestion/clover/token_store.py`: `load()` / `save()` interface, file-backed stub writing under a gitignored path. **Youngli writes:** atomic save so a crash mid-rotation cannot lose the new refresh token.
   - `ingestion/clover/client.py`: `get(path, params)` and `paginate(path, params)`. **Youngli writes:** pagination loop, 401-refresh-retry, 429/retry-after backoff.
   - `scripts/spike/`: one throwaway script per step below (seed, pull, errors, oauth). Not production code.
   - `tests/fixtures/clover/` (committed, fake data only) and `tests/ingestion/` (pytest against fixtures with a stubbed HTTP layer).
   - `.gitignore` additions: token file path, `data/private/`. Add a guard so real data cannot land in `tests/fixtures/clover/` (short README there plus a check in the scrub step).
   - Runs end to end with stubs: `python -m scripts.spike.pull` returns canned fixture data without network.
2. **Account.** Sandbox developer account, a test merchant, an app. Two credentials on purpose: a *write-capable test token* used only to seed data, and the *read-only OAuth app* (orders + inventory read) that mirrors what production would use. Being able to explain that split is part of the point.
3. **Seed.** Inventory items first (REST-created orders may not show in the dashboard without valid inventory items), then orders with line items, modifiers, an order-level discount, a line-item discount. Refund via the sandbox dashboard (docs say the platform API can't refund). Also a void/deleted line item. Try to set timestamps on seeded orders (clientCreatedTime); if that isn't possible, note it, because daypart tests then need hand-edited fixtures labeled synthetic.
4. **Pull.** Orders with line items, modifiers, discounts, payments expanded. Test `limit`/`offset` (max page size, what happens past the end, does offset shift when rows are added mid-pagination). Incremental pull by `modifiedTime`: does a refund or a modifier edit bump it? Identify the "order date" field and its timezone (compare to the merchant's timezone property). Record how refunds, voids and modifiers appear.
5. **Errors.** 401 (bad/expired token), 404, 400 (bad filter). Try to trigger 429 with a concurrent burst above 16 req/s per token; if sandbox doesn't enforce, say so and mark the 429 fixture `hand-built from docs, not observed`.
6. **OAuth end to end.** Register redirect URL, authorize, exchange code, call API, wait past 30 minutes (or force expiry) to see the 401, refresh, persist the new pair. Measure: refresh token lifetime, whether refresh rotates the refresh token, whether the old one still works. Confirm the sandbox flow matches the docs, and label everything sandbox-verified with a "may differ in production" note.
7. **Fixtures + NOTES.** Save raw responses into `tests/fixtures/clover/` (one file per scenario, names like `orders_page1.json`, `order_refunded.json`, `error_401.json`, `token_refresh.json`). Scrub tokens, merchant/app secrets. Complete NOTES.md and empty its "Open" section.

**Acceptance criteria.**
- Every claim about pagination, timestamps, refunds and token behavior in NOTES.md is tagged `docs` or `sandbox-verified`, with a source URL or a fixture filename. Zero untagged claims. The "Open" list in NOTES.md is empty or explicitly moved to "production-only unknowns".
- Fixtures exist for: a full page, a partial last page, an empty page, an order with modifiers + discount, a refunded order, a voided/deleted line item, a 401, a 429 (observed or labeled hand-built), a token response, a refreshed token response.
- A pytest loader-style test reads the fixtures and passes without network or credentials.
- Timebox: 3 sessions. If seeding through REST turns out to be a swamp, stop at 2 and record it (see Risk 3 in the risks section).

**Interview question it answers.** "Walk me through how you handled OAuth token expiry and refresh in an ingestion job." And: "How did you find out what the API actually does, versus what the docs say?"

### A2. Rep logistics request  ·  todo  ·  mode: n/a (writing)  ·  blocked: PEOPLE (dad, rep)
**Goal.** Send the rep a logistics-only ask.
**Acceptance.** Draft saved in `docs/private/`, sent by dad or with dad's OK. Asks only: does a dashboard login exist; merchant ID per location; direct with Clover or via a reseller; can I be added as a read-only dashboard user. No API questions.
**Depends.** Dad's OK (PEOPLE). Independent of A1.
**Interview question.** "How did you get access to real data, and what was the trust/consent model?"

### A3. Production access path — CSV export is the default, OAuth API is a swap-in  ·  status: doing (Youngli submitting the app in parallel)  ·  mode: n/a (decision + submission)  ·  blocked: PEOPLE (A2 answers, parents' export access) + PEOPLE (Clover approval, unknown turnaround)
**Decision (2026-09-23).** Default path is dashboard CSV export of order line items. The private-app OAuth path is a swap-in if/when Clover approves it — it is not the plan of record, because approval (a) requires the production Developer Dashboard, a separate track from sandbox, and (b) has no stated turnaround and is outside our control. Building API-only with no fallback would block the whole project on someone else's queue.
**What this changes.** D2's raw contract and L2's extractor must both accept CSV as the primary input shape, with the OAuth JSON path as an alternate source feeding the *same* raw contract — not two different pipelines downstream of landing.
**Acceptance.**
- Decision recorded here and, later, in README Design Decisions.
- CSV path: identify exactly which export(s) carry order line items with modifiers, discounts and refunds, and their columns. Needs a sample file from parents' dashboard (PEOPLE), stored only under `docs/private/` or `data/private/`, never committed.
- API path (parallel, not blocking): Youngli starts the production app submission once basic app details are ready — do not wait for the spike to finish first. Track submission date and any response in `docs/private/`.
- Either way the S3 landing contract (D6) stays the same, so downstream work doesn't change regardless of which source wins.
**Depends.** A1 (sandbox client still gets built and used, in case/when API access lands), A2 (whether CSV export is even reachable without a dashboard login).
**Interview question.** "What would you do if the vendor API wasn't available to you?" — now answerable concretely: designed the loader source-agnostic and shipped on CSV while an API approval sat in a queue I didn't control.

### A4. Parents' buy-in and data-sharing terms  ·  todo  ·  blocked: PEOPLE (parents)
**Goal.** Agree in plain language what data I access, what stays private, and what I may publish.
**Acceptance.** Short written note in `docs/private/` recording: read-only, no card/customer/wage data, aggregate or anonymized results only in public, and which restaurant/location names may appear. Also: a first pass at what decision they would act on (feeds D1).
**Interview question.** "How did you handle data privacy on a real business's data?"

---

## 2. Design decisions

Each is a decision doc (a few sentences in this file, later promoted to README Design Decisions). Mode: review (Youngli writes; Claude pressure-tests).

### D1. Business questions  ·  todo  ·  blocked: PEOPLE (parents)
**Goal.** Pick 1-2 questions they would act on. Candidates: sales by daypart vs. staffing, item and modifier mix, location comparison.
**Acceptance.** Each chosen question has: the decision it informs, the metric, the grain, the minimum data needed, and a kill condition ("if the data can't answer this, drop it"). Wages are out of scope, so "vs. staffing" means hours of operation and rough headcount supplied by parents, not payroll data; note that.
**Depends.** A4 (loosely); can start with a candidate list before data access.
**Interview question.** "What business question did the data answer, and what did they do with it?"

### D2. Raw contract and grain  ·  todo  ·  blocked: TECH (A1, A3)
**Goal.** Decide what lands raw, given the source is now CSV-first (A3) with OAuth JSON as a parallel/future source into the *same* contract.
**Recommendation to pressure-test.** Land each source unmodified except for a field allowlist that drops PII/card fields at extraction (CSV: drop columns; JSON: drop keys); flatten/normalize in dbt staging, not at ingest. A `source_system` column (`clover_csv` / `clover_api`) on the raw tables lets both feed the same staging models without staging needing to know which one produced a row. Reason: reprocessing is possible without re-exporting or re-calling the API, and it mirrors the existing "raw stays raw" rule.
**Acceptance.** Written contract in `models/staging/_sources.md` style: grain and PK for each raw table (orders, line_items, modifiers, discounts, payments-summary), what "one row" means after updates, the field allowlist per source, and the `source_system` tag.
**Interview question.** "Why land raw JSON instead of flattening on ingest?" plus "how did you design for two source formats without duplicating the pipeline?"

### D3. Customer identity and coexistence with the synthetic model  ·  todo  ·  blocked: TECH (A1, A3 findings)
**Goal.** Decide how a no-customer-identity source fits a model built around `customers` and `loyalty_tier`.
**Options.** (i) new `clover` source and parallel staging/marts, synthetic pipeline untouched; (ii) replace synthetic. **Recommendation:** (i). The synthetic pipeline stays as the reproducible demo (anyone can run it), and Clover models are additive. `dim_customers` and `fct_price_ratio_by_tier` have no Clover analog and are not ported. State this rather than faking a customer table.
**Acceptance.** Decision written; list of existing models that are reused, adapted or not applicable; note on `generate_schema_name` collision risk for the new schemas.
**Interview question.** "The real data broke your data model. What did you change, and what did you refuse to fake?"

### D4. Delivery and third-party channel coverage  ·  todo  ·  blocked: PEOPLE (parents)
**Goal.** Establish which sales are not in Clover (third-party delivery) and how to handle them.
**Acceptance.** Parents say which channels they use and whether those orders ring through Clover. If not: scope statement that the model covers "Clover-recorded sales", with a stated share estimate. No claims about total revenue.
**Interview question.** "How do you communicate the limits of a dataset to a non-technical owner?"

### D5. Time and money conventions  ·  todo  ·  blocked: TECH (A1)
**Goal.** Fix the order-date field, timezone, and units.
**Acceptance.** Documented: which field defines order date; conversion from epoch milliseconds to Eastern time (DC, DST-aware); money stored as integer cents in raw and converted once in staging; late-night orders after midnight belong to which business day (ask parents for closing hours).
**Interview question.** "How did you handle timezones and DST in order timestamps?"

### D6. S3 layout and load semantics  ·  todo  ·  blocked: TECH (A1)
**Goal.** Pick key layout and write semantics.
**Recommendation.** `s3://<bucket>/clover/<merchant_id>/<resource>/extract_date=YYYY-MM-DD/<run_id>.ndjson`; objects immutable, one run writes new keys; retries with the same run_id overwrite the same key (idempotent). Decide whether the bucket lives in a personal AWS account or the family's account (see open question 1).
**Acceptance.** Layout, naming and idempotency rule written; explanation of what makes a re-run safe.
**Interview question.** "How do you make an extract-and-load job safe to re-run?"

### D7. Publication policy  ·  todo  ·  blocked: PEOPLE (A4)
**Goal.** Decide what goes in the public repo and README.
**Acceptance.** Written rule: public = code, sandbox fixtures, schema diagrams, aggregate or anonymized results with parents' OK; private = raw exports, merchant IDs, location-level financials unless approved. A pre-commit check or CI grep for obvious leaks (merchant IDs, `.p8`, tokens).
**Interview question.** "What's in your public repo and what isn't, and how do you enforce it?"

---

## 3. Landing zone (S3 / IAM)

### L1. S3 bucket + IAM for the extractor and Snowflake  ·  todo  ·  mode: scaffold  ·  blocked: PEOPLE (AWS account ownership, open question 1) + TECH (D6)
**Goal.** A dedicated landing bucket (not the Framer site bucket) with two least-privilege identities: one that writes (extractor) and one that Snowflake assumes to read.
**Scaffold delivers (complete, boilerplate).** Bucket policy (block public access, TLS-only, SSE), IAM policy JSON for writer (PutObject on the prefix) and for the Snowflake reader role (GetObject/ListBucket on the prefix), a CloudFormation or plain CLI script to create them, lifecycle rule, and a README of what each piece is for. **Youngli fills in:** decisions on prefix scoping and lifecycle retention, and writes the "why" for each statement.
**Acceptance.**
- Bucket has public access blocked, encryption on, versioning decision recorded.
- Writer identity can put only under `clover/`; a test upload to another prefix fails (proof of least privilege).
- No AWS keys in the repo; extractor uses a named profile or env vars documented in the README.
- Cost note: expected storage and request cost (should be pennies).
**Interview question.** "Why a separate bucket and role for Snowflake? What would go wrong with one over-broad role?" (Framed as familiarity-level: "I set this up for a personal project," not production.)

### L2. Loader to S3 — CSV path first, API extractor as a swap-in  ·  todo  ·  mode: scaffold  ·  blocked: TECH (D2, D6, L1) + PEOPLE (A3: sample export, or approval)
**Goal.** Land Clover order data in S3 under the shared raw contract (D2), from whichever source is live. Two producers, one landing shape:
- `ingestion/clover/csv_loader.py` — **the default, built first.** Reads a manually-placed dashboard export (from `data/private/`, gitignored), validates columns against the D2 contract, tags rows `source_system='clover_csv'`, writes to S3. No watermark/incremental logic needed if exports are pulled by hand for now — note that limitation rather than over-building.
- `ingestion/clover/extract.py` — the OAuth API extractor from A1, promoted to run against production once A3's approval lands. Same `run(merchant_id, since, until, run_id)` shape, tags rows `source_system='clover_api'`, writes to the same S3 prefix structure.
**Scaffold delivers.** Both modules' skeletons, config for merchants, shared S3 writer wrapper (boto3), CLI entrypoints, stubs runnable against fixtures with a mocked S3 (moto or a local dir). **Youngli writes:** CSV column validation and error handling; for the API path, the incremental watermark (storage, lookback window for late modifications), pagination/watermark interplay, the field allowlist, idempotent key naming.
**Acceptance.**
- CSV path runs end to end on a sample export (fake/sandbox-shaped until a real one exists) and writes correctly-tagged S3 objects.
- Re-running the same input produces the same keys, no duplicate data downstream (idempotent either way).
- API path (once exercised): a record modified after the watermark is picked up on the next run (fixture test); watermark advances only after a successful write; two merchant IDs don't share a watermark.
- Allowlist test on both paths: a fixture with PII fields lands with those fields removed.
**Interview question.** "How does your incremental extract avoid missing late updates or double-counting?" plus "how did you keep a CSV path and an API path from becoming two pipelines?"

### L3. Token storage for production  ·  todo  ·  mode: scaffold  ·  blocked: TECH (A1, A3)
**Goal.** Decide where refresh tokens live between runs (local file with permissions vs. AWS SSM Parameter Store/Secrets Manager) and make rotation safe.
**Design reason to check before adding an AWS service.** If the extractor runs only on Youngli's laptop, a chmod-600 local file is enough and adding Secrets Manager is keyword padding. If it moves to a scheduled runner (Airflow follow-on X1), a shared store is justified. Decide on that basis.
**Acceptance.** Decision written; if rotation applies, concurrent-run test shows two runs can't clobber each other's rotated token (lock or single-writer).
**Interview question.** "What happens if your job crashes right after Clover rotates the refresh token?"

### L4. Snowflake storage integration + external stage  ·  todo  ·  mode: scaffold  ·  blocked: TECH (L1) + Snowflake privileges (I own the account; check TRANSFORMER vs ACCOUNTADMIN needs)
**Goal.** Snowflake reads S3 without stored keys, via storage integration and an IAM role trust relationship.
**Scaffold delivers (complete SQL templates).** `sql/02_clover_stage.sql`: storage integration, `DESC INTEGRATION` step to copy the AWS IAM user ARN and external ID into the role trust policy, external stage, file format (NDJSON), grants. **Youngli fills in:** the two-step trust handshake, and explains it.
**Acceptance.** `LIST @clover_stage` shows the objects; no AWS keys in Snowflake or the repo; grants documented (who can use the integration).
**Interview question.** "Why a storage integration instead of access keys on the stage?"

---

## 4. Load and modeling

### M1. Idempotent load into `RAW_DATA.CLOVER`  ·  todo  ·  mode: scaffold  ·  blocked: TECH (L4, D2)
**Goal.** COPY INTO raw tables (`VARIANT` payload plus load metadata: file name, row number, load time) safely re-runnable.
**Scaffold delivers.** DDL and COPY templates, a small `load.py` that runs them. **Youngli writes:** the idempotency reasoning (COPY load history vs. `FORCE`, what happens on reprocess after a fix, how to replay a single day).
**Acceptance.** Loading the same files twice yields the same row count; replaying a specific `extract_date` is a documented, tested procedure; metadata columns let a bad file be traced back to its S3 key.
**Interview question.** "How does COPY INTO avoid loading a file twice, and how do you deliberately reload?"

### M2. Staging models `stg_clover_*`  ·  todo  ·  mode: review  ·  blocked: TECH (M1 or fixtures loaded to a dev schema)
**Goal.** 1:1 staging: flatten orders, line items, modifiers, discounts; cents to dollars; epoch ms to local timestamp.
**Acceptance.** Model per raw table, sources declared with `_sources.md` contract, grain and PK documented; tests: `not_null`/`unique` on ids, `accepted_values` on state fields (using `arguments:` nesting per dbt 2.0.0a2); passes `dbt build` against fixtures loaded to a dev schema. Includes a conceptual walkthrough of `LATERAL FLATTEN` before Youngli writes it (unfamiliar pattern).
**Interview question.** "How did you flatten nested JSON in Snowflake, and what's the grain after flattening?"

### M3. Intermediate: latest-version-per-order, refunds/voids, modifiers  ·  todo  ·  mode: review  ·  blocked: TECH (M2, A1 refund findings)
**Goal.** One current row per order (the same order can arrive multiple times as it is modified), correct net revenue after refunds/voids, modifier exploded to a usable grain.
**Acceptance.** Dedup keyed on order id, keeping the latest `modifiedTime`, replacing the synthetic "byte-identical duplicates" assumption (README states that caveat explicitly; here it doesn't hold, and this is the real fix). Refund/void treatment documented with a fixture-based test. Revenue reconciles to the sum of fixture line items minus discounts minus refunds.
**Interview question.** "Your synthetic dedup assumed identical duplicates. What changed with real data?"

### M4. Marts for the chosen business questions  ·  todo  ·  mode: review  ·  blocked: PEOPLE (D1) + TECH (M3)
**Goal.** One or two marts that answer D1 (e.g., `fct_sales_by_location_daypart`, `fct_modifier_mix`).
**Acceptance.** Grain and metric definitions in `_marts.yml`; `relationships` tests to dims; daypart boundaries agreed with parents; each mart maps to a stated decision.
**Interview question.** "How did you define daypart, and why those boundaries?"

### M5. Dashboard tab(s)  ·  todo  ·  mode: review  ·  blocked: TECH (M4)
**Goal.** Streamlit view of the marts for parents.
**Acceptance.** Uses the same schema-qualified query pattern; parents can read it without explanation; public screenshots use sandbox or anonymized/aggregated data only (D7).
**Interview question.** "How did you tell whether the dashboard was useful to the actual user?"

### X1. Airflow (local Docker)  ·  NOT COMMITTED  ·  mode: scaffold  ·  blocked: TECH (L2, M1)
**Goal.** Schedule extract, load, dbt build with dependency ordering and retries.
**Gate (design reason required before starting).** Only if the manual chain (L2, M1, dbt) runs reliably by hand and there is a real reason for scheduling (parents want a daily refresh; a token rotation or backfill story). Otherwise a cron/launchd job is the honest answer, and skipping Airflow is a defensible decision to write in Design Decisions.
**Acceptance (if started).** DAG task boundaries justified (one task per idempotent step); a failed task retries without duplicate loads; docker-compose committed with no secrets.
**Interview question.** "Why Airflow here, and what did it buy you over cron?"

---

## 5. Data quality and reconciliation

### Q1. dbt tests and source freshness on Clover models  ·  todo  ·  mode: review  ·  blocked: TECH (M2, M3)
**Acceptance.** Source freshness on `RAW_DATA.CLOVER` (with a threshold justified by extract cadence); uniqueness after dedup; `relationships` line item to order; accepted-values on state; a singular test that no order has negative totals unless a refund explains it. Severities justified (warn vs. error), as with the earlier signup check.
**Interview question.** "Which tests did you make errors vs. warnings, and why?"

### Q2. Reconcile to Clover's own reports  ·  todo  ·  mode: review  ·  blocked: PEOPLE (dashboard read access, A2) + TECH (M4)
**Goal.** Match our daily net sales per location to what Clover's dashboard reports for the same days.
**Acceptance.** Table in `docs/private/` for N sample days per location with our number, Clover's number, difference, and explanation of any difference (tax, tips, refunds timing, voids, delivery not in Clover). Unexplained difference above a stated tolerance blocks calling the model "reconciled". Public README says only "reconciled to the POS dashboard within X" if true, no dollar figures without approval.
**Interview question.** "How did you know your numbers were right?"

### Q3. Hop-by-hop counts and amounts  ·  todo  ·  mode: review  ·  blocked: TECH (M1, M3)
**Goal.** Row and amount conservation S3 to raw to staging to marts.
**Acceptance.** A script or dbt analysis that reports object/row counts and summed cents at each hop for a date range, and explains each expected drop (dedup, allowlist). Runs against fixtures in CI-style test.
**Interview question.** "Where can rows get lost in this pipeline, and how would you notice?"

### Q4. Late-update and lookback test  ·  todo  ·  mode: review  ·  blocked: TECH (L2, M3)
**Acceptance.** Fixture-driven test: an order edited or refunded after first extraction ends up reflected once, with the later value, in the mart.
**Interview question.** "What if a refund shows up three days after the sale?"

### Q5. PII/card leakage check  ·  todo  ·  mode: scaffold (small)  ·  blocked: TECH (L2)
**Goal.** Prove the allowlist works on real-shaped data.
**Acceptance.** Test that scans landed ndjson for forbidden field names and patterns (card last4, names, phone, email, employee fields) and fails the run if found; run on sandbox fixtures and on the first real extract before anything is loaded to Snowflake.
**Interview question.** "How do you make sure sensitive data never enters the warehouse?"

---

## 6. Docs and interview prep

### Doc1. README Phase 7 section  ·  todo  ·  mode: review  ·  blocked: TECH (as milestones land)
**Acceptance.** Architecture diagram updated (S3 and stage added), Design Decisions entries for D2, D3, D5, D6, the access-path choice, and (if skipped) Airflow; setup steps still runnable from a clean clone using sandbox fixtures; `WIP:` prefix on in-progress commits; no real figures.
**Interview question.** "Give me the 2-minute architecture walkthrough."

### Doc2. Resume and LinkedIn wording  ·  todo  ·  blocked: TECH (facts settled)
**Acceptance.** Each bullet has a sentence in this file naming the evidence (repo path, test, or doc). AWS phrased as "personal project, familiarity-level S3/IAM". No claims about production, scale, or Clover partnership. Cross-checked against the "false analogs" rule (e.g., don't equate Cosmos/SCOPE experience with S3/Airflow).

### Doc3. Interview prep sheet  ·  todo  ·  mode: Claude asks cold, Youngli answers
**Acceptance.** One page in `docs/` listing the interview questions above, with Youngli's own two-sentence answers, and a list of "things I'd do differently" and "things I didn't build and why" (Airflow, streaming, incremental dbt).

### Doc4. NOTES.md upkeep  ·  ongoing
**Acceptance.** Each fact used in code has a NOTES.md line; no pasted doc text; snapshots stay local.

---

## Top 3 risks and fallbacks

| # | Risk | Likelihood / impact | Early signal | Fallback |
|---|---|---|---|---|
| 1 | **Production API access isn't obtainable.** Docs say even a private app needs Clover approval; the account may be through a reseller; no dashboard login may exist. | High / high | A2 answers (reseller? login?), or approval stalls after A3. | Dashboard CSV exports of order line items, landed in the same S3 layout (D6), with the extractor replaced by a CSV validator/loader. Everything from L1 down is unchanged. Also check the Export API. The sandbox spike still stands as evidence for OAuth work, but resume wording says "sandbox" for anything I couldn't run in production. |
| 2 | **The data can't answer a question my parents would act on.** Delivery orders missing, no customer identity, messy item/modifier naming, or too little history. | Medium / high | D1/D4 conversations; first real extract shows few modifiers or big channel gaps. | Narrow to item/modifier mix and location comparison (they need only orders and line items). Set the kill condition in D1 in advance: if no question survives, ship the pipeline with sandbox data and aggregate-only real results, and say so plainly in the README rather than stretching claims. |
| 3 | **Privacy or publication problem.** Real financials or identifying data leak into the public repo, or parents decline public results. Related: the spike swamps time (REST seeding, dashboard quirks). | Medium / high | Any real file appearing in `git status`; parents hesitate in A4. | Allowlist at extraction (Q5), gitignored `data/private/` and `docs/private/`, leak grep in D7. If parents decline: public repo shows pipeline + sandbox fixtures only, real analysis stays in a private note I can describe verbally. For the spike-time risk: 3-session timebox in A1, then hand-build fixtures labeled as such. |

Runners-up to watch, not top 3: dbt 2.0.0a2 alpha gaps for `FLATTEN`/sources (fallback: pin known-good behavior and write the workaround in NOTES.md); refresh-token rotation bug losing access (L3); AWS/Snowflake cost drift (set a budget alert at L1).

## Open questions — resolved 2026-09-23

1. **AWS account:** Youngli's own personal account, a new bucket, separate from the Framer site's bucket. Confirms L1/L3/D6 as written.
2. **Repo visibility:** stays public, as assumed. `docs/` stays public except the gitignored `docs/clover/snapshots/` and `docs/private/`.
3. **Spike scripts:** commit `scripts/spike/` — the exploration code is an artifact worth keeping, separate from the JSON it produces (`tests/fixtures/clover/`).

## Decisions log

- 2026-09-21: Backlog created.
- 2026-09-23: Confirmed AWS account, repo visibility, and spike-script commit policy (see resolved open questions above).
- 2026-09-23: **Production access path defaults to CSV export, not the OAuth API.** Youngli read the private-apps approval page directly: submitting for approval is a real step with no stated turnaround, and it requires the *production* Developer Dashboard — a separate thing from sandbox, so it doesn't block A1 or the client scaffold at all. Since approval timing is outside our control, the loader is designed CSV-first (A3 fallback is now the default path), with the OAuth API as a swap-in once/if approval comes through. App submission (A3) starts in parallel once basic app details are ready, rather than waiting on the spike to finish. This changes A3, D2, and L2 below (updated inline).
