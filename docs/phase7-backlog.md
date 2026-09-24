# Phase 7 backlog: real Clover data + Microsoft Fabric

Branch: `phase-7-clover` (kept — this is still "the Clover integration phase," the destination stack changed, not the data source). Last updated: 2026-09-24.

**2026-09-24: pivoted from AWS to Microsoft Fabric.** AWS/S3 is dropped from Phase 7 entirely — see the decisions log. Everything under the old "Landing zone (S3/IAM)" milestone is replaced. The Clover-side work (A1's sandbox spike, the `ingestion/clover/` client scaffold already committed) is unaffected: it was written destination-agnostic (confirmed by grep — no AWS/S3/boto references anywhere in `ingestion/`, `scripts/`, or `tests/`), so nothing there needs to change.

**Legend.** Mode: `scaffold` (Claude builds the skeleton, Youngli fills in the core and owns it, Claude then reviews) or `review` (Youngli writes, Claude reviews). Blocked: `PEOPLE` (needs a human outside this repo) or `TECH` (needs another item first). Status: `todo`, `doing`, `done`, `blocked`.

**Ground rules that apply to every item** (from CLAUDE.md): read-only scopes only (orders, inventory); no card data, customer PII or wages; no secrets in git; real exports never in the public repo; sandbox JSON is fake and may be committed; every Clover fact goes in `docs/clover/NOTES.md`, every Fabric fact goes in `docs/fabric/NOTES.md`, both tagged `docs`/`sandbox-verified`/`trial-verified`. Fabric is described as a personal-project build; production Azure data engineering experience (Synapse/ADF/ADLS Gen2/T-SQL/SSAS/Key Vault/Bicep) is real and separate — never blur the two. A Fabric trial capacity is, per Microsoft's own docs, "for evaluation and testing only" — never call any of this "production."

## Milestone order and critical path

```
A1 sandbox spike ──┬─► D2 raw contract ─► L2 notebook ingest ─► M1 bronze ─► M2 silver (PySpark) ─► M3 gold (dbt-fabric) ─► M4 Direct Lake ─► M5 Power BI ─► Q* ─► Doc*
A2 rep email ──► A3 access path ──┘                                              ▲
F1 Fabric trial access ──► L1 workspace/Lakehouse ──────────────────────────────┘
D1 business questions (PEOPLE) ───────────────────────────────────────────────────┘ (shapes M3/M4, not landing)
```

A1, A2, and F1 all start immediately and in parallel — none depends on the others or on people outside this repo (F1 needs only Youngli's own Microsoft account). Everything downstream of A3/F1 can be built and tested against sandbox fixtures and a trial capacity, so a slow rep or a blocked trial signup stalls *real* data, not the build.

---

## 0. Fabric access (new milestone — this blocks the whole platform side)

### F1. Start the Fabric trial  ·  status: doing (Youngli starting now)  ·  mode: n/a (account action)  ·  blocked: none
**Goal.** Get a usable Fabric capacity before anything else on the platform side can be built.
**What's confirmed (docs/fabric/NOTES.md).** Trial capacity = 60 days, F64-equivalent, up to 1TB in OneLake, supports every non-Power-BI item this project needs (Lakehouse, Warehouse, Notebooks, Pipelines, Direct Lake). Only Copilot, AI Experiences, and Private Link are unavailable — none needed here. Docs call it "evaluation and testing only," which is a gift for the resume framing, not a constraint that blocks this project.
**What's unconfirmed and can only be settled by trying.** Whether a brand-new tenant gets blocked from starting a trial for 60-90 days (community-reported, not in any official doc). This is exactly the "docs are silent, so test it" situation CLAUDE.md's Clover section describes — the test here is just: try to start the trial.
**Acceptance.** Either a working trial capacity exists, or, if blocked, one of the two fallbacks is active within a week of finding out: (a) pay-as-you-go F2 capacity, paused when idle (bills per-second, no commitment, per docs/fabric/NOTES.md), or (b) an existing Microsoft 365 tenant with Fabric-eligible licensing, if one exists. Whichever path is used, record it and the reason in the decisions log below.
**Depends.** Nothing. Start today.
**Interview question.** "How did you get access to the platform you built this on?" — same shape as the Clover access story, worth telling together: two different vendors, two different gatekeeping models.

---

## 1. Access and discovery (Clover side — unchanged by the platform pivot)

### A1. SANDBOX SPIKE  ·  status: todo  ·  mode: scaffold  ·  blocked: none
Unchanged from the original backlog — see the committed scaffold (`ingestion/clover/`, `scripts/spike/`, `tests/fixtures/clover/`) and `docs/clover/NOTES.md`. This work is destination-agnostic: whatever lands from Clover (JSON via API, or CSV via export) gets written to Fabric Lakehouse Files instead of S3, but the client, auth, pagination, and fixture work don't change.

**Goal.** Learn Clover's real API behavior (pagination, timestamps, refunds, tokens) in the sandbox, and leave behind a tested client skeleton, fixtures and a complete NOTES.md.
**Status:** scaffold committed (`ac5c658`/`e96f7db`). Steps 2-7 (account, seeding, live pulls, error triggers, full OAuth cycle, fixture replacement) are still todo — Youngli's hands-on work, per the original scope.
**Acceptance criteria.** Unchanged: every claim about pagination, timestamps, refunds and token behavior in NOTES.md is tagged `docs` or `sandbox-verified`; fixtures exist for the listed scenarios; a pytest loader-style test (already passing — 6/6, `pytest -q`) reads them without network.
**Interview question.** "Walk me through how you handled OAuth token expiry and refresh." / "How did you find out what the API actually does, versus what the docs say?"

### A2. Rep logistics request  ·  todo  ·  blocked: PEOPLE (dad, rep)
Unchanged. Draft saved in `docs/private/`, asks only about account existence, merchant IDs, reseller-vs-direct, and read-only dashboard access.

### A3. Production access path — CSV export is the default, OAuth API is a swap-in  ·  status: doing  ·  blocked: PEOPLE (A2, sample export, or approval)
Unchanged decision from 2026-09-23: CSV export is the default path; the private-app OAuth path is a swap-in if/when Clover approves it (no stated turnaround, needs the production Developer Dashboard). **What changes with the platform pivot:** "the S3 landing contract stays the same regardless" becomes "the Lakehouse Files (bronze) landing contract stays the same regardless" — see D2/D6 below. Nothing else about this decision changes.

### A4. Parents' buy-in and data-sharing terms  ·  todo  ·  blocked: PEOPLE (parents)
Unchanged.

---

## 2. Design decisions

### D1. Business questions  ·  todo  ·  blocked: PEOPLE (parents)  ·  STILL THE OPEN QUESTION
Unchanged, and still the thing that determines whether this extension is worth doing at all. Candidates: sales by daypart vs. staffing, item and modifier mix, location comparison. Each chosen question needs: the decision it informs, the metric, the grain, the minimum data needed, and a kill condition.

### D2. Raw contract and grain  ·  todo  ·  blocked: TECH (A1, A3)
**Goal.** Decide what lands in the Lakehouse's bronze layer (Files section, not yet a Delta table), given the source is CSV-first (A3) with OAuth JSON as a parallel/future source into the *same* contract.
**Recommendation to pressure-test.** Land each source's raw shape unmodified except for a field allowlist that drops PII/card fields at the point of landing (CSV: drop columns; JSON: drop keys). A `source_system` column (`clover_csv` / `clover_api`) carried through so the silver notebook (M2) doesn't need to know which one produced a row. Reason: reprocessing is possible without re-exporting or re-calling the API, and flattening/normalizing happens once, in the PySpark notebook, not scattered across ingestion and transform.
**Acceptance.** Written contract in `models/staging/_sources.md` style (or a new `docs/clover/` equivalent, since this data no longer flows through dbt staging models): grain and PK for each raw entity (orders, line_items, modifiers, discounts, payments-summary), what "one row" means after updates, the field allowlist per source, and the `source_system` tag.
**Interview question.** "How did you design for two source formats without duplicating the pipeline?"

### D3. Customer identity and coexistence with the synthetic model  ·  todo  ·  blocked: TECH (A1, A3)
**Goal.** Decide how a no-customer-identity source fits alongside the existing Phases 1-6 model, now that the two live on genuinely different platforms (Snowflake vs. Fabric), not just different schemas in the same warehouse.
**Recommendation.** Phases 1-6 (Snowflake/dbt/Streamlit, synthetic data) stay exactly as they are — untouched, still the "phases 1-6, done" resume line. Phase 7 is a fully separate pipeline in the same repo (new top-level areas: `ingestion/clover/`, a Fabric-facing dbt project or profile, notebooks), not a migration and not a merge. `dim_customers` and `fct_price_ratio_by_tier` have no Clover analog and are not ported — say that plainly rather than fake a customer table.
**Acceptance.** Decision written; repo layout for the two pipelines documented (README gets a "two pipelines, two platforms, on purpose" note); list of which Phase 1-6 concepts (dedup pattern, transaction-time pricing) carry over as *ideas* even though the code doesn't.
**Interview question.** "You have two working pipelines on two platforms in one repo. Why not migrate the first one?"

### D4. Delivery and third-party channel coverage  ·  todo  ·  blocked: PEOPLE (parents)
Unchanged.

### D5. Time and money conventions  ·  todo  ·  blocked: TECH (A1)
Unchanged: fix the order-date field, timezone (Eastern, DST-aware), money as integer cents converted once, and which business day a post-midnight order belongs to.

### D6. Lakehouse layout and load semantics (was: S3 layout)  ·  todo  ·  blocked: TECH (A1)
**Goal.** Pick the medallion layout and write semantics inside the Lakehouse.
**Recommendation.**
- **Bronze** (Lakehouse **Files**, not a table): `Files/clover/<merchant_id>/<resource>/extract_date=YYYY-MM-DD/<run_id>.ndjson` (or `.csv`) — same idempotency idea as the old S3 plan: immutable objects, a retried run overwrites the same `run_id` key.
- **Silver** (Lakehouse **Tables**, managed Delta): one Delta table per cleaned entity (`orders`, `line_items`, `modifiers`, `discounts`), written by the PySpark notebook (M2), keyed and deduped there.
- **Gold**: Fabric **Warehouse** tables, written by dbt-fabric (M3) reading the silver Delta tables via OneLake (a Warehouse can query Lakehouse tables directly — verify the exact cross-item query mechanism, e.g. a shortcut vs. a three-part name, against current Fabric docs before building M3, and record it in `docs/fabric/NOTES.md`).
**Acceptance.** Layout, naming, and idempotency rule written; explanation of what makes a re-run safe at each layer (bronze: same run_id, same key; silver: notebook overwrite/merge semantics stated explicitly, not assumed).
**Interview question.** "Walk me through your medallion layers and what 'safe to re-run' means at each one."

### D7. Publication policy  ·  todo  ·  blocked: PEOPLE (A4)
Unchanged, plus: the resume/README framing constraint that Fabric is personal-project work while Synapse/ADF/ADLS/Key Vault is real production experience — never blur the two (see Doc2).

---

## 3. Landing and Lakehouse setup (Fabric) — replaces "Landing zone (S3/IAM)"

### L1. Fabric workspace + Lakehouse  ·  todo  ·  mode: scaffold  ·  blocked: TECH (F1) + TECH (D6)
**Goal.** A dedicated workspace on the trial (or fallback) capacity, with a Lakehouse item, and an access model that doesn't run everything under Youngli's personal login.
**What's different from the AWS plan.** Fabric's access model is workspace roles (Admin/Member/Contributor/Viewer) plus, for anything automated, a Microsoft Entra **service principal** — not IAM policy JSON. This is the Fabric-native version of "least privilege": the notebook/pipeline that writes bronze data runs as a service principal scoped to Contributor on this one workspace, not as Youngli's own admin identity.
**Scaffold delivers (complete, boilerplate).** A short script/checklist for workspace creation, capacity assignment, Lakehouse creation, and registering an Entra app for the service principal; a README of what each piece is for and why it isn't just "run everything as me." **Youngli fills in:** the actual workspace/capacity names, and the reasoning write-up for defending the service-principal choice.
**Acceptance.**
- Workspace exists on a Fabric-enabled capacity (trial or fallback), with a Lakehouse item.
- A service principal (not a personal account) is what the notebook/pipeline authenticates as for automated runs; verified by checking the run's identity in the workspace's access/activity log.
- No credentials in the repo; the service principal's secret lives in Key Vault (L3) from day one, not as a placeholder to fix later.
**Interview question.** "How did you avoid running an automated job under your own admin login?"

### L2. Notebook ingestion — CSV path first, API notebook as a swap-in  ·  todo  ·  mode: scaffold  ·  blocked: TECH (D2, D6, L1) + PEOPLE (A3: sample export, or approval)
**Goal.** Land Clover order data into Lakehouse Files (bronze) under the shared raw contract (D2), from whichever source is live. Same "two producers, one landing shape" idea as before, just running inside a Fabric notebook instead of a script writing to S3:
- **CSV path (default, built first).** A notebook cell/module reads a manually-placed dashboard export (from `data/private/`, gitignored, uploaded to the Lakehouse's Files area by hand for now), validates columns against the D2 contract, tags rows `source_system='clover_csv'`, writes to bronze. No watermark/incremental logic needed while exports are pulled by hand — say that limitation outright.
- **API path (swap-in once A3 lands).** The `ingestion/clover/` client (already scaffolded, unchanged) runs *inside* a Fabric notebook instead of a local script, calling `client.paginate(...)` and writing results to the same bronze path, tagged `source_system='clover_api'`.
**Scaffold delivers.** Notebook skeleton (or a `.py` module imported by the notebook, so it stays testable outside the Fabric runtime) with a `write_bronze(records, source_system, merchant_id, extract_date, run_id)` function signature, CSV column-validation stub, and wiring to call the existing `CloverClient` for the API path. **Youngli writes:** CSV column validation and error handling; the incremental watermark for the API path (storage — likely a small Delta "watermark" table rather than a file, since the notebook already has Delta available); pagination/watermark interplay; idempotent bronze key naming.
**Acceptance.**
- CSV path runs end to end on a sample export (fake/sandbox-shaped until a real one exists) and writes correctly-tagged bronze files.
- Re-running the same input produces the same bronze keys, no duplicate data downstream.
- API path (once exercised): a record modified after the watermark is picked up on the next run; watermark advances only after a successful write; two merchant IDs don't share a watermark.
- Allowlist test on both paths: a fixture with PII fields lands with those fields removed.
**Interview question.** "How did you keep a CSV path and an API path from becoming two pipelines?" (unchanged from the AWS-era phrasing — the answer's the same, the target storage changed.)

### L3. Credential storage — Azure Key Vault  ·  todo  ·  mode: scaffold  ·  blocked: TECH (L1)
**Goal.** Store the Clover OAuth refresh token and the Fabric service principal's secret somewhere real, not a local file.
**Why this is different from the old plan.** The AWS-era version of this item asked "is a chmod-600 file enough, or is Secrets Manager overkill for a laptop job?" With Fabric, the honest answer flips: Key Vault is already how the notebook authenticates its service principal (L1), so a second secret (the Clover refresh token) belongs in the same Key Vault, not a separate mechanism — using Key Vault here isn't keyword padding, it's the same tool already load-bearing for L1. This is also **real prior experience**, not a new tool — say so directly in the interview, unlike the trial-only Fabric pieces.
**Scaffold delivers.** Key Vault creation script/checklist, notebook snippet for reading a secret via the Fabric-to-Key-Vault connection, and a README on the access-policy scoping. **Youngli writes:** the token rotation logic (same TODO as `ingestion/clover/auth.py`/`token_store.py` already scaffolded — reading/writing to Key Vault instead of a local JSON file is a different `TokenStore` backend, not a rewrite of the rotation logic itself).
**Acceptance.** No secret in the repo or in notebook output; a concurrent-run test shows two runs can't clobber each other's rotated token (same requirement as before, different backend).
**Interview question.** "You'd used Key Vault in production before — what was different about wiring it into a personal Fabric project?"

### L4. *(retired)* Snowflake storage integration + external stage
Dropped — Snowflake isn't part of the Phase 7 pipeline anymore (D3). See the decisions log.

---

## 4. Load and modeling (hybrid: PySpark notebook for bronze→silver, dbt-fabric for silver→gold)

**Why hybrid, not one tool end to end** (decision made 2026-09-24, see decisions log): flattening nested Clover JSON and handling per-order dedup/refund logic is unstructured-data wrangling — Spark's actual strength, and exactly what the existing Databricks/PySpark notebook from Phase 6 already demonstrates. Aggregating clean, structured Delta tables into tested, documented marts is exactly what the existing dbt project already does well, on Snowflake, and `dbt-fabric` lets that same skill and the same test/doc pattern (`_marts.yml`, `relationships` tests) carry over to a new adapter. Each tool sits where it's the honest fit; this is also a stronger interview story than either pure option ("why did you use both" has a real answer) and hits more of DP-700's Domain 1 ("choose between Dataflow Gen2, a pipeline, and a notebook") directly.

### M1. Notebook ingestion → bronze  ·  todo  ·  mode: scaffold  ·  blocked: TECH (L2)
Covered by L2 above — kept as a separate acceptance checkpoint because "data lands in bronze, idempotently" is worth verifying on its own before silver logic depends on it.
**Acceptance.** A scheduled (or manually triggered) notebook run produces bronze files for a given `extract_date`; running it again for the same date changes nothing downstream.

### M2. PySpark notebook: bronze → silver  ·  todo  ·  mode: review  ·  blocked: TECH (M1, A1 refund findings)
**Goal.** One notebook (or a small set of them) that reads bronze, and writes clean silver Delta tables: flattened orders/line items/modifiers/discounts, one current row per order (replacing the old "byte-identical duplicates" assumption — a real order can legitimately arrive multiple times as it's modified, so this keeps the latest `modifiedTime`, not an arbitrary pick), and refund/void handling that nets out revenue correctly.
**Why review, not scaffold.** PySpark notebook work is closer to what Phase 6's Databricks notebook already was — Youngli-written, Claude-reviewed — than to a brand-new tool. A short conceptual walkthrough is still owed before any unfamiliar pattern (e.g., `MERGE INTO` for idempotent Delta writes, if that's the chosen upsert method — confirm this is genuinely new to Youngli before treating it as a walkthrough item, since Synapse/ADF work may already cover Delta MERGE).
**Acceptance.** Grain and PK documented per silver table; a fixture-driven test (using `tests/fixtures/clover/`) proves the latest-version-per-order logic and refund netting; revenue reconciles to the sum of fixture line items minus discounts minus refunds.
**Interview question.** "Your synthetic dedup assumed identical duplicates. What changed with real data, and why does that belong in Spark instead of SQL?"

### M3. dbt-fabric: silver → gold marts  ·  todo  ·  mode: scaffold (adapter/profile setup) then review (the SQL itself)  ·  blocked: TECH (M2) + PEOPLE (D1)
**Goal.** dbt project (new, or a new target on the existing one — decide based on how much genuinely carries over) targeting Fabric Warehouse, building the marts that answer D1's chosen question(s).
**Scaffold delivers (NEW TOOLS: dbt-fabric is a new adapter).** `profiles.yml`-equivalent target for Fabric Warehouse (service-principal auth via Key Vault, not a password), a minimal `dbt debug`-passing connection, and a short README on what differs from the Snowflake adapter (auth model, and whatever SQL dialect gaps surface — Fabric Warehouse is T-SQL-based, so functions like `LATERAL FLATTEN` don't exist; that's moot here since flattening already happened in M2, which is itself part of the design reason for the hybrid split). **Youngli writes:** the actual staging/mart SQL, same as always.
**Acceptance.** `dbt debug` and `dbt build` succeed against the Fabric Warehouse from a clean checkout using a documented setup; `_marts.yml`-style tests (`relationships`, `not_null`, `unique`) pass; each mart traces to a D1 question.
**Interview question.** "What actually changed moving a dbt project from Snowflake to Fabric — auth, SQL dialect, or something else?" Also: "Where's the boundary between what Spark does and what dbt does in this pipeline, and why there?"

### M4. Direct Lake semantic model  ·  todo  ·  mode: scaffold  ·  blocked: TECH (M3)
**Goal.** A Direct Lake semantic model over the gold tables — no import, no scheduled refresh into a separate model.
**Needs a docs check before building (per the same "settle it, don't guess" discipline as Clover/Fabric-access facts):** whether Direct Lake reads Warehouse-native tables directly or needs them exposed to the Lakehouse via a shortcut, and the conditions under which Direct Lake silently falls back to DirectQuery (this matters for an honest "yes, it's actually in Direct Lake mode" claim, not just a semantic model that happens to point at Fabric). Record the answer in `docs/fabric/NOTES.md` before it goes in the README.
**Acceptance.** Semantic model built on gold tables; a check (Fabric's own monitoring, per DP-700's "monitor semantic model refresh" skill) confirms it's actually operating in Direct Lake mode, not a silent DirectQuery fallback.
**Interview question.** "What is Direct Lake actually doing differently from import mode, and how did you confirm it was working as intended rather than assume it?"

### M5. Power BI report for parents  ·  todo  ·  mode: review  ·  blocked: TECH (M4)
**Goal.** The report parents actually look at, built on the Direct Lake model.
**Why review, not scaffold.** Power BI is explicitly part of Youngli's real production background — this is the one Phase 7 tool that's genuinely familiar territory, not new.
**Acceptance.** Answers the D1 question(s) directly; parents can read it without explanation; public screenshots use sandbox or anonymized/aggregated data only (D7).
**Interview question.** "How did you tell whether the report was useful to the actual user?"

### X1. Fabric-managed Apache Airflow  ·  NOT COMMITTED  ·  mode: scaffold  ·  blocked: TECH (L2, M1)
**Goal.** Schedule ingestion, notebook transforms, and dbt build with dependency ordering and retries.
**What's different from the Docker-Airflow plan.** Per `docs/fabric/NOTES.md`: an Airflow **starter pool** (dev/test, auto-shuts down after ~20 min idle, no idle cost) has no stated minimum capacity SKU, so it's plausibly usable on the trial capacity — worth an actual test run before relying on it. A **custom** (always-on) pool needs F8+ and reads as "production," which contradicts the framing decision — so if this is built at all, it's a starter pool, explicitly, and that choice is defensible on its own (matches "dev/test" honestly instead of overclaiming).
**Gate (design reason required before starting).** Same gate as before: only if the manual chain (ingest → notebook → dbt) runs reliably by hand and there's a real scheduling reason. A Fabric Data Pipeline's own scheduler may be the simpler, equally honest answer over Airflow specifically — that comparison is itself worth writing up (and maps directly onto DP-700's "choose between Dataflow Gen2, a pipeline, and a notebook").
**Acceptance (if started).** Verify starter-pool behavior on trial capacity first (record as `trial-verified` in NOTES.md); task boundaries justified; a failed task retries without duplicate loads.
**Interview question.** "Why Airflow-on-Fabric here instead of a native Fabric pipeline schedule, or was it?" (Honest answer might be "it wasn't" — that's fine, say so.)

---

## 5. Data quality and reconciliation

### Q1. Tests and monitoring across bronze/silver/gold  ·  todo  ·  mode: review  ·  blocked: TECH (M2, M3)
**Acceptance.** dbt tests on gold (uniqueness, `relationships`, `accepted_values`, severities justified as warn vs. error); a notebook-level check on silver (latest-per-order uniqueness, no negative totals unless a refund explains it); Fabric's Capacity Metrics app or run history used to notice a failed/skipped ingestion run, not just dbt's own test suite.
**Interview question.** "Which tests did you make errors vs. warnings, and why?"

### Q2. Reconcile to Clover's own reports  ·  todo  ·  mode: review  ·  blocked: PEOPLE (dashboard read access, A2) + TECH (M4/M5)
Unchanged in substance from the original plan — a sample of days per location, our number vs. Clover's dashboard number, differences explained (tax, tips, refund timing, delivery not in Clover), tolerance stated, no dollar figures published without approval.

### Q3. Hop-by-hop counts and amounts  ·  todo  ·  mode: review  ·  blocked: TECH (M1, M2, M3)
**Goal.** Row and amount conservation bronze → silver → gold (was S3 → raw → staging → marts).
**Acceptance.** A notebook cell or dbt analysis reporting counts/summed cents at each hop for a date range, explaining every expected drop (dedup, allowlist).
**Interview question.** "Where can rows get lost in this pipeline, and how would you notice?"

### Q4. Late-update and lookback test  ·  todo  ·  mode: review  ·  blocked: TECH (L2, M2)
Unchanged in substance: a fixture-driven test that an order edited or refunded after first landing ends up reflected once, with the later value, in gold.

### Q5. PII/card leakage check  ·  todo  ·  mode: scaffold (small)  ·  blocked: TECH (L2)
Unchanged in substance: a scanner over landed bronze data for forbidden field names/patterns (mirrors `scripts/spike/scrub_check.py`, already committed for the sandbox fixtures — this is the same idea applied to real landed data, not sandbox fixtures).

---

## 6. Docs and interview prep

### Doc1. README Phase 7 section  ·  todo  ·  mode: review  ·  blocked: TECH (as milestones land)
**Acceptance.** Architecture diagram updated to the Fabric shape (bronze Files → silver Delta via notebook → gold Warehouse via dbt-fabric → Direct Lake → Power BI); Design Decisions entries for D2, D3, D5, D6, the access-path choice, and the hybrid-transform choice with its "why both tools" reasoning; a clear "two pipelines, two platforms, on purpose" note per D3; no real figures.

### Doc2. Resume and LinkedIn wording  ·  todo  ·  blocked: TECH (facts settled)
**Acceptance.** Each bullet cites its evidence (repo path, test, or doc). Fabric phrased as personal-project work; Synapse/ADF/ADLS Gen2/T-SQL/SSAS/Key Vault/Bicep phrased as real production experience — the two claims stay clearly separated, never blurred into one. The Quisitive-letter OneLake/Xbox-scale claim gets either a defended version or a softened one before it's reused anywhere else — track the decision here once made.

### Doc3. Interview prep sheet  ·  todo  ·  mode: Claude asks cold, Youngli answers
Unchanged: one page listing the interview questions above with two-sentence answers, plus "things I'd do differently" and "things I didn't build and why."

### Doc4. NOTES.md upkeep  ·  ongoing
**Acceptance.** Now covers both `docs/clover/NOTES.md` and `docs/fabric/NOTES.md`; no pasted doc text in either; snapshots stay local (`docs/clover/snapshots/`, and a matching `docs/fabric/snapshots/` if full-page copies are ever saved).

### Doc5. DP-700 checklist, tracked against this backlog  ·  NEW  ·  ongoing
**Goal.** Make the exam prep and the build the same activity, as planned.
**Setup (already done in `docs/fabric/NOTES.md`).** The three DP-700 domains, each 30-35%: *Implement and manage an analytics solution* (workspace config, lifecycle/version control, security, orchestration choice) → mapped to L1/L2/X1. *Ingest and transform data* (full/incremental loads, dedup/late-arriving data, choosing Dataflow Gen2 vs. notebook vs. T-SQL, denormalizing/aggregating) → mapped to L2/M2/M3. *Monitor and optimize an analytics solution* (monitoring ingestion/transforms/semantic model refresh, resolving pipeline/notebook/T-SQL errors, optimizing Lakehouse tables/warehouse/Spark) → mapped to Q1/Q3/M4.
**Note.** The fetched outline is already the version effective October 19, 2026; its own change log shows only two "Minor" changes from the prior version, so building against it now is safe regardless of when the build finishes relative to that date.
**Acceptance.** As each milestone above lands, note here which DP-700 sub-skill it exercised — by the time M1-M5 are done, most of Domains 1-2 should already be checked off from real build experience rather than cold study.

---

## Top 3 risks and fallbacks

| # | Risk | Likelihood / impact | Early signal | Fallback |
|---|---|---|---|---|
| 1 | **Production Clover API access isn't obtainable.** Even a private app needs Clover approval with no stated turnaround; the account may be via a reseller; no dashboard login may exist. | High / high | A2 answers, or approval stalls after A3. | Dashboard CSV exports, landed the same way (D6/L2) regardless of source — L2 is already designed source-agnostic. Resume wording says "sandbox" for anything not run in production. |
| 2 | **Fabric trial access is blocked or too constrained.** New-tenant trial blocks are community-reported, not documented; even if granted, 60 days is a hard clock against an open-ended Clover-access timeline (risk 1). | Medium / high | Trial signup fails or is delayed; 60-day clock runs out before Clover access resolves. | Pay-as-you-go F2 capacity, paused when idle — confirmed billable-per-second with no commitment (`docs/fabric/NOTES.md`). Or an existing M365 tenant if one exists. Either way, nothing built against the trial needs to change — same workspace/Lakehouse concepts, just a paid capacity underneath. |
| 3 | **The data can't answer a question my parents would act on.** Delivery orders missing, no customer identity, messy item/modifier naming, or too little history. | Medium / high | D1/D4 conversations; first real extract shows few modifiers or big channel gaps. | Narrow to item/modifier mix and location comparison. Kill condition set in advance in D1: if nothing survives, ship the pipeline with sandbox data and aggregate-only real results, stated plainly. |

Runners-up to watch, not top 3:
- **`dbt-fabric` adapter is newer and less battle-tested than `dbt-snowflake`.** If it hits rough edges the Snowflake adapter never had, the fallback is folding gold-layer logic into the PySpark notebook too (collapsing to the "notebook-native only" option from the transform-layer decision) rather than fighting the adapter indefinitely.
- **Direct Lake fallback-to-DirectQuery behavior**, if not understood before the README is written, risks an inaccurate "Direct Lake" claim (M4).
- Privacy/publication leakage (Q5, D7) — same guard as before, just applied to Lakehouse Files instead of S3 objects.
- AWS/Snowflake cost drift — moot now (dropped); Fabric capacity cost (trial → F2) is the analogous thing to watch, covered under risk 2.

## Decisions log

- 2026-09-21: Backlog created.
- 2026-09-23: Confirmed AWS account choice, repo visibility, spike-script commit policy; confirmed CSV-export-first production access decision.
- 2026-09-24: **Pivoted the platform from AWS to Microsoft Fabric.** Reasoning (Youngli's): production Azure experience (Synapse/ADF/ADLS Gen2/T-SQL/SSAS/Key Vault/Bicep at Microsoft Gaming via TCS) is a real, defensible differentiator; the public Microsoft stack has moved to Fabric, so that experience currently reads as one generation behind (an application-history review found a "Fabric gap" bridged in 7 of 28 cover letters). A portfolio S3 bucket on zero production AWS was judged to be a keyword, not a claim; Fabric on top of real Synapse/ADF experience is one coherent claim. Dagster is off the list. Airflow is possible later (X1) via Fabric-managed Airflow, gated on confirming a starter pool actually runs on trial capacity. dbt/Snowflake (Phases 1-6) stay on the resume as completed work and stay untouched in the repo (D3); dbt is reused for Phase 7 via the `dbt-fabric` adapter, targeting Fabric Warehouse, not Snowflake.
- 2026-09-24: **Transform-layer decision: hybrid.** PySpark notebook handles bronze→silver (flatten, dedupe, refund/void logic — Spark's strength, and matches the Phase 6 Databricks notebook precedent). `dbt-fabric` handles silver→gold marts against Fabric Warehouse (reuses the existing dbt test/doc pattern). Chosen over notebook-only (drops dbt from Phase 7 entirely) and dbt-only (misses a chance to showcase PySpark-on-Fabric and weakens the DP-700 "choose the right transform tool" story). Confirmed by Youngli 2026-09-24.
- 2026-09-24: Verified via docs.microsoft.com (see `docs/fabric/NOTES.md`): trial capacity supports everything this project needs; DP-700's Oct 19, 2026 outline update is minor; a starter-pool Airflow job has no documented minimum capacity SKU, unlike custom pools (F8+).
