# Microsoft Fabric facts

Same discipline as `docs/clover/NOTES.md`: one line per fact, own words, a
source URL, a date, and a tag. Tags here: `docs` = read on
learn.microsoft.com, not yet exercised. `trial-verified` = observed
first-hand once the trial capacity exists. A claim with neither tag, or
sourced only to a blog/forum, is not yet load-bearing — treat it as a
lead, not a fact.

## Trial capacity
| Claim | Source | Date | Tag |
|---|---|---|---|
| Trial capacity runs 60 days, sized as F64-equivalent (or F4, upgradable to F64), free, one per capacity admin | learn.microsoft.com/fabric/fundamentals/fabric-trial | 2026-09-24 | docs |
| Trial supports all non-Power-BI Fabric items (Lakehouse, Warehouse, Notebooks, Pipelines) and up to 1 TB in OneLake — no capability gap for this project | same | 2026-09-24 | docs |
| Trial does NOT support Copilot, AI experiences (Data agent, AI functions), or Private Link — none of these are needed here | same | 2026-09-24 | docs |
| Docs explicitly say trial capacity is "for evaluation and testing only," not production — matches the "never call it production" framing already decided | same | 2026-09-24 | docs |
| On expiry: 7-day grace period to reassign a workspace to a paid capacity before non-Power-BI items become inactive/deletable | same | 2026-09-24 | docs |
| Whether a brand-new tenant can be blocked from starting a trial for 60–90 days (community-reported) is NOT addressed anywhere in the official trial or FAQ pages | same | 2026-09-24 | docs (absence noted) |

## Capacity SKUs / cost
| Claim | Source | Date | Tag |
|---|---|---|---|
| F SKUs (Azure) bill per-second, no commitment, can be paused; F2 = 2 capacity units, the smallest paid tier | learn.microsoft.com/fabric/enterprise/licenses | 2026-09-24 | docs |
| The general licensing page does not list any Fabric engineering item (Lakehouse/Warehouse/Notebook/Pipeline) as requiring more than the smallest F SKU — size constraints found so far are specific to Apache Airflow custom pools (see below), not general engineering items | same | 2026-09-24 | docs |
| Viewing Power BI content with only a Free license needs an F64+ (or Trial) capacity; smaller F SKUs need a Pro/PPU/individual-trial viewer license — irrelevant here since parents will view via a shared workspace/viewer role the account owner (me) sets up, but worth remembering if report sharing breaks after the trial ends and drops to F2 | same | 2026-09-24 | docs |

## Apache Airflow in Fabric (X1, still not committed)
| Claim | Source | Date | Tag |
|---|---|---|---|
| Airflow jobs need a genuine F-SKU or Trial capacity (not the default shared capacity) | learn.microsoft.com/fabric/data-factory/apache-airflow-jobs-concepts (via search) | 2026-09-24 | docs |
| Starter pools (dev/test): ~2 vCPU/8GB nodes, auto-shutdown after ~20 min idle, no idle cost, ~3-5 min cold start. No minimum capacity SKU is stated for starter pools — sizing guidance in the docs applies only to custom pools | learn.microsoft.com/fabric/data-factory/apache-airflow-compute | 2026-09-24 | docs |
| Custom pools (always-on/production-style): small pool needs F8 minimum (5 CU base), large pool needs F16 minimum (10 CU base) | same | 2026-09-24 | docs |
| Reading through: a starter pool is very plausibly usable on Trial capacity for dev/test orchestration; a custom pool is not something this project should need or claim (would read as "production," contradicting the framing decision) | same (my inference, not stated outright) | 2026-09-24 | docs (inference — verify with a real starter-pool run once trial exists) |

## DP-700 exam
| Claim | Source | Date | Tag |
|---|---|---|---|
| Passing score is 700 (confirmed, matches Youngli's prior info); 100 minutes; three domains each weighted 30-35%: Implement and manage an analytics solution / Ingest and transform data / Monitor and optimize an analytics solution | learn.microsoft.com/credentials/certifications/resources/study-guides/dp-700 | 2026-09-24 | docs |
| The fetched skills outline is already the version effective **October 19, 2026** — i.e. it's the *upcoming* one, not the one that will soon be replaced | same | 2026-09-24 | docs |
| Per the page's own change log, the Oct 19, 2026 update is Minor in exactly two places: "Configure Microsoft Fabric workspace settings" and "Optimize performance." Nothing else changed. Building against this outline now is safe either side of that date | same | 2026-09-24 | docs |
| Domain 2 ("Ingest and transform data") explicitly lists: full/incremental loads, handling duplicate/missing/late-arriving data, choosing between Dataflow Gen2/notebook/KQL/T-SQL for transforms, denormalizing, grouping/aggregating — this maps almost one-to-one onto the existing int_orders_deduped / int_order_items_priced problem set, just on a new platform | same | 2026-09-24 | docs |
| Domain 1 explicitly lists "Choose between Dataflow gen 2, a pipeline and a notebook" and "Implement orchestration patterns with notebooks and pipelines" as measured skills — whatever orchestration choice Phase 7 makes should be able to articulate this tradeoff, not just default to one tool | same | 2026-09-24 | docs |

## Open (needs a real trial/tenant to settle, not resolvable from docs)
- Whether Youngli's specific tenant/new Entra ID gets blocked from starting a trial for 60-90 days. No official page confirms or denies this; the only way to know is to try (already the plan).
- Whether a starter-pool Airflow job actually runs on Trial capacity in practice (docs imply yes by omission of a minimum, not by saying so directly).
- Exact exam price in Youngli's proctoring region (docs say "price based on region," don't state a figure).
