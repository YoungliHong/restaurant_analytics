# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Merge test: batch 2 through bronze append + silver MERGE
# MAGIC
# MAGIC Proves the incremental load does what it claims, against a small
# MAGIC hand-made second batch:
# MAGIC 1. **Load batch 2** through `01_bronze` and `02_silver`: new orders are
# MAGIC    inserted, real corrections update, repeats and cosmetic-only
# MAGIC    re-deliveries change nothing, and a key duplicated inside the batch
# MAGIC    lands once.
# MAGIC 2. **Rerun batch 2**: bronze doesn't grow; silver's MERGE inserts 0 and
# MAGIC    updates 0.
# MAGIC 3. **Restore** bronze and silver to their pre-batch-2 versions, delete
# MAGIC    the batch-2 files, rebuild gold, rerun reconcile: still 7/7, because
# MAGIC    Snowflake only has batch 1.
# MAGIC
# MAGIC **Prerequisite:** the batch-1 regression check has passed — bronze reset
# MAGIC and reloaded from the per-table subfolders, and 01 → 02 → 03 → 04
# MAGIC reconciling 7/7 on batch 1 alone.
# MAGIC
# MAGIC **Scaffold boundary:** fixtures, version bookkeeping, the `%run` cells,
# MAGIC restore, and cleanup are written in full. The two expected-outcome
# MAGIC functions (sections 5 and 8) are docstring + TODO stubs.
# MAGIC
# MAGIC **Known gap (documented, not tested):** two rows for the same key
# MAGIC *within one batch* that differ in value tie on both dedup keys
# MAGIC (`_ingested_at`, then `order_timestamp`), so which one wins is
# MAGIC arbitrary. Batch 2's within-batch duplicate (order 5003) is
# MAGIC byte-identical — a retry — so its outcome is deterministic. Resolving a
# MAGIC different-valued within-batch duplicate needs a source-side sequence
# MAGIC number or `updated_at`, which this feed doesn't have.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: tables, version bookkeeping, row lookups

# COMMAND ----------

from pyspark.sql.functions import col

VOLUME_PATH = "/Volumes/workspace/bronze/raw_files"
SOURCE_TABLES = ["restaurants", "menu_items", "customers", "orders", "order_items"]
BRONZE = [f"workspace.bronze.{t}" for t in SOURCE_TABLES]
SILVER = [f"workspace.silver.{t}" for t in SOURCE_TABLES]
ALL_TABLES = BRONZE + SILVER


def table_version(table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def snapshot_state() -> dict:
    """Version and row count of every bronze and silver table, right now."""
    return {t: {"version": table_version(t), "rows": spark.table(t).count()} for t in ALL_TABLES}


def merge_totals(table: str, since_version: int) -> dict:
    """
    Sum inserted / updated over the MERGE commits made after `since_version`.

    Reads exactly the commits in (since_version, current], never just the
    latest history row — if a no-op MERGE writes no commit, "latest" would
    be the previous run's MERGE and report its numbers by mistake.
    committed=False means the version didn't move at all.
    """
    commits = (
        spark.sql(f"DESCRIBE HISTORY {table}")
        .filter(col("version") > since_version)
        .orderBy("version")
        .collect()
    )
    merges = [c for c in commits if c["operation"] == "MERGE"]
    metric = lambda c, k: int((c["operationMetrics"] or {}).get(k, 0))
    return {
        "committed": len(commits) > 0,
        "operations": [c["operation"] for c in commits],
        "merge_commits": len(merges),
        "source_rows": sum(metric(c, "numSourceRows") for c in merges),
        "inserted": sum(metric(c, "numTargetRowsInserted") for c in merges),
        "updated": sum(metric(c, "numTargetRowsUpdated") for c in merges),
        "deleted": sum(metric(c, "numTargetRowsDeleted") for c in merges),
    }


def silver_order(order_id: int):
    """All silver rows for one order_id (a list, so duplicates are visible)."""
    return spark.table("workspace.silver.orders").filter(col("order_id") == order_id).collect()


def silver_item(order_item_id: int):
    """All silver rows for one order_item_id."""
    return spark.table("workspace.silver.order_items").filter(col("order_item_id") == order_item_id).collect()


def print_state(label: str, state: dict) -> None:
    print(f"--- {label} ---")
    for t, s in state.items():
        print(f"{t:<34} v{s['version']:<4} {s['rows']:>7,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Batch 2 fixtures
# MAGIC
# MAGIC Written straight into the table folders under new filenames, so the
# MAGIC data lives (versioned) in this notebook and a rerun of 05 needs no
# MAGIC manual upload. Same header and raw formatting as batch 1 (unquoted
# MAGIC dirty values, prices like `10.8`). Synthetic data, safe to commit.
# MAGIC
# MAGIC | # | order_id | scenario | expected in silver |
# MAGIC |---|---|---|---|
# MAGIC | 1 | 4845 | status completed → **refunded** | updated; lineage → batch 2 |
# MAGIC | 2 | 4936 | status completed → **cancelled** | updated; lineage → batch 2 |
# MAGIC | 3 | 4905 | `" Delivery "` / `CARD` — identical after cleaning | not updated; keeps batch-1 lineage |
# MAGIC | 4 | 4998 | exact repeat (header + line 13081) | not updated |
# MAGIC | 5 | 5001 | new, 2 lines | inserted |
# MAGIC | 6 | 5002 | new guest order, `" TAKEOUT"` / `Mobile Pay` | inserted as takeout / mobile_pay, null customer |
# MAGIC | 7–8 | 5003 | new, header sent twice byte-identically (retry), 2 lines | inserted once, gift_card |
# MAGIC
# MAGIC No line items for 4845 / 4936: a status correction is header-only, and
# MAGIC their batch-1 lines are untouched.

# COMMAND ----------

ORDERS_BATCH2 = """order_id,restaurant_id,customer_id,order_timestamp,order_type,payment_method,status
4845,2,178,2024-12-20 16:05:41,takeout,card,refunded
4936,1,687,2024-12-26 20:19:13,delivery,card,cancelled
4905,1,382,2024-12-24 17:29:07, Delivery ,CARD,completed
4998,4,600,2024-12-31 19:31:33,dine_in,card,completed
5001,1,228,2025-01-02 12:15:00,dine_in,card,completed
5002,3,,2025-01-02 18:40:00, TAKEOUT,Mobile Pay,completed
5003,4,600,2025-01-03 19:05:00,delivery,giftcard,completed
5003,4,600,2025-01-03 19:05:00,delivery,giftcard,completed
"""

ORDER_ITEMS_BATCH2 = """order_item_id,order_id,menu_item_id,quantity,unit_price
13081,4998,27,2,10.8
13089,5001,8,1,16.2
13090,5001,25,2,5.4
13091,5002,27,1,10.8
13092,5003,15,1,18.36
13093,5003,30,1,3.24
"""

BATCH2_FILES = {
    f"{VOLUME_PATH}/orders/orders_batch2.csv": ORDERS_BATCH2,
    f"{VOLUME_PATH}/order_items/order_items_batch2.csv": ORDER_ITEMS_BATCH2,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Record the pre-batch-2 state, then write the files
# MAGIC
# MAGIC State is recorded **before** the files land, so the restore target in
# MAGIC section 9 is guaranteed to be batch 1 only.

# COMMAND ----------

STATE_BEFORE = snapshot_state()
print_state("before batch 2", STATE_BEFORE)

for path, content in BATCH2_FILES.items():
    dbutils.fs.put(path, content, overwrite=True)
    print(f"wrote {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Load batch 2: bronze, then silver
# MAGIC
# MAGIC `%run` executes each notebook in this one's context. 02 runs with
# MAGIC `batch1_checks="false"`, which skips only the 2b byte-identical
# MAGIC pre-check (bronze now holds two different rows for 4845 and 4936 by
# MAGIC design); its section 8 checks are batch-agnostic and run as normal.
# MAGIC Whether `%run` with widget arguments works on serverless isn't stated
# MAGIC in the docs — this cell is the test.

# COMMAND ----------

# MAGIC %run ./01_bronze

# COMMAND ----------

# MAGIC %run ./02_silver $batch1_checks="false"

# COMMAND ----------

STATE_AFTER_BATCH2 = snapshot_state()
print_state("after batch 2", STATE_AFTER_BATCH2)

BATCH2_MERGES = {
    t: merge_totals(t, STATE_BEFORE[t]["version"])
    for t in ["workspace.silver.orders", "workspace.silver.order_items"]
}
for t, m in BATCH2_MERGES.items():
    print(f"{t}: {m}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Batch 2 expected outcome — SCAFFOLD ONLY

# COMMAND ----------

def assert_batch2_outcome(before: dict, after: dict, merges: dict) -> None:
    """
    Assert batch 2 did exactly what the fixture table in section 2 says.
    SCAFFOLD ONLY. `before` / `after` are snapshot_state() dicts; `merges`
    is BATCH2_MERGES (merge_totals per silver fact table).

    Bronze (append, files never double-loaded):
      - orders: rows +8 (5,020 -> 5,028); order_items: rows +6 (13,088 -> 13,094)
      - restaurants / menu_items / customers: version AND rows unchanged
        (no batch-2 dim files -> no write at all)

    Silver MERGE metrics (merges[...]):
      - orders: inserted 3, updated 2
      - order_items: inserted 5, updated 0
      - source_rows: 5,003 / 13,093 — the whole deduped bronze, not just
        batch 2 (silver re-merges all of bronze each run)

    Silver row counts: orders 5,000 -> 5,003; order_items 13,088 -> 13,093.

    Row checks (silver_order / silver_item return lists — check len == 1
    first, which also proves the 5003 within-batch duplicate collapsed):
      - 4845: status 'refunded'; _source_file ends with
        '/orders/orders_batch2.csv'
      - 4936: status 'cancelled'; _source_file ends with '/orders/orders_batch2.csv'
      - 4905: order_type 'delivery', payment_method 'card', and _source_file
        still ends with '/orders/orders.csv' (cosmetic-only -> not updated,
        lineage NOT refreshed)
      - 4998: _source_file still batch 1 (exact repeat -> not updated)
      - 5002: customer_id is None; order_type 'takeout'; payment_method 'mobile_pay'
      - 5003: exactly one row; payment_method 'gift_card'
      - order_item 13081: _source_file still ends with
        '/order_items/order_items.csv' (repeat -> not updated)
      - order_items 13089-13093: one row each; 13090 line_item_revenue
        == Decimal('10.80')
    """
    # TODO: write the assertions.
    print("assert_batch2_outcome: TODO — stub, nothing asserted")


assert_batch2_outcome(STATE_BEFORE, STATE_AFTER_BATCH2, BATCH2_MERGES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Rerun batch 2 (same files, nothing new)

# COMMAND ----------

# MAGIC %run ./01_bronze

# COMMAND ----------

# MAGIC %run ./02_silver $batch1_checks="false"

# COMMAND ----------

STATE_AFTER_RERUN = snapshot_state()
print_state("after rerun", STATE_AFTER_RERUN)

RERUN_MERGES = {
    t: merge_totals(t, STATE_AFTER_BATCH2[t]["version"])
    for t in ["workspace.silver.orders", "workspace.silver.order_items"]
}
for t, m in RERUN_MERGES.items():
    print(f"{t}: {m}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. What the rerun settles
# MAGIC
# MAGIC Two behaviors the docs don't settle, observed in the output above:
# MAGIC - Does a MERGE that changes nothing write a commit? (`committed`)
# MAGIC - Does a matched row whose update condition is false count in
# MAGIC   `numTargetRowsUpdated`? (`updated` should be 0 if not)
# MAGIC
# MAGIC Record what you see in the README's Design Decisions when you write it up.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Rerun expected outcome — SCAFFOLD ONLY

# COMMAND ----------

def assert_rerun_is_noop(after_batch2: dict, after_rerun: dict, merges: dict) -> None:
    """
    Assert the rerun changed nothing. SCAFFOLD ONLY. `merges` is RERUN_MERGES.

    Bronze: for all five tables, version AND row count identical between
    after_batch2 and after_rerun (01 found no new files, so it didn't write).

    Silver orders / order_items:
      - inserted == 0 and updated == 0 — whether or not a commit was
        written (merges[t]["committed"] may be either; both are correct).
      - row counts identical to after_batch2.

    Silver dims are NOT part of this check: they overwrite every run, so
    their version moves by design even though their content doesn't.
    """
    # TODO: write the assertions.
    print("assert_rerun_is_noop: TODO — stub, nothing asserted")


assert_rerun_is_noop(STATE_AFTER_BATCH2, STATE_AFTER_RERUN, RERUN_MERGES)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Restore to batch 1, remove the batch-2 files
# MAGIC
# MAGIC `RESTORE TABLE ... TO VERSION AS OF` each table whose version moved.
# MAGIC Restore is itself a new commit (history is kept, not rewound). Because
# MAGIC bronze's "already loaded" state *is* its `_source_file` column, the
# MAGIC restore also forgets that batch 2 was loaded — which is why the batch-2
# MAGIC files are deleted right after: otherwise the next plain run of 01 would
# MAGIC load them again. (A rerun of 05 rewrites them in section 3 anyway.)
# MAGIC
# MAGIC Silver dims are restored too: their content didn't change, but
# MAGIC restoring every moved table keeps "pre-batch-2 state" literal.

# COMMAND ----------

for t in ALL_TABLES:
    target_version = STATE_BEFORE[t]["version"]
    if table_version(t) != target_version:
        spark.sql(f"RESTORE TABLE {t} TO VERSION AS OF {target_version}")
        print(f"restored {t} to v{target_version}")
    else:
        print(f"{t}: unchanged at v{target_version}")

for path in BATCH2_FILES:
    dbutils.fs.rm(path)
    print(f"removed {path}")

STATE_AFTER_RESTORE = snapshot_state()
print()
print(f"{'table':<34} {'rows before':>12} {'rows now':>9}  match")
for t in ALL_TABLES:
    b, n = STATE_BEFORE[t]["rows"], STATE_AFTER_RESTORE[t]["rows"]
    print(f"{t:<34} {b:>12,} {n:>9,}  {b == n}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Rebuild gold and reconcile: must still be 7/7
# MAGIC
# MAGIC Snowflake only ever had batch 1, so after the restore gold must match it
# MAGIC exactly again.

# COMMAND ----------

# MAGIC %run ./03_gold

# COMMAND ----------

# MAGIC %run ./04_reconcile
