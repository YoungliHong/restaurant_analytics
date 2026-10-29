# Databricks medallion extension

Companion to the [Databricks Medallion Extension](../README.md#databricks-medallion-extension) section of the README. Code is in [`notebooks/databricks/`](../notebooks/databricks/).

## Contents
- [Background](#background)
- [How a batch flows](#how-a-batch-flows)
- [Reconciliation](#reconciliation)
- [The Job](#the-job)
- [Design decisions](#design-decisions)
- [Known gaps](#known-gaps)
  
## Background

This started as a single-transform check. `int_order_items_priced` was ported to PySpark ([`notebooks/int_order_items_priced.ipynb`](../notebooks/int_order_items_priced.ipynb)) and run on Databricks Free Edition, to get hands-on time with Spark outside Cosmos (Microsoft's internal data lake, queried with SCOPE). That notebook read the source CSV, computed line-item revenue in memory, and matched the dbt/Snowflake total revenue to the cent ($180,604.30). It wrote no tables and compared a total, not rows.

The medallion build extends that into the full pipeline: all seven marts, Delta tables at every layer, incremental loads, and a row-level reconciliation against Snowflake.

## How a batch flows

**1. Files land in a volume.** Each source table has its own folder in a Unity Catalog volume (`/Volumes/workspace/bronze/raw_files/<table>/`). A new batch is a new CSV in that folder, e.g. `orders/orders_batch2.csv`. Files are treated as immutable: a file that has already been loaded is skipped even if its contents change, so corrections arrive as new files.

**2. Bronze appends only unseen files** (`01_bronze`). For each table, every CSV in its folder is read with every column as a string and tagged with two lineage columns:
- `_source_file`: the file's real path, from `_metadata.file_path`
- `_ingested_at`: one `current_timestamp()` per load, so every row in a batch shares it

Rows whose `_source_file` is already in bronze are dropped with an anti-join, and the rest are appended. If no file is new, nothing is written and the table's Delta version doesn't move. Bronze is append-only: it's a history of every load.

**3. Silver reads all of bronze** (`02_silver`). Orders and line items are read in full on every run. The dimensions (restaurants, menu items, customers) are filtered to the latest snapshot, the rows with the newest `_ingested_at`, because each dimension file is a complete picture.

**4. Silver cleans exactly like dbt staging.** Casts match the Snowflake types: integer IDs and quantities, `DECIMAL(10,2)` prices, `DATE` for signup and opening dates, and `TIMESTAMP_NTZ` for order times. Text cleanup matches `stg_orders` and `stg_customers`: `order_type` and `payment_method` are trimmed and lowercased, `mobile pay` becomes `mobile_pay`, `giftcard` becomes `gift_card`, and emails and phone numbers are trimmed, with blanks turned into NULL.

**5. Silver deduplicates: latest load wins.** Orders keep one row per `order_id`, ordered by:
1. `_ingested_at` descending, so a newer batch's version replaces an older one
2. `order_timestamp` descending, dbt's original rule
3. the business columns, as a final tiebreak so the same row is picked on every run

Line items keep one row per `order_item_id` the same way, without the timestamp step. The result has exactly one row per key, which the MERGE requires.

**6. Silver prices line items.** Deduplicated line items are inner-joined to deduplicated orders, and `line_item_revenue = quantity × unit_price` is stored as `DECIMAL(12,2)`, using the `unit_price` recorded at order time. Each line item keeps its own lineage, not its order's.

**7. Silver writes.** Dimensions are overwritten. Orders and line items are merged on their keys:
- On the first run the target table is created empty, so the initial load is just a MERGE where every row is new.
- A source with duplicate keys is rejected before merging.
- A matched row is updated only if a business column actually differs, compared null-safely; the update also refreshes its lineage columns.
- New keys are inserted. Nothing is deleted.

**8. Gold rebuilds the marts** (`03_gold`). All seven marts are rebuilt from silver and overwritten, dimensions first, then the facts that use them. Lineage columns aren't carried into gold.

**9. Reconcile gates the run** (`04_reconcile`). Each gold mart is compared by key against its Snowflake export, and the run fails unless all seven match. See [Reconciliation](#reconciliation).

**Example: batch 2.** Eight order rows arrive: two status corrections, a cosmetic-only re-delivery, an exact repeat, and three new orders, one of them sent twice. Bronze grows by those 8 rows. Silver's deduplicated source has 5,003 orders, and the MERGE inserts 3 and updates 2. The other three rows either match an existing row with nothing to change or collapse in deduplication.

## Reconciliation

**Getting the Snowflake side into Databricks.** The dbt marts are exported rather than queried live. [`scripts/export_marts.py`](../scripts/export_marts.py) runs locally with the same key-pair credentials dbt uses, selects every row of the seven marts in `ANALYTICS.MARTS`, and writes one CSV per mart in a fixed format:
- timestamps as `YYYY-MM-DD HH:MM:SS` with no timezone (the script stops on a timezone-aware or sub-second value instead of silently truncating it)
- dates as `YYYY-MM-DD`, booleans as `true`/`false`, NULL as an empty field
- decimals written exactly as stored, doubles in the shortest form that converts back to the same value

The CSVs are uploaded to the volume `/Volumes/workspace/reconcile/snowflake_exports/`. They hold batch-1 data only, because that is all Snowflake has.

**Both sides typed the same way.** `04_reconcile` reads each export with the same schema that `03_gold` builds its mart to: column names, order, and types, with money as `DECIMAL(38,2)`. The read is strict: the CSV header must match the schema's column names, and any value that doesn't parse to its type stops the run instead of becoming NULL. A bad export can't quietly turn into nulls that then "match".

**Comparing each mart.** Each mart has a key, its grain:

| Mart | Key |
|---|---|
| `dim_restaurants` | `restaurant_id` |
| `dim_menu_items` | `menu_item_id` |
| `dim_customers` | `customer_id` |
| `fct_orders` | `order_id` |
| `fct_revenue_by_restaurant_category_month` | `restaurant_id`, `category`, `month` |
| `fct_item_demand_by_hour` | `menu_item_id`, `hour_of_day` |
| `fct_price_ratio_by_tier` | `loyalty_tier` |

For each mart:
1. The key must be unique on both sides, or the comparison stops. A duplicate key would multiply rows in the join and distort every count after it.
2. Rows whose key exists only in gold, and rows whose key exists only in Snowflake, are counted.
3. Rows with a matching key are joined, and every other column is compared exactly and null-safely, so two NULLs count as equal and a NULL against a value counts as different. Rows where any column differs are counted as mismatches.

Every column is compared exactly, including the two `DOUBLE` ratio columns in `fct_price_ratio_by_tier`: both engines compute those ratios from identical inputs and they match to the last bit.

The gold table's schema is also checked against its contract: names, order, and types. Nullability is ignored, because whether Delta marks a computed column nullable is a storage detail, not a difference in the data.

**The gate.** A mart passes when its schema matches and all three counts are zero. After printing a summary row per mart, the notebook fails unless all seven pass, so a Job run that reaches this step is green only if gold reproduces the dbt marts exactly.

## The Job

`restaurant_analytics_medallion` runs the four pipeline notebooks as a chain of tasks: bronze → silver → gold → reconcile. Each task starts only if the one before it succeeded, so a failure stops the run at the step that broke. Its definition is in [`notebooks/databricks/job.yml`](../notebooks/databricks/job.yml).

- **Git-sourced:** each task runs the notebook from the `databricks-delta` branch on GitHub, not a copy in the workspace, so a run always uses committed code.
- **Serverless:** tasks run on serverless compute (environment version 6); there is no cluster to configure.
- **Silver parameter:** `batch1_checks` is set to `false`, which skips a diagnostic in `02_silver` that only makes sense on batch-1 data. It prints and never fails, so this only keeps the run's output clean.
- **Manual trigger:** there is no schedule yet.

`05_merge_test` is not part of the Job. It writes test files and restores tables, so it runs on demand.

Because the Snowflake exports are a fixed batch-1 snapshot, the Job works as a regression check on batch-1 data. A run after a new batch has landed in bronze would fail the gate by design, since Snowflake doesn't have that batch.

## Design decisions

### Skipping already-loaded files by path, not COPY INTO

Bronze decides what's new by comparing each file's path against the `_source_file` values already in the table. COPY INTO would also skip files it has already loaded, but its documentation (checked October 2026) doesn't say where it keeps that record or whether `RESTORE TABLE` resets it. That mattered here: the merge test restores bronze to its pre-test version and must be able to load the same test files again on its next run. With path-based skipping, the record of what's loaded *is* the table's data, so restoring the table restores the record too, and it can be inspected with a plain query (`SELECT DISTINCT _source_file`).

The trade-offs:
- Every run reads every file in the folder before filtering, so the cost grows with the number of files.
- A file re-uploaded under the same name is skipped, which COPY INTO also does; corrections have to arrive as new files.

At larger scale, COPY INTO or Auto Loader, which tracks the files it has seen in its own checkpoint, would replace the anti-join without scanning bronze on every run.

### Re-merging all of bronze instead of a watermark

Silver reads every bronze row on every run rather than only rows from new files. Nothing has to be stored between runs, so there's no watermark to advance, lose, or roll back. After a restore or a run that failed halfway, the next run simply converges again. Deduplication also sees every version of every key, so latest-load-wins is decided over the full history rather than one batch at a time.

The cost is a full read of bronze every run, which is trivial at this data size; the conditional MERGE keeps the writes proportional to what actually changed. The usual alternative at scale is to process only rows above a stored `_ingested_at` watermark, or to have Auto Loader hand each new micro-batch to a MERGE (`foreachBatch`).

### Latest load wins, with a fixed tiebreak

dbt's `int_orders_deduped` keeps one row per order by `order_timestamp`, which was enough when there was only ever one batch. With several batches, the newer delivery of an order should win, so silver sorts by `_ingested_at` first, then dbt's `order_timestamp`, then the business columns.

- **Batch-1 output can't change:** batch 1 is a single load, so every row shares one `_ingested_at` and dbt's rule decides, and batch 1's duplicates are byte-identical anyway.
- **The final tiebreak exists for reruns, not correctness:** without it, two different rows tied on both timestamps could be picked in a different order on each run, so a rerun could flip which one wins and report an update. With it, the pick is the same every time. Whether it's the *right* pick is a known gap (see [Known gaps](#known-gaps)).

### Lineage columns are not part of change detection

The MERGE updates a matched row only when one of its business columns differs. `_ingested_at` and `_source_file` are left out of that comparison, so a file that re-delivers an unchanged order doesn't count as a change. When a row *is* updated, its lineage columns are refreshed along with it.

As a result, a row's `_source_file` names the load that last *changed* it, not the last load that delivered it. Tracking the last delivery instead (for example, to spot orders that stopped arriving) would need a separate `_last_seen_at` column. In batch 2, order 4905 arrives with only cosmetic differences (`" Delivery "`, `CARD`), which are identical after cleaning, so it keeps its batch-1 lineage; the merge test asserts exactly that.

### What MERGE does on a no-op rerun

Two behaviors the Delta documentation doesn't spell out, observed on Databricks Free Edition serverless, October 2026:
- **A MERGE that changes nothing still writes a commit**, with 0 rows inserted, updated, and deleted. So a silver table's version moving is not evidence that anything changed. The merge test therefore reads the operation metrics of exactly the commits made since a recorded version, rather than the latest history entry. (Bronze is different: when no file is new it skips the write entirely, so its version really doesn't move.)
- **Rows that match but fail the update condition aren't counted as updated.** In batch 2 the MERGE matched all 5,000 existing orders and reported 2 updated, the two real corrections. On the rerun it matched 5,003 and reported 0.

## Known gaps

### 1. Conflicting duplicates within one batch

- **Gap:** two different rows for the same key in one load are resolved by the business-column tiebreak: deterministically, but not necessarily correctly.
- **Risk:** silver could silently keep the stale version of a corrected order.
- **Fix:** sort by a source `updated_at` or sequence number if the feed provides one. Without one, fail or quarantine conflicting keys instead of picking one.

### 2. Deletes

- **Gap:** the pipeline can't represent something being removed.
- **Risk:** an order item could be voided, but the pipeline would never know. Silver and gold would keep counting its revenue, overstating the order's total.
- **Fix:** three alternatives:
  1. **Soft delete at the source:** removed items arrive with a `voided` status; silver keeps them and gold excludes them.
  2. **Treat each new delivery of an order as the complete set of its items**, and delete any item that doesn't appear in the newest delivery, but only when that delivery includes line items for the order. Status-only corrections, like batch 2's orders 4845 and 4936, arrive without line items and must not wipe them out.
  3. **CDC:** the source sends explicit insert, update, and delete operations.

### 3. Gold on incremental silver

- **Gap:** gold has never been built while batch 2 was in silver, only ever from batch-1 silver.
- **Risk:** a gold bug that only shows up when statuses change or a new month arrives would go unnoticed, because reconcile has only ever compared batch-1 data.
- **Fix:** build the equivalent final state (batch 1 with batch 2 applied) in dbt on Snowflake and use it as the expected outcome. Apply batch 2 to the raw CSVs by replacing the corrected rows rather than appending them: an appended correction has the same `order_timestamp` as the original, so dbt's `int_orders_deduped`, which sorts only by `order_timestamp`, would pick between them arbitrarily. Then, in `05_merge_test`, build gold before the restore and reconcile it against that.

### 4. Batch-specific checks in silver section 8

- **Gap:** the dimension-completeness and guest-count checks assume a single batch.
- **Risk:** false failures once a second dimension snapshot or a guest/customer change arrives. The Job would go red on correct data.
- **Fix:** compare against the latest-snapshot and latest-load-wins versions of bronze, not raw bronze. Fix this together with gap 5 (below).

### 5. `latest_snapshot` is untested

- **Gap:** `latest_snapshot()` has never run with more than one snapshot of a dimension table.
- **Risk:** if it's wrong, silver dimensions could keep the old snapshot, or both. Customer changes (a new email, a loyalty-tier change) would then be lost, or customers duplicated, and `dim_customers` in gold would be wrong. One concrete edge case: two files for the same dimension landing in the same load share one `_ingested_at`, so the "newest" filter would keep both. Section 8's primary-key uniqueness check would catch the duplicates if the two files share customers (for example, two full snapshots); files covering different customers would pass it.
- **Fix:** add a dimension fixture to `05_merge_test`: a second, complete customers snapshot with all 800 existing customers (one of them changed) plus one new customer. It has to be complete because `latest_snapshot` treats the newest file as the whole truth; a file with only the changed and new customers would leave silver with just those two. Assert that silver has the new count, the changed value, and the new load's `_ingested_at`. Silver dimensions don't carry `_source_file`, so asserting on the file itself would first need it added to the dimension cleaning functions. Fix gap 4's check at the same time: the same fixture is what would trigger its false failure, so the two fixes go together.

### 6. The Job is a batch-1 regression check

- **Gap:** reconcile compares against a fixed batch-1 export, so the Job is a regression check, not an ongoing quality gate.
- **Risk:** the first real new batch fails the reconcile gate by design, so a red run can't tell a real bug from new data.
- **Fix:** once gap 4 is fixed, gate ongoing runs on the invariant checks already in `02_silver` and `03_gold`, and run reconcile separately as a regression check. Until then, those checks would also fail on correct new data. Optionally, add an incremental-vs-full-rebuild comparison to test the MERGE path.
