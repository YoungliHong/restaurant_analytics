# Databricks medallion extension

Companion to the [Databricks Medallion Extension](../README.md#databricks-medallion-extension) section of the README. Code is in [`notebooks/databricks/`](../notebooks/databricks/).

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
