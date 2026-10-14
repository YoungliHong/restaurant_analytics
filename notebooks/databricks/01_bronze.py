# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze layer: land the synthetic source CSVs as Delta tables
# MAGIC
# MAGIC Extends the existing `int_order_items_priced` PySpark notebook
# MAGIC (`notebooks/int_order_items_priced.ipynb`) into a full medallion build on
# MAGIC Unity Catalog. This notebook is **bronze only**: read the five source
# MAGIC CSVs as-is, tag them with ingestion metadata, and write each as a Delta
# MAGIC table. No cleaning, no typing, no dedup — that's silver's job. Bronze's
# MAGIC only promise is "what did we actually receive."
# MAGIC
# MAGIC **The CSVs are uploaded by hand, not generated here.** The generator's
# MAGIC seed (42, applied to Faker/random/numpy) only guarantees identical output
# MAGIC against one fixed set of library versions. Databricks serverless's Python
# MAGIC and package versions won't match the local Python 3.9.6 / Faker / numpy
# MAGIC this was generated with, so re-running the generator here could silently
# MAGIC produce a *different* dataset than the one already loaded into Snowflake —
# MAGIC which would quietly break every cross-platform reconciliation this whole
# MAGIC extension exists to do. Uploading the exact same local `raw_data/*.csv`
# MAGIC files removes that risk entirely: bronze is guaranteed byte-identical to
# MAGIC what Snowflake already has.
# MAGIC
# MAGIC Written in full (no TODOs) — this is boilerplate setup, not the core
# MAGIC logic (dedup, transaction-time pricing, MERGE) that's genuinely new and
# MAGIC worth building by hand in silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Catalog / schema / volume setup
# MAGIC
# MAGIC Free Edition gives one metastore and, by default, one catalog
# MAGIC (`workspace`) — creating additional catalogs isn't confirmed to work
# MAGIC (Stage 0 finding), so the medallion boundary lives at the *schema* level
# MAGIC instead (`workspace.bronze`, and `workspace.silver` / `workspace.gold`
# MAGIC once we get there), not the catalog level. In a production Unity Catalog
# MAGIC deployment with multiple catalogs available, bronze/silver/gold would
# MAGIC more commonly each get their own catalog — worth naming that difference
# MAGIC if asked, rather than implying this mirrors production exactly.
# MAGIC
# MAGIC The volume is a **managed** Unity Catalog volume (not external): Free
# MAGIC Edition doesn't support custom workspace storage locations, so there's no
# MAGIC external cloud storage to point an external volume at. A managed volume
# MAGIC uses Unity Catalog's own storage and is still a governed, catalog-visible
# MAGIC place for raw *files* — the correct landing spot for CSVs before they
# MAGIC become Delta tables, unlike a plain Workspace file path (what the old
# MAGIC notebook used) or DBFS (deprecated, disabled in Free Edition).
# MAGIC
# MAGIC Run this cell first, before uploading anything — the volume has to exist
# MAGIC to upload into it. Upload the five CSVs from your local `raw_data/` folder
# MAGIC (the same ones already loaded into Snowflake) into **one subfolder per
# MAGIC table** — batch 1 is just the first file in each folder:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/bronze/raw_files/restaurants/restaurants.csv
# MAGIC /Volumes/workspace/bronze/raw_files/menu_items/menu_items.csv
# MAGIC /Volumes/workspace/bronze/raw_files/customers/customers.csv
# MAGIC /Volumes/workspace/bronze/raw_files/orders/orders.csv
# MAGIC /Volumes/workspace/bronze/raw_files/order_items/order_items.csv
# MAGIC ```
# MAGIC
# MAGIC (Catalog Explorer → `workspace` → `bronze` → `raw_files` → create each
# MAGIC folder → **Upload to this volume**.) Later batches land as new files in
# MAGIC the same folder (e.g. `orders/orders_batch2.csv`). One folder per table
# MAGIC is also the layout COPY INTO and Auto Loader expect, so moving to either
# MAGIC later is a path change, not a restructure.
# MAGIC
# MAGIC Files left at the old flat paths (`raw_files/orders.csv`) are ignored —
# MAGIC the load reads table subfolders only.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1b. One-time reset (off by default)
# MAGIC
# MAGIC Needed once, when switching bronze from overwrite to append. The old
# MAGIC overwrite-built tables carry a hardcoded `_source_file` (`"orders.csv"`),
# MAGIC which never equals a real `_metadata.file_path` — so the append below
# MAGIC would treat batch 1 as unseen and load it a second time. Silver
# MAGIC `orders` / `order_items` are dropped too: they were built by overwrite
# MAGIC without `_source_file`, and MERGE needs a target whose schema matches
# MAGIC its source. (Silver dims are rewritten with `overwriteSchema` every run,
# MAGIC so they don't need it.)
# MAGIC
# MAGIC Set `RESET = True`, run this cell, set it back to `False`. Then run the
# MAGIC rest of 01 → 02 → 03 → 04 on batch 1 alone: that must still reconcile
# MAGIC 7/7 (the regression check for the MERGE conversion).

# COMMAND ----------

RESET = False

if RESET:
    for table in ["restaurants", "menu_items", "customers", "orders", "order_items"]:
        spark.sql(f"DROP TABLE IF EXISTS workspace.bronze.{table}")
    for table in ["orders", "order_items"]:
        spark.sql(f"DROP TABLE IF EXISTS workspace.silver.{table}")
    print("dropped all bronze tables and silver orders / order_items — set RESET back to False")
else:
    print("RESET is False: nothing dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Read each CSV as all-strings, tag with ingestion metadata, write Delta
# MAGIC
# MAGIC **All-string read (`inferSchema=False`):** bronze's job is to preserve
# MAGIC exactly what arrived, not to guess types. The generator injects
# MAGIC deliberate messiness (blank phones, inconsistent order-type casing);
# MAGIC letting Spark infer types at the earliest layer risks it silently
# MAGIC coercing or misreading a dirty value before silver ever gets a chance to
# MAGIC clean it on purpose. Same principle as the dbt staging layer casting
# MAGIC explicitly rather than trusting an inferred type.
# MAGIC
# MAGIC **`_ingested_at` / `_source_file`:** standard bronze metadata. Without
# MAGIC them, two loads of the same table are indistinguishable after the fact —
# MAGIC this is what a later idempotent/incremental design (MERGE, in silver)
# MAGIC would reason about ("what came from which run").
# MAGIC
# MAGIC **`append`, so bronze is a history of every load.** Each run appends
# MAGIC only rows from files bronze hasn't seen yet. `_source_file` is the real
# MAGIC `_metadata.file_path` of the file each row came from (not a hardcoded
# MAGIC name), and `_ingested_at` is one `current_timestamp()` per load —
# MAGIC Spark evaluates it once per query, so every row of a batch shares it.
# MAGIC That shared value is what silver's latest-load-wins dedup sorts on.
# MAGIC
# MAGIC **Idempotency: skip any file whose path is already in bronze.** The
# MAGIC "what's been loaded" state is the bronze table itself
# MAGIC (`SELECT DISTINCT _source_file`): queryable, and rolled back by
# MAGIC `RESTORE TABLE` along with the data. That's the deciding reason over
# MAGIC COPY INTO, whose docs say it skips already-loaded files but not where
# MAGIC that state lives or whether a RESTORE resets it — and the merge test
# MAGIC (05) restores bronze, then must be able to reload batch 2 on its next
# MAGIC run. Paths are compared only against other `_metadata.file_path` values,
# MAGIC never against `dbutils.fs.ls` output, since the two needn't share a
# MAGIC URI prefix.
# MAGIC
# MAGIC Shared limitation with COPY INTO: a corrected file re-uploaded under
# MAGIC the *same* name is skipped. Corrections arrive as new files. At
# MAGIC production scale, COPY INTO or Auto Loader (checkpointed file tracking,
# MAGIC no scan of bronze per run) would replace the anti-join.
# MAGIC
# MAGIC If nothing is new, the write is skipped entirely — no empty append, so
# MAGIC a rerun leaves every bronze table's version unchanged.
# MAGIC
# MAGIC Every file in a folder must share one header in the same column order:
# MAGIC Spark reads a multi-file CSV positionally against the first header.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

SOURCE_TABLES = ["restaurants", "menu_items", "customers", "orders", "order_items"]
VOLUME_PATH = "/Volumes/workspace/bronze/raw_files"


def read_table_folder(table: str):
    """Every CSV in the table's folder, all-string, tagged with load metadata."""
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", False)  # every column lands as STRING — see note above
        .option("pathGlobFilter", "*.csv")  # ignore .DS_Store and other stray files
        .csv(f"{VOLUME_PATH}/{table}")
        .select(
            "*",
            current_timestamp().alias("_ingested_at"),
            col("_metadata.file_path").alias("_source_file"),
        )
    )


for table in SOURCE_TABLES:
    target = f"workspace.bronze.{table}"
    incoming = read_table_folder(table)

    if spark.catalog.tableExists(target):
        loaded = spark.table(target).select("_source_file").distinct()
        # left_anti on a named key moves it to the front; reselect to keep column order
        new_rows = incoming.join(loaded, "_source_file", "left_anti").select(*incoming.columns)
    else:
        new_rows = incoming

    new_files = sorted(r["_source_file"] for r in new_rows.select("_source_file").distinct().collect())
    if not new_files:
        print(f"{target}: no new files, nothing appended")
        continue

    new_rows.write.format("delta").mode("append").saveAsTable(target)
    print(f"{target}: appended {len(new_files)} new file(s)")
    for path in new_files:
        print(f"    {path}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify: table list, row counts vs. source, one sample query
# MAGIC
# MAGIC Row counts compare **each file** in the volume against the bronze rows
# MAGIC tagged with that `_source_file`. Per file, not per table: with append,
# MAGIC a table holds several files, and a per-table total can't tell a missing
# MAGIC file from a double-loaded one. Both sides are keyed by
# MAGIC `_metadata.file_path`, so paths compare like for like. Bronze does no
# MAGIC aggregation or dedup, so every file should match exactly, including the
# MAGIC ~0.4% duplicate `orders` rows the generator injects on purpose — dedup
# MAGIC is silver's job, not bronze's. A file in the volume but not in bronze
# MAGIC shows `bronze_rows = 0`.

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.bronze"))

# COMMAND ----------

from pyspark.sql.functions import coalesce, count, lit

per_file = []
for table in SOURCE_TABLES:
    file_counts = read_table_folder(table).groupBy("_source_file").agg(count("*").alias("csv_rows"))
    bronze_counts = (
        spark.table(f"workspace.bronze.{table}")
        .groupBy("_source_file").agg(count("*").alias("bronze_rows"))
    )
    per_file.append(
        file_counts.join(bronze_counts, "_source_file", "full_outer")
        .select(
            lit(table).alias("table"),
            "_source_file",
            coalesce(col("csv_rows"), lit(0)).alias("csv_rows"),
            coalesce(col("bronze_rows"), lit(0)).alias("bronze_rows"),
        )
        .withColumn("match", col("csv_rows") == col("bronze_rows"))
    )

file_report = per_file[0]
for df in per_file[1:]:
    file_report = file_report.unionByName(df)

display(file_report.orderBy("table", "_source_file"))

# COMMAND ----------

# MAGIC %md
# MAGIC Sample query — eyeball that every column landed as a string and both
# MAGIC metadata columns are present:

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.bronze.orders LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC `printSchema` closes the loop on the all-string claim above: a sample
# MAGIC row can *look* like it's all strings while a numeric-looking column
# MAGIC (`order_id`, `restaurant_id`, `customer_id`) was actually inferred as an
# MAGIC int — this confirms the schema itself, not just the printed values.

# COMMAND ----------

spark.table("workspace.bronze.orders").printSchema()