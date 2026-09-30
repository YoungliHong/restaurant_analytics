# Databricks notebook source
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
# MAGIC (the same ones already loaded into Snowflake) to:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/bronze/raw_files/restaurants.csv
# MAGIC /Volumes/workspace/bronze/raw_files/menu_items.csv
# MAGIC /Volumes/workspace/bronze/raw_files/customers.csv
# MAGIC /Volumes/workspace/bronze/raw_files/orders.csv
# MAGIC /Volumes/workspace/bronze/raw_files/order_items.csv
# MAGIC ```
# MAGIC
# MAGIC (Catalog Explorer → `workspace` → `bronze` → `raw_files` → **Upload to
# MAGIC this volume**, or drag-and-drop each file — same five filenames as
# MAGIC `SOURCE_TABLES` below.)

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_files")

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
# MAGIC **`overwrite` for now:** there's one batch of source data and no
# MAGIC watermark concept yet. Building an appendable, idempotent load is
# MAGIC explicitly a silver-layer (MERGE) concern — solving it here would mean
# MAGIC solving a problem bronze doesn't have yet.

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

SOURCE_TABLES = ["restaurants", "menu_items", "customers", "orders", "order_items"]
VOLUME_PATH = "/Volumes/workspace/bronze/raw_files"

for table in SOURCE_TABLES:
    csv_path = f"{VOLUME_PATH}/{table}.csv"

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)  # every column lands as STRING — see note above
        .csv(csv_path)
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", lit(f"{table}.csv"))
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(f"workspace.bronze.{table}")
    )
    print(f"wrote workspace.bronze.{table}: {df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Verify: table list, row counts vs. source, one sample query
# MAGIC
# MAGIC Row counts compare the volume CSV (the thing actually loaded) against
# MAGIC the Delta table. Bronze does no aggregation or dedup, so these should
# MAGIC match exactly, including the ~0.4% duplicate `orders` rows the generator
# MAGIC injects on purpose — dedup is silver's job, not bronze's.

# COMMAND ----------

display(spark.sql("SHOW TABLES IN workspace.bronze"))

# COMMAND ----------

import pandas as pd

rows = []
for table in SOURCE_TABLES:
    csv_count = len(pd.read_csv(f"{VOLUME_PATH}/{table}.csv"))
    delta_count = spark.table(f"workspace.bronze.{table}").count()
    rows.append({
        "table": table,
        "csv_row_count": csv_count,
        "delta_row_count": delta_count,
        "match": csv_count == delta_count,
    })

display(spark.createDataFrame(pd.DataFrame(rows)))

# COMMAND ----------

# MAGIC %md
# MAGIC Sample query — eyeball that every column landed as a string and both
# MAGIC metadata columns are present:

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.bronze.orders LIMIT 10"))
