# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver layer: dedup, transaction-time pricing, dbt-exact typing
# MAGIC
# MAGIC **Incremental (MERGE stage):** bronze is now append-only. Every run
# MAGIC re-reads all of bronze; orders / order_items are deduped latest-load-wins
# MAGIC and MERGEd with a conditional update, so a rerun changes nothing; dims
# MAGIC take bronze's latest snapshot and overwrite. Batch 1 alone must still
# MAGIC reconcile 7/7 against Snowflake. See sections 1 and 6.
# MAGIC
# MAGIC Reads `workspace.bronze.*` (all-string, one row per source row) and
# MAGIC reproduces the dbt staging + intermediate logic exactly — same target
# MAGIC types, same null handling, same canonical value mappings, same dedup
# MAGIC rule — so that gold reconciles against the Snowflake/dbt output later.
# MAGIC Source of truth for every spec below: `models/staging/*.sql`,
# MAGIC `models/intermediate/*.sql`, and `sql/01_load_raw.sql` (Snowflake's raw
# MAGIC column types).
# MAGIC
# MAGIC **Scaffold boundary, same as bronze:** setup, pre-checks, the Delta
# MAGIC writes, and the verification cells are written in full. The five
# MAGIC `clean_*` functions, `dedup_orders`, and `price_order_items` are
# MAGIC signatures + docstrings + TODOs only — that's the core logic worth
# MAGIC writing by hand. Every stub returns its input unchanged so the whole
# MAGIC notebook runs top to bottom before anything is filled in; the
# MAGIC verification cells are written to degrade gracefully (not crash) against
# MAGIC that unfilled state, and will report real numbers once the logic is in.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: create schema, read each bronze table

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, nullif, lit, lower, when, row_number, sum as spark_sum, max as spark_max
from pyspark.sql.window import Window
from delta.tables import DeltaTable

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

# "true" runs the batch-1-only pre-check (2b). 05_merge_test passes "false"
# via `%run ./02_silver $batch1_checks="false"`, because after batch 2
# bronze legitimately holds non-identical rows for corrected orders.
dbutils.widgets.text("batch1_checks", "true")
BATCH1_CHECKS = dbutils.widgets.get("batch1_checks").lower() == "true"


def latest_snapshot(bronze_df: DataFrame) -> DataFrame:
    """
    Rows from the most recent load only (max _ingested_at).

    The dims arrive as complete snapshots, so the newest snapshot is the
    whole truth and older ones are history. Bronze is append-only now; without
    this filter a second restaurants file would make every restaurant appear
    twice in silver. On batch 1 alone (a single load) it keeps every row.
    """
    latest = bronze_df.agg(spark_max("_ingested_at")).first()[0]
    return bronze_df.filter(col("_ingested_at") == latest)


# Dims: latest snapshot only, then overwrite (section 6).
bronze_restaurants = latest_snapshot(spark.table("workspace.bronze.restaurants"))
bronze_menu_items = latest_snapshot(spark.table("workspace.bronze.menu_items"))
bronze_customers = latest_snapshot(spark.table("workspace.bronze.customers"))

# Facts: ALL of bronze, every run. Dedup (latest load wins) + conditional
# MERGE make re-processing history correct and cheap at this size, with no
# watermark to store or restore. Production alternative: only rows with
# _ingested_at above a stored watermark, or Auto Loader foreachBatch MERGE.
bronze_orders = spark.table("workspace.bronze.orders")
bronze_order_items = spark.table("workspace.bronze.order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2a. Pre-check: ANSI mode
# MAGIC
# MAGIC Whether a bad cast (e.g. a non-numeric string cast to INT) raises an
# MAGIC error or silently becomes NULL depends entirely on
# MAGIC `spark.sql.ansi.enabled`. This matters for every `clean_*` function
# MAGIC below: under non-ANSI, a cast failure disappears into a null and a
# MAGIC null-count check could pass even though data was quietly dropped.
# MAGIC Know the answer before writing a single cast, not after debugging one.

# COMMAND ----------

print("spark.sql.ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))

test_df = spark.createDataFrame([("not_a_number",)], ["bad_value"])
try:
    result = test_df.select(col("bad_value").cast("int").alias("casted")).collect()
    print("bad cast ('not_a_number' -> int) result:", result)
except Exception as e:
    print("bad cast ('not_a_number' -> int) raised:", type(e).__name__, "-", str(e)[:200])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2b. Pre-check: are the bronze order duplicates byte-identical?
# MAGIC
# MAGIC `int_orders_deduped`'s dedup rule — and the README's own Design
# MAGIC Decisions caveat — assumes the ~0.4% duplicate orders are
# MAGIC byte-identical (same value in every column), not near-duplicates that
# MAGIC happen to share an `order_id`. If that assumption doesn't hold here,
# MAGIC `ROW_NUMBER() ... ORDER BY order_timestamp DESC` picking an arbitrary
# MAGIC one of them is the wrong fix, and `dedup_orders` needs to know that
# MAGIC before it's written, not after.
# MAGIC
# MAGIC Method: if every duplicate set is truly identical, the count of
# MAGIC distinct full rows (excluding the bronze-only lineage columns) must
# MAGIC equal the count of distinct `order_id` — because two rows that differ
# MAGIC in any column but share an `order_id` would inflate the first count
# MAGIC without inflating the second.
# MAGIC
# MAGIC **Batch-1 only** (guarded by the `batch1_checks` widget): once batch 2
# MAGIC corrects an order, bronze holds two *different* rows for that
# MAGIC `order_id` by design, and this check would correctly print False.

# COMMAND ----------

if BATCH1_CHECKS:
    non_lineage_cols = [c for c in bronze_orders.columns if c not in ("_ingested_at", "_source_file")]

    distinct_full_rows = bronze_orders.select(*non_lineage_cols).distinct().count()
    distinct_order_ids = bronze_orders.select("order_id").distinct().count()

    print(f"distinct full rows (excl. lineage cols): {distinct_full_rows:,}")
    print(f"distinct order_id:                       {distinct_order_ids:,}")
    print("byte-identical duplicates confirmed:", distinct_full_rows == distinct_order_ids)
else:
    print("batch1_checks = false: skipping the batch-1 byte-identical pre-check")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Per-table cleaning 
# MAGIC
# MAGIC Fill-in order (simplest to most involved, each one a prerequisite
# MAGIC for something later — see the stop note at the end):
# MAGIC 1. `clean_menu_items` — one decimal cast, nothing else.
# MAGIC 2. `clean_restaurants` — a rename plus a date cast.
# MAGIC 3. `clean_customers` — trim + nullif on two columns.
# MAGIC 4. `clean_order_items` — several int casts plus one decimal cast.
# MAGIC 5. `clean_orders` — the most involved: an NTZ timestamp cast and the
# MAGIC    two-column dirty-value remapping (order_type, payment_method).
# MAGIC
# MAGIC You'll likely want some of these when implementing the casts:
# MAGIC `from pyspark.sql.types import IntegerType, StringType, DateType, DecimalType, TimestampNTZType`

# COMMAND ----------



def clean_restaurants(bronze_df: DataFrame) -> DataFrame:
    """
    Mirrors models/staging/stg_restaurants.sql exactly.

    Transform (no cleaning beyond typing + one rename):
      - restaurant_id -> IntegerType   (Snowflake: INTEGER)
      - name          -> renamed to restaurant_name, StringType, value unchanged
      - city          -> StringType, unchanged
      - state         -> StringType, unchanged
      - opened_date   -> DateType      (Snowflake: DATE)

    Output columns, in order: restaurant_id, restaurant_name, city, state,
    opened_date. Bronze's `_ingested_at` / `_source_file` lineage columns
    have no dbt equivalent (dbt has no bronze layer) — decide whether to
    carry them through or drop them; that's a Databricks-specific call,
    not something to match against dbt.

    """
    
    return bronze_df.select(
      col("restaurant_id").cast("int").alias("restaurant_id"),
      col("name").alias("restaurant_name"),
      col("city"),
      col("state"),
      col("opened_date").cast("date").alias("opened_date"),
      col("_ingested_at")
    )


def clean_menu_items(bronze_df: DataFrame) -> DataFrame:
    """
    Mirrors models/staging/stg_menu_items.sql exactly.

    Transform:
      - menu_item_id -> IntegerType          (Snowflake: INTEGER)
      - item_name    -> StringType, unchanged
      - category     -> StringType, unchanged
      - base_price   -> DecimalType(10, 2)   (Snowflake: NUMBER(10,2);
                         dbt: base_price::numeric(10,2))

    No trimming/casing cleanup — dbt applies none to this table.
    Output columns, in order: menu_item_id, item_name, category, base_price.

    """
    return bronze_df.select(
      col("menu_item_id").cast("int").alias("menu_item_id"),
      col("item_name"),
      col("category"),
      col("base_price").cast("decimal(10,2)").alias("base_price"),
      col("_ingested_at")
    )


def clean_customers(bronze_df: DataFrame) -> DataFrame:
    """
    Mirrors models/staging/stg_customers.sql exactly.

    Transform:
      - customer_id  -> IntegerType (Snowflake: INTEGER)
      - first_name   -> StringType, unchanged
      - last_name    -> StringType, unchanged
      - email        -> StringType; trim, then empty string -> NULL
                        (dbt: nullif(trim(email), '')). Note: Snowflake's
                        raw COPY INTO already NULL_IFs empty fields, and
                        Spark's CSV reader does the same by default, so
                        bronze may already hold NULL rather than '' here —
                        apply trim+nullif anyway; it's a no-op if so and
                        matches dbt's logic exactly either way.
      - phone        -> StringType; same trim + nullif('') rule as email
                        (dbt: nullif(trim(phone), ''))
      - signup_date  -> DateType (Snowflake: DATE)
      - loyalty_tier -> StringType, unchanged. Values are bronze/silver/gold
                        — the generator's loyalty-tier naming, unrelated to
                        (and coincidentally the same words as) our medallion
                        layer names. Don't let the collision cause confusion
                        when reading output later.

    Output columns, in order: customer_id, first_name, last_name, email,
    phone, signup_date, loyalty_tier.

    """
    return bronze_df.select(
      col("customer_id").cast("int").alias("customer_id"),
      col("first_name"),
      col("last_name"),
      nullif(trim(col("email")), lit("")).alias("email"),
      nullif(trim(col("phone")), lit("")).alias("phone"),
      col("signup_date").cast("date").alias("signup_date"),
      col("loyalty_tier"),
      col("_ingested_at")
    )


def clean_order_items(bronze_df: DataFrame) -> DataFrame:
    """
    Mirrors models/staging/stg_order_items.sql exactly.

    Transform:
      - order_item_id -> IntegerType        (Snowflake: INTEGER)
      - order_id      -> IntegerType        (Snowflake: INTEGER)
      - menu_item_id  -> IntegerType        (Snowflake: INTEGER)
      - quantity      -> IntegerType        (Snowflake: INTEGER; dbt doesn't
                         cast this explicitly, it's already the right shape)
      - unit_price    -> DecimalType(10, 2) (Snowflake: NUMBER(10,2);
                         dbt: unit_price::numeric(10,2))

    No trimming/casing — nothing here is a string needing cleanup.
    Output columns, in order: order_item_id, order_id, menu_item_id,
    quantity, unit_price.

    """
    return bronze_df.select(
      col("order_item_id").cast("int").alias("order_item_id"),
      col("order_id").cast("int").alias("order_id"),
      col("menu_item_id").cast("int").alias("menu_item_id"),
      col("quantity").cast("int").alias("quantity"),
      col("unit_price").cast("decimal(10,2)").alias("unit_price"),
      col("_ingested_at"),
      col("_source_file")
    )


def clean_orders(bronze_df: DataFrame) -> DataFrame:
    """
    Mirrors models/staging/stg_orders.sql exactly.

    Transform:
      - order_id        -> IntegerType (Snowflake: INTEGER)
      - restaurant_id   -> IntegerType (Snowflake: INTEGER)
      - customer_id     -> IntegerType, NULLABLE (Snowflake: INTEGER,
                           nullable — ~18% guest orders carry no customer)
      - order_timestamp -> TimestampNTZType (pyspark.sql.types). Snowflake's
                           column is TIMESTAMP_NTZ exactly (sql/01_load_raw.sql)
                           — this is NOT the same as Spark's legacy,
                           timezone-aware TimestampType; using the wrong one
                           risks a silent timezone shift when this gets
                           reconciled against Snowflake later. dbt: ::timestamp.
      - order_type      -> StringType. lower(trim(order_type)).
                           Canonical values: dine_in, takeout, delivery.
      - payment_method  -> StringType. lower(trim(payment_method)), THEN:
                             'mobile pay' -> 'mobile_pay'
                             'giftcard'   -> 'gift_card'
                             else unchanged
                           (exact case logic from stg_orders.sql — note the
                           checks run AFTER lower+trim, so they match the
                           lowercased/trimmed value, not the raw one).
                           Canonical values: card, cash, mobile_pay, gift_card.
      - status          -> StringType, unchanged. Canonical values:
                           completed, cancelled, refunded — the generator
                           injects no messiness here, so no cleanup needed.

    Output columns, in order: order_id, restaurant_id, customer_id,
    order_timestamp, order_type, payment_method, status.

    """
    pm = lower(trim(col("payment_method")))
    return bronze_df.select(
      col("order_id").cast("int").alias("order_id"),
      col("restaurant_id").cast("int").alias("restaurant_id"),
      col("customer_id").cast("int").alias("customer_id"),
      col("order_timestamp").cast("timestamp_ntz").alias("order_timestamp"),
      lower(trim(col("order_type"))).alias("order_type"),
      when(pm == "mobile pay", "mobile_pay")
      .when(pm == "giftcard", "gift_card")
      .otherwise(pm)
      .alias("payment_method"),
      col("status").alias("status"),
      col("_ingested_at"),
      col("_source_file")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dedup

# COMMAND ----------

def dedup_orders(cleaned_orders_df: DataFrame) -> DataFrame:
    """
    Based on models/intermediate/int_orders_deduped.sql, extended for
    multiple batches.

    Input: the OUTPUT of clean_orders (already lowercased/remapped/typed),
    not bronze directly. dbt's int_orders_deduped refs stg_orders, which
    is itself the cleaned layer, not the raw source.

    Rule: row_number() over (partition by order_id order by _ingested_at
    desc, order_timestamp desc, then the business columns as the final tiebreaker), keep rn = 1, drop the helper column.

    This departs deliberately from int_orders_deduped, which only ever saw
    one batch:
      - Across batches, the newer load's version of an order wins, so a
        batch-2 correction beats the batch-1 row.
      - Within one batch, every row shares one _ingested_at, so
        order_timestamp desc (dbt's rule) decides.
    Batch 1's output is unchanged: one load means _ingested_at ties
    everywhere, and its duplicates are byte-identical (2b).

    The output is the MERGE source, so it must hold exactly one row per
    order_id. Delta enforces no primary key: a duplicate new key is
    silently double-inserted, and a duplicate existing key makes MERGE fail.

    Expected: one row per distinct order_id in bronze (5,000 for batch 1).

    Known gap: the business-column tie breaker makes the pick deterministic (stable across reruns) but not necessarily correct; a real feed would need a source-side sequence number or updated_at col.
    """
    window_spec = Window.partitionBy("order_id").orderBy(col("_ingested_at").desc(), col("order_timestamp").desc(), *(col(c).desc() for c in ORDERS_BUSINESS_COLS))
    return (
      cleaned_orders_df
      .withColumn("row_num", row_number().over(window_spec)) 
      .filter(col("row_num") == 1) 
      .drop("row_num")
    )


def dedup_order_items(cleaned_order_items_df: DataFrame) -> DataFrame:
    """
    Latest load wins on order_item_id. NEW at the MERGE stage; no dbt
    equivalent (stg_order_items tests order_item_id unique, and batch 1
    satisfies that, so on batch 1 alone this removes nothing).

    Why it's required now: bronze is append-only and silver re-reads all of
    it, so a repeated order (batch 2 re-sends order 4998 with its line item
    13081) leaves order_item_id 13081 in bronze twice. Two source rows
    matching one target row makes MERGE fail; two new duplicate keys would
    be double-inserted. The MERGE source must be unique on order_item_id.

    Rule: row_number() over partition by order_item_id order by
    _ingested_at desc, then order_id, menu_item_id, quantity, unit_price as the final tiebreaker (line_item_revenue is derived from these, and doesn't exist until price_order_items), keep rn = 1, drop the helper. Same within-batch
    known gap as dedup_orders: the business column tie-breaker makes the pick deterministic but not necessarily correct. 

    Input: the output of clean_order_items. Output: same columns, one row
    per order_item_id. Runs BEFORE price_order_items, so pricing and the
    join see one row per line item.
    """
    window_spec = Window.partitionBy("order_item_id").orderBy(col("_ingested_at").desc(), *(col(c).desc() for c in ["order_id", "menu_item_id", "quantity", "unit_price"]))
    return (
      cleaned_order_items_df
      .withColumn("row_num", row_number().over(window_spec)) 
      .filter(col("row_num") == 1) 
      .drop("row_num")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transaction-time pricing

# COMMAND ----------

def price_order_items(cleaned_order_items_df: DataFrame, deduped_orders_df: DataFrame) -> DataFrame:
    """
    Mirrors models/intermediate/int_order_items_priced.sql exactly.

    Inner join cleaned_order_items to deduped_orders on order_id. In
    practice this join drops nothing: every order_id in order_items is
    also present, exactly once, in deduped_orders, since dedup only
    collapses exact-duplicate HEADER rows — it never removes a distinct
    order_id. The join exists in dbt as an explicit guarantee that every
    line item resolves to a real, deduplicated order (and
    int_order_items_priced.order_id is tested with a `relationships` test
    against int_orders_deduped for exactly this reason) — not to filter
    anything out.

    Computes: line_item_revenue = quantity * unit_price. dbt doesn't cast
    this product's type explicitly; Snowflake widens INTEGER * NUMBER(10,2)
    automatically under its own promotion rules. Pick and document an
    explicit Decimal precision here (e.g. DecimalType(12, 2)) rather than
    relying on whatever Spark's default decimal-multiplication promotion
    produces — an undocumented precision is exactly the kind of thing that
    silently drifts from Snowflake's result at reconciliation time.

    This is the transaction-time pricing model: unit_price is read as
    already-snapshotted at order time (see clean_order_items), never
    joined back to menu_items.base_price — see the README's
    "Transaction-time pricing" Design Decision for why that distinction
    matters.

    Output columns, in order: order_item_id, order_id, menu_item_id,
    quantity, unit_price, line_item_revenue.

    line_item_revenue is cast to DECIMAL(12,2): scale 2 matches the cents in unit_price, so no rounding happens, and precision 12 leaves ample headroom for quantity × price at this dataset's scale. Under ANSI mode, an overflow would raise an error rather than silently produce null.

    With multiple batches, a line item's lineage is the load that delivered
    that line, which needn't be the load that delivered its order header
    (e.g. batch 2 corrects order 4845's header but re-sends no lines).
    Output columns become: ..., line_item_revenue, _ingested_at, _source_file.
    """
    return cleaned_order_items_df.join(deduped_orders_df, "order_id", how = "inner").select(
        col("order_item_id"),
        col("order_id"),
        col("menu_item_id"),
        col("quantity"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).cast("decimal(12,2)").alias("line_item_revenue"),
        cleaned_order_items_df["_ingested_at"],
        cleaned_order_items_df["_source_file"]
    )
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write: dims overwrite, orders / order_items MERGE
# MAGIC
# MAGIC **Dims (restaurants, menu_items, customers) stay `overwrite`.** Each dim
# MAGIC file is a complete snapshot, so the newest snapshot (filtered in setup)
# MAGIC *is* the correct table; dbt treats them the same way (`dim_menu_items`
# MAGIC is the *current* price, no history). MERGE would only earn its keep if
# MAGIC dim files were partial (only changed customers) or if we wanted SCD2
# MAGIC history — neither is in scope.
# MAGIC
# MAGIC **orders / order_items are MERGEd** on `order_id` / `order_item_id`.
# MAGIC One code path: on the first run the target is created empty (from the
# MAGIC source's schema), so the initial load is just a MERGE where every row is
# MAGIC NOT MATCHED. Each merge goes through `run_merge`, which:
# MAGIC 1. creates the empty target if it doesn't exist,
# MAGIC 2. refuses a source with duplicate keys (a clear error here instead of
# MAGIC    MERGE's multiple-match failure or a silent double insert),
# MAGIC 3. records the table version before and after, and prints the
# MAGIC    `operationMetrics` of exactly the commits this run made — or "no
# MAGIC    commit" if the version didn't move. Whether a MERGE that changes
# MAGIC    nothing writes a commit at all isn't documented; this reports either
# MAGIC    case correctly, and 05's rerun step is where it gets observed.
# MAGIC
# MAGIC Change detection compares **cleaned** business columns, so a
# MAGIC casing-only correction in bronze (batch 2's order 4905) is not a change.
# MAGIC Lineage columns are excluded from the comparison but refreshed whenever
# MAGIC a row is updated.

# COMMAND ----------

ORDERS_KEY = "order_id"
ORDERS_BUSINESS_COLS = ["restaurant_id", "customer_id", "order_timestamp",
                        "order_type", "payment_method", "status"]

ORDER_ITEMS_KEY = "order_item_id"
ORDER_ITEMS_BUSINESS_COLS = ["order_id", "menu_item_id", "quantity",
                             "unit_price", "line_item_revenue"]

LINEAGE_COLS = ["_ingested_at", "_source_file"]


def table_version(table: str) -> int:
    return spark.sql(f"DESCRIBE HISTORY {table} LIMIT 1").first()["version"]


def run_merge(table: str, key: str, source_df: DataFrame, merge_fn) -> None:
    """Create-if-missing, key-uniqueness guard, merge, and report what this run committed."""
    if not spark.catalog.tableExists(table):
        source_df.limit(0).write.format("delta").saveAsTable(table)
        print(f"{table}: created empty target (initial load)")

    total = source_df.count()
    distinct_keys = source_df.select(key).distinct().count()
    if distinct_keys != total:
        raise ValueError(
            f"{table}: MERGE source has {total - distinct_keys} duplicate {key}s — dedup must run first"
        )

    before = table_version(table)
    merge_fn(source_df)
    after = table_version(table)

    if after == before:
        print(f"{table}: no commit (version stays {before}); source rows {total:,}")
        return
    commits = (
        spark.sql(f"DESCRIBE HISTORY {table}")
        .filter((col("version") > before) & (col("version") <= after))
        .orderBy("version")
        .collect()
    )
    for c in commits:
        m = c["operationMetrics"] or {}
        print(
            f"{table}: v{c['version']} {c['operation']} — "
            f"source {m.get('numSourceRows', '-')}, "
            f"inserted {m.get('numTargetRowsInserted', '-')}, "
            f"updated {m.get('numTargetRowsUpdated', '-')}, "
            f"deleted {m.get('numTargetRowsDeleted', '-')}"
        )


def merge_orders(source_df: DataFrame) -> None:
    """
    MERGE source_df into workspace.silver.orders.

    source_df: dedup_orders(clean_orders(all of bronze)) — one row per
    order_id (run_merge has already checked that), columns order_id,
    ORDERS_BUSINESS_COLS, then LINEAGE_COLS.

    Expected: batch 1 on an empty target -> inserted 5,000, updated 0.
    Batch 2 -> inserted 3 (5001-5003), updated 2 (4845, 4936). Any rerun
    -> inserted 0, updated 0 (or no commit at all).
    """
    target = DeltaTable.forName(spark, "workspace.silver.orders").alias("t")
    source = source_df.alias("s")

    changed = None
    for c in ORDERS_BUSINESS_COLS:
        cond = ~col(f"t.{c}").eqNullSafe(col(f"s.{c}"))  
        changed = cond if changed is None else (changed | cond)

    (target
     .merge(source, "s.order_id = t.order_id")
     .whenMatchedUpdateAll(condition = changed)
     .whenNotMatchedInsertAll()
     .execute())     


def merge_order_items(source_df: DataFrame) -> None:
    """
    MERGE source_df into workspace.silver.order_items. 

    source_df: price_order_items(dedup_order_items(clean_order_items(all of
    bronze)), silver_orders) — one row per order_item_id, columns
    order_item_id, ORDER_ITEMS_BUSINESS_COLS, then LINEAGE_COLS.

    Must do: same shape as merge_orders —
      - Match on target.order_item_id = source.order_item_id.
      - WHEN MATCHED and any ORDER_ITEMS_BUSINESS_COLS differs null-safely:
        UPDATE business columns + LINEAGE_COLS.
      - WHEN NOT MATCHED: INSERT all columns.
      - No delete clause.

    Expected: batch 1 -> inserted 13,088, updated 0. Batch 2 -> inserted 5
    (13089-13093), updated 0 (13081 is an exact repeat). Rerun -> 0 / 0.
    """
    target = DeltaTable.forName(spark, "workspace.silver.order_items").alias("t")
    source = source_df.alias("s")

    changed = None
    for c in ORDER_ITEMS_BUSINESS_COLS:
        cond = ~col(f"t.{c}").eqNullSafe(col(f"s.{c}"))
        changed = cond if changed is None else (changed | cond)

    (target
     .merge(source, "s.order_item_id = t.order_item_id")
     .whenMatchedUpdateAll(condition = changed)
     .whenNotMatchedInsertAll()
     .execute())

# COMMAND ----------

silver_restaurants = clean_restaurants(bronze_restaurants)
silver_menu_items = clean_menu_items(bronze_menu_items)
silver_customers = clean_customers(bronze_customers)

cleaned_orders = clean_orders(bronze_orders)
cleaned_order_items = clean_order_items(bronze_order_items)

silver_orders = dedup_orders(cleaned_orders)
silver_order_items = price_order_items(dedup_order_items(cleaned_order_items), silver_orders)

SILVER_TABLES = {
    "restaurants": silver_restaurants,
    "menu_items": silver_menu_items,
    "customers": silver_customers,
    "orders": silver_orders,
    "order_items": silver_order_items,
}

for name in ["restaurants", "menu_items", "customers"]:
    df = SILVER_TABLES[name]
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"workspace.silver.{name}")
    print(f"wrote workspace.silver.{name}: {df.count():,} rows (overwrite)")

run_merge("workspace.silver.orders", ORDERS_KEY, silver_orders, merge_orders)
run_merge("workspace.silver.order_items", ORDER_ITEMS_KEY, silver_order_items, merge_order_items)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verification
# MAGIC
# MAGIC Row counts, schemas, and a per-column casting-null check. 

# COMMAND ----------

for name in SILVER_TABLES:
    print(f"workspace.silver.{name}: {spark.table(f'workspace.silver.{name}').count():,} rows")

# COMMAND ----------

for name in SILVER_TABLES:
    print(f"--- workspace.silver.{name} ---")
    spark.table(f"workspace.silver.{name}").printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Casting-null check: for each table, join bronze to silver on primary
# MAGIC key and count rows where a column was non-null in bronze but is null
# MAGIC in the corresponding silver column. Must be 0 for every column once
# MAGIC the functions are implemented — any nonzero count means a cast
# MAGIC silently turned a real value into a lost one. A column reported as
# MAGIC "not present yet" just means its `clean_*` function hasn't been
# MAGIC filled in (e.g. `restaurant_name` doesn't exist until `clean_restaurants`
# MAGIC performs the rename) — expected before fill-in, not an error.
# MAGIC
# MAGIC `email`/`phone` are deliberately excluded for customers (bronze nulls
# MAGIC there are expected by design, ~ 2%/~1%), and `customer_id` is excluded
# MAGIC for orders (the ~18% guest-order nulls are legitimate in both layers).
# MAGIC If the bronze/silver primary-key column types differ (string vs. int)
# MAGIC once casts are implemented, the join below may need an explicit cast
# MAGIC to match — tie that back to the ANSI-mode pre-check if it misbehaves.

# COMMAND ----------

def count_casting_nulls(bronze_df, silver_df, pk_col, compare_cols):
    """compare_cols: list of (bronze_col_name, silver_col_name) pairs."""
    
    b = bronze_df.alias("b")
    s = silver_df.alias("s")
    joined = b.join(s, col(f"b.{pk_col}").cast("int") == col(f"s.{pk_col}"), "inner")
    results = {}
    for bronze_col, silver_col in compare_cols:
        if silver_col not in silver_df.columns:
            results[silver_col] = "not present yet (stub not filled in)"
            continue
        bad = joined.filter(col(f"b.{bronze_col}").isNotNull() & col(f"s.{silver_col}").isNull()).count()
        results[silver_col] = bad
    return results


checks = {
    "restaurants": count_casting_nulls(
        bronze_restaurants, silver_restaurants, "restaurant_id",
        [("restaurant_id", "restaurant_id"), ("name", "restaurant_name"),
         ("city", "city"), ("state", "state"), ("opened_date", "opened_date")],
    ),
    "menu_items": count_casting_nulls(
        bronze_menu_items, silver_menu_items, "menu_item_id",
        [("menu_item_id", "menu_item_id"), ("item_name", "item_name"),
         ("category", "category"), ("base_price", "base_price")],
    ),
    "customers": count_casting_nulls(
        bronze_customers, silver_customers, "customer_id",
        [("customer_id", "customer_id"), ("first_name", "first_name"),
         ("last_name", "last_name"), ("signup_date", "signup_date")],
    ),
    "orders": count_casting_nulls(
        bronze_orders, silver_orders, "order_id",
        [("order_id", "order_id"), ("restaurant_id", "restaurant_id"),
         ("order_timestamp", "order_timestamp"), ("order_type", "order_type"),
         ("payment_method", "payment_method"), ("status", "status")],
    ),
    "order_items": count_casting_nulls(
        bronze_order_items, silver_order_items, "order_item_id",
        [("order_item_id", "order_item_id"), ("order_id", "order_id"),
         ("menu_item_id", "menu_item_id"), ("quantity", "quantity"),
         ("unit_price", "unit_price")],
    ),
}

for table, cols in checks.items():
    for col_name, bad_count in cols.items():
        print(f"{table}.{col_name}: {bad_count} casting-introduced nulls")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver checks: logic assertions
# MAGIC
# MAGIC Section 7's verification covers structure (row counts, schemas, and
# MAGIC nulls introduced by casting). This section checks that the *logic* did
# MAGIC what the dbt spec says, against known expected values:
# MAGIC
# MAGIC - **orders:** one row per `order_id`; and exactly as many distinct bronze `order_ids` (dedupe removed duplicates and nothing else); `order_type`, `payment_type`, and `status` contain exactly their canonical values; guest orders (null `customer_id`) were preserved.
# MAGIC - **all other tables:** unique on the primary key and complete against bronze (order_items: distinct bronze keys; dims: bronze row count).
# MAGIC - **customers:** no blank or untrimmed `email`/`phone` survived cleanup.
# MAGIC - **order_items:** no null `line_item_revenue`, and `quantity x unit_price = line_item_revenue`on every row; total revenue printed for inspection.
# MAGIC
# MAGIC
# MAGIC Fixed expectations are `assert`s, so a regression fails loudly on rerun.

# COMMAND ----------


"""
Orders:
1. Uniqueness: distinct order_id = row count (one row per order).
2. Completeness: row count = distinct order_ids in bronze (dedup removed duplicates and nothing else)
3. order_type, payment_method, and status contain exactly their canonical values
4. Guest orders are preserved: null customer_id count = deduped bronze's null count
"""
orders = spark.table("workspace.silver.orders")
bronze_orders = spark.table("workspace.bronze.orders")

orders_total = orders.count()
distinct_order_ids = orders.select("order_id").distinct().count()
expected_orders = bronze_orders.select("order_id").distinct().count()

assert distinct_order_ids == orders_total, f"silver orders: {orders_total - distinct_order_ids} duplicate order_ids"
assert orders_total == expected_orders, f"silver orders: expected {expected_orders} (distinct bronze_ids), got {orders_total}"
print(f"silver_orders: {orders_total} rows; bronze: {bronze_orders.count()} rows; duplicates removed: {bronze_orders.count() - orders_total}")

def distinct_values(table, column):
    rows = spark.table(table).select(column).distinct().collect()
    return {row[column] for row in rows}

expected_values = {"order_type":{"dine_in", "takeout", "delivery"}, "payment_method": {"cash", "card", "mobile_pay", "gift_card"}, "status": {"completed", "cancelled", "refunded"}}

for column, expected in expected_values.items():
    actual = distinct_values("workspace.silver.orders", column)
    assert actual == expected, f"silver orders.{column}: expected {expected}, got {actual}"

expected_guests = bronze_orders.dropDuplicates(["order_id"]).filter(col("customer_id").isNull()).count()
actual_guests = spark.table("workspace.silver.orders").filter(col("customer_id").isNull()).count()
assert expected_guests == actual_guests, f"silver orders: expected {expected_guests} guest orders, got {actual_guests}"

"""
All silver tables: uniqueness on the primary key, and completeness against
bronze. orders and order_items: distinct bronze keys (latest-load-wins dedup
collapses repeats across batches and removes nothing else). Dims: bronze
row count, since nothing is deduped or filtered there.
"""
primary_keys = {
    "restaurants":"restaurant_id",
    "menu_items":"menu_item_id",
    "customers":"customer_id",
    "order_items":"order_item_id",
}

for name, pk in primary_keys.items():
    silver = spark.table(f"workspace.silver.{name}")
    silver_n = silver.count()
    distinct_n = silver.select(pk).distinct().count()
    bronze = spark.table(f"workspace.bronze.{name}")
    if name == "order_items":
        bronze_n = bronze.select(pk).distinct().count()
        expected_label = f"distinct bronze {pk}s"
    else:
        bronze_n = bronze.count()
        expected_label = "bronze rows"
    assert distinct_n == silver_n, f"silver {name}: {silver_n - distinct_n} duplicate {pk}s"
    assert silver_n == bronze_n, f"silver {name}: expected {bronze_n} rows (={expected_label}), got {silver_n}"

"""
Customers

1. zero blank email or phone after trimming
"""

customers = spark.table("workspace.silver.customers")
blank_emails = customers.filter(trim(col("email")) == "").count()
blank_phones = customers.filter(trim(col("phone")) == "").count()
untrimmed =  customers.filter((col("email") != trim(col("email"))) | (col("phone") != trim(col("phone")))).count()
assert blank_emails == 0, f"silver customers: expected 0 blank emails, got {blank_emails}"
assert blank_phones == 0, f"silver customers: expected 0 blank phone numbers, got{blank_phones}"
assert untrimmed == 0, f"silver customers: {untrimmed} rows with untrimmed email or phone"

"""
Order items

1. no null line_item_revenue
2. line_item_revenue = quantity x unit_price on every row
3. Sample rows displayed and total revenue printed for inspection
"""
items = spark.table("workspace.silver.order_items")
null_revenue = items.filter(col("line_item_revenue").isNull()).count()
assert null_revenue == 0, f"silver order_items: expected 0 null line_item_revenue, got {null_revenue}"

mismatches = items.filter(col("quantity") * col("unit_price") != col("line_item_revenue")).count()
assert mismatches == 0, f"silver order_items: {mismatches} rows where quantity * unit_price != line_item_revenue"

display(items.select("order_item_id", "quantity", "unit_price", "line_item_revenue").limit(5))

total = items.agg(spark_sum("line_item_revenue")).first()[0]
print(f"Total line_item_revenue: ${total:,.2f}")
                