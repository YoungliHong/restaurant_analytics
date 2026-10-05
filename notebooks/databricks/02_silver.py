# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver layer: dedup, transaction-time pricing, dbt-exact typing
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
from pyspark.sql.functions import col, trim, nullif, lit, lower, when, row_number, sum as spark_sum
from pyspark.sql.window import Window

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.silver")

bronze_restaurants = spark.table("workspace.bronze.restaurants")
bronze_menu_items = spark.table("workspace.bronze.menu_items")
bronze_customers = spark.table("workspace.bronze.customers")
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

# COMMAND ----------

non_lineage_cols = [c for c in bronze_orders.columns if c not in ("_ingested_at", "_source_file")]

distinct_full_rows = bronze_orders.select(*non_lineage_cols).distinct().count()
distinct_order_ids = bronze_orders.select("order_id").distinct().count()

print(f"distinct full rows (excl. lineage cols): {distinct_full_rows:,}")
print(f"distinct order_id:                       {distinct_order_ids:,}")
print("byte-identical duplicates confirmed:", distinct_full_rows == distinct_order_ids)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Per-table cleaning — SCAFFOLD ONLY
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
      col("_ingested_at")
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
      col("_ingested_at")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Dedup — SCAFFOLD ONLY

# COMMAND ----------

def dedup_orders(cleaned_orders_df: DataFrame) -> DataFrame:
    """
    Mirrors models/intermediate/int_orders_deduped.sql exactly.

    Input: the OUTPUT of clean_orders (already lowercased/remapped/typed),
    not bronze directly — dbt's int_orders_deduped refs stg_orders, which
    is itself the cleaned layer, not the raw source.

    Rule: row_number() over (partition by order_id order by
    order_timestamp desc), keep rn = 1, drop the helper column.

    The ordering is arbitratry, not a meaningful tiebreak — per the 2b
    pre-check and the README's own Design Decisions caveat, the ~0.4%
    duplicate rows are byte-identical, so any deterministic pick is
    correct. order_timestamp desc is just what dbt happens to use.

    Expected result once implemented: exactly 5,000 rows (5,020 bronze
    rows minus the 20 confirmed-identical duplicates), one row per
    distinct order_id.
    """
    window_spec = Window.partitionBy("order_id").orderBy(col("order_timestamp").desc())
    return (
      cleaned_orders_df
      .withColumn("row_num", row_number().over(window_spec)) 
      .filter(col("row_num") == 1) 
      .drop("row_num")
    )


# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Transaction-time pricing — SCAFFOLD ONLY

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
    """
    return cleaned_order_items_df.join(deduped_orders_df, "order_id", how = "inner").select(
        col("order_item_id"),
        col("order_id"),
        col("menu_item_id"),
        col("quantity"),
        col("unit_price"),
        (col("quantity") * col("unit_price")).cast("decimal(12,2)").alias("line_item_revenue"),
        deduped_orders_df["_ingested_at"]
    )
    

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write each result as a Delta table in workspace.silver
# MAGIC
# MAGIC `overwrite` for now, same reasoning as bronze: there's no watermark
# MAGIC concept yet, and building one is explicitly a MERGE-stage concern —
# MAGIC not this notebook's.

# COMMAND ----------

silver_restaurants = clean_restaurants(bronze_restaurants)
silver_menu_items = clean_menu_items(bronze_menu_items)
silver_customers = clean_customers(bronze_customers)

cleaned_orders = clean_orders(bronze_orders)
cleaned_order_items = clean_order_items(bronze_order_items)

silver_orders = dedup_orders(cleaned_orders)
silver_order_items = price_order_items(cleaned_order_items, silver_orders)

SILVER_TABLES = {
    "restaurants": silver_restaurants,
    "menu_items": silver_menu_items,
    "customers": silver_customers,
    "orders": silver_orders,
    "order_items": silver_order_items,
}

for name, df in SILVER_TABLES.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"workspace.silver.{name}")
    print(f"wrote workspace.silver.{name}: {df.count():,} rows")

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
# MAGIC there are expected by design, ~2%/~1%), and `customer_id` is excluded
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
# MAGIC %md
# MAGIC ## 8. Silver checks: logic assertions
# MAGIC
# MAGIC Section 7's verification covers structure (row counts, schemas, and
# MAGIC nulls introduced by casting). This section checks that the *logic* did
# MAGIC what the dbt spec says, against known expected values:
# MAGIC
# MAGIC - **orders:** dedup left exactly one row per `order_id`; `order_type`,
# MAGIC   `payment_method`, and `status` contain only their canonical values;
# MAGIC   guest orders (null `customer_id`) were preserved, not dropped.
# MAGIC - **customers:** no blank `email` or `phone` survived the trim-then-nullif
# MAGIC   cleanup.
# MAGIC - **order_items:** every line item has a `line_item_revenue`; one row is
# MAGIC   spot-checked by hand, and the revenue total is printed for comparison
# MAGIC   with the earlier PySpark notebook.
# MAGIC
# MAGIC Fixed expectations are `assert`s, so a regression fails loudly on rerun.

# COMMAND ----------


"""
Orders:

1. distinct order_id = 5,000
2. order_type values exactly {dine_in, takeout, delivery}
3. payment_method values exactly {card, cash, mobile_pay, gift_card}
4. status values exactly {completed, cancelled, refunded}
5. null customer_id count = spark.table("workspace.bronze.orders").dropDuplicates(["order_id"]).filter(col("customer_id").isNull()).count()
"""
distinct_orders = spark.table("workspace.silver.orders").select("order_id").distinct().count()
assert distinct_orders == 5000, f"orders: expected 5000 distinct order_ids, got {distinct_orders}"

def distinct_values(table, column):
    rows = spark.table(table).select(column).distinct().collect()
    return {row[column] for row in rows}

expected_order_types = {"dine_in", "takeout", "delivery"}
actual = distinct_values("workspace.silver.orders", "order_type")
assert actual == expected_order_types, f"order_type: expected {expected_order_types}, got {actual}"

expected_payment_methods = {"card", "cash", "mobile_pay", "gift_card"}
actual = distinct_values("workspace.silver.orders", "payment_method")
assert actual == expected_payment_methods, f"payment_method: expected {expected_payment_methods}, got {actual}"

expected_status = {"completed", "cancelled", "refunded"}
actual = distinct_values("workspace.silver.orders", "status")
assert actual == expected_status, f"status: expected {expected_status}, got {actual}"

expected_guests = spark.table("workspace.bronze.orders").dropDuplicates(["order_id"]).filter(col("customer_id").isNull()).count()
actual_guests = spark.table("workspace.silver.orders").filter(col("customer_id").isNull()).count()
assert expected_guests == actual_guests, f"orders: expected {expected_guests} guest orders, got {actual_guests}"

"""
Customers

1. zero blank email or phone after trimming
"""
customers = spark.table("workspace.silver.customers")
blank_emails = customers.filter(trim(col("email")) == "").count()
blank_phones = customers.filter(trim(col("phone")) == "").count()
assert blank_emails == 0, f"customers: expected 0 blank emails, got {blank_emails}"
assert blank_phones == 0, f"customers: expected 0 blank phone numbers, got {blank_phones}"

"""
Order items

1. zero null line_item_revenue
2. a one-row spot-check of quantity × unit_price
3. print sum(line_item_revenue)
"""
items = spark.table("workspace.silver.order_items")
null_revenue = items.filter(col("line_item_revenue").isNull()).count()
assert null_revenue == 0, f"order_items: expected 0 null line_item_revenue, got {null_revenue}"

mismatches = items.filter(col("quantity") * col("unit_price") != col("line_item_revenue")).count()
assert mismatches == 0, f"order_items: {mismatches} rows where quantity * unit_price != line_item_revenue"

display(items.select("order_item_id", "quantity", "unit_price", "line_item_revenue").limit(5))

total = items.agg(spark_sum("line_item_revenue")).first()[0]
print(f"Total line_item_revenue: ${total:,.2f}")

