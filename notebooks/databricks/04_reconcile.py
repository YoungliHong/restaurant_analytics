# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Reconcile: gold vs. the dbt marts in Snowflake
# MAGIC
# MAGIC Proves the Databricks medallion build reproduces the dbt/Snowflake
# MAGIC pipeline exactly: for each of the seven marts, every key present on one
# MAGIC side is present on the other, and every value on a matching key agrees.
# MAGIC
# MAGIC **How the Snowflake side gets here:** Free Edition can't connect out to
# MAGIC this Snowflake account, so `scripts/export_marts.py` dumps
# MAGIC `ANALYTICS.MARTS.*` to local CSVs in a fixed format (NTZ timestamps as
# MAGIC `YYYY-MM-DD HH:MM:SS`, booleans as `true`/`false`, NULL as an empty
# MAGIC field, exact decimals, round-trippable doubles), and those files are
# MAGIC uploaded to a Unity Catalog volume. Each CSV is parsed against the *same*
# MAGIC contract schema gold was built to, so both sides arrive typed identically.
# MAGIC
# MAGIC **Scaffold boundary, same as silver/gold:** setup, the reads, and the
# MAGIC summary are written in full. The seven `reconcile_*` functions are
# MAGIC signature + docstring + TODO; each stub returns `None`, which the
# MAGIC summary reports as TODO, so the notebook runs top to bottom now.
# MAGIC
# MAGIC **What every `reconcile_*` function returns:**
# MAGIC `{"gold_only": n, "snowflake_only": n, "value_mismatches": n}`
# MAGIC - `gold_only` — key rows in gold with no matching key in Snowflake
# MAGIC - `snowflake_only` — key rows in Snowflake with no matching key in gold
# MAGIC - `value_mismatches` — matching keys where any non-key column differs
# MAGIC
# MAGIC A mart passes when all three are 0.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: schema + volume, then upload the exports
# MAGIC
# MAGIC Same pattern as bronze: a managed Unity Catalog volume is the governed
# MAGIC landing spot for files before they're read. It lives in its own
# MAGIC `workspace.reconcile` schema, not `bronze`, because these files aren't
# MAGIC pipeline input — they're the *other* system's output, used only for
# MAGIC comparison.
# MAGIC
# MAGIC Run this cell first, so the volume exists. Then locally run
# MAGIC `venv/bin/python scripts/export_marts.py` from the repo root and upload
# MAGIC the seven files from `reconcile_exports/` to:
# MAGIC
# MAGIC ```
# MAGIC /Volumes/workspace/reconcile/snowflake_exports/dim_restaurants.csv
# MAGIC /Volumes/workspace/reconcile/snowflake_exports/dim_menu_items.csv
# MAGIC /Volumes/workspace/reconcile/snowflake_exports/dim_customers.csv
# MAGIC /Volumes/workspace/reconcile/snowflake_exports/fct_orders.csv
# MAGIC /Volumes/workspace/reconcile/snowflake_exports/fct_revenue_by_restaurant_category_month.csv
# MAGIC /Volumes/workspace/reconcile/snowflake_exports/fct_item_demand_by_hour.csv
# MAGIC /Volumes/workspace/reconcile/snowflake_exports/fct_price_ratio_by_tier.csv
# MAGIC ```
# MAGIC
# MAGIC (Catalog Explorer → `workspace` → `reconcile` → `snowflake_exports` →
# MAGIC **Upload to this volume** — same filenames as `MARTS` below.)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, count, abs as spark_abs
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    LongType,
    StringType,
    DateType,
    DecimalType,
    DoubleType,
    BooleanType,
    TimestampNTZType,
)
from pyspark.testing import assertDataFrameEqual, assertSchemaEqual

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.reconcile")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.reconcile.snowflake_exports")

EXPORT_DIR = "/Volumes/workspace/reconcile/snowflake_exports"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Contract schemas (copied from 03_gold.py)
# MAGIC
# MAGIC The same `StructType`s gold was built to. Copied rather than imported:
# MAGIC Databricks source-format notebooks can't import from each other without
# MAGIC `%run`, and `%run` would also execute gold's writes. If a contract
# MAGIC changes in `03_gold.py`, change it here too — the schema column in the
# MAGIC summary will flag it if you forget.

# COMMAND ----------

MONEY = DecimalType(38, 2)

DIM_RESTAURANTS_SCHEMA = StructType([
    StructField("restaurant_id", IntegerType()),
    StructField("restaurant_name", StringType()),
    StructField("city", StringType()),
    StructField("state", StringType()),
    StructField("opened_date", DateType()),
])

DIM_MENU_ITEMS_SCHEMA = StructType([
    StructField("menu_item_id", IntegerType()),
    StructField("item_name", StringType()),
    StructField("category", StringType()),
    StructField("current_base_price", DecimalType(10, 2)),
])

DIM_CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", IntegerType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("email", StringType()),
    StructField("phone", StringType()),
    StructField("signup_date", DateType()),
    StructField("loyalty_tier", StringType()),
    StructField("first_order_timestamp", TimestampNTZType()),
    StructField("is_signup_after_first_order", BooleanType()),
])

FCT_ORDERS_SCHEMA = StructType([
    StructField("order_id", IntegerType()),
    StructField("restaurant_id", IntegerType()),
    StructField("customer_id", IntegerType()),
    StructField("order_timestamp", TimestampNTZType()),
    StructField("order_type", StringType()),
    StructField("payment_method", StringType()),
    StructField("status", StringType()),
    StructField("item_count", LongType()),
    StructField("total_quantity", LongType()),
    StructField("order_total", MONEY),
])

FCT_REVENUE_SCHEMA = StructType([
    StructField("restaurant_id", IntegerType()),
    StructField("category", StringType()),
    StructField("month", TimestampNTZType()),
    StructField("gross_revenue", MONEY),
    StructField("refunded_amount", MONEY),
    StructField("units_sold", LongType()),
    StructField("order_count", LongType()),
    StructField("refunded_order_count", LongType()),
    StructField("net_revenue", MONEY),
])

FCT_ITEM_DEMAND_SCHEMA = StructType([
    StructField("menu_item_id", IntegerType()),
    StructField("item_name", StringType()),
    StructField("category", StringType()),
    StructField("hour_of_day", IntegerType()),
    StructField("units_sold", LongType()),
    StructField("order_count", LongType()),
])

FCT_PRICE_RATIO_SCHEMA = StructType([
    StructField("loyalty_tier", StringType()),
    StructField("pre_units", LongType()),
    StructField("post_units", LongType()),
    StructField("units_ratio", DoubleType()),
    StructField("pre_revenue", MONEY),
    StructField("post_revenue", MONEY),
    StructField("revenue_ratio", DoubleType()),
])

MARTS = {
    "dim_restaurants": DIM_RESTAURANTS_SCHEMA,
    "dim_menu_items": DIM_MENU_ITEMS_SCHEMA,
    "dim_customers": DIM_CUSTOMERS_SCHEMA,
    "fct_orders": FCT_ORDERS_SCHEMA,
    "fct_revenue_by_restaurant_category_month": FCT_REVENUE_SCHEMA,
    "fct_item_demand_by_hour": FCT_ITEM_DEMAND_SCHEMA,
    "fct_price_ratio_by_tier": FCT_PRICE_RATIO_SCHEMA,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read both sides
# MAGIC
# MAGIC CSV options, each chosen so a bad export fails loudly instead of
# MAGIC quietly becoming nulls that then "match":
# MAGIC - `schema=<contract>` — no inference; the export is typed exactly like gold.
# MAGIC - `enforceSchema=False` — Spark checks the CSV header names against the
# MAGIC   schema's field names, so a reordered or renamed column errors out
# MAGIC   instead of being silently read into the wrong field.
# MAGIC - `mode="FAILFAST"` — any value that won't parse to its contract type
# MAGIC   raises, rather than becoming NULL (the default `PERMISSIVE` behavior).
# MAGIC - `timestampNTZFormat` — matches the export's `YYYY-MM-DD HH:MM:SS`.
# MAGIC
# MAGIC Empty fields read as NULL (Spark's default `nullValue`), matching the
# MAGIC export's NULL encoding.

# COMMAND ----------

snowflake_marts = {
    name: (
        spark.read
        .option("header", True)
        .option("enforceSchema", False)
        .option("mode", "FAILFAST")
        .option("timestampNTZFormat", "yyyy-MM-dd HH:mm:ss")
        .option("dateFormat", "yyyy-MM-dd")
        .schema(schema)
        .csv(f"{EXPORT_DIR}/{name}.csv")
    )
    for name, schema in MARTS.items()
}

gold_marts = {name: spark.table(f"workspace.gold.{name}") for name in MARTS}

for name in MARTS:
    print(f"{name:<42} gold {gold_marts[name].count():>7,}   snowflake {snowflake_marts[name].count():>7,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Per-mart reconciliation — SCAFFOLD ONLY
# MAGIC
# MAGIC Shared rules for every function below (each docstring repeats only
# MAGIC what's specific to its mart):
# MAGIC - **Key uniqueness first.** Confirm the key is unique on *both* sides
# MAGIC   before joining; a duplicate key fans out the join and inflates
# MAGIC   `value_mismatches` (or hides a real duplicate as "matched").
# MAGIC - **Missing keys:** a `left_anti` join each way on the key columns gives
# MAGIC   `gold_only` and `snowflake_only`.
# MAGIC - **Value mismatches:** inner join on the key, then count rows where any
# MAGIC   non-key column differs. Compare with `eqNullSafe` (`<=>`), not `==`/`!=`:
# MAGIC   NULL != NULL evaluates to NULL, so a row where both sides are NULL would
# MAGIC   neither match nor mismatch, and a row where only one side is NULL would
# MAGIC   silently drop out of the mismatch count.
# MAGIC - **Aliases:** both sides have identical column names; alias them
# MAGIC   (`"g"` / `"s"`) and compare qualified columns.
# MAGIC - `assertDataFrameEqual(gold_df, snowflake_df)` is an optional final
# MAGIC   yes/no check (order-insensitive by default). It raises with a row diff
# MAGIC   on failure rather than returning these three counts, so it complements
# MAGIC   the anti-join logic — it doesn't replace it.

# COMMAND ----------

def reconcile_dim_restaurants(gold_df: DataFrame, snowflake_df: DataFrame) -> dict | None:
    """
    Key: restaurant_id.
    Exact compare: restaurant_name, city, state, opened_date.
    Tolerance: none.
    Pass: gold_only = 0, snowflake_only = 0, value_mismatches = 0
    (expected 4 rows each side).
    """
    # TODO: key-uniqueness check, anti-joins both ways, null-safe value compare.
    return None


def reconcile_dim_menu_items(gold_df: DataFrame, snowflake_df: DataFrame) -> dict | None:
    """
    Key: menu_item_id.
    Exact compare: item_name, category, current_base_price (DECIMAL(10,2) —
    compare as decimals, never via float).
    Tolerance: none.
    Pass: gold_only = 0, snowflake_only = 0, value_mismatches = 0
    (expected 38 rows each side).
    """
    # TODO: key-uniqueness check, anti-joins both ways, null-safe value compare.
    return None


def reconcile_dim_customers(gold_df: DataFrame, snowflake_df: DataFrame) -> dict | None:
    """
    Key: customer_id.
    Exact compare: first_name, last_name, email, phone, signup_date,
    loyalty_tier, first_order_timestamp, is_signup_after_first_order.
    Tolerance: none.
    Null-safe compare matters here: email, phone (trim+nullif'd in silver)
    and first_order_timestamp (customers with no orders) carry legitimate
    NULLs on both sides.
    Pass: gold_only = 0, snowflake_only = 0, value_mismatches = 0
    (expected 800 rows each side; 166 flagged true in Snowflake — including
    2 customers whose first order fell ON their signup date, which Snowflake
    flags false. Those two are the first place to look if the flag mismatches.)
    """
    # TODO: key-uniqueness check, anti-joins both ways, null-safe value compare.
    return None


def reconcile_fct_orders(gold_df: DataFrame, snowflake_df: DataFrame) -> dict | None:
    """
    Key: order_id.
    Exact compare: restaurant_id, customer_id, order_timestamp, order_type,
    payment_method, status, item_count, total_quantity, order_total.
    Tolerance: none.
    Null-safe compare matters here: customer_id is NULL for guest orders.
    Pass: gold_only = 0, snowflake_only = 0, value_mismatches = 0
    (expected 5,000 rows each side; sum(order_total) = 180,604.30 in Snowflake).
    """
    # TODO: key-uniqueness check, anti-joins both ways, null-safe value compare.
    return None


def reconcile_fct_revenue_by_restaurant_category_month(gold_df: DataFrame, snowflake_df: DataFrame) -> dict | None:
    """
    Key: restaurant_id, category, month (composite — join on all three;
    month is TIMESTAMP_NTZ on both sides, so the join is on the exact
    midnight-on-the-1st value, not a date).
    Exact compare: gross_revenue, refunded_amount, units_sold, order_count,
    refunded_order_count, net_revenue.
    Tolerance: none.
    Pass: gold_only = 0, snowflake_only = 0, value_mismatches = 0
    (expected 288 rows each side).
    """
    # TODO: key-uniqueness check, anti-joins both ways, null-safe value compare.
    return None


def reconcile_fct_item_demand_by_hour(gold_df: DataFrame, snowflake_df: DataFrame) -> dict | None:
    """
    Key: menu_item_id, hour_of_day (composite).
    Exact compare: item_name, category, units_sold, order_count.
    Tolerance: none.
    Pass: gold_only = 0, snowflake_only = 0, value_mismatches = 0
    (expected 456 rows each side). If keys are missing in a whole-hour band,
    suspect a timezone shift in hour() before anything else.
    """
    # TODO: key-uniqueness check, anti-joins both ways, null-safe value compare.
    return None


def reconcile_fct_price_ratio_by_tier(gold_df: DataFrame, snowflake_df: DataFrame) -> dict | None:
    """
    Key: loyalty_tier.
    Exact compare: pre_units, post_units, pre_revenue, post_revenue.
    Tolerance: units_ratio and revenue_ratio (DOUBLE) — absolute tolerance
    1e-9: a ratio matches when abs(gold - snowflake) <= 1e-9.
    Why absolute 1e-9: both ratios are O(1) (~0.88 to ~1.13 in Snowflake), so
    an absolute bound is effectively a relative one here; 1e-9 is ~7 orders of
    magnitude above double rounding error at that size, yet far below any
    difference that would mean real logic drift (one unit out of ~1,300 moves
    units_ratio by ~1e-3). Both engines do an IEEE-754 double division on
    identical inputs, so an exact match is likely; the tolerance exists so a
    last-bit difference in float conversion isn't reported as a logic bug.
    The ratios are never NULL (pre/post inner join; pre_units > 0), but keep
    the null-safe pattern for the exact columns anyway.
    Pass: gold_only = 0, snowflake_only = 0, value_mismatches = 0
    (expected 4 rows each side: guest, bronze, silver, gold — tier names,
    not medallion layers).
    """
    # TODO: key-uniqueness check, anti-joins both ways, exact compare on the
    # four exact columns + abs-diff <= 1e-9 on the two ratios.
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary: all seven marts
# MAGIC
# MAGIC One row per mart. **schema** checks the written gold table against
# MAGIC its contract (names, order, and types). Nullability is normalized away
# MAGIC first: whether Delta records an aggregate column as nullable is a
# MAGIC storage detail, not a contract difference, and would otherwise fail
# MAGIC the check for no meaningful reason. The Snowflake side can't drift —
# MAGIC it's parsed *with* the contract — so this column only catches gold.
# MAGIC
# MAGIC **result:** PASS when schema matches and all three counts are 0; TODO
# MAGIC when the function is still a stub; ERROR if the function raised (the
# MAGIC message prints below the table).

# COMMAND ----------

RECONCILERS = {
    "dim_restaurants": reconcile_dim_restaurants,
    "dim_menu_items": reconcile_dim_menu_items,
    "dim_customers": reconcile_dim_customers,
    "fct_orders": reconcile_fct_orders,
    "fct_revenue_by_restaurant_category_month": reconcile_fct_revenue_by_restaurant_category_month,
    "fct_item_demand_by_hour": reconcile_fct_item_demand_by_hour,
    "fct_price_ratio_by_tier": reconcile_fct_price_ratio_by_tier,
}


def _as_nullable(schema: StructType) -> StructType:
    """Same names, order, and types, with every field nullable."""
    return StructType([StructField(f.name, f.dataType, True) for f in schema.fields])


def _schema_matches(gold_df: DataFrame, contract: StructType) -> bool:
    try:
        assertSchemaEqual(_as_nullable(gold_df.schema), _as_nullable(contract))
        return True
    except AssertionError:
        return False


rows = []
errors = {}
for name, reconcile in RECONCILERS.items():
    gold_df, snowflake_df = gold_marts[name], snowflake_marts[name]
    schema_ok = _schema_matches(gold_df, MARTS[name])
    try:
        outcome = reconcile(gold_df, snowflake_df)
    except Exception as e:
        outcome = "ERROR"
        errors[name] = f"{type(e).__name__}: {str(e)[:300]}"

    if outcome is None:
        counts, result = ("-", "-", "-"), "TODO"
    elif outcome == "ERROR":
        counts, result = ("-", "-", "-"), "ERROR"
    else:
        counts = (outcome["gold_only"], outcome["snowflake_only"], outcome["value_mismatches"])
        result = "PASS" if schema_ok and all(c == 0 for c in counts) else "FAIL"

    rows.append((name, gold_df.count(), snowflake_df.count(), "ok" if schema_ok else "DIFF", *counts, result))

header = ("mart", "gold", "snowflake", "schema", "gold_only", "sf_only", "mismatches", "result")
widths = [42, 7, 9, 6, 9, 7, 10, 6]
line = "  ".join(f"{h:<{w}}" for h, w in zip(header, widths))
print(line)
print("-" * len(line))
for row in rows:
    print("  ".join(f"{str(v):<{w}}" for v, w in zip(row, widths)))

for name, message in errors.items():
    print(f"\n{name} raised -> {message}")

results = [row[-1] for row in rows]
print(f"\n{results.count('PASS')}/{len(results)} marts PASS"
      + (f", {results.count('TODO')} TODO" if "TODO" in results else ""))
