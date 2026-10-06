# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold layer: the seven dbt marts, rebuilt from silver
# MAGIC
# MAGIC Reads the written `workspace.silver.*` tables (not a re-run of the
# MAGIC silver notebook) and reproduces each dbt mart in `models/marts/*.sql`
# MAGIC exactly — same grain, same columns in the same order, same filters,
# MAGIC same target types — so every gold table reconciles row-for-row against
# MAGIC `ANALYTICS.MARTS.*` in Snowflake next.
# MAGIC
# MAGIC Silver already *is* dbt's intermediate layer: `workspace.silver.orders`
# MAGIC = `int_orders_deduped`, `workspace.silver.order_items` =
# MAGIC `int_order_items_priced`. Gold therefore never re-dedups or re-prices.
# MAGIC
# MAGIC **Decisions already made (apply to every function below):**
# MAGIC - Money columns -> `DECIMAL(38,2)`. dbt never casts mart money columns;
# MAGIC   Snowflake infers `NUMBER(38,2)` (INTEGER x NUMBER(10,2) -> NUMBER(38,2),
# MAGIC   SUM keeps the scale at precision 38). `current_base_price` is the one
# MAGIC   exception: dbt staging casts it to `NUMBER(10,2)`, so it stays `DECIMAL(10,2)`.
# MAGIC - Pass-through ids (and `hour_of_day`) -> `IntegerType`; counts and sums of
# MAGIC   integers -> `LongType` (Spark's natural `count`/`sum` result). Snowflake
# MAGIC   stores all of these as `NUMBER(38,0)`; values reconcile either way.
# MAGIC - Lineage: `_ingested_at` is dropped from every gold table. Gold rows are
# MAGIC   aggregates (or dims rebuilt from aggregates) with no single source row
# MAGIC   to point back to; lineage lives in bronze/silver.
# MAGIC - Gold dims are built first and passed into the facts as DataFrames —
# MAGIC   the Spark equivalent of dbt's `ref('dim_menu_items')`.
# MAGIC
# MAGIC **Scaffold boundary, same as silver:** setup, target schemas, the Delta
# MAGIC writes, and verification are written in full. The seven `build_*`
# MAGIC functions are signature + docstring + TODO; each stub returns an
# MAGIC *empty* DataFrame with the exact target schema, so the notebook runs top
# MAGIC to bottom before any logic exists and `printSchema` already shows the
# MAGIC types you're aiming for.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup: imports, create schema, read silver tables
# MAGIC
# MAGIC Every function the `build_*` bodies are likely to need is imported here,
# MAGIC so nothing is undefined at call time. No `Window` import: no mart uses a
# MAGIC window function (dedup already happened in silver).

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    when,
    coalesce,
    hour,
    date_trunc,
    trunc,
    to_date,
    count,
    count_distinct,
    sum as spark_sum,
    min as spark_min,
)
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

from pyspark.testing import assertSchemaEqual

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.gold")

silver_restaurants = spark.table("workspace.silver.restaurants")
silver_menu_items = spark.table("workspace.silver.menu_items")
silver_customers = spark.table("workspace.silver.customers")
silver_orders = spark.table("workspace.silver.orders")            # = int_orders_deduped
silver_order_items = spark.table("workspace.silver.order_items")  # = int_order_items_priced

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Target schemas
# MAGIC
# MAGIC One `StructType` per mart: the contract each `build_*` function must
# MAGIC produce (column order included). The stubs return empty DataFrames with
# MAGIC these schemas; once a function is filled in, its output should still
# MAGIC match — your check section can compare `df.schema` against these.

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


def _empty(schema: StructType) -> DataFrame:
    """Stub return value: zero rows, exact target schema, so downstream cells run."""
    return spark.createDataFrame([], schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimensions — SCAFFOLD ONLY
# MAGIC
# MAGIC Fill-in order: `build_dim_restaurants` and `build_dim_menu_items`
# MAGIC (select + rename), then `build_dim_customers` (an aggregate and a
# MAGIC left join). The facts in section 4 take these as inputs.

# COMMAND ----------

def build_dim_restaurants(silver_restaurants_df: DataFrame) -> DataFrame:
    """
    Mirrors models/marts/dim_restaurants.sql exactly.

    Grain: one row per restaurant (restaurant_id unique, not null — dbt test).

    Source: workspace.silver.restaurants (= stg_restaurants). Silver already
    did the `name -> restaurant_name` rename and the date cast; dbt's mart
    is a pure pass-through select. No joins, no aggregation, no filters.

    Output columns, in order (target type; Snowflake type):
      restaurant_id    IntegerType   (NUMBER(38,0))
      restaurant_name  StringType    (TEXT)
      city             StringType    (TEXT)
      state            StringType    (TEXT)
      opened_date      DateType      (DATE)

    Drop _ingested_at (gold lineage decision).
    """
    return silver_restaurants_df.select(
      col("restaurant_id"),
      col("restaurant_name"),
      col("city"),
      col("state"),
      col("opened_date")
    )


def build_dim_menu_items(silver_menu_items_df: DataFrame) -> DataFrame:
    """
    Mirrors models/marts/dim_menu_items.sql exactly.

    Grain: one row per menu item (menu_item_id unique, not null — dbt test).

    Source: workspace.silver.menu_items (= stg_menu_items). One rename:
    base_price -> current_base_price. The rename is the point — it's the
    *current* list price, not what anyone paid; revenue never uses it (see
    README "Transaction-time pricing"). No joins, no aggregation, no filters.

    Output columns, in order (target type; Snowflake type):
      menu_item_id        IntegerType      (NUMBER(38,0))
      item_name           StringType       (TEXT)
      category            StringType       (TEXT)
      current_base_price  DecimalType(10,2) (NUMBER(10,2) — dbt staging
                          casts base_price::numeric(10,2), so this one is NOT
                          the DECIMAL(38,2) money default; silver already has it)

    Drop _ingested_at.
    """
    return silver_menu_items_df.select(
      col("menu_item_id"),
      col("item_name"),
      col("category"),
      col("base_price").alias("current_base_price")
    )


def build_dim_customers(silver_customers_df: DataFrame, silver_orders_df: DataFrame) -> DataFrame:
    """
    Mirrors models/marts/dim_customers.sql exactly.

    Grain: one row per customer (customer_id unique, not null — dbt test).
    Every silver customer survives (left join), whether or not they ordered.

    Steps (from the dbt CTEs):
      1. first_orders: from silver_orders (= int_orders_deduped — deduped,
         cancelled orders INCLUDED; dbt applies no status filter here),
         WHERE customer_id IS NOT NULL, GROUP BY customer_id,
         min(order_timestamp) AS first_order_timestamp.
      2. LEFT JOIN customers c to first_orders fo ON c.customer_id = fo.customer_id.
      3. is_signup_after_first_order =
           CASE WHEN fo.first_order_timestamp IS NOT NULL
                 AND fo.first_order_timestamp < c.signup_date
                THEN true ELSE false END
         Never null (the ELSE covers customers with no orders).
         Type gotcha: this compares TIMESTAMP_NTZ to DATE. Snowflake promotes
         the DATE to midnight, so a first order on the signup day itself is
         NOT before signup -> false. Cast signup_date to timestamp_ntz
         explicitly rather than trusting Spark's implicit coercion to agree.

    Output columns, in order (target type; Snowflake type):
      customer_id                  IntegerType      (NUMBER(38,0))
      first_name                   StringType       (TEXT)
      last_name                    StringType       (TEXT)
      email                        StringType       (TEXT)
      phone                        StringType       (TEXT)
      signup_date                  DateType         (DATE)
      loyalty_tier                 StringType       (TEXT)
      first_order_timestamp        TimestampNTZType (TIMESTAMP_NTZ; null if no orders)
      is_signup_after_first_order  BooleanType      (BOOLEAN; never null)

    ~~Aliases: both inputs carry `customer_id` and `_ingested_at`, so alias
    both sides (e.g. "c" / "fo") and select qualified columns. This DataFrame
    is later joined alongside silver_orders again in fct_price_ratio_by_tier,
    so leave no stray orders-side columns on the output.~~

    Note no aliases needed: first_orders is aggregated (only customer_id and first_order_timestamp survive the groupBy) and joined on the string key, so no column is ambiguous.

    Drop _ingested_at. Reference value from the README's analysis: ~166 of
    800 customers flagged true.
    """
    first_orders = silver_orders_df \
      .filter(col("customer_id").isNotNull()) \
        .groupBy("customer_id") \
          .agg(spark_min(col("order_timestamp")).alias("first_order_timestamp"))
    joined = silver_customers_df.join(first_orders, on = "customer_id", how = "left")
    return joined.select(
      col("customer_id"),
      col("first_name"),
      col("last_name"),
      col("email"),
      col("phone"),
      col("signup_date"),
      col("loyalty_tier"),
      col("first_order_timestamp"),
      when(
        col("first_order_timestamp").isNotNull() 
        & (col("first_order_timestamp") < col("signup_date").cast("timestamp_ntz")),
        True
        ).otherwise(False).alias("is_signup_after_first_order")
      )
      

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Facts — SCAFFOLD ONLY
# MAGIC
# MAGIC Fill-in order: `build_fct_orders` (aggregate then left join), then
# MAGIC `build_fct_item_demand_by_hour` and
# MAGIC `build_fct_revenue_by_restaurant_category_month` (same three-way inner
# MAGIC join shape, different aggregation), then `build_fct_price_ratio_by_tier`
# MAGIC (the self-join).
# MAGIC
# MAGIC Filter note for every fact that has one: dbt's `status != 'cancelled'`
# MAGIC would also drop NULL statuses (NULL != x is NULL, not true). Spark's
# MAGIC `!=` behaves the same way, so a straight translation matches.

# COMMAND ----------

def build_fct_orders(silver_orders_df: DataFrame, silver_order_items_df: DataFrame) -> DataFrame:
    """
    Mirrors models/marts/fct_orders.sql exactly.

    Grain: one row per deduplicated order — ALL statuses, cancelled
    included (dbt applies no status filter). order_id unique, not null;
    restaurant_id must exist in dim_restaurants (dbt relationships test).
    Expected rows: 5,000 (= silver orders).

    Steps:
      1. order_aggregate: from silver_order_items (= int_order_items_priced),
         GROUP BY order_id:
           count(*)               AS item_count      (line rows, not units)
           sum(quantity)          AS total_quantity
           sum(line_item_revenue) AS order_total
      2. LEFT JOIN silver_orders o to order_aggregate oa ON o.order_id = oa.order_id.
         An order with no line items keeps NULL item_count/total_quantity/
         order_total — dbt does not coalesce to 0, so don't either.

    Output columns, in order (target type; Snowflake type):
      order_id         IntegerType      (NUMBER(38,0))
      restaurant_id    IntegerType      (NUMBER(38,0))
      customer_id      IntegerType      (NUMBER(38,0); nullable — guest orders)
      order_timestamp  TimestampNTZType (TIMESTAMP_NTZ)
      order_type       StringType       (TEXT)
      payment_method   StringType       (TEXT)
      status           StringType       (TEXT)
      item_count       LongType         (NUMBER(18,0) from COUNT)
      total_quantity   LongType         (NUMBER(38,0))
      order_total      DecimalType(38,2) (NUMBER(38,2); dbt doesn't cast —
                       Spark's sum over silver's DECIMAL(12,2) gives
                       DECIMAL(22,2), so cast to DECIMAL(38,2) explicitly)

    Aliases: both sides have `order_id` and `_ingested_at` — select only
    what the aggregate needs before joining, or alias both sides.

    Drop _ingested_at. Reference value: sum(order_total) should equal the
    total line_item_revenue from silver, $180,604.30 (README Spark section).
    """
    # TODO: aggregate line items per order, left join, select + cast.
    return _empty(FCT_ORDERS_SCHEMA)


def build_fct_revenue_by_restaurant_category_month(
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_menu_items_df: DataFrame,
) -> DataFrame:
    """
    Mirrors models/marts/fct_revenue_by_restaurant_category_month.sql exactly.

    Grain: one row per restaurant_id x category x month, over non-cancelled
    orders only. dbt tests: each of the three not null; the combination
    unique; singular test tests/gross_equals_net_plus_refunded.sql
    (round(gross, 2) = round(net + refunded, 2)).

    Joins (both INNER):
      order_items INNER JOIN orders     ON order_items.order_id = orders.order_id
                  INNER JOIN menu_items ON order_items.menu_item_id = menu_items.menu_item_id
      menu_items is the gold dim_menu_items DataFrame (dbt refs the mart).
    Filter: orders.status != 'cancelled'  (leaves completed + refunded).

    Row-level columns before aggregating:
      month   = date_trunc('month', orders.order_timestamp)
      revenue = order_items.quantity * order_items.unit_price
                (dbt recomputes this instead of using line_item_revenue —
                same values; Spark's product type is DECIMAL(21,2), the final
                cast below is what matters)

    Aggregation, GROUP BY restaurant_id, category, month:
      gross_revenue        = sum(revenue where status in ('completed','refunded'), else 0)
      refunded_amount      = sum(revenue where status = 'refunded', else 0)
      units_sold           = sum(quantity)
      order_count          = count(distinct order_id)
      refunded_order_count = count(distinct order_id where status = 'refunded')
                             (dbt: CASE ... THEN order_id END — no ELSE, so
                             non-refunded rows are NULL and not counted)
      net_revenue          = gross_revenue - refunded_amount  (computed after
                             the aggregate, appended as the LAST column)

    Output columns, in order (target type; Snowflake type):
      restaurant_id         IntegerType       (NUMBER(38,0))
      category              StringType        (TEXT)
      month                 TimestampNTZType  (TIMESTAMP_NTZ — a timestamp at
                            midnight on the 1st, NOT a DATE)
      gross_revenue         DecimalType(38,2) (NUMBER(38,2); dbt doesn't cast)
      refunded_amount       DecimalType(38,2) (NUMBER(38,2); dbt doesn't cast)
      units_sold            LongType          (NUMBER(38,0))
      order_count           LongType          (NUMBER(18,0))
      refunded_order_count  LongType          (NUMBER(18,0))
      net_revenue           DecimalType(38,2) (NUMBER(38,2); dbt doesn't cast)

    `month` type check: confirm with printSchema whether Spark's date_trunc
    on a TIMESTAMP_NTZ input returns NTZ or session-timezone TIMESTAMP. If it's
    the latter, `trunc(to_date(ts), 'MM')` -> cast to timestamp_ntz avoids any
    timezone path entirely. Your call; document it.

    Aliases: order_items and orders both carry `order_id` and `_ingested_at`;
    alias all three inputs and select qualified columns.
    Drop _ingested_at.
    """
    # TODO: three-way inner join, filter, row-level month/revenue, aggregate, net_revenue, cast.
    return _empty(FCT_REVENUE_SCHEMA)


def build_fct_item_demand_by_hour(
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_menu_items_df: DataFrame,
) -> DataFrame:
    """
    Mirrors models/marts/fct_item_demand_by_hour.sql exactly.

    Grain: one row per menu_item_id x hour_of_day, over non-cancelled
    orders only. dbt tests: menu_item_id and hour_of_day not null; the
    combination unique. item_name and category ride along in the GROUP BY
    (functionally dependent on menu_item_id, so they don't change the grain).

    Joins (both INNER), same shape as the revenue mart:
      order_items INNER JOIN orders     ON order_items.order_id = orders.order_id
                  INNER JOIN menu_items ON order_items.menu_item_id = menu_items.menu_item_id
      menu_items is the gold dim_menu_items DataFrame.
    Filter: orders.status != 'cancelled'.

    Row-level: hour_of_day = hour(orders.order_timestamp)  (0-23, from the NTZ
    wall-clock value — no timezone conversion should occur).

    Aggregation, GROUP BY menu_item_id, item_name, category, hour_of_day:
      units_sold  = sum(quantity)
      order_count = count(distinct order_id)

    Output columns, in order (target type; Snowflake type):
      menu_item_id  IntegerType (NUMBER(38,0))
      item_name     StringType  (TEXT)
      category      StringType  (TEXT)
      hour_of_day   IntegerType (NUMBER — extracted value, not a count)
      units_sold    LongType    (NUMBER(38,0))
      order_count   LongType    (NUMBER(18,0))

    Aliases: order_items and orders both carry `order_id` and `_ingested_at`;
    alias all three inputs and select qualified columns.
    Drop _ingested_at. Sanity reference: lunch/dinner peaks near 12 and 19
    (README Dashboard Tab 2, generator HOUR_WEIGHTS).
    """
    # TODO: three-way inner join, filter, hour_of_day, aggregate.
    return _empty(FCT_ITEM_DEMAND_SCHEMA)


def build_fct_price_ratio_by_tier(
    silver_order_items_df: DataFrame,
    silver_orders_df: DataFrame,
    dim_customers_df: DataFrame,
) -> DataFrame:
    """
    Mirrors models/marts/fct_price_ratio_by_tier.sql exactly.

    Grain: one row per loyalty_tier that has rows in BOTH periods (the final
    pre/post join is INNER — a tier missing from either period disappears).
    No dbt tests on this mart.

    Joins:
      items INNER JOIN orders    ON items.order_id = orders.order_id
            LEFT  JOIN customers ON orders.customer_id = customers.customer_id
      customers is the gold dim_customers DataFrame (dbt refs the mart).
    Filter: orders.status != 'cancelled'.

    Row-level columns:
      loyalty_tier = coalesce(customers.loyalty_tier, 'guest')
                     -> 'guest' covers null customer_id AND any customer_id
                        with no dim_customers match (left join miss)
      period       = 'post' if to_date(orders.order_timestamp) >= DATE '2024-07-01'
                     else 'pre'   (dbt: order_timestamp::date >= '2024-07-01')
      quantity     = items.quantity
      revenue      = items.quantity * items.unit_price

    agg: GROUP BY loyalty_tier, period:
      units_sold = sum(quantity), total_revenue = sum(revenue)

    pre  = agg WHERE period = 'pre'  -> loyalty_tier, pre_units,  pre_revenue
    post = agg WHERE period = 'post' -> loyalty_tier, post_units, post_revenue
    pre INNER JOIN post USING (loyalty_tier)

    Ratios: post_units::float / pre_units and post_revenue::float / pre_revenue
    -> cast the numerator to double BEFORE dividing (Snowflake FLOAT is a
    64-bit double); dividing two decimals in Spark returns a decimal, not a
    double, with different rounding.

    Output columns, in order (target type; Snowflake type):
      loyalty_tier   StringType        (TEXT)
      pre_units      LongType          (NUMBER(38,0))
      post_units     LongType          (NUMBER(38,0))
      units_ratio    DoubleType        (FLOAT)
      pre_revenue    DecimalType(38,2) (NUMBER(38,2); dbt doesn't cast)
      post_revenue   DecimalType(38,2) (NUMBER(38,2); dbt doesn't cast)
      revenue_ratio  DoubleType        (FLOAT)
    Ratios are floats: reconciliation needs a tolerance, not exact equality.

    Aliases — two places, both needed:
      1. pre and post are BOTH derived from the same `agg` DataFrame. Joining
         a DataFrame to a filter of itself is Spark's classic ambiguous
         self-join: alias both sides ("pre" / "post") and reference columns
         through the aliases.
      2. dim_customers_df was itself built partly from silver_orders, and is
         joined here alongside silver_orders; both carry customer_id.
         Alias orders and customers and select qualified columns.

    Drop _ingested_at.
    """
    # TODO: join + filter, tier/period/revenue, agg, pre/post split, self-join, ratios, cast.
    return _empty(FCT_PRICE_RATIO_SCHEMA)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Write each mart as a Delta table in workspace.gold
# MAGIC
# MAGIC `overwrite` + `overwriteSchema`, same reasoning as bronze/silver: full
# MAGIC rebuild every run, no watermark yet (that's the MERGE stage).
# MAGIC Dims are built first and handed to the facts that ref them.

# COMMAND ----------

dim_restaurants = build_dim_restaurants(silver_restaurants)
dim_menu_items = build_dim_menu_items(silver_menu_items)
dim_customers = build_dim_customers(silver_customers, silver_orders)

fct_orders = build_fct_orders(silver_orders, silver_order_items)
fct_revenue_by_restaurant_category_month = build_fct_revenue_by_restaurant_category_month(
    silver_order_items, silver_orders, dim_menu_items
)
fct_item_demand_by_hour = build_fct_item_demand_by_hour(
    silver_order_items, silver_orders, dim_menu_items
)
fct_price_ratio_by_tier = build_fct_price_ratio_by_tier(
    silver_order_items, silver_orders, dim_customers
)

GOLD_TABLES = {
    "dim_restaurants": dim_restaurants,
    "dim_menu_items": dim_menu_items,
    "dim_customers": dim_customers,
    "fct_orders": fct_orders,
    "fct_revenue_by_restaurant_category_month": fct_revenue_by_restaurant_category_month,
    "fct_item_demand_by_hour": fct_item_demand_by_hour,
    "fct_price_ratio_by_tier": fct_price_ratio_by_tier,
}

for name, df in GOLD_TABLES.items():
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"workspace.gold.{name}")
    print(f"wrote workspace.gold.{name}: {df.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verification
# MAGIC
# MAGIC Row counts and schemas, read back from the written tables. Structure
# MAGIC only — logic checks go in your own section after this one.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dimension building validation
# MAGIC
# MAGIC Expected outputs:
# MAGIC * Row counts: dim_restaurants 4, dim_menu_items 38, dim_customers 800
# MAGIC * printSchema against the docstring types, especially current_base_price as decimal(10,2), first_order_timestamp as timestamp_ntz, and is_signup_after_first_order as boolean
# MAGIC * The flag: about 166 true. Also confirm zero nulls in the flag, since the docstring says it's never null.
# MAGIC * IDs are unique, per the dbt tests: distinct customer_id = 800, distinct menu_item_id = 38, distinct restaurant_id = 4

# COMMAND ----------

# Row counts
dim_tables = {"dim_restaurants": 4, "dim_menu_items": 38, "dim_customers": 800}
for t, n in dim_tables.items():
    dim_rows = spark.table(f"workspace.gold.{t}").count()
    assert dim_rows == n, f"{t}: expected {n} rows, got {dim_rows}"

# Schema Validation
dim_tables_schemas = {"dim_restaurants": DIM_RESTAURANTS_SCHEMA, "dim_menu_items": DIM_MENU_ITEMS_SCHEMA, "dim_customers": DIM_CUSTOMERS_SCHEMA}
for t, schema in dim_tables_schemas.items():
    assertSchemaEqual(spark.table(f"workspace.gold.{t}").schema, schema)

# Flagging Validation
flagged = spark.table("workspace.gold.dim_customers").filter(col("is_signup_after_first_order")).count()
assert flagged == 166, f"dim_customers: expected 166 flagged, got {flagged}"

null_flags = spark.table("workspace.gold.dim_customers").filter(col("is_signup_after_first_order").isNull()).count()
assert null_flags == 0, f"dim_customers: expected zero null is_signup_after_first_order flags, got {null_flags}"

# Order/Item ID Uniqueness
table_ids = {"dim_customers": ("customer_id", 800), "dim_menu_items": ("menu_item_id", 38), "dim_restaurants": ("restaurant_id", 4)}
for t, (id_col, expected_rows) in table_ids.items():
    id_count = spark.table(f"workspace.gold.{t}").select(id_col).distinct().count()
    assert id_count == expected_rows, f"{t}: expected {expected_rows} unique {id_col}, got {id_count}"

# COMMAND ----------

for name in GOLD_TABLES:
    print(f"workspace.gold.{name}: {spark.table(f'workspace.gold.{name}').count():,} rows")

# COMMAND ----------

for name in GOLD_TABLES:
    print(f"--- workspace.gold.{name} ---")
    spark.table(f"workspace.gold.{name}").printSchema()