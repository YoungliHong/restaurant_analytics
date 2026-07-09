-- =============================================================================
-- 01_load_raw.sql
-- Phase 1.2 — Load synthetic restaurant CSVs into Snowflake RAW_DATA
--
-- Run with:  snowsql -f 01_load_raw.sql
-- (run from the repo root, so the relative file:// paths below resolve)
-- =============================================================================

USE DATABASE RAW_DATA;
USE SCHEMA RESTAURANT;
USE WAREHOUSE RESTAURANT_WH;

-- -----------------------------------------------------------------------------
-- File format + stage
-- -----------------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT csv_ff
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  NULL_IF = ('', 'NULL', 'null')
  EMPTY_FIELD_AS_NULL = TRUE;

CREATE OR REPLACE STAGE raw_stage
  FILE_FORMAT = csv_ff;

-- -----------------------------------------------------------------------------
-- Raw tables — kept as-is (no cleaning, no type coercion beyond basic typing).
-- Messy columns (order_type, payment_method, phone, email) stay VARCHAR so the
-- injected dirt survives into raw untouched — that's the point of Phase 1.
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE restaurants (
    restaurant_id   INTEGER,
    name            VARCHAR,
    city            VARCHAR,
    state           VARCHAR,
    opened_date     DATE
);

CREATE OR REPLACE TABLE menu_items (
    menu_item_id    INTEGER,
    item_name       VARCHAR,
    category        VARCHAR,
    base_price      NUMBER(10,2)
);

CREATE OR REPLACE TABLE customers (
    customer_id     INTEGER,
    first_name      VARCHAR,
    last_name       VARCHAR,
    email           VARCHAR,   -- ~2% null by design
    phone           VARCHAR,   -- ~1% blank string by design
    signup_date     DATE,
    loyalty_tier    VARCHAR
);

CREATE OR REPLACE TABLE orders (
    order_id         INTEGER,
    restaurant_id    INTEGER,
    customer_id      INTEGER,  -- nullable: ~18% guest orders
    order_timestamp  TIMESTAMP_NTZ,
    order_type       VARCHAR,  -- dirty casing/whitespace by design
    payment_method   VARCHAR,  -- dirty casing/whitespace by design
    status           VARCHAR
);

CREATE OR REPLACE TABLE order_items (
    order_item_id   INTEGER,
    order_id        INTEGER,
    menu_item_id    INTEGER,
    quantity        INTEGER,
    unit_price      NUMBER(10,2)
);

-- -----------------------------------------------------------------------------
-- PUT — stage the local CSVs (compressed, uploaded to the internal stage)
-- -----------------------------------------------------------------------------
PUT file://raw_data/restaurants.csv   @raw_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://raw_data/menu_items.csv    @raw_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://raw_data/customers.csv     @raw_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://raw_data/orders.csv        @raw_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;
PUT file://raw_data/order_items.csv   @raw_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;

-- -----------------------------------------------------------------------------
-- COPY INTO — load staged files into the raw tables
-- -----------------------------------------------------------------------------
COPY INTO restaurants  FROM @raw_stage/restaurants.csv.gz   FILE_FORMAT = csv_ff;
COPY INTO menu_items   FROM @raw_stage/menu_items.csv.gz    FILE_FORMAT = csv_ff;
COPY INTO customers    FROM @raw_stage/customers.csv.gz     FILE_FORMAT = csv_ff;
COPY INTO orders       FROM @raw_stage/orders.csv.gz        FILE_FORMAT = csv_ff;
COPY INTO order_items  FROM @raw_stage/order_items.csv.gz   FILE_FORMAT = csv_ff;

-- -----------------------------------------------------------------------------
-- Sanity check — row counts should match the generator's printed output:
--   restaurants=4  menu_items=38  customers=800  orders=5020  order_items=13088
-- -----------------------------------------------------------------------------
SELECT 'restaurants'  AS table_name, COUNT(*) AS row_count FROM restaurants
UNION ALL
SELECT 'menu_items',   COUNT(*) FROM menu_items
UNION ALL
SELECT 'customers',    COUNT(*) FROM customers
UNION ALL
SELECT 'orders',       COUNT(*) FROM orders
UNION ALL
SELECT 'order_items',  COUNT(*) FROM order_items;
