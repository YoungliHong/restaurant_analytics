# Restaurant Analytics Pipeline
## Contents
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Getting Started](#getting-started)
- [Data Model](#data-model)
- [Lineage Graph](#lineage-graph)
- [Design Decisions](#design-decisions)
- [Dashboard](#dashboard)
- [Databricks Medallion Extension](#databricks-medallion-extension)
- [Testing](#testing)
- [Future Work](#future-work)
  
## Overview

A dbt + Snowflake pipeline built on synthetic restaurant POS data, modeling transaction-time pricing, deduplication, and staging cleanup.

A second implementation on Databricks rebuilds the same seven marts as a bronze/silver/gold Delta pipeline, loads new batches incrementally with Delta MERGE, and is reconciled row for row against the Snowflake marts. See [Databricks medallion extension](#databricks-medallion-extension).

Built to practice pipeline design patterns end to end: idempotent incremental loads, cross-engine reconciliation, and tests for each behavior.

## Architecture

```mermaid
flowchart LR
    A["generate_restaurant_data.py<br/>(5 CSVs)"] --> B[("Snowflake RAW_DATA<br/>stage + COPY INTO")]
    B --> C["staging models<br/>(stg_*)"]
    C --> D["intermediate models<br/>(dedup, transaction-time pricing)"]
    D --> E["marts<br/>(fct_orders, dim_*)"]
    D --> F["revenue marts<br/>(fct_revenue_*, fct_item_demand_*)"]
    E --> G["Streamlit Dashboard"]
    F --> G

    style A fill:#e1f5ff,color:#000
    style B fill:#fff4e1,color:#000
    style G fill:#e1ffe1,color:#000
```

**Databricks medallion extension**

```mermaid
flowchart LR
    A["generate_restaurant_data.py<br/>(same 5 CSVs)"] --> V[("Unity Catalog volume<br/>one folder per table")]
    subgraph JOB["Databricks Job: restaurant_analytics_medallion"]
        direction LR
        B["bronze<br/>(append new files)"] --> S["silver<br/>(MERGE orders + line items,<br/>overwrite dims)"]
        S --> G["gold<br/>(7 marts, overwrite)"]
        G --> R{"reconcile<br/>(fails unless 7/7 match)"}
    end
    V --> B
    M[("Snowflake<br/>ANALYTICS.MARTS")] --> X["scripts/export_marts.py"]
    X -->|manual upload| E[("CSV exports<br/>(reconcile volume)")]
    E --> R

    style A fill:#e1f5ff,color:#000
    style V fill:#fff4e1,color:#000
    style M fill:#fff4e1,color:#000
    style E fill:#fff4e1,color:#000
    style JOB fill:#f0e1ff,color:#000
```

**Stack flow**
1. Python generator → Snowflake `RAW_DATA` (stage + COPY INTO)

2. dbt: staging views → intermediate views → marts tables (Snowflake `ANALYTICS`)

3. Streamlit dashboard consumes the intermediates and marts.

    **Parallel:** a Databricks medallion build (bronze → silver → gold) rebuilds the same seven marts from the same CSVs, loads new batches incrementally with Delta MERGE, and is reconciled against the Snowflake marts. See [Databricks Medallion Extension](#databricks-medallion-extension).

## Repository Structure

```
restaurant_analytics/
├── models/
│   ├── staging/                      # 1:1 with raw sources, light cleanup, views
│   │   └── _sources.md               # raw source contract: grain, PK, messiness
│   ├── intermediate/                 # dedup, transaction-time pricing, views
│   └── marts/                        # fact/dim tables, dashboard-facing
├── macros/
│   ├── generate_alias_name.sql       # vw_ prefix for view-materialized models
│   └── generate_schema_name.sql      # maps models to staging/intermediate/marts schemas
├── analyses/                         # ad-hoc queries, not part of the DAG
│   └── signup_after_first_order_rate.sql
├── notebooks/
│   ├── int_order_items_priced.ipynb  # original PySpark port (total-revenue check, writes no tables)
│   └── databricks/                   # medallion extension, Databricks source format
│       ├── 01_bronze.py              # append new files from the volume, skip loaded ones
│       ├── 02_silver.py              # clean like dbt staging, dedup, MERGE
│       ├── 03_gold.py                # rebuild the seven marts
│       ├── 04_reconcile.py           # key-level diff vs Snowflake exports; fails unless 7/7
│       ├── 05_merge_test.py          # batch-2 scenarios, rerun, restore + reconcile
│       └── job.yml                   # Databricks Job: bronze → silver → gold → reconcile
├── scripts/
│   └── export_marts.py               # exports the seven Snowflake marts to CSV for reconcile
├── tests/                            # custom singular tests
├── sql/                              # raw stage + COPY INTO scripts (pre-dbt)
├── dashboard/
│   └── app.py                        # Streamlit app
├── docs/
│   └── databricks.md                 # medallion flow, reconciliation, design decisions, known gaps
├── generate_restaurant_data.py       # synthetic source data generator
├── dbt_project.yml
├── packages.yml
├── requirements.txt
├── sample.profiles.yml               # dbt connection template 
├── sample.secrets.toml               # Streamlit connection template
└── README.md
```

## Getting Started

### Prerequisites

- Python 3.9+
- A Snowflake account with a warehouse, and a role with create/select privileges on two databases (`RAW_DATA`, `ANALYTICS`)
- dbt Core (dbt-snowflake adapter)
- Databricks account (Free Edition works) - only needed for the Databricks medallion extension

### Setup

1. Clone the repo and create a virtual environment:  
    ```bash
    git clone https://github.com/YoungliHong/restaurant_analytics
    cd restaurant_analytics
    python -m venv venv
    source venv/bin/activate
    ```

2. Install Python dependencies:
    ```bash
    pip install -r requirements.txt
    ```

3. Generate a key pair for Snowflake authentication:
    ```bash
    openssl genrsa -out snowflake_key.p8 2048
    openssl rsa -in snowflake_key.p8 -pubout -out snowflake_key.pub
    ```
    Register the public key on your Snowflake user. 
    (`ALTER USER <user> SET RSA_PUBLIC_KEY = <'contents of .pub file'>;`)

4. Configure your dbt Snowflake connection in `~/.dbt/profiles.yml` (see `sample.profiles.yml` for expected structure)

5. Generate synthetic source data:
    ```bash
    python generate_restaurant_data.py
    ```

6. Load raw CSVs into Snowflake (stage + COPY INTO  - see `sql/01_load_raw.sql`)

7. Run dbt:
    ```bash
    dbt run
    dbt test
    ```

8. Configure Streamlit's Snowflake connection in `.streamlit/secrets.toml` (see `sample.secrets.toml`)

### Running the Databricks extension

Run this after the dbt setup above, since reconcile compares against the dbt marts in Snowflake.

1. **Load batch 1 into the volume.** Run the first cell of `01_bronze` to create the volume, then upload the five CSVs from `raw_data/` into one folder per table under `/Volumes/workspace/bronze/raw_files/` (for example `orders/orders.csv`).
2. **Export the Snowflake marts.** Run `python scripts/export_marts.py`, which writes seven CSVs to `reconcile_exports/` (gitignored) using your dbt profile's key-pair credentials. Run the first cell of `04_reconcile` to create its volume, then upload the CSVs to `/Volumes/workspace/reconcile/snowflake_exports/`.
3. **Run the pipeline:** either the Job, which was created in the Databricks UI with its definition exported to `notebooks/databricks/job.yml`, or `01_bronze` → `02_silver` → `03_gold` → `04_reconcile` by hand, which needs the repo added to the workspace as a Git folder. Reconcile fails unless all seven marts match.
4. **Run the merge test (optional):** `05_merge_test` writes a small second batch, checks the inserts and updates, reruns it, then restores every table to batch 1 and reconciles again.

`01_bronze` has a one-time `RESET` cell, off by default. It's only needed when moving from bronze tables built by the earlier overwrite version of the notebook; a fresh workspace doesn't need it.

## Data Model

### Raw Sources

 Five CSVs land in `RAW_DATA`, similar to what would be seen in a POS extract: two small reference tables (`restaurants` and `menu_items`), a growing customer dimension (`customers`), and two fact tables (`orders` and `order_items`).  The full grain, primary keys, and known source messiness is documented in `models/staging/_sources.md`. 

### Entity Relationship Diagram

```mermaid
erDiagram
  DIM_CUSTOMERS ||--o{ FCT_ORDERS : places
  DIM_RESTAURANTS ||--o{ FCT_ORDERS : hosts
  DIM_RESTAURANTS ||--o{ FCT_REVENUE_BY_RESTAURANT_CATEGORY_MONTH : rolls_up_to
  DIM_MENU_ITEMS ||--o{ FCT_ITEM_DEMAND_BY_HOUR : rolls_up_to
  DIM_CUSTOMERS ||--o{ FCT_PRICE_RATIO_BY_TIER : groups_by_tier

  DIM_CUSTOMERS {
    number customer_id PK  
    text first_name
    text last_name
    text email
    text phone
    date signup_date
    text loyalty_tier
    timestamp first_order_timestamp
    boolean is_signup_after_first_order
  }
  DIM_MENU_ITEMS {
    number menu_item_id PK
    text item_name
    text category
    number current_base_price
  }
  DIM_RESTAURANTS {
    number restaurant_id PK
    text restaurant_name
    text city
    text state
    date opened_date
  }
  FCT_ORDERS {
    number order_id PK
    number restaurant_id FK
    number customer_id FK
    timestamp order_timestamp
    text order_type
    text payment_method
    text status
    number item_count
    number total_quantity
    number order_total
  }
  FCT_ITEM_DEMAND_BY_HOUR {
    number menu_item_id FK
    text item_name
    text category
    number hour_of_day
    number units_sold
    number order_count
  }
  FCT_REVENUE_BY_RESTAURANT_CATEGORY_MONTH {
    number restaurant_id FK
    text category
    timestamp month
    number gross_revenue
    number refunded_amount
    number units_sold
    number order_count
    number refunded_order_count
    number net_revenue
  }
  FCT_PRICE_RATIO_BY_TIER {
    text loyalty_tier
    number pre_units
    number post_units
    float units_ratio
    number pre_revenue
    number post_revenue
    float revenue_ratio
  }
```
### Staging → Intermediate → Marts 

**Staging** - one model per source, same grain as the source table (1:1, no joins or aggregation). Light cleanup only: casing/whitespace normalization, null coercion. Materialized as views, staging feeds both intermediate (for fact-table logic requiring dedup/pricing) and marts directly (for dimension tables with no cardinality-changing transforms needed).

**Intermediates** - logic that changes cardinality or is shared by multiple downstream models: order deduplication (many-to-one -> one-to-one per order_id), transaction-time price application.  Materialized as views (queried directly by marts and dashboard - see Design Decisions). 

**Marts** - final, business-facing models. Fact tables (`fct_orders`, `fct_revenue_by_restaurant_category_month`, etc.) at event/measurement grain, dimension tables (`dim_customers`, `dim_restaurants`, `dim_menu_items`) with descriptive attributes, joined star-schema style. Materialized as tables, queried by the dashboard. 

## Lineage Graph

```mermaid
graph LR
    subgraph Staging
        stg_orders
        stg_order_items
        stg_customers
        stg_menu_items
        stg_restaurants
    end

    subgraph Intermediate
        int_orders_deduped
        int_order_items_priced
    end

    subgraph Marts
        dim_customers
        dim_menu_items
        dim_restaurants
        fct_orders
        fct_item_demand_by_hour
        fct_price_ratio_by_tier
        fct_revenue_by_restaurant_category_month
    end

    stg_orders --> int_orders_deduped
    stg_order_items --> int_order_items_priced
    int_orders_deduped --> int_order_items_priced

    int_orders_deduped --> dim_customers
    stg_customers --> dim_customers
    stg_menu_items --> dim_menu_items
    stg_restaurants --> dim_restaurants

    int_order_items_priced --> fct_orders
    int_orders_deduped --> fct_orders

    dim_menu_items --> fct_item_demand_by_hour
    int_order_items_priced --> fct_item_demand_by_hour
    int_orders_deduped --> fct_item_demand_by_hour

    dim_customers --> fct_price_ratio_by_tier
    int_order_items_priced --> fct_price_ratio_by_tier
    int_orders_deduped --> fct_price_ratio_by_tier

    dim_menu_items --> fct_revenue_by_restaurant_category_month
    int_order_items_priced --> fct_revenue_by_restaurant_category_month
    int_orders_deduped --> fct_revenue_by_restaurant_category_month
```
*Note: Graph is rendered via Mermaid from `manifest.json`'s `parent_map`, since `dbt docs generate` isn't fully supported under the dbt Fusion engine (alpha) used here.*


## Design Decisions
### Two-database architecture (RAW_DATA vs ANALYTICS)

The decision to use two databases was made by considering blast radius and contract boundaries. Isolating the source tables in a separate database means we can easily rebuild our model without regenerating the source data, while simultaneously supporting the principle of least privilege. 
    
### Views vs. tables by layer (and why intermediate deviates from ephemeral)

Intermediates use views rather than dbt's more common ephemeral default, because the dashboard queries both intermediate views for the price-pass through tab. Ephemeral models don't exist as queryable objects outside of dbt's own compile graph. They only exist as inlined CTEs within whatever references them.


### Transaction-time pricing (unit_price snapshot vs. base_price)

Revenue calculations use `order_items.unit_price` rather than joining to `menu_items.base_price` because the former yields the transaction time price snapshot. The base price table only holds the current state price and using that would silently overstate the historical revenue before the price increase. The third tab of the dashboard validates this, we see that the transaction time pricing accurately reflects ~1.08 average realized price ratio which is consistent with the price increase.
  
### Order deduplication (ROW_NUMBER, fan-out risk, test placement)

Deduping runs before joining to line-item order, post join the revenue would be double counted for every duplicate row. We choose one of the duplicates to keep, since they're byte identical the one we pick isn't important -> ` ROW_NUMBER() PARTITION BY order_id ORDER BY order_timestamp`
 
Note: Ordering by the timestamp here doesn't matter nor does it guarantee it takes the first entry since they're byte identical, we use it to just deterministically pick one.

Honest Caveats: This dedup logic assumes byte identical duplicates which won't always be the case in a production scenario - e.g. retried submissions or sync conflicts. In those cases we would need to consider similar entries with slightly differing fields (duplicate landing across a price change boundary). It's not currently addressed in this implementation.

### Signup-after-first-order handling

The generator intentionally produces some customers with a signup_date after their first order — a plausible real-world pattern (a walk-in customer joining loyalty program after their first visit), not corrupted data. An early version of this check was a dbt test asserting `signup_date <= first_order_date`. In practice, `analyses/signup_after_first_order_rate.sql` shows this holds for ~21% of customers (166/800) - too high a rate to be noise, and not actually invalid. A test expected to fail on one in five rows isn't a meaningful test, so the check was moved to an analysis reporting the rate directly, and a boolean flag (`is_signup_after_first_order`) was added to `dim_customers` so the pattern is queryable rather than either silently ignored or treated as a pipeline failure.

### Naming convention: model name (layer) vs. alias (materialization)

Model file names reflect DAG layer (`stg_`/`int_`/`fct_`/`dim_`), while a macro override adds a `vw_` prefix only to the physical Snowflake object for view-materialized models. Keeping the these separate means changing the model's materialization later doesn't require renaming the file or updating any `ref()` calls - file identity and physical object identity are deliberately decoupled.

## Dashboard

**Launch the dashboard**:
```bash
streamlit run dashboard/app.py
```


### Tab 1: Revenue by Restaurant / Category / Month 
This is created by `fct_revenue_by_restaurant_category_month` with a join with `dim_restaurants` to get the restaurant names. 

Two available filters: restaurant name and category 

Based on selection of previous filters, we group by/aggregate the revenue (gross, net, refunded) per month. Displayed as a bar chart.

### Tab 2: Revenue by hour of day

This is created from `fct_item_demand_by_hour` and reads units sold vs hour of day with a filter for category. The user should see two distinct spikes each day at around 12 pm (lunch) and 7 pm (dinner), a quick sanity check in the generate_restaurant_data.py script confirms this should be the case (see HOUR_WEIGHTS).

`HOUR_WEIGHTS = {11: 6, 12: 12, 13: 11, 14: 6, 15: 3, 16: 4,
                17: 8, 18: 13, 19: 14, 20: 11, 21: 7, 22: 4}`

### Tab 3: Price-pass through

Given we know the price of menu items increased on July 1, 2024, we want to determine if that increase actually showed up in realized revenue for each item or if it resulted in mix shift. 

To do so we start with `vw_int_order_items_priced`, which is on item-level grain and inner join with `vw_int_orders_deduped` to get the individual timestamp each item was ordered. Then we can use the item's order timestamp to signal which period it belongs to (pre or post). It's also important to use the order's status here to filter out items ordered under cancelled orders. 

From this order item grain dataset, we can now aggregate each item's pre vs post revenue along with the total quantity ordered. The average realized price for each item's period can be computed as the quotient of the total revenue by the total quantity ordered.

From the result of the previous aggregation, we have each menu-item's pre and post price spike average realized prices so the last step is to coalesce back into item-grain. Price ratio is computed here as $\frac{pre.avg\_realized\_price}{post.avg\_realized\_price}$. Similarly the units ratio is $\frac{pre.units\_sold}{post.units\_sold}$.

For this dataset, the price ratio agrees with the price increase of around ~1.08 for all items. Answering our initial question of whether the increase would be observed in the revenue or mix shift.



## Databricks Medallion Extension
The seven dbt marts, rebuilt as a bronze/silver/gold Delta pipeline on Databricks Free Edition (serverless, Unity Catalog) with incremental loading added. It reads the same CSVs that were loaded into Snowflake. Every mart has a dbt counterpart, and on that batch-1 data gold must reproduce the dbt output exactly. Code is in `notebooks/databricks/`.

| Layer | Notebook | What it does | dbt counterpart | Write |
|---|---|---|---|---|
| Bronze | `01_bronze` | Lands each source file as-is (all strings), tagged with `_ingested_at` and `_source_file`; skips files already loaded | sources (`RAW_DATA`) | append |
| Silver | `02_silver` | Casts and cleans like dbt staging, dedups (latest load wins), prices line items at transaction-time `unit_price` | staging + intermediate | MERGE (orders, line items); overwrite (dims) |
| Gold | `03_gold` | Builds the seven marts from silver | marts | overwrite |
| Reconcile | `04_reconcile` | Compares each gold mart to its Snowflake export by key; fails the run unless all seven match | — | read-only |

Dimension files are complete snapshots, so silver keeps only the newest one and overwrites; a MERGE would add nothing there. A Databricks Job runs the four notebooks in order, Git-sourced, on serverless compute, triggered manually.

**Testing** 
- **Batch-1 regression:** the full pipeline on the original data reconciles 7/7 against Snowflake.
- **Batch-2** (`05_merge_test`)**:** a small hand-made batch where each row targets one behavior: real corrections, a cosmetic-only re-delivery, an exact repeat, new orders (including a guest order with dirty values), and a duplicate within a batch. Every row's expected outcome is asserted.
- **Rerun:** loading batch 2 again is a no-op: bronze doesn't grow, and the MERGE inserts 0 and updates 0.
- **Restore:** tables are rolled back to their pre-batch-2 Delta versions, gold is rebuilt, and reconcile is still 7/7.

Spark earns its keep when the data volume exceeds single-node or warehouse-scale efficiency (multi-TB, hundreds of millions+ rows), for transformations that don't map cleanly to SQL (custom Python/Scala logic), or for streaming/real-time event processing. For this project's data, a single warehouse covers both memory and compute needs, so dbt + Snowflake remains the right primary choice; the Databricks build exists to practice the medallion and incremental patterns, not because the data requires it.

Step-by-step flow, reconciliation details, design decisions, and known gaps: [docs/databricks.md](docs/databricks.md)

## Testing

Testing is done against the outputs at each stage of the pipeline: 

**Staging**  - Staging model tests are defined in `models/staging/_stg_models.yml`. `not_null` on all primary keys, `accepted_values` on cleaned categorical columns (`order_type`, `payment_method`), `unique` on all primary keys except `stg_orders`. The uniqueness test is omitted for `stg_orders` because the data generator injects 0.4% of orders as duplicates, meaning the test would fail inevitably by design.

**Intermediate** (`models/intermediate/_int_models.yml`) - `unique` + `not_null` on `order_id` in `int_orders_deduped`, asserting the dedup logic actually produced one row per order. `int_order_items_priced.order_id` is tested with `relationships` against `int_orders_deduped`, confirming every line item resolves to a real, deduplicated order.

**Marts** (`models/marts/_marts.yml`) - `unique` + `not_null` on every dimension key and on `fct_orders.order_id`; `dbt_utils.unique_combination_of_columns` on the revenue and item-demand marts' grain; and a `relationships` test catching any `fct_orders` row whose `restaurant_id` has no matching `dim_restaurants` row. A singular test (`tests/gross_equals_net_plus_refunded.sql`) checks that gross revenue = net revenue + refunded.


Run all tests:
```bash
dbt test
```

Run tests for a single model:
```bash 
dbt test --select stg_orders
```

The Databricks pipeline has its own tests (batch-1 regression, incremental merge scenarios, rerun idempotency, restore + reconcile); see [Databricks Medallion Extension](#databricks-medallion-extension).

## Future Work
- **Phase 7 (real POS data)** - swapping in a real extract from family restaurant's POS system, mapped onto the existing staging layer. Deliberately scoped tight: one or two genuine insights, not a second project.

- **Incremental materialization in dbt** - the dbt models fully refresh on every run, which is fine at this data volume. Would reach for `incremental` materializations at a scale where reprocessing full history becomes the actual bottleneck. (The Databricks extension already loads incrementally with MERGE.)

- **Full-refresh equivalence** - verify gold built on incremental silver: apply batch 2 to the raw data, rebuild the marts in dbt on Snowflake, and reconcile post-batch-2 gold against them (known gap 3 in [docs/databricks.md](docs/databricks.md#known-gaps)).

- **Scheduled trigger** - the Job runs manually. A schedule makes sense once ongoing runs are gated on invariant checks rather than the fixed batch-1 reconcile (known gap 6).

- **Auto Loader / Change Data Feed** - Auto Loader to discover new files in place of bronze's path anti-join at larger scale; Change Data Feed so downstream layers read only the rows that changed instead of whole tables.

- **Real-time/streaming-ingestion** - the current pipeline is batch, matching how POS data is actually available (periodic exports, not a live event feed). Streaming would only be warranted if a future requirement needed sub-minute latency on live order data. 







