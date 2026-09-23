# Restaurant Analytics Pipeline

## Overview

A dbt + Snowflake + PySpark pipeline built on synthetic restaurant POS data, modeling transaction-time pricing, dedup, and staging cleanup the way a real operational system would.

Built to demonstrate production-grade pipeline design outside a vendor/staffing environment.

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
    D -.->|ported to PySpark| H["Databricks Free Edition<br/>(int_order_items_priced)"]

    style A fill:#e1f5ff,color:#000
    style B fill:#fff4e1,color:#000
    style H fill:#f0e1ff,color:#000
    style G fill:#e1ffe1,color:#000
```

**Stack flow**
1. Python generator → Snowflake `RAW_DATA` (stage + COPY INTO)

2. dbt: staging views → intermediate views → marts tables (Snowflake `ANALYTICS`)

3. Streamlit dashboard consumes the intermediates and marts.

    **Parallel:** PySpark/Databricks reimplements the line-item pricing transform independently, reconciled against the dbt/SQL output.

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
│   └── int_order_items_priced.ipynb  # PySpark port, Databricks Free Edition
├── tests/                            # custom singular tests
├── sql/                              # raw stage + COPY INTO scripts (pre-dbt)
├── dashboard/
│   └── app.py                        # Streamlit app
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

- Python 3.10+ 
- A Snowflake account with a warehouse, and a role with create/select privileges on two databases (`RAW_DATA`, `ANALYTICS`)
- dbt Core (dbt-snowflake adapter)
- Databricks account (Free Edition works) - only needed for PySpark layer

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

6. Load raw CSVs into Snowflake (stage + COPY INTO  - see `sql/load_raw.sql`)

7. Run dbt:
    ```bash
    dbt run
    dbt test
    ```

8. Configure Streamlit's Snowflake connection in `.streamlit/secrets.toml` (see `sample.secrets.toml`)

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
*Note: Graph is rendered via Mermaid from `manifest.json`'s `parent_map`, since `dbt docs generate` isn't fully supported under dbt-core 2.0.0a2 (alpha), used here.*


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

The generator intentionally produces some customers with a signup_date after their first order — a plausible real-world pattern (a walk-in customer joining loyalty program after their first visit), not corrupted data. An early version of this check was a dbt test asserting `signup_date <= first_order_date`. In practice, `analyses/signup_after_first_order_rate.sql` shows this holds for ~21% of customers (166/800) - too high a rate to be noise, and not actually invalid. A test expected to fail on one in five rows isn't a meaningful test, so the check was moved to an analysis reporting the rate directly, and a boolean flag (`signup_after_first_order`) was added to `dim_customers` so the pattern is queryable rather than either silently ignored or treated as a pipeline failure.

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



## Spark / Databricks Layer

`int_order_items_priced` was ported to PySpark and run on Databricks Free Edition (serverless), not out of a data-driven necessity, but to validate the transform in a distributed compute context and to get hands-on experience with distributed computing systems outside of Cosmos/SCOPE (Microsoft's internal big data framework). While the two share the same underlying distributed computing principles, Cosmos and SCOPE are optimized for ultra large batch workloads. The output matched the dbt/Snowflake version exactly ($180,604.30).

Spark earns its keep when the data volume exceeds single-node or warehouse-scale efficiency (multi-TB, hundreds of millions+ rows), transformations that don't map cleanly to SQL (custom Python/Scala logic), or for streaming/real-time event processing pipelines. For this project, a single warehouse satisfies both memory and computation needs so it remains the correct option.


## Testing

Testing is done against the outputs at each stage of the pipeline: 

**Staging**  - Staging model tests are defined in `models/staging/_stg_models.yml`. `not_null` on all primary keys, `accepted_values` on cleaned categorical columns (`order_type`, `payment_method`), `unique` on all primary keys except `stg_orders`. The uniqueness test is omitted for `stg_orders` because the data generator injects 0.4% of orders as duplicates, meaning the test would fail inevitably by design.

**Intermediate** (`models/intermediate/_int_models.yml`) - `unique` + `not_null` on `order_id` in `int_orders_deduped`, asserting the dedup logic actually produced one row per order. `int_order_items_priced.order_id` is tested with `relationships` against `int_orders_deduped`, confirming every line item resolves to a real, deduplicated order.

**Marts** (`models/marts/_marts.yml`) - `relationships` tests linking facts to their dimensions (e.g. `fct_revenue_by_restaurant_category_month.restaurant_id` -> `dim_restaurants.restaurant_id`), catching any join that would silently drop or orphan rows. 

Run all tests:
```bash
dbt test
```

Run tests for a single model:
```bash 
dbt test --select stg_orders
```


## Future Work
- **Phase 7 (real POS data)** - swapping in a real extract from family restaurant's POS system, mapped onto the existing staging layer. Deliberately scoped tight: one or two genuine insights, not a second project.

- **Incremental materialization** - the current models fully refresh on every run, which is fine at this data volume. Would reach for `incremental` materializations at a scale where reprocessing full history becomes the actual bottleneck.

- **Real-time/streaming-ingestion** - the current pipeline is batch, matching how POS data is actually available (periodic exports, not a live event feed). Streaming would only be warranted if a future requirement needed sub-minute latency on live order data. 







