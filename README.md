## Summary

## Architecture
This was created to model the fluctuations in restaurant revenue in relation to price increases of certain menu items. I chose restaurant orders since I plan to import real POS Clover data from my parents' restaurant. The stack is: Python generator -> Snowflake (raw_data + analytics) -> dbt_core(dbt_youngli) -> PySpark/Databricks -> Git. The source data consists of 4 restaurants, 5000 orders, and 13000 line items. I intentionally injected some messiness and included the snapshot price so that I could address the transaction-time problem.



```mermaid
flowchart LR
    A["generate_restaurant_data.py<br/>(5 CSVs)"] --> B[("Snowflake RAW_DATA<br/>stage + COPY INTO")]
    B --> C["staging models<br/>(stg_*)"]
    C --> D["intermediate models<br/>(dedup, transaction-time pricing)"]
    D --> E["marts<br/>(fct_orders, dim_*)"]
    E --> F["revenue marts<br/>(fct_revenue_*, fct_item_demand_*)"]
    F --> G["Streamlit Dashboard"]
    D -.->|ported to PySpark| H["Databricks Free Edition<br/>(int_order_items_priced)"]

    style A fill:#e1f5ff,color:#000
    style B fill:#fff4e1,color:#000
    style H fill:#f0e1ff,color:#000
    style G fill:#e1ffe1,color:#000
​```


## Database design
Two separate databases for two separate purposes: raw_analytics houses the raw data, dbt_youngli contains all the derived tables. raw_analytics requires tighter access and can't be recovered easily if data gets lost while the dbt_youngli tables can be recomputed. The dbt layers for staging, intermediates, and marts share the same lifecycle: rebuildable from raw on demand and all owned by the same transformation logic.


## Data flow narrative
1. Generated initial dataset with generate_restaurant_data.py which contains 3 dims - restaurants, menu_items, and customers and 2 facts - orders (order grain) and order_items (line-item grain).

2. The generated csvs land in Snowflake via stage + COPY INTO. 

3. Define sources in _sources.yml, and then build the staging models with light cleanup only (stg_customers, stg_menu_items, stg_order_items, stg_orders, stg_restaurants).

    a. stg_customers: phone number can be missing or have trailing/leading whitespace -> nullif(trim(phone), ''). signup_date cast to date type.

    b. stg_menu_items: base_price::numeric(10,2) makes sure base_price is a valid dollar amount (max ten digits to left of decimal and two to the right)

    c. stg_order_items: 

4. Defined staging tests - unique/non-null on every key _stg_models.yml

5. Created intermediate models:
    a. int_orders_deduped removes duplicates using ROW_NUMBER() PARTITION BY order_id ORDER BY timestamp. The duplicates injected by the generation script inserts byte identical rows so tie-breaking isn't important here. We just pick the first one that shows up per unique order.
    b. int_order_items_priced captures the transaction-time price of the ordered item. Since menu_items.base_price doesn't account for the price fluctuations of the order item, we need to use order_items.unit_price which is the snapshot price. 

6. Marts 
    a. fct_orders: From int_order_items_priced, aggregate line items per order to generate order grain
    b. dim_customers: columns describing customer specific details like name, email, and phone number. We create a new flag here "is_signup_after_first_order" to signal when a customer has a first order timestamp before their signup date.
    dim_restaurants and dim_menu_items are pulled from staging tables without extra computations.

7. Revenue Marts
    a. fct_revenue_by_restaurant_category_month: this is an inner join of int_order_items_priced, int_orders_deduped, and dim_menu_items. We calculate gross revenue, and by restaurant, category, and month. 

    b. fct_item_demand_by_hour: this is the same inner join but we're instead looking at menu items and the amount they sold per order in relation to the hour of the timestamp.

8. StreamLit Dashboard
Visualize the tables from the previous part - dish demand by hour and revenue marts

streamlit run dashboard/app.py

*OPTIONAL*
9. Ported the line-item revenue transform to validate the pipeline from outside the warehouse.
[`pyspark/revenue_reconciliation.ipynb`](./pyspark/revenue_reconciliation.ipynb) - reimplements transaction-time pricing logic and reconciles the output against the dbt/Snowflake result.

## When Spark, when warehouse

This project's volume (5K orders) never needed Spark — Snowflake/dbt handled every real transformation here, and that's the honest default up to hundreds of millions of rows, where warehouse SQL gives you testing, lineage, and version control nearly for free. Phase 5's PySpark port of the line-item pricing logic was a deliberate skills exercise, not a data-driven necessity — Spark earns its complexity past single-warehouse scale, or when the transform needs distributed logic SQL can't express cleanly.# 


## Design Decisions
## Signup-after-order test note
## Honest Caveats


