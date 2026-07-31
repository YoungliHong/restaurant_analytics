## When Spark, when warehouse

This project's volume (5K orders) never needed Spark — Snowflake/dbt handled every real transformation here, and that's the honest default up to hundreds of millions of rows, where warehouse SQL gives you testing, lineage, and version control nearly for free. Phase 5's PySpark port of the line-item pricing logic was a deliberate skills exercise, not a data-driven necessity — Spark earns its complexity past single-warehouse scale, or when the transform needs distributed logic SQL can't express cleanly.# 
```mermaid
## Architecture


​```mermaid
flowchart TD
    A[generate_restaurant_data.py] -->|5 CSVs| B[(Snowflake RAW_DATA<br/>stage + COPY INTO)]
    B --> C[stg_ models<br/>staging layer]
    C --> D[int_orders_deduped<br/>int_order_items_priced]
    D --> E[fct_orders<br/>dim_restaurants / dim_menu_items / dim_customers]
    E --> F[fct_revenue_by_restaurant_category_month<br/>fct_item_demand_by_hour]
    D -.->|ported to PySpark| G[Databricks Free Edition<br/>int_order_items_priced]
    F --> H[Streamlit Dashboard]

    style A fill:#e1f5ff,color:#000
    style B fill:#fff4e1,color:#000
    style G fill:#f0e1ff,color:#000
    style H fill:#e1ffe1,color:#000
​```




