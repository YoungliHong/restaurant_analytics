-- models/marts/fct_orders.sql

with deduped_orders as (
    select * from  {{ ref('int_orders_deduped') }}
),

priced_items as (
    select * from {{ ref('int_order_items_priced') }}
),

order_aggregate as (
    select 
        order_id,
        count(*) as item_count,
        sum(quantity) as total_quantity,
        sum(line_item_revenue) as order_total

    from priced_items
    group by order_id
)

select 
    o.order_id,
    o.restaurant_id,
    o.customer_id,
    o.order_timestamp,
    o.order_type,
    o.payment_method,
    o.status,
    oa.item_count,
    oa.total_quantity,
    oa.order_total
from deduped_orders o 
left join order_aggregate oa 
    on o.order_id = oa.order_id