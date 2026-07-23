-- models/intermediate/int_order_items_prices.sql

with order_items as (
    select * from {{ ref('stg_order_items') }}
),

deduped_orders as (
    select * from {{ ref('int_orders_deduped') }}
),

priced as (
    select 
        oi.order_item_id,
        oi.order_id,
        oi.menu_item_id,
        oi.quantity,
        oi.unit_price,
        oi.quantity * oi.unit_price as line_item_revenue
    
    from order_items oi
    inner join deduped_orders o
        on  oi.order_id = o.order_id

)

select * from priced