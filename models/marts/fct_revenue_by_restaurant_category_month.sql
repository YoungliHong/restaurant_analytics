-- models/marts/fct_revenue_by_restaurant_category_month.sql

{{ config(materialized='table' )}}

with order_items as (
    select * from {{ ref('int_order_items_priced') }}
),

orders as (
    select * from {{ ref('int_orders_deduped') }}
),

menu_items as (
    select * from {{ ref('dim_menu_items') }}
),

joined as (
    select 
        orders.order_id,
        orders.restaurant_id,
        menu_items.category,
        date_trunc('month', orders.order_timestamp) as month,
        orders.status,
        order_items.quantity,
        order_items.quantity * order_items.unit_price as revenue
    from order_items 
    inner join orders 
        on order_items.order_id = orders.order_id
    inner join menu_items
        on order_items.menu_item_id = menu_items.menu_item_id
    where orders.status != 'cancelled'
),

agg as (
    select 
    restaurant_id,
    category,
    month,
    sum(case when status in ('completed', 'refunded') then revenue else 0 end) as gross_revenue,
    sum(case when status = 'refunded' then revenue else 0 end) as refunded_amount,
    sum(quantity) as units_sold,
    count(distinct order_id) as order_count,
    count(distinct case when status = 'refunded' then order_id end) as refunded_order_count
from joined
group by restaurant_id, category, month
)

select 
    *, 
    gross_revenue - refunded_amount as net_revenue 
from agg 