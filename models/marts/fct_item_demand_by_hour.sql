-- models/marts/fct_item_demand_by_hour.sql

{{ config(materialized='table') }}

with order_items as (
    select * from {{ ref('int_order_items_priced') }}
),

orders as (
    select * from {{ ref('int_orders_deduped') }}
),

menu_items as  (
    select * from {{ ref('dim_menu_items') }}
),

joined as (
    select
        menu_items.menu_item_id,
        menu_items.item_name,
        menu_items.category,
        hour(orders.order_timestamp) as hour_of_day,
        order_items.order_id,
        order_items.quantity 
    from order_items
    inner join orders
        on order_items.order_id = orders.order_id
    inner join menu_items
        on order_items.menu_item_id = menu_items.menu_item_id
    where orders.status != 'cancelled'
)

select 
   menu_item_id,
   item_name,
   category,
   hour_of_day,
   sum(quantity) as units_sold,
   count(distinct order_id) as order_count
from joined
group by menu_item_id, item_name, category, hour_of_day
