-- models/marts/dim_menu_items.sql

with stg_menu_items as (

    select * from  {{ref ('stg_menu_items')}}

)

select 
    menu_item_id,
    item_name,
    category,
    base_price as current_base_price
from stg_menu_items