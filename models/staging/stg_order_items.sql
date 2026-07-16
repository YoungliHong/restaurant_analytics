-- models/staging/stg_order_items.sql

with source as (

    select * from {{ source('raw_data', 'order_items') }}

),

renamed as (

    select
        order_item_id,
        order_id,
        menu_item_id,
        quantity,
        unit_price::numeric(10, 2) as unit_price
    from source

)

select * from renamed
