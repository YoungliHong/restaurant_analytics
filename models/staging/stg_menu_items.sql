-- models/staging/stg_menu_items.sql

with source as (

    select * from {{ source('raw_data', 'menu_items') }}

),

renamed as (

    select
        menu_item_id,
        item_name,
        category,
        base_price::numeric(10, 2) as base_price

    from source

)

select * from renamed
