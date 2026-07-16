-- models/staging/stg_orders.sql

with source as (

    select * from {{ source('raw_data', 'orders') }}

),

renamed as (

    select
        order_id,
        restaurant_id,
        customer_id,
        order_timestamp::timestamp as order_timestamp,

        -- standardize casing/whitespace injected upstream; canonical values are
        -- dine_in / takeout / delivery
        lower(trim(order_type))    as order_type,

        -- canonical values are card / cash / mobile_pay / gift_card
        case
            when lower(trim(payment_method)) = 'mobile pay' then 'mobile_pay'
            when lower(trim(payment_method)) = 'giftcard'   then 'gift_card'
            else lower(trim(payment_method))
        end                         as payment_method,

        status

    from source

)

select * from renamed
