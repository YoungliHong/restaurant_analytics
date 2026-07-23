-- models/intermediate/int_orders_deduped.sql

with stg_orders as (

    select * from {{ ref('stg_orders') }}

),

deduped as (

    select
        *,
        row_number() over (
            partition by order_id
            order by order_timestamp desc
        ) as rn

    from stg_orders

)

select
    * exclude (rn)   -- drop the helper column, keep everything else
from deduped
where rn = 1