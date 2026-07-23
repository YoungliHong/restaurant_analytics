-- models/marts/dim_customers.sql

with stg_customers as (
    select * from {{ ref ('stg_customers') }}

),
first_orders as (
    select 
        customer_id,
        min(order_timestamp) as first_order_timestamp
    from {{ ref('int_orders_deduped') }}
    where customer_id is not null
    group by customer_id

),
final as (
    select
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email, 
        c.phone,
        c.signup_date,
        c.loyalty_tier,
        fo.first_order_timestamp,
    case 
        when fo.first_order_timestamp is not null
            and fo.first_order_timestamp < c.signup_date
        then true
        else false
    end as is_signup_after_first_order

    from stg_customers c
    left join first_orders fo
        on c.customer_id = fo.customer_id

)

select * from final