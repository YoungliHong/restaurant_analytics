-- models/marts/dim_customers.sql

with stg_customers as (
    select * from {{ ref ('stg_customers') }}

)

select
    customer_id,
    first_name,
    last_name,
    email, 
    phone,
    signup_date,
    loyalty_tier

from stg_customers