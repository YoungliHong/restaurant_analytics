-- models/staging/stg_customers.sql

with source as (

    select * from {{ source('raw_data', 'customers') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name,
        email,
        nullif(trim(phone), '') as phone,
        signup_date::date       as signup_date,
        loyalty_tier
    from source

)

select * from renamed
