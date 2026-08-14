-- analyses/signup_after_first_order_rate.sql

select 
    count(*) as total_customers,
    count_if(is_signup_after_first_order) as signed_up_after_first_order,
    round(100.0 * count_if(is_signup_after_first_order) / count(*), 2) as pct_signed_up_after_first_order
from {{ ref('dim_customers') }}