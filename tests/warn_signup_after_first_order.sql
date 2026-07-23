-- tests/warn_signup_after_first_order.sql
{{ config(severity = 'warn') }}

select *
from {{ ref('dim_customers') }}
where is_signup_after_first_order = true