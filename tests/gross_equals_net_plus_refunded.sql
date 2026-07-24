-- tests/gross_equals_net_plus_refunded.sql

select 
    restaurant_id,
    category,
    month,
    gross_revenue,
    net_revenue,
    refunded_amount
from {{ ref('fct_revenue_by_restaurant_category_month') }}
where round(gross_revenue, 2) != round(net_revenue + refunded_amount, 2)