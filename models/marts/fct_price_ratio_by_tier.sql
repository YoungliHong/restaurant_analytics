with items as (
    select * from {{ ref('int_order_items_priced') }}
),

orders as (
    select * from {{ ref('int_orders_deduped') }}
),

customers as (
    select * from {{ ref('dim_customers')}}
),

joined as (
    select 
        coalesce(customers.loyalty_tier, 'guest') as loyalty_tier,
        case
            when orders.order_timestamp::date >= '2024-07-01' then 'post'
            else 'pre'
        end as period,
        items.quantity,
        items.quantity * items.unit_price as revenue,
    from items 
    inner join orders 
        on items.order_id = orders.order_id
    left join customers
        on orders.customer_id = customers.customer_id
    where orders.status != 'cancelled'
),

agg as (
    select 
        loyalty_tier,
        period,
        sum(quantity) as units_sold,
        sum(revenue) as total_revenue
    from joined
    group by loyalty_tier, period
),

pre as (
    select loyalty_tier, units_sold as pre_units, total_revenue as pre_revenue
    from agg where period = 'pre'
),
post as (
    select loyalty_tier, units_sold as post_units, total_revenue as post_revenue
    from agg where period = 'post'
)

select 
    pre.loyalty_tier,
    pre.pre_units,
    post.post_units,
    post.post_units::float / pre.pre_units as units_ratio,
    pre.pre_revenue,
    post.post_revenue,
    post.post_revenue::float / pre.pre_revenue as revenue_ratio
from pre
inner join post using (loyalty_tier)

