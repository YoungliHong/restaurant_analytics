-- models/marts/dim_restaurants.sql

with stg_restaurants as (
    select * from {{ ref('stg_restaurants') }}
)

select 
    restaurant_id,
    restaurant_name as restaurant_name,
    city,
    state,
    opened_date

from stg_restaurants