with source as (

    select * from {{ source('raw_data', 'restaurants') }}

),

renamed as (

    select
        restaurant_id,
        name              as restaurant_name,
        city,
        state,
        opened_date::date as opened_date

    from source

)

select * from renamed
