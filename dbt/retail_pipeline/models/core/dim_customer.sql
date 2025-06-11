{{ config(materialized = 'table') }}

with ranked_locations as (
    select
        customer_id,
        location,
        loaded_at,
        row_number() over (
            partition by customer_id
            order by loaded_at desc
        ) as rn
    from {{ ref('stg_orders') }}
    where location is not null
)

select
    customer_id,
    location as current_location
from ranked_locations
where rn = 1
