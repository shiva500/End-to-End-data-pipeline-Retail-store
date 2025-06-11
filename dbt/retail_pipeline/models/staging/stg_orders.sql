{{ config(materialized='incremental', unique_key='order_id') }}

with source_data as (
  select * from {{ source('raw_orders','orders') }}
),

cleaned as (
  select
    order_id,
    order_timestamp as order_ts,
    customer_id,
    product_id,
    quantity,
    unit_price,
    initcap(location)       as location,
    product_description     as description,
    current_timestamp()     as loaded_at
  from source_data
)

select * from cleaned

{% if is_incremental() %}
  where order_ts > (select max(order_ts) from {{ this }})
{% endif %}
