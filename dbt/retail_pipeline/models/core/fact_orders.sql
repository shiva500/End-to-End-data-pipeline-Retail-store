-- models/core/fact_orders.sql
{{ config(
    materialized = 'incremental',
    unique_key   = 'order_id'
) }}

select
  order_id,
  date_trunc('day', order_ts) as order_date,
  customer_id,
  product_id,
  quantity,
  unit_price,
  quantity * unit_price     as extended_amount
from {{ ref('stg_orders') }}

{% if is_incremental() %}
  where date_trunc('day', order_ts) > (
    select max(order_date) from {{ this }}
  )
{% endif %}
