{{ config(materialized = 'table') }}

with dates as (
  select distinct date_trunc('day', order_ts) as full_date
  from {{ ref('stg_orders') }}
)

select
  full_date,
  day(full_date)       as day_of_month,
  month(full_date)     as month,
  year(full_date)      as year,
  dayofweek(full_date) as weekday
from dates
