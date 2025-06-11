-- models/marts/mart_sales_summary.sql
{{ config(materialized='table') }}

select
  d.full_date,
  count(f.order_id)     as orders_count,
  sum(f.extended_amount) as total_revenue,
  avg(f.extended_amount) as avg_order_value
from {{ ref('fact_orders') }} as f
join {{ ref('dim_date') }}    as d on f.order_date = d.full_date
group by d.full_date
order by d.full_date
