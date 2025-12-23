with orders as (
    select * from {{ ref('orders') }}
),

truck_summary as (
    select
        truck_id,
        truck_brand_name,
        primary_city,
        -- using DISTINCT because order_total is repeated on every line item 
        -- for the same order_id in your stg_orders view.
        count(distinct order_id) as total_unique_orders,
        sum(price) as total_revenue_from_items,
        avg(order_total) as average_order_value
    from orders
    group by 1, 2, 3
)

select
    *,
    case 
        when total_revenue_from_items > 500 then 'Top Tier'
        when total_revenue_from_items > 200 then 'Mid Tier'
        else 'Low Tier'
    end as performance_tier
from truck_summary
