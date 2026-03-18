with customers as (
    select
        CUSTOMER_ID,
        FIRST_NAME,
        LAST_NAME
    from TASTY_BYTES_DBT_DB.DEV.RAW_CUSTOMER_CUSTOMER_LOYALTY
    -- from {{ ref('raw_customer_customer_loyalty') }}
),
orders as (
    select
        ORDER_ID,
        CUSTOMER_ID,
        ORDER_TS,
        ORDER_TOTAL
    from  TASTY_BYTES_DBT_DB.DEV.RAW_POS_ORDER_HEADER
    -- from  {{ ref('raw_pos_order_header') }}
),
customer_orders as (
    select
        CUSTOMER_ID,
        min(ORDER_TS) as FIRST_ORDER_TS,
        max(ORDER_TS) as MOST_RECENT_ORDER_TS,
        count(ORDER_ID) as NB_ORDERS,
        sum(ORDER_TOTAL) as TOTAL_REVENUE,
        avg(ORDER_TOTAL) as AVG_ORDER_VALUE
    from
        orders
    group by
        1
),
final as (
    select
        customers.CUSTOMER_ID,
        customers.FIRST_NAME,
        customers.LAST_NAME,
        customer_orders.FIRST_ORDER_TS,
        customer_orders.MOST_RECENT_ORDER_TS,
        coalesce(customer_orders.NB_ORDERS, 0) as NB_ORDERS,
        coalesce(customer_orders.TOTAL_REVENUE, 0) as TOTAL_REVENUE,
        coalesce(customer_orders.AVG_ORDER_VALUE, 0) as AVG_ORDER_VALUE
    from
        customers
        left join customer_orders using (CUSTOMER_ID)
)
select
    *
from
    final