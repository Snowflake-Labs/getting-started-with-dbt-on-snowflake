{{ config(materialized='table') }}
SELECT 
    customer_id,
    first_name,
    last_name,
    total_sales,
    CASE 
        WHEN total_sales > 500 THEN 'VIP'
        WHEN total_sales > 100 THEN 'Regular'
        ELSE 'Occasional'
    END AS loyalty_segment
FROM {{ ref('customer_loyalty_metrics') }}