WITH all_metrics AS (
    SELECT *
    FROM {{ ref("customers_metrics") }}
)
, vip_customers AS (
    SELECT *
    FROM {{ ref("vip_customers") }}
) 
SELECT a.*
FROM all_metrics a
INNER JOIN vip_customers b USING (CUSTOMER_ID)
