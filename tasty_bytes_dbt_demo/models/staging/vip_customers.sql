{{ config(
    materialized="table"
) }}

WITH vip_ids AS (
    SELECT *
    FROM {{ ref("vip_ids") }}
)
, raw_customer_customer_loyalty AS (
    SELECT *, 1/0 as une_erreur
    FROM {{ ref("raw_customer_customer_loyalty") }}
)
SELECT a.* 
FROM raw_customer_customer_loyalty a
INNER JOIN vip_ids b USING (CUSTOMER_ID)