-- What tables exist?
SHOW TABLES IN DATABASE tasty_bytes_dbt_db;

-- What is the scale of data? 
SELECT COUNT(*) FROM TASTY_BYTES_DBT_DB.RAW.ORDER_HEADER;

-- Understand a query that might be used in a mart
Select top 1000 *
from TASTY_BYTES_DBT_DB.DEV.CUSTOMER_LOYALTY_METRICS
;

