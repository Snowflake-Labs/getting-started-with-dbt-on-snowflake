-- What tables exist?
SHOW TABLES IN SCHEMA tb_101.raw_pos;

-- What is the scale of data? 
SELECT COUNT(*) FROM tb_101.raw_pos.order_header;

-- Understand a query that might be used in a mart
SELECT 
    cl.customer_id,
    cl.city,
    cl.country,
    cl.first_name,
    cl.last_name,
    cl.phone_number,
    cl.e_mail,
    SUM(oh.order_total) AS total_sales,
    ARRAY_AGG(DISTINCT oh.location_id) AS visited_location_ids_array
FROM tb_101.raw.customer_loyalty cl
JOIN tb_101.raw.order_header oh
ON cl.customer_id = oh.customer_id
GROUP BY cl.customer_id, cl.city, cl.country, cl.first_name,
cl.last_name, cl.phone_number, cl.e_mail;




-- Create NETWORK RULE for external access integration

CREATE OR REPLACE NETWORK RULE dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  -- Minimal URL allowlist that is required for dbt deps
  VALUE_LIST = (
    'hub.getdbt.com',
    'codeload.github.com'
    );

/***  Pour la gestion de librairie  ***/

-- Create EXTERNAL ACCESS INTEGRATION for dbt access to external dbt package locations

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_ext_access
  ALLOWED_NETWORK_RULES = (dbt_network_rule)
  ENABLED = TRUE;

GRANT USAGE ON DATABASE TASTY_BYTES_DBT_DB TO ROLE DEMO_ROLE;

-- Elementary needs to create its own schema (usually named 'ELEMENTARY')
GRANT CREATE SCHEMA ON DATABASE TASTY_BYTES_DBT_DB TO ROLE DEMO_ROLE;



/***  Pour la gestion de tag snowflake  ***/

CREATE TAG TASTY_BYTES_DBT_DB.dev.cost_center;

-- 2. (Optional) Let your dbt role use this tag
GRANT APPLY ON TAG TASTY_BYTES_DBT_DB.dev.cost_center TO ROLE demo_role;
