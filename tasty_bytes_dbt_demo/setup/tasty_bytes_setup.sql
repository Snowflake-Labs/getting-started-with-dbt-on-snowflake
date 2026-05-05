-- =============================================================================
-- Tasty Bytes dbt Demo: Environment Setup & Source Data
-- Source: https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-getting-started-tutorial
--
-- This script sets up the complete environment for the Tasty Bytes dbt project:
--   1. Warehouse for executing workspace actions
--   2. Database and schemas for integrations and model materializations
--   3. Logging, tracing, and metrics for observability
--   4. GitHub secret and API integration for connecting to your repository
--   5. Network rule and external access integration for dbt dependencies
--   6. Source data: Tasty Bytes foundational data model (raw zone tables + data load)
--
-- NOTE: Before running this script in a workspace, comment out any CREATE statements
-- for objects you already created during the "Set up your environment" steps:
--   CREATE OR REPLACE WAREHOUSE ...
--   CREATE OR REPLACE API INTEGRATION ...
--   CREATE OR REPLACE NETWORK RULE ...
--   CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ...
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- =============================================================================
-- STEP 1: Create a warehouse for executing workspace actions
-- A dedicated warehouse assigned to your workspace helps you log, trace,
-- and identify actions initiated from within that workspace.
-- The Tasty Bytes data model is fairly large, so an XL warehouse is recommended.
-- Alternatively, you can use an existing warehouse in your account.
-- =============================================================================

CREATE WAREHOUSE tasty_bytes_dbt_wh WAREHOUSE_SIZE = XLARGE;

-- =============================================================================
-- STEP 2: Create a database and schemas for integrations and model materializations
-- The INTEGRATIONS schema stores objects Snowflake needs for GitHub integration.
-- The DEV and PROD schemas store materialized objects that your dbt project creates.
-- The RAW schema holds the Tasty Bytes foundational source data.
-- =============================================================================

CREATE DATABASE IF NOT EXISTS tasty_bytes_dbt_db;
CREATE SCHEMA IF NOT EXISTS tasty_bytes_dbt_db.dev;
CREATE SCHEMA IF NOT EXISTS tasty_bytes_dbt_db.prod;
-- Used for storing objects Snowflake needs for GitHub integration (secrets, etc.)
CREATE SCHEMA IF NOT EXISTS tasty_bytes_dbt_db.integrations;
-- Used for the Tasty Bytes foundational source data loaded from S3
CREATE SCHEMA IF NOT EXISTS tasty_bytes_dbt_db.raw;

-- =============================================================================
-- STEP 3: Enable logging, tracing, and metrics
-- You can capture logging and tracing events for a dbt project object and for
-- the task that runs it on a schedule. These settings must be applied to the
-- schemas where the dbt project object and task are deployed.
-- See: https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake-monitoring-observability
-- =============================================================================

ALTER SCHEMA tasty_bytes_dbt_db.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA tasty_bytes_dbt_db.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA tasty_bytes_dbt_db.dev SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA tasty_bytes_dbt_db.prod SET LOG_LEVEL = 'INFO';
ALTER SCHEMA tasty_bytes_dbt_db.prod SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA tasty_bytes_dbt_db.prod SET METRIC_LEVEL = 'ALL';

-- =============================================================================
-- STEP 4: Create a GitHub secret and API integration
-- Snowflake needs an API integration to interact with GitHub.
-- If your repository is private, you must also create a secret to store GitHub
-- credentials. You then reference the secret in the API integration definition
-- and when creating the workspace for your dbt project.
--
-- Creating a secret requires a personal access token for your repository.
-- See: https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens
--
-- Alternatively, your admin can set up one OAuth2 integration for the team instead of managing personal access tokens.
-- See: https://docs.snowflake.com/en/user-guide/ui-snowsight/workspaces-git
-- =============================================================================

USE tasty_bytes_dbt_db.integrations;
CREATE OR REPLACE SECRET tasty_bytes_dbt_db.integrations.tb_dbt_git_secret
  TYPE = password
  USERNAME = 'your-gh-username'
  PASSWORD = 'YOUR_PERSONAL_ACCESS_TOKEN';

-- Replace 'https://github.com/my-github-account' with the URL of the GitHub
-- account for your forked repository.
-- This API integration is used when creating a workspace in Snowsight (Projects > Workspaces)
-- to connect Snowflake to your forked GitHub repository.
CREATE OR REPLACE API INTEGRATION tb_dbt_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/my-github-account')
  -- Comment out the following line if your forked repository is public
  ALLOWED_AUTHENTICATION_SECRETS = (tasty_bytes_dbt_db.integrations.tb_dbt_git_secret)
  ENABLED = TRUE;

-- =============================================================================
-- STEP 5: (Optional) Create a network rule and external access integration
-- If you plan to run 'dbt deps' in a workspace, dbt will need to access remote
-- URLs to download dependencies (e.g. packages from the dbt Package Hub or
-- from GitHub). Most dbt projects specify dependencies in their packages.yml
-- file, which must be installed in the workspace before other commands will work.
-- See: https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access
-- =============================================================================

-- Create NETWORK RULE for external access integration
-- CREATE OR REPLACE NETWORK RULE dbt_network_rule
--   MODE = EGRESS
--   TYPE = HOST_PORT
--   -- Minimal URL allowlist that is required for dbt deps
--   VALUE_LIST = (
--     'hub.getdbt.com',
--     'codeload.github.com'
--     );

-- Create EXTERNAL ACCESS INTEGRATION for dbt access to external dbt package locations
-- CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_ext_access
--   ALLOWED_NETWORK_RULES = (dbt_network_rule)
--   ENABLED = TRUE;

-- =============================================================================
-- STEP 6: Set up source data - Tasty Bytes foundational data model
-- The dbt project uses the foundational data model for the fictitious Tasty Bytes
-- food truck brand as its source data for transformations.
-- This section creates a file format and external stage pointing to S3, builds
-- the raw zone tables, and loads data into them.
-- =============================================================================

-- File format and external stage

CREATE OR REPLACE FILE FORMAT tasty_bytes_dbt_db.public.csv_ff 
type = 'csv';

CREATE OR REPLACE STAGE tasty_bytes_dbt_db.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = tasty_bytes_dbt_db.public.csv_ff;

list @tasty_bytes_dbt_db.public.s3load;
-- =============================================================================
--  Raw zone table builds
-- =============================================================================

-- country table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.country
(
    country_id NUMBER(18,0),
    country VARCHAR(16777216),
    iso_currency VARCHAR(3),
    iso_country VARCHAR(2),
    city_id NUMBER(19,0),
    city VARCHAR(16777216),
    city_population VARCHAR(16777216)
) 
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- franchise table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.franchise 
(
    franchise_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216) 
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- location table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.location
(
    location_id NUMBER(19,0),
    placekey VARCHAR(16777216),
    location VARCHAR(16777216),
    city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    country VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- menu table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.menu
(
    menu_id NUMBER(19,0),
    menu_type_id NUMBER(38,0),
    menu_type VARCHAR(16777216),
    truck_brand_name VARCHAR(16777216),
    menu_item_id NUMBER(38,0),
    menu_item_name VARCHAR(16777216),
    item_category VARCHAR(16777216),
    item_subcategory VARCHAR(16777216),
    cost_of_goods_usd NUMBER(38,4),
    sale_price_usd NUMBER(38,4),
    menu_item_health_metrics_obj VARIANT
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- truck table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.truck
(
    truck_id NUMBER(38,0),
    menu_type_id NUMBER(38,0),
    primary_city VARCHAR(16777216),
    region VARCHAR(16777216),
    iso_region VARCHAR(16777216),
    country VARCHAR(16777216),
    iso_country_code VARCHAR(16777216),
    franchise_flag NUMBER(38,0),
    year NUMBER(38,0),
    make VARCHAR(16777216),
    model VARCHAR(16777216),
    ev_flag NUMBER(38,0),
    franchise_id NUMBER(38,0),
    truck_opening_date DATE
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- order_header table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.order_header
(
    order_id NUMBER(38,0),
    truck_id NUMBER(38,0),
    location_id FLOAT,
    customer_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    shift_id NUMBER(38,0),
    shift_start_time TIME(9),
    shift_end_time TIME(9),
    order_channel VARCHAR(16777216),
    order_ts TIMESTAMP_NTZ(9),
    served_ts VARCHAR(16777216),
    order_currency VARCHAR(3),
    order_amount NUMBER(38,4),
    order_tax_amount VARCHAR(16777216),
    order_discount_amount VARCHAR(16777216),
    order_total NUMBER(38,4)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- order_detail table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.order_detail 
(
    order_detail_id NUMBER(38,0),
    order_id NUMBER(38,0),
    menu_item_id NUMBER(38,0),
    discount_id VARCHAR(16777216),
    line_number NUMBER(38,0),
    quantity NUMBER(5,0),
    unit_price NUMBER(38,4),
    price NUMBER(38,4),
    order_item_discount_amount VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- customer_loyalty table build
CREATE OR REPLACE TABLE tasty_bytes_dbt_db.raw.customer_loyalty
(
    customer_id NUMBER(38,0),
    first_name VARCHAR(16777216),
    last_name VARCHAR(16777216),
    city VARCHAR(16777216),
    country VARCHAR(16777216),
    postal_code VARCHAR(16777216),
    preferred_language VARCHAR(16777216),
    gender VARCHAR(16777216),
    favourite_brand VARCHAR(16777216),
    marital_status VARCHAR(16777216),
    children_count VARCHAR(16777216),
    sign_up_date DATE,
    birthday_date DATE,
    e_mail VARCHAR(16777216),
    phone_number VARCHAR(16777216)
)
COMMENT = '{"origin":"sf_sit-is", "name":"tasty-bytes-dbt", "version":{"major":1, "minor":0}, "attributes":{"is_quickstart":1, "source":"sql"}}';

-- =============================================================================
--  Raw zone data loads from S3 stage
-- =============================================================================

-- country table load
COPY INTO tasty_bytes_dbt_db.raw.country
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/country/;

-- franchise table load
COPY INTO tasty_bytes_dbt_db.raw.franchise
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/franchise/;

-- location table load
COPY INTO tasty_bytes_dbt_db.raw.location
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/location/;

-- menu table load
COPY INTO tasty_bytes_dbt_db.raw.menu
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/menu/;

-- truck table load
COPY INTO tasty_bytes_dbt_db.raw.truck
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/truck/;

-- customer_loyalty table load
COPY INTO tasty_bytes_dbt_db.raw.customer_loyalty
FROM @tasty_bytes_dbt_db.public.s3load/raw_customer/customer_loyalty/;

-- order_header table load
COPY INTO tasty_bytes_dbt_db.raw.order_header
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/order_header/;

-- order_detail table load
COPY INTO tasty_bytes_dbt_db.raw.order_detail
FROM @tasty_bytes_dbt_db.public.s3load/raw_pos/order_detail/;

-- =============================================================================
-- Setup complete
-- =============================================================================

SELECT 'tasty_bytes_dbt_db setup is now complete' AS note;
