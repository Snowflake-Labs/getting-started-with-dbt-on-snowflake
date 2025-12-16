USE ROLE accountadmin;

CREATE OR REPLACE WAREHOUSE tasty_bytes_dbt_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE
    COMMENT = 'warehouse for tasty bytes dbt demo';

USE WAREHOUSE tasty_bytes_dbt_wh;

CREATE DATABASE IF NOT EXISTS tasty_bytes_dbt_db;
CREATE OR REPLACE SCHEMA tasty_bytes_dbt_db.raw;
CREATE OR REPLACE SCHEMA tasty_bytes_dbt_db.dev;
CREATE OR REPLACE SCHEMA tasty_bytes_dbt_db.prod;


ALTER SCHEMA tasty_bytes_dbt_db.dev SET LOG_LEVEL = 'INFO';
ALTER SCHEMA tasty_bytes_dbt_db.dev SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA tasty_bytes_dbt_db.dev SET METRIC_LEVEL = 'ALL';

ALTER SCHEMA tasty_bytes_dbt_db.prod SET LOG_LEVEL = 'INFO';
ALTER SCHEMA tasty_bytes_dbt_db.prod SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA tasty_bytes_dbt_db.prod SET METRIC_LEVEL = 'ALL';

-- API integration needed to interact with Github
CREATE OR REPLACE API INTEGRATION tb_dbt_git_api_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/rawitner')
  -- ALLOWED_AUTHENTICATION_SECRETS = (tasty_bytes_dbt_db.integrations.tb_dbt_git_secret) // not needed if repo public
  API_USER_AUTHENTICATION = (TYPE = SNOWFLAKE_GITHUB_APP) // use oauth rather than secret
  ENABLED = TRUE;



-- create external access integration to get dependency files from remote URLs (optional)
-- Create NETWORK RULE for external access integration

CREATE OR REPLACE NETWORK RULE dbt_network_rule
  MODE = EGRESS
  TYPE = HOST_PORT
  -- Minimal URL allowlist that is required for dbt deps
  VALUE_LIST = (
    'hub.getdbt.com',
    'codeload.github.com'
    );

-- Create EXTERNAL ACCESS INTEGRATION for dbt access to external dbt package locations

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION dbt_ext_access
  ALLOWED_NETWORK_RULES = (dbt_network_rule)
  ENABLED = TRUE;

CREATE OR REPLACE FILE FORMAT tasty_bytes_dbt_db.public.csv_ff 
type = 'csv';

CREATE OR REPLACE STAGE tasty_bytes_dbt_db.public.s3load
COMMENT = 'Quickstarts S3 Stage Connection'
url = 's3://sfquickstarts/frostbyte_tastybytes/'
file_format = tasty_bytes_dbt_db.public.csv_ff;

/*--
 raw zone table build 
--*/

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

-- customer loyalty table build
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

/*--
 raw zone table load 
--*/

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

-- setup completion note
SELECT 'tasty_bytes_dbt_db setup is now complete' AS note;

-- task creation
create or replace task tasty_bytes_dbt_db.dev.test_dbt_task
    warehouse = tasty_bytes_dbt_wh
    schedule = 'USING CRON 1 1 * * * America/New_York'
    as EXECUTE dbt project TASTY_BYTES_DBT_PROJECT args = 'run --target dev'