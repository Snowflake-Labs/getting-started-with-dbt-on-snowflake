-- =============================================================================
-- CI/CD Setup for dbt Projects on Snowflake
-- Source: https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-ci-cd-tutorial
-- =============================================================================

-- =============================================================================
-- STEP 1: Set up your environment
-- Create dev and prod databases and schemas
-- Choose one of the following three options:
--
-- NOTE: If you have already run tasty_bytes_setup.sql, your database and schemas
-- already exist. See the getting-started tutorial for details:
-- https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-getting-started-tutorial#run-the-sql-commands-in-tasty-bytes-setup-sql-to-set-up-source-data
-- =============================================================================

-- Option 1: Clone your production database
-- Use Snowflake's zero-copy cloning to create a full replica of your production database.
-- This gives you a high-fidelity testing environment and is cost-effective because you
-- only pay storage for tables that change during dbt runs.
CREATE DATABASE IF NOT EXISTS tasty_bytes_dbt_db CLONE other_tasty_bytes_dbt_db;

-- Option 2: Create an empty dev database and clone the production schemas you need
-- Use this method when you only need specific schemas for testing.
-- CREATE DATABASE IF NOT EXISTS tasty_bytes_dbt_db;

-- Repeat the line below for other necessary schemas
-- CREATE SCHEMA IF NOT EXISTS tasty_bytes_dbt_db.dev CLONE tasty_bytes_dbt_db.dev;

-- Option 3: Create new dev and prod databases and schemas
-- This is the simplest approach when you're starting from scratch.
-- CREATE DATABASE IF NOT EXISTS tasty_bytes_dbt_db;
-- CREATE SCHEMA IF NOT EXISTS tasty_bytes_dbt_db.dev;

-- CREATE SCHEMA IF NOT EXISTS tasty_bytes_dbt_db.prod;

-- =============================================================================
-- STEP 2: Create a GitHub service user in Snowflake (recommended)
-- =============================================================================

-- Recommended: OIDC-based service user
-- This approach uses OpenID Connect (OIDC) rather than long-lived credentials.
CREATE USER IF NOT EXISTS github_actions_service_user
  TYPE = SERVICE
  WORKLOAD_IDENTITY = (
    TYPE = OIDC
    ISSUER = 'https://token.actions.githubusercontent.com',
    SUBJECT = 'repo:your_repo_org/your_dbt_repo:environment:prod'
  )
  DEFAULT_ROLE = <role_name>
  COMMENT = 'Service user for GitHub Actions';

-- After you create your user, explicitly grant the default role for the service user
-- to assume that role. The DEFAULT_ROLE parameter only sets the user's default role
-- and doesn't grant it.
GRANT ROLE <role_name> TO USER github_actions_service_user;

-- Set a default warehouse:
ALTER USER github_actions_service_user SET DEFAULT_WAREHOUSE = 'tasty_bytes_dbt_wh';

-- Alternative: PAT-based authentication (less secure)
-- If you prefer to use one Snowflake user across multiple repositories, or cannot use
-- OIDC, you can create the user with a personal access token (PAT) instead.
-- This method is easier to reuse across repositories but less secure because it relies
-- on long-lived credentials and requires manual rotation.
-- CREATE USER IF NOT EXISTS github_actions_service_user
-- TYPE = SERVICE
-- COMMENT = 'Service user for GitHub Actions';

-- Grant the level of access to your user that can create network, auth policies,
-- and objects such as DBs and schemas
-- GRANT ROLE ACCOUNTADMIN TO USER github_actions_service_user;

-- Setting up databases and schemas to store policies and network rules
-- CREATE DATABASE IF NOT EXISTS github_actions_access_management;
-- CREATE SCHEMA IF NOT EXISTS github_actions_access_management.NETWORKS;
-- CREATE SCHEMA IF NOT EXISTS github_actions_access_management.POLICIES;

-- CREATE AUTHENTICATION POLICY github_actions_access_management.POLICIES.github_auth_policy
-- authentication_methods = ('PROGRAMMATIC_ACCESS_TOKEN')
-- pat_policy = (
-- default_expiry_in_days = 15, -- default value
-- max_expiry_in_days = 365, -- default value
-- network_policy_evaluation = ENFORCED_NOT_REQUIRED -- this is needed to ensure you can generate a PAT on Snowsight
-- );

-- ALTER USER github_actions_service_user SET AUTHENTICATION POLICY github_actions_access_management.POLICIES.github_auth_policy;

-- Set a default warehouse:
-- ALTER USER github_actions_service_user SET DEFAULT_WAREHOUSE = 'tasty_bytes_dbt_wh';

-- =============================================================================
-- STEP 3: (Optional) Set up a network policy for GitHub Actions
-- =============================================================================

-- Option 1: Create a new network policy and apply it to the user
-- A Snowflake user can have only one network policy at a time. If the user doesn't
-- have one or you want to replace the existing policy, complete the following steps:
CREATE NETWORK POLICY github_actions_policy
  ALLOWED_NETWORK_RULE_LIST = ('SNOWFLAKE.NETWORK_SECURITY.GITHUBACTIONS_GLOBAL', <other required rules>)
  BLOCKED_NETWORK_RULE_LIST = ();

ALTER USER GitHub_Actions_Service_User
  SET NETWORK_POLICY = github_actions_policy;

-- Option 2: Add a network rule to an existing network policy
-- If the user already has a network policy, you can add the GitHub Actions rule to it.

-- Check the user's current network policy:
-- SHOW PARAMETERS LIKE 'NETWORK_POLICY' FOR USER github_actions_service_user;

-- Add the new rule:
-- ALTER NETWORK POLICY <name>
--   ADD ALLOWED_NETWORK_RULE_LIST = ('SNOWFLAKE.NETWORK_SECURITY.GITHUBACTIONS_GLOBAL');
