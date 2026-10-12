--set up warehouse, target for model materializations, api integration to connect to Git, and external access integration for package dependencies
CREATE OR REPLACE WAREHOUSE ...;
CREATE OR REPLACE DATABASE ...;
CREATE OR REPLACE SCHEMA ...;
CREATE OR REPLACE API INTEGRATION ...;
CREATE OR REPLACE NETWORK RULE ...;
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ...;

--enable logging, tracing, and metrics
CREATE OR REPLACE WAREHOUSE ...;
CREATE OR REPLACE API INTEGRATION ...;
CREATE OR REPLACE NETWORK RULE ...;
CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION ...;

--create roles to interact with dbt project
CREATE ROLE IF NOT EXISTS ...;
CREATE ROLE IF NOT EXISTS ...;

--create CI/CD service user
CREATE USER IF NOT EXISTS github_actions_service_user
  TYPE = SERVICE
  WORKLOAD_IDENTITY = (
    TYPE = OIDC
    ISSUER = 'https://token.actions.githubusercontent.com',
    SUBJECT = 'repo:your_repo_org/your_dbt_repo:environment:prod'
  )
  DEFAULT_ROLE = DBT_CI_ROLE
  COMMENT = 'Service user for GitHub Actions';

GRANT ROLE DBT_CI_ROLE TO USER github_actions_service_user;

ALTER USER github_actions_service_user
  SET DEFAULT_WAREHOUSE = 'TASTY_BYTES_DBT_WH';

--create github action scripts for CI and CD
--https://github.com/rachaelgfriedman/snow_dbt_projects_demo/blob/main/.github/workflows/incoming_pr_slim_ci.yml