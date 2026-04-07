# Getting Started with dbt Projects on Snowflake

## Overview

This repository contains an example dbt project for [dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/data-engineering/dbt-projects-on-snowflake). It uses the fictitious **Tasty Bytes** food truck brand as sample data and walks through environment setup, data modeling, CI/CD, and scheduling — all running natively inside Snowflake.

This repository is based on these Snowflake tutorials:
- [Get started with dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-getting-started-tutorial)
- [Set up CI/CD for dbt Projects on Snowflake](https://docs.snowflake.com/en/user-guide/tutorials/dbt-projects-on-snowflake-ci-cd-tutorial)

## What's Included

### Setup Scripts (`setup/`)

- **`tasty_bytes_setup.sql`** — Creates the warehouse, database, schemas, GitHub integration, network rules, and loads the Tasty Bytes source data from S3 into raw tables.
- **`ci_cd_setup.sql`** — Creates a GitHub Actions service user with OIDC authentication and optional network policies for CI/CD pipelines.

### dbt Project (`tasty_bytes_dbt_demo/`)

- **Staging models** — Views that clean and rename columns from raw source tables (orders, trucks, menus, locations, franchises, customer loyalty).
- **Mart models** — Tables that aggregate business metrics: `orders`, `customer_loyalty_metrics`, and `sales_metrics_by_location` (Python model).
- **Custom macros** — Schema name generation for multi-environment deployments (dev/prod).
- **Generic tests** — Reusable test for validating positive amounts.

### CI/CD (`.github/workflows/`)

- **`incoming_pr.yml`** — Runs dbt checks against a dev environment when a PR is opened.
- **`pr_merged.yml`** — Deploys the dbt project to production when a PR is merged.

### Scheduling (`schedules.sql`)

Task definitions for running the dbt project on a schedule using Snowflake Tasks.

## Quick Start

1. Fork this repository.
2. Run `tasty_bytes_setup.sql` in a Snowflake worksheet to create the environment and load source data.
3. Create a [workspace in Snowsight](https://docs.snowflake.com/en/user-guide/ui-snowsight/workspaces) connected to your fork.
4. Run `dbt deps`, then `dbt run` from the workspace.
