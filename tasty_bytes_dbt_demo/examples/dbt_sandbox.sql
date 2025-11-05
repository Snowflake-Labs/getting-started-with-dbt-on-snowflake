TASTY_BYTES_DBT_DB.DEV.TASTY_BYTES_DBTSHOW TABLES IN DATABASE tasty_bytes_dbt_db;

SHOW VIEWS IN DATABASE tasty_bytes_dbt_db;

-- 利用可能なワークスペースを確認
SHOW GIT REPOSITORIES;

-- Git Repositoryの詳細を確認
DESC GIT REPOSITORY tasty_bytes_dbt;
USE ROLE ACCOUNTADMIN;

-- 現在のロールに権限を付与
GRANT USAGE ON DATABASE TASTY_BYTES_DBT_DB TO ROLE SYSADMIN;
GRANT USAGE ON SCHEMA DEV TO ROLE SYSADMIN;
GRANT READ ON GIT REPOSITORY tasty_bytes_dbt TO ROLE SYSADMIN;

-- 現在のロール、データベース、スキーマを確認
SELECT CURRENT_ROLE(), CURRENT_DATABASE(), CURRENT_SCHEMA();

-- 現在のロールを確認
SHOW ROLES;

-- ACCOUNTADMINに切り替え
USE ROLE ACCOUNTADMIN;

-- すべてのGit Repositoryを確認
SHOW GIT REPOSITORIES;

-- 特定のデータベース内を確認
SHOW GIT REPOSITORIES IN DATABASE tasty_bytes_dbt_db;

-- アカウント全体を確認
SHOW GIT REPOSITORIES IN ACCOUNT;

CREATE OR REPLACE GIT REPOSITORY tasty_bytes_dbt
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/nadayoshi523/getting-started-with-dbt-on-snowflake.git';

  -- 確認
SHOW GIT REPOSITORIES;

-- すべてのスキーマを確認
SHOW SCHEMAS IN DATABASE tasty_bytes_dbt_db;

-- 各スキーマでGit Repositoryを確認
SHOW GIT REPOSITORIES IN SCHEMA tasty_bytes_dbt_db.dev;
SHOW GIT REPOSITORIES IN SCHEMA tasty_bytes_dbt_db.public;

-- クエリ履歴から確認
SELECT 
  query_text, 
  start_time, 
  end_time
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE query_text ILIKE '%GIT REPOSITORY%'
  OR query_text ILIKE '%EXECUTE DBT%'
ORDER BY start_time DESC
LIMIT 10;

-- USER$スキーマで確認
SHOW GIT REPOSITORIES IN SCHEMA USER$TAKUYANADAYOSHI.PUBLIC;


ALTER GIT REPOSITORY tasty_bytes_dbt FETCH;

-- データベースとスキーマを使用
USE DATABASE tasty_bytes_dbt_db;
USE SCHEMA dev;

-- dbt runを実行
EXECUTE DBT PROJECT FROM WORKSPACE USER$.PUBLIC.tasty_bytes_dbt 
  PROJECT_ROOT='/tasty_bytes_dbt_demo' 
  ARGS='run --target dev' 
  EXTERNAL_ACCESS_INTEGRATIONS = ();

  -- ACCOUNTADMINロールに切り替えてから再試行
-- ACCOUNTADMINロールで実行
USE ROLE ACCOUNTADMIN;

-- Workspaceへの権限を付与
GRANT USAGE ON DATABASE TASTY_BYTES_DBT_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA TASTY_BYTES_DBT_DB.DEV TO ROLE ACCOUNTADMIN;

-- Git Repositoryへの権限を付与
GRANT READ ON GIT REPOSITORY TASTY_BYTES_DBT_DB.DEV.TASTY_BYTES_DBT TO ROLE ACCOUNTADMIN;
GRANT WRITE ON GIT REPOSITORY TASTY_BYTES_DBT_DB.DEV.TASTY_BYTES_DBT TO ROLE ACCOUNTADMIN;

-- Warehouseへの権限を付与（使用するウェアハウス名に変更）
GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE ACCOUNTADMIN;

-- ACCOUNTADMINロールで実行
USE ROLE ACCOUNTADMIN;
USE DATABASE TASTY_BYTES_DBT_DB;
USE SCHEMA DEV;

-- Dbt Projectを作成
CREATE DBT PROJECT tasty_bytes_dbt_project
  FROM $$snow://git/TASTY_BYTES_DBT_DB/DEV/TASTY_BYTES_DBT/versions/main/tasty_bytes_dbt_demo$$;

  -- Git Repositoryの詳細を確認
DESC GIT REPOSITORY TASTY_BYTES_DBT_DB.DEV.TASTY_BYTES_DBT;

-- ファイル構造を確認
LS @TASTY_BYTES_DBT_DB.DEV.TASTY_BYTES_DBT/branches/main;

-- USER$TAKUYANADAYOSHI.PUBLICスキーマのGit Repositoryを確認
SHOW GIT REPOSITORIES IN SCHEMA USER$TAKUYANADAYOSHI.PUBLIC;

-- Git Repositoryを作成
CREATE OR REPLACE GIT REPOSITORY "getting-started-with-dbt-on-snowflake"
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/nadayoshi523/getting-started-with-dbt-on-snowflake.git';

  ALTER GIT REPOSITORY "getting-started-with-dbtTASTY_BYTES_DBT_DB.PUBLIC.TASTY_BYTES_DBTTASTY_BYTES_DBT_DB.PUBLIC.TASTY_BYTES_DBT-on-snowflake" FETCH;

  EXECUTE DBT PROJECT FROM WORKSPACE USER$TAKUYANADAYOSHI.PUBLIC.tasty_bytes_dbt
  PROJECT_ROOT='/tasty_bytes_dbt_demo'


  USE ROLE ACCOUNTADMIN;

-- USER$TAKUYANADAYOSHI.PUBLICにGit Repositoryを作成
CREATE OR REPLACE GIT REPOSITORY tasty_bytes_dbt
  API_INTEGRATION = git_api_integration
  ORIGIN = 'https://github.com/nadayoshi523/getting-started-with-dbt-on-snowflake.git';

-- 実行
EXECUTE DBT PROJECT FROM GIT REPOSITORY tasty_bytes_dbt
  PROJECT_ROOT='/tasty_bytes_dbt_demo'
  ARGS='run --target dev'
  EXTERNAL_ACCESS_INTEGRATIONS = ();
