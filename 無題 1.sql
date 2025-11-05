/**-- -------------------------------------
-- 1. 作業用データベースの作成
-- -------------------------------------
CREATE OR REPLACE DATABASE DEMO_9;
-- -------------------------------------
-- 2. dbt タスクの作成
-- -------------------------------------
CREATE OR REPLACE TASK DEMO_251029.PUBLIC.DBT_TASK
WAREHOUSE = compute_wh
SCHEDULE = 'USING CRON 0 9 1 1 * Asia/Tokyo'
AS
EXECUTE DBT PROJECT DEMO_251029.PUBLIC.DBT_TEST ARGS = 'build --target dev';
**/-- -------------------------------------
-- 3. イベントテーブル用データベースとスキーマの作成
-- -------------------------------------
USE ROLE SYSADMIN;
CREATE OR REPLACE DATABASE DEMO_EVENT_DB;
CREATE OR REPLACE SCHEMA DEMO_EVENT_DB.DBT;
-- イベントテーブル作成
CREATE OR REPLACE EVENT TABLE DEMO_EVENT_DB.DBT.DBT_EVENTS;
-- -------------------------------------
-- 4. イベントテーブルをアカウントに紐づけ
-- -------------------------------------
-- 必ずACCOUNTレベルで設定
ALTER ACCOUNT SET EVENT_TABLE = DEMO_EVENT_DB.DBT.DBT_EVENTS;
-- 設定確認（アカウントレベル）
SHOW PARAMETERS LIKE 'event_table' IN ACCOUNT;
-- -------------------------------------
-- 5. スキーマレベルのログ・トレース・メトリクス設定
-- -------------------------------------
ALTER SCHEMA DEMO_251029.PUBLIC SET LOG_LEVEL = 'INFO';
ALTER SCHEMA DEMO_251029.PUBLIC SET TRACE_LEVEL = 'ALWAYS';
ALTER SCHEMA DEMO_251029.PUBLIC SET METRIC_LEVEL = 'ALL';
-- -------------------------------------
-- 6. dbtタスクの即時手動実行（スケジュール待たずに確認したい場合）
-- -------------------------------------
EXECUTE TASK DEMO_251029.PUBLIC.DBT_TASK;