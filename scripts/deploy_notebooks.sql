--!jinja

/*-----------------------------------------------------------------------------
Hands-On Lab: Intro to Data Engineering with Notebooks
Script:       deploy_notebooks.sql
Author:       Jeremiah Hansen
Last Updated: 6/11/2024
-----------------------------------------------------------------------------*/

-- See https://docs.snowflake.com/en/LIMITEDACCESS/execute-immediate-from-template

-- Create the Notebooks
--USE SCHEMA {{env}}_SCHEMA;

CREATE OR REPLACE database IDENTIFIER('"DevOps"."{{env}}_SCHEMA"."{{env}}_06_load_excel_files"')
    FROM '@"DevOps"."INTEGRATIONS"."DEMO_GIT_REPO"/branches/"{{branch}}"/notebooks/06_load_excel_files/'
    QUERY_WAREHOUSE = 'TB_TEST_WH'
    MAIN_FILE = '06_load_excel_files.ipynb';

ALTER database "DevOps"."{{env}}_SCHEMA"."{{env}}_06_load_excel_files" ADD LIVE VERSION FROM LAST;

CREATE OR REPLACE database IDENTIFIER('"DevOps"."{{env}}_SCHEMA"."{{env}}_07_load_daily_city_metrics"')
    FROM '@"DevOps"."INTEGRATIONS"."DEMO_GIT_REPO"/branches/"{{branch}}"/notebooks/07_load_daily_city_metrics/'
    QUERY_WAREHOUSE = 'TB_TEST_WH'
    MAIN_FILE = '07_load_daily_city_metrics.ipynb';

ALTER database "DEMO_DB"."{{env}}_SCHEMA"."{{env}}_07_load_daily_city_metrics" ADD LIVE VERSION FROM LAST;
