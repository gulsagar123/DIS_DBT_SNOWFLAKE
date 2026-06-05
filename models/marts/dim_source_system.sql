{{
    config(
        materialized='table',
        schema='SILVER_DBT',
        alias='DIM_SOURCE_SYSTEM'
    )
}}

/*
    Silver Layer - DIM_SOURCE_SYSTEM
    ---------------------------------
    - No staging layer needed
    - Static lookup table
    - Full refresh on every run
*/

SELECT
    1                       AS SOURCE_SYSTEM_ID,
    'Home Care Home Base'   AS SOURCE_SYSTEM_NAME,
    'HCHB'                  AS SYSTEM_CODE,
    TRUE                    AS IS_ACTIVE,
    CURRENT_TIMESTAMP()     AS INSERTED_DATE,
    'ETL_PROCESS'           AS INSERTED_BY,
    CURRENT_TIMESTAMP()     AS UPDATED_DATE,
    'ETL_PROCESS'           AS UPDATED_BY