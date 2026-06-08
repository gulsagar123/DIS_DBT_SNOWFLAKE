{{
    config(
        materialized='incremental',
        incremental_strategy='merge',
        unique_key='PROGRAM_KEY',
        schema='silver_dbt',
        alias='DIM_PROGRAM',
        merge_update_columns=['PROGRAM_NAME', 'IS_ACTIVE', 'UPDATED_DATE', 'UPDATED_BY'],
        tags=['silver', 'hchb', 'dim_program']
    )
}}

WITH source AS (
    SELECT
        MD5(CAST(SOURCE_SYSTEM_ID AS VARCHAR) || '-' || CAST(PROGRAM_ID AS VARCHAR))  AS PROGRAM_KEY,
        PROGRAM_ID,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        PROGRAM_NAME,
        IS_ACTIVE
    FROM {{ ref('stage_hchb_dim_program') }}
)

SELECT
    PROGRAM_KEY,
    PROGRAM_ID,
    SOURCE_SYSTEM_ID,
    SYSTEM_CODE,
    PROGRAM_NAME,
    IS_ACTIVE,
    CURRENT_TIMESTAMP       AS INSERTED_DATE,
    'ETL_PROCESS'           AS INSERTED_BY,
    CURRENT_TIMESTAMP       AS UPDATED_DATE,
    'ETL_PROCESS'           AS UPDATED_BY
FROM source