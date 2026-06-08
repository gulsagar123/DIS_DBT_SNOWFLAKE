-- models/silver/dim_branch.sql

{{
    config(
       materialized='incremental',
        unique_key='BRANCH_KEY',
        incremental_strategy='merge',
         schema='silver_dbt',
        alias='DIM_BRANCH',
        merge_update_columns=[
            'BRANCH_NAME', 'PARENT_BRANCH', 'ADDRESS_LINE_1',
            'CITY', 'STATE_CODE', 'ZIP', 'LATITUDE', 'LONGITUDE',
            'FAX', 'EMAIL', 'PHONE', 'IS_ACTIVE', 'BRANCH_GL_NUMBER',
            'UPDATED_DATE', 'UPDATED_BY'
        ]
    )
}}

SELECT
    MD5(CAST(SOURCE_SYSTEM_ID AS VARCHAR) || '-' || BRANCH_CODE) AS BRANCH_KEY,
    SOURCE_SYSTEM_ID,
    SYSTEM_CODE,
    NULL                                    AS BRANCH_ID,
    BRANCH_CODE,
    BRANCH_NAME,
    PARENT_BRANCH,
    ADDRESS_LINE_1,
    ADDRESS_LINE_2,
    CITY,
    COUNTY,
    STATE_CODE,
    ZIP,
    LATITUDE,
    LONGITUDE,
    FAX,
    EMAIL,
    PHONE,
    IS_ACTIVE,
    BRANCH_GL_NUMBER,

    -- Insert audit columns (merge_update_columns excludes these on update)
    CURRENT_TIMESTAMP                       AS INSERTED_DATE,
    'ETL_PROCESS'                           AS INSERTED_BY,

    -- Update audit columns (included in merge_update_columns)
    CURRENT_TIMESTAMP                       AS UPDATED_DATE,
    'ETL_PROCESS'                           AS UPDATED_BY

FROM {{ ref('stage_hchb_dim_branch') }}          -- replaces silver_core.STAGE_DIM_BRANCH