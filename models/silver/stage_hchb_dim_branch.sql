{{
    config(
        schema        = 'silver_core_dbt',
        alias         = 'STAGE_HCHB_DIM_BRANCH',
        materialized  = 'table',
        transient     = false,
        snowflake_warehouse = var('warehouse', target.warehouse)
    )
}}

/*
    Simple TABLE materialization — dbt does CREATE OR REPLACE TABLE,
    which is identical to INSERT OVERWRITE. No watermark, no incremental
    complexity. Always loads only the latest batch (max date rows).
*/

WITH max_date AS (
    SELECT
        MAX(CONVERT_TIMEZONE('UTC', GREATEST(UPDATED_DATE, INSERTED_DATE))::TIMESTAMP_NTZ) AS max_ts
    FROM {{ source('BRONZE_HCHB', 'branches') }}
)

SELECT
    1                   AS SOURCE_SYSTEM_ID,
    'HCHB'              AS SYSTEM_CODE,
    b.branch_code       AS BRANCH_CODE,
    b.branch_name       AS BRANCH_NAME,
    b.branch_parent     AS PARENT_BRANCH,
    b.branch_street     AS ADDRESS_LINE_1,
    NULL::VARCHAR       AS ADDRESS_LINE_2,
    b.branch_city       AS CITY,
    NULL::VARCHAR       AS COUNTY,
    b.branch_state      AS STATE_CODE,
    b.branch_zip        AS ZIP,
    b.branch_latitude   AS LATITUDE,
    b.branch_longitude  AS LONGITUDE,
    b.branch_fax        AS FAX,
    b.branch_email      AS EMAIL,
    b.branch_phone      AS PHONE,
    b.branch_active     AS IS_ACTIVE,
    b.branch_branchgl   AS BRANCH_GL_NUMBER,
    CURRENT_TIMESTAMP   AS LOADED_AT,
    b.UPDATED_DATE      AS SOURCE_LAST_UPDATE,
    b.cdc_ts            AS CDC_TIMESTAMP,
    b.cdc_op            AS CDC_OPRATION

FROM {{ source('BRONZE_HCHB', 'branches') }} b
JOIN max_date m
    ON CONVERT_TIMEZONE('UTC', GREATEST(b.UPDATED_DATE, b.INSERTED_DATE))::TIMESTAMP_NTZ
     = m.max_ts