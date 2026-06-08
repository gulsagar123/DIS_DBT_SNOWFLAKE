{{
    config(
        schema        = 'silver_core_dbt',
        alias         = 'STAGE_DIM_PROGRAM',
        materialized  = 'table',
        transient     = false,
        snowflake_warehouse = var('warehouse', target.warehouse)
    )
}}


WITH max_date AS (
    SELECT
        MAX(CONVERT_TIMEZONE('UTC', GREATEST(UPDATED_DATE, INSERTED_DATE))::TIMESTAMP_NTZ) AS max_ts
    FROM {{ source('BRONZE_HCHB', 'programs') }}
)

    SELECT
        1                                           AS SOURCE_SYSTEM_ID,
        'HCHB'                                      AS SYSTEM_CODE,
        pg.pg_id                                    AS PROGRAM_ID,
        TRIM(UPPER(pn.pn_name))                     AS PROGRAM_NAME,
        pg.pg_active                                AS IS_ACTIVE,
        CURRENT_TIMESTAMP                           AS LOADED_AT,
        pg.UPDATED_DATE                             AS SOURCE_LAST_UPDATE,
         pg.cdc_ts            AS CDC_TIMESTAMP,
    pg.cdc_op            AS CDC_OPRATION
    FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.PROGRAMS AS pg
    LEFT JOIN DATA_INGESTION_SOLUTION.BRONZE_HCHB.PROGRAM_NAMES AS pn
        ON pg.pg_pnid = pn.pn_id
JOIN max_date m
    ON CONVERT_TIMEZONE('UTC', GREATEST(pg.UPDATED_DATE, pg.INSERTED_DATE))::TIMESTAMP_NTZ
     = m.max_ts
   