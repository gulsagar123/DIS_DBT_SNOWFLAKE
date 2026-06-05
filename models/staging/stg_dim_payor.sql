{{
    config(
        materialized='incremental',
        schema='SILVER_CORE_DBT',
        alias='STAGE_DIM_PAYOR',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    1                       AS SOURCE_SYSTEM_ID,
    'HCHB'                  AS SYSTEM_CODE,
    ps.PS_ID                AS PAYOR_ID,
    ps.PS_DESC              AS PAYOR_NAME,
    pt.PT_DESC              AS PAYOR_TYPE,
    CASE WHEN ps.PS_ACTIVE = 'Y' THEN TRUE
         ELSE FALSE END     AS IS_ACTIVE,
    CURRENT_TIMESTAMP()     AS LOADED_AT,
    ps.CDC_TS               AS SOURCE_LAST_UPDATE,
    ps.CDC_OP               AS CDC_OP,
    ps.CDC_TS               AS CDC_TS

FROM {{ source('BRONZE_STAGE', 'payor_sources') }} ps
LEFT JOIN {{ source('BRONZE_STAGE', 'payor_types') }} pt
    ON ps.PS_PTID = pt.PT_ID

{% if is_incremental() %}
    WHERE ps.CDC_TS > (
        SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}