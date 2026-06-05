{{
    config(
        materialized='incremental',
        schema='SILVER_CORE_DBT',
        alias='STAGE_DIM_SERVICE_LINE',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    1                       AS SOURCE_SYSTEM_ID,
    'HCHB'                  AS SYSTEM_CODE,
    SL_ID                   AS SERVICE_LINE_ID,
    SL_DESC                 AS SERVICE_LINE_NAME,
    SL_FULL_DESC            AS SERVICE_LINE_DESCRIPTION,
    SL_ACTIVE               AS IS_ACTIVE,
    CURRENT_TIMESTAMP()     AS LOADED_AT,
    UPDATED_DATE            AS SOURCE_LAST_UPDATE,
    CDC_OP                  AS CDC_OP,
    CDC_TS                  AS CDC_TS

FROM {{ source('BRONZE_STAGE', 'service_line') }}

{% if is_incremental() %}
    WHERE CDC_TS > (
        SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}