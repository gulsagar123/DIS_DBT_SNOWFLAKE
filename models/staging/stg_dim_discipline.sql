{{
    config(
        materialized='incremental',
        schema='SILVER_CORE_DBT',
        alias='STAGE_DIM_DISCIPLINE',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    1                       AS SOURCE_SYSTEM_ID,
    'HCHB'                  AS SYSTEM_CODE,
    DSC_CODE                AS CODE,
    DSC_ID                  AS ID,
    DSC_DESC                AS DESCRIPTION,
    DSC_ACTIVE              AS IS_ACTIVE,
    DSC_RATE                AS RATE,
    DSC_REVENUECODE         AS REVENUECODE,
    CURRENT_TIMESTAMP()     AS LOADED_AT,
    CDC_TS                  AS SOURCE_LAST_UPDATE,
    CDC_OP                  AS CDC_OP,
    CDC_TS                  AS CDC_TS

FROM {{ source('BRONZE_STAGE', 'disciplines') }}

{% if is_incremental() %}
    WHERE CDC_TS > (
        SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}