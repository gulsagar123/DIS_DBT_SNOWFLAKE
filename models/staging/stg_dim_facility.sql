{{
    config(
        materialized='incremental',
        schema='SILVER_CORE_DBT',
        alias='STAGE_DIM_FACILITY',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    1                                               AS SOURCE_SYSTEM_ID,
    'HCHB'                                          AS SYSTEM_CODE,
    f.FA_ID                                         AS FACILITY_ID,
    f.FA_NAME                                       AS FACILITY_NAME,
    rft.RFT_DESC                                    AS FACILITY_TYPE,
    f.FA_NPI                                        AS NPI,
    CASE WHEN f.FA_ACTIVE = 'Y' THEN TRUE
         ELSE FALSE END                             AS IS_ACTIVE,
    f.FA_STREET                                     AS ADDRESS,
    f.FA_CITY                                       AS CITY,
    f.FA_STATE                                      AS STATE_CODE,
    f.FA_ZIP                                        AS ZIP,
    f.FA_TELEPHONE                                  AS WORK_PHONE,
    CURRENT_TIMESTAMP()                             AS LOADED_AT,
    f.FA_LASTUPDATE                                 AS SOURCE_LAST_UPDATE,
    f.CDC_OP                                        AS CDC_OP,
    f.CDC_TS                                        AS CDC_TS

FROM {{ source('BRONZE_STAGE', 'facilities') }} f
LEFT JOIN {{ source('BRONZE_STAGE', 'referring_facility_types') }} rft
    ON f.FA_RFTID = rft.RFT_ID

{% if is_incremental() %}
    WHERE f.CDC_TS > (
        SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}