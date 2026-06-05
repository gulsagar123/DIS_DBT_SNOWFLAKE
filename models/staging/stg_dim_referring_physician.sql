{{
    config(
        materialized='incremental',
        schema='SILVER_CORE_DBT',
        alias='STAGE_DIM_REFERRING_PHYSICIAN',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

SELECT DISTINCT
    1                               AS SOURCE_SYSTEM_ID,
    'HCHB'                          AS SYSTEM_CODE,
    phy.PH_ID                       AS PHYSICIAN_ID,
    ofc.PHO_ADDRESS                 AS PHYSICIAN_OFFICE,
    grp.PHG_NAME                    AS PHYSICIAN_GROUP,
    phy.PH_FIRSTNAME                AS PHYSICIAN_FIRST_NAME,
    phy.PH_LASTNAME                 AS PHYSICIAN_LAST_NAME,
    ofc.PHO_ADDRESS                 AS ADDRESS_LINE_1,
    NULL                            AS ADDRESS_LINE_2,
    ofc.PHO_CITY                    AS CITY,
    NULL                            AS COUNTY,
    ofc.PHO_STATE                   AS STATE,
    ofc.PHO_ZIP                     AS ZIP_CODE,
    CURRENT_TIMESTAMP()             AS LOADED_AT,
    GREATEST(
        COALESCE(phy.CDC_TS, '1900-01-01'::TIMESTAMP),
        COALESCE(ofc.CDC_TS, '1900-01-01'::TIMESTAMP),
        COALESCE(grp.CDC_TS, '1900-01-01'::TIMESTAMP)
    )                               AS SOURCE_LAST_UPDATE,
    phy.CDC_OP                      AS CDC_OP,
    phy.CDC_TS                      AS CDC_TS

FROM {{ source('BRONZE_STAGE', 'physicians') }} phy
LEFT JOIN {{ source('BRONZE_STAGE', 'physician_offices') }} ofc
    ON ofc.PHO_ID = phy.PH_ID
    AND UPPER(ofc.PHO_ACTIVE) = 'Y'
LEFT JOIN {{ source('BRONZE_STAGE', 'physician_groups') }} grp
    ON phy.PH_PGID = grp.PHG_ID

{% if is_incremental() %}
    WHERE phy.CDC_TS > (
        SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}