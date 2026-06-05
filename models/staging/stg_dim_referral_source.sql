{{
    config(
        materialized='incremental',
        schema='SILVER_CORE_DBT',
        alias='STAGE_DIM_REFERRAL_SOURCE',
        incremental_strategy='append',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    1                                                       AS SOURCE_SYSTEM_ID,
    'HCHB'                                                  AS SYSTEM_CODE,
    ROW_NUMBER() OVER (ORDER BY EPI_REFERRALSOURCE)         AS REFERRAL_SOURCE_ID,
    EPI_REFERRALSOURCE                                      AS SOURCE_NAME,
    'Y'                                                     AS IS_ACTIVE,
    CURRENT_TIMESTAMP()                                     AS LOADED_AT,
    MAX(EPI_LASTUPDATE) OVER (PARTITION BY EPI_REFERRALSOURCE) AS SOURCE_LAST_UPDATE,
    CDC_OP                                                  AS CDC_OP,
    CDC_TS                                                  AS CDC_TS

FROM {{ source('BRONZE_STAGE', 'client_episodes_all') }}

WHERE EPI_REFERRALSOURCE IS NOT NULL

{% if is_incremental() %}
    AND CDC_TS > (
        SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
        FROM {{ this }}
    )
{% endif %}

QUALIFY ROW_NUMBER() OVER (PARTITION BY EPI_REFERRALSOURCE ORDER BY CDC_TS DESC) = 1