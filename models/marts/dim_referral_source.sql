{{
    config(
        materialized='incremental',
        schema='SILVER_DBT',
        alias='DIM_REFERRAL_SOURCE',
        incremental_strategy='merge',
        unique_key='REFERRAL_SOURCE_KEY',
        on_schema_change='sync_all_columns'
    )
}}

WITH ranked_source AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['SOURCE_SYSTEM_ID', 'REFERRAL_SOURCE_ID']) }} AS REFERRAL_SOURCE_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        REFERRAL_SOURCE_ID,
        SOURCE_NAME,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS,

        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM_ID, REFERRAL_SOURCE_ID
            ORDER BY CDC_TS DESC
        ) AS row_num

    FROM {{ ref('stg_dim_referral_source') }}

    {% if is_incremental() %}
        WHERE CDC_TS > (
            SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}

),

latest_records AS (

    SELECT
        REFERRAL_SOURCE_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        REFERRAL_SOURCE_ID,
        SOURCE_NAME,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS

    FROM ranked_source
    WHERE row_num = 1
      AND CDC_OP != 'DELETE'

)

{% if is_incremental() %}

SELECT
    l.REFERRAL_SOURCE_KEY,
    l.SOURCE_SYSTEM_ID,
    l.SYSTEM_CODE,
    l.REFERRAL_SOURCE_ID,
    l.SOURCE_NAME,
    l.IS_ACTIVE,
    l.CDC_OP,
    l.CDC_TS,
    COALESCE(t.INSERTED_DATE, CURRENT_TIMESTAMP())  AS INSERTED_DATE,
    COALESCE(t.INSERTED_BY,   'ETL_PROCESS')        AS INSERTED_BY,
    CURRENT_TIMESTAMP()                             AS UPDATED_DATE,
    'ETL_PROCESS'                                   AS UPDATED_BY

FROM latest_records l
LEFT JOIN {{ this }} t ON l.REFERRAL_SOURCE_KEY = t.REFERRAL_SOURCE_KEY

{% else %}

SELECT
    REFERRAL_SOURCE_KEY,
    SOURCE_SYSTEM_ID,
    SYSTEM_CODE,
    REFERRAL_SOURCE_ID,
    SOURCE_NAME,
    IS_ACTIVE,
    CDC_OP,
    CDC_TS,
    CURRENT_TIMESTAMP()     AS INSERTED_DATE,
    'ETL_PROCESS'           AS INSERTED_BY,
    CURRENT_TIMESTAMP()     AS UPDATED_DATE,
    'ETL_PROCESS'           AS UPDATED_BY

FROM latest_records

{% endif %}