{{
    config(
        materialized='incremental',
        schema='SILVER_DBT',
        alias='DIM_PAYOR',
        incremental_strategy='merge',
        unique_key='PAYOR_KEY',
        on_schema_change='sync_all_columns'
    )
}}

WITH ranked_source AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['SOURCE_SYSTEM_ID', 'PAYOR_ID']) }} AS PAYOR_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        PAYOR_ID,
        PAYOR_NAME,
        PAYOR_TYPE,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS,

        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM_ID, PAYOR_ID
            ORDER BY CDC_TS DESC
        ) AS row_num

    FROM {{ ref('stg_dim_payor') }}

    {% if is_incremental() %}
        WHERE CDC_TS > (
            SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}

),

latest_records AS (

    SELECT
        PAYOR_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        PAYOR_ID,
        PAYOR_NAME,
        PAYOR_TYPE,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS

    FROM ranked_source
    WHERE row_num = 1
      AND CDC_OP != 'DELETE'

)

{% if is_incremental() %}

SELECT
    l.PAYOR_KEY,
    l.SOURCE_SYSTEM_ID,
    l.SYSTEM_CODE,
    l.PAYOR_ID,
    l.PAYOR_NAME,
    l.PAYOR_TYPE,
    l.IS_ACTIVE,
    l.CDC_OP,
    l.CDC_TS,
    COALESCE(t.INSERTED_DATE, CURRENT_TIMESTAMP())  AS INSERTED_DATE,
    COALESCE(t.INSERTED_BY,   'ETL_PROCESS')        AS INSERTED_BY,
    CURRENT_TIMESTAMP()                             AS UPDATED_DATE,
    'ETL_PROCESS'                                   AS UPDATED_BY

FROM latest_records l
LEFT JOIN {{ this }} t ON l.PAYOR_KEY = t.PAYOR_KEY

{% else %}

SELECT
    PAYOR_KEY,
    SOURCE_SYSTEM_ID,
    SYSTEM_CODE,
    PAYOR_ID,
    PAYOR_NAME,
    PAYOR_TYPE,
    IS_ACTIVE,
    CDC_OP,
    CDC_TS,
    CURRENT_TIMESTAMP()     AS INSERTED_DATE,
    'ETL_PROCESS'           AS INSERTED_BY,
    CURRENT_TIMESTAMP()     AS UPDATED_DATE,
    'ETL_PROCESS'           AS UPDATED_BY

FROM latest_records

{% endif %}