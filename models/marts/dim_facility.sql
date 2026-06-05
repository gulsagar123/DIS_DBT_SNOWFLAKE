{{
    config(
        materialized='incremental',
        schema='SILVER_DBT',
        alias='DIM_FACILITY',
        incremental_strategy='merge',
        unique_key='FACILITY_KEY',
        on_schema_change='sync_all_columns'
    )
}}

WITH ranked_source AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['SOURCE_SYSTEM_ID', 'FACILITY_ID']) }} AS FACILITY_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        FACILITY_ID,
        FACILITY_NAME,
        FACILITY_TYPE,
        NPI,
        IS_ACTIVE,
        ADDRESS,
        CITY,
        STATE_CODE,
        ZIP,
        WORK_PHONE,
        CDC_OP,
        CDC_TS,

        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM_ID, FACILITY_ID
            ORDER BY CDC_TS DESC
        ) AS row_num

    FROM {{ ref('stg_dim_facility') }}

    {% if is_incremental() %}
        WHERE CDC_TS > (
            SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}

),

latest_records AS (

    SELECT
        FACILITY_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        FACILITY_ID,
        FACILITY_NAME,
        FACILITY_TYPE,
        NPI,
        IS_ACTIVE,
        ADDRESS,
        CITY,
        STATE_CODE,
        ZIP,
        WORK_PHONE,
        CDC_OP,
        CDC_TS

    FROM ranked_source
    WHERE row_num = 1
      AND CDC_OP != 'DELETE'

)

{% if is_incremental() %}

SELECT
    l.FACILITY_KEY,
    l.SOURCE_SYSTEM_ID,
    l.SYSTEM_CODE,
    l.FACILITY_ID,
    l.FACILITY_NAME,
    l.FACILITY_TYPE,
    l.NPI,
    l.IS_ACTIVE,
    l.ADDRESS,
    l.CITY,
    l.STATE_CODE,
    l.ZIP,
    l.WORK_PHONE,
    l.CDC_OP,
    l.CDC_TS,
    COALESCE(t.INSERTED_DATE, CURRENT_TIMESTAMP())  AS INSERTED_DATE,
    COALESCE(t.INSERTED_BY,   'ETL_PROCESS')        AS INSERTED_BY,
    CURRENT_TIMESTAMP()                             AS UPDATED_DATE,
    'ETL_PROCESS'                                   AS UPDATED_BY

FROM latest_records l
LEFT JOIN {{ this }} t ON l.FACILITY_KEY = t.FACILITY_KEY

{% else %}

SELECT
    FACILITY_KEY,
    SOURCE_SYSTEM_ID,
    SYSTEM_CODE,
    FACILITY_ID,
    FACILITY_NAME,
    FACILITY_TYPE,
    NPI,
    IS_ACTIVE,
    ADDRESS,
    CITY,
    STATE_CODE,
    ZIP,
    WORK_PHONE,
    CDC_OP,
    CDC_TS,
    CURRENT_TIMESTAMP()     AS INSERTED_DATE,
    'ETL_PROCESS'           AS INSERTED_BY,
    CURRENT_TIMESTAMP()     AS UPDATED_DATE,
    'ETL_PROCESS'           AS UPDATED_BY

FROM latest_records

{% endif %}