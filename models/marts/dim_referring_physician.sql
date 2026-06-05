{{
    config(
        materialized='incremental',
        schema='SILVER_DBT',
        alias='DIM_REFERRING_PHYSICIAN',
        incremental_strategy='merge',
        unique_key='REFERRING_PHYSICIAN_KEY',
        on_schema_change='sync_all_columns'
    )
}}

WITH ranked_source AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['SOURCE_SYSTEM_ID', 'PHYSICIAN_ID']) }} AS REFERRING_PHYSICIAN_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        PHYSICIAN_ID,
        PHYSICIAN_OFFICE,
        PHYSICIAN_GROUP,
        PHYSICIAN_FIRST_NAME,
        PHYSICIAN_LAST_NAME,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        CITY,
        COUNTY,
        STATE,
        ZIP_CODE,
        'Y'                 AS IS_ACTIVE,
        CDC_OP,
        CDC_TS,

        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM_ID, PHYSICIAN_ID
            ORDER BY CDC_TS DESC
        ) AS row_num

    FROM {{ ref('stg_dim_referring_physician') }}

    {% if is_incremental() %}
        WHERE CDC_TS > (
            SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}

),

latest_records AS (

    SELECT
        REFERRING_PHYSICIAN_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        PHYSICIAN_ID,
        PHYSICIAN_OFFICE,
        PHYSICIAN_GROUP,
        PHYSICIAN_FIRST_NAME,
        PHYSICIAN_LAST_NAME,
        ADDRESS_LINE_1,
        ADDRESS_LINE_2,
        CITY,
        COUNTY,
        STATE,
        ZIP_CODE,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS

    FROM ranked_source
    WHERE row_num = 1
      AND CDC_OP != 'DELETE'

)

{% if is_incremental() %}

SELECT
    l.REFERRING_PHYSICIAN_KEY,
    l.SOURCE_SYSTEM_ID,
    l.SYSTEM_CODE,
    l.PHYSICIAN_ID,
    l.PHYSICIAN_OFFICE,
    l.PHYSICIAN_GROUP,
    l.PHYSICIAN_FIRST_NAME,
    l.PHYSICIAN_LAST_NAME,
    l.ADDRESS_LINE_1,
    l.ADDRESS_LINE_2,
    l.CITY,
    l.COUNTY,
    l.STATE,
    l.ZIP_CODE,
    l.IS_ACTIVE,
    l.CDC_OP,
    l.CDC_TS,
    COALESCE(t.INSERTED_DATE, CURRENT_TIMESTAMP())  AS INSERTED_DATE,
    COALESCE(t.INSERTED_BY,   'ETL_PROCESS')        AS INSERTED_BY,
    CURRENT_TIMESTAMP()                             AS UPDATED_DATE,
    'ETL_PROCESS'                                   AS UPDATED_BY

FROM latest_records l
LEFT JOIN {{ this }} t ON l.REFERRING_PHYSICIAN_KEY = t.REFERRING_PHYSICIAN_KEY

{% else %}

SELECT
    REFERRING_PHYSICIAN_KEY,
    SOURCE_SYSTEM_ID,
    SYSTEM_CODE,
    PHYSICIAN_ID,
    PHYSICIAN_OFFICE,
    PHYSICIAN_GROUP,
    PHYSICIAN_FIRST_NAME,
    PHYSICIAN_LAST_NAME,
    ADDRESS_LINE_1,
    ADDRESS_LINE_2,
    CITY,
    COUNTY,
    STATE,
    ZIP_CODE,
    IS_ACTIVE,
    CDC_OP,
    CDC_TS,
    CURRENT_TIMESTAMP()     AS INSERTED_DATE,
    'ETL_PROCESS'           AS INSERTED_BY,
    CURRENT_TIMESTAMP()     AS UPDATED_DATE,
    'ETL_PROCESS'           AS UPDATED_BY

FROM latest_records

{% endif %}