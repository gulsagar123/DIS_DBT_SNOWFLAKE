{{
    config(
        materialized='incremental',
        schema='SILVER_DBT',
        alias='DIM_SERVICE_LINE',
        incremental_strategy='merge',
        unique_key='SERVICE_LINE_KEY',
        on_schema_change='sync_all_columns'
    )
}}

/*
    Silver Layer - DIM_SERVICE_LINE
    --------------------------------
    - Source    : STAGE_DIM_SERVICE_LINE (staging layer)
    - Strategy  : MERGE INTO using CDC_OP and CDC_TS
    - SK        : SERVICE_LINE_KEY generated from SOURCE_SYSTEM_ID + SERVICE_LINE_ID
    - CDC_OP    : INSERT, UPDATE, DELETE
    - CDC_TS    : Used to pick latest record per key
    - Audit     : INSERTED_DATE, INSERTED_BY, UPDATED_DATE, UPDATED_BY
*/

WITH ranked_source AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['SOURCE_SYSTEM_ID', 'SERVICE_LINE_ID']) }}
                                AS SERVICE_LINE_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        SERVICE_LINE_ID,
        SERVICE_LINE_NAME,
        SERVICE_LINE_DESCRIPTION,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS,

        ROW_NUMBER() OVER (
            PARTITION BY SOURCE_SYSTEM_ID, SERVICE_LINE_ID
            ORDER BY CDC_TS DESC
        ) AS row_num

    FROM {{ ref('stg_dim_service_line') }}

    {% if is_incremental() %}
        WHERE CDC_TS > (
            SELECT COALESCE(MAX(CDC_TS), '1900-01-01'::TIMESTAMP)
            FROM {{ this }}
        )
    {% endif %}

),

latest_records AS (

    SELECT
        SERVICE_LINE_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        SERVICE_LINE_ID,
        SERVICE_LINE_NAME,
        SERVICE_LINE_DESCRIPTION,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS

    FROM ranked_source
    WHERE row_num = 1
      AND CDC_OP != 'DELETE'

)

{% if is_incremental() %}

    -- MERGE: existing records get UPDATED_DATE and UPDATED_BY refreshed
    SELECT
        l.SERVICE_LINE_KEY,
        l.SOURCE_SYSTEM_ID,
        l.SYSTEM_CODE,
        l.SERVICE_LINE_ID,
        l.SERVICE_LINE_NAME,
        l.SERVICE_LINE_DESCRIPTION,
        l.IS_ACTIVE,
        l.CDC_OP,
        l.CDC_TS,
        COALESCE(t.INSERTED_DATE, CURRENT_TIMESTAMP())     AS INSERTED_DATE,
        COALESCE(t.INSERTED_BY,   'ETL_PROCESS')           AS INSERTED_BY,
        CURRENT_TIMESTAMP()                                 AS UPDATED_DATE,
        'ETL_PROCESS'                                       AS UPDATED_BY

    FROM latest_records l
    LEFT JOIN {{ this }} t
        ON l.SERVICE_LINE_KEY = t.SERVICE_LINE_KEY

{% else %}

    -- FULL REFRESH: all records are new inserts
    SELECT
        SERVICE_LINE_KEY,
        SOURCE_SYSTEM_ID,
        SYSTEM_CODE,
        SERVICE_LINE_ID,
        SERVICE_LINE_NAME,
        SERVICE_LINE_DESCRIPTION,
        IS_ACTIVE,
        CDC_OP,
        CDC_TS,
        CURRENT_TIMESTAMP()     AS INSERTED_DATE,
        'ETL_PROCESS'           AS INSERTED_BY,
        CURRENT_TIMESTAMP()     AS UPDATED_DATE,
        'ETL_PROCESS'           AS UPDATED_BY

    FROM latest_records

{% endif %}