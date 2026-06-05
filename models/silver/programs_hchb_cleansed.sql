{# Automated Python Compilation for Pipeline: PROGRAMS_PL #}
{{
    config(
        materialized='incremental',
        unique_key='PG_ID',
        incremental_strategy='merge',
        alias='PROGRAMS_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM(TRIM("PG_ID")), '') AS "PG_ID",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PG_INSERTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "PG_INSERTDATE",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    NULLIF(TRIM(TRIM("PG_ACTIVE")), '') AS "PG_ACTIVE",
    NULLIF(TRIM(TRIM("PG_PNID")), '') AS "PG_PNID",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PG_LASTUPDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "PG_LASTUPDATE",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.PROGRAMS
