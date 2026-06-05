{# Automated Python Compilation for Pipeline: REFERRAL_SOURCES_PL #}
{{
    config(
        materialized='incremental',
        unique_key='RS_ID',
        incremental_strategy='merge',
        alias='REFERRAL_SOURCES_HCHB_CLEANSED'
    )
}}

SELECT
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    NULLIF(TRIM(TRIM("RS_ACTIVE")), '') AS "RS_ACTIVE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEUPDATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEUPDATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEUPDATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEUPDATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEUPDATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEUPDATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEUPDATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RS_DATEUPDATED",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM(TRIM("RS_ID")), '') AS "RS_ID",
    NULLIF(TRIM(TRIM("RS_DESCRIPTION")), '') AS "RS_DESCRIPTION",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEINSERTED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEINSERTED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEINSERTED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEINSERTED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEINSERTED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEINSERTED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RS_DATEINSERTED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RS_DATEINSERTED",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.REFERRAL_SOURCES
