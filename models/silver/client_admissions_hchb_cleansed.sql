{# Automated Python Compilation for Pipeline: CLIENT_ADMISSIONS_PL #}
{{
    config(
        materialized='incremental',
        unique_key='CA_ID',
        incremental_strategy='merge',
        alias='CLIENT_ADMISSIONS_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    NULLIF(TRIM(TRIM("CA_RRID")), '') AS "CA_RRID",
    NULLIF(TRIM(TRIM("CA_SLID")), '') AS "CA_SLID",
    NULLIF(TRIM(TRIM("CA_PAID")), '') AS "CA_PAID",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "CA_SOCDATE",
    NULLIF(TRIM(TRIM("CA_ID")), '') AS "CA_ID",
    NULLIF(TRIM(TRIM("CA_CEVID")), '') AS "CA_CEVID",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_HCHBSTARTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_HCHBSTARTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_HCHBSTARTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_HCHBSTARTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_HCHBSTARTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_HCHBSTARTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CA_HCHBSTARTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "CA_HCHBSTARTDATE",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.CLIENT_ADMISSIONS

{% if is_incremental() %}
  WHERE "UPDATED_DATE" > DATEADD(minute, -5, (SELECT MAX("UPDATED_DATE") FROM {{ this }}))
{% endif %}
