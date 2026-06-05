{# Automated Python Compilation for Pipeline: CAREPLANS_PL #}
{{
    config(
        materialized='incremental',
        unique_key='CP_ID',
        incremental_strategy='merge',
        alias='CAREPLANS_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("CP_CAID")), '') AS "CP_CAID",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_MODIFIEDDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_MODIFIEDDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_MODIFIEDDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_MODIFIEDDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_MODIFIEDDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_MODIFIEDDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_MODIFIEDDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "CP_MODIFIEDDATE",
    NULLIF(TRIM(TRIM("CP_MODIFIEDBY")), '') AS "CP_MODIFIEDBY",
    NULLIF(TRIM(TRIM("CP_ID")), '') AS "CP_ID",
    NULLIF(TRIM(TRIM("CP_OID")), '') AS "CP_OID",
    NULLIF(TRIM(TRIM("CP_INSERTEDBY")), '') AS "CP_INSERTEDBY",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("CP_INSERTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "CP_INSERTDATE",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM(TRIM("CP_PAID")), '') AS "CP_PAID",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.CAREPLANS

{% if is_incremental() %}
  WHERE "UPDATED_DATE" > DATEADD(minute, -5, (SELECT MAX("UPDATED_DATE") FROM {{ this }}))
{% endif %}
