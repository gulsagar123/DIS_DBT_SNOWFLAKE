{# Automated Python Compilation for Pipeline: CLIENT_ORDERS_ALL_PL #}
{{
    config(
        materialized='incremental',
        unique_key='O_ID',
        incremental_strategy='merge',
        alias='CLIENT_ORDERS_ALL_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("O_CALCULATEDFREQ")), '') AS "O_CALCULATEDFREQ",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_ORDERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_ORDERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_ORDERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_ORDERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_ORDERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_ORDERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_ORDERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "O_ORDERDATE",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_LASTUPDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "O_LASTUPDATE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_EFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_EFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_EFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_EFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_EFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_EFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_EFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "O_EFFECTIVEDATE",
    NULLIF(TRIM(TRIM("O_ORDERMERGEDTOORDERID")), '') AS "O_ORDERMERGEDTOORDERID",
    NULLIF(TRIM(TRIM("O_DISCIPLINE")), '') AS "O_DISCIPLINE",
    NULLIF(TRIM(TRIM("O_DELETED")), '') AS "O_DELETED",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEAPPROVED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEAPPROVED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEAPPROVED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEAPPROVED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEAPPROVED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEAPPROVED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEAPPROVED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "O_DATEAPPROVED",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEDECLINED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEDECLINED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEDECLINED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEDECLINED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEDECLINED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEDECLINED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEDECLINED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "O_DATEDECLINED",
    NULLIF(TRIM(TRIM("O_OTGID")), '') AS "O_OTGID",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_INSERTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "O_INSERTDATE",
    NULLIF(TRIM(TRIM("O_ID")), '') AS "O_ID",
    NULLIF(TRIM(TRIM("O_ORDERFREQ")), '') AS "O_ORDERFREQ",
    NULLIF(TRIM(TRIM("O_MRNUM")), '') AS "O_MRNUM",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM(TRIM("O_RESOURCEID")), '') AS "O_RESOURCEID",
    NULLIF(TRIM(TRIM("O_PAPERNARRATIVE")), '') AS "O_PAPERNARRATIVE",
    NULLIF(TRIM(TRIM("O_CEVID")), '') AS "O_CEVID",
    NULLIF(TRIM(TRIM("O_DESC")), '') AS "O_DESC",
    NULLIF(TRIM(TRIM("O_LOC")), '') AS "O_LOC",
    NULLIF(TRIM(TRIM("O_SECPHID")), '') AS "O_SECPHID",
    NULLIF(TRIM(TRIM("O_POID")), '') AS "O_POID",
    NULLIF(TRIM(TRIM("O_ORDERNUMBER")), '') AS "O_ORDERNUMBER",
    NULLIF(TRIM(TRIM("O_GOALS")), '') AS "O_GOALS",
    NULLIF(TRIM(TRIM("O_OTID")), '') AS "O_OTID",
    NULLIF(TRIM(TRIM("O_ORDERS")), '') AS "O_ORDERS",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_F2FENCOUNTERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_F2FENCOUNTERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_F2FENCOUNTERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_F2FENCOUNTERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_F2FENCOUNTERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_F2FENCOUNTERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_F2FENCOUNTERDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "O_F2FENCOUNTERDATE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEENTERED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEENTERED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEENTERED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEENTERED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEENTERED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEENTERED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("O_DATEENTERED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "O_DATEENTERED",
    NULLIF(TRIM(TRIM("O_PHID")), '') AS "O_PHID",
    NULLIF(TRIM(TRIM("O_EPIID")), '') AS "O_EPIID",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.CLIENT_ORDERS_ALL

{% if is_incremental() %}
  WHERE "UPDATED_DATE" > DATEADD(minute, -5, (SELECT MAX("UPDATED_DATE") FROM {{ this }}))
{% endif %}
