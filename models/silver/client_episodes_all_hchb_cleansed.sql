{# Automated Python Compilation for Pipeline: CLIENT_EPISODES_ALL_PL #}
{{
    config(
        materialized='incremental',
        unique_key='EPI_ID',
        incremental_strategy='merge',
        alias='CLIENT_EPISODES_ALL_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("EPI_PHID")), '') AS "EPI_PHID",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_WPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_WPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "EPI_WPHONE",
    NULLIF(TRIM(TRIM("EPI_SLID")), '') AS "EPI_SLID",
    REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SSN")), ''), '[^0-9]+', '') AS "EPI_SSN",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_SOCDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "EPI_SOCDATE",
    NULLIF(TRIM(TRIM("EPI_TEAMID")), '') AS "EPI_TEAMID",
    NULLIF(TRIM(TRIM("EPI_ADDONSERVICECODE2")), '') AS "EPI_ADDONSERVICECODE2",
    NULLIF(TRIM(TRIM("EPI_DOLASTSEEN")), '') AS "EPI_DOLASTSEEN",
    NULLIF(TRIM(TRIM("EPI_WEIGHT")), '') AS "EPI_WEIGHT",
    NULLIF(TRIM(TRIM("EPI_STARTOFEPISODE")), '') AS "EPI_STARTOFEPISODE",
    NULLIF(TRIM(TRIM("EPI_RRID")), '') AS "EPI_RRID",
    NULLIF(TRIM(TRIM("EPI_PRID")), '') AS "EPI_PRID",
    NULLIF(TRIM(TRIM("EPI_ENDOFEPISODE")), '') AS "EPI_ENDOFEPISODE",
    NULLIF(TRIM(TRIM("EPI_ID")), '') AS "EPI_ID",
    UPPER(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_MARITALSTATUS")), ''), '[^A-Za-z ]+', '')) AS "EPI_MARITALSTATUS",
    CASE
    WHEN NULLIF(TRIM(TRIM("EPI_GENDER")), '') IS NULL THEN NULL
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("EPI_GENDER")), ''))) IN ('M','MALE','MAN') THEN 'M'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("EPI_GENDER")), ''))) IN ('F','FEMALE','WOMAN') THEN 'F'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("EPI_GENDER")), ''))) IN ('U','UNK','UNKNOWN','UNSPECIFIED','N/A','NA','OTHER') THEN 'U'
    ELSE 'U'
  END AS "EPI_GENDER",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) AS "EPI_FIRSTNAME",
    NULLIF(TRIM(TRIM("EPI_DCRID")), '') AS "EPI_DCRID",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_LASTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_LASTNAME")), ''), '[^A-Za-z ]+', ''))) AS "EPI_LASTNAME",
    NULLIF(TRIM(TRIM("EPI_ADMITDISCIPLINE")), '') AS "EPI_ADMITDISCIPLINE",
    NULLIF(TRIM(TRIM("EPI_DCCODE")), '') AS "EPI_DCCODE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DISCHARGEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DISCHARGEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DISCHARGEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DISCHARGEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DISCHARGEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DISCHARGEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DISCHARGEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "EPI_DISCHARGEDATE",
    NULLIF(TRIM(TRIM("EPI_PAID")), '') AS "EPI_PAID",
    NULLIF(TRIM(TRIM("EPI_REFERRALSOURCE")), '') AS "EPI_REFERRALSOURCE",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM(TRIM("EPI_AGID")), '') AS "EPI_AGID",
    NULLIF(TRIM(TRIM("EPI_LOCID")), '') AS "EPI_LOCID",
    NULLIF(TRIM(TRIM("EPI_TIMING")), '') AS "EPI_TIMING",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_LASTUPDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "EPI_LASTUPDATE",
    NULLIF(TRIM(TRIM("EPI_BRANCHCODE")), '') AS "EPI_BRANCHCODE",
    NULLIF(TRIM(TRIM("EPI_RECERTFLAG")), '') AS "EPI_RECERTFLAG",
    NULLIF(TRIM(TRIM("EPI_ADMITTYPE")), '') AS "EPI_ADMITTYPE",
    NULLIF(TRIM(TRIM("EPI_ADMITSERVICECODE")), '') AS "EPI_ADMITSERVICECODE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "EPI_DOB",
    NULLIF(TRIM(TRIM("EPI_REFERRALCONTACT")), '') AS "EPI_REFERRALCONTACT",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    NULLIF(TRIM(TRIM("EPI_ADDONSERVICECODE")), '') AS "EPI_ADDONSERVICECODE",
    CASE
    WHEN CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END IS NULL THEN NULL
    WHEN NOT REGEXP_LIKE(CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END, '^[a-z0-9._%+-]+@[a-z0-9.-]+[.][a-z]{2,}$') THEN NULL
    WHEN SPLIT_PART(CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END, '@', 2) IN ('test.com','example.com','fake.com') THEN NULL
    ELSE CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("EPI_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END
  END AS "EPI_EMAIL",
    NULLIF(TRIM(TRIM("EPI_REFERRALRELID")), '') AS "EPI_REFERRALRELID",
    NULLIF(TRIM(TRIM("EPI_FAID")), '') AS "EPI_FAID",
    NULLIF(TRIM(TRIM("EPI_REFERRALTAKENBY")), '') AS "EPI_REFERRALTAKENBY",
    NULLIF(TRIM(TRIM("EPI_PHID2")), '') AS "EPI_PHID2",
    NULLIF(TRIM(TRIM("EPI_MODIFIEDBY")), '') AS "EPI_MODIFIEDBY",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DATEOFREFERRAL")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DATEOFREFERRAL")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DATEOFREFERRAL")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DATEOFREFERRAL")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DATEOFREFERRAL")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DATEOFREFERRAL")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_DATEOFREFERRAL")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "EPI_DATEOFREFERRAL",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_INSERTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "EPI_INSERTDATE",
    NULLIF(TRIM(TRIM("EPI_MRNUM")), '') AS "EPI_MRNUM",
    NULLIF(TRIM(TRIM("EPI_PHYSINFO")), '') AS "EPI_PHYSINFO",
    UPPER(REGEXP_REPLACE(NULLIF(TRIM(TRIM("EPI_STATUS")), ''), '[^A-Za-z ]+', '')) AS "EPI_STATUS",
    NULLIF(TRIM(TRIM("EPI_HEIGHT")), '') AS "EPI_HEIGHT",
    NULLIF(TRIM(TRIM("EPI_POID")), '') AS "EPI_POID",
    NULLIF(TRIM(TRIM("EPI_PHID1")), '') AS "EPI_PHID1",
    NULLIF(TRIM(TRIM("EPI_NUMOFINITIATION")), '') AS "EPI_NUMOFINITIATION",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.CLIENT_EPISODES_ALL

{% if is_incremental() %}
  WHERE "UPDATED_DATE" > DATEADD(minute, -5, (SELECT MAX("UPDATED_DATE") FROM {{ this }}))
{% endif %}
