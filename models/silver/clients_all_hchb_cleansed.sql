{# Automated Python Compilation for Pipeline: CLIENTS_ALL_PL #}
{{
    config(
        materialized='incremental',
        unique_key='PA_ID',
        incremental_strategy='merge',
        alias='CLIENTS_ALL_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("PA_ID")), '') AS "PA_ID",
    NULLIF(TRIM(TRIM("PA_MEDICAIDNUMBER")), '') AS "PA_MEDICAIDNUMBER",
    NULLIF(TRIM(TRIM("PA_MI")), '') AS "PA_MI",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_APHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_APHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "PA_APHONE",
    UPPER(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_STATUS")), ''), '[^A-Za-z ]+', '')) AS "PA_STATUS",
    UPPER(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MARITALSTATUS")), ''), '[^A-Za-z ]+', '')) AS "PA_MARITALSTATUS",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_NICKNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_NICKNAME")), ''), '[^A-Za-z ]+', ''))) AS "PA_NICKNAME",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) AS "PA_FIRSTNAME",
    CASE
    WHEN NULLIF(TRIM(TRIM("PA_GENDER")), '') IS NULL THEN NULL
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("PA_GENDER")), ''))) IN ('M','MALE','MAN') THEN 'M'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("PA_GENDER")), ''))) IN ('F','FEMALE','WOMAN') THEN 'F'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("PA_GENDER")), ''))) IN ('U','UNK','UNKNOWN','UNSPECIFIED','N/A','NA','OTHER') THEN 'U'
    ELSE 'U'
  END AS "PA_GENDER",
    NULLIF(TRIM(TRIM("PA_LIVINGARRANGMENTS")), '') AS "PA_LIVINGARRANGMENTS",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "PA_MPHONE",
    REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_SSN")), ''), '[^0-9]+', '') AS "PA_SSN",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_LASTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_LASTNAME")), ''), '[^A-Za-z ]+', ''))) AS "PA_LASTNAME",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAIDEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAIDEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAIDEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAIDEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAIDEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAIDEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAIDEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "PA_MEDICAIDEFFECTIVEDATE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREBEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREBEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREBEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREBEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREBEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREBEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREBEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "PA_MEDICAREBEFFECTIVEDATE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREAEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREAEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREAEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREAEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREAEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREAEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_MEDICAREAEFFECTIVEDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "PA_MEDICAREAEFFECTIVEDATE",
    CASE
    WHEN CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("PA_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END
  END AS "PA_EMAIL",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "PA_DOB",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_WPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("PA_WPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "PA_WPHONE",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.CLIENTS_ALL

{% if is_incremental() %}
  WHERE "UPDATED_DATE" > DATEADD(minute, -5, (SELECT MAX("UPDATED_DATE") FROM {{ this }}))
{% endif %}
