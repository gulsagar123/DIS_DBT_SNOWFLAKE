{# Automated Python Compilation for Pipeline: WORKER_BASE_PL #}
{{
    config(
        materialized='incremental',
        unique_key='wkr_id',
        incremental_strategy='merge',
        alias='WORKER_BASE_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("WKR_HCHBEMPLOYEE")), '') AS "WKR_HCHBEMPLOYEE",
    CASE
    WHEN CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("WKR_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END
  END AS "WKR_EMAIL",
    NULLIF(TRIM(TRIM("WKR_ISSATINWORKWEEK")), '') AS "WKR_ISSATINWORKWEEK",
    NULLIF(TRIM(TRIM("WKR_MAXVISITSPERWEEK")), '') AS "WKR_MAXVISITSPERWEEK",
    NULLIF(TRIM(TRIM("WKR_LATITUDE")), '') AS "WKR_LATITUDE",
    NULLIF(TRIM(TRIM("WKR_ISTUEINWORKWEEK")), '') AS "WKR_ISTUEINWORKWEEK",
    NULLIF(TRIM(TRIM("WKR_PIN")), '') AS "WKR_PIN",
    NULLIF(TRIM(TRIM("WKR_MAXVISITSPERDAY")), '') AS "WKR_MAXVISITSPERDAY",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_FULLNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_FULLNAME")), ''), '[^A-Za-z ]+', ''))) AS "WKR_FULLNAME",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_CELLPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_CELLPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "WKR_CELLPHONE",
    NULLIF(TRIM(TRIM("WKR_ISWEDINWORKWEEK")), '') AS "WKR_ISWEDINWORKWEEK",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_STATE")), ''), '[^A-Za-z ]+', '')) AS "WKR_STATE",
    NULLIF(TRIM(TRIM("WKR_ISSUNINWORKWEEK")), '') AS "WKR_ISSUNINWORKWEEK",
    NULLIF(TRIM(TRIM("WKR_LONGITUDE")), '') AS "WKR_LONGITUDE",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_HOMEPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_HOMEPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "WKR_HOMEPHONE",
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_STREET")), ''), '[^A-Za-z0-9 ,.#/()_-]+', ''), '[[:space:]]+', ' ')) AS "WKR_STREET",
    NULLIF(TRIM(TRIM("WKR_RESOURCEID")), '') AS "WKR_RESOURCEID",
    NULLIF(TRIM(TRIM("WKR_ID")), '') AS "WKR_ID",
    NULLIF(TRIM(TRIM("WKR_ISFRIINWORKWEEK")), '') AS "WKR_ISFRIINWORKWEEK",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_CITY")), ''), '[^A-Za-z ]+', '')) AS "WKR_CITY",
    NULLIF(TRIM(TRIM("WKR_COUNTY")), '') AS "WKR_COUNTY",
    NULLIF(TRIM(TRIM("WKR_MAXHOURSPERWEEK")), '') AS "WKR_MAXHOURSPERWEEK",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_LASTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_LASTNAME")), ''), '[^A-Za-z ]+', ''))) AS "WKR_LASTNAME",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) AS "WKR_FIRSTNAME",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATETERMINATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATETERMINATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATETERMINATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATETERMINATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATETERMINATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATETERMINATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATETERMINATED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "WKR_DATETERMINATED",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "WKR_DOB",
    NULLIF(TRIM(TRIM("WKR_MAXHOURSPERDAY")), '') AS "WKR_MAXHOURSPERDAY",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_WORKPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_WORKPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "WKR_WORKPHONE",
    NULLIF(TRIM(TRIM("WKR_PRIMARYJDID")), '') AS "WKR_PRIMARYJDID",
    NULLIF(TRIM(TRIM("WKR_ISTHUINWORKWEEK")), '') AS "WKR_ISTHUINWORKWEEK",
    CASE
    WHEN NULLIF(TRIM(TRIM("WKR_GENDER")), '') IS NULL THEN NULL
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("WKR_GENDER")), ''))) IN ('M','MALE','MAN') THEN 'M'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("WKR_GENDER")), ''))) IN ('F','FEMALE','WOMAN') THEN 'F'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("WKR_GENDER")), ''))) IN ('U','UNK','UNKNOWN','UNSPECIFIED','N/A','NA','OTHER') THEN 'U'
    ELSE 'U'
  END AS "WKR_GENDER",
    CASE 
     WHEN NULLIF(TRIM(TRIM("WKR_ZIP")), '') IS NULL THEN NULL
     ELSE REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_ZIP")), ''), '[^0-9]+', '')
 END AS "WKR_ZIP",
    REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_SSN")), ''), '[^0-9]+', '') AS "WKR_SSN",
    NULLIF(TRIM(TRIM("WKR_MI")), '') AS "WKR_MI",
    NULLIF(TRIM(TRIM("WKR_ISMONINWORKWEEK")), '') AS "WKR_ISMONINWORKWEEK",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATEHIRED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATEHIRED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATEHIRED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATEHIRED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATEHIRED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATEHIRED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("WKR_DATEHIRED")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "WKR_DATEHIRED",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.WORKER_BASE
