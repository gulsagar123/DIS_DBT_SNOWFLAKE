{# Automated Python Compilation for Pipeline: BRANCHES_PL #}
{{
    config(
        materialized='incremental',
        unique_key='branch_resourceId',
        incremental_strategy='merge',
        alias='BRANCHES_HCHB_CLEANSED'
    )
}}

SELECT
    NULLIF(TRIM(TRIM("BRANCH_FAX")), '') AS "BRANCH_FAX",
    CASE
    WHEN CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("BRANCH_EMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END
  END AS "BRANCH_EMAIL",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_CITY")), ''), '[^A-Za-z ]+', '')) AS "BRANCH_CITY",
    NULLIF(TRIM(TRIM("BRANCH_PARENT")), '') AS "BRANCH_PARENT",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_LASTUPDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "BRANCH_LASTUPDATE",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_HOSSTATELICENSE")), ''), '[^A-Za-z ]+', '')) AS "BRANCH_HOSSTATELICENSE",
    NULLIF(TRIM(TRIM("BRANCH_LATITUDE")), '') AS "BRANCH_LATITUDE",
    NULLIF(TRIM(TRIM("BRANCH_INCLUDEINALL")), '') AS "BRANCH_INCLUDEINALL",
    NULLIF(TRIM(TRIM("BRANCH_LONGITUDE")), '') AS "BRANCH_LONGITUDE",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_NAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_NAME")), ''), '[^A-Za-z ]+', ''))) AS "BRANCH_NAME",
    NULLIF(TRIM(TRIM("BRANCH_CODE")), '') AS "BRANCH_CODE",
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_STREET")), ''), '[^A-Za-z0-9 ,.#/()_-]+', ''), '[[:space:]]+', ' ')) AS "BRANCH_STREET",
    NULLIF(TRIM(TRIM("BRANCH_ACTIVE")), '') AS "BRANCH_ACTIVE",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_SMSHELPPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_SMSHELPPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "BRANCH_SMSHELPPHONE",
    NULLIF(TRIM(TRIM("BRANCH_PROVIDERNUMBER")), '') AS "BRANCH_PROVIDERNUMBER",
    NULLIF(TRIM(TRIM("BRANCH_INCLUDEONPHYSICIANWEBSITE")), '') AS "BRANCH_INCLUDEONPHYSICIANWEBSITE",
    NULLIF(TRIM(TRIM("BRANCH_LATLONGMETHOD")), '') AS "BRANCH_LATLONGMETHOD",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM(TRIM("BRANCH_CORPOFFICE")), '') AS "BRANCH_CORPOFFICE",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_STATE")), ''), '[^A-Za-z ]+', '')) AS "BRANCH_STATE",
    NULLIF(TRIM(TRIM("BRANCH_BRANCHGL")), '') AS "BRANCH_BRANCHGL",
    CASE 
     WHEN NULLIF(TRIM(TRIM("BRANCH_ZIP")), '') IS NULL THEN NULL
     ELSE REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_ZIP")), ''), '[^0-9]+', '')
 END AS "BRANCH_ZIP",
    NULLIF(TRIM(TRIM("BRANCH_FEDERALTAXID")), '') AS "BRANCH_FEDERALTAXID",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_PHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_PHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "BRANCH_PHONE",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_FRIENDLYNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_FRIENDLYNAME")), ''), '[^A-Za-z ]+', ''))) AS "BRANCH_FRIENDLYNAME",
    NULLIF(TRIM(TRIM("BRANCH_RESOURCEID")), '') AS "BRANCH_RESOURCEID",
    NULLIF(TRIM(TRIM("BRANCH_IDENTITY")), '') AS "BRANCH_IDENTITY",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_STATELICENSE")), ''), '[^A-Za-z ]+', '')) AS "BRANCH_STATELICENSE",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_INSERTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "BRANCH_INSERTDATE",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_SCHEDULINGPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("BRANCH_SCHEDULINGPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "BRANCH_SCHEDULINGPHONE",
    NULLIF(TRIM(TRIM("BRANCH_REGID")), '') AS "BRANCH_REGID",
    NULLIF(TRIM(TRIM("BRANCH_CONTACT")), '') AS "BRANCH_CONTACT",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.BRANCHES
