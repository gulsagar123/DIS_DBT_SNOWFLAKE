{# Automated Python Compilation for Pipeline: REFERRAL_REQUESTS_PL #}
{{
    config(
        materialized='incremental',
        unique_key='RR_ID',
        incremental_strategy='merge',
        alias='REFERRAL_REQUESTS_HCHB_CLEANSED'
    )
}}

SELECT
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_INSERTDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "RR_INSERTDATE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_DOB")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RR_DOB",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RR_ONSETEXACERBATIONDATE",
    NULLIF(TRIM(TRIM("RR_ONSETEXACERBATION")), '') AS "RR_ONSETEXACERBATION",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATIONDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RR_SECONSETEXACERBATIONDATE",
    NULLIF(TRIM(TRIM("RR_ICID")), '') AS "RR_ICID",
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_STREET")), ''), '[^A-Za-z0-9 ,.#/()_-]+', ''), '[[:space:]]+', ' ')) AS "RR_STREET",
    NULLIF(TRIM(TRIM("RR_COUNTY")), '') AS "RR_COUNTY",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ICPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ICPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "RR_ICPHONE",
    NULLIF(TRIM(TRIM("RR_PRIMARYDX")), '') AS "RR_PRIMARYDX",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RR_REFERRALDATE",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTWPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTWPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "RR_CONTACTWPHONE",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CITY")), ''), '[^A-Za-z ]+', '')) AS "RR_CITY",
    NULLIF(TRIM(TRIM("RR_POID")), '') AS "RR_POID",
    CASE 
     WHEN NULLIF(TRIM(TRIM("RR_ZIP")), '') IS NULL THEN NULL
     ELSE REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_ZIP")), ''), '[^0-9]+', '')
 END AS "RR_ZIP",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_PHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_PHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "RR_PHONE",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRAL_READY_DATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRAL_READY_DATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRAL_READY_DATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRAL_READY_DATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRAL_READY_DATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRAL_READY_DATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_REFERRAL_READY_DATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RR_REFERRAL_READY_DATE",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTCITY")), ''), '[^A-Za-z ]+', '')) AS "RR_CONTACTCITY",
    NULLIF(TRIM(TRIM("RR_FAKID")), '') AS "RR_FAKID",
    NULLIF(TRIM(TRIM("RR_CCTID")), '') AS "RR_CCTID",
    NULLIF(TRIM(TRIM("RR_PHYSINFO")), '') AS "RR_PHYSINFO",
    NULLIF(TRIM(TRIM("RR_ORDSERV")), '') AS "RR_ORDSERV",
    NULLIF(TRIM(TRIM("RR_PSID")), '') AS "RR_PSID",
    REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_SSN")), ''), '[^0-9]+', '') AS "RR_SSN",
    NULLIF(TRIM(TRIM("RR_SECONSETEXACERBATION")), '') AS "RR_SECONSETEXACERBATION",
    NULLIF(TRIM(TRIM("REFERRAL_CHANNEL")), '') AS "REFERRAL_CHANNEL",
    NULLIF(TRIM(TRIM("RR_PHKID")), '') AS "RR_PHKID",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_LASTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_LASTNAME")), ''), '[^A-Za-z ]+', ''))) AS "RR_LASTNAME",
    TRIM(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTSTREET")), ''), '[^A-Za-z0-9 ,.#/()_-]+', ''), '[[:space:]]+', ' ')) AS "RR_CONTACTSTREET",
    NULLIF(TRIM(TRIM("RR_ID")), '') AS "RR_ID",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_FIRSTNAME")), ''), '[^A-Za-z ]+', ''))) AS "RR_FIRSTNAME",
    NULLIF(REGEXP_REPLACE(IFF(LENGTH(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTHPHONE")), ''), '[^0-9]+', '')) IN (10), REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTHPHONE")), ''), '[^0-9]+', ''), NULL), '^(0+|1+|2+|3+|4+|5+|6+|7+|8+|9+)$', ''), '') AS "RR_CONTACTHPHONE",
    CASE
    WHEN NULLIF(TRIM(TRIM("RR_GENDER")), '') IS NULL THEN NULL
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("RR_GENDER")), ''))) IN ('M','MALE','MAN') THEN 'M'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("RR_GENDER")), ''))) IN ('F','FEMALE','WOMAN') THEN 'F'
    WHEN UPPER(TRIM(NULLIF(TRIM(TRIM("RR_GENDER")), ''))) IN ('U','UNK','UNKNOWN','UNSPECIFIED','N/A','NA','OTHER') THEN 'U'
    ELSE 'U'
  END AS "RR_GENDER",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTLASTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTLASTNAME")), ''), '[^A-Za-z ]+', ''))) AS "RR_CONTACTLASTNAME",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_STATE")), ''), '[^A-Za-z ]+', '')) AS "RR_STATE",
    NULLIF(TRIM(TRIM("RR_HOSMRNUM")), '') AS "RR_HOSMRNUM",
    CASE
    WHEN CASE
    WHEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
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
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END IS NULL THEN NULL
    WHEN REGEXP_COUNT(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@') <> 1 THEN CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END
    ELSE
      /* fix @. and .@, collapse dots */
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(CASE
    WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@') IS NULL THEN NULL
    WHEN REGEXP_COUNT(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '@') <= 1 THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@')
    ELSE REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '^[^@]+') || '@' || REGEXP_SUBSTR(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(LOWER(TRIM(NULLIF(TRIM(TRIM("RR_CONTACTEMAIL")), ''))), '[[:space:]]+', ''), '[^a-z0-9._%+@-]+', ''), '@{2,}', '@'), '[^@]+$')
  END, '@[.]', '@'),
            '[.]@', '@'
          ),
          '[.]{2,}', '.'
        ),
        '^[.]+|[.]+$', ''
      )
  END
  END AS "RR_CONTACTEMAIL",
    NULLIF(TRIM(TRIM("RR_MEDICARENUM")), '') AS "RR_MEDICARENUM",
    NULLIF(TRIM(TRIM("CDC_OP")), '') AS "CDC_OP",
    NULLIF(TRIM(TRIM("RR_ADMITDISCIPLINE")), '') AS "RR_ADMITDISCIPLINE",
    NULLIF(TRIM(TRIM("RR_FAID")), '') AS "RR_FAID",
    TRY_TO_TIMESTAMP(NULLIF(TRIM(TRIM("CDC_TS")), '')) AS "CDC_TS",
    TRY_TO_DATE(
    CASE
      WHEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_EVALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-') IS NULL THEN NULL
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_EVALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}$') THEN REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_EVALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')
      WHEN REGEXP_LIKE(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_EVALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '^[0-9]{1,2}-[0-9]{1,2}-[0-9]{4}$')
        THEN SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_EVALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 3) || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_EVALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 2), 2, '0') || '-' ||
             LPAD(SPLIT_PART(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_EVALDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-'), '-', 1), 2, '0')
      ELSE NULL
    END
) AS "RR_EVALDATE",
    NULLIF(TRIM(TRIM("RR_BRANCHCODE")), '') AS "RR_BRANCHCODE",
    NULLIF(TRIM(TRIM("RR_RELID")), '') AS "RR_RELID",
    NULLIF(TRIM(TRIM("RR_RRRRID")), '') AS "RR_RRRRID",
    INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTSTATE")), ''), '[^A-Za-z ]+', '')) AS "RR_CONTACTSTATE",
    NULLIF(TRIM(TRIM("RR_SECONDARYDX")), '') AS "RR_SECONDARYDX",
    NULLIF(TRIM(TRIM("RR_RSID")), '') AS "RR_RSID",
    NULLIF(TRIM(TRIM("RR_PAID")), '') AS "RR_PAID",
    NULLIF(TRIM(TRIM("RR_SLID")), '') AS "RR_SLID",
    IFF(UPPER(INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTFIRSTNAME")), ''), '[^A-Za-z ]+', ''))) IN ('TEST','NA','N/A','UNKNOWN','DUMMY','NONE','DEFAULT'), NULL, INITCAP(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTFIRSTNAME")), ''), '[^A-Za-z ]+', ''))) AS "RR_CONTACTFIRSTNAME",
    CASE 
     WHEN NULLIF(TRIM(TRIM("RR_CONTACTZIP")), '') IS NULL THEN NULL
     ELSE REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_CONTACTZIP")), ''), '[^0-9]+', '')
 END AS "RR_CONTACTZIP",
    TRY_TO_TIMESTAMP(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(NULLIF(TRIM(TRIM("RR_LASTUPDATE")), ''), '[^0-9/.-]+', ''), '[/.]', '-'), '-{2,}', '-')) AS "RR_LASTUPDATE",
    NULLIF(TRIM(TRIM("RR_PYID")), '') AS "RR_PYID",
    NULLIF(TRIM(TRIM("RR_PGID")), '') AS "RR_PGID",
    NULLIF(TRIM("INSERTED_DATE"), '') AS "INSERTED_DATE",
    NULLIF(TRIM(TRIM("INSERTED_BY")), '') AS "INSERTED_BY",
    NULLIF(TRIM("UPDATED_DATE"), '') AS "UPDATED_DATE"

FROM DATA_INGESTION_SOLUTION.BRONZE_HCHB.REFERRAL_REQUESTS

{% if is_incremental() %}
  WHERE "UPDATED_DATE" > DATEADD(minute, -5, (SELECT MAX("UPDATED_DATE") FROM {{ this }}))
{% endif %}
