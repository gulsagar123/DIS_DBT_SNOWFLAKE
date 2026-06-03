{{
    config(
        schema               = 'silver_core',
        alias                = 'STAGE_DIM_BRANCH',
        materialized         = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key           = 'BRANCH_CODE',
        on_schema_change     = 'sync_all_columns',
        tags                 = ['silver', 'hchb', 'dim_branch']
    )
}}

WITH source AS (

    SELECT
        branch_code,
        branch_name,
        branch_parent,
        branch_street,
        branch_city,
        branch_state,
        branch_zip,
        branch_latitude,
        branch_longitude,
        branch_fax,
        branch_email,
        branch_phone,
        branch_active,
        branch_branchgl,
        UPDATED_DATE,
        INSERTED_DATE
    FROM {{ source('silver_staging', 'BRANCHES_HCHB_CLEANSED') }}

    {% if is_incremental() %}
        WHERE UPDATED_DATE  > (SELECT MAX(LOADED_AT) FROM {{ this }})
           OR INSERTED_DATE > (SELECT MAX(LOADED_AT) FROM {{ this }})
    {% endif %}

),

transformed AS (

    SELECT
        1                 AS SOURCE_SYSTEM_ID,
        'HCHB'            AS SYSTEM_CODE,
        branch_code       AS BRANCH_CODE,
        branch_name       AS BRANCH_NAME,
        branch_parent     AS PARENT_BRANCH,
        branch_street     AS ADDRESS_LINE_1,
        NULL              AS ADDRESS_LINE_2,
        branch_city       AS CITY,
        NULL              AS COUNTY,
        branch_state      AS STATE_CODE,
        branch_zip        AS ZIP,
        branch_latitude   AS LATITUDE,
        branch_longitude  AS LONGITUDE,
        branch_fax        AS FAX,
        branch_email      AS EMAIL,
        branch_phone      AS PHONE,
        branch_active     AS IS_ACTIVE,
        branch_branchgl   AS BRANCH_GL_NUMBER,
        CURRENT_TIMESTAMP AS LOADED_AT,
        UPDATED_DATE      AS SOURCE_LAST_UPDATE
    FROM source

)

SELECT * FROM transformed