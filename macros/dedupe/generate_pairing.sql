{% macro generate_pairing(source_name) %}

```
{% set dedupe_config = get_dedupe_config(source_name) %}

{% set pairing_config = dedupe_config["config"]["PAIRS_ON"] %}

{% set sql_parts = [] %}

{% for field, method in pairing_config.items() %}

    {% set sql %}

        SELECT

            ARRAY_CONSTRUCT(A.PK, B.PK) AS PK,

            ARRAY_CONSTRUCT(A.ID, B.ID) AS ID,

            ARRAY_CONSTRUCT(A.FIRST_NAME, B.FIRST_NAME) AS FIRST_NAME,

            ARRAY_CONSTRUCT(A.LAST_NAME, B.LAST_NAME) AS LAST_NAME,

            ARRAY_CONSTRUCT(A.FIRST_LAST_NAME, B.FIRST_LAST_NAME) AS FIRST_LAST_NAME,

            ARRAY_CONSTRUCT(A.EMAIL, B.EMAIL) AS EMAIL,

            ARRAY_CONSTRUCT(A.SRC_ADDRESS, B.SRC_ADDRESS) AS SRC_ADDRESS,

            ARRAY_CONSTRUCT(A.SSN, B.SSN) AS SSN,

            ARRAY_CONSTRUCT(A.DOB, B.DOB) AS DOB,

            ARRAY_CONSTRUCT(A.PHONE_NUMBER, B.PHONE_NUMBER) AS PHONE_NUMBER,

            ARRAY_CONSTRUCT(A.MEDICAID_ID, B.MEDICAID_ID) AS MEDICAID_ID,

            ARRAY_CONSTRUCT(A.MEDICARE_ID, B.MEDICARE_ID) AS MEDICARE_ID,

            '{{ field }}' AS PAIRED_ON,

            '{{ method }}' AS PAIRING_METHOD

        FROM {{ ref('dedupe_input') }} A

        INNER JOIN {{ ref('dedupe_input') }} B

            ON A.{{ field }} = B.{{ field }}
           AND A.PK > B.PK

    {% endset %}

    {% do sql_parts.append(sql) %}

{% endfor %}

{{ return(sql_parts | join(' UNION ALL ')) }}
```

{% endmacro %}
