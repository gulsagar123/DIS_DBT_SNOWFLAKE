{% set dedupe_config = get_dedupe_config(var('source_name')) %}

{% set input_query = dedupe_config["input_query"] %}

WITH source_data AS (

```
{{ input_query }}
```

),

normalized AS (

```
SELECT

    ROW_NUMBER() OVER (
        ORDER BY ID
    ) AS PK,

    *,

    UPPER(
        REGEXP_REPLACE(
            CONCAT(
                COALESCE(FIRST_NAME, ''),
                COALESCE(LAST_NAME, '')
            ),
            '[^A-Z]',
            ''
        )
    ) AS FIRST_LAST_NAME,

    ROW_NUMBER() OVER (
        ORDER BY
            CONCAT(
                COALESCE(FIRST_NAME, ''),
                COALESCE(LAST_NAME, '')
            ),
            ID
    ) AS SORT_INDEX

FROM source_data
```

)

SELECT *
FROM normalized
