{% set dedupe_config = get_dedupe_config(var('source_name')) %}

{% set output_query = dedupe_config["output_query"] %}

WITH output_data AS (

```
{{ output_query }}
```

),

final_output AS (

```
SELECT

    O.*,

    G.CLUSTER_ID,

    D.RULE_ID,

    CURRENT_TIMESTAMP() AS DEDUPE_INSERTED_DATE

FROM output_data O

LEFT JOIN {{ ref('dedupe_input') }} I
    ON O.ID = I.ID

LEFT JOIN {{ ref('grouped_pk') }} G
    ON I.PK = G.PK1

LEFT JOIN {{ ref('duplicates') }} D
    ON I.PK = D.PK[0]::INTEGER
```

)

SELECT *
FROM final_output
