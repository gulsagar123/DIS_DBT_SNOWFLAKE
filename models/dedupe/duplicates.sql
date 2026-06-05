SELECT *

FROM (

```
SELECT

    *,

    {{ generate_rule_case(var('source_name')) }} AS RULE_ID

FROM {{ ref('similarity_score') }}
```

)

WHERE RULE_ID IS NOT NULL
