SELECT

```
PK,
ID,

{{ generate_similarity(var('source_name')) }}
```

FROM {{ ref('pairs') }}
