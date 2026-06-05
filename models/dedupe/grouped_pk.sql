WITH RECURSIVE clusters AS (

```
SELECT
    PK[0]::INTEGER AS PK1,
    PK[1]::INTEGER AS PK2
FROM {{ ref('duplicates') }}

UNION

SELECT
    c.PK1,
    d.PK[1]::INTEGER
FROM clusters c
INNER JOIN {{ ref('duplicates') }} d
    ON c.PK2 = d.PK[0]::INTEGER
```

),

final_clusters AS (

```
SELECT
    PK1,
    MIN(PK2) AS CLUSTER_ID
FROM clusters
GROUP BY PK1
```

)

SELECT *
FROM final_clusters
