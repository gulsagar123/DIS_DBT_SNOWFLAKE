{% macro get_rules(rule_type) %}

```
{% set query %}

    SELECT PARSE_JSON(RULES) AS RULES
    FROM DATA_INGESTION_SOLUTION.ETL_MANAGEMENT.ETL_CONFIG_DEDUPE_RULES_SNOWPARK
    WHERE RULE_TYPE = '{{ rule_type }}'

{% endset %}

{% if execute %}

    {% set results = run_query(query) %}

    {% if results.rows | length == 0 %}

        {{ exceptions.raise_compiler_error(
            "No RULES found for RULE_TYPE: " ~ rule_type
        ) }}

    {% endif %}

    {{ return(results.rows[0][0]) }}

{% endif %}
```

{% endmacro %}
