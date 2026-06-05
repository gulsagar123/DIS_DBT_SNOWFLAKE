{% macro get_dedupe_config(source_name) %}

```
{% set query %}

    SELECT
        CONFIG,
        INPUT_QUERY,
        OUTPUT_QUERY,
        RULE_TYPE
    FROM DATA_INGESTION_SOLUTION.ETL_MANAGEMENT.ETL_CONFIG_DEDUPE_DETAILS_SNOWPARK
    WHERE SOURCE_NAME = '{{ source_name }}'

{% endset %}

{% if execute %}

    {% set results = run_query(query) %}

    {% if results.rows | length == 0 %}

        {{ exceptions.raise_compiler_error(
            "No config found for SOURCE_NAME: " ~ source_name
        ) }}

    {% endif %}

    {% set config_text = results.rows[0][0] | string %}
    {% set input_query = results.rows[0][1] | string %}
    {% set output_query = results.rows[0][2] | string %}
    {% set rule_type = results.rows[0][3] | string %}

    {{ log("CONFIG TEXT = " ~ config_text, info=True) }}
    {{ log("INPUT QUERY = " ~ input_query, info=True) }}
    {{ log("OUTPUT QUERY = " ~ output_query, info=True) }}
    {{ log("RULE TYPE = " ~ rule_type, info=True) }}

    {% set config_json = fromjson(config_text) %}

    {{ return({
        "config": config_json,
        "input_query": input_query,
        "output_query": output_query,
        "rule_type": rule_type
    }) }}

{% endif %}
```

{% endmacro %}
