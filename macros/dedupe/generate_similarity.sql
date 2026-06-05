{% macro generate_similarity(source_name) %}

```
{% set dedupe_config = get_dedupe_config(source_name) %}

{% set fields = dedupe_config["config"]["FIELDS"] %}

{% set similarity_sql = [] %}

{% for field, algo in fields.items() %}

    {% if algo | lower == 'jarowinkler' %}

        {% set expr %}

            JAROWINKLER_SIMILARITY(
                {{ field }}[0]::STRING,
                {{ field }}[1]::STRING
            ) / 100 AS {{ field }}

        {% endset %}

    {% else %}

        {% set expr %}

            1 - (
                EDITDISTANCE(
                    {{ field }}[0]::STRING,
                    {{ field }}[1]::STRING
                )
                /
                GREATEST(
                    LENGTH({{ field }}[0]::STRING),
                    LENGTH({{ field }}[1]::STRING)
                )
            ) AS {{ field }}

        {% endset %}

    {% endif %}

    {% do similarity_sql.append(expr) %}

{% endfor %}

{{ return(similarity_sql | join(',')) }}
```

{% endmacro %}
