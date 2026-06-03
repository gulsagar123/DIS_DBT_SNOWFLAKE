{% macro insert_into_stage(source_relation, unique_key, columns) %}
    INSERT INTO {{ this }}
    ({{ columns | join(', ') }})
    SELECT
        {{ columns | join(',\n        ') }}
    FROM {{ source_relation }}
    WHERE {{ unique_key }} NOT IN (
        SELECT {{ unique_key }} FROM {{ this }}
    )
{% endmacro %}