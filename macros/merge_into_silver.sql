{% macro merge_into_silver(target_table, source_table, unique_key, columns, cdc_op_col='CDC_OP', cdc_ts_col='CDC_TS') %}

MERGE INTO {{ target_table }} AS tgt
USING (
    SELECT *
    FROM {{ source_table }}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY {{ unique_key }}
        ORDER BY {{ cdc_ts_col }} DESC
    ) = 1
) AS src
ON tgt.{{ unique_key }} = src.{{ unique_key }}

WHEN MATCHED AND src.{{ cdc_op_col }} = 'D' THEN
    DELETE

WHEN MATCHED AND src.{{ cdc_op_col }} IN ('U', 'I') THEN
    UPDATE SET
        {% for col in columns if col != unique_key %}
        tgt.{{ col }} = src.{{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}

WHEN NOT MATCHED AND src.{{ cdc_op_col }} != 'D' THEN
    INSERT ({{ columns | join(', ') }})
    VALUES (
        {% for col in columns %}
        src.{{ col }}{% if not loop.last %},{% endif %}
        {% endfor %}
    );

{% endmacro %}