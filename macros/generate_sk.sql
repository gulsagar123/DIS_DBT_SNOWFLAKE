{% macro generate_sk(natural_key_expr) %}
    MD5({{ natural_key_expr }})
{% endmacro %}