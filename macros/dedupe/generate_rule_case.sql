{% macro generate_rule_case(source_name) %}

```
{% set dedupe_config = get_dedupe_config(source_name) %}

{% set rule_type = dedupe_config["rule_type"] %}

{% set rules = get_rules(rule_type) %}

CASE

{% for rule_id in rules.keys() %}

    WHEN {{ rules[rule_id] }}
    THEN '{{ rule_id }}'

{% endfor %}

END
```

{% endmacro %}
