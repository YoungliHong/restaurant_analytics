-- macros/generate_alias_name.sql

{% macro generate_alias_name(custom_alias_name=none, node=none) %}
    {%- if custom_alias_name is not none -%}
        {{ custom_alias_name | trim }}
    {%- elif node.config.materialized == 'view' -%}
        vw_{{ node.name }}
    {%- else -%}
        {{ node.name }}
    {%- endif -%}
{% endmacro %}