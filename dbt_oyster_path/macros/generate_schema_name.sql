{#
    Environment-aware schema naming (driven by the dbt target, e.g. --target prod).
    prod + custom schema (+schema: bronze)  ->  bronze
    other + custom schema                   ->  <target.name>_bronze   e.g. dev_bronze
    no custom schema                        ->  target.schema from profiles.yml
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- elif target.name | lower == 'prod' -%}
        {{ custom_schema_name | trim | lower }}
    {%- else -%}
        {{ target.name | trim | lower }}_{{ custom_schema_name | trim | lower }}
    {%- endif -%}
{%- endmacro %}
