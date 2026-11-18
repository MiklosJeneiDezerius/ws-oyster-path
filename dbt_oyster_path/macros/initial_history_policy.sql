{# Initial-load history policy controlled by var('silver_infer_initial_history').
   version_group = 1 is the first observed version of a business key. #}
{% macro inferred_valid_from(alias) %}
    {%- if var('silver_infer_initial_history', false) -%}
    case when {{ alias }}.version_group = 1
        then cast('0001-01-01' as datetime2(6))
        else {{ alias }}.valid_from
    end
    {%- else -%}
    {{ alias }}.valid_from
    {%- endif -%}
{% endmacro %}

{% macro inferred_start_flag(alias) %}
    {%- if var('silver_infer_initial_history', false) -%}
    cast(case when {{ alias }}.version_group = 1 then 1 else 0 end as bit)
    {%- else -%}
    cast(0 as bit)
    {%- endif -%}
{% endmacro %}
