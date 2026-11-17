{% macro silver_calendar_bounds() %}
    {% set start_text = var('silver_date_start', '2020-01-01') %}
    {% set end_text = var('silver_date_end', '2035-12-31') %}
    {% set start_date = modules.datetime.datetime.strptime(start_text, '%Y-%m-%d').date() %}
    {% set end_date = modules.datetime.datetime.strptime(end_text, '%Y-%m-%d').date() %}
    {% if start_date.isoformat() != start_text or end_date.isoformat() != end_text %}
        {{ exceptions.raise_compiler_error('Silver calendar bounds must use YYYY-MM-DD.') }}
    {% endif %}
    {% set days = (end_date - start_date).days + 1 %}
    {% if days < 1 or days > 100000 %}
        {{ exceptions.raise_compiler_error('Silver calendar must contain 1 to 100000 days, inclusive.') }}
    {% endif %}
    {{ return({'start': start_text, 'end': end_text, 'days': days}) }}
{% endmacro %}
