{% macro coalesce_observed_dimension(relation, business_key, version_key, attributes, unknown_key=none) %}
with source_states as (
    select
        {{ business_key }},
        {{ attributes | join(', ') }},
        observed_valid_from as valid_from,
        observed_valid_to as valid_to
    from {{ relation }}
),
previous_states as (
    select
        *,
        lag(valid_to) over (
            partition by {{ business_key }} order by valid_from
        ) as previous_valid_to
        {% for attribute in attributes %}
        , lag({{ attribute }}) over (
            partition by {{ business_key }} order by valid_from
        ) as previous_{{ attribute }}
        {% endfor %}
    from source_states
),
changes as (
    select
        *,
        case
            when previous_valid_to = valid_from
                and not exists (
                    select {{ attributes | join(', ') }}
                    except
                    select
                        {% for attribute in attributes %}
                        previous_{{ attribute }}{% if not loop.last %},{% endif %}
                        {% endfor %}
                )
            then 0
            else 1
        end as starts_version
    from previous_states
),
version_groups as (
    select
        *,
        sum(starts_version) over (
            partition by {{ business_key }} order by valid_from
            rows between unbounded preceding and current row
        ) as version_group
    from changes
),
version_ranges as (
    select
        {{ business_key }},
        version_group,
        min(valid_from) as valid_from,
        max(valid_to) as valid_to
    from version_groups
    group by {{ business_key }}, version_group
)
select
    convert(
        varchar(64),
        hashbytes(
            'SHA2_256',
            concat(
                len(versions.{{ business_key }}), ':', versions.{{ business_key }}, ':',
                convert(varchar(27), cast(versions.valid_from as datetime2(6)), 126)
            )
        ),
        2
    ) as {{ version_key }},
    versions.{{ business_key }},
    {% for attribute in attributes %}
    states.{{ attribute }},
    {% endfor %}
    versions.valid_from,
    versions.valid_to,
    cast(
        case when versions.valid_to = cast('9999-12-31' as datetime2(6))
            then 1 else 0
        end as bit
    ) as is_current
    {% if unknown_key is not none %}
    , cast(0 as bit) as is_unknown
    {% endif %}
from version_ranges as versions
inner join version_groups as states
    on states.{{ business_key }} = versions.{{ business_key }}
   and states.version_group = versions.version_group
   and states.valid_from = versions.valid_from
{% if unknown_key is not none %}

union all

select
    cast('{{ unknown_key }}' as varchar(64)) as {{ version_key }},
    cast(null as varchar(8000)) as {{ business_key }},
    {% for attribute in attributes %}
    cast(null as {{ 'bit' if attribute == 'is_deleted' else 'varchar(8000)' }}) as {{ attribute }},
    {% endfor %}
    cast('0001-01-01' as datetime2(6)) as valid_from,
    cast('9999-12-31' as datetime2(6)) as valid_to,
    cast(1 as bit) as is_current,
    cast(1 as bit) as is_unknown
{% endif %}
{% endmacro %}
