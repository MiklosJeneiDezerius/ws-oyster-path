{% set attributes = ['first_name', 'last_name', 'is_deleted'] %}
{% for prefix in ['permanent', 'mailing', 'billing'] %}
    {% for field in ['country', 'postal_code', 'city', 'street', 'house_number'] %}
        {% do attributes.append(prefix ~ '_' ~ field) %}
    {% endfor %}
{% endfor %}

with previous_states as (
    select
        *,
        lag(valid_to) over (
            partition by customer_id order by valid_from
        ) as previous_valid_to
        {% for attribute in attributes %}
        , lag({{ attribute }}) over (
            partition by customer_id order by valid_from
        ) as previous_{{ attribute }}
        {% endfor %}
    from {{ ref('int_customer_states') }}
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
            partition by customer_id order by valid_from
            rows between unbounded preceding and current row
        ) as version_group
    from changes
),
version_ranges as (
    select
        customer_id,
        version_group,
        min(valid_from) as valid_from,
        max(valid_to) as valid_to
    from version_groups
    group by customer_id, version_group
)
select
    convert(
        varchar(64),
        hashbytes(
            'SHA2_256',
            concat(
                len(versions.customer_id), ':', versions.customer_id, ':',
                convert(varchar(27), cast(versions.valid_from as datetime2(6)), 126)
            )
        ),
        2
    ) as customer_key,
    versions.customer_id,
    {% for attribute in attributes %}
    states.{{ attribute }},
    {% endfor %}
    versions.valid_from,
    versions.valid_to,
    cast(
        case when versions.valid_to = cast('9999-12-31' as datetime2(6))
            then 1 else 0
        end as bit
    ) as is_current,
    cast(0 as bit) as is_unknown
from version_ranges as versions
inner join version_groups as states
    on states.customer_id = versions.customer_id
   and states.version_group = versions.version_group
   and states.valid_from = versions.valid_from

union all

select
    cast('UNKNOWN_CUSTOMER' as varchar(64)) as customer_key,
    cast(null as varchar(8000)) as customer_id,
    {% for attribute in attributes %}
    cast(null as {{ 'bit' if attribute == 'is_deleted' else 'varchar(8000)' }}) as {{ attribute }},
    {% endfor %}
    cast('0001-01-01' as datetime2(6)) as valid_from,
    cast('9999-12-31' as datetime2(6)) as valid_to,
    cast(1 as bit) as is_current,
    cast(1 as bit) as is_unknown
