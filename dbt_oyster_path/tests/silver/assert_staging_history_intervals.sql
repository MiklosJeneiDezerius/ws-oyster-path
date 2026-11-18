with history as (
    {% for entity, key, staging_model in [
        ('customer', 'customer_id', 'bis_dim_customer_staging'),
        ('customer_address', 'address_id', 'bis_dim_customer_address_staging'),
        ('account', 'account_id', 'bis_dim_account_staging'),
        ('debitcard', 'card_id', 'bis_dim_debitcard_staging')
    ] %}
    select
        '{{ entity }}' as entity_name,
        {{ key }} as business_key,
        bronze_version_id,
        observed_valid_from,
        observed_valid_to
    from {{ ref(staging_model) }}
    {% if not loop.last %}union all{% endif %}
    {% endfor %}
),
intervals as (
    select
        *,
        max(observed_valid_to) over (
            partition by entity_name, business_key
            order by observed_valid_from, bronze_version_id
            rows between unbounded preceding and 1 preceding
        ) as previous_max_end
    from history
)
select *
from intervals
where observed_valid_from >= observed_valid_to
   or observed_valid_from < previous_max_end
