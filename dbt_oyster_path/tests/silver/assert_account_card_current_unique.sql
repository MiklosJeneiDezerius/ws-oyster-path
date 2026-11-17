with current_versions as (
    select 'account' as entity_name, account_id as business_key
    from {{ ref('dim_account') }}
    where is_current = 1 and is_unknown = 0

    union all

    select 'debitcard', card_id
    from {{ ref('dim_debitcard') }}
    where is_current = 1
)
select entity_name, business_key, count(*) as current_version_count
from current_versions
group by entity_name, business_key
having count(*) > 1
