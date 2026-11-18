with history as (
    select
        'account' as entity_name,
        account_id as business_key,
        account_key as version_key,
        valid_from,
        valid_to
    from {{ ref('s_dim_account') }}
    where is_unknown = 0

    union all

    select 'debitcard', card_id, debitcard_key, valid_from, valid_to
    from {{ ref('s_dim_debitcard') }}
),
intervals as (
    select
        *,
        max(valid_to) over (
            partition by entity_name, business_key
            order by valid_from, version_key
            rows between unbounded preceding and 1 preceding
        ) as previous_max_end
    from history
)
select *
from intervals
where valid_from >= valid_to
   or valid_from < previous_max_end
