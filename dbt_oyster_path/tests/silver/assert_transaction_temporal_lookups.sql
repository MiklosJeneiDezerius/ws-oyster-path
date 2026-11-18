select facts.transaction_id
from {{ ref('s_fct_transaction') }} as facts
left join {{ ref('s_dim_account') }} as accounts
    on accounts.account_key = facts.account_key
left join {{ ref('s_dim_customer') }} as customers
    on customers.customer_key = facts.customer_key
where (
    facts.account_key = 'UNKNOWN_ACCOUNT'
    and (
        facts.customer_id is not null
        or facts.customer_key <> 'UNKNOWN_CUSTOMER'
        or exists (
            select 1
            from {{ ref('s_dim_account') }} as eligible
            where eligible.account_id = facts.account_id
              and eligible.is_unknown = 0
              and eligible.is_deleted = 0
              and facts.transaction_timestamp >= eligible.valid_from
              and facts.transaction_timestamp < eligible.valid_to
        )
    )
)
or (
    facts.account_key <> 'UNKNOWN_ACCOUNT'
    and (
        accounts.is_unknown <> 0 or accounts.is_deleted <> 0
        or accounts.account_id <> facts.account_id
        or facts.transaction_timestamp < accounts.valid_from
        or facts.transaction_timestamp >= accounts.valid_to
        or exists (
            select facts.customer_id
            except
            select accounts.customer_id
        )
    )
)
or (
    facts.customer_key = 'UNKNOWN_CUSTOMER'
    and exists (
        select 1
        from {{ ref('s_dim_customer') }} as eligible
        where eligible.customer_id = facts.customer_id
          and eligible.is_unknown = 0
          and eligible.is_deleted = 0
          and facts.transaction_timestamp >= eligible.valid_from
          and facts.transaction_timestamp < eligible.valid_to
    )
)
or (
    facts.customer_key <> 'UNKNOWN_CUSTOMER'
    and (
        customers.is_unknown <> 0 or customers.is_deleted <> 0
        or customers.customer_id <> facts.customer_id
        or facts.transaction_timestamp < customers.valid_from
        or facts.transaction_timestamp >= customers.valid_to
    )
)
or facts.account_lookup_status <>
    case when facts.account_key = 'UNKNOWN_ACCOUNT'
        then 'no_account_history' else 'matched'
    end
or facts.customer_lookup_status <>
    case
        when facts.account_key = 'UNKNOWN_ACCOUNT' then 'account_unresolved'
        when facts.customer_key = 'UNKNOWN_CUSTOMER' then 'no_customer_history'
        else 'matched'
    end
