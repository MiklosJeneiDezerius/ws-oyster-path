with account_lookup as (
    select
        transactions.*,
        accounts.account_key,
        accounts.customer_id
    from {{ ref('bis_fct_transaction_staging') }} as transactions
    left join {{ ref('s_dim_account') }} as accounts
        on accounts.account_id = transactions.account_id
       and accounts.is_unknown = 0
       and accounts.is_deleted = 0
       and transactions.transaction_timestamp >= accounts.valid_from
       and transactions.transaction_timestamp < accounts.valid_to
)
select
    transactions.transaction_id,
    transactions.transaction_id_raw,
    transactions.account_id,
    transactions.account_id_raw,
    transactions.customer_id,
    coalesce(transactions.account_key, 'UNKNOWN_ACCOUNT') as account_key,
    coalesce(customers.customer_key, 'UNKNOWN_CUSTOMER') as customer_key,
    dates.date_key,
    times.time_key,
    transactions.transaction_timestamp,
    transactions.amount,
    case when transactions.account_key is null
        then 'no_account_history' else 'matched'
    end as account_lookup_status,
    case
        when transactions.account_key is null then 'account_unresolved'
        when customers.customer_key is null then 'no_customer_history'
        else 'matched'
    end as customer_lookup_status,
    transactions.meta_batch_timestamp,
    transactions.meta_load_timestamp,
    transactions.meta_source_file
from account_lookup as transactions
left join {{ ref('s_dim_customer') }} as customers
    on customers.customer_id = transactions.customer_id
   and customers.is_unknown = 0
   and customers.is_deleted = 0
   and transactions.transaction_timestamp >= customers.valid_from
   and transactions.transaction_timestamp < customers.valid_to
left join {{ ref('s_dim_date') }} as dates
    on dates.calendar_date = cast(transactions.transaction_timestamp as date)
left join {{ ref('s_dim_time') }} as times
    on times.time_key =
        datepart(hour, transactions.transaction_timestamp)*3600
        + datepart(minute, transactions.transaction_timestamp)*60
        + datepart(second, transactions.transaction_timestamp)
