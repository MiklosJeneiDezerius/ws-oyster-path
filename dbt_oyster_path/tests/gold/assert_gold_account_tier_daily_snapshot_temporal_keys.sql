select snapshot.snapshot_date, snapshot.account_id, snapshot.account_key
from {{ ref('gold_fct_account_tier_daily_snapshot') }} as snapshot
left join {{ ref('gold_dim_account') }} as accounts
    on accounts.account_key = snapshot.account_key
where accounts.account_key is null
   or accounts.account_id <> snapshot.account_id
   or accounts.is_unknown <> 0
   or accounts.is_deleted <> 0
   or accounts.valid_from >= dateadd(
        day, 1, cast(snapshot.snapshot_date as datetime2(6))
   )
   or accounts.valid_to < dateadd(
        day, 1, cast(snapshot.snapshot_date as datetime2(6))
   )

union all

select snapshot.snapshot_date, snapshot.account_id, snapshot.account_key
from {{ ref('gold_fct_account_tier_daily_snapshot') }} as snapshot
left join {{ ref('gold_dim_date') }} as dates
    on dates.date_key = snapshot.date_key
where dates.date_key is null
   or dates.calendar_date <> snapshot.snapshot_date

union all

select snapshot.snapshot_date, snapshot.account_id, snapshot.account_key
from {{ ref('gold_fct_account_tier_daily_snapshot') }} as snapshot
left join {{ ref('gold_dim_account') }} as accounts
    on accounts.account_key = snapshot.account_key
left join {{ ref('gold_dim_customer') }} as customers
    on customers.customer_key = snapshot.customer_key
where accounts.account_key is not null
  and (
      customers.customer_key is null
      or (
          snapshot.customer_lookup_status = 'matched'
          and (
              customers.customer_id <> accounts.customer_id
              or customers.is_deleted <> 0
              or customers.is_unknown <> 0
              or customers.valid_from >= dateadd(
                    day, 1, cast(snapshot.snapshot_date as datetime2(6))
                 )
              or customers.valid_to < dateadd(
                    day, 1, cast(snapshot.snapshot_date as datetime2(6))
                 )
          )
      )
      or (
          snapshot.customer_lookup_status = 'no_customer_history'
          and snapshot.customer_key <> 'UNKNOWN_CUSTOMER'
      )
  )

union all

select snapshot.snapshot_date, snapshot.account_id, snapshot.account_key
from {{ ref('gold_fct_account_tier_daily_snapshot') }} as snapshot
left join {{ ref('gold_dim_debitcard') }} as cards
    on cards.debitcard_key = snapshot.debitcard_key
where snapshot.debitcard_key is not null
  and (
      cards.debitcard_key is null
      or cards.account_id <> snapshot.account_id
      or cards.is_active <> 1
      or cards.is_deleted <> 0
      or cards.valid_from >= dateadd(
            day, 1, cast(snapshot.snapshot_date as datetime2(6))
         )
      or cards.valid_to < dateadd(
            day, 1, cast(snapshot.snapshot_date as datetime2(6))
         )
  )
