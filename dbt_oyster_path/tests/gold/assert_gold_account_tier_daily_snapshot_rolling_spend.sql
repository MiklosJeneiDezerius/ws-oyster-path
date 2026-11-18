select
    snapshot.snapshot_date,
    snapshot.account_id,
    snapshot.rolling_30_day_spend as actual_spend,
    coalesce(sum(transactions.amount), cast(0 as decimal(38, 2))) as expected_spend
from {{ ref('gold_fct_account_tier_daily_snapshot') }} as snapshot
left join {{ ref('gold_fct_transaction') }} as transactions
    on transactions.account_id = snapshot.account_id
   and transactions.transaction_timestamp >= dateadd(
        day, -29, cast(snapshot.snapshot_date as datetime2(6))
   )
   and transactions.transaction_timestamp < dateadd(
        day, 1, cast(snapshot.snapshot_date as datetime2(6))
   )
group by
    snapshot.snapshot_date,
    snapshot.account_id,
    snapshot.rolling_30_day_spend
having snapshot.rolling_30_day_spend
    <> coalesce(sum(transactions.amount), cast(0 as decimal(38, 2)))
