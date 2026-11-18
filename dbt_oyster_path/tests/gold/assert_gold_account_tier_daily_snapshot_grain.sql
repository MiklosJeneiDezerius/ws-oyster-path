select
    snapshot_date,
    account_id,
    count(*) as row_count
from {{ ref('g_fct_account_tier_daily_snapshot') }}
group by snapshot_date, account_id
having count(*) <> 1
