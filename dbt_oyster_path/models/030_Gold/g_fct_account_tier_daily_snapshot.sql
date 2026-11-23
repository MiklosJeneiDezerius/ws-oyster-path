{% set gold_threshold = var('gold_tier_gold_threshold', 2000) | int %}
{% set platinum_threshold = var('gold_tier_platinum_threshold', 5000) | int %}
{% set snapshot_end_text = var('gold_snapshot_end_date', '2026-11-09') %}
{% set snapshot_end_date = modules.datetime.datetime.strptime(snapshot_end_text, '%Y-%m-%d').date() %}
{% if gold_threshold <= 0 or platinum_threshold <= gold_threshold %}
    {{ exceptions.raise_compiler_error(
        'Gold tier thresholds must be positive whole numbers, with Platinum greater than Gold.'
    ) }}
{% endif %}
{% if snapshot_end_date.isoformat() != snapshot_end_text %}
    {{ exceptions.raise_compiler_error('Gold snapshot end date must use YYYY-MM-DD.') }}
{% endif %}

with transaction_bounds as (
    select
        min(cast(transaction_timestamp as date)) as first_transaction_date,
        max(cast(transaction_timestamp as date)) as last_transaction_date
    from {{ ref('g_fct_transaction') }}
    where transaction_timestamp is not null
),
snapshot_dates as (
    select
        calendar.date_key,
        calendar.calendar_date as snapshot_date
    from {{ ref('g_dim_date') }} as calendar
    cross join transaction_bounds as bounds
    where calendar.calendar_date >= bounds.first_transaction_date
      and calendar.calendar_date <= bounds.last_transaction_date
      and calendar.calendar_date <= cast('{{ snapshot_end_text }}' as date)
),
account_days as (
    select
        dates.date_key,
        dates.snapshot_date,
        accounts.account_key,
        accounts.account_id,
        accounts.customer_id
    from {{ ref('g_dim_account') }} as accounts
    inner join snapshot_dates as dates
        on accounts.is_unknown = 0
       and accounts.is_deleted = 0
       and accounts.valid_from < dateadd(
            day, 1, cast(dates.snapshot_date as datetime2(6))
       )
       and accounts.valid_to >= dateadd(
            day, 1, cast(dates.snapshot_date as datetime2(6))
       )
),
account_card_days as (
    select
        account_days.date_key,
        account_days.snapshot_date,
        account_days.account_key,
        account_days.account_id,
        coalesce(customers.customer_key, 'UNKNOWN_CUSTOMER') as customer_key,
        cards.debitcard_key,
        case
            when cards.debitcard_key is null then 'no_active_card'
            when nullif(trim(cards.card_tier), '') is null then 'missing_tier'
            else 'active'
        end as actual_card_status,
        case upper(nullif(trim(cards.card_tier), ''))
            when 'STANDARD' then 'Standard'
            when 'GOLD' then 'Gold'
            when 'PLATINUM' then 'Platinum'
            else nullif(trim(cards.card_tier), '')
        end as actual_card_tier,
        case
            when customers.customer_key is null then 'no_customer_history'
            else 'matched'
        end as customer_lookup_status
    from account_days
    left join {{ ref('g_dim_customer') }} as customers
        on customers.customer_id = account_days.customer_id
       and customers.is_unknown = 0
       and customers.is_deleted = 0
       and customers.valid_from < dateadd(
            day, 1, cast(account_days.snapshot_date as datetime2(6))
       )
       and customers.valid_to >= dateadd(
            day, 1, cast(account_days.snapshot_date as datetime2(6))
       )
    left join {{ ref('g_dim_debitcard') }} as cards
        on cards.account_id = account_days.account_id
       and cards.is_active = 1
       and cards.is_deleted = 0
       and cards.valid_from < dateadd(
            day, 1, cast(account_days.snapshot_date as datetime2(6))
       )
       and cards.valid_to >= dateadd(
            day, 1, cast(account_days.snapshot_date as datetime2(6))
       )
),
transaction_coverage as (
    select
        account_id,
        min(cast(transaction_timestamp as date)) as first_account_transaction_date
    from {{ ref('g_fct_transaction') }}
    where transaction_timestamp is not null
    group by account_id
),
rolling_spend as (
    select
        account_days.snapshot_date,
        account_days.account_id,
        coalesce(
            sum(transactions.amount),
            cast(0 as decimal(38, 2))
        ) as rolling_30_day_spend
    from account_days
    left join {{ ref('g_fct_transaction') }} as transactions
        on transactions.account_id = account_days.account_id
       and transactions.transaction_timestamp >= dateadd(
            day, -29, cast(account_days.snapshot_date as datetime2(6))
       )
       and transactions.transaction_timestamp < dateadd(
            day, 1, cast(account_days.snapshot_date as datetime2(6))
       )
    group by account_days.snapshot_date, account_days.account_id
),
spend_tiers as (
    select
        account_card_days.*,
        rolling_spend.rolling_30_day_spend,
        case
            when rolling_spend.rolling_30_day_spend >= {{ platinum_threshold }}
                then 'Platinum'
            when rolling_spend.rolling_30_day_spend >= {{ gold_threshold }}
                then 'Gold'
            else 'Standard'
        end as calculated_spending_tier,
        case
            when coverage.first_account_transaction_date is not null
             and account_card_days.snapshot_date >= dateadd(
                    day, 30, coverage.first_account_transaction_date
                 )
                then cast(1 as bit)
            else cast(0 as bit)
        end as has_full_30_day_source_window
    from account_card_days
    inner join rolling_spend
        on rolling_spend.snapshot_date = account_card_days.snapshot_date
       and rolling_spend.account_id = account_card_days.account_id
    left join transaction_coverage as coverage
        on coverage.account_id = account_card_days.account_id
)
select
    date_key,
    snapshot_date,
    account_key,
    account_id,
    customer_key,
    debitcard_key,
    actual_card_status,
    actual_card_tier,
    rolling_30_day_spend,
    calculated_spending_tier,
    case
        when actual_card_status <> 'active'
          or upper(actual_card_tier) not in ('STANDARD', 'GOLD', 'PLATINUM')
            then cast(null as bit)
        when calculated_spending_tier = 'Platinum'
         and actual_card_tier <> 'Platinum'
            then cast(1 as bit)
        when calculated_spending_tier = 'Gold'
         and actual_card_tier = 'Standard'
            then cast(1 as bit)
        else cast(0 as bit)
    end as upgrade_candidate,
    case
        when actual_card_status <> 'active'
          or upper(actual_card_tier) not in ('STANDARD', 'GOLD', 'PLATINUM')
            then cast(null as bit)
        when actual_card_tier = 'Platinum'
         and calculated_spending_tier <> 'Platinum'
            then cast(1 as bit)
        when actual_card_tier = 'Gold'
         and calculated_spending_tier = 'Standard'
            then cast(1 as bit)
        else cast(0 as bit)
    end as downgrade_candidate,
    case
        when actual_card_status <> 'active' then actual_card_status
        when upper(actual_card_tier) not in ('STANDARD', 'GOLD', 'PLATINUM')
            then 'unrecognized_tier'
        when actual_card_tier = calculated_spending_tier then 'aligned'
        else 'misaligned'
    end as alignment_status,
    customer_lookup_status,
    has_full_30_day_source_window
from spend_tiers
