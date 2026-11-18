{% set gold_threshold = var('gold_tier_gold_threshold', 2000) | int %}
{% set platinum_threshold = var('gold_tier_platinum_threshold', 5000) | int %}

select
    snapshot_date,
    account_id,
    actual_card_status,
    actual_card_tier,
    rolling_30_day_spend,
    calculated_spending_tier,
    upgrade_candidate,
    downgrade_candidate,
    alignment_status
from {{ ref('gold_fct_account_tier_daily_snapshot') }}
where calculated_spending_tier <> case
        when rolling_30_day_spend >= {{ platinum_threshold }} then 'Platinum'
        when rolling_30_day_spend >= {{ gold_threshold }} then 'Gold'
        else 'Standard'
    end
   or (
       actual_card_status <> 'active'
       and (
           upgrade_candidate is not null
           or downgrade_candidate is not null
           or alignment_status <> actual_card_status
       )
   )
   or (
       actual_card_status = 'active'
       and upper(actual_card_tier) in ('STANDARD', 'GOLD', 'PLATINUM')
       and (
           upgrade_candidate <> case
               when calculated_spending_tier = 'Platinum'
                and actual_card_tier <> 'Platinum' then 1
               when calculated_spending_tier = 'Gold'
                and actual_card_tier = 'Standard' then 1
               else 0
           end
           or downgrade_candidate <> case
               when actual_card_tier = 'Platinum'
                and calculated_spending_tier <> 'Platinum' then 1
               when actual_card_tier = 'Gold'
                and calculated_spending_tier = 'Standard' then 1
               else 0
           end
           or alignment_status <> case
               when actual_card_tier = calculated_spending_tier then 'aligned'
               else 'misaligned'
           end
       )
   )
   or (
       actual_card_status = 'active'
       and upper(actual_card_tier) not in ('STANDARD', 'GOLD', 'PLATINUM')
       and (
           upgrade_candidate is not null
           or downgrade_candidate is not null
           or alignment_status <> 'unrecognized_tier'
       )
   )
