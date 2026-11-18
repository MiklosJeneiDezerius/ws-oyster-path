{% set snapshot_end_text = var('gold_snapshot_end_date', '2026-11-09') %}
{% set snapshot_end_date = modules.datetime.datetime.strptime(snapshot_end_text, '%Y-%m-%d').date() %}
{% if snapshot_end_date.isoformat() != snapshot_end_text %}
    {{ exceptions.raise_compiler_error('Gold snapshot end date must use YYYY-MM-DD.') }}
{% endif %}

select min('snapshot end date does not match configured reporting date') as failure
from {{ ref('gold_fct_account_tier_daily_snapshot') }}
having count(*) = 0
    or max(snapshot_date) <> cast('{{ snapshot_end_text }}' as date)
