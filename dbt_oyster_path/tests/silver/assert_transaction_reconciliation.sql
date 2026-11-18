{% set columns = [
    'transaction_id', 'transaction_id_raw', 'account_id', 'account_id_raw',
    'transaction_timestamp', 'amount',
    'meta_batch_timestamp', 'meta_load_timestamp', 'meta_source_file'
] %}
with source_rows as (
    select {{ columns | join(', ') }}
    from {{ ref('bis_fct_transaction_staging') }}
),
fact_rows as (
    select {{ columns | join(', ') }}
    from {{ ref('s_fct_transaction') }}
),
missing_or_changed as (
    select * from source_rows
    except
    select * from fact_rows
),
extra_or_changed as (
    select * from fact_rows
    except
    select * from source_rows
),
source_totals as (
    select
        count_big(*) as row_count,
        coalesce(sum(cast(amount as decimal(38,2))), 0) as total_amount
    from source_rows
),
fact_totals as (
    select
        count_big(*) as row_count,
        coalesce(sum(cast(amount as decimal(38,2))), 0) as total_amount
    from fact_rows
)
select 'missing or changed source transaction' as failure
from missing_or_changed
union all
select 'extra or changed fact transaction' as failure
from extra_or_changed
union all
select 'count or amount mismatch' as failure
from source_totals as source
cross join fact_totals as fact
where source.row_count <> fact.row_count
   or source.total_amount <> fact.total_amount
