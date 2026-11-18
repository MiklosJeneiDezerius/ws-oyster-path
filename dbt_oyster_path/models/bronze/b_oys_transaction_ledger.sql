{{ config(
    materialized='incremental',
    incremental_strategy='append',
    full_refresh=false,
    schema='bronze',
    alias='b_oys_transaction_ledger'
) }}

with source_transactions as (
    select *
    from {{ source('landing', 'transaction_ledger') }}
)

select source_transactions.*
from source_transactions

{% if is_incremental() %}
where not exists (
    select 1
    from {{ this }} as existing
    where existing.TransactionID = source_transactions.TransactionID
)
{% endif %}
