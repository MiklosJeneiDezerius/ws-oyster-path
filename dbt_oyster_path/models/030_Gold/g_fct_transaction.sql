select
    transaction_id,
    transaction_id_raw,
    account_id,
    account_id_raw,
    customer_id,
    account_key,
    customer_key,
    date_key,
    time_key,
    transaction_timestamp,
    amount,
    account_lookup_status,
    customer_lookup_status,
    meta_batch_timestamp,
    meta_load_timestamp,
    meta_source_file
from {{ ref('s_fct_transaction') }}
