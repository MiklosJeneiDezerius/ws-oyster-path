select
    account_key,
    account_id,
    customer_id,
    valid_from,
    valid_to,
    observed_valid_from,
    is_inferred_start,
    is_current,
    is_deleted,
    is_unknown
from {{ ref('s_dim_account') }}
