select
    debitcard_key,
    card_id,
    account_id,
    card_tier,
    is_active,
    valid_from,
    valid_to,
    observed_valid_from,
    is_inferred_start,
    is_current,
    is_deleted
from {{ ref('s_dim_debitcard') }}
