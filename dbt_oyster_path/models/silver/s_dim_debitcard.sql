{{ coalesce_observed_dimension(
    relation=ref('bis_dim_debitcard_staging'),
    business_key='card_id',
    version_key='debitcard_key',
    attributes=['account_id', 'card_tier', 'is_active', 'is_deleted']
) }}
