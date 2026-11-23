{{ coalesce_observed_dimension(
    relation=ref('bis_dim_account_staging'),
    business_key='account_id',
    version_key='account_key',
    attributes=['customer_id', 'is_deleted'],
    unknown_key='UNKNOWN_ACCOUNT'
) }}
