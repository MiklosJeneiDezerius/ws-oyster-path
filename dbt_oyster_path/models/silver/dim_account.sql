{{ coalesce_observed_dimension(
    relation=ref('stg_oys_account'),
    business_key='account_id',
    version_key='account_key',
    attributes=['customer_id', 'is_deleted'],
    unknown_key='UNKNOWN_ACCOUNT'
) }}
