{% snapshot b_oys_transaction_history %}

{{ config(
    target_database=var('target_snapshot_db'),
    target_schema='core_bronze',
    unique_key='transaction_id',
    strategy='check',
    check_cols=['account_id', 'transaction_amt', 'transaction_dttm'],
    hard_deletes='invalidate',
    snapshot_meta_column_names={
        'dbt_valid_from': 'm_valid_from_dttm',
        'dbt_valid_to': 'm_valid_to_dttm',
        'dbt_scd_id': 'm_scd_id',
        'dbt_updated_at': 'm_update_at_dttm'
    },
    dbt_valid_to_current="CAST('9999-12-31 00:00:00' AS DATETIME2(6))"
) }}

SELECT
    transaction_id,
    account_id,
    transaction_amt,
    transaction_dttm,
    batch_id,
    ingested_dttm
FROM {{ ref('b_oys_transaction') }}

{% endsnapshot %}
