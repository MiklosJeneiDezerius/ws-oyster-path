{% snapshot ldh_oys_debitcard %}

{{ config(
    target_database=var('target_snapshot_db'),
    target_schema=var('target_snapshot_schema'),
    unique_key='card_id',
    strategy='check',
    check_cols=['account_id', 'card_tier_nm'],
    hard_deletes='invalidate',
    snapshot_meta_column_names={
        'dbt_valid_from': 'm_valid_from_dttm',
        'dbt_valid_to': 'm_valid_to_dttm',
        'dbt_scd_id': 'm_scd_id',
        'dbt_updated_at': 'm_update_at_dttm'
    },
    dbt_valid_to_current="CAST('9999-12-31 00:00:00' AS DATETIME2(6))"
) }}

-- Serialize core snapshots because Fabric Warehouse locks tables during writes.
-- depends_on: {{ ref('ldh_oys_customer') }}

SELECT
    card_id,
    account_id,
    card_tier_nm
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY card_id ORDER BY account_id, card_tier_nm
    ) AS card_rank
    FROM {{ ref('b_oys_debit_card_history') }}
    WHERE CAST(m_valid_to_dttm AS DATETIME2(6)) =
        CAST('9999-12-31 00:00:00' AS DATETIME2(6))
) AS current_cards
WHERE card_rank = 1

{% endsnapshot %}