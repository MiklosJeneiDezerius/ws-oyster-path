{% snapshot ldh_oys_customer %}

{% set address_columns = ['permanent_address', 'mailing_address', 'billing_address'] %}
{% set previous_addresses = namespace(available=false) %}
{% if execute and not var('include_address', false) %}
    {% set previous_snapshot = adapter.get_relation(database=var('target_snapshot_db'), schema=var('target_snapshot_schema'), identifier='ldh_oys_customer') %}
    {% if previous_snapshot %}
        {% set existing_columns = adapter.get_columns_in_relation(previous_snapshot) | map(attribute='name') | map('lower') | list %}
        {% set previous_addresses.available = 'permanent_address' in existing_columns and 'mailing_address' in existing_columns and 'billing_address' in existing_columns %}
    {% endif %}
{% endif %}

{{ config(
    target_database=var('target_snapshot_db'),
    target_schema=var('target_snapshot_schema'),
    unique_key='customer_id',
    strategy='check',
    check_cols=['first_nm', 'last_nm', 'permanent_address', 'mailing_address', 'billing_address'],
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
    current_customers.customer_id,
    current_customers.first_nm,
    current_customers.last_nm,
    {% for column in address_columns %}
    {% if var('include_address', false) or previous_addresses.available %}
    addresses.{{ column }},
    {% else %}
    CAST(NULL AS VARCHAR(800)) AS {{ column }},
    {% endif %}
    {% endfor %}
    current_customers.ingested_dttm
FROM (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id ORDER BY first_nm, last_nm
    ) AS customer_rank
    FROM {{ ref('b_oys_customer_history') }}
    WHERE CAST(m_valid_to_dttm AS DATETIME2(6)) =
        CAST('9999-12-31 00:00:00' AS DATETIME2(6))
) AS current_customers
{% if var('include_address', false) %}
LEFT JOIN (
    SELECT customer_id,
        MAX(CASE WHEN address_type = 'PERMANENT' THEN formatted_address END) AS permanent_address,
        MAX(CASE WHEN address_type = 'MAILING' THEN formatted_address END) AS mailing_address,
        MAX(CASE WHEN address_type = 'BILLING' THEN formatted_address END) AS billing_address
    FROM (
        SELECT customer_id, UPPER(TRIM(address_type)) AS address_type,
            CAST(NULLIF(CONCAT_WS(', ',
                NULLIF(TRIM(CONCAT_WS(' ', NULLIF(TRIM(street), ''), NULLIF(TRIM(house_number), ''))), ''),
                NULLIF(TRIM(city), ''), NULLIF(TRIM(postal_code), ''), NULLIF(TRIM(country), '')
            ), '') AS VARCHAR(800)) AS formatted_address,
            ROW_NUMBER() OVER (
                PARTITION BY customer_id, UPPER(TRIM(address_type))
                ORDER BY address_id DESC, street, house_number, city, postal_code, country
            ) AS address_rank
        FROM {{ ref('b_oys_customer_address_history') }}
        WHERE CAST(m_valid_to_dttm AS DATETIME2(6)) =
            CAST('9999-12-31 00:00:00' AS DATETIME2(6))
    ) AS ranked_addresses
    WHERE address_rank = 1
    GROUP BY customer_id
) AS addresses ON current_customers.customer_id = addresses.customer_id
{% elif previous_addresses.available %}
LEFT JOIN (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY m_valid_from_dttm DESC, m_update_at_dttm DESC, m_scd_id DESC
    ) AS address_version_rank
    FROM {{ previous_snapshot }}
) AS addresses ON current_customers.customer_id = addresses.customer_id
    AND addresses.address_version_rank = 1
{% endif %}
WHERE customer_rank = 1

{% endsnapshot %}