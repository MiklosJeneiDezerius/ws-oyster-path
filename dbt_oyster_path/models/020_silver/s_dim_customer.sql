WITH distinct_versions AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id, m_valid_from_dttm
        ORDER BY COALESCE(m_valid_to_dttm, CAST('9999-12-31 00:00:00' AS DATETIME2(6))) DESC,
            m_update_at_dttm DESC, m_scd_id DESC, first_nm, last_nm
    ) AS duplicate_rank
    FROM {{ ref('ldh_oys_customer') }}
),
snapshot_records AS (
    SELECT
        customer_id,
        TRIM(first_nm) AS first_nm,
        TRIM(last_nm) AS last_nm,
        permanent_address,
        mailing_address,
        billing_address,
        CAST(m_valid_from_dttm AS DATETIME2(6)) AS snapshot_valid_from_ts,
        COALESCE(
            CAST(m_valid_to_dttm AS DATETIME2(6)),
            CAST('9999-12-31 00:00:00.000000' AS DATETIME2(6))
        ) AS valid_to_ts,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY m_valid_from_dttm ASC
        ) AS version_number
    FROM distinct_versions
    WHERE duplicate_rank = 1
),
customer_versions AS (
    SELECT
        customer_id,
        first_nm,
        last_nm,
        permanent_address,
        mailing_address,
        billing_address,
        CASE
            WHEN version_number = 1 THEN CAST('1900-01-01 00:00:00.000000' AS DATETIME2(6))
            ELSE snapshot_valid_from_ts
        END AS valid_from_dttm,
        valid_to_ts AS valid_to_dttm
    FROM snapshot_records
)
SELECT
    CONVERT(
        VARCHAR(64),
        HASHBYTES(
            'SHA2_256',
            CONCAT(
                CAST(customer_id AS VARCHAR(50)),
                '|',
                CONVERT(VARCHAR(50), valid_from_dttm, 121)
            )
        ),
        2
    ) AS s_dim_customer_sk,
    customer_id,
    first_nm,
    last_nm,
    CONCAT(first_nm, ' ', last_nm) AS full_nm,
    permanent_address,
    mailing_address,
    billing_address,
    valid_from_dttm,
    valid_to_dttm,
    CASE
        WHEN valid_to_dttm >= CAST('9999-12-31 00:00:00.000000' AS DATETIME2(6)) THEN 1
        ELSE 0
    END AS is_current_version_flag
FROM customer_versions