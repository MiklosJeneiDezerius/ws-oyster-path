WITH distinct_versions AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY card_id, m_valid_from_dttm
        ORDER BY COALESCE(m_valid_to_dttm, CAST('9999-12-31 00:00:00' AS DATETIME2(6))) DESC,
            m_update_at_dttm DESC, m_scd_id DESC, account_id, card_tier_nm
    ) AS duplicate_rank
    FROM {{ ref('ldh_oys_debitcard') }}
),
snapshot_source AS (
    SELECT
        c.card_id,
        c.account_id,
        TRIM(c.card_tier_nm) AS card_tier_nm,
        CAST(c.m_valid_from_dttm AS DATETIME2(6)) AS original_valid_from_ts,
        COALESCE(
            CAST(c.m_valid_to_dttm AS DATETIME2(6)),
            CAST('9999-12-31 00:00:00.000000' AS DATETIME2(6))
        ) AS valid_to_ts,
        ROW_NUMBER() OVER (
            PARTITION BY c.card_id
            ORDER BY c.m_valid_from_dttm ASC
        ) AS version_number
    FROM distinct_versions c
    WHERE c.duplicate_rank = 1
),
card_versions AS (
    SELECT
        card_id,
        account_id,
        card_tier_nm,
        CASE
            WHEN version_number = 1 THEN CAST('1900-01-01 00:00:00.000000' AS DATETIME2(6))
            ELSE original_valid_from_ts
        END AS valid_from_dttm,
        valid_to_ts AS valid_to_dttm
    FROM snapshot_source
)
SELECT
    CONVERT(
        VARCHAR(64),
        HASHBYTES(
            'SHA2_256',
            CONCAT(CAST(card_id AS VARCHAR(50)), '_', CONVERT(VARCHAR(50), valid_from_dttm, 121))
        ),
        2
    ) AS s_dim_card_sk,
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CAST(account_id AS VARCHAR(50))), 2)
        AS s_dim_account_sk,
    card_id,
    account_id,
    card_tier_nm,
    valid_from_dttm,
    valid_to_dttm,
    CASE
        WHEN valid_to_dttm >= CAST('9999-12-31 00:00:00.000000' AS DATETIME2(6)) THEN 1
        ELSE 0
    END AS is_current_version_flag
FROM card_versions