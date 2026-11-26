WITH latest_accounts AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY ingested_dttm DESC, batch_id DESC, customer_id, balance_amt DESC
    ) AS account_rank
    FROM {{ ref('b_oys_account') }}
),
latest_customers AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY customer_id
        ORDER BY is_current_version_flag DESC, valid_from_dttm DESC, s_dim_customer_sk
    ) AS customer_rank
    FROM {{ ref('s_dim_customer') }}
),
current_cards AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY account_id
        ORDER BY valid_from_dttm DESC, card_id DESC, s_dim_card_sk
    ) AS card_rank
    FROM {{ ref('s_dim_card') }}
    WHERE is_current_version_flag = 1
)
SELECT
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CAST(a.[account_id] AS VARCHAR(50))), 2) AS [s_dim_account_sk],
    c.[s_dim_customer_sk],
    a.[account_id],
    a.[customer_id],
    a.[balance_amt] AS [current_balance_amt],
    dc.[card_id],
    dc.[card_tier_nm]
FROM latest_accounts a
LEFT JOIN latest_customers c
    ON a.[customer_id] = c.[customer_id]
    AND c.[customer_rank] = 1
LEFT JOIN current_cards dc
    ON a.[account_id] = dc.[account_id]
    AND dc.[card_rank] = 1
WHERE a.[account_rank] = 1