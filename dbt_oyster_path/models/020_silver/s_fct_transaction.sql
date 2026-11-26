WITH latest_transactions AS (
    SELECT *, ROW_NUMBER() OVER (
        PARTITION BY transaction_id
        ORDER BY ingested_dttm DESC, batch_id DESC, transaction_dttm DESC, account_id, transaction_amt DESC
    ) AS transaction_rank
    FROM {{ ref('b_oys_transaction') }}
),
source_transactions AS (
    SELECT
        t.[transaction_id],
        t.[account_id],
        t.[transaction_amt],
        t.[transaction_dttm]
    FROM latest_transactions t
    WHERE t.transaction_rank = 1
),
accounts AS (
    SELECT
        [s_dim_account_sk],
        [account_id],
        [customer_id]
    FROM {{ ref('s_dim_account') }}
),
customer_history AS (
    SELECT
        [s_dim_customer_sk],
        [customer_id],
        [valid_from_dttm],
        [valid_to_dttm]
    FROM {{ ref('s_dim_customer') }}
),
card_history AS (
    SELECT
        [s_dim_card_sk],
        [account_id],
        [valid_from_dttm],
        [valid_to_dttm]
    FROM {{ ref('s_dim_card') }}
)
SELECT
    CONVERT(VARCHAR(64), HASHBYTES('SHA2_256', CAST(st.[transaction_id] AS VARCHAR(50))), 2) AS [s_fct_transaction_sk],
    COALESCE(dc.[s_dim_card_sk], 'UNASSIGNED') AS [s_dim_card_sk],
    a.[s_dim_account_sk],
    cu.[s_dim_customer_sk],
    CONVERT(INT, CONVERT(CHAR(8), st.[transaction_dttm], 112)) AS [s_dim_date_sk],
    st.[transaction_id],
    st.[account_id],
    st.[transaction_amt],
    st.[transaction_dttm]
FROM source_transactions st
INNER JOIN accounts a
    ON st.[account_id] = a.[account_id]
INNER JOIN customer_history cu
    ON a.[customer_id] = cu.[customer_id]
   AND st.[transaction_dttm] >= cu.[valid_from_dttm]
   AND st.[transaction_dttm] < cu.[valid_to_dttm]
LEFT JOIN card_history dc
    ON st.[account_id] = dc.[account_id]
   AND st.[transaction_dttm] >= dc.[valid_from_dttm]
   AND st.[transaction_dttm] < dc.[valid_to_dttm]
