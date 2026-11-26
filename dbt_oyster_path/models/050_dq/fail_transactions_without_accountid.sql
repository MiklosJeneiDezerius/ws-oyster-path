SELECT
    [transaction_id],
    [transaction_amt],
    [transaction_dttm]
FROM {{ ref('b_oys_transaction') }}
WHERE [account_id] IS NULL
