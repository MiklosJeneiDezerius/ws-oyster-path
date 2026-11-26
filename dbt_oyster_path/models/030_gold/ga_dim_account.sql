SELECT
    [s_dim_account_sk] AS [ga_dim_account_sk],
    [s_dim_customer_sk],
    [account_id],
    [customer_id],
    [current_balance_amt],
    [card_id],
    [card_tier_nm]
FROM {{ ref('s_dim_account') }}
