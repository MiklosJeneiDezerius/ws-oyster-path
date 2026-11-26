SELECT
    (SELECT COUNT_BIG(DISTINCT account_id) FROM {{ ref('b_oys_account') }}) AS [bronze_row_count],
    (SELECT COUNT_BIG(*) FROM {{ ref('s_dim_account') }}) AS [silver_row_count]
WHERE (SELECT COUNT_BIG(DISTINCT account_id) FROM {{ ref('b_oys_account') }})
   <> (SELECT COUNT_BIG(*) FROM {{ ref('s_dim_account') }})
