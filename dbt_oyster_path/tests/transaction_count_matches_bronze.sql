SELECT
    (SELECT COUNT_BIG(DISTINCT transaction_id) FROM {{ ref('b_oys_transaction') }}) AS [bronze_row_count],
    (SELECT COUNT_BIG(*) FROM {{ ref('s_fct_transaction') }}) AS [silver_row_count]
WHERE (SELECT COUNT_BIG(DISTINCT transaction_id) FROM {{ ref('b_oys_transaction') }})
   <> (SELECT COUNT_BIG(*) FROM {{ ref('s_fct_transaction') }})
