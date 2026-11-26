WITH expected_spend AS (
    SELECT
        g.[g_fct_periodic_spend_snapshot_sk],
        g.[rolling_30_day_spend_amt],
        g.[calculated_spending_tier_nm],
        COALESCE(SUM(t.[transaction_amt]), 0) AS [expected_spend_amt]
    FROM {{ ref('g_fct_periodic_spend_snapshot') }} g
    LEFT JOIN {{ ref('s_fct_transaction') }} t
        ON g.[s_dim_account_sk] = t.[s_dim_account_sk]
       AND t.[transaction_dttm] >= DATEADD(day, -29, CAST(g.[snapshot_dt] AS DATETIME2(6)))
       AND t.[transaction_dttm] < DATEADD(day, 1, CAST(g.[snapshot_dt] AS DATETIME2(6)))
    GROUP BY
        g.[g_fct_periodic_spend_snapshot_sk],
        g.[rolling_30_day_spend_amt],
        g.[calculated_spending_tier_nm]
)
SELECT *
FROM expected_spend
WHERE [rolling_30_day_spend_amt] <> [expected_spend_amt]
   OR [calculated_spending_tier_nm] <> CASE
       WHEN [expected_spend_amt] >= 5000.00 THEN 'Platinum'
       WHEN [expected_spend_amt] >= 2000.00 THEN 'Gold'
       ELSE 'Standard'
   END
