WITH date_spine AS (
    SELECT [s_dim_date_sk] AS [DateSK], [full_dt] AS [FullDate]
    FROM {{ ref('s_dim_date') }}
    WHERE [full_dt] BETWEEN
        DATEADD(day, -29, CAST('2025-01-01' AS DATE))
        AND CAST(GETDATE() AS DATE)
),
active_accounts AS (
    SELECT DISTINCT [account_id] AS [AccountID]
    FROM {{ ref('s_dim_account') }}
),
daily_account_spine AS (
    SELECT
        d.[DateSK],
        d.[FullDate],
        a.[AccountID],
        DATEADD(day, 1, CAST(d.[FullDate] AS DATETIME2(6))) AS [NextDayStartTS]
    FROM date_spine d
    CROSS JOIN active_accounts a
),
accounts AS (
    SELECT
        [s_dim_account_sk] AS [AccountSK],
        [account_id] AS [AccountID],
        [customer_id] AS [CustomerID]
    FROM {{ ref('s_dim_account') }}
),
customer_history AS (
    SELECT
        [s_dim_customer_sk] AS [CustomerSK],
        [customer_id] AS [CustomerID],
        [valid_from_dttm] AS [ValidFromTS],
        [valid_to_dttm] AS [ValidToTS]
    FROM {{ ref('s_dim_customer') }}
),
historical_card_states AS (
    SELECT
        [s_dim_account_sk] AS [AccountSK],
        [s_dim_card_sk] AS [CardSK],
        [card_id] AS [CardID],
        [card_tier_nm] AS [CardTier],
        [valid_from_dttm] AS [ValidFromTS],
        [valid_to_dttm] AS [ValidToTS]
    FROM {{ ref('s_dim_card') }}
),
daily_transactions AS (
    SELECT
        [s_dim_account_sk] AS [AccountSK],
        CAST([transaction_dttm] AS DATE) AS [TransactionDate],
        SUM([transaction_amt]) AS [DailySpendAmount]
    FROM {{ ref('s_fct_transaction') }}
    GROUP BY [s_dim_account_sk], CAST([transaction_dttm] AS DATE)
),
rolling_spend AS (
    SELECT
        s.[DateSK],
        s.[FullDate],
        s.[AccountID],
        da.[AccountSK],
        cu.[CustomerSK],
        dc.[CardSK],
        dc.[CardID],
        COALESCE(dc.[CardTier], 'Standard') AS [ActualCardTier],
        SUM(COALESCE(t.[DailySpendAmount], 0)) OVER (
            PARTITION BY s.[AccountID]
            ORDER BY s.[FullDate]
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS [Rolling30DaySpend]
    FROM daily_account_spine s
    INNER JOIN accounts da
        ON s.[AccountID] = da.[AccountID]
        OUTER APPLY (
                SELECT TOP (1) ch.[CustomerSK]
                FROM customer_history ch
                WHERE da.[CustomerID] = ch.[CustomerID]
                    AND ch.[ValidFromTS] < s.[NextDayStartTS]
                ORDER BY ch.[ValidFromTS] DESC, ch.[CustomerSK]
        ) cu
    LEFT JOIN daily_transactions t
        ON da.[AccountSK] = t.[AccountSK]
       AND s.[FullDate] = t.[TransactionDate]
    LEFT JOIN historical_card_states dc
        ON da.[AccountSK] = dc.[AccountSK]
       AND dc.[ValidFromTS] < s.[NextDayStartTS]
       AND dc.[ValidToTS] >= s.[NextDayStartTS]
)
SELECT
    CONVERT(VARCHAR(64), HASHBYTES(
        'SHA2_256',
        CONCAT(CAST([DateSK] AS VARCHAR(20)), '_', CAST([AccountID] AS VARCHAR(50)))
    ), 2) AS [g_fct_periodic_spend_snapshot_sk],
    [DateSK] AS [s_dim_date_sk],
    [FullDate] AS [snapshot_dt],
    [AccountSK] AS [s_dim_account_sk],
    [AccountID] AS [account_id],
    [CustomerSK] AS [s_dim_customer_sk],
    [CardSK] AS [s_dim_card_sk],
    [CardID] AS [card_id],
    [ActualCardTier] AS [actual_card_tier_nm],
    [Rolling30DaySpend] AS [rolling_30_day_spend_amt],
    CASE
        WHEN [Rolling30DaySpend] >= 5000.00 THEN 'Platinum'
        WHEN [Rolling30DaySpend] >= 2000.00 THEN 'Gold'
        ELSE 'Standard'
    END AS [calculated_spending_tier_nm],
    CASE
        WHEN [Rolling30DaySpend] >= 5000.00
             AND [ActualCardTier] IN ('Standard', 'Gold') THEN 1
        WHEN [Rolling30DaySpend] >= 2000.00
             AND [ActualCardTier] = 'Standard' THEN 1
        ELSE 0
    END AS [is_upgrade_candidate_flag],
    CASE
        WHEN [ActualCardTier] = 'Platinum'
             AND [Rolling30DaySpend] < 5000.00 THEN 1
        WHEN [ActualCardTier] = 'Gold'
             AND [Rolling30DaySpend] < 2000.00 THEN 1
        ELSE 0
    END AS [is_low_spend_premium_anomaly_flag],
    CASE
        WHEN [ActualCardTier] = 'Platinum'
             AND [Rolling30DaySpend] < 5000.00 THEN 1
        WHEN [ActualCardTier] = 'Gold'
             AND [Rolling30DaySpend] < 2000.00 THEN 1
        ELSE 0
    END AS [is_downgrade_candidate_flag],
    CASE
        WHEN [Rolling30DaySpend] >= 5000.00
             AND [ActualCardTier] = 'Platinum' THEN 'Aligned'
        WHEN [Rolling30DaySpend] >= 2000.00
             AND [Rolling30DaySpend] < 5000.00
             AND [ActualCardTier] = 'Gold' THEN 'Aligned'
        WHEN [Rolling30DaySpend] < 2000.00
             AND [ActualCardTier] = 'Standard' THEN 'Aligned'
        ELSE 'Misaligned'
    END AS [alignment_status_cd],
    CAST(GETDATE() AS DATETIME2(6)) AS [snapshot_materialized_dttm]
FROM rolling_spend
WHERE [FullDate] >= CAST('2025-01-01' AS DATE)