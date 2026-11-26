WITH Params AS (
    SELECT CAST('2026-09-15' AS date) AS SnapshotDate
),
Accounts AS (
    SELECT DISTINCT CAST(AccountID AS varchar(100)) AS AccountID
    FROM dbo.ld_oys_account
    WHERE AccountID IS NOT NULL
),
RankedCards AS (
    SELECT
        CAST(c.AccountID AS varchar(100)) AS AccountID,
        COALESCE(NULLIF(LTRIM(RTRIM(c.CardTier)), ''), 'Standard') AS ActualCardTier,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(c.AccountID AS varchar(100))
            ORDER BY c.m_loaded_at_dttm DESC, c.m_source_dttm DESC, c.CardID DESC
        ) AS rn
    FROM dbo.ld_oys_debitcard AS c
),
CurrentCards AS (
    SELECT AccountID, ActualCardTier
    FROM RankedCards
    WHERE rn = 1
),
DailyTransactions AS (
    SELECT
        CAST(tr.AccountID AS varchar(100)) AS AccountID,
        SUM(TRY_CONVERT(decimal(18, 4), tr.Amount)) AS Spend30Day
    FROM dbo.ld_oys_transaction_ledger AS tr
    CROSS JOIN Params AS p
    WHERE TRY_CONVERT(datetime2, tr.[Timestamp])
            >= DATEADD(day, -29, CAST(p.SnapshotDate AS datetime2))
      AND TRY_CONVERT(datetime2, tr.[Timestamp])
            < DATEADD(day, 1, CAST(p.SnapshotDate AS datetime2))
    GROUP BY CAST(tr.AccountID AS varchar(100))
),
Classified AS (
    SELECT
        p.SnapshotDate,
        a.AccountID,
        COALESCE(c.ActualCardTier, 'Standard') AS ActualCardTier,
        COALESCE(dt.Spend30Day, CAST(0 AS decimal(18, 4))) AS Rolling30DaySpend,
        CASE
            WHEN COALESCE(dt.Spend30Day, 0) >= 5000 THEN 'Platinum'
            WHEN COALESCE(dt.Spend30Day, 0) >= 2000 THEN 'Gold'
            ELSE 'Standard'
        END AS CalculatedSpendingTier,
        CASE
            WHEN COALESCE(dt.Spend30Day, 0) >= 5000
                 AND COALESCE(c.ActualCardTier, 'Standard') IN ('Standard', 'Gold') THEN 1
            WHEN COALESCE(dt.Spend30Day, 0) >= 2000
                 AND COALESCE(c.ActualCardTier, 'Standard') = 'Standard' THEN 1
            ELSE 0
        END AS UpgradeCandidate,
        CASE
            WHEN COALESCE(c.ActualCardTier, 'Standard') = 'Platinum'
                 AND COALESCE(dt.Spend30Day, 0) < 5000 THEN 1
            WHEN COALESCE(c.ActualCardTier, 'Standard') = 'Gold'
                 AND COALESCE(dt.Spend30Day, 0) < 2000 THEN 1
            ELSE 0
        END AS DowngradeCandidate,
        CASE
            WHEN COALESCE(dt.Spend30Day, 0) >= 5000
                 AND COALESCE(c.ActualCardTier, 'Standard') = 'Platinum' THEN 'Aligned'
            WHEN COALESCE(dt.Spend30Day, 0) >= 2000
                 AND COALESCE(dt.Spend30Day, 0) < 5000
                 AND COALESCE(c.ActualCardTier, 'Standard') = 'Gold' THEN 'Aligned'
            WHEN COALESCE(dt.Spend30Day, 0) < 2000
                 AND COALESCE(c.ActualCardTier, 'Standard') = 'Standard' THEN 'Aligned'
            ELSE 'Misaligned'
        END AS AlignmentStatus
    FROM Accounts AS a
    CROSS JOIN Params AS p
    LEFT JOIN CurrentCards AS c
        ON a.AccountID = c.AccountID
    LEFT JOIN DailyTransactions AS dt
        ON a.AccountID = dt.AccountID
),
Metrics AS (
    SELECT
        COUNT(DISTINCT AccountID) AS TotalAccounts,
        SUM(UpgradeCandidate) AS UpgradeCandidateCount,
        SUM(DowngradeCandidate) AS DowngradeCandidateCount,
        SUM(CASE WHEN AlignmentStatus = 'Misaligned' THEN 1 ELSE 0 END)
            AS MisalignedAccountCount,
        CAST(
            SUM(CASE WHEN AlignmentStatus = 'Misaligned' THEN 1.0 ELSE 0.0 END)
            / NULLIF(COUNT(DISTINCT AccountID), 0)
            AS decimal(9, 4)
        ) AS TierMismatchRate
    FROM Classified
)
SELECT TOP (1000)
    c.SnapshotDate,
    c.AccountID,
    c.ActualCardTier,
    c.CalculatedSpendingTier,
    c.Rolling30DaySpend,
    c.UpgradeCandidate,
    c.DowngradeCandidate,
    c.AlignmentStatus,
    m.TotalAccounts,
    m.UpgradeCandidateCount,
    m.DowngradeCandidateCount,
    m.MisalignedAccountCount,
    m.TierMismatchRate
FROM Classified AS c
CROSS JOIN Metrics AS m
ORDER BY c.AccountID;