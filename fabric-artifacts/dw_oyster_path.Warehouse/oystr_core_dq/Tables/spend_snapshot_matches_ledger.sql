CREATE TABLE [oystr_core_dq].[spend_snapshot_matches_ledger] (
    [g_fct_periodic_spend_snapshot_sk] VARCHAR (64)    NULL,
    [rolling_30_day_spend_amt]         DECIMAL (38, 4) NULL,
    [calculated_spending_tier_nm]      VARCHAR (8)     NOT NULL,
    [expected_spend_amt]               DECIMAL (38, 4) NULL
);


GO