CREATE TABLE [dev_gold].[g_fct_account_tier_daily_snapshot] (
    [date_key]                      INT             NULL,
    [snapshot_date]                 DATE            NULL,
    [account_key]                   VARCHAR (64)    NULL,
    [account_id]                    VARCHAR (8000)  NULL,
    [customer_key]                  VARCHAR (64)    NULL,
    [debitcard_key]                 VARCHAR (64)    NULL,
    [actual_card_status]            VARCHAR (14)    NOT NULL,
    [actual_card_tier]              VARCHAR (8000)  NULL,
    [rolling_30_day_spend]          DECIMAL (38, 2) NULL,
    [calculated_spending_tier]      VARCHAR (8)     NOT NULL,
    [upgrade_candidate]             BIT             NULL,
    [downgrade_candidate]           BIT             NULL,
    [alignment_status]              VARCHAR (17)    NOT NULL,
    [customer_lookup_status]        VARCHAR (19)    NOT NULL,
    [has_full_30_day_source_window] BIT             NULL
);


GO