CREATE TABLE [oystr_core_dq].[not_null_g_fct_periodic_spend_snapshot_s_dim_customer_sk] (
    [g_fct_periodic_spend_snapshot_sk]  VARCHAR (64)    NULL,
    [s_dim_date_sk]                     INT             NULL,
    [snapshot_dt]                       DATE            NULL,
    [s_dim_account_sk]                  VARCHAR (64)    NULL,
    [account_id]                        INT             NULL,
    [s_dim_customer_sk]                 VARCHAR (64)    NULL,
    [s_dim_card_sk]                     VARCHAR (64)    NULL,
    [card_id]                           INT             NULL,
    [actual_card_tier_nm]               VARCHAR (8000)  NULL,
    [rolling_30_day_spend_amt]          DECIMAL (38, 4) NULL,
    [calculated_spending_tier_nm]       VARCHAR (8)     NOT NULL,
    [is_upgrade_candidate_flag]         INT             NOT NULL,
    [is_low_spend_premium_anomaly_flag] INT             NOT NULL,
    [is_downgrade_candidate_flag]       INT             NOT NULL,
    [alignment_status_cd]               VARCHAR (10)    NOT NULL,
    [snapshot_materialized_dttm]        DATETIME2 (6)   NULL
);


GO