CREATE TABLE [oystr_core_dq].[not_null_ga_dim_account_ga_dim_account_sk] (
    [ga_dim_account_sk]   VARCHAR (64)    NULL,
    [s_dim_customer_sk]   VARCHAR (64)    NULL,
    [account_id]          INT             NULL,
    [customer_id]         INT             NULL,
    [current_balance_amt] DECIMAL (18, 4) NULL,
    [card_id]             INT             NULL,
    [card_tier_nm]        VARCHAR (8000)  NULL
);


GO