CREATE TABLE [oystr_core_dq].[not_null_s_fct_transaction_s_dim_customer_sk] (
    [s_fct_transaction_sk] VARCHAR (64)    NULL,
    [s_dim_card_sk]        VARCHAR (64)    NULL,
    [s_dim_account_sk]     VARCHAR (64)    NULL,
    [s_dim_customer_sk]    VARCHAR (64)    NULL,
    [s_dim_date_sk]        INT             NULL,
    [transaction_id]       INT             NULL,
    [account_id]           INT             NULL,
    [transaction_amt]      DECIMAL (18, 4) NULL,
    [transaction_dttm]     DATETIME2 (6)   NULL
);


GO