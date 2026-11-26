CREATE TABLE [oystr_core_dq].[not_null_b_oys_account_balance_amt] (
    [account_id]    INT             NULL,
    [customer_id]   INT             NULL,
    [balance_amt]   DECIMAL (18, 4) NULL,
    [batch_id]      VARCHAR (36)    NULL,
    [ingested_dttm] DATETIME2 (6)   NULL
);


GO