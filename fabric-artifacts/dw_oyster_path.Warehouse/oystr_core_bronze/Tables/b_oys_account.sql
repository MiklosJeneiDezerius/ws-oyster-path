CREATE TABLE [oystr_core_bronze].[b_oys_account] (
    [account_id]    INT             NULL,
    [customer_id]   INT             NULL,
    [balance_amt]   DECIMAL (18, 4) NULL,
    [batch_id]      VARCHAR (36)    NULL,
    [ingested_dttm] DATETIME2 (6)   NULL
);


GO