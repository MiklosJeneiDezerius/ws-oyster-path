CREATE TABLE [oystr_core_bronze].[b_oys_transaction] (
    [transaction_id]   INT             NULL,
    [account_id]       INT             NULL,
    [transaction_amt]  DECIMAL (18, 4) NULL,
    [transaction_dttm] DATETIME2 (6)   NULL,
    [batch_id]         VARCHAR (36)    NULL,
    [ingested_dttm]    DATETIME2 (6)   NULL
);


GO