CREATE TABLE [dev_bronze].[b_oys_transaction_ledger] (
    [TransactionID]        VARCHAR (8000) NULL,
    [AccountID]            VARCHAR (8000) NULL,
    [Amount]               VARCHAR (8000) NULL,
    [Timestamp]            VARCHAR (8000) NULL,
    [meta_batch_timestamp] DATETIME2 (6)  NULL,
    [meta_load_timestamp]  DATETIME2 (6)  NULL,
    [meta_source_file]     VARCHAR (8000) NULL
);


GO