CREATE TABLE [dev_gold].[gold_fct_transaction] (
    [transaction_id]         VARCHAR (8000)  NULL,
    [transaction_id_raw]     VARCHAR (8000)  NULL,
    [account_id]             VARCHAR (8000)  NULL,
    [account_id_raw]         VARCHAR (8000)  NULL,
    [customer_id]            VARCHAR (8000)  NULL,
    [account_key]            VARCHAR (64)    NULL,
    [customer_key]           VARCHAR (64)    NULL,
    [date_key]               INT             NULL,
    [time_key]               INT             NULL,
    [transaction_timestamp]  DATETIME2 (6)   NULL,
    [amount]                 DECIMAL (18, 2) NULL,
    [account_lookup_status]  VARCHAR (18)    NOT NULL,
    [customer_lookup_status] VARCHAR (19)    NOT NULL,
    [meta_batch_timestamp]   DATETIME2 (6)   NULL,
    [meta_load_timestamp]    DATETIME2 (6)   NULL,
    [meta_source_file]       VARCHAR (8000)  NULL
);


GO