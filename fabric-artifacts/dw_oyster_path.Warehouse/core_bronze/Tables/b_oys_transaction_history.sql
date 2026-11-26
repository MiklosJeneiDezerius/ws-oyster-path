CREATE TABLE [core_bronze].[b_oys_transaction_history] (
    [transaction_id]    INT             NULL,
    [account_id]        INT             NULL,
    [transaction_amt]   DECIMAL (18, 4) NULL,
    [transaction_dttm]  DATETIME2 (6)   NULL,
    [batch_id]          VARCHAR (36)    NULL,
    [ingested_dttm]     DATETIME2 (6)   NULL,
    [m_scd_id]          VARCHAR (32)    NULL,
    [m_update_at_dttm]  DATETIME2 (6)   NULL,
    [m_valid_from_dttm] DATETIME2 (6)   NULL,
    [m_valid_to_dttm]   DATETIME2 (6)   NULL
);


GO