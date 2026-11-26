CREATE TABLE [core_bronze].[b_oys_account_history] (
    [account_id]        INT             NULL,
    [customer_id]       INT             NULL,
    [balance_amt]       DECIMAL (18, 4) NULL,
    [batch_id]          VARCHAR (36)    NULL,
    [ingested_dttm]     DATETIME2 (6)   NULL,
    [m_scd_id]          VARCHAR (32)    NULL,
    [m_update_at_dttm]  DATETIME2 (6)   NULL,
    [m_valid_from_dttm] DATETIME2 (6)   NULL,
    [m_valid_to_dttm]   DATETIME2 (6)   NULL
);


GO