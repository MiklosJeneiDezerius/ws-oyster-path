CREATE TABLE [core_bronze].[b_oys_debit_card_history] (
    [card_id]           INT            NULL,
    [account_id]        INT            NULL,
    [card_tier_nm]      VARCHAR (8000) NULL,
    [batch_id]          VARCHAR (36)   NULL,
    [ingested_dttm]     DATETIME2 (6)  NULL,
    [m_scd_id]          VARCHAR (32)   NULL,
    [m_update_at_dttm]  DATETIME2 (6)  NULL,
    [m_valid_from_dttm] DATETIME2 (6)  NULL,
    [m_valid_to_dttm]   DATETIME2 (6)  NULL
);


GO