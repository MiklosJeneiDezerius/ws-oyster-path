CREATE TABLE [oystr_core_bronze].[b_oys_debit_card] (
    [card_id]       INT            NULL,
    [account_id]    INT            NULL,
    [card_tier_nm]  VARCHAR (8000) NULL,
    [batch_id]      VARCHAR (36)   NULL,
    [ingested_dttm] DATETIME2 (6)  NULL
);


GO