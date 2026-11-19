CREATE TABLE [dev_bronze].[b_oys_debitcard] (
    [CardID]               VARCHAR (8000) NULL,
    [AccountID]            VARCHAR (8000) NULL,
    [CardTier]             VARCHAR (8000) NULL,
    [IsActive]             VARCHAR (8000) NULL,
    [meta_batch_timestamp] DATETIME2 (6)  NULL,
    [meta_load_timestamp]  DATETIME2 (6)  NULL,
    [meta_source_file]     VARCHAR (8000) NULL,
    [dbt_scd_id]           VARCHAR (32)   NULL,
    [dbt_updated_at]       DATETIME2 (6)  NULL,
    [dbt_valid_from]       DATETIME2 (6)  NULL,
    [dbt_valid_to]         DATETIME2 (6)  NULL,
    [dbt_is_deleted]       VARCHAR (5)    NOT NULL
);


GO