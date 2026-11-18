CREATE TABLE [dev_silver].[dim_debitcard] (
    [debitcard_key]       VARCHAR (64)   NULL,
    [card_id]             VARCHAR (8000) NULL,
    [account_id]          VARCHAR (8000) NULL,
    [card_tier]           VARCHAR (8000) NULL,
    [is_active]           BIT            NULL,
    [is_deleted]          BIT            NULL,
    [valid_from]          DATETIME2 (6)  NULL,
    [valid_to]            DATETIME2 (6)  NULL,
    [observed_valid_from] DATETIME2 (6)  NULL,
    [is_inferred_start]   BIT            NULL,
    [is_current]          BIT            NULL
);


GO