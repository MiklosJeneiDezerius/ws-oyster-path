CREATE TABLE [dev_silver].[s_dim_account] (
    [account_key]         VARCHAR (64)   NULL,
    [account_id]          VARCHAR (8000) NULL,
    [customer_id]         VARCHAR (8000) NULL,
    [is_deleted]          BIT            NULL,
    [valid_from]          DATETIME2 (6)  NULL,
    [valid_to]            DATETIME2 (6)  NULL,
    [observed_valid_from] DATETIME2 (6)  NULL,
    [is_inferred_start]   BIT            NULL,
    [is_current]          BIT            NULL,
    [is_unknown]          BIT            NULL
);


GO