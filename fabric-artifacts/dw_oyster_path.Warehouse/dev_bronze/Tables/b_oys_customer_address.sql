CREATE TABLE [dev_bronze].[b_oys_customer_address] (
    [AddressID]            VARCHAR (8000) NULL,
    [CustomerID]           VARCHAR (8000) NULL,
    [AddressType]          VARCHAR (8000) NULL,
    [Country]              VARCHAR (8000) NULL,
    [PostalCode]           VARCHAR (8000) NULL,
    [City]                 VARCHAR (8000) NULL,
    [Street]               VARCHAR (8000) NULL,
    [HouseNumber]          VARCHAR (8000) NULL,
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