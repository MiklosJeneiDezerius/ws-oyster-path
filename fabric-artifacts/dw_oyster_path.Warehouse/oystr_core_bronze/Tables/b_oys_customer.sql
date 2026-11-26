CREATE TABLE [oystr_core_bronze].[b_oys_customer] (
    [customer_id]   INT            NULL,
    [first_nm]      VARCHAR (8000) NULL,
    [last_nm]       VARCHAR (8000) NULL,
    [batch_id]      VARCHAR (36)   NULL,
    [ingested_dttm] DATETIME2 (6)  NULL
);


GO