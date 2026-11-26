CREATE TABLE [oystr_core_dq].[not_null_s_dim_customer_s_dim_customer_sk] (
    [s_dim_customer_sk]       VARCHAR (64)   NULL,
    [customer_id]             INT            NULL,
    [first_nm]                VARCHAR (8000) NULL,
    [last_nm]                 VARCHAR (8000) NULL,
    [full_nm]                 VARCHAR (8000) NOT NULL,
    [permanent_address]       VARCHAR (800)  NULL,
    [mailing_address]         VARCHAR (800)  NULL,
    [billing_address]         VARCHAR (800)  NULL,
    [valid_from_dttm]         DATETIME2 (6)  NULL,
    [valid_to_dttm]           DATETIME2 (6)  NULL,
    [is_current_version_flag] INT            NOT NULL
);


GO