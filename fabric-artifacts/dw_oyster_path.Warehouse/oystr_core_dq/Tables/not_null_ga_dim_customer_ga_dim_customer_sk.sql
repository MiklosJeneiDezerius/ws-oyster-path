CREATE TABLE [oystr_core_dq].[not_null_ga_dim_customer_ga_dim_customer_sk] (
    [ga_dim_customer_sk] VARCHAR (64)   NULL,
    [customer_id]        INT            NULL,
    [first_nm]           VARCHAR (8000) NULL,
    [last_nm]            VARCHAR (8000) NULL,
    [full_nm]            VARCHAR (8000) NOT NULL
);


GO