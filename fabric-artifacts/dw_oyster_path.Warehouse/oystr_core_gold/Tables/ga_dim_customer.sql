CREATE TABLE [oystr_core_gold].[ga_dim_customer] (
    [ga_dim_customer_sk] VARCHAR (64)   NULL,
    [customer_id]        INT            NULL,
    [first_nm]           VARCHAR (8000) NULL,
    [last_nm]            VARCHAR (8000) NULL,
    [full_nm]            VARCHAR (8000) NOT NULL
);


GO