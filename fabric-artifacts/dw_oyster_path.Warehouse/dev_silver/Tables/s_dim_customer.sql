CREATE TABLE [dev_silver].[s_dim_customer] (
    [customer_key]           VARCHAR (64)   NULL,
    [customer_id]            VARCHAR (8000) NULL,
    [first_name]             VARCHAR (8000) NULL,
    [last_name]              VARCHAR (8000) NULL,
    [is_deleted]             BIT            NULL,
    [permanent_country]      VARCHAR (8000) NULL,
    [permanent_postal_code]  VARCHAR (8000) NULL,
    [permanent_city]         VARCHAR (8000) NULL,
    [permanent_street]       VARCHAR (8000) NULL,
    [permanent_house_number] VARCHAR (8000) NULL,
    [mailing_country]        VARCHAR (8000) NULL,
    [mailing_postal_code]    VARCHAR (8000) NULL,
    [mailing_city]           VARCHAR (8000) NULL,
    [mailing_street]         VARCHAR (8000) NULL,
    [mailing_house_number]   VARCHAR (8000) NULL,
    [billing_country]        VARCHAR (8000) NULL,
    [billing_postal_code]    VARCHAR (8000) NULL,
    [billing_city]           VARCHAR (8000) NULL,
    [billing_street]         VARCHAR (8000) NULL,
    [billing_house_number]   VARCHAR (8000) NULL,
    [valid_from]             DATETIME2 (6)  NULL,
    [valid_to]               DATETIME2 (6)  NULL,
    [observed_valid_from]    DATETIME2 (6)  NULL,
    [is_inferred_start]      BIT            NULL,
    [is_current]             BIT            NULL,
    [is_unknown]             BIT            NULL
);


GO