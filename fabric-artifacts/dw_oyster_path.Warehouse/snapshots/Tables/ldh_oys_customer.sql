CREATE TABLE [snapshots].[ldh_oys_customer] (
    [customer_id]       INT            NULL,
    [first_nm]          VARCHAR (8000) NULL,
    [last_nm]           VARCHAR (8000) NULL,
    [permanent_address] VARCHAR (800)  NULL,
    [mailing_address]   VARCHAR (800)  NULL,
    [billing_address]   VARCHAR (800)  NULL,
    [ingested_dttm]     DATETIME2 (6)  NULL,
    [m_scd_id]          VARCHAR (32)   NULL,
    [m_update_at_dttm]  DATETIME2 (6)  NULL,
    [m_valid_from_dttm] DATETIME2 (6)  NULL,
    [m_valid_to_dttm]   DATETIME2 (6)  NULL
);


GO