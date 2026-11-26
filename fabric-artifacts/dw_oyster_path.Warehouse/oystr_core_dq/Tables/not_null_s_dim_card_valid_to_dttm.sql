CREATE TABLE [oystr_core_dq].[not_null_s_dim_card_valid_to_dttm] (
    [s_dim_card_sk]           VARCHAR (64)   NULL,
    [s_dim_account_sk]        VARCHAR (64)   NULL,
    [card_id]                 INT            NULL,
    [account_id]              INT            NULL,
    [card_tier_nm]            VARCHAR (8000) NULL,
    [valid_from_dttm]         DATETIME2 (6)  NULL,
    [valid_to_dttm]           DATETIME2 (6)  NULL,
    [is_current_version_flag] INT            NOT NULL
);


GO