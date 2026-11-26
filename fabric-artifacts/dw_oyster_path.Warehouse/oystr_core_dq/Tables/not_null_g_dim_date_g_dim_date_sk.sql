CREATE TABLE [oystr_core_dq].[not_null_g_dim_date_g_dim_date_sk] (
    [g_dim_date_sk]        INT          NULL,
    [full_dt]              DATE         NULL,
    [calendar_year_num]    INT          NULL,
    [calendar_quarter_num] INT          NULL,
    [calendar_quarter_nm]  VARCHAR (13) NOT NULL,
    [month_of_year_num]    INT          NULL,
    [month_nm]             VARCHAR (50) NULL,
    [month_short_nm]       VARCHAR (50) NULL,
    [week_of_year_num]     INT          NULL,
    [day_of_month_num]     INT          NULL,
    [day_of_year_num]      INT          NULL,
    [day_of_week_num]      INT          NULL,
    [day_nm]               VARCHAR (50) NULL,
    [day_short_nm]         VARCHAR (50) NULL,
    [is_weekend_flag]      INT          NOT NULL,
    [year_month_cd]        VARCHAR (7)  NULL
);


GO