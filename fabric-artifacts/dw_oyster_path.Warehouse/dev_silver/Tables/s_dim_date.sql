CREATE TABLE [dev_silver].[s_dim_date] (
    [date_key]         INT  NULL,
    [calendar_date]    DATE NULL,
    [calendar_year]    INT  NULL,
    [calendar_quarter] INT  NULL,
    [month_number]     INT  NULL,
    [day_of_month]     INT  NULL,
    [iso_weekday]      INT  NULL,
    [is_weekend]       BIT  NULL
);


GO