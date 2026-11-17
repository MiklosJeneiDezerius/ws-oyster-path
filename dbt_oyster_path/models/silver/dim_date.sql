{% set bounds = silver_calendar_bounds() %}

with digits as (
    select 0 as digit union all select 1 union all select 2 union all select 3
    union all select 4 union all select 5 union all select 6 union all select 7
    union all select 8 union all select 9
),
numbers as (
    select a.digit + 10*b.digit + 100*c.digit + 1000*d.digit + 10000*e.digit as n
    from digits as a
    cross join digits as b
    cross join digits as c
    cross join digits as d
    cross join digits as e
),
dates as (
    select dateadd(day, n, cast('{{ bounds.start }}' as date)) as calendar_date
    from numbers
    where n < {{ bounds.days }}
),
calendar as (
    select
        calendar_date,
        ((datediff(day, cast('19000101' as date), calendar_date) % 7 + 7) % 7) + 1 as iso_weekday
    from dates
)
select
    year(calendar_date)*10000 + month(calendar_date)*100 + day(calendar_date) as date_key,
    calendar_date,
    year(calendar_date) as calendar_year,
    datepart(quarter, calendar_date) as calendar_quarter,
    month(calendar_date) as month_number,
    day(calendar_date) as day_of_month,
    iso_weekday,
    cast(case when iso_weekday in (6, 7) then 1 else 0 end as bit) as is_weekend
from calendar
