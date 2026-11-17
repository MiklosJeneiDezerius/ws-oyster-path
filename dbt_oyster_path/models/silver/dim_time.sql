with digits as (
    select 0 as digit union all select 1 union all select 2 union all select 3
    union all select 4 union all select 5 union all select 6 union all select 7
    union all select 8 union all select 9
),
seconds as (
    select
        a.digit + 10*b.digit + 100*c.digit + 1000*d.digit + 10000*e.digit as n
    from digits as a
    cross join digits as b
    cross join digits as c
    cross join digits as d
    cross join digits as e
)
select
    n as time_key,
    cast(dateadd(second, n, cast('19000101' as datetime2(6))) as time(0)) as time_of_day,
    n / 3600 as hour_number,
    (n % 3600) / 60 as minute_number,
    n % 60 as second_number
from seconds
where n < 86400
