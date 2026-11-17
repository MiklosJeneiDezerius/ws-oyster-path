select min('coverage') as failure
from {{ ref('dim_time') }}
having count(*) <> 86400
    or count(distinct time_key) <> 86400
    or min(time_key) <> 0
    or max(time_key) <> 86399

union all

select 'attributes' as failure
from {{ ref('dim_time') }}
where time_key <> hour_number*3600 + minute_number*60 + second_number
   or hour_number not between 0 and 23
   or minute_number not between 0 and 59
   or second_number not between 0 and 59
   or datepart(hour, time_of_day) <> hour_number
   or datepart(minute, time_of_day) <> minute_number
   or datepart(second, time_of_day) <> second_number
