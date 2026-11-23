select
    time_key,
    time_of_day,
    hour_number,
    minute_number,
    second_number
from {{ ref('s_dim_time') }}
