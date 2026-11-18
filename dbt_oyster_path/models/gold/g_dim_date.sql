select
    date_key,
    calendar_date,
    calendar_year,
    calendar_quarter,
    month_number,
    day_of_month,
    iso_weekday,
    is_weekend
from {{ ref('s_dim_date') }}
