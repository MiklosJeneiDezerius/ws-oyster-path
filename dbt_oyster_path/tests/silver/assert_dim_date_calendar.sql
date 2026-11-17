{% set bounds = silver_calendar_bounds() %}

select min('coverage') as failure
from {{ ref('dim_date') }}
having count(*) <> {{ bounds.days }}
    or count(distinct calendar_date) <> {{ bounds.days }}
    or min(calendar_date) <> cast('{{ bounds.start }}' as date)
    or max(calendar_date) <> cast('{{ bounds.end }}' as date)

union all

select 'attributes' as failure
from {{ ref('dim_date') }}
where date_key <> year(calendar_date)*10000 + month(calendar_date)*100 + day(calendar_date)
   or calendar_year <> year(calendar_date)
   or calendar_quarter <> datepart(quarter, calendar_date)
   or month_number <> month(calendar_date)
   or day_of_month <> day(calendar_date)
   or iso_weekday <> ((datediff(day, cast('19000101' as date), calendar_date) % 7 + 7) % 7) + 1
   or is_weekend <> case when iso_weekday in (6, 7) then 1 else 0 end
