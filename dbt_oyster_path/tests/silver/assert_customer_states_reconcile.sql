with intervals as (
    select customer_id, valid_from, valid_to, 1 as expected_rows, 0 as actual_rows
    from {{ ref('bis_dim_customer_intervals') }}

    union all

    select customer_id, valid_from, valid_to, 0 as expected_rows, 1 as actual_rows
    from {{ ref('bis_dim_customer_states') }}
)
select
    customer_id,
    valid_from,
    valid_to,
    sum(expected_rows) as expected_rows,
    sum(actual_rows) as actual_rows
from intervals
group by customer_id, valid_from, valid_to
having sum(expected_rows) <> 1
    or sum(actual_rows) <> 1
