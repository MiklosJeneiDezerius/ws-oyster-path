with intervals as (
    select
        customer_key,
        customer_id,
        valid_from,
        valid_to,
        max(valid_to) over (
            partition by customer_id
            order by valid_from, customer_key
            rows between unbounded preceding and 1 preceding
        ) as previous_max_end
    from {{ ref('dim_customer') }}
    where is_unknown = 0
)
select *
from intervals
where valid_from >= valid_to
   or valid_from < previous_max_end
