select customer_id, valid_from, count(*) as state_count
from {{ ref('int_customer_states') }}
group by customer_id, valid_from
having count(*) > 1
