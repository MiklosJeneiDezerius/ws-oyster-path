select customer_id, valid_from, count(*) as state_count
from {{ ref('bis_dim_customer_states') }}
group by customer_id, valid_from
having count(*) > 1
