select customer_id, count(*) as current_version_count
from {{ ref('dim_customer') }}
where is_current = 1 and is_unknown = 0
group by customer_id
having count(*) > 1
