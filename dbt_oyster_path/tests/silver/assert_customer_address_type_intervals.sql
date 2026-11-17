with active_addresses as (
    select
        customer_id,
        address_type,
        address_id,
        bronze_version_id,
        observed_valid_from,
        observed_valid_to
    from {{ ref('stg_oys_customer_address') }}
    where is_deleted = 0
),
intervals as (
    select
        *,
        max(observed_valid_to) over (
            partition by customer_id, address_type
            order by observed_valid_from, bronze_version_id
            rows between unbounded preceding and 1 preceding
        ) as previous_max_end
    from active_addresses
)
select *
from intervals
where observed_valid_from < previous_max_end
