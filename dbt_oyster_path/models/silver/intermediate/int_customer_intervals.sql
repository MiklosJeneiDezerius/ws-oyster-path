{{ config(materialized='view') }}

with boundaries as (
    select customer_id, observed_valid_from as boundary_at
    from {{ ref('stg_oys_customer') }}

    union

    select customer_id, observed_valid_to
    from {{ ref('stg_oys_customer') }}

    union

    select customer_id, observed_valid_from
    from {{ ref('stg_oys_customer_address') }}

    union

    select customer_id, observed_valid_to
    from {{ ref('stg_oys_customer_address') }}
),
intervals as (
    select
        customer_id,
        boundary_at as valid_from,
        lead(boundary_at) over (
            partition by customer_id
            order by boundary_at
        ) as valid_to
    from boundaries
)
select
    intervals.customer_id,
    intervals.valid_from,
    intervals.valid_to
from intervals
where intervals.valid_to is not null
  and exists (
      select 1
      from {{ ref('stg_oys_customer') }} as customer
      where customer.customer_id = intervals.customer_id
        and intervals.valid_from >= customer.observed_valid_from
        and intervals.valid_from < customer.observed_valid_to
  )
