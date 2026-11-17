{{ config(materialized='view') }}

{% set address_types = [
    ('permanent', 'Permanent'),
    ('mailing', 'Mailing'),
    ('billing', 'Billing')
] %}
{% set address_fields = [
    'country', 'postal_code', 'city', 'street', 'house_number'
] %}

select
    timeline.customer_id,
    timeline.valid_from,
    timeline.valid_to,
    customer.first_name,
    customer.last_name,
    customer.is_deleted
    {% for prefix, address_type in address_types %}
        {% for field in address_fields %}
    , {{ prefix }}.{{ field }} as {{ prefix }}_{{ field }}
        {% endfor %}
    {% endfor %}
from {{ ref('int_customer_intervals') }} as timeline
inner join {{ ref('stg_oys_customer') }} as customer
    on customer.customer_id = timeline.customer_id
   and timeline.valid_from >= customer.observed_valid_from
   and timeline.valid_from < customer.observed_valid_to

{% for prefix, address_type in address_types %}
left join {{ ref('stg_oys_customer_address') }} as {{ prefix }}
    on {{ prefix }}.customer_id = timeline.customer_id
   and {{ prefix }}.address_type = '{{ address_type }}'
   and {{ prefix }}.is_deleted = 0
   and timeline.valid_from >= {{ prefix }}.observed_valid_from
   and timeline.valid_from < {{ prefix }}.observed_valid_to
{% endfor %}
