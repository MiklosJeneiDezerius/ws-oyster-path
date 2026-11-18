{% for model, key, business_keys, reserved_key in [
    ('s_dim_customer', 'customer_key', ['customer_id'], 'UNKNOWN_CUSTOMER'),
    ('s_dim_account', 'account_key', ['account_id', 'customer_id'], 'UNKNOWN_ACCOUNT')
] %}
select min('{{ model }}: missing or duplicate Unknown') as failure
from {{ ref(model) }}
having sum(case when is_unknown = 1 then 1 else 0 end) <> 1
    or count(*) = 0

union all

select '{{ model }}: invalid member contract' as failure
from {{ ref(model) }}
where is_unknown is null
   or is_unknown not in (0, 1)
   or (
       is_unknown = 1 and (
           {{ key }} is null or {{ key }} <> '{{ reserved_key }}'
           {% for business_key in business_keys %}
           or {{ business_key }} is not null
           {% endfor %}
           or is_deleted is not null
           or is_current is null or is_current <> 1
           or valid_from is null or valid_from <> cast('0001-01-01' as datetime2(6))
           or valid_to is null or valid_to <> cast('9999-12-31' as datetime2(6))
       )
   )
   or (is_unknown = 0 and {{ key }} = '{{ reserved_key }}')
{% if not loop.last %}union all{% endif %}
{% endfor %}
