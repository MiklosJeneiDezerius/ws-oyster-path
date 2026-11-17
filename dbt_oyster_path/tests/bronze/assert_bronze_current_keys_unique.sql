with current_versions as (
    select 'customer' as entity_name, cast(CustomerID as varchar(8000)) as business_key
    from {{ ref('bz_oys_customer') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))

    union all
    select 'customer_address', cast(AddressID as varchar(8000))
    from {{ ref('bz_oys_customer_address') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))

    union all
    select 'account', cast(AccountID as varchar(8000))
    from {{ ref('bz_oys_account') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))

    union all
    select 'debitcard', cast(CardID as varchar(8000))
    from {{ ref('bz_oys_debitcard') }}
    where dbt_valid_to = cast('9999-12-31' as datetime2(6))
)
select entity_name, business_key, count(*) as current_row_count
from current_versions
group by entity_name, business_key
having business_key is null or count(*) > 1
