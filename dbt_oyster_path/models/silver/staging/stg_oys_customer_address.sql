select
    AddressID as address_id_raw,
    nullif(trim(AddressID), '') as address_id,
    CustomerID as customer_id_raw,
    nullif(trim(CustomerID), '') as customer_id,
    nullif(trim(AddressType), '') as address_type,
    nullif(trim(Country), '') as country,
    nullif(trim(PostalCode), '') as postal_code,
    nullif(trim(City), '') as city,
    nullif(trim(Street), '') as street,
    nullif(trim(HouseNumber), '') as house_number,
    dbt_scd_id as bronze_version_id,
    dbt_valid_from as observed_valid_from,
    dbt_valid_to as observed_valid_to,
    dbt_is_deleted as source_deleted_raw,
    case lower(trim(dbt_is_deleted))
        when 'true' then cast(1 as bit)
        when 'false' then cast(0 as bit)
        else cast(null as bit)
    end as is_deleted,
    meta_batch_timestamp,
    meta_load_timestamp,
    meta_source_file
from {{ ref('bz_oys_customer_address') }}
