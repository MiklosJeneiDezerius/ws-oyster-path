create view [dev_silver].[stg_oys_customer] as select
    CustomerID as customer_id_raw,
    nullif(trim(CustomerID), '') as customer_id,
    nullif(trim(FirstName), '') as first_name,
    nullif(trim(LastName), '') as last_name,
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
from [dw_oyster_path].[dev_bronze].[bz_oys_customer];

GO