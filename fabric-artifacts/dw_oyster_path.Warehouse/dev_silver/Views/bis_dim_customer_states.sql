create view [dev_silver].[bis_dim_customer_states] as 




select
    timeline.customer_id,
    timeline.valid_from,
    timeline.valid_to,
    customer.first_name,
    customer.last_name,
    customer.is_deleted
    
        
    , permanent.country as permanent_country
        
    , permanent.postal_code as permanent_postal_code
        
    , permanent.city as permanent_city
        
    , permanent.street as permanent_street
        
    , permanent.house_number as permanent_house_number
        
    
        
    , mailing.country as mailing_country
        
    , mailing.postal_code as mailing_postal_code
        
    , mailing.city as mailing_city
        
    , mailing.street as mailing_street
        
    , mailing.house_number as mailing_house_number
        
    
        
    , billing.country as billing_country
        
    , billing.postal_code as billing_postal_code
        
    , billing.city as billing_city
        
    , billing.street as billing_street
        
    , billing.house_number as billing_house_number
        
    
from [dw_oyster_path].[dev_silver].[bis_dim_customer_intervals] as timeline
inner join [dw_oyster_path].[dev_silver].[bis_dim_customer_staging] as customer
    on customer.customer_id = timeline.customer_id
   and timeline.valid_from >= customer.observed_valid_from
   and timeline.valid_from < customer.observed_valid_to


left join [dw_oyster_path].[dev_silver].[bis_dim_customer_address_staging] as permanent
    on permanent.customer_id = timeline.customer_id
   and permanent.address_type = 'Permanent'
   and permanent.is_deleted = 0
   and timeline.valid_from >= permanent.observed_valid_from
   and timeline.valid_from < permanent.observed_valid_to

left join [dw_oyster_path].[dev_silver].[bis_dim_customer_address_staging] as mailing
    on mailing.customer_id = timeline.customer_id
   and mailing.address_type = 'Mailing'
   and mailing.is_deleted = 0
   and timeline.valid_from >= mailing.observed_valid_from
   and timeline.valid_from < mailing.observed_valid_to

left join [dw_oyster_path].[dev_silver].[bis_dim_customer_address_staging] as billing
    on billing.customer_id = timeline.customer_id
   and billing.address_type = 'Billing'
   and billing.is_deleted = 0
   and timeline.valid_from >= billing.observed_valid_from
   and timeline.valid_from < billing.observed_valid_to
;

GO