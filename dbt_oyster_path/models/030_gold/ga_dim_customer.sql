SELECT
    [s_dim_customer_sk] AS [ga_dim_customer_sk],
    [customer_id],
    [first_nm],
    [last_nm],
    [full_nm]
FROM {{ ref('s_dim_customer') }}
