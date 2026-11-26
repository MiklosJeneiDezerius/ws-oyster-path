{{ config(enabled=var('include_address', false), materialized='table') }}

SELECT
    CAST([AddressID] AS BIGINT) AS [address_id],
    CAST([CustomerID] AS INT) AS [customer_id],
    CAST([AddressType] AS VARCHAR(100)) AS [address_type],
    CAST([Country] AS VARCHAR(100)) AS [country],
    CAST([PostalCode] AS VARCHAR(50)) AS [postal_code],
    CAST([City] AS VARCHAR(200)) AS [city],
    CAST([Street] AS VARCHAR(300)) AS [street],
    CAST([HouseNumber] AS VARCHAR(50)) AS [house_number],
    CAST('{{ invocation_id }}' AS VARCHAR(36)) AS [batch_id],
    CAST(GETDATE() AS DATETIME2(6)) AS [ingested_dttm]
FROM {{ source('landing_delta_core', 'customeraddress') }}