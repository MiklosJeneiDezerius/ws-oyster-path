{{ config(materialized='table') }}

SELECT
    CAST([TransactionID] AS INT) AS [transaction_id],
    CAST([AccountID] AS INT) AS [account_id],
    CAST([Amount] AS DECIMAL(18, 4)) AS [transaction_amt],
    CAST([Timestamp] AS DATETIME2(6)) AS [transaction_dttm],
    CAST('{{ invocation_id }}' AS VARCHAR(36)) AS [batch_id],
    CAST(GETDATE() AS DATETIME2(6)) AS [ingested_dttm]
FROM {{ source('landing_delta_core', 'transaction') }}