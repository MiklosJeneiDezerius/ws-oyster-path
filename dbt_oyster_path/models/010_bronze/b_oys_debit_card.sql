{{ config(materialized='table') }}

SELECT
    CAST([CardID] AS INT) AS [card_id],
    CAST([AccountID] AS INT) AS [account_id],
    TRIM([CardTier]) AS [card_tier_nm],
    CAST('{{ invocation_id }}' AS VARCHAR(36)) AS [batch_id],
    CAST(GETDATE() AS DATETIME2(6)) AS [ingested_dttm]
FROM {{ source('landing_delta_core', 'debitcard') }}
