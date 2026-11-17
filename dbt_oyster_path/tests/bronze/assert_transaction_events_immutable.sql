select incoming.TransactionID
from {{ source('landing', 'transaction_ledger') }} as incoming
inner join {{ ref('bz_oys_transaction_ledger') }} as stored
    on incoming.TransactionID = stored.TransactionID
where exists (
    select incoming.AccountID, incoming.Amount, incoming.[Timestamp]
    except
    select stored.AccountID, stored.Amount, stored.[Timestamp]
)
