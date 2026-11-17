select transaction_id
from {{ ref('fct_transaction') }}
where date_key <>
    year(transaction_timestamp)*10000
    + month(transaction_timestamp)*100
    + day(transaction_timestamp)
   or time_key <>
    datepart(hour, transaction_timestamp)*3600
    + datepart(minute, transaction_timestamp)*60
    + datepart(second, transaction_timestamp)
