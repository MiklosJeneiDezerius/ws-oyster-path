-- Fails (returns rows) when the landing tables do not all come from the same batch.
-- A mixed state happens after a partial reload (p_entities) or a failed loader run.
-- Snapshotting a mixed state would record an inconsistent picture in Bronze.
with batches as (
    select distinct meta_batch_timestamp from {{ source('landing', 'customer') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'customer_address') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'account') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'debitcard') }}
    union
    select distinct meta_batch_timestamp from {{ source('landing', 'transaction_ledger') }}
)
select meta_batch_timestamp
from batches
where (select count(*) from batches) > 1
