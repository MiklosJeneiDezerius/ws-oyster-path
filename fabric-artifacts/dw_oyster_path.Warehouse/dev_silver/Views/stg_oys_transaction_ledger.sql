create view [dev_silver].[stg_oys_transaction_ledger] as select
    TransactionID as transaction_id_raw,
    nullif(trim(TransactionID), '') as transaction_id,
    AccountID as account_id_raw,
    nullif(trim(AccountID), '') as account_id,
    Amount as amount_raw,
    try_convert(decimal(18,2), nullif(trim(Amount), '')) as amount,
    [Timestamp] as transaction_timestamp_raw,
    try_convert(
        datetime2(6), nullif(trim([Timestamp]), ''), 120
    ) as transaction_timestamp,
    meta_batch_timestamp,
    meta_load_timestamp,
    meta_source_file
from [dw_oyster_path].[dev_bronze].[bz_oys_transaction_ledger];

GO