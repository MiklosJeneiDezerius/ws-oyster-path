create view [oystr_core_dq].[fail_transactions_without_accountid] as SELECT
    [transaction_id],
    [transaction_amt],
    [transaction_dttm]
FROM [dw_oyster_path].[oystr_core_bronze].[b_oys_transaction]
WHERE [account_id] IS NULL;

GO