{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}


{{ config(
    materialized='incremental',
    unique_key='transid',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "transactioncompleted",
      "data_type": "timestamp"
    },
    partitions = partitions_to_replace
)}}



WITH master AS 
(SELECT 
    CAST(REGEXP_REPLACE(userid,'[^0-9 ]','') AS INT64) AS userid,
    CASE
        WHEN transactiontype = 'Vendor2User' OR transactiontype = 'Deposit'
        THEN 'Deposit'
        ELSE 'Withdraw'
    END AS transactiontype,
    transactioncompleted,
    (creditamount*eurcreditexchangerate) as amounteur,
    creditamount as amount,
    eurcreditexchangerate as eurexchangerate,
    creditcurrency as currency,
    transid,
    lastnote       
FROM vip.Transaction
WHERE (transactionstatus = 'Success' OR transactionstatus = 'RollBack')
    AND (  transactiontype='Deposit' OR transactiontype='Withdraw' OR transactiontype='Vendor2User' OR transactiontype='User2Vendor'  )
    AND creditamount <> 0)
SELECT *
FROM master 

{% if is_incremental() %}
        -- recalculate yesterday + today
        where DATE(transactioncompleted) in ({{ partitions_to_replace | join(',') }})
    {% endif %}
