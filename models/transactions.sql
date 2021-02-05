SELECT 
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
    AND creditamount <> 0
