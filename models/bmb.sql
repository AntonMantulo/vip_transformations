WITH temp AS
(WITH d AS 
(WITH s AS(SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid,
      payitemtypename, 
      postingcompleted,
      eurexchangerate
FROM `stitch-test-296708.vip.Posting` AS posting
WHERE ((payitemtypename = 'BonusCashout' 
      OR payitemtypename = 'BonusExpiredBonus'
      OR payitemtypename = 'BonusForfeitedBonus'
      OR note LIKE 'ReturnAmountCausedByCompletion%')
  AND note is not null)
  AND postingtype = 'Bonus')
SELECT *,
      ROW_NUMBER () OVER (PARTITION BY bonuswalletid) AS rn   
FROM s) 
SELECT * 
FROM d 
WHERE rn = 1)
SELECT  betactivity.betid,
        betactivity.amount, 
        betactivity.amounteur,
        betactivity.currency, 
        betactivity.endtime, 
        betactivity.gamecode, 
        betactivity.gamegroup, 
        betactivity.gamegroup, 
        betactivity.userid,
        betactivity.wallettype,
        temp.eurexchangerate,
        temp.payitemtypename,
        temp.bonuswalletid,
        temp.postingcompleted
FROM `stitch-test-296708.vip.BetActivity` AS betactivity
LEFT JOIN temp
ON betactivity.bonuswalletid = temp.bonuswalletid
WHERE wallettype = 'BonusMoney'