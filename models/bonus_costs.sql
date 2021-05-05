WITH master AS (WITH s AS (WITH asd AS(SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid,
      CASE 
        WHEN LENGTH(TRIM(RIGHT(note, 13))) = 12
        THEN CAST(LEFT(TRIM(RIGHT(note, 13)), 7) AS INT64)
        ELSE CAST(LEFT(TRIM(RIGHT(note, 13)), 8) AS INT64)
      END AS userid,
      amount * eurexchangerate * (-1) AS amounteur,
      amount * (-1) as amount,
      eurexchangerate,
      currency
FROM `stitch-test-296708.vip.Posting` AS posting
WHERE payitemtypename = 'BonusGranted'   
  AND note is not null
  AND postingtype = 'Bonus')
SELECT *
FROM asd 
WHERE bonuswalletid IN (SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid
                        FROM `stitch-test-296708.vip.Posting` 
                        WHERE (payitemtypename = 'BonusCashout' OR note LIKE 'ReturnAmountCausedByCompletion%')
                          AND note is not null
                          AND postingtype = 'Bonus')
   ),
                          
d AS (WITH s AS (SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid2,
                        note AS note2, 
                        postingcompleted AS postingcompleted2 
                 FROM `stitch-test-296708.vip.Posting` 
                 WHERE (payitemtypename = 'BonusCashout' OR note LIKE 'ReturnAmountCausedByCompletion%')  
                  AND note is not null
                  AND postingtype = 'Bonus') 
                  
                    SELECT *, ROW_NUMBER () OVER (PARTITION BY bonuswalletid2 ORDER BY postingcompleted2) AS rn 
                    FROM s),
                    
w AS (WITH asd AS(SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid,
      CASE 
      WHEN LENGTH(TRIM(RIGHT(note, 13))) = 12
      THEN CAST(LEFT(TRIM(RIGHT(note, 13)), 7) AS INT64)
      ELSE CAST(LEFT(TRIM(RIGHT(note, 13)), 8) AS INT64)
      END AS userid,
      amount * eurexchangerate * (-1) AS amounteur,
      amount * (-1) as amount,
      eurexchangerate,
      currency
FROM `stitch-test-296708.vip.Posting` AS posting
WHERE payitemtypename = 'BonusGranted'   
  AND note is not null
  AND postingtype = 'Bonus')

SELECT *
FROM asd 
WHERE bonuswalletid IN (SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid
                        FROM `stitch-test-296708.vip.Posting` 
                        WHERE (payitemtypename = 'BonusExpiredBonus' OR payitemtypename = 'BonusForfeitedBonus')  
                          AND note is not null AND postingtype = 'Bonus')
   ), 
      
x AS (SELECT CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid, 
        amount AS amount2,  
        amount * eurexchangerate AS amounteur2, 
        payitemtypename, 
        postingcompleted AS postingcompleted4
      FROM `stitch-test-296708.vip.Posting` 
      WHERE (payitemtypename = 'BonusExpiredBonus' OR payitemtypename = 'BonusForfeitedBonus')  
        AND note is not null 
        AND postingtype = 'Bonus')
      
SELECT userid,
  bonuswalletid,
  CASE 
    WHEN note2 like 'Return%'
    THEN 'used-up'
    ELSE 'released'
  END as bonus_status,
  amount,
  eurexchangerate,
  currency,
  amounteur,
  postingcompleted2 AS postingcompleted
FROM s
JOIN d
ON s.bonuswalletid = d.bonuswalletid2
WHERE rn = 1
UNION ALL
SELECT userid,
  w.bonuswalletid,
  CASE 
    WHEN payitemtypename = 'BonusForfeitedBonus'
    THEN 'forfeited'
    ELSE 'expired'
  END as bonus_status,
  amount - amount2 AS amount,
  eurexchangerate,
  currency,
  amounteur - amounteur2 AS amounteur,
  postingcompleted4 AS postingcompleted
FROM w
LEFT JOIN x
ON w.bonuswalletid = x.bonuswalletid), 
bw AS(SELECT userid, gamecode, bonuswalletid, ROW_NUMBER () OVER (PARTITION BY bonuswalletid) AS rn
      FROM vip.BetActivity)

SELECT m.userid, 
       m.bonuswalletid, 
       m.bonus_status, 
       m.amount, 
       m.eurexchangerate, 
       m.amounteur, 
       m.postingcompleted,
       CASE 
        WHEN b.gamecode = 'OddsMatrix2'
        THEN 'sport'
        ELSE 'casino'
       END AS type
FROM master AS m
JOIN bw AS b
ON m.bonuswalletid = b.bonuswalletid 
WHERE rn = 1 
UNION ALL 
SELECT userid, 
  CAST(TRIM(RIGHT(note, 13)) AS INT64) AS bonuswalletid, 
  'WR0' as bonus_status, 
  amount * (-1) as amount, 
  eurexchangerate,
  amount * eurexchangerate * (-1) AS amounteur,
  postingcompleted,
  'both' as type
FROM vip.Posting
WHERE  postingtype = 'Bonus' 
and note like 'ReleaseBonus%' 
and paymenttype IS NULL 
and payitemname = 'UBS'
