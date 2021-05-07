{% set partitions_to_replace = [
  'current_date',
  'date_sub(current_date, interval 1 day)'
] %}


{{ config(
    materialized='incremental',
    unique_key='postingid',
    cluster_by = 'userid',
    incremental_strategy = 'insert_overwrite', 
    partition_by={
      "field": "postingcompleted",
      "data_type": "timestamp"
    },
    partitions = partitions_to_replace
)}}

WITH master AS 
(WITH temp AS
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
        betactivity.gamename, 
        betactivity.userid,
        betactivity.wallettype,
        betactivity.postingid,
        temp.eurexchangerate,
        temp.payitemtypename,
        temp.bonuswalletid,
        temp.postingcompleted
FROM `stitch-test-296708.vip.BetActivity` AS betactivity
LEFT JOIN temp
ON betactivity.bonuswalletid = temp.bonuswalletid
WHERE wallettype = 'BonusMoney')
SELECT *
FROM master
WHERE postingcompleted IS NOT NULL

{% if is_incremental() %}
        -- recalculate yesterday + today
        and DATE(postingcompleted) in ({{ partitions_to_replace | join(',') }})
    {% endif %}