WITH master2 AS (WITH master AS(WITH joins AS (

WITH rmb AS (SELECT userid,CAST(endtime AS DATE) AS endtime, SUM(amounteur) as rmb_eur
              FROM vip.BetActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              

bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmb') }}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              GROUP BY userid,postingcompleted), 
              
rmw as(SELECT userid,SUM(amounteur) as rmw_eur,CAST(endtime AS DATE) AS endtime
              FROM vip.WinActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmw') }}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bonus_costs')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              GROUP BY userid,postingcompleted)
                                          
SELECT rmb.userid, rmb.endtime AS date
FROM rmb
UNION DISTINCT 
SELECT bmb.userid, bmb.postingcompleted AS date
FROM bmb
UNION DISTINCT 
SELECT rmw.userid, rmw.endtime AS date
FROM rmw
UNION DISTINCT 
SELECT bmw.userid, bmw.postingcompleted AS date
FROM bmw
UNION DISTINCT
SELECT bc.userid, bc.postingcompleted AS date
FROM bc),

rmb AS (SELECT userid,CAST(endtime AS DATE) AS endtime, SUM(amounteur) as rmb_eur
              FROM vip.BetActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              
bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmb')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              GROUP BY userid,postingcompleted), 

rmw as(SELECT userid,SUM(amounteur) as rmw_eur,CAST(endtime AS DATE) AS endtime
              FROM vip.WinActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmw')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bonus_costs')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              GROUP BY userid,postingcompleted), 

trans AS (SELECT userid, SUM (amounteur) AS deposits, CAST (transactioncompleted AS DATE) AS transactioncompleted
            FROM {{ref ('transactions')}} WHERE transactiontype = 'Deposit' 
                  AND DATE(transactioncompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
                 GROUP BY userid, transactioncompleted)
                                               
                                          
SELECT joins.userid,
       joins.date,
       User.country, 
       User.username,  
       User.affiliatemarker AS affiliatecode,
       User.registrationdate AS registrationdate, 
       IFNULL(trans.deposits, 0) AS deposit, 
       IFNULL(rmb_eur, 0) + IFNULL(bmb_eur, 0) AS bets,
       IFNULL(rmb_eur, 0) + IFNULL(bmb_eur, 0) - IFNULL(rmw_eur, 0) - IFNULL(bmw_eur, 0) AS ggr,      
FROM joins 
FULL JOIN rmb
ON joins.userid = rmb.userid AND joins.date = rmb.endtime 
FULL JOIN bmb 
ON bmb.userid = joins.userid AND joins.date = bmb.postingcompleted  
FULL JOIN rmw 
ON rmw.userid = joins.userid AND rmw.endtime = joins.date
FULL JOIN bmw 
ON bmw.userid = joins.userid AND bmw.postingcompleted = joins.date
FULL JOIN bc 
ON bc.userid = joins.userid AND bc.postingcompleted = joins.date
LEFT JOIN vip.User 
ON joins.userid=User.userid
LEFT JOIN trans
ON joins.userid = trans.userid AND joins.date = trans.transactioncompleted
ORDER BY joins.date ASC) 
SELECT userid, 
   date,
   username,
   affiliatecode,
   primary_aff,
   secondary_aff, 
   registrationdate,
   country,
   deposit,
   bets,
   ggr,
   primary_commission,
   secondary_commission
FROM master
LEFT JOIN `dbt_vip.Affiliates cardinality`  AS t
ON LEFT(master.affiliatecode, 7) = t.secondary_aff),

casino AS(WITH joins AS (

WITH rmb AS (SELECT userid,CAST(endtime AS DATE) AS endtime, SUM(amounteur) as rmb_eur
              FROM vip.BetActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash' AND gamecode<>'OddsMatrix2'
              GROUP BY userid,endtime),
              

bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmb')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode<>'OddsMatrix2'
              GROUP BY userid,postingcompleted), 
              
rmw as(SELECT userid,SUM(amounteur) as rmw_eur,CAST(endtime AS DATE) AS endtime
              FROM vip.WinActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash' AND gamecode<>'OddsMatrix2'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmw')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode<>'OddsMatrix2'
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bonus_costs')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              GROUP BY userid,postingcompleted)
                                          
SELECT rmb.userid, rmb.endtime AS date
FROM rmb
UNION DISTINCT 
SELECT bmb.userid, bmb.postingcompleted AS date
FROM bmb
UNION DISTINCT 
SELECT rmw.userid, rmw.endtime AS date
FROM rmw
UNION DISTINCT 
SELECT bmw.userid, bmw.postingcompleted AS date
FROM bmw
UNION DISTINCT
SELECT bc.userid, bc.postingcompleted AS date
FROM bc),

rmb AS (SELECT userid,CAST(endtime AS DATE) AS endtime, SUM(amounteur) as rmb_eur
              FROM vip.BetActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash' AND gamecode<>'OddsMatrix2'
              GROUP BY userid,endtime),
              
bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmb')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode<>'OddsMatrix2'
              GROUP BY userid,postingcompleted), 

rmw as(SELECT userid,SUM(amounteur) as rmw_eur,CAST(endtime AS DATE) AS endtime
              FROM vip.WinActivity
              WHERE DATE(endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND wallettype = 'RealCash' AND gamecode<>'OddsMatrix2'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmw')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode<>'OddsMatrix2'
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bonus_costs')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND type='casino'
              GROUP BY userid,postingcompleted) 
SELECT joins.userid,
       joins.date,
       IFNULL(rmb_eur, 0) + IFNULL(bmb_eur, 0) - IFNULL(rmw_eur, 0) - IFNULL(bmw_eur, 0) - IFNULL(bc_eur, 0) AS ngr_casino      
FROM joins 
FULL JOIN rmb
ON joins.userid = rmb.userid AND joins.date = rmb.endtime 
FULL JOIN bmb 
ON bmb.userid = joins.userid AND joins.date = bmb.postingcompleted  
FULL JOIN rmw 
ON rmw.userid = joins.userid AND rmw.endtime = joins.date
FULL JOIN bmw 
ON bmw.userid = joins.userid AND bmw.postingcompleted = joins.date
FULL JOIN bc 
ON bc.userid = joins.userid AND bc.postingcompleted = joins.date
ORDER BY joins.date ASC),

sport AS(WITH joins AS (

WITH rmb AS (SELECT B.userid,CAST(W.endtime AS DATE) as endtime,SUM(B.amounteur) as rmb_eur
              FROM vip.BetActivity as B JOIN vip.WinActivity as W ON B.postingid=W.matchingpostingid
              WHERE DATE(W.endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE() and W.wallettype="RealCash" and B.gamecode='OddsMatrix2'
              GROUP BY userid,endtime),
              

bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmb')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode='OddsMatrix2'
              GROUP BY userid,postingcompleted), 
              
rmw as(SELECT W.userid,CAST(W.endtime AS DATE) as endtime,SUM(W.amounteur) as rmw_eur
              FROM vip.BetActivity as B JOIN vip.WinActivity as W ON B.postingid=W.matchingpostingid
              WHERE DATE(W.endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND W.wallettype = 'RealCash' AND W.gamecode='OddsMatrix2'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmw')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode='OddsMatrix2'
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bonus_costs')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE() AND type='sport'
              GROUP BY userid,postingcompleted)
                                          
SELECT rmb.userid, rmb.endtime AS date
FROM rmb
UNION DISTINCT 
SELECT bmb.userid, bmb.postingcompleted AS date
FROM bmb
UNION DISTINCT 
SELECT rmw.userid, rmw.endtime AS date
FROM rmw
UNION DISTINCT 
SELECT bmw.userid, bmw.postingcompleted AS date
FROM bmw
UNION DISTINCT
SELECT bc.userid, bc.postingcompleted AS date
FROM bc),

rmb AS (SELECT B.userid,CAST(W.endtime AS DATE) as endtime,SUM(B.amounteur) as rmb_eur
              FROM vip.BetActivity as B JOIN vip.WinActivity as W ON B.postingid=W.matchingpostingid
              WHERE DATE(W.endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE() and W.wallettype="RealCash" and B.gamecode='OddsMatrix2'
              GROUP BY userid,endtime),
              
bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmb')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode='OddsMatrix2'
              GROUP BY userid,postingcompleted), 

rmw as(SELECT W.userid,CAST(W.endtime AS DATE) as endtime,SUM(W.amounteur) as rmw_eur
              FROM vip.BetActivity as B JOIN vip.WinActivity as W ON B.postingid=W.matchingpostingid
              WHERE DATE(W.endtime) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
              AND W.wallettype = 'RealCash' AND W.gamecode='OddsMatrix2'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmw')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE()
             AND gamecode='OddsMatrix2'
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bonus_costs')}}
             WHERE DATE(postingcompleted) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH) AND CURRENT_DATE() AND type='sport'
              GROUP BY userid,postingcompleted) 
SELECT joins.userid,
       joins.date,
       IFNULL(rmb_eur, 0) + IFNULL(bmb_eur, 0) - IFNULL(rmw_eur, 0) - IFNULL(bmw_eur, 0) - IFNULL(bc_eur, 0) AS ngr_sport      
FROM joins 
FULL JOIN rmb
ON joins.userid = rmb.userid AND joins.date = rmb.endtime 
FULL JOIN bmb 
ON bmb.userid = joins.userid AND joins.date = bmb.postingcompleted  
FULL JOIN rmw 
ON rmw.userid = joins.userid AND rmw.endtime = joins.date
FULL JOIN bmw 
ON bmw.userid = joins.userid AND bmw.postingcompleted = joins.date
FULL JOIN bc 
ON bc.userid = joins.userid AND bc.postingcompleted = joins.date
ORDER BY joins.date ASC) 


SELECT master2.userid AS userid, 
   master2.date AS date,
   username,
   affiliatecode,
   country,
   CASE 
      WHEN primary_aff IS NULL 
      THEN ""
      ELSE primary_aff
   END AS primary_aff,
   secondary_aff, 
   registrationdate,
   deposit,
   bets,
   ggr,
   IFNULL(ngr_sport,0)+IFNULL(ngr_casino,0) AS ngr,
   ngr_casino,
   ngr_sport,
   primary_commission,
   secondary_commission
FROM master2
FULL JOIN casino ON master2.userid=casino.userid AND master2.date=casino.date
FULL JOIN sport ON master2.userid=sport.userid AND master2.date=sport.date