WITH joins AS (

WITH rmb AS (SELECT userid,CAST(endtime AS DATE) AS endtime, SUM(amounteur) as rmb_eur
              FROM vip.BetActivity
              WHERE EXTRACT( MONTH from endtime)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from endtime)= EXTRACT (YEAR from CURRENT_DATE())
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              

bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bmb') }}
             WHERE EXTRACT( MONTH from postingcompleted)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from postingcompleted)= EXTRACT (YEAR from CURRENT_DATE())
              GROUP BY userid,postingcompleted), 
              
rmw as(SELECT userid,SUM(amounteur) as rmw_eur,CAST(endtime AS DATE) AS endtime
              FROM vip.WinActivity
              WHERE EXTRACT( MONTH from endtime)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from endtime)= EXTRACT (YEAR from CURRENT_DATE())
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ ref ('bmw') }}
             WHERE EXTRACT( MONTH from postingcompleted)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from postingcompleted)= EXTRACT (YEAR from CURRENT_DATE())
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ref ('bonus_costs')}}
             WHERE EXTRACT( MONTH from postingcompleted)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from postingcompleted)= EXTRACT (YEAR from CURRENT_DATE())
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
              WHERE EXTRACT( MONTH from endtime)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from endtime)= EXTRACT (YEAR from CURRENT_DATE())
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              
bmb as(SELECT userid,SUM(amounteur) as bmb_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ ref ('bmb')}}
             WHERE EXTRACT( MONTH from postingcompleted)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from postingcompleted)= EXTRACT (YEAR from CURRENT_DATE())
              GROUP BY userid,postingcompleted), 

rmw as(SELECT userid,SUM(amounteur) as rmw_eur,CAST(endtime AS DATE) AS endtime
              FROM vip.WinActivity
              WHERE EXTRACT( MONTH from endtime)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from endtime)= EXTRACT (YEAR from CURRENT_DATE())
              AND wallettype = 'RealCash'
              GROUP BY userid,endtime),
              
bmw as(SELECT userid,SUM(amounteur) as bmw_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ ref ('bmw')}}
             WHERE EXTRACT( MONTH from postingcompleted)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from postingcompleted)= EXTRACT (YEAR from CURRENT_DATE())
              GROUP BY userid,postingcompleted),              
              
bc as (SELECT userid,SUM(amounteur) as bc_eur,CAST(postingcompleted AS DATE) AS postingcompleted
             FROM {{ ref ('bonus_costs')}}
             WHERE EXTRACT( MONTH from postingcompleted)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from postingcompleted)= EXTRACT (YEAR from CURRENT_DATE())
              GROUP BY userid,postingcompleted), 

trans AS (SELECT userid, SUM (amounteur) AS deposits, CAST (transactioncompleted AS DATE) AS transactioncompleted
            FROM {{ ref ('transactions')}} WHERE transactiontype = 'Deposit' 
                  AND EXTRACT( MONTH from transactioncompleted)= EXTRACT (MONTH from CURRENT_DATE())
                 AND EXTRACT( YEAR from transactioncompleted)= EXTRACT (YEAR from CURRENT_DATE())
                 GROUP BY userid, transactioncompleted)
                                               
                                          
SELECT joins.userid,
       joins.date,
       User.username,  
       User.affiliatemarker AS affiliatecode,
       User.registrationdate AS registrationdate, 
       IFNULL(trans.deposits, 0) AS deposit, 
       IFNULL(rmb_eur, 0) + IFNULL(bmb_eur, 0) AS bets,
       IFNULL(rmb_eur, 0) + IFNULL(bmb_eur, 0) - IFNULL(rmw_eur, 0) - IFNULL(bmw_eur, 0) AS ggr,
       IFNULL(rmb_eur, 0) + IFNULL(bmb_eur, 0) - IFNULL(rmw_eur, 0) - IFNULL(bmw_eur, 0) - IFNULL(bc_eur, 0) AS ngr       
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
ORDER BY joins.date ASC 