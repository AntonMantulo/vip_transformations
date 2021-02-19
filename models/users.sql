WITH bmb AS (
                SELECT userid, SUM(amounteur) AS amounteur  FROM {{ ref ('bmb') }} GROUP BY userid),

bmw AS (
                SELECT userid, SUM(amounteur) AS amounteur  FROM {{ ref ('bmw') }} GROUP BY userid), 

bonus_costs AS ( 
                SELECT userid, SUM(amounteur) AS amounteur  FROM {{ ref ('bonus_costs') }} GROUP BY userid),

transactions AS (
                SELECT userid, SUM(amounteur) AS amounteur  FROM {{ ref ('transactions') }} WHERE transactiontype = 'Deposit' GROUP BY userid),

firstdep AS ( WITH deps AS( 
                SELECT userid, transactioncompleted, ROW_NUMBER () OVER (PARTITION BY userid ORDER BY transactioncompleted ASC) AS rn FROM {{ ref ('transactions') }} WHERE transactiontype = 'Deposit')
            SELECT * FROM deps WHERE rn = 1),

lastdep AS ( WITH deps AS( 
                SELECT userid, transactioncompleted, ROW_NUMBER () OVER (PARTITION BY userid ORDER BY transactioncompleted DESC) AS rn FROM {{ ref ('transactions') }} WHERE transactiontype = 'Deposit')
            SELECT * FROM deps WHERE rn = 1),

user AS (
                SELECT userid, username, affiliatemarker, country, status, registrationdate
                FROM `stitch-test-296708.vip.User`),

rmb AS (
                SELECT userid, SUM(amounteur) AS amounteur FROM `stitch-test-296708.vip.BetActivity` WHERE wallettype = 'RealCash' GROUP BY userid),

rmw AS (
                SELECT userid, SUM(amounteur) AS amounteur FROM `stitch-test-296708.vip.WinActivity` WHERE wallettype = 'RealCash' GROUP BY userid)

SELECT user.userid,
    user.username,
    user.affiliatemarker,
    user.country,
    user.status,
    user.registrationdate,
    transactions.amounteur AS deposits,
    rmb.amounteur + bmb.amounteur AS turnover,
    IFNULL(rmb.amounteur, 0) + IFNULL(bmb.amounteur, 0) - IFNULL(rmw.amounteur, 0) - IFNULL(bmw.amounteur, 0) AS ggr,
    IFNULL(rmb.amounteur, 0) + IFNULL(bmb.amounteur, 0) - IFNULL(rmw.amounteur, 0) - IFNULL(bmw.amounteur, 0) - IFNULL(bonus_costs.amounteur, 0) AS ngr,
    firstdep.transactioncompleted AS first_deposit,
    lastdep.transactioncompleted AS last_deposit
FROM user
FULL JOIN transactions
ON user.userid = transactions.userid
FULL JOIN rmb
ON user.userid = rmb.userid
FULL JOIN bmb
ON user.userid = bmb.userid
FULL JOIN rmw
ON user.userid = rmw.userid
FULL JOIN bmw
ON user.userid = bmw.userid
FULL JOIN bonus_costs
ON user.userid = bonus_costs.userid 
FULL JOIN firstdep
ON user.userid = firstdep.userid 
FULL JOIN lastdep
ON user.userid = lastdep.userid 
WHERE user.userid IS NOT NULL