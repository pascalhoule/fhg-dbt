{{  config(alias='__base_trans_remove_splitcode', database='normalize', schema='investment')  }} 
   

WITH JOINT_REP_CODES_REPLACED AS (
    SELECT
        TRANSACTIONCODE,
        FUNDACCOUNT_CODE,
        SOURCE_NUMBER,
        JREP.INDIVIDUALREPRESENTATIVECODE AS TRANSACTIONREPCODE,
        CLIENTREPCODE,
        SETTLEMENTDATE,
        TRADEDATE,
        SHARES_UNITS,
        UNIT_PRICE,
        AMOUNT * (JREP.SHARE / 100) AS AMOUNT,
        JREP.SHARE,
        NETAMOUNT,
        GROSSCOMMISSION,
        DEPOSIT_DATE,
        CURRENCYNAME,
        PAYMENTSTATUS,
        TRANSACTIONTYPECODE,
        FUNDPRODUCTCODE,
        MANUALENTRYFLAG,
        ENTEREDINSYSTEMFLAG,
        ENTEREDBYUSERID,
        APPROVALDATE,
        APPROVALBY,
        SECAPPROVALDATE,
        SECAPPROVALBY
    FROM
        {{ ref('transactions_vc_normalize_investment') }} T
        JOIN {{ ref('jointrepresentatives_vc_normalize_investment') }} JREP ON T.TRANSACTIONREPCODE = JREP.JOINTREPRESENTATIVECODE
    UNION
    SELECT
        TRANSACTIONCODE,
        FUNDACCOUNT_CODE,
        SOURCE_NUMBER,
        TRANSACTIONREPCODE,
        CLIENTREPCODE,
        SETTLEMENTDATE,
        TRADEDATE,
        SHARES_UNITS,
        UNIT_PRICE,
        AMOUNT,
        JREP.SHARE,
        NETAMOUNT,
        GROSSCOMMISSION,
        DEPOSIT_DATE,
        CURRENCYNAME,
        PAYMENTSTATUS,
        TRANSACTIONTYPECODE,
        FUNDPRODUCTCODE,
        MANUALENTRYFLAG,
        ENTEREDINSYSTEMFLAG,
        ENTEREDBYUSERID,
        APPROVALDATE,
        APPROVALBY,
        SECAPPROVALDATE,
        SECAPPROVALBY
    FROM
        {{ ref('transactions_vc_normalize_investment') }} t
        LEFT JOIN {{ ref('jointrepresentatives_vc_normalize_investment') }} JREP ON T.TRANSACTIONREPCODE = JREP.JOINTREPRESENTATIVECODE
    WHERE
        JREP.SHARE IS NULL
)
SELECT
   -- REP.INSAGENTCODE AS REPID,
    T.*
FROM
    JOINT_REP_CODES_REPLACED T
    JOIN {{ ref('representatives_vc_normalize_investment') }} REP ON T.TRANSACTIONREPCODE = REP.REPRESENTIATIVECODE
  