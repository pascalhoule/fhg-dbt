{{  
    config(alias='__base_sf_prod_transactions', 
    database='normalize', 
    schema='investment')  
}} 

WITH T AS (
    SELECT
        TRANS.CODE,
        RP.REPID AS REP_ID,
        CONCAT(REP.LAST_NAME, ' ', REP.FIRST_NAME) AS REP,
        FA.ACCOUNTTYPE AS PLAN_TYPE,
        SPONSOR.NAME AS SPONSOR_NAME,
        FPROD.FUNDID,
        FPROD.NAME AS FUNDNAME,
        FPROD.LOADTYPE AS LOAD_TYPE,
        TRANS.TRADE_DATE::DATE AS TRANSACTION_DATE,
        TRANS.TRANSACTION_TYPE AS TRANSACTION_STATUS_CODE,
        STAT.MAPPEDTRANSACTIONSTATUS AS TRANSACTION_STATUS,
        T_TYPE.TRANSACTIONTYPENAME AS TRANSACTION_TYPE,
        TRANS.AMOUNT * T_TYPE.DBSIGN AS TRANSACTION_AMOUNT
    FROM
        {{ ref('transactions_normalize_investment') }} AS TRANS
        JOIN {{ ref('transactiontypes_fh_normalize_investment') }} AS T_TYPE ON TRANS.EXT_TYPE_CODE = T_TYPE.TRANSACTIONTYPECODE
        JOIN {{ ref('representatives_vc_normalize_investment') }} AS REP ON TRANS.REP_CODE = REP.REPRESENTATIVECODE
        JOIN {{ ref('representatives_normalize_investment') }} AS RP ON RP.code = REP.code
        JOIN {{ ref('fundaccount_vc_normalize_investment') }} AS FA ON TRANS.FUNDACCOUNT_CODE = FA.FUNDACCOUNTCODE
        JOIN {{ ref('fundproducts_vc_normalize_investment') }} AS FPROD ON FA.FUNDPRODUCT_CODE = FPROD.FUNDPRODUCTCODE
        JOIN {{ ref('sponsors_vc_normalize_investment') }} AS SPONSOR ON FPROD.SPONSORID = SPONSOR.SPONSORID
        JOIN {{ ref('transactionstatus_normalize_investment') }} AS STAT ON TRANS.TRANSACTION_FLAG = STAT.TRANSACTIONFLAG
    WHERE
        EXT_TYPE_CODE IN ('308', '314', '315', '316', '429', '378') 
),
T1 AS (
    SELECT
        T.CODE,
        T.REP_ID,
        T.REP,
        CONCAT(CLIENT.LAST_NAME, ' ', CLIENT.FIRST_NAME) AS CLIENT,
        FAC.ACCOUNT_NUMBER AS ACCOUNT_NUMBER,
        BCH.NAME AS BRANCH_NAME,
        REG.NAME AS SUB_REGION,
        PLAN_TYPE,
        SPONSOR_NAME,
        FUNDID,
        FUNDNAME,
        LOAD_TYPE,
        TRANSACTION_DATE,
        TRANSACTION_STATUS_CODE,
        TRANSACTION_STATUS,
        TRANSACTION_TYPE,
        T_VC.AMOUNT * T_TYPE.DBSIGN AS TRANSACTION_AMOUNT,
        0 AS NET_COMMISSION_AMOUNT
    FROM
        T
        INNER JOIN {{ ref('__base_trans_remove_splitcode_normalize_investment') }} AS T_VC ON T.CODE = T_VC.TRANSACTIONCODE
        INNER JOIN {{ ref('transactiontypes_fh_normalize_investment') }} AS T_TYPE ON T_VC.TRANSACTIONTYPECODE = T_TYPE.TRANSACTIONTYPECODE
        INNER JOIN {{ ref('representatives_vc_normalize_investment') }} AS REP ON T_VC.TRANSACTIONREPCODE = REP.REPRESENTATIVECODE
        INNER JOIN {{ ref('branches_vc_normalize_investment') }} AS BCH ON BCH.CODE = REP.BRANCH_CODE
        INNER JOIN {{ ref('region_vc_normalize_investment') }} AS REG ON BCH.SUBREGIONCODE = REG.SUBREGIONCODE
        INNER JOIN {{ ref('fundaccount_vc_normalize_investment') }} AS FA ON T_VC.FUNDACCOUNT_CODE = FA.FUNDACCOUNTCODE
        INNER JOIN {{ ref('fundaccount_normalize_investment') }} AS FAC ON FAC.CODE = FA.FUNDACCOUNTCODE
        INNER JOIN {{ ref('registration_normalize_investment') }} AS REGIS ON FAC.ACCOUNT_NUMBER = REGIS.REGISTRATION_NUMBER
        AND REGIS.CODE = FAC.REGISTRATION_CODE
        INNER JOIN {{ ref('clients_normalize_investment') }} AS CLIENT
        ON REGIS.KYC_CODE = CLIENT.CODE
    WHERE
        TRANSACTION_STATUS = 'Settled N/C'
        AND FAC.ACCOUNT_NUMBER IS NOT NULL
        AND TRANSACTION_TYPE IN (
            'Buy',
            'Commission Rebate',
            'Deposit',
            'Purchase',
            'Purchase - Reversal',
            'Purchase Management Fee Rebate',
            'Purchase Management Fee Rebate Adjustment',
            'Purchase Non Wire',
            'Purchase P.A.C.',
            'Purchase P.A.C. - Reversal',
            'Purchase Wire',
            'Purchase Wire - Reversal',
            'QESI',
            'RESP/RDSP'
        )
),
AMOUNT_VARIATION AS (
    SELECT
        CODE
    FROM
        T1
    WHERE
        REP LIKE '%/%'
    GROUP BY
        CODE,
        REP_ID,
        REP,
        CLIENT,
        ACCOUNT_NUMBER,
        BRANCH_NAME,
        SUB_REGION,
        PLAN_TYPE,
        SPONSOR_NAME,
        FUNDID,
        FUNDNAME,
        LOAD_TYPE,
        TRANSACTION_DATE,
        TRANSACTION_STATUS_CODE,
        TRANSACTION_STATUS,
        TRANSACTION_TYPE 
),
SUMMED_TRANSACTIONS AS (
    SELECT
        CODE,
        MAX(REP_ID) AS REP_ID,
        MAX(REP) AS REP,
        MAX(CLIENT) as CLIENT,
        MAX(ACCOUNT_NUMBER) AS ACCOUNT_NUMBER,
        MAX(BRANCH_NAME) AS BRANCH_NAME,
        MAX(SUB_REGION) AS SUB_REGION,
        MAX(PLAN_TYPE) AS PLAN_TYPE,
        MAX(SPONSOR_NAME) AS SPONSOR_NAME,
        MAX(FUNDID) AS FUNDID,
        MAX(FUNDNAME) AS FUNDNAME,
        MAX(LOAD_TYPE) AS LOAD_TYPE,
        MAX(TRANSACTION_DATE) AS TRANSACTION_DATE,
        MAX(TRANSACTION_STATUS_CODE) AS TRANSACTION_STATUS_CODE,
        MAX(TRANSACTION_STATUS) AS TRANSACTION_STATUS,
        MAX(TRANSACTION_TYPE) AS TRANSACTION_TYPE,
        SUM(TRANSACTION_AMOUNT) AS TRANSACTION_AMOUNT
    FROM
        T1
    WHERE
        CODE IN (
            SELECT
                CODE
            FROM
                AMOUNT_VARIATION
        )
    GROUP BY
        CODE
),
UNCHANGED_TRANSACTIONS AS (
    SELECT
        CODE,
        REP_ID,
        REP,
        CLIENT,
        ACCOUNT_NUMBER,
        BRANCH_NAME,
        SUB_REGION,
        PLAN_TYPE,
        SPONSOR_NAME,
        FUNDID,
        FUNDNAME,
        LOAD_TYPE,
        TRANSACTION_DATE,
        TRANSACTION_STATUS_CODE,
        TRANSACTION_STATUS,
        TRANSACTION_TYPE,
        TRANSACTION_AMOUNT
    FROM
        T1
    WHERE
        CODE NOT IN (
            SELECT
                CODE
            FROM
                AMOUNT_VARIATION
        )
),
FINAL AS (
    SELECT
        *
    FROM
        SUMMED_TRANSACTIONS
    UNION ALL
    SELECT
        *
    FROM
        UNCHANGED_TRANSACTIONS
),
F1 AS (
    SELECT
        FINAL.*
    FROM
        FINAL
    WHERE
        FINAL.REP_ID IN (
            SELECT
                JOINTID
            FROM
                {{ ref('joint_id_rate_fh_normalize_investment') }}
        )
),
F2 AS (
    SELECT
        CODE,
        REP_ID,
        CONCAT(JR.LAST_NAME, ' ', JR.FIRST_NAME) AS REP,
        CLIENT,
        ACCOUNT_NUMBER,
        BRANCH_NAME,
        SUB_REGION,
        PLAN_TYPE,
        SPONSOR_NAME,
        FUNDID,
        FUNDNAME,
        LOAD_TYPE,
        TRANSACTION_DATE,
        TRANSACTION_STATUS_CODE,
        TRANSACTION_STATUS,
        TRANSACTION_TYPE,
        TRANSACTION_AMOUNT * CAST(REPLACE(JR.SHARE, '.', '') AS FLOAT) / 100 AS TRANSACTION_AMOUNT
    FROM
        F1
        JOIN {{ ref('joint_id_rate_fh_normalize_investment') }} JR on F1.REP_ID = JR.JOINTID
),
F3 AS (
    SELECT
        *
    FROM
        FINAL
    WHERE
        FINAL.REP_ID NOT IN (
            SELECT
                JOINTID
            FROM
                {{ ref('joint_id_rate_fh_normalize_investment') }}
        )
),
FF AS (
    SELECT
        *
    FROM
        F2
    UNION ALL
    SELECT
        *
    FROM
        F3
)
SELECT
    *,
    0 AS NET_COMMISSION
FROM
    FF

