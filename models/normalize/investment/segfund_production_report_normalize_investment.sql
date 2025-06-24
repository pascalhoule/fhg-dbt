{{			
    config (			
        materialized="view",			
        alias='segfund_production_report', 			
        database='normalize', 			
        schema='investment'  		
    )			
}}


WITH T1 AS (
    SELECT
        TRANS.CODE,
        REP.LAST_NAME,
        REP.FIRST_NAME,
        FA.ACCOUNTTYPE AS PLAN_TYPE,
        SPONSOR.NAME AS SPONSOR_NAME,
        FPROD.FUNDID,
        FPROD.LOADTYPE AS LOAD_TYPE,
        TRANS.TRADE_DATE::DATE AS TRANSACTION_DATE,
        TRANS.TRANSACTION_TYPE AS TRANSACTION_STATUS_CODE,
        STAT.MAPPEDTRANSACTIONSTATUS AS TRANSACTION_STATUS,
        T_TYPE.TRANSACTIONTYPENAME AS TRANSACTION_TYPE,
        TRANS.AMOUNT * T_TYPE.DBSIGN AS TRANSACTION_AMOUNT
    FROM {{ ref('transactions_clean_investment') }} AS TRANS
    INNER JOIN
        {{ ref('transactiontypes_fh_normalize_investment') }} AS T_TYPE
        ON TRANS.EXT_TYPE_CODE = T_TYPE.TRANSACTIONTYPECODE
    INNER JOIN
        {{ ref('representatives_vc_clean_investment') }} AS REP
        ON TRANS.REP_CODE = REP.REPRESENTIATIVECODE
    INNER JOIN
        {{ ref('fundaccount_vc_clean_investment') }} AS FA
        ON TRANS.FUNDACCOUNT_CODE = FA.FUNDACCOUNTCODE
    INNER JOIN
        {{ ref('fundproducts_vc_clean_investment') }} AS FPROD
        ON FA.FUNDPRODUCT_CODE = FPROD.FUNDPRODUCTCODE
    INNER JOIN
        {{ ref('sponsors_vc_clean_investment') }} AS SPONSOR
        ON FPROD.SPONSORID = SPONSOR.SPONSORID
    INNER JOIN
        {{ ref('transactionstatus_clean_investment') }} AS STAT
        ON TRANS.TRANSACTION_FLAG = STAT.TRANSACTIONFLAG
    WHERE EXT_TYPE_CODE IN ('308', '314', '315', '316', '429', '378')
)

SELECT
    T1.CODE,
    T1.LAST_NAME,
    T1.FIRST_NAME,
    BCH.NAME AS BRANCH_NAME,
    REG.NAME AS SUB_REGION,
    PLAN_TYPE,
    SPONSOR_NAME,
    FUNDID,
    LOAD_TYPE,
    TRANSACTION_DATE,
    TRANSACTION_STATUS_CODE,
    TRANSACTION_STATUS,
    TRANSACTION_TYPE,
    T_VC.AMOUNT * T_TYPE.DBSIGN AS TRANSACTION_AMOUNT
FROM T1
INNER JOIN
    {{ ref('__base_trans_remove_splitcode_normalize_investment') }} AS T_VC
    ON T1.CODE = T_VC.TRANSACTIONCODE
INNER JOIN
    {{ ref('transactiontypes_fh_normalize_investment') }} AS T_TYPE
    ON T_VC.TRANSACTIONTYPECODE = T_TYPE.TRANSACTIONTYPECODE
INNER JOIN
    {{ ref('representatives_vc_clean_investment') }} AS REP
    ON T_VC.TRANSACTIONREPCODE = REP.REPRESENTIATIVECODE
INNER JOIN
    {{ ref('branches_vc_clean_investment') }} AS BCH
    ON BCH.CODE = REP.BRANCH_CODE
INNER JOIN
    {{ ref('region_vc_clean_investment') }} AS REG
    ON BCH.SUBREGIONCODE = REG.SUBREGIONCODE
