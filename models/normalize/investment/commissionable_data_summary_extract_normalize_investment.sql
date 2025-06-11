{{			
    config (			
        materialized="view",			
        alias='commissionable_data_summary_extract', 			
        database='normalize', 			
        schema='investment'  		
    )			
}}

SELECT
    T.CODE AS TRANSACTION_CODE,
    REP.REPID AS TRANSACTION_REPID,
    SPONSOR.NAME AS SPONSOR,
    SPONSOR.ALIAS AS SPONSOR_ID,
    FPROD.SUBTYPENAME AS PRODUCT_TYPE,
    FUNDPRODUCT_FUNDID AS CUSIP,
    FUNDPRODUCT_NAME AS PRODUCT_NAME,
    FPROD.LOADTYPE AS LOAD_TYPE,
    REP.REPID AS COMMISSION_REPID,
    BCH.NAME AS BRANCH_NAME,
    REG.NAME AS SUB_REGION,
    DEALER_COMMISSION AS GROSS_COMMISSION,
    T.COMM_RUN_CODE AS COMMISSION_RUN_ID,
    C.DATE::DATE AS COMMISSION_CHEQUE_DATE,
    CONCAT(REP.FIRST_NAME, ' ', REP.LAST_NAME) AS TRANSACTION_REP_NAME,
    CONCAT(REP.FIRST_NAME, ' ', REP.LAST_NAME) AS COMMISSION_REP_NAME,
    T.AMOUNT * T_TYPE.DBSIGN AS INVESTMENT_AMOUNT
FROM {{ ref('transactions_clean_investment') }} AS T
INNER JOIN
    {{ ref('cheque_clean_investment') }} AS C
    ON T.CHEQUELINK = C.CODE
INNER JOIN
    NORMALIZE.PROD_INVESTMENT.TRANSACTIONTYPES_FH AS T_TYPE
    ON T.EXT_TYPE_CODE = T_TYPE.TRANSACTIONTYPECODE
INNER JOIN
    {{ ref('representatives_vc_clean_investment') }} AS REP
    ON T.REP_CODE = REP.REPRESENTIATIVECODE
INNER JOIN
    {{ ref('fundaccount_vc_clean_investment') }} AS FA
    ON T.FUNDACCOUNT_CODE = FA.FUNDACCOUNTCODE
INNER JOIN
    {{ ref('fundproducts_vc_clean_investment') }} AS FPROD
    ON FA.FUNDPRODUCT_CODE = FPROD.FUNDPRODUCTCODE
INNER JOIN
    {{ ref('sponsors_vc_clean_investment') }} AS SPONSOR
    ON FPROD.SPONSORID = SPONSOR.SPONSORID
INNER JOIN
    {{ ref('branches_vc_clean_investment') }} AS BCH
    ON REP.BRANCH_CODE = BCH.CODE
INNER JOIN
    {{ ref('region_vc_clean_investment') }} AS REG
    ON BCH.SUBREGIONCODE = REG.SUBREGIONCODE
