{{  
    config(alias='segfund_production_report_transaction', 
    database='normalize', 
    schema='investment')  
}} 

WITH T1 AS (
    SELECT
        CODE AS TRANSACTION_ID,
        CASE
            WHEN J.REPID IS NOT NULL THEN J.REPID
            ELSE S.REP_ID
        END AS BROKER_ID,
        REP AS BROKER_NAME,
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
        TRANSACTION_AMOUNT,
        NET_COMMISSION
    FROM
        {{ ref('__base_sf_prod_transactions_normalize_investment') }} S
        LEFT JOIN {{ ref('joint_id_rate_fh_normalize_investment') }} J ON S.REP_ID = J.JOINTID
        AND REP = CONCAT(J.LAST_NAME, ' ', J.FIRST_NAME)
),
T2 AS (
    SELECT
        TRANSACTION_ID,
        BROKER_ID,
        BROKER_NAME,
        ACCOUNT_NUMBER,
        BRANCH_NAME,
        REPLACE(SUB_REGION, 'LOC Brokers', '') AS BRANCH,
        0 AS TOTAL_FYC,
        0 AS BROKER_FYC,
        0 AS BRANCH_REVENUE,
        0 AS REGION_REVENUE,
        PLAN_TYPE,
        CARRI.CARRIER_DATABASE_NAME AS CARRIER,
        FUNDID,
        FUNDNAME,
        LOAD_TYPE AS BLUE_SUN_LOAD_TYPE,
        'No Load' AS MY_LOAD_TYPE,
        'Actual' AS LEDGER,
        'WS Investments' AS SOURCE,
        'FY Commission' AS TRANS_TYPE,
        NULL AS SUB_BRANCH,
        YEAR(TRANSACTION_DATE) AS YEAR,
        MONTH(TRANSACTION_DATE) AS MONTH,
        TRANSACTION_DATE,
        TRANSACTION_STATUS_CODE,
        TRANSACTION_STATUS,
        TRANSACTION_TYPE,
        TRANSACTION_AMOUNT AS Deposit,
        NET_COMMISSION
    FROM
        T1
        JOIN {{ source('norm', 'carrier_fin_fh') }} AS CARRI ON UPPER(CARRI.SPONSOR_NAME) = UPPER(T1.SPONSOR_NAME)
)
SELECT
    TRANSACTION_ID,
    BROKER_ID,
    BROKER_NAME,
    T2.BRANCH AS BRANCH,
    SUB_BRANCH,
    SF_REG.REGION AS REGION,
    TRANS_TYPE,
    CARRIER,
    BROKER_FYC,
    BRANCH_REVENUE,
    REGION_REVENUE,
    TOTAL_FYC,
    MONTH,
    YEAR,
    ACCOUNT_NUMBER,
    FUNDID,
    FUNDNAME,
    DEPOSIT,
    BLUE_SUN_LOAD_TYPE,
    MY_LOAD_TYPE,
    SOURCE,
    LEDGER,
    TRANSACTION_DATE,
    TRANSACTION_STATUS
FROM
    T2
    LEFT JOIN {{ source('norm', 'region_fin_fh') }} AS SF_REG ON TRIM(SF_REG.BRANCH) = TRIM(T2.BRANCH)
