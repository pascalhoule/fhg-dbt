{{  config(alias='detailed_FYC_FYB', database='finance', schema='queries', materialization = "view")  }} 

WITH MOST_DATA AS (
        SELECT
            A.OWNERCODE AS OWNERCODE,
            A.ACCRUALCODE AS ACCRUALCODE_FROM_ACCURALS,
            A.CHQCODE,
            C.COMMISSIONRUNCODE,
            RUN.TITLE AS COMMISSION_RUN,
            'Unknown' AS OCCURRENCE_TYPE,
            CASE
                WHEN A.OWNERTYPE = 1 THEN 'MGA'
                WHEN A.OWNERTYPE = 2 THEN 'AGA'
                WHEN A.OWNERTYPE = 3 THEN 'Broker'
                ELSE 'Unclassified'
            END AS OWNER_TYPE,
            A.AGENTFULLNAME AS OWNER_NAME,
            B.COMPANYNAME AS OWNER_COMPANY,
            TRIM(A.POLICYNUMBEREX) AS POLICY_NUMBER,
            A.POLICYCODE AS POLICYCODE,
            CARRIER.ICNAME AS CARRIER,
            A.PLANNAMEEX AS PLAN_NAME,
            C.STATUS,
            CASE
                WHEN A.TRXTYPE = 'FYC' THEN A.PAID
                ELSE 0
            END AS TOTAL_FYC,
            CASE
                WHEN A.TRXTYPE = 'FYB' THEN A.PAID
                ELSE 0
            END AS TOTAL_FYB,
            POSTDATE::DATE AS POSTDATE,
            A.COMMPREM AS TRANSACTION_COMMISSIONABLE_PREMIUM,
            A.COMMRATE AS COMMISSION_RATE,
            A.SPLITSHARE AS COMMISSION_SPLIT_SHARE
        FROM
            {{ ref('accruals_finance_insurance') }} A
            JOIN {{ ref('cheque_finance_insurance') }} C ON C.CHQCODE = A.CHQCODE
            JOIN {{ ref('commissionrun_finance_insurance') }} RUN ON C.COMMISSIONRUNCODE = RUN.COMMISSIONRUNCODE
            LEFT JOIN {{ ref('broker_vc_finance_insurance') }} B ON B.AGENTCODE = A.OWNERCODE
            LEFT JOIN {{ ref('ic_vc_finance_insurance') }} CARRIER ON CARRIER.ICID = A.ICID
        WHERE
            POSTDATE::DATE BETWEEN CURRENT_DATE() - 1830
            AND CURRENT_DATE() 
    ),
    WRIT_AGT AS (
        SELECT
            P.POLICYCODE,
            P.AGENTCODE,
            P.TYPE,
            B.BROKERID,
            B.AGENTNAME
        FROM
            {{ ref('policyagentlinking_finance_insurance') }} P
            JOIN {{ ref('broker_vc_finance_insurance') }} B ON B.AGENTCODE = P.AGENTCODE
        WHERE
            POLICYCODE IN (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    MOST_DATA
            )
            AND TYPE = 1
        GROUP BY
            1,2,3,4,5
    ),
    P_NAME AS (
        SELECT
            ACCRUALCODE,
            POLICYCODE,
            CASE
                WHEN CONTAINS(PLANNAMEEX, '(UpToMin)') THEN REPLACE(PLANNAMEEX, '(UpToMin)', '')
                WHEN CONTAINS(PLANNAMEEX, '(MinToMax)') THEN REPLACE(PLANNAMEEX, '(MinToMax)', '')
                WHEN CONTAINS(PLANNAMEEX, '(AboveMax)') THEN REPLACE(PLANNAMEEX, '(AboveMax)', '')
                ELSE PLANNAMEEX
            END AS PLANNAME
        FROM
            finance.prod_insurance.accruals
        WHERE
            POLICYCODE in (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    MOST_DATA
            )
        GROUP BY
            1,2,3
    ),
    POLICYCODE_TYPE AS (
        SELECT
            POLICYCODE,
            PLANTYPE
        FROM
            {{ ref('commission_vc_finance_insurance') }}
        WHERE
            POLICYCODE in (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    MOST_DATA
            )
        GROUP BY
            1,2
    ),
    POL_TYPE AS (
        SELECT
            TRIM(POLICYNUMBER) AS POLICY_NUMBER,
            MIN(PLANTYPE) AS PLANTYPE
        FROM
            {{ ref('policy_vc_finance_insurance') }}
        WHERE
            POLICYCODE in (
                SELECT
                    DISTINCT POLICYCODE
                FROM
                    MOST_DATA
            )
        GROUP BY
            1
    ),
    TRX_DATA AS (
        SELECT
            COMM_TRX.ACCRUALCODE,
            COMM_TRX.COMMISSIONRUNCODE,
            COMM_TRX.OWNERTYPE,
            COMM_TRX.POLICYCODE,
            COMM_TRX.CHQCODE as COMM_TRX_CHQCODE,
            MOST_DATA.CHQCODE,
            MOST_DATA.POLICY_NUMBER,
            MOST_DATA.POLICYCODE,
            CASE
                WHEN COMM_TRX.OWNERTYPE = 1 THEN 'MGA'
                WHEN COMM_TRX.OWNERTYPE = 2 THEN 'AGA'
                WHEN COMM_TRX.OWNERTYPE = 3 THEN 'Broker'
                ELSE 'Unclassified'
            END AS OWNER_TYPE,
            COMM_TRX.OWNERCODE AS OWNERCODE,
            CASE
                WHEN COMM_TRX.OWNERTYPE = 1 THEN MGA.MGAID
                WHEN COMM_TRX.OWNERTYPE = 2 THEN AGA.AGANAME
                WHEN COMM_TRX.OWNERTYPE = 3 THEN B.FULLAGENTNAME
                ELSE MOST_DATA.OWNER_NAME
            END AS OWNER_NAME,
            CASE
                WHEN COMM_TRX.OWNERTYPE = 1 THEN MGA.MGAID
                WHEN COMM_TRX.OWNERTYPE = 2 THEN AGA.COMPANY
                WHEN COMM_TRX.OWNERTYPE = 3 THEN B.COMPANYNAME
                ELSE MOST_DATA.OWNER_COMPANY
            END AS OWNER_COMPANY,
            CASE
                WHEN TRXTYPE = 'FYC' THEN TRXAMOUNT
                ELSE 0
            END AS TOTAL_FYC,
            CASE
                WHEN TRXTYPE = 'FYB' THEN TRXAMOUNT
                ELSE 0
            END AS TOTAL_FYB
        FROM
            MOST_DATA
            LEFT JOIN {{ ref('commissiontrx_finance_insurance') }} COMM_TRX ON MOST_DATA.ACCRUALCODE_FROM_ACCURALS = COMM_TRX.ACCRUALCODE
            LEFT JOIN {{ ref('aga_finance_insurance') }}  AGA ON AGA.AGAcode = COMM_TRX.OWNERCODE
            LEFT JOIN {{ ref('mga_finance_insurance') }}  MGA ON MGA.MGAcode = COMM_TRX.OWNERCODE
            LEFT JOIN {{ ref('broker_vc_finance_insurance') }} B ON B.AGENTCODE = COMM_TRX.OWNERCODE 
    ),
    MAP_TRANSACTIONS as (
        SELECT
            MOST_DATA.ACCRUALCODE_FROM_ACCURALS,
            MOST_DATA.CHQCODE,
            MOST_DATA.OWNERCODE,
            MOST_DATA.COMMISSIONRUNCODE,
            MOST_DATA.COMMISSION_RUN,
            CASE
                WHEN TRX_DATA.ACCRUALCODE is NOT NULL THEN 'Transaction'
                ELSE 'Accrual'
            END AS OCCURRENCE_TYPE,
            WRIT_AGT.AGENTNAME AS WRITING_AGENT,
            WRIT_AGT.BROKERID AS REP_ID,
            CASE
                WHEN TRX_DATA.ACCRUALCODE IS NOT NULL THEN TRX_DATA.OWNER_TYPE
                ELSE MOST_DATA.OWNER_TYPE
            END AS OWNER_TYPE,
            MOST_DATA.OWNER_NAME,
            TRIM(MOST_DATA.POLICY_NUMBER)::TEXT AS POLICY_NUMBER,
            MOST_DATA.POLICYCODE,
            MOST_DATA.CARRIER,
            P_NAME.PLANNAME AS PLAN_NAME,
            CASE
                WHEN POLICYCODE_TYPE.PLANTYPE IS NOT NULL THEN POLICYCODE_TYPE.PLANTYPE
                WHEN MOST_DATA.PLAN_NAME = 'Group Benefits' THEN 'Group Benefits'
                ELSE POL_TYPE.PLANTYPE
            END AS PLANCATEGORY,
            MOST_DATA.STATUS,
            CASE
                WHEN TRX_DATA.TOTAL_FYC IS NOT NULL THEN ROUND(TRX_DATA.TOTAL_FYC, 2)
                ELSE ROUND(MOST_DATA.TOTAL_FYC, 2)
            END AS TOTAL_FYC,
            CASE
                WHEN TRX_DATA.TOTAL_FYB IS NOT NULL THEN ROUND(TRX_DATA.TOTAL_FYB, 2)
                ELSE ROUND(MOST_DATA.TOTAL_FYB, 2)
            END AS TOTAL_FYB,
            MOST_DATA.POSTDATE,
            MOST_DATA.TRANSACTION_COMMISSIONABLE_PREMIUM,
            MOST_DATA.COMMISSION_RATE,
            MOST_DATA.COMMISSION_SPLIT_SHARE,
        FROM
            MOST_DATA
            LEFT JOIN TRX_DATA ON MOST_DATA.ACCRUALCODE_FROM_ACCURALS = TRX_DATA.ACCRUALCODE
            AND MOST_DATA.COMMISSIONRUNCODE = TRX_DATA.COMMISSIONRUNCODE
            LEFT JOIN p_NAME ON P_NAME.POLICYCODE = MOST_DATA.POLICYCODE
            AND P_NAME.ACCRUALCODE = MOST_DATA.ACCRUALCODE_FROM_ACCURALS
            LEFT JOIN POLICYCODE_TYPE ON POLICYCODE_TYPE.POLICYCODE = MOST_DATA.POLICYCODE
            LEFT JOIN POL_TYPE ON POL_TYPE.POLICY_NUMBER = MOST_DATA.POLICY_NUMBER
            LEFT JOIN WRIT_AGT ON WRIT_AGT.POLICYCODE = MOST_DATA.POLICYCODE
    )
SELECT
    MAP_TRANSACTIONS.ACCRUALCODE_FROM_ACCURALS AS ACCURALCODE,
    COMMISSION_RUN,
    OCCURRENCE_TYPE,
    WRITING_AGENT,
    REP_ID,
    OWNER_TYPE,
    CASE
        WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'AGA' THEN AGA.AGANAME
        WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'MGA' THEN MGA.MGAID
        WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'Broker' THEN B.AGENTNAME
    END AS OWNER_NAME,
    CASE
        WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'AGA' THEN AGA.company
        WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'MGA' THEN MGA.MGAid
    END AS OWNER_COMPANY,
    MAP_TRANSACTIONS.POLICY_NUMBER,
    MAP_TRANSACTIONS.POLICYCODE,
    CARRIER,
    PLAN_NAME,
    PLANCATEGORY,
    MAP_TRANSACTIONS.STATUS,
    MAP_TRANSACTIONS.TOTAL_FYC,
    MAP_TRANSACTIONS.TOTAL_FYB,
    POSTDATE,
    TRANSACTION_COMMISSIONABLE_PREMIUM,
    COMMISSION_RATE,
    COMMISSION_SPLIT_SHARE
FROM
    MAP_TRANSACTIONS
    LEFT JOIN {{ ref('aga_finance_insurance') }} AGA ON AGA.AGACODE = MAP_TRANSACTIONS.OWNERCODE
    LEFT JOIN {{ ref('mga_finance_insurance') }} MGA ON MGA.MGACODE = MAP_TRANSACTIONS.OWNERCODE
    LEFT JOIN {{ ref('broker_vc_finance_insurance') }} B on B.AGENTCODE = MAP_TRANSACTIONS.OWNERCODE
WHERE
    OCCURRENCE_TYPE = 'Accrual' 
UNION
SELECT
    MAP_TRANSACTIONS.ACCRUALCODE_FROM_ACCURALS AS ACCURALCODE,
    COMMISSION_RUN,
    OCCURRENCE_TYPE,
    WRITING_AGENT,
    REP_ID,
    TRX_DATA.OWNER_TYPE,
    TRX_DATA.OWNER_NAME,
    TRX_DATA.OWNER_COMPANY,
    MAP_TRANSACTIONS.POLICY_NUMBER,
    MAP_TRANSACTIONS.POLICYCODE,
    CARRIER,
    PLAN_NAME,
    PLANCATEGORY,
    MAP_TRANSACTIONS.STATUS,
    MAP_TRANSACTIONS.TOTAL_FYC,
    MAP_TRANSACTIONS.TOTAL_FYB,
    POSTDATE,
    TRANSACTION_COMMISSIONABLE_PREMIUM,
    COMMISSION_RATE,
    COMMISSION_SPLIT_SHARE
FROM
    MAP_TRANSACTIONS
    LEFT JOIN TRX_DATA ON MAP_TRANSACTIONS.ACCRUALCODE_FROM_ACCURALS = TRX_DATA.ACCRUALCODE
    AND MAP_TRANSACTIONS.COMMISSIONRUNCODE = TRX_DATA.COMMISSIONRUNCODE
    AND MAP_TRANSACTIONS.TOTAL_FYB = TRX_DATA.TOTAL_FYB
    AND MAP_TRANSACTIONS.TOTAL_FYC = TRX_DATA.TOTAL_FYC
WHERE
    OCCURRENCE_TYPE = 'Transaction'