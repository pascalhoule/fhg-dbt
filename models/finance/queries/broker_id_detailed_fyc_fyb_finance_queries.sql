{{ config(
    alias = 'brokerid_detailed_fyc_fyb', 
    database = 'finance', 
    schema = 'queries', 
    grants = {'ownership': ['FINANCE']},
    materialized = "view") }} 

WITH ACCRUAL_COMM_AGT_SPLIT AS (
    SELECT DISTINCT
            A.POLICYCODE,
            B.AGENTNAME AS AGENTFULLNAME,
            B.BROKERID,
            A.OWNERCODE,
            B.AGENTCODE,
            BA.USERDEFINED2,
            A.SPLITCODE
    FROM
        {{ ref('accruals_finance_insurance') }} AS A
        INNER JOIN
        {{ ref('brokeradvanced_vc_finance_insurance') }} AS BA
        ON A.OWNERCODE = BA.AGENTCODE
        INNER JOIN
        {{ ref('broker_vc_finance_insurance') }} AS B
        ON A.OWNERCODE = B.AGENTCODE
    WHERE
        OWNERTYPE = 3
        AND CHQCODE != 0
         AND SPLITCODE != 0
    ),
    ACCRUAL_COMM_AGT AS (
        SELECT DISTINCT
            A.POLICYCODE,
            B.AGENTNAME AS AGENTFULLNAME,
            B.BROKERID,
            A.OWNERCODE,
            BA.USERDEFINED2,
            A.SPLITCODE,
            COUNT(DISTINCT A.OWNERCODE)
                OVER (PARTITION BY A.POLICYCODE)
                AS NUM_OF_AGTS
        FROM
            {{ ref('accruals_finance_insurance') }} AS A
            INNER JOIN
                {{ ref('brokeradvanced_vc_finance_insurance') }} AS BA
            ON A.OWNERCODE = BA.AGENTCODE
            INNER JOIN
                {{ ref('broker_vc_finance_insurance') }} AS B
            ON A.OWNERCODE = B.AGENTCODE
        WHERE
            OWNERTYPE = 3
            AND CHQCODE != 0
            AND SPLITCODE = 0
            AND POLICYCODE != 0
    ),
    ACCRUAL_COMM_AGT_CORRECT AS (
        --accounts for the case where there were 2 agents on a policy but now only 1 or the agent changes
        SELECT ACCRUAL_COMM_AGT.*
        FROM
            ACCRUAL_COMM_AGT
            INNER JOIN
                {{ ref('policyagentlinking_finance_insurance') }} AS LINK
            ON ACCRUAL_COMM_AGT.POLICYCODE = LINK.POLICYCODE
        WHERE
            NUM_OF_AGTS > 1
            AND MAINAGENT = 1
            AND ACCRUAL_COMM_AGT.OWNERCODE = LINK.AGENTCODE
    ),
    PAL AS (
        -- for cases where the ownertype = 3 is not on the accural but need to map agent another way.
        SELECT
            POLICYCODE,
            P.AGENTCODE,
            B.AGENTNAME,
            B.BROKERID,
            CASE
                WHEN CONTAINS(BA.USERDEFINED2, '/') THEN '999999999'
                ELSE BA.USERDEFINED2
            END AS USERDEFINED2
        FROM
            {{ ref('policyagentlinking_finance_insurance') }} AS P
            LEFT JOIN
                {{ ref('broker_vc_finance_insurance') }} AS B
            ON P.AGENTCODE = B.AGENTCODE
            LEFT JOIN
                {{ ref('brokeradvanced_vc_finance_insurance') }} AS BA
            ON P.AGENTCODE = BA.AGENTCODE
        WHERE
            MAINAGENT = 1
        GROUP BY
            1, 2, 3, 4, 5
    ),
    MOST_DATA_BASE AS (
        SELECT
            A.OWNERCODE,
            A.ACCRUALCODE AS ACCRUALCODE_FROM_ACCURALS,
            A.CHQCODE,
            C.COMMISSIONRUNCODE,
            RUN.TITLE AS COMMISSION_RUN,
            'Unknown' AS OCCURRENCE_TYPE,
            A.AGENTFULLNAME AS OWNER_NAME,
            B.COMPANYNAME AS OWNER_COMPANY,
            A.POLICYCODE,
            CARRIER.ICNAME AS CARRIER,
            C.STATUS,
            POSTDATE::DATE AS POSTDATE,
            A.COMMPREM AS TRANSACTION_COMMISSIONABLE_PREMIUM,
            A.COMMRATE AS COMMISSION_RATE,
            A.SPLITSHARE AS COMMISSION_SPLIT_SHARE,
            A.SPLITCODE,
            COALESCE(
                COMM_AGT_SPLIT.AGENTFULLNAME,
                ACCRUAL_COMM_AGT_CORRECT.AGENTFULLNAME,
                COMM_AGT.AGENTFULLNAME,
                Z_COMM_AGT_SPLIT.AGENTFULLNAME
            ) AS COMMISSIONABLE_AGENT,
            COALESCE(
                COMM_AGT_SPLIT.USERDEFINED2,
                ACCRUAL_COMM_AGT_CORRECT.USERDEFINED2,
                COMM_AGT.USERDEFINED2,
                Z_COMM_AGT_SPLIT.USERDEFINED2
            ) AS COMMISSIONABLE_AGENT_ID,
            COALESCE(
                COMM_AGT_SPLIT.BROKERID,
                ACCRUAL_COMM_AGT_CORRECT.BROKERID,
                COMM_AGT.BROKERID,
                Z_COMM_AGT_SPLIT.BROKERID
            ) AS COMMISSIONABLE_BROKER_ID,
            CASE
                WHEN A.OWNERTYPE = 1 THEN 'MGA'
                WHEN A.OWNERTYPE = 2 THEN 'AGA'
                WHEN A.OWNERTYPE = 3 THEN 'Broker'
                ELSE 'Unclassified'
            END AS OWNER_TYPE,
            TRIM(A.POLICYNUMBEREX) AS POLICY_NUMBER,
            CASE
                WHEN
                    CONTAINS(A.PLANNAMEEX, '(UpToMin)')
                    THEN REPLACE(A.PLANNAMEEX, '(UpToMin)', '')
                WHEN
                    CONTAINS(A.PLANNAMEEX, '(MinToMax)')
                    THEN REPLACE(A.PLANNAMEEX, '(MinToMax)', '')
                WHEN
                    CONTAINS(A.PLANNAMEEX, '(AboveMax)')
                    THEN REPLACE(A.PLANNAMEEX, '(AboveMax)', '')
                ELSE A.PLANNAMEEX
            END AS PLANNAME,
            CASE
                WHEN A.TRXTYPE = 'FYC' THEN A.PAID
                ELSE 0
            END AS TOTAL_FYC,
            CASE
                WHEN A.TRXTYPE = 'FYB' THEN A.PAID
                ELSE 0
            END AS TOTAL_FYB
        FROM
            {{ ref('accruals_finance_insurance') }} AS A
            INNER JOIN
                {{ ref('cheque_finance_insurance') }} AS C
            ON A.CHQCODE = C.CHQCODE
            INNER JOIN
                {{ ref('commissionrun_finance_insurance') }} AS RUN
            ON C.COMMISSIONRUNCODE = RUN.COMMISSIONRUNCODE
            LEFT JOIN
                ACCRUAL_COMM_AGT_SPLIT AS COMM_AGT_SPLIT
            ON A.POLICYCODE = COMM_AGT_SPLIT.POLICYCODE
            AND A.SPLITCODE = COMM_AGT_SPLIT.SPLITCODE
            AND A.OWNERCODE = COMM_AGT_SPLIT.OWNERCODE
            LEFT JOIN
                ACCRUAL_COMM_AGT_SPLIT AS Z_COMM_AGT_SPLIT
            ON A.POLICYCODE = Z_COMM_AGT_SPLIT.POLICYCODE
            AND A.SPLITCODE = Z_COMM_AGT_SPLIT.SPLITCODE
            LEFT JOIN
                ACCRUAL_COMM_AGT AS COMM_AGT
            ON A.POLICYCODE = COMM_AGT.POLICYCODE
            AND A.OWNERCODE = COMM_AGT.OWNERCODE
            LEFT JOIN
                ACCRUAL_COMM_AGT_CORRECT
                ON A.POLICYCODE = ACCRUAL_COMM_AGT_CORRECT.POLICYCODE
            LEFT JOIN
                FINANCE.PROD_INSURANCE.BROKER_VC AS B
            ON A.OWNERCODE = B.AGENTCODE
            LEFT JOIN
                FINANCE.PROD_INSURANCE.IC_VC AS CARRIER
            ON A.ICID = CARRIER.ICID
        WHERE
            POSTDATE::DATE BETWEEN CURRENT_DATE() - 400
            AND CURRENT_DATE()
    ),
    MOST_DATA AS (
        SELECT
            A.OWNERCODE,
            ACCRUALCODE_FROM_ACCURALS,
            CHQCODE,
            COMMISSIONRUNCODE,
            COMMISSION_RUN,
            'Unknown' AS OCCURRENCE_TYPE,
            OWNER_TYPE,
            OWNER_NAME,
            OWNER_COMPANY,
            POLICY_NUMBER,
            A.POLICYCODE,
            CARRIER,
            PLANNAME,
            STATUS,
            TOTAL_FYC,
            TOTAL_FYB,
            POSTDATE,
            TRANSACTION_COMMISSIONABLE_PREMIUM,
            COMMISSION_RATE,
            COMMISSION_SPLIT_SHARE,
            A.SPLITCODE,
            COALESCE(
                A.COMMISSIONABLE_AGENT,
                COMM_AGT.AGENTFULLNAME,
                PAL.AGENTNAME
            ) AS COMMISSIONABLE_AGENT,
            COALESCE(
                A.COMMISSIONABLE_AGENT_ID,
                COMM_AGT.USERDEFINED2,
                PAL.USERDEFINED2
            ) AS COMMISSIONABLE_AGENT_ID,
            COALESCE(
                A.COMMISSIONABLE_BROKER_ID,
                COMM_AGT.BROKERID,
                PAL.BROKERID
            ) AS COMMISSIONABLE_BROKER_ID
        FROM
            MOST_DATA_BASE AS A
            LEFT JOIN
                ACCRUAL_COMM_AGT AS COMM_AGT
            ON A.POLICYCODE = COMM_AGT.POLICYCODE
            LEFT JOIN PAL ON A.POLICYCODE = PAL.POLICYCODE
    ),
    WRIT_AGT AS (
        SELECT
            P.POLICYCODE,
            P.AGENTCODE,
            P.TYPE,
            B.BROKERID,
            B.AGENTNAME
        FROM
            {{ ref('policyagentlinking_finance_insurance') }} AS P
            INNER JOIN
            {{ ref('broker_vc_finance_insurance') }} AS B
            ON P.AGENTCODE = B.AGENTCODE
        WHERE
            POLICYCODE IN (
                SELECT DISTINCT POLICYCODE
                FROM
                    MOST_DATA
            )
            AND TYPE = 1
        GROUP BY
            1, 2, 3, 4, 5
    ),
    POLICYCODE_TYPE AS (
        SELECT
            POLICYCODE,
            PLANTYPE
        FROM
            {{ ref('commission_vc_finance_insurance') }}
        WHERE
            POLICYCODE IN (
                SELECT DISTINCT POLICYCODE
                FROM
                    MOST_DATA
            )
        GROUP BY
            1, 2
    ),
    POL_TYPE AS (
        SELECT
            TRIM(POLICYNUMBER) AS POLICY_NUMBER,
            MIN(PLANTYPE) AS PLANTYPE
        FROM
            {{ ref('policy_vc_finance_insurance') }}
        WHERE
            POLICYCODE IN (
                SELECT DISTINCT POLICYCODE
                FROM
                    MOST_DATA
            )
            AND ISMAINCOVERAGE = 'yes'
        GROUP BY
            1
    ),
    TRX_DATA AS (
        SELECT
            COMM_TRX.ACCRUALCODE,
            COMM_TRX.COMMISSIONRUNCODE,
            COMM_TRX.OWNERTYPE,
            COMM_TRX.POLICYCODE,
            COMM_TRX.CHQCODE AS COMM_TRX_CHQCODE,
            MOST_DATA.CHQCODE,
            MOST_DATA.COMMISSIONABLE_AGENT,
            MOST_DATA.COMMISSIONABLE_AGENT_ID,
            MOST_DATA.COMMISSIONABLE_BROKER_ID,
            MOST_DATA.POLICY_NUMBER,
            MOST_DATA.POLICYCODE,
            COMM_TRX.OWNERCODE,
            MOST_DATA.SPLITCODE,
            CASE
                WHEN COMM_TRX.OWNERTYPE = 1 THEN 'MGA'
                WHEN COMM_TRX.OWNERTYPE = 2 THEN 'AGA'
                WHEN COMM_TRX.OWNERTYPE = 3 THEN 'Broker'
                ELSE 'Unclassified'
            END AS OWNER_TYPE,
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
            LEFT JOIN
                {{ ref('commissiontrx_finance_insurance') }} AS COMM_TRX
            ON MOST_DATA.ACCRUALCODE_FROM_ACCURALS = COMM_TRX.ACCRUALCODE
            LEFT JOIN
                {{ ref('aga_finance_insurance') }} AS AGA
            ON COMM_TRX.OWNERCODE = AGA.AGACODE
            LEFT JOIN
                {{ ref('mga_finance_insurance') }} AS MGA
            ON COMM_TRX.OWNERCODE = MGA.MGACODE
            LEFT JOIN
                {{ ref('broker_vc_finance_insurance') }} AS B
            ON COMM_TRX.OWNERCODE = B.AGENTCODE
    ),
    MAP_TRANSACTIONS AS (
        SELECT
            MOST_DATA.ACCRUALCODE_FROM_ACCURALS,
            MOST_DATA.CHQCODE,
            MOST_DATA.OWNERCODE,
            MOST_DATA.COMMISSIONRUNCODE,
            MOST_DATA.COMMISSION_RUN,
            MOST_DATA.COMMISSIONABLE_AGENT,
            MOST_DATA.COMMISSIONABLE_AGENT_ID,
            MOST_DATA.COMMISSIONABLE_BROKER_ID,
            WRIT_AGT.AGENTNAME AS WRITING_AGENT,
            WRIT_AGT.BROKERID AS REP_ID,
            MOST_DATA.OWNER_NAME,
            TRIM(MOST_DATA.POLICY_NUMBER)::TEXT AS POLICY_NUMBER,
            MOST_DATA.POLICYCODE,
            MOST_DATA.CARRIER,
            MOST_DATA.PLANNAME AS PLAN_NAME,
            MOST_DATA.STATUS,
            MOST_DATA.POSTDATE,
            MOST_DATA.TRANSACTION_COMMISSIONABLE_PREMIUM,
            MOST_DATA.COMMISSION_RATE,
            MOST_DATA.COMMISSION_SPLIT_SHARE,
            MOST_DATA.SPLITCODE,
            CASE
                WHEN TRX_DATA.ACCRUALCODE IS NOT NULL THEN 'Transaction'
                ELSE 'Accrual'
            END AS OCCURRENCE_TYPE,
            CASE
                WHEN TRX_DATA.ACCRUALCODE IS NOT NULL THEN TRX_DATA.OWNER_TYPE
                ELSE MOST_DATA.OWNER_TYPE
            END AS OWNER_TYPE,
            CASE
                WHEN
                    POLICYCODE_TYPE.PLANTYPE IS NOT NULL
                    THEN POLICYCODE_TYPE.PLANTYPE
                WHEN MOST_DATA.PLANNAME = 'Group Benefits' THEN 'Group Benefits'
                ELSE POL_TYPE.PLANTYPE
            END AS PLANCATEGORY,
            CASE
                WHEN
                    TRX_DATA.TOTAL_FYC IS NOT NULL
                    THEN ROUND(TRX_DATA.TOTAL_FYC, 2)
                ELSE ROUND(MOST_DATA.TOTAL_FYC, 2)
            END AS TOTAL_FYC,
            CASE
                WHEN
                    TRX_DATA.TOTAL_FYB IS NOT NULL
                    THEN ROUND(TRX_DATA.TOTAL_FYB, 2)
                ELSE ROUND(MOST_DATA.TOTAL_FYB, 2)
            END AS TOTAL_FYB
        FROM
            MOST_DATA
            LEFT JOIN
                TRX_DATA
                ON MOST_DATA.ACCRUALCODE_FROM_ACCURALS = TRX_DATA.ACCRUALCODE
            AND MOST_DATA.COMMISSIONRUNCODE = TRX_DATA.COMMISSIONRUNCODE
            LEFT JOIN
                POLICYCODE_TYPE
                ON MOST_DATA.POLICYCODE = POLICYCODE_TYPE.POLICYCODE
            LEFT JOIN
                POL_TYPE
                ON MOST_DATA.POLICY_NUMBER = POL_TYPE.POLICY_NUMBER
            LEFT JOIN WRIT_AGT ON MOST_DATA.POLICYCODE = WRIT_AGT.POLICYCODE
    ),
    COMM_AGT_MISSING_MAP_ZERO AS (
        SELECT
            MAP_TRX.POLICYCODE,
            P.AGENTCODE,
            B.FULLAGENTNAME,
            B.AGENTNAME,
            B.BROKERID,
            BA.USERDEFINED2,
            MAP_TRX.SPLITCODE,
            MAP_TRX.OWNER_TYPE
        FROM
            MAP_TRANSACTIONS AS MAP_TRX
            LEFT JOIN
                {{ ref('policyagentlinking_finance_insurance') }} AS P
            ON MAP_TRX.POLICYCODE = P.POLICYCODE
            LEFT JOIN
                {{ ref('brokeradvanced_vc_finance_insurance') }} AS BA
            ON P.AGENTCODE = BA.AGENTCODE
            LEFT JOIN
                {{ ref('broker_vc_finance_insurance') }} AS B
            ON P.AGENTCODE = B.AGENTCODE
        WHERE
            COMMISSIONABLE_AGENT IS NULL
            AND P.TYPE = 2
            AND SPLITCODE = 0
        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8
    ),
    COMM_AGT_MISSING_MAP AS (
        SELECT
            MAP_TRX.POLICYCODE,
            P.AGENTCODE,
            B.FULLAGENTNAME,
            B.AGENTNAME,
            B.BROKERID,
            CASE
                WHEN CONTAINS(BA.USERDEFINED2, '/') THEN '999999999'
                ELSE BA.USERDEFINED2
            END AS USERDEFINED2,
            MAP_TRX.SPLITCODE,
            MAP_TRX.OWNER_TYPE
        FROM
            MAP_TRANSACTIONS AS MAP_TRX
            LEFT JOIN
                {{ ref('policyagentlinking_finance_insurance') }} AS P
            ON MAP_TRX.POLICYCODE = P.POLICYCODE
            LEFT JOIN
                {{ ref('brokeradvanced_vc_finance_insurance') }} AS BA
            ON P.AGENTCODE = BA.AGENTCODE
            LEFT JOIN
                {{ ref('broker_vc_finance_insurance') }} AS B
            ON P.AGENTCODE = B.AGENTCODE
        WHERE
            COMMISSIONABLE_AGENT IS NULL
            AND P.TYPE = 2
            AND SPLITCODE != 0
        GROUP BY
            1, 2, 3, 4, 5, 6, 7, 8
    ),
    WITH_COMM_AGT_FILLEDIN AS (
        SELECT
            T.ACCRUALCODE_FROM_ACCURALS,
            T.CHQCODE,
            T.OWNERCODE,
            T.COMMISSIONRUNCODE,
            T.COMMISSION_RUN,
            T.OCCURRENCE_TYPE,
            T.WRITING_AGENT,
            T.REP_ID,
            T.OWNER_TYPE,
            T.OWNER_NAME,
            T.POLICY_NUMBER,
            T.POLICYCODE,
            T.CARRIER,
            T.PLAN_NAME,
            T.PLANCATEGORY,
            T.STATUS,
            T.TOTAL_FYC,
            T.TOTAL_FYB,
            T.POSTDATE,
            T.TRANSACTION_COMMISSIONABLE_PREMIUM,
            T.COMMISSION_RATE,
            T.COMMISSION_SPLIT_SHARE,
            T.SPLITCODE,
            CASE
                WHEN T.COMMISSIONABLE_AGENT IS NULL
                AND T.SPLITCODE = 0 THEN ZERO.FULLAGENTNAME
                WHEN T.COMMISSIONABLE_AGENT IS NULL
                AND T.SPLITCODE != 0 THEN MAP.FULLAGENTNAME
                ELSE T.COMMISSIONABLE_AGENT
            END AS COMMISSIONABLE_AGENT,
            CASE
                WHEN T.COMMISSIONABLE_AGENT IS NULL
                AND T.SPLITCODE = 0 THEN ZERO.USERDEFINED2
                WHEN T.COMMISSIONABLE_AGENT IS NULL
                AND T.SPLITCODE != 0 THEN MAP.USERDEFINED2
            END AS COMMISSIONABLE_AGENT_ID,
            CASE
                WHEN T.COMMISSIONABLE_AGENT IS NULL
                AND T.SPLITCODE = 0 THEN ZERO.BROKERID
                WHEN T.COMMISSIONABLE_AGENT IS NULL
                AND T.SPLITCODE != 0 THEN MAP.BROKERID
            END AS COMMISSIONABLE_BROKER_ID
        FROM
            MAP_TRANSACTIONS AS T
            LEFT JOIN
                COMM_AGT_MISSING_MAP_ZERO AS ZERO
            ON T.POLICYCODE = ZERO.POLICYCODE
            LEFT JOIN
                COMM_AGT_MISSING_MAP AS MAP
            ON T.POLICYCODE = MAP.POLICYCODE
            AND T.SPLITCODE = MAP.SPLITCODE
    )
    SELECT
        MAP_TRANSACTIONS.ACCRUALCODE_FROM_ACCURALS AS ACCURALCODE,
        COMMISSION_RUN,
        OCCURRENCE_TYPE,
        MAP_TRANSACTIONS.COMMISSIONABLE_AGENT,
        MAP_TRANSACTIONS.COMMISSIONABLE_AGENT_ID,
        MAP_TRANSACTIONS.COMMISSIONABLE_BROKER_ID,
        WRITING_AGENT,
        REP_ID,
        OWNER_TYPE,
        CASE
            WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'AGA' THEN AGA.AGANAME
            WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'MGA' THEN MGA.MGAID
            WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'Broker' THEN B.AGENTNAME
        END AS OWNER_NAME,
        CASE
            WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'AGA' THEN AGA.COMPANY
            WHEN MAP_TRANSACTIONS.OWNER_TYPE = 'MGA' THEN MGA.MGAID
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
        LEFT JOIN
            {{ ref('aga_finance_insurance') }} AS AGA
        ON MAP_TRANSACTIONS.OWNERCODE = AGA.AGACODE
        LEFT JOIN
            {{ ref('mga_finance_insurance') }} AS MGA
        ON MAP_TRANSACTIONS.OWNERCODE = MGA.MGACODE
        LEFT JOIN
            {{ ref('broker_vc_finance_insurance') }} AS B
        ON MAP_TRANSACTIONS.OWNERCODE = B.AGENTCODE
    WHERE
        OCCURRENCE_TYPE = 'Accrual'
    UNION
    SELECT
        MAP_TRANSACTIONS.ACCRUALCODE_FROM_ACCURALS AS ACCURALCODE,
        COMMISSION_RUN,
        OCCURRENCE_TYPE,
        MAP_TRANSACTIONS.COMMISSIONABLE_AGENT,
        MAP_TRANSACTIONS.COMMISSIONABLE_AGENT_ID,
        MAP_TRANSACTIONS.COMMISSIONABLE_BROKER_ID,
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
        LEFT JOIN
            TRX_DATA
            ON MAP_TRANSACTIONS.ACCRUALCODE_FROM_ACCURALS = TRX_DATA.ACCRUALCODE
        AND MAP_TRANSACTIONS.COMMISSIONRUNCODE = TRX_DATA.COMMISSIONRUNCODE
        AND MAP_TRANSACTIONS.TOTAL_FYB = TRX_DATA.TOTAL_FYB
        AND MAP_TRANSACTIONS.TOTAL_FYC = TRX_DATA.TOTAL_FYC
    WHERE
        OCCURRENCE_TYPE = 'Transaction'
