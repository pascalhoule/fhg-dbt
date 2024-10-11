{{  config(alias='__fix_two_serv_one_comm', database='normalize', schema='insurance', tags=["policy_fh"])  }}

 WITH FINANCE AS (
        SELECT
            *
        FROM
            {{ ref('detailed_FYC_FYB_finance_queries') }}
    ),
    --this section of code identifies the mult serv agts and 1 comm agt cases.
    SERV_AGT_COUNT AS (
        SELECT
            POLICYCODE,
            COUNT(DISTINCT FH_SERVICINGAGTCODE) NUM_SERV
        FROM
            {{ ref('__FHFORMAT_insurance') }}
        WHERE
            POLICYNUMBER IS NOT NULL
        GROUP BY
            1
        HAVING
            NUM_SERV > 1
    ),
    COMM_AGT_COUNT AS (
        SELECT
            POLICYCODE,
            COUNT(DISTINCT OWNERCODE) NUM_COMM
        FROM
            {{ ref('__COMM_WGTS_insurance') }}
        GROUP BY
            1
        HAVING
            NUM_COMM = 1
    ),
    TWO_SERV_ONE_COMM_AGT_LIST AS (
        SELECT
            SERV_AGT_COUNT.POLICYCODE,
            SERV_AGT_COUNT.NUM_SERV,
            COMM_AGT_COUNT.NUM_COMM
        FROM
            SERV_AGT_COUNT
            JOIN COMM_AGT_COUNT ON SERV_AGT_COUNT.POLICYCODE = COMM_AGT_COUNT.POLICYCODE
    ),
    POLDATA AS (
        SELECT
            POLICYNUMBER,
            POLICYCODE,
            SUM(FH_FYCCOMMAMT) POL_FYC
        FROM
            {{ ref('__FIX_COMM_NEGATIVE_normalize_insurance') }}
        GROUP BY
            1, 2
    ),
    FINDATA AS (
        SELECT
            POLICY_NUMBER AS POLICYNUMBER,
            POLICYCODE,
            SUM(TOTAL_FYC) FIN_FYC
        FROM
            FINANCE
        WHERE
            POLICYNUMBER IN (
                SELECT
                    DISTINCT POLICYNUMBER
                FROM
                    POLDATA
            )
        GROUP BY
            1, 2
    ),
    UNMATCHED AS (
        SELECT
            *
        FROM
            FINDATA NATURAL FULL
            OUTER JOIN POLDATA
        WHERE
            ROUND(FIN_FYC, 2) != ROUND(POL_FYC, 2)
    ),
    LIST_TO_CORRECT AS (
        SELECT
            UN.POLICYCODE,
            ROUND(UN.FIN_FYC, 2) AS COMM_FYC
        FROM
            UNMATCHED UN
            JOIN TWO_SERV_ONE_COMM_AGT_LIST LST ON UN.POLICYCODE = LST.POLICYCODE
    ),
    ADD_COMMAGTCODE_TO_LIST_TO_CORRECT AS (
        SELECT
            LST.*,
            WGT.OWNERCODE AS COMM_AGT_CODE
        FROM
            {{ ref('__COMM_WGTS_insurance') }} WGT
            JOIN LIST_TO_CORRECT LST ON WGT.POLICYCODE = LST.POLICYCODE
    ),
    RECORDS_TO_CORRECT AS (
        SELECT
            POL.POLICYNUMBER,
            POL.POLICYCODE,
            LST.COMM_AGT_CODE,
            LST.COMM_FYC,
            1 AS COMM_AGT_WT,
            MIN(POL.FH_SERVICINGAGTCODE) AS FH_SERVICINGAGTCODE
        FROM
            {{ ref('__FIX_COMM_NEGATIVE_normalize_insurance') }} POL
            JOIN ADD_COMMAGTCODE_TO_LIST_TO_CORRECT LST ON POL.POLICYCODE = LST.POLICYCODE
        GROUP BY
            1, 2, 3, 4, 5
    )
    SELECT
        FH_POLICYCATEGORY,
        POL.POLICYCODE,
        POLICYGROUPCODE,
        POL.FH_SERVICINGAGTCODE,
        FH_SERVICINGAGTSPLIT,
        COALESCE(POL.FH_COMMISSIONINGAGTCODE, REC.COMM_AGT_CODE) AS FH_COMMISSIONINGAGTCODE,
        COALESCE(POL.FH_COMMISSIONINGAGTSPLIT, REC.COMM_AGT_WT) AS FH_COMMISSIONINGAGTSPLIT,
        FH_CARRIERENG,
        FH_CARRIERFR,
        POL.POLICYNUMBER,
        PLANID,
        FH_PLANTYPE,
        FH_PLANNAMEENG,
        FH_PLANNAMEFR,
        PREMIUMAMOUNT,
        ANNUALPREMIUMAMOUNT,
        COMMPREMIUMAMOUNT,
        PAYMENTMODE,
        FACEAMOUNT,
        CREATEDBY,
        CREATEDDATE,
        CONTRACTDATE,
        SETTLEMENTDATE,
        EXPIRYDATE,
        RENEWALDATE,
        SENTTOICDATE,
        MAILEDDATE,
        FH_FINPOSTDATE,
        FH_STATUSCODE,
        FH_STATUSNAMEENG,
        FH_STATUSNAMEFR,
        FH_STATUSCATEGORY,
        APPCOUNT,
        FH_FYCSERVAMT,
        CASE
            WHEN POL.FH_FYCCOMMAMT = 0 THEN REC.COMM_FYC
            ELSE POL.FH_FYCCOMMAMT
        END AS FH_FYCCOMMAMT,
        MGAFYOAMOUNT,
        ISSUEPROVINCE,
        FH_APPSOURCE,
        FH_APPTYPE,
        LASTCOMMISSIONPROCESSDATE,
        FIRSTOWNERCLIENTCODE,
        FIRSTINSUREDCLIENTCODE,
        ISMAINCOVERAGE,
        FH_SETTLEMENTDATE,
        FH_STARTDATE,
        FH_PREMIUM,
        FH_PREM_COMMWGT,
        FH_PREM_SERVWGT,
        APPLICATIONDATE
    FROM
        {{ ref('__FIX_COMM_NEGATIVE_normalize_insurance') }} POL
        LEFT JOIN RECORDS_TO_CORRECT REC ON POL.POLICYNUMBER = REC.POLICYNUMBER
        AND POL.POLICYCODE = REC.POLICYCODE
        AND POL.FH_SERVICINGAGTCODE = REC.FH_SERVICINGAGTCODE