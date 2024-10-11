{{  config(alias='__fix_comm_negative', database='normalize', schema='insurance', tags=["policy_fh"], materialized="table")  }} 

WITH AGT_SPLIT AS (
        SELECT
            A.POLICYCODE,
            A.OWNERCODE,
            TRXTYPE,
            SUM(A.PAID) AS PAID
        FROM
            {{ ref('accruals_clean_insurance') }}  A
            JOIN {{ ref('cheque_clean_insurance') }} C ON A.CHQCODE = C.CHQCODE
        WHERE
            TRXTYPE = 'FYC'
            AND C.POSTDATE::DATE >= '2018-01-01'
        GROUP BY
            1, 2, 3 
    )
    SELECT
        FH_POLICYCATEGORY,
        POL.POLICYCODE,
        POLICYGROUPCODE,
        FH_SERVICINGAGTCODE,
        FH_SERVICINGAGTSPLIT,
        COALESCE(POL.FH_COMMISSIONINGAGTCODE, AGT_SPLIT.OWNERCODE) AS FH_COMMISSIONINGAGTCODE,
        FH_COMMISSIONINGAGTSPLIT,
        FH_CARRIERENG,
        FH_CARRIERFR,
        POLICYNUMBER,
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
            WHEN POL.FH_FYCCOMMAMT != AGT_SPLIT.PAID THEN AGT_SPLIT.PAID
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
        {{ ref('__FREEZE_APPDT_insurance') }} POL
        LEFT JOIN AGT_SPLIT ON POL.POLICYCODE = AGT_SPLIT.POLICYCODE
        AND POL.FH_COMMISSIONINGAGTCODE = AGT_SPLIT.OWNERCODE
    WHERE
        POL.FH_FYCCOMMAMT != AGT_SPLIT.PAID