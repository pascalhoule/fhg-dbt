{{  config(alias='__postdate', database='normalize', schema='insurance')  }} 

WITH
FIN_FIRST_POSTDATE AS (
    SELECT
        A.POLICYCODE,
        MIN(C.POSTDATE::DATE) AS FIRST_POST_DATE
    FROM
        {{ ref('accruals_clean_insurance') }} A
        LEFT JOIN {{ ref('cheque_clean_insurance') }} C ON A.CHQCODE = C.CHQCODE   
    WHERE
        TRXTYPE = 'FYC'
        AND C.POSTDATE::DATE >= '2018-01-01'
    GROUP BY
        1
)
    SELECT
        POLICYCATEGORY,
        POL.POLICYCODE,
        POLICYGROUPCODE,
        AGENTCODE AS SERVICINGAGTCODE,
        CARRIER,
        POLICYNUMBER,
        PLANID,
        PLANTYPE,
        PLANNAME,
        PREMIUMAMOUNT,
        ANNUALPREMIUMAMOUNT,
        COMMPREMIUMAMOUNT,
        PAYMENTMODE,
        FACEAMOUNT,
        CREATEDBY,
        CREATEDDATE,
        APPLICATIONDATE,
        CONTRACTDATE,
        SETTLEMENTDATE,
        EXPIRYDATE,
        RENEWALDATE,
        SENTTOICDATE,
        MAILEDDATE,
        FIN_FIRST_POSTDATE.FIRST_POST_DATE AS FINPOSTDATE,
        STATUS,
        SPLITRATE AS SERVICINGAGTSPLIT,
        APPCOUNT,
        FYCAMOUNT,
        MGAFYOAMOUNT,
        ISSUEPROVINCE,
        APPSOURCE,
        APPTYPE,
        LASTCOMMISSIONPROCESSDATE,
        FIRSTOWNERCLIENTCODE,
        FIRSTINSUREDCLIENTCODE,
        ISMAINCOVERAGE,
        COALESCE(FINPOSTDATE, SETTLEMENTDATE) AS FH_SETTLEMENTDATE
    FROM
        {{ ref ('__POLICYCATEGORY_insurance') }} POL
        LEFT JOIN FIN_FIRST_POSTDATE ON POL.POLICYCODE = FIN_FIRST_POSTDATE.POLICYCODE
    WHERE
        APPLICATIONDATE >= '2018-01-01'