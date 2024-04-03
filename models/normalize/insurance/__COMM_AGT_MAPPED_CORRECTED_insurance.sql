{{  config(alias='__comm_agt_mapped_corrected', database='normalize', schema='insurance')  }}

WITH POSSIBLE_ERRORS AS (
    SELECT
        POLICYCODE,
        sum(FYCSERVAMT) FYCSERVAMT,
        sum(FYCCOMMAMT) FYCCOMMAMT,
        (
            ROUND(sum(FYCSERVAMT), 3) - ROUND(sum(FYCCOMMAMT), 3)
        ) AS DIFF
    FROM
        {{ ref ('__COMM_AGT_MAPPED_insurance') }}
    WHERE
        FH_STARTDATE between '2020-01-01'
        and current_date()
        and STATUSNAMEENG in ('Inforce - Commission Paid', 'Terminated')
    GROUP BY
        1
    HAVING
        ABS(DIFF) > 0.021
    ORDER BY
        1, 2
),
COMPARE_TO_FIN AS (
    SELECT
        A.POLICYCODE,
        A.OWNERCODE,
        ROUND(SUM(A.PAID), 2) AS PAID
    FROM
        {{ ref ('accruals_normalize_insurance') }} A
        LEFT JOIN {{ ref ('cheque_normalize_insurance') }} C ON A.CHQCODE = C.CHQCODE
    WHERE
        TRXTYPE = 'FYC'
        AND A.POLICYCODE IN (
            SELECT
                DISTINCT POLICYCODE
            FROM
                POSSIBLE_ERRORS
        )
        AND C.POSTDATE::DATE >= '2018-01-01'
    GROUP BY
        1, 2
    ORDER BY
        1
),
SERV_SPLIT_ERROR AS --this is the table used to correct the servicing agent split
(
    SELECT
        POLICYCODE,
        SUM(SERVICINGAGTSPLIT) Tot_Split
    FROM
        {{ ref ('__COMM_AGT_MAPPED_insurance') }}
    GROUP BY
        1
    HAVING
        SUM(SERVICINGAGTSPLIT) > 1
        OR SUM(SERVICINGAGTSPLIT) < 0.9999
),
APP_COUNT_GT_ONE AS --these are the records to correct app counts
(
    SELECT
        POLICYNUMBER,
        SUM(APPCOUNT) AS APPCOUNT
    FROM
        {{ ref ('__COMM_AGT_MAPPED_insurance') }}
    WHERE
        APPLICATIONDATE >= '2020-01-01'
        AND POLICYNUMBER is not null
        AND POLICYNUMBER not in ('TBA', 'TBD', 'IAETBD')
    GROUP BY
        1
    HAVING
        SUM(APPCOUNT) > 1
)
SELECT
    POLICYCATEGORY,
    POL.POLICYCODE,
    POLICYGROUPCODE,
    SERVICINGAGTCODE,
    COALESCE(
        POL.SERVICINGAGTSPLIT / SERV_SPLIT_ERROR.Tot_Split,
        POL.SERVICINGAGTSPLIT
    ) AS SERVICINGAGTSPLIT,
    POL.COMMISSIONINGAGTCODE,
    COMMISSIONINGAGTSPLIT,
    CARRIERENG,
    CARRIERFR,
    POL.POLICYNUMBER,
    PLANID,
    PLANTYPE,
    PLANNAMEENG,
    PLANNAMEFR,
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
    FINPOSTDATE,
    STATUSCODE,
    STATUSNAMEENG,
    STATUSNAMEFR,
    STATUSCATEGORY,
    COALESCE(
        POL.APPCOUNT / APP_COUNT_GT_ONE.APPCOUNT,
        POL.APPCOUNT
    ) AS APPCOUNT,
    FYCSERVAMT,
    COALESCE(COMPARE_TO_FIN.PAID, POL.FYCCOMMAMT) AS FYCCOMMAMT,
    MGAFYOAMOUNT,
    ISSUEPROVINCE,
    APPSOURCE,
    APPTYPE,
    LASTCOMMISSIONPROCESSDATE,
    FIRSTOWNERCLIENTCODE,
    FIRSTINSUREDCLIENTCODE,
    ISMAINCOVERAGE,
    FH_SETTLEMENTDATE,
    FH_STARTDATE,
    FH_PREMIUM
FROM
    {{ ref ('__COMM_AGT_MAPPED_insurance') }} POL
    LEFT JOIN COMPARE_TO_FIN ON POL.POLICYCODE = COMPARE_TO_FIN.POLICYCODE
    AND COMPARE_TO_FIN.OWNERCODE = POL.COMMISSIONINGAGTCODE
    LEFT JOIN SERV_SPLIT_ERROR ON POL.POLICYCODE = SERV_SPLIT_ERROR.POLICYCODE
    LEFT JOIN APP_COUNT_GT_ONE ON POL.POLICYNUMBER = APP_COUNT_GT_ONE.POLICYNUMBER