{{  config(alias='__policycategory', database='normalize', schema='insurance', tags=["policy_fh"])  }} 

WITH 
    LIST_FOR_SERVICE AS (
        SELECT
            POL_VC.POLICYNUMBER,
            POL_VC.PLANNAME AS PLANNAME,
            POL_VC.STATUS AS STATUS,
            POL_VC.APPLICATIONDATE,
            POL_VC.CONTRACTDATE AS EFFECTIVE_DATE,
            POL_VC.SETTLEMENTDATE,
            'SERVICE' AS POLICYCATEGORY,
            SUM(POL_VC.APPCOUNT) AS APPCOUNT
        from
             {{ ref ('policy_vc_clean_insurance') }} POL_VC
        where
            POL_VC.PLANTYPE IN (
                'CI',
                'DI',
                'Life',
                'Permanent',
                'Term',
                'UL',
                'LTC',
                'Health',
                'HealthSickness',
                'SERVICE',
                'Travel',
                'WL'
            )
            AND (
                (
                    POL_VC.STATUS IN ('ASVC', 'ADLRC', 'TRSO')
                    OR (POL_VC.PLANNAME ILIKE '%unknown%')
                    OR (
                        CREATEDBY IN (
                            'Inforce-Update',
                            'Inforce-Create',
                            'Inforce-Replace'
                        )
                        AND POL_VC.FYCAMOUNT = 0
                    )
                    OR
                    (
                        POL_VC.CREATEDDATE > POL_VC.SETTLEMENTDATE
                        AND POL_VC.SETTLEMENTDATE IS NOT NULL
                        AND POL_VC.STATUS IN (
                            'AEXP',
                            'ALAP',
                            'ARSC',
                            'APST',
                            'ANTK',
                            'ACLS',
                            'AUKN',
                            'ACLM',
                            'ASUR',
                            'TERM',
                            'ANPW',
                            'ADCL'
                        )
                        AND ISMAINCOVERAGE = 'yes'
                    )
                    OR POL_VC.STATUS = 'ADLRP'
                    and POL_VC.FYCAMOUNT = 0
                    OR (
                        POL_VC.STATUS IN ('ADLRP', 'AEXP', 'TERM', 'ACLM', 'ASUR', 'AUKN')
                        AND POL_VC.FYCAMOUNT = 0
                        AND (POL_VC.APPLICATIONDATE > EFFECTIVE_DATE)
                        AND ISMAINCOVERAGE = 'yes'
                    )
                    OR (
                        POL_VC.STATUS IN ('ALAP', 'ACNV', 'AUKN', 'AWTH')
                        AND POL_VC.FYCAMOUNT = 0
                        AND ISMAINCOVERAGE = 'yes'
                    )
                    OR (
                        POL_VC.STATUS = 'ASUR'
                        AND (POL_VC.APPLICATIONDATE > EFFECTIVE_DATE)
                    )
                )
            )
        GROUP BY
            1,2,3,4,5,6,7
        HAVING
            SUM(POL_VC.APPCOUNT) > 0
            AND POL_VC.APPLICATIONDATE >= '1990-01-01'
    ),
    LIST_NEW_POL AS (
        SELECT
            POL.POLICYNUMBER,
            POL.POLICYGROUPCODE,
            SUM(POL.APPCOUNT) AS APP_COUNT,
            MIN(APPLICATIONDATE) FIRST_APP_DT,
            SUM(FYCAMOUNT) AS TOTAL_FYC
        FROM
            {{ ref ('policy_vc_clean_insurance') }} POL
        WHERE
            PLANTYPE IN (
                'CI',
                'DI',
                'Life',
                'Permanent',
                'Term',
                'UL',
                'LTC',
                'Health',
                'HealthSickness',
                'SERVICE',
                'Travel',
                'WL'
            )
            AND POL.APPLICATIONDATE >= '1990-01-01'
        GROUP BY
            1,2
        HAVING
            APP_COUNT > 0
    ),
    MAPPING_BASE AS (
        SELECT
            POL_VC.POLICYCODE,
            POL_VC.POLICYNUMBER,
            POL_VC.POLICYGROUPCODE,
            POL_VC.APPLICATIONDATE,
            POL_VC.ISMAINCOVERAGE,
            'UNKNOWN' as POLICYCATEGORY
        FROM
            {{ ref ('policy_vc_clean_insurance') }} POL_VC 
        WHERE
            POL_VC.APPLICATIONDATE >= '1990-01-01'
    ),
    MAPPING_BASE_W_SERVICE AS (
        SELECT
            MAPPING_BASE.POLICYCODE,
            MAPPING_BASE.POLICYNUMBER,
            MAPPING_BASE.APPLICATIONDATE,
            ISMAINCOVERAGE,
            MAPPING_BASE.POLICYGROUPCODE,
            CASE
                WHEN LIST_FOR_SERVICE.POLICYCATEGORY = 'SERVICE' THEN 'SERVICE'
                ELSE MAPPING_BASE.POLICYCATEGORY
            END AS POLICYCATEGORY
        from
            MAPPING_BASE
            LEFT JOIN LIST_FOR_SERVICE ON MAPPING_BASE.POLICYNUMBER = LIST_FOR_SERVICE.POLICYNUMBER
    ),
    MAPPING_BASE_W_ALL_POLICYCATEGORIES AS (
        SELECT
            MAPPING_BASE_W_SERVICE.POLICYCODE,
            MAPPING_BASE_W_SERVICE.POLICYNUMBER,
            MAPPING_BASE_W_SERVICE.POLICYGROUPCODE,
            MAPPING_BASE_W_SERVICE.APPLICATIONDATE,
            DATEDIFF(DAY, LIST_NEW_POL.FIRST_APP_DT, APPLICATIONDATE),
            LIST_NEW_POL.FIRST_APP_DT,
            CASE
                WHEN (
                    POLICYCATEGORY = 'UNKNOWN'
                    OR POLICYCATEGORY IS NULL
                )
                AND LIST_NEW_POL.POLICYNUMBER IS NOT NULL
                AND DATEDIFF(
                    DAY,
                    FIRST_APP_DT,
                    MAPPING_BASE_W_SERVICE.APPLICATIONDATE
                ) < 1 
                THEN 'NEW POLICY'
                WHEN (
                    POLICYCATEGORY = 'UNKNOWN'
                    OR POLICYCATEGORY IS NULL
                )
                AND LIST_NEW_POL.POLICYNUMBER IS NULL
                AND LIST_NEW_POL.APP_COUNT > 0
                AND DATEDIFF(DAY, LIST_NEW_POL.FIRST_APP_DT, APPLICATIONDATE) < 1 THEN 'NEW POLICY'
                WHEN (
                    POLICYCATEGORY = 'UNKNOWN'
                    OR POLICYCATEGORY IS NULL
                ) 
                THEN 'NEW RIDER'
                ELSE POLICYCATEGORY
            END AS POLICYCATEGORY
        FROM
            MAPPING_BASE_W_SERVICE
            LEFT JOIN LIST_NEW_POL ON LIST_NEW_POL.POLICYGROUPCODE =  MAPPING_BASE_W_SERVICE.POLICYGROUPCODE
    ),
    DE_DUP AS (
        SELECT
            POLICYCATEGORY,
            POL.*
        FROM
            {{ ref ('policy_vc_clean_insurance') }} pol
            LEFT JOIN MAPPING_BASE_W_ALL_POLICYCATEGORIES ON POL.POLICYCODE = MAPPING_BASE_W_ALL_POLICYCATEGORIES.policycode 
        WHERE
            PLANTYPE IN (
                'CI',
                'DI',
                'Life',
                'Permanent',
                'Term',
                'UL',
                'LTC',
                'Health',
                'HealthSickness',
                'SERVICE',
                'Travel',
                'WL'
            ) AND POL.APPLICATIONDATE >= '1990-01-01'
        group by
            1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35
    )
    select
        POLICYCATEGORY AS FH_POLICYCATEGORY,
        POLICYCODE,
        POLICYGROUPCODE,
        AGENTCODE,
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
        STATUS,
        CASE
            WHEN SUM(SPLITRATE) OVER (PARTITION BY POLICYCODE) = 0.5 THEN 1
            ELSE SPLITRATE
        END AS SPLITRATE,
        CASE
            WHEN SUM(APPCOUNT) OVER (PARTITION BY POLICYGROUPCODE) = 0.5 THEN 1
            ELSE APPCOUNT
        END AS APPCOUNT,
        CASE
            WHEN sum(APPCOUNT) OVER (PARTITION BY POLICYGROUPCODE) = 0.5 THEN 2 * FYCAMOUNT
            WHEN sum(SPLITRATE) OVER (PARTITION BY POLICYCODE) = 0.5 THEN 2 * FYCAMOUNT
            ELSE FYCAMOUNT
        END AS FYCAMOUNT,
        MGAFYOAMOUNT,
        ISSUEPROVINCE,
        APPSOURCE,
        APPTYPE,
        LASTCOMMISSIONPROCESSDATE,
        FIRSTOWNERCLIENTCODE,
        FIRSTINSUREDCLIENTCODE,
        ISMAINCOVERAGE
    from
        DE_DUP
     