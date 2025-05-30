{{ config( alias='b_life_credits', database='agt_comm', schema='contest', materialized = "view" ) }} 

WITH DATES AS (
            SELECT
                LIFE_CREDITS_STARTDATE,
                LIFE_CREDITS_ENDDATE
            FROM
                {{ source('contest', 'date_ranges') }}
            WHERE
                INCL_IN_RPT = TRUE
        )
        SELECT
            BA.USERDEFINED2 AS UD2,
            C.OWNERCODE AS AGENTCODE,
            SUM(C.COMMISSIONAMOUNT) AS FYC_AMOUNT,
            SUM(C.COMMISSIONAMOUNT) AS FYC_CREDITS
FROM
    {{ ref('brokeradvanced_vc_agt_comm_insurance') }} AS BA
INNER JOIN
    {{ ref('commission_vc_agt_comm_insurance') }} AS C
    ON BA.AGENTCODE = C.OWNERCODE
INNER JOIN
    {{ ref('a_dim_agt_comm_contest') }} DIM
    ON BA.USERDEFINED2 = DIM.USERDEFINED2 AND BA.AGENTCODE = DIM.AGENTCODE
WHERE
    C.PAIDDATE >= (
                SELECT
                    LIFE_CREDITS_STARTDATE
                FROM
                    DATES
            )
            AND C.PAIDDATE <= (
                SELECT
                    LIFE_CREDITS_ENDDATE
                FROM
                    DATES
            )
    AND PAIDBY = 'carrierdirect'
    AND TRXTYPE = 'FYC'
    AND PLANTYPE IN (
        'DI',
        'Permanent',
        'Term',
        'CI',
        'UL',
        'SERVICE',
        'LTC',
        'Loan',
        'Life'
    )
GROUP BY
    1, 2
