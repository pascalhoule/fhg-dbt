{{ config( alias='d_aum_credits', database='agt_comm', schema='contest', materialized = "view" ) }}

SELECT
    BA.USERDEFINED2 AS UD2,
    BA.AGENTCODE,
    SUM(AUA.MARKETVALUE) AS SEG_AUMAMOUNT,
    SUM(AUA.MARKETVALUE) * 0.001 / 12 AS SEG_AUMCREDIT
FROM
    {{ ref('aua_vc_agt_comm_investment') }} AS AUA
LEFT JOIN
    {{ ref('representatives_vc_agt_comm_investment') }} AS REP
    ON AUA.REPCODE = REP.REPRESENTIATIVECODE
INNER JOIN
    {{ ref('brokeradvanced_vc_agt_comm_insurance') }} AS BA
    ON REP.INSAGENTCODE = BA.AGENTCODE
WHERE
    AUA.TRENDDATE = (
        SELECT MAX(AUM_TRENDDATE)
        FROM
            {{ source('contest', 'date_ranges') }}
    )
    AND BA.USERDEFINED2 IS NOT NULL
GROUP BY
    1, 2
