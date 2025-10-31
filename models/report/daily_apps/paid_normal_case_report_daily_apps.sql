{{			
    config (			
        materialized="view",			
        alias='paid_normal_case', 			
        database='report', 			
        schema='daily_apps',
        tags=["daily_apps", "large case"]			
    )			
}}

 SELECT
    FH_POLICYCATEGORY,
    POLICYNUMBER,
    POLICYGROUPCODE,
    SUM(COMMPREMIUMAMOUNT*FH_COMMISSIONINGAGTSPLIT) AS PAID_COMM_PREM
FROM
    {{ ref('policy_fh_cl_report_insurance') }}
WHERE
    FH_SETTLEMENTDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_SETTLEMENTDATE is not null
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
    AND FH_CARRIERENG != 'CL Direct'
GROUP BY
    1, 2, 3
HAVING
    PAID_COMM_PREM < 25000

UNION

SELECT
    FH_POLICYCATEGORY,
    POLICYNUMBER,
    POLICYGROUPCODE,
    SUM(FH_PREM_COMMWGT) AS PAID_COMM_PREM
FROM
    {{ ref('policy_fh_cl_report_insurance') }}
WHERE
    FH_SETTLEMENTDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_SETTLEMENTDATE is not null
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
    AND FH_CARRIERENG = 'CL Direct'
GROUP BY
    1, 2, 3
HAVING
    PAID_COMM_PREM < 25000   