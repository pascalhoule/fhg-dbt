{{			
    config (			
        materialized="view",			
        alias='paid_large_case', 			
        database='report', 			
        schema='sales',
        grants = {'ownership': ['FH_READER']},
        tags=["sales", "large_case"]			
    )			
}}	

SELECT
    FH_POLICYCATEGORY,
    POLICYNUMBER,
    POLICYGROUPCODE,
    SUM(COMMPREMIUMAMOUNT*FH_COMMISSIONINGAGTSPLIT) AS PAID_COMM_PREM
FROM
    {{ ref('policy_fh_report_insurance') }}
WHERE
    FH_SETTLEMENTDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_SETTLEMENTDATE is not null
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
GROUP BY
    1, 2, 3
HAVING
    PAID_COMM_PREM >= 25000