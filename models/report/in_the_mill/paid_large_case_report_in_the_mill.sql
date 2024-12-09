{{			
    config (			
        materialized="view",			
        alias='paid_large_case', 			
        database='report', 			
        schema='in_the_mill',
        grants = {'ownership': ['FH_READER']},
        tags=["in_the_mill", "large_case"]			
    )			
}}	

SELECT
    POLICYNUMBER,
    POLICYGROUPCODE,
    FH_SETTLEMENTDATE,
    SUM(FH_FYCCOMMAMT) AS PAID_FYC
FROM
    {{ ref('policy_fh_report_insurance') }}
where
    FH_SETTLEMENTDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
GROUP BY
    1, 2, 3
HAVING
    PAID_FYC > 25000