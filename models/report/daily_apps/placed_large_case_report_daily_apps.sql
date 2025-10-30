{{			
    config (			
        materialized="view",			
        alias='placed_large_case', 			
        database='report', 			
        schema='daily_apps',
        tags=["daily_apps", "large_case"]			
    )			
}}	

SELECT
    FH_POLICYCATEGORY,
    POLICYNUMBER,
    POLICYGROUPCODE,
    SUM(COMMPREMIUMAMOUNT*FH_SERVICINGAGTSPLIT) AS PLACED_COMM_PREM
FROM
   {{ ref('policy_fh_cl_report_insurance') }}
WHERE
    FH_PLACEDDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
    AND FH_PLACEDDATE is not null
GROUP BY
    1, 2, 3
HAVING
    PLACED_COMM_PREM >= 25000