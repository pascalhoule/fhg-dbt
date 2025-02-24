{{			
    config (			
        materialized="view",			
        alias='written_normal_case', 			
        database='report', 			
        schema='daily_apps',
        tags=["daily_apps", "large_case"]			
    )			
}}

SELECT
    POLICYNUMBER,
    POLICYGROUPCODE,
    FH_STARTDATE,
    SUM(COMMPREMIUMAMOUNT*FH_SERVICINGAGTSPLIT) AS WRITTEN_COMM_PREM
FROM
    {{ ref('policy_fh_report_insurance') }} 
WHERE
    FH_STARTDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
GROUP BY
    1, 2, 3
    HAVING
    WRITTEN_COMM_PREM < 25000