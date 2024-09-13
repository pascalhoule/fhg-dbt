{{			
    config (			
        materialized="view",			
        alias='written_large_case', 			
        database='report', 			
        schema='daily_apps',
        tags=["daily_apps"]			
    )			
}}	

SELECT
    POLICYNUMBER,
    POLICYGROUPCODE,
    FH_STARTDATE,
    SUM(FH_FYCSERVAMT) AS WRITTEN_FYC
FROM
    {{ ref('policy_fh_report_insurance') }}
WHERE
    FH_STARTDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
GROUP BY
    1, 2, 3
HAVING
    WRITTEN_FYC > 25000