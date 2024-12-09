{{			
    config (			
        materialized="view",			
        alias='written_normal_case', 			
        database='report', 			
        schema='in_the_mill',
        grants = {'ownership': ['FH_READER']},
        tags=["daily_apps", "large_case"]			
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
    WRITTEN_FYC <= 25000