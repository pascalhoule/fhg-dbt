{{			
    config (			
        materialized="view",			
        alias='placed_normal_case', 			
        database='report', 			
        schema='sales',
        tags=["sales", "large case"]			
    )			
}}

SELECT
    POLICYNUMBER,
    POLICYGROUPCODE,
    FH_PLACEDDATE,
    SUM(FH_FYCPLACED) AS PLACED_FYC
FROM
    {{ ref('policy_fh_report_insurance') }} 
WHERE
    FH_PLACEDDATE BETWEEN '2020-01-01'
    AND CURRENT_DATE
    AND FH_POLICYCATEGORY IN ('NEW POLICY', 'NEW RIDER')
GROUP BY
    1, 2, 3
HAVING
    PLACED_FYC < 25000