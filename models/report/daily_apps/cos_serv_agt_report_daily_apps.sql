{{			
    config (			
        materialized="view",			
        alias='cos_serv_agt', 			
        database='report', 			
        schema='daily_apps',
        tags=["daily_apps"]			
    )			
}}	

SELECT
    'BDD' AS ROLE,
    CASE
        WHEN
            CONTAINS(UPPER(COS_SALES_BDD), 'ALIMONTE')
            OR CONTAINS(UPPER(COS_SALES_BDD), 'DALIMONTE')
            THEN 'Rob D\'Alimonte'
        WHEN
            COS_SALES_BDD IN ('Vlad Dumitrescu')
            THEN 'Vladimir Dumitrescu' ELSE
            INITCAP(COS_SALES_BDD)
    END AS EMPLOYEE,
    AGENTCODE
FROM {{ ref('broker_fh_cl_integrate_insurance') }}
WHERE EMPLOYEE IS NOT null AND EMPLOYEE != '-'
GROUP BY 1, 2, 3

UNION

SELECT
    'BDC' AS ROLE,
    INITCAP(COS_SALES_BDC) AS EMPLOYEE,
    AGENTCODE
FROM {{ ref('broker_fh_cl_integrate_insurance') }}
WHERE EMPLOYEE IS NOT null AND EMPLOYEE != '-'
GROUP BY 1, 2, 3

UNION

SELECT
    'RVP' AS ROLE,
    INITCAP(COS_SALES_RVP) AS EMPLOYEE,
    AGENTCODE
FROM {{ ref('broker_fh_cl_integrate_insurance') }}
WHERE EMPLOYEE IS NOT null AND EMPLOYEE != '-'
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
