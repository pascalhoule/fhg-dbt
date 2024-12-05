{{			
    config (			
        materialized="view",			
        alias='recursive_hierarchy', 			
        database='report_cl', 			
        schema='insurance',
        grants = {'ownership': ['BI_DEV']},			
    )			
}}	

SELECT *
FROM {{ ref ('recursive_hierarchy_analyze_insurance')  }}