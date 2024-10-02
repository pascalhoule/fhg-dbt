{{			
    config (			
        materialized="view",			
        alias='employee_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('employee_vc_analyze_insurance')  }}