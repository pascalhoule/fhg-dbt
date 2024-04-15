{{			
    config (			
        materialized="view",			
        alias='employee_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('employee_vc_analyze_insurance')  }}