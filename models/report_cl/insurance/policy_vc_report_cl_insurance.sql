{{			
    config (			
        materialized="view",			
        alias='policy_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('policy_vc_analyze_insurance')  }}