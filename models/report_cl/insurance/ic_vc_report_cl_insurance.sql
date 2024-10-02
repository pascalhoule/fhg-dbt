{{			
    config (			
        materialized="view",			
        alias='ic_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('ic_vc_analyze_insurance')  }}