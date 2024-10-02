{{			
    config (			
        materialized="view",			
        alias='policystatus_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('policystatus_vc_analyze_insurance')  }}