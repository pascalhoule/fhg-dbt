{{			
    config (			
        materialized="view",			
        alias='brokeradvanced_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokeradvanced_vc_analyze_insurance')  }}