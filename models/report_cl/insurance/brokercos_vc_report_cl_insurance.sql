{{			
    config (			
        materialized="view",			
        alias='brokercos_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercos_vc_analyze_insurance')  }}