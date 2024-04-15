{{			
    config (			
        materialized="view",			
        alias='brokeradvanced_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokeradvanced_vc_analyze_insurance')  }}