{{			
    config (			
        materialized="view",			
        alias='brokercos_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercos_vc_analyze_insurance')  }}