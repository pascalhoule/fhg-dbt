{{			
    config (			
        materialized="view",			
        alias='brokerphone_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokerphone_vc_analyze_insurance')  }}