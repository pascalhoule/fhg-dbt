{{			
    config (			
        materialized="view",			
        alias='brokeraddress_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokeraddress_vc_analyze_insurance')  }}