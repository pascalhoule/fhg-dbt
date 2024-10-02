{{			
    config (			
        materialized="view",			
        alias='brokercontract_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercontract_vc_analyze_insurance')  }}