{{			
    config (			
        materialized="view",			
        alias='brokercontract_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercontract_vc_analyze_insurance')  }}