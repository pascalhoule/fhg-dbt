{{			
    config (			
        materialized="view",			
        alias='brokercontractprovince_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercontractprovince_vc_analyze_insurance')  }}