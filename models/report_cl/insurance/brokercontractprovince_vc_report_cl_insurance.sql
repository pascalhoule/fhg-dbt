{{			
    config (			
        materialized="view",			
        alias='brokercontractprovince_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercontractprovince_vc_analyze_insurance')  }}