{{			
    config (			
        materialized="view",			
        alias='policystatuschanges', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('policystatuschanges_analyze_insurance')  }}