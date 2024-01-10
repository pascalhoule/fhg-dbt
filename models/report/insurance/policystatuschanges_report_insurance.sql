{{			
    config (			
        materialized="view",			
        alias='policystatuschanges', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('policystatuschanges_analyze_insurance')  }}