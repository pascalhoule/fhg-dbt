{{			
    config (			
        materialized="view",			
        alias='policy_fh', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('policy_fh_analyze_insurance')  }}