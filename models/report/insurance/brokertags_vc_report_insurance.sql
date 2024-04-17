{{			
    config (			
        materialized="view",			
        alias='brokertags_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokertags_vc_analyze_insurance')  }}