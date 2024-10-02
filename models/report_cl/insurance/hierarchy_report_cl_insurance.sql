{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('hierarchy_analyze_insurance')  }}