{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('hierarchy_analyze_insurance')  }}