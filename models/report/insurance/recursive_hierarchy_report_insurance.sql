{{			
    config (			
        materialized="view",			
        alias='recursive_hierarchy', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('recursive_hierarchy_analyze_insurance')  }}