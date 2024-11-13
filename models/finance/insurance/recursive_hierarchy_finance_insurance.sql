{{			
    config (			
        materialized="view",			
        alias='recursive_hierarchy', 			
        database='finance', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('recursive_hierarchy_analyze_insurance')  }}