{{			
    config (			
        materialized="view",			
        alias='recursive_hierarchy', 			
        database='integrate', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('recursive_hierarchy_normalize_insurance')  }}