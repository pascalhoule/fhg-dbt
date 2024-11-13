{{			
    config (			
        materialized="view",			
        alias='recursive_hierarchy', 			
        database='analyze', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('recursive_hierarchy_integrate_insurance')  }}