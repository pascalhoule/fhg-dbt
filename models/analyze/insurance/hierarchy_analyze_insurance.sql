{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='analyze', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('hierarchy_integrate_insurance')  }}