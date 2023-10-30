{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='integrate', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('hierarchy_normalize_insurance')  }}