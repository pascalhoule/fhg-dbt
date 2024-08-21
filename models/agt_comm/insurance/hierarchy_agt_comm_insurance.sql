{{			
    config (			
        materialized="view",			
        alias='hierarchy', 			
        database='agt_comm', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('hierarchy_analyze_insurance')  }}