{{			
    config (			
        materialized="view",			
        alias='aga', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('aga_analyze_insurance')  }}