{{			
    config (			
        materialized="view",			
        alias='aga', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('aga_analyze_insurance')  }}