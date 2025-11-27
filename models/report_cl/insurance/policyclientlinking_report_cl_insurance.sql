{{			
    config (			
        materialized="view",			
        alias='policyclientlinking', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('policyclientlinking_analyze_insurance')  }}