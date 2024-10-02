{{			
    config (			
        materialized="view",			
        alias='ic', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('ic_analyze_insurance')  }}