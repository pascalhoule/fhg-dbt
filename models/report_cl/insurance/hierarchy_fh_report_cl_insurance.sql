{{			
    config (			
        materialized="view",			
        alias='hierarchy_fh', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('hierarchy_fh_analyze_insurance')  }}