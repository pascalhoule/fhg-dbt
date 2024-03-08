{{			
    config (			
        materialized="view",			
        alias='brokeremail_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokeremail_vc_analyze_insurance')  }}