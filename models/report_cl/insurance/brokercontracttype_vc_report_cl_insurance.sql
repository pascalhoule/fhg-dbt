{{			
    config (			
        materialized="view",			
        alias='brokercontracttype_vc', 			
        database='report_cl', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercontracttype_vc_analyze_insurance')  }}