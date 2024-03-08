{{			
    config (			
        materialized="view",			
        alias='brokercontractstatus_vc', 			
        database='report', 			
        schema='insurance'			
    )			
}}	

SELECT *
FROM {{ ref ('brokercontractstatus_vc_analyze_insurance')  }}