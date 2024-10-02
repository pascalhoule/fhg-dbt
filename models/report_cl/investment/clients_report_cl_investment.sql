{{			
    config (			
        materialized="view",			
        alias='clients', 			
        database='report_cl', 			
        schema='investment'			
    )			
}}	

SELECT *
FROM {{ ref ('clients_analyze_investment')  }}