{{			
    config (			
        materialized="view",			
        alias='clients', 			
        database='report', 			
        schema='investment'			
    )			
}}	

SELECT *
FROM {{ ref ('clients_analyze_investment')  }}