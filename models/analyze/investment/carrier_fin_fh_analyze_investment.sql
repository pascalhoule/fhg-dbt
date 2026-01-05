{{			
    config (			
        materialized="view",			
        alias='carrier_fin_fh', 			
        database='analyze', 			
        schema='investment'  		
    )			
}}

SELECT *
FROM {{ ref('carrier_fin_fh_integrate_investment') }}