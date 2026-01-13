{{			
    config (			
        materialized="view",			
        alias='region_fin_fh', 			
        database='analyze', 			
        schema='investment'  		
    )			
}}

SELECT *
FROM {{ ref('region_fin_fh_integrate_investment') }}