{{			
    config (			
        materialized="view",			
        alias='carrier_fin_fh', 			
        database='integrate', 			
        schema='investment'  		
    )			
}}

SELECT 
*
FROM {{ source('norm', 'carrier_fin_fh') }}