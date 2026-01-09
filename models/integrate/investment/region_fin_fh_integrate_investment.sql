{{			
    config (			
        materialized="view",			
        alias='region_fin_fh', 			
        database='integrate', 			
        schema='investment'  		
    )			
}}

SELECT 
*
FROM
{{ source('norm', 'region_fin_fh') }}