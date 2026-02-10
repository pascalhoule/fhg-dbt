{{			
    config (			
        materialized="view",			
        alias='fundproducts_fh', 			
        database='integrate', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('fundproducts_fh_normalize_investment') }}