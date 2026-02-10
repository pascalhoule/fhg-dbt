{{			
    config (			
        materialized="view",			
        alias='fundproducts_fh', 			
        database='analyze', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('fundproducts_fh_integrate_investment') }}