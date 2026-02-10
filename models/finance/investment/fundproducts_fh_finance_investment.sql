{{			
    config (			
        materialized="view",			
        alias='fundproducts_fh', 			
        database='finance', 			
        schema='investment'  		
    )			
}}

SELECT * FROM {{ ref('fundproducts_fh_analyze_investment') }}